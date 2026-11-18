//! Tests for the cache of table metadata that `Client::insert()` fills with `DESCRIBE TABLE`.
//!
//! They use a small local HTTP server instead of ClickHouse, because the server must be able
//! to hold the response to `DESCRIBE TABLE`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use clickhouse::{Client, Compression, Row};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::timeout;

/// The server holds the response to `DESCRIBE TABLE` for this table until `release()`.
const HELD_TABLE: &str = "held";

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Row, Serialize)]
struct Data {
    id: u64,
}

#[derive(Default)]
struct State {
    /// Tables from the received `DESCRIBE TABLE` queries, in order of arrival.
    described: Mutex<Vec<String>>,
    received: Notify,
    released: Mutex<bool>,
    release: Notify,
}

struct DescribeServer {
    url: String,
    state: Arc<State>,
}

impl DescribeServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(State::default());

        tokio::spawn({
            let state = state.clone();
            async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    tokio::spawn(serve(stream, state.clone()));
                }
            }
        });

        Self { url, state }
    }

    fn client(&self) -> Client {
        Client::default()
            .with_url(&self.url)
            .with_compression(Compression::None)
    }

    fn describe_count(&self, table: &str) -> usize {
        let described = self.state.described.lock().unwrap();
        described.iter().filter(|name| *name == table).count()
    }

    /// Waits until the server has received `DESCRIBE TABLE` for the table.
    async fn wait_for_describe(&self, table: &str) {
        timeout(TIMEOUT, async {
            loop {
                let received = self.state.received.notified();
                if self.describe_count(table) > 0 {
                    return;
                }
                received.await;
            }
        })
        .await
        .expect("the server did not receive DESCRIBE TABLE");
    }

    fn release(&self) {
        *self.state.released.lock().unwrap() = true;
        self.state.release.notify_waiters();
    }
}

async fn serve(mut stream: TcpStream, state: Arc<State>) {
    let query = read_query(&mut stream).await;
    let table = query
        .strip_prefix("DESCRIBE TABLE ")
        .unwrap_or_else(|| panic!("unexpected query: {query}"))
        .trim_matches('`')
        .to_string();

    state.described.lock().unwrap().push(table.clone());
    state.received.notify_waiters();

    if table == HELD_TABLE {
        loop {
            let release = state.release.notified();
            if *state.released.lock().unwrap() {
                break;
            }
            release.await;
        }
    }

    let body = describe_response();
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.shutdown().await.unwrap();
}

/// Reads one HTTP request and returns its body, which is the SQL query.
async fn read_query(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut buf = [0; 1024];

    loop {
        let read = stream.read(&mut buf).await.unwrap();
        assert_ne!(read, 0, "the client closed the connection");
        request.extend_from_slice(&buf[..read]);

        let Some(head_len) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&request[..head_len]).to_ascii_lowercase();
        let body_len: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .expect("the request has no Content-Length")
            .trim()
            .parse()
            .unwrap();

        let body = &request[head_len + 4..];
        if body.len() >= body_len {
            return String::from_utf8(body[..body_len].to_vec()).unwrap();
        }
    }
}

/// The result of `DESCRIBE TABLE` for a table with one column `id UInt64`,
/// in the `RowBinaryWithNamesAndTypes` format.
fn describe_response() -> Vec<u8> {
    const COLUMNS: [&str; 7] = [
        "name",
        "type",
        "default_type",
        "default_expression",
        "comment",
        "codec_expression",
        "ttl_expression",
    ];
    const ROW: [&str; 7] = ["id", "UInt64", "", "", "", "", ""];

    // All lengths are less than 128, so each of them is one LEB128 byte.
    fn put_str(out: &mut Vec<u8>, s: &str) {
        out.push(s.len() as u8);
        out.extend_from_slice(s.as_bytes());
    }

    let mut out = vec![COLUMNS.len() as u8];
    for name in COLUMNS {
        put_str(&mut out, name);
    }
    for _ in COLUMNS {
        put_str(&mut out, "String");
    }
    for value in ROW {
        put_str(&mut out, value);
    }
    out
}

#[tokio::test]
async fn describe_does_not_block_other_tables() {
    let server = DescribeServer::start().await;
    let client = server.client();

    // Put the metadata of one table into the cache.
    let _insert = client.insert::<Data>("cached").await.unwrap();

    let held = tokio::spawn({
        let client = client.clone();
        async move { client.insert::<Data>(HELD_TABLE).await.map(drop) }
    });
    server.wait_for_describe(HELD_TABLE).await;

    let _insert = timeout(TIMEOUT, client.insert::<Data>("cached"))
        .await
        .expect("an insert into a cached table waited for DESCRIBE TABLE of another table")
        .unwrap();
    assert_eq!(server.describe_count("cached"), 1);

    let _insert = timeout(TIMEOUT, client.insert::<Data>("other"))
        .await
        .expect("DESCRIBE TABLE waited for DESCRIBE TABLE of another table")
        .unwrap();

    server.release();
    timeout(TIMEOUT, held).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_inserts_describe_a_table_once() {
    let server = DescribeServer::start().await;
    let client = server.client();

    let inserts: Vec<_> = (0..4)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.insert::<Data>(HELD_TABLE).await.map(drop) })
        })
        .collect();
    server.wait_for_describe(HELD_TABLE).await;
    server.release();

    for insert in inserts {
        timeout(TIMEOUT, insert).await.unwrap().unwrap().unwrap();
    }

    assert_eq!(server.describe_count(HELD_TABLE), 1);
}

#[tokio::test]
async fn clear_does_not_wait_for_describe() {
    let server = DescribeServer::start().await;
    let client = server.client();

    let held = tokio::spawn({
        let client = client.clone();
        async move { client.insert::<Data>(HELD_TABLE).await.map(drop) }
    });
    server.wait_for_describe(HELD_TABLE).await;

    timeout(TIMEOUT, client.clear_cached_metadata())
        .await
        .expect("clear_cached_metadata() waited for DESCRIBE TABLE");

    server.release();
    timeout(TIMEOUT, held).await.unwrap().unwrap().unwrap();

    // The cache was cleared while the request was in flight, so its result is not kept.
    let _insert = client.insert::<Data>(HELD_TABLE).await.unwrap();
    assert_eq!(server.describe_count(HELD_TABLE), 2);
}

#[tokio::test]
async fn cancelled_describe_is_sent_again() {
    let server = DescribeServer::start().await;
    let client = server.client();

    let held = tokio::spawn({
        let client = client.clone();
        async move { client.insert::<Data>(HELD_TABLE).await.map(drop) }
    });
    server.wait_for_describe(HELD_TABLE).await;

    held.abort();
    assert!(held.await.unwrap_err().is_cancelled());
    server.release();

    let _insert = timeout(TIMEOUT, client.insert::<Data>(HELD_TABLE))
        .await
        .expect("an insert waited for a cancelled DESCRIBE TABLE")
        .unwrap();
    assert_eq!(server.describe_count(HELD_TABLE), 2);
}
