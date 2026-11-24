//! Tests of the protocol with a scripted server.

use crate::Compression;
use crate::error::Error;
use crate::native::from_row_binary::RowBinaryColumns;
use crate::native::reader::WireFormat;
use crate::native::{Block, block_encoder, to_row_binary, varuint};
use crate::tcp::connection::{Connection, Hello, HelloAuth, QueryPacket, ServerPacket};
use crate::tcp::protocol::{self, CLIENT_REVISION, ClientInfo};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

struct Server {
    stream: DuplexStream,
}

impl Server {
    async fn varuint(&mut self) -> u64 {
        let mut value = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.stream.read_u8().await.unwrap();
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                break;
            }
        }
        value
    }

    async fn string(&mut self) -> String {
        let len = usize::try_from(self.varuint().await).unwrap();
        let mut buf = vec![0; len];
        self.stream.read_exact(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }

    async fn write(&mut self, data: &[u8]) {
        self.stream.write_all(data).await.unwrap();
    }

    /// Read everything that the client sent until it stops writing for a moment.
    async fn read_available(&mut self) -> Vec<u8> {
        let mut data = Vec::new();
        let mut buf = [0u8; 1];

        while let Ok(Ok(1)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            self.stream.read(&mut buf),
        )
        .await
        {
            data.push(buf[0]);
        }

        data
    }
}

fn varuint(out: &mut Vec<u8>, value: usize) {
    varuint::write(&mut *out, value);
}

fn string(out: &mut Vec<u8>, value: &str) {
    varuint(out, value.len());
    out.extend_from_slice(value.as_bytes());
}

fn block(columns: &[(&str, &str)], rows: &[u8]) -> Block {
    let header = Block::header(columns);

    let mut builder = RowBinaryColumns::new(&header).unwrap();
    let mut input = rows;
    while builder.read_row(&mut input).unwrap() {}
    assert!(input.is_empty());
    builder.take_block().unwrap()
}

fn encode(block: &Block, revision: u64) -> Vec<u8> {
    let mut out = BytesMut::new();
    let format = WireFormat {
        revision,
        variant_mode_prefix: true,
    };
    block_encoder::write_block(block, format, &mut out).unwrap();
    out.to_vec()
}

/// Connect to a scripted server that sends `Hello` with `server_revision`.
async fn connect(server_revision: u64) -> (Connection, Server) {
    let (client, server) = tokio::io::duplex(1);
    let mut server = Server { stream: server };

    let script = async {
        assert_eq!(server.varuint().await, 0, "Hello");
        assert!(server.string().await.starts_with("clickhouse-rs/"));
        let _major = server.varuint().await;
        let _minor = server.varuint().await;
        assert_eq!(server.varuint().await, CLIENT_REVISION);
        assert_eq!(server.string().await, "db");
        assert_eq!(server.string().await, "user");
        assert_eq!(server.string().await, "secret");

        let revision = server_revision.min(CLIENT_REVISION);
        let mut hello = Vec::new();
        varuint(&mut hello, 0);
        string(&mut hello, "ClickHouse");
        varuint(&mut hello, 26);
        varuint(&mut hello, 10);
        varuint(&mut hello, usize::try_from(server_revision).unwrap());
        if revision >= protocol::REVISION_WITH_SERVER_TIMEZONE {
            string(&mut hello, "UTC");
        }
        if revision >= protocol::REVISION_WITH_SERVER_DISPLAY_NAME {
            string(&mut hello, "display name");
        }
        if revision >= protocol::REVISION_WITH_VERSION_PATCH {
            varuint(&mut hello, 4);
        }
        server.write(&hello).await;

        if revision >= protocol::REVISION_WITH_ADDENDUM {
            assert_eq!(server.string().await, "quota", "quota key in the addendum");
        }
    };

    let user_agent = crate::headers::get_user_agent(&[], &[]);
    let hello = Hello {
        client_name: &user_agent,
        database: "db",
        auth: HelloAuth::Password {
            user: "user",
            password: "secret",
        },
        quota_key: "quota",
    };
    let connect = Connection::handshake(Box::new(client), &hello);

    let (conn, ()) = tokio::join!(connect, script);
    let conn = conn.unwrap();

    assert_eq!(conn.server().version_major, 26);
    assert_eq!(conn.server().version_minor, 10);
    (conn, server)
}

fn query<'a>(sql: &'a str, parameters: &'a [(&'a str, &'a str)]) -> QueryPacket<'a> {
    QueryPacket {
        query_id: "id",
        client_info: ClientInfo {
            client_name: "client",
            quota_key: "quota",
            #[cfg(feature = "opentelemetry")]
            trace_context: None,
        },
        settings: &[("max_threads", "1")],
        query: sql.as_bytes(),
        parameters,
        compression: Compression::None,
    }
}

#[tokio::test]
async fn handshake_with_old_and_new_servers() {
    for revision in [54405, 54449, 54458, CLIENT_REVISION, 54497] {
        let (conn, _) = connect(revision).await;
        assert_eq!(conn.server().revision, revision);
        assert_eq!(
            conn.server().version_patch,
            if revision >= protocol::REVISION_WITH_VERSION_PATCH {
                4
            } else {
                revision
            }
        );
        assert_eq!(conn.server().timezone.as_deref(), Some("UTC"));
    }
}

#[tokio::test]
async fn server_exception_in_handshake() {
    let (client, server) = tokio::io::duplex(64);
    let mut server = Server { stream: server };

    let script = async {
        server.read_available().await;
        let mut exception = Vec::new();
        varuint(&mut exception, 2);
        exception.extend_from_slice(&516i32.to_le_bytes());
        string(&mut exception, "DB::Exception");
        string(&mut exception, "DB::Exception: user: Authentication failed");
        string(&mut exception, "");
        exception.push(0);
        server.write(&exception).await;
    };

    let hello = Hello {
        client_name: "c",
        database: "",
        auth: HelloAuth::Jwt("token"),
        quota_key: "",
    };
    let connect = Connection::handshake(Box::new(client), &hello);

    let (result, ()) = tokio::join!(connect, script);
    let Err(Error::BadResponse(message)) = result else {
        panic!("expected an error");
    };
    assert!(
        message.starts_with(
            "Code: 516. DB::Exception: user: Authentication failed. (AUTHENTICATION_FAILED)"
        ),
        "{message}"
    );
}

#[tokio::test]
async fn query_packet() {
    let (mut conn, mut server) = connect(CLIENT_REVISION).await;

    conn.write_query(&query("SELECT {x:String}", &[("x", "it's")]))
        .unwrap();
    conn.write_data(None, Compression::None).unwrap();
    let (flushed, sent) = tokio::join!(conn.flush(), server.read_available());
    flushed.unwrap();

    let find = |needle: &[u8]| sent.windows(needle.len()).any(|window| window == needle);

    // Settings and parameters are custom settings with quoted values
    assert!(find(b"\x0bmax_threads\x02\x03'1'"));
    assert!(find(b"\x01x\x02\x07'it\\'s'"));
    assert!(find(b"\x11SELECT {x:String}"));
    // The query ID is the first field
    assert!(sent.starts_with(b"\x01\x02id"));
    // The empty block that ends the data
    assert!(sent.ends_with(&[2, 0, 1, 0, 2, 0xff, 0xff, 0xff, 0xff, 0, 0, 0]));
}

#[tokio::test]
async fn parameters_need_a_new_server() {
    let (mut conn, _) = connect(54449).await;
    let error = conn
        .write_query(&query("SELECT {x:UInt8}", &[("x", "1")]))
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "{error:?}");
}

#[tokio::test]
async fn all_packet_types() {
    for revision in [54449, CLIENT_REVISION] {
        all_packet_types_with(revision, false).await;

        #[cfg(feature = "lz4")]
        all_packet_types_with(revision, true).await;
    }
}

async fn all_packet_types_with(revision: u64, compressed: bool) {
    let (mut conn, mut server) = connect(revision).await;
    let revision = revision.min(CLIENT_REVISION);

    let header = block(&[("n", "UInt64"), ("s", "Nullable(String)")], &[]);
    let rows_data = [
        &1u64.to_le_bytes()[..],
        &[0, 1, b'a'],
        &2u64.to_le_bytes()[..],
        &[1],
    ]
    .concat();
    let rows = block(&[("n", "UInt64"), ("s", "Nullable(String)")], &rows_data);
    let log = block(&[("text", "String")], &[3, b'l', b'o', b'g']);

    let data_packet = |packet: usize, block: &Block, compress: bool| {
        let mut out = Vec::new();
        varuint(&mut out, packet);
        string(&mut out, "");
        let encoded = encode(block, revision);

        if !compress {
            out.extend_from_slice(&encoded);
            return out;
        }

        // Split the block into two compressed frames.
        #[cfg(feature = "lz4")]
        for part in encoded.chunks(encoded.len() / 2 + 1) {
            out.extend_from_slice(&crate::compression::lz4::compress(part).unwrap());
        }

        out
    };

    let mut response = Vec::new();
    // TableColumns
    varuint(&mut response, 11);
    string(&mut response, "");
    string(&mut response, "columns format version: 1");
    // Log and ProfileEvents are never compressed
    response.extend(data_packet(10, &log, false));
    response.extend(data_packet(14, &log, false));
    // TimezoneUpdate
    varuint(&mut response, 17);
    string(&mut response, "Europe/Berlin");
    // PartUUIDs
    varuint(&mut response, 12);
    varuint(&mut response, 1);
    response.extend_from_slice(&[7; 16]);
    // Progress
    varuint(&mut response, 3);
    for value in [10, 100, 1000, 1, 2] {
        varuint(&mut response, value);
    }
    if revision >= protocol::REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
        // Elapsed time
        varuint(&mut response, 5);
    }
    response.extend(data_packet(1, &header, compressed));
    response.extend(data_packet(1, &rows, compressed));
    // Totals and Extremes
    response.extend(data_packet(7, &rows, compressed));
    response.extend(data_packet(8, &rows, compressed));
    // ProfileInfo
    varuint(&mut response, 6);
    for value in [2, 1, 17] {
        varuint(&mut response, value);
    }
    response.extend_from_slice(&[0, 0, 1]);
    // EndOfStream
    varuint(&mut response, 5);

    let reader = async {
        let mut packets = Vec::new();
        loop {
            let packet = conn.read_packet(compressed).await.unwrap();
            let end = matches!(packet, ServerPacket::EndOfStream);
            packets.push(packet);
            if end {
                return packets;
            }
        }
    };

    let (packets, ()) = tokio::join!(reader, server.write(&response));

    let mut data = Vec::new();
    let mut progress = None;
    let mut profile = None;
    let mut others = 0;

    for packet in packets {
        match packet {
            ServerPacket::Data(block) => data.push(block),
            ServerPacket::Progress(value) => progress = Some(value),
            ServerPacket::ProfileInfo(value) => profile = Some(value),
            ServerPacket::Other | ServerPacket::TableColumns | ServerPacket::ProfileEvents(_) => {
                others += 1;
            }
            ServerPacket::EndOfStream => {}
            ServerPacket::Pong | ServerPacket::Exception(_) => panic!("unexpected packet"),
        }
    }

    // TableColumns, Log, ProfileEvents, TimezoneUpdate, PartUUIDs, Totals, Extremes
    assert_eq!(others, 7);
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].num_rows(), 0);
    assert_eq!(
        data[0].columns()[1].data_type().to_string(),
        "Nullable(String)"
    );

    let mut converted = BytesMut::new();
    to_row_binary::write_rows(&data[1], &mut converted).unwrap();
    assert_eq!(&converted[..], &rows_data[..]);

    let progress = progress.unwrap();
    assert_eq!(
        (
            progress.read_rows,
            progress.read_bytes,
            progress.total_rows_to_read
        ),
        (10, 100, 1000)
    );
    assert_eq!((progress.written_rows, progress.written_bytes), (1, 2));
    assert_eq!(
        progress.elapsed_ns,
        if revision >= protocol::REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
            5
        } else {
            0
        }
    );
    let profile = profile.unwrap();
    assert_eq!((profile.rows, profile.bytes), (2, 17));
}

#[tokio::test]
async fn nested_exception_and_reuse() {
    let (mut conn, mut server) = connect(CLIENT_REVISION).await;

    let mut response = Vec::new();
    varuint(&mut response, 2);
    for (code, message, nested) in [
        (60, "DB::Exception: Unknown table", 1),
        (1000, "Poco::Exception: inner", 0),
    ] {
        response.extend_from_slice(&i32::to_le_bytes(code));
        string(&mut response, message.split(':').next().unwrap());
        string(&mut response, message);
        string(&mut response, "stack trace");
        response.push(nested);
    }
    // The next query gets `Pong`
    varuint(&mut response, 4);

    let reader = async {
        let first = conn.read_packet(false).await.unwrap();
        let second = conn.read_packet(false).await.unwrap();
        (first, second)
    };
    let ((first, second), ()) = tokio::join!(reader, server.write(&response));

    let ServerPacket::Exception(Error::BadResponse(message)) = first else {
        panic!("expected an exception");
    };
    assert!(
        message.starts_with(
            "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE) (version 26.10.4)"
        ),
        "{message}"
    );
    assert!(
        message.contains("Code: 1000. Poco::Exception: inner. (POCO_EXCEPTION)"),
        "{message}"
    );
    assert!(matches!(second, ServerPacket::Pong));
}

#[tokio::test]
async fn corrupted_data_is_an_error() {
    let (mut conn, mut server) = connect(CLIENT_REVISION).await;

    // Unknown packet type
    let ((), result) = tokio::join!(server.write(&[99]), conn.read_packet(false));
    assert!(matches!(result, Err(Error::BadResponse(_))));

    // A string longer than the limit
    let (mut conn, mut server) = connect(CLIENT_REVISION).await;
    let mut packet = Vec::new();
    varuint(&mut packet, 17);
    varuint(&mut packet, 1 << 40);
    let ((), result) = tokio::join!(server.write(&packet), conn.read_packet(false));
    assert!(matches!(result, Err(Error::BadResponse(_))));

    // The connection is closed in the middle of a packet
    let (mut conn, server) = connect(CLIENT_REVISION).await;
    drop(server);
    assert!(matches!(
        conn.read_packet(false).await,
        Err(Error::Network(_))
    ));
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn corrupted_frame_checksum() {
    let (mut conn, mut server) = connect(CLIENT_REVISION).await;

    let mut frame = crate::compression::lz4::compress(&encode(
        &block(&[("n", "UInt8")], &[1]),
        CLIENT_REVISION,
    ))
    .unwrap()
    .to_vec();
    let last = frame.len() - 1;
    frame[last] ^= 0xff;

    let mut packet = Vec::new();
    varuint(&mut packet, 1);
    string(&mut packet, "");
    packet.extend_from_slice(&frame);

    let ((), result) = tokio::join!(server.write(&packet), conn.read_packet(true));
    let Err(Error::Decompression(error)) = result else {
        panic!("expected a decompression error");
    };
    assert!(error.to_string().contains("checksum"), "{error}");
}

fn profile_events_block(rows: &[(&str, u64, i8, &str, i64)]) -> Block {
    let mut data = Vec::new();

    for &(host, thread, kind, name, value) in rows {
        string(&mut data, host);
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&thread.to_le_bytes());
        data.extend_from_slice(&kind.to_le_bytes());
        string(&mut data, name);
        data.extend_from_slice(&value.to_le_bytes());
    }

    block(
        &[
            ("host_name", "String"),
            ("current_time", "DateTime"),
            ("thread_id", "UInt64"),
            ("type", "Enum8('increment' = 1, 'gauge' = 2)"),
            ("name", "LowCardinality(String)"),
            ("value", "Int64"),
        ],
        &data,
    )
}

#[test]
fn profile_events_are_aggregated() {
    use crate::tcp::profile_events::ProfileEvents;

    let mut events = ProfileEvents::default();

    events.add(&profile_events_block(&[
        ("a", 0, 1, "ReadCompressedBytes", 100),
        // Rows of threads repeat the values of the query (older servers).
        ("a", 7, 1, "ReadCompressedBytes", 100),
        ("a", 0, 2, "MemoryTrackerPeakUsage", 50),
        ("b", 0, 2, "MemoryTrackerPeakUsage", 5),
    ]));
    events.add(&profile_events_block(&[
        ("a", 0, 1, "ReadCompressedBytes", 23),
        ("b", 0, 1, "SelectedRows", 3),
        ("a", 0, 2, "MemoryTrackerPeakUsage", 70),
    ]));
    // A block with other columns is ignored.
    events.add(&block(&[("text", "String")], &[1, b'x']));

    let values = events.values();
    assert_eq!(values.get("ReadCompressedBytes"), Some(&123));
    assert_eq!(values.get("SelectedRows"), Some(&3));
    // The last value of each host, summed over hosts.
    assert_eq!(values.get("MemoryTrackerPeakUsage"), Some(&75));
    assert_eq!(values.len(), 3);
}
