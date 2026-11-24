//! Execution of queries that return data.

use crate::error::{Error, Result};
use crate::native::reader::WireFormat;
use crate::native::{Block, block_encoder, to_row_binary};
use crate::query_summary::QuerySummary;
use crate::response::{Chunk, Chunks, Response};
use crate::tcp::connection::{Connection, ProfileInfo, Progress, QueryPacket, ServerPacket};
use crate::tcp::pool::{Pool, PooledConnection};
use crate::tcp::protocol::ClientInfo;
use crate::tcp::{Request, changes_session, leading_words, params, trailing_format};
use crate::{Client, Compression};
use bytes::BytesMut;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The number of converted chunks that the query task sends ahead of the cursor.
const CHANNEL_CAPACITY: usize = 4;

/// How long to wait for the end of a canceled query before the connection is closed.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(5);

/// The format that the cursor reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    /// The result is not read (`Query::execute()`).
    Discard,
    RowBinary {
        names: bool,
        types: bool,
    },
    Native,
}

impl Output {
    pub(crate) fn new(format: Option<&str>, sql: &str) -> Result<Self> {
        let Some(format) = format else {
            return Ok(Self::Discard);
        };

        // Like the HTTP interface, a `FORMAT` clause overrides the default format.
        Ok(match trailing_format(sql).unwrap_or(format) {
            "RowBinary" => Self::RowBinary {
                names: false,
                types: false,
            },
            "RowBinaryWithNames" => Self::RowBinary {
                names: true,
                types: false,
            },
            "RowBinaryWithNamesAndTypes" => Self::RowBinary {
                names: true,
                types: true,
            },
            "Native" => Self::Native,
            other => {
                return Err(Error::Unsupported(format!(
                    "format {other} is not supported by the native TCP transport; \
                     supported formats: Native, RowBinary, RowBinaryWithNames, \
                     RowBinaryWithNamesAndTypes"
                )));
            }
        })
    }

    fn write_header(self, block: &Block, out: &mut BytesMut) -> Result<()> {
        match self {
            Self::RowBinary { names: true, types } => {
                to_row_binary::write_names_and_types(block, types, out)
            }
            _ => Ok(()),
        }
    }

    /// `server_format` is the format of the server: the `Native` output is the same as the
    /// output of the server's `Native` format.
    fn write_rows(
        self,
        block: &Block,
        server_format: WireFormat,
        out: &mut BytesMut,
    ) -> Result<()> {
        if block.num_rows() == 0 {
            return Ok(());
        }

        match self {
            Self::Discard => Ok(()),
            Self::RowBinary { .. } => to_row_binary::write_rows(block, out),
            Self::Native => block_encoder::write_block(
                block,
                WireFormat {
                    revision: 0,
                    ..server_format
                },
                out,
            ),
        }
    }
}

/// Execute a query, returning a response with the data in `format`.
pub(crate) fn query(client: &Client, sql: String, format: Option<&str>) -> Result<Response> {
    let output = Output::new(format, &sql)?;
    let mut request = Request::new(client)?;

    if matches!(output, Output::RowBinary { .. })
        && request
            .setting("output_format_binary_write_json_as_string")
            .is_some_and(|value| value == "1")
    {
        // The native protocol sends JSON in its binary serialization by default,
        // which cannot be converted to the strings of `RowBinary`.
        request.add_setting("output_format_native_write_json_as_string", "1");
    }

    let pool = client.tcp.clone();
    let (header_tx, header_rx) = oneshot::channel();
    let (chunk_tx, chunk_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let future = async move {
        tokio::spawn(run(
            pool,
            request,
            sql,
            output,
            Sink::new(header_tx, chunk_tx),
        ));

        match header_rx.await {
            Ok(Ok(summary)) => Ok((Chunks::from_receiver(chunk_rx), summary)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Network("the query task stopped unexpectedly".into())),
        }
    };

    Ok(Response::Waiting(Box::pin(future)))
}

type HeaderResult = Result<Option<Box<QuerySummary>>>;

/// Delivers the response to the cursor.
struct Sink {
    header_tx: Option<oneshot::Sender<HeaderResult>>,
    chunk_tx: mpsc::Sender<Result<Chunk>>,
    /// The response is collected before it is sent (`wait_end_of_query = 1`).
    buffer: Option<Vec<Chunk>>,
}

impl Sink {
    fn new(
        header_tx: oneshot::Sender<HeaderResult>,
        chunk_tx: mpsc::Sender<Result<Chunk>>,
    ) -> Self {
        Self {
            header_tx: Some(header_tx),
            chunk_tx,
            buffer: None,
        }
    }

    /// Send the start of the response, if not sent yet.
    fn start(&mut self, summary: QuerySummary) {
        if self.buffer.is_none()
            && let Some(header_tx) = self.header_tx.take()
        {
            // The cursor may be dropped already.
            let _ = header_tx.send(Ok(Some(Box::new(summary))));
        }
    }

    /// Returns `false` if the cursor is dropped.
    async fn send(&mut self, chunk: Chunk) -> bool {
        if chunk.data.is_empty() {
            return true;
        }

        match &mut self.buffer {
            Some(buffer) => {
                buffer.push(chunk);
                true
            }
            None => self.chunk_tx.send(Ok(chunk)).await.is_ok(),
        }
    }

    /// Send the end of a successful response.
    async fn finish(mut self, summary: QuerySummary) {
        let buffer = self.buffer.take();
        self.start(summary);

        for chunk in buffer.into_iter().flatten() {
            if self.chunk_tx.send(Ok(chunk)).await.is_err() {
                return;
            }
        }
    }

    async fn fail(mut self, error: Error) {
        match self.header_tx.take() {
            Some(header_tx) => {
                let _ = header_tx.send(Err(error));
            }
            None => {
                let _ = self.chunk_tx.send(Err(error)).await;
            }
        }
    }
}

enum Outcome {
    Completed {
        summary: QuerySummary,
    },
    /// The cursor was dropped before the end of the response.
    Canceled {
        compression: Compression,
    },
}

/// An error and whether the connection can be used again.
pub(crate) struct Failure {
    pub(crate) error: Error,
    pub(crate) reusable: bool,
}

impl Failure {
    pub(crate) fn broken(error: Error) -> Self {
        Self {
            error,
            reusable: false,
        }
    }
}

async fn run(pool: Arc<Pool>, request: Request, sql: String, output: Output, mut sink: Sink) {
    if request.wait_end_of_query {
        sink.buffer = Some(Vec::new());
    }

    let mut pooled = match pool.acquire(&request.key, request.session.clone()).await {
        Ok(pooled) => pooled,
        Err(e) => return sink.fail(e).await,
    };

    let result = match pooled.conn() {
        Ok(conn) => execute(conn, &request, sql.as_bytes(), output, &mut sink).await,
        Err(e) => Err(Failure::broken(e)),
    };

    let keeps_session = request.session.is_some();
    let stateless = keeps_session || !changes_session(sql.as_bytes());

    match result {
        Ok(Outcome::Completed { summary }) => {
            pooled.release(stateless);
            sink.finish(summary).await;
        }
        Ok(Outcome::Canceled { compression }) => {
            let reusable = cancel(&mut pooled, compression).await;
            pooled.release(reusable && stateless);
        }
        Err(failure) => {
            pooled.release(failure.reusable && stateless);
            sink.fail(failure.error).await;
        }
    }
}

/// Send the `Query` packet and the empty block of external tables.
///
/// Returns the compression of the query, which can differ from the compression of the request.
pub(crate) async fn send_query(
    conn: &mut Connection,
    request: &Request,
    sql: &[u8],
) -> Result<Compression, Failure> {
    set_roles(conn, request).await?;

    let mut settings: Vec<(&str, &str)> = request
        .settings
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();

    let (compression, compression_settings) = negotiate_compression(conn, request).await?;
    settings.extend(
        compression_settings
            .iter()
            .map(|(name, value)| (*name, value.as_str())),
    );
    let (sql, renamed) = parameters(conn, request, sql);
    let parameters: Vec<(&str, &str)> = renamed
        .iter()
        .map(|(name, value)| (name.as_ref(), *value))
        .collect();

    conn.write_query(&QueryPacket {
        query_id: &request.query_id,
        client_info: ClientInfo {
            client_name: &request.key.client_name,
            quota_key: &request.key.quota_key,
            #[cfg(feature = "opentelemetry")]
            trace_context: request.trace_context.clone(),
        },
        settings: &settings,
        query: &sql,
        parameters: &parameters,
        compression,
    })
    // Nothing was sent yet.
    .map_err(|error| Failure {
        error,
        reusable: true,
    })?;

    conn.write_data(None, compression)
        .map_err(Failure::broken)?;
    conn.flush().await.map_err(Failure::broken)?;
    Ok(compression)
}

/// Query parameters as `(name, value)` pairs.
type Parameters<'a> = Vec<(Cow<'a, str>, &'a str)>;

/// The query and the query parameters to send.
///
/// Servers before 26.7 read parameters as settings, so a parameter with the name of a setting
/// (for example `limit`) fails. For these servers, the parameters are renamed in the query.
fn parameters<'a>(
    conn: &Connection,
    request: &'a Request,
    sql: &'a [u8],
) -> (Cow<'a, [u8]>, Parameters<'a>) {
    let server = conn.server();

    if request.parameters.is_empty() || (server.version_major, server.version_minor) >= (26, 7) {
        let parameters = request
            .parameters
            .iter()
            .map(|(name, value)| (Cow::Borrowed(name.as_str()), value.as_str()))
            .collect();
        return (Cow::Borrowed(sql), parameters);
    }

    let names: Vec<&str> = request
        .parameters
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    let parameters = request
        .parameters
        .iter()
        .map(|(name, value)| {
            (
                Cow::Owned(format!("{}{name}", params::PREFIX)),
                value.as_str(),
            )
        })
        .collect();

    (
        Cow::Owned(params::rename_parameters(sql, &names)),
        parameters,
    )
}

/// A compression method of the native protocol: the name and the ZSTD level.
type Method = (&'static str, Option<i32>);

/// The method of `compression`, or `None` if compression is disabled.
fn method(compression: Compression) -> Option<Method> {
    match compression {
        Compression::None => None,
        #[cfg(feature = "lz4")]
        #[allow(deprecated)]
        Compression::Lz4 | Compression::Lz4Hc(_) => Some(("LZ4", None)),
        #[cfg(feature = "zstd")]
        Compression::Zstd(level) => Some(("ZSTD", Some(level))),
    }
}

/// Returns `true` if this build can decompress data compressed with `method`.
fn can_decompress(method: Method) -> bool {
    (method.0 == "LZ4" && cfg!(feature = "lz4")) || (method.0 == "ZSTD" && cfg!(feature = "zstd"))
}

/// Select the compression of a query and the settings that request it.
///
/// The server compresses data with `network_compression_method`: `LZ4` by default,
/// `ZSTD(3)` since 26.9. If the method of the client is different, it is requested.
/// A user with `readonly = 1` cannot change settings: then the default method of the server is
/// used if this build can decompress it, else the query is not compressed.
async fn negotiate_compression(
    conn: &mut Connection,
    request: &Request,
) -> Result<(Compression, Vec<(&'static str, String)>), Failure> {
    let Some(desired) = method(request.compression) else {
        return Ok((Compression::None, Vec::new()));
    };

    if request.setting("network_compression_method").is_some() {
        // The user selected the method.
        return Ok((request.compression, Vec::new()));
    }

    let server = conn.server();
    let server_default: Method = if (server.version_major, server.version_minor) >= (26, 9) {
        ("ZSTD", Some(3))
    } else {
        ("LZ4", None)
    };

    let same_method =
        desired.0 == server_default.0 && (desired.0 != "ZSTD" || desired.1 == server_default.1);

    if same_method {
        return Ok((request.compression, Vec::new()));
    }

    if can_change_compression(conn).await? {
        let mut settings = vec![("network_compression_method", desired.0.to_owned())];

        if let Some(level) = desired.1 {
            settings.push(("network_zstd_compression_level", level.to_string()));
        }

        return Ok((request.compression, settings));
    }

    if can_decompress(server_default) {
        Ok((request.compression, Vec::new()))
    } else {
        Ok((Compression::None, Vec::new()))
    }
}

/// Returns `true` if the user can change settings: `readonly` is not `1`.
///
/// The result is remembered for the connection.
async fn can_change_compression(conn: &mut Connection) -> Result<bool, Failure> {
    if let Some(known) = conn.can_change_compression {
        return Ok(known);
    }

    let probe = QueryPacket {
        query_id: "",
        client_info: ClientInfo {
            client_name: "",
            quota_key: "",
            #[cfg(feature = "opentelemetry")]
            trace_context: None,
        },
        settings: &[],
        query: b"SELECT toUInt64(getSetting('readonly'))",
        parameters: &[],
        compression: Compression::None,
    };

    conn.write_query(&probe).map_err(Failure::broken)?;
    conn.write_data(None, Compression::None)
        .map_err(Failure::broken)?;
    conn.flush().await.map_err(Failure::broken)?;

    let mut readonly = None;

    loop {
        match conn.read_packet(false).await.map_err(Failure::broken)? {
            ServerPacket::Data(block) if block.num_rows() > 0 => {
                readonly = block
                    .columns()
                    .first()
                    .and_then(|column| column.iter::<u64>().ok()?.next()?.ok());
            }
            ServerPacket::EndOfStream => break,
            ServerPacket::Exception(error) => {
                return Err(Failure {
                    error,
                    reusable: true,
                });
            }
            _ => {}
        }
    }

    let known = readonly != Some(1);
    conn.can_change_compression = Some(known);
    Ok(known)
}

/// Make the roles of the session of `conn` match the roles of the request.
async fn set_roles(conn: &mut Connection, request: &Request) -> Result<(), Failure> {
    if conn.roles == request.roles {
        return Ok(());
    }

    let mut sql = String::from("SET ROLE ");

    if request.roles.is_empty() {
        sql.push_str("DEFAULT");
    } else {
        for (index, role) in request.roles.iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }

            crate::sql::escape::identifier(role, &mut sql)
                .map_err(|e| Failure::broken(Error::Other(e.to_string().into())))?;
        }
    }

    let set_roles = Request {
        roles: conn.roles.clone(),
        settings: Vec::new(),
        parameters: Vec::new(),
        query_id: String::new(),
        session: None,
        key: request.key.clone(),
        compression: request.compression,
        wait_end_of_query: false,
        #[cfg(feature = "opentelemetry")]
        trace_context: None,
    };

    // `set_roles` has the roles of the connection, so `send_query()` does not recurse.
    let compression = Box::pin(send_query(conn, &set_roles, sql.as_bytes())).await?;
    drain(conn, compression, None).await?;

    conn.roles.clone_from(&request.roles);
    Ok(())
}

/// Detects that the server waits for the data of an `INSERT`.
#[derive(Default)]
pub(crate) struct DataRequest {
    table_columns: bool,
    answered: bool,
}

impl DataRequest {
    /// Returns `true` once if `packet` is the request of the server for the data of an `INSERT`.
    ///
    /// The server sends the structure of the table as a block without rows. Before it,
    /// the server sends the columns of the table, unless `input_format_defaults_for_omitted_fields`
    /// is disabled. Since 22.8 (or earlier), the server sends a block for an `INSERT` only when
    /// it waits for the data; older servers also send blocks for `INSERT` with inline data.
    pub(crate) fn check(&mut self, packet: &ServerPacket, conn: &Connection, sql: &[u8]) -> bool {
        match packet {
            ServerPacket::TableColumns => {
                self.table_columns = true;
                false
            }
            ServerPacket::Data(_) if !self.answered => {
                let server = conn.server();
                let is_insert = leading_words(sql, 1)
                    .first()
                    .is_some_and(|word| word.eq_ignore_ascii_case(b"INSERT"));

                self.answered = true;
                self.table_columns
                    || (is_insert && (server.version_major, server.version_minor) >= (22, 8))
            }
            _ => false,
        }
    }
}

/// Read the packets of a query that returns no data, until the end.
///
/// If the server waits for the data of an `INSERT` in `sql`, the end of data is sent.
pub(crate) async fn drain(
    conn: &mut Connection,
    compression: Compression,
    sql: Option<&[u8]>,
) -> Result<Progress, Failure> {
    let mut progress = Progress::default();
    let mut data_request = DataRequest::default();

    loop {
        let packet = conn
            .read_packet(compression.is_enabled())
            .await
            .map_err(Failure::broken)?;

        if let Some(sql) = sql
            && data_request.check(&packet, conn, sql)
        {
            conn.write_data(None, compression)
                .map_err(Failure::broken)?;
            conn.flush().await.map_err(Failure::broken)?;
            continue;
        }

        match packet {
            ServerPacket::EndOfStream => return Ok(progress),
            ServerPacket::Exception(error) => {
                return Err(Failure {
                    error,
                    reusable: true,
                });
            }
            ServerPacket::Progress(increment) => progress.add(&increment),
            _ => {}
        }
    }
}

async fn execute(
    conn: &mut Connection,
    request: &Request,
    sql: &[u8],
    output: Output,
    sink: &mut Sink,
) -> Result<Outcome, Failure> {
    let compression = send_query(conn, request, sql).await?;

    let compressed = compression.is_enabled();
    let mut data_request = DataRequest::default();

    let mut progress = Progress::default();
    let mut profile: Option<ProfileInfo> = None;
    let mut received_header = false;
    let mut received_bytes = conn.received_bytes();

    loop {
        let packet = conn
            .read_packet(compressed)
            .await
            .map_err(Failure::broken)?;

        if data_request.check(&packet, conn, sql) {
            // The HTTP interface inserts the empty request body.
            conn.write_data(None, compression)
                .map_err(Failure::broken)?;
            conn.flush().await.map_err(Failure::broken)?;
            continue;
        }

        let block = match packet {
            ServerPacket::Data(block) => block,
            ServerPacket::Progress(increment) => {
                progress.add(&increment);
                continue;
            }
            ServerPacket::ProfileInfo(info) => {
                profile = Some(info);
                continue;
            }
            ServerPacket::EndOfStream => {
                return Ok(Outcome::Completed {
                    summary: progress.to_summary(profile.as_ref()),
                });
            }
            ServerPacket::Exception(error) => {
                return Err(Failure {
                    error,
                    reusable: true,
                });
            }
            ServerPacket::Pong | ServerPacket::Other | ServerPacket::TableColumns => continue,
        };

        let mut data = BytesMut::new();

        if !received_header {
            received_header = true;

            output
                .write_header(&block, &mut data)
                .map_err(Failure::broken)?;
            sink.start(progress.to_summary(profile.as_ref()));
        }

        output
            .write_rows(&block, conn.wire_format(), &mut data)
            .map_err(Failure::broken)?;

        let total_received = conn.received_bytes();
        let chunk = Chunk {
            data: data.freeze(),
            net_size: usize::try_from(total_received.saturating_sub(received_bytes))
                .unwrap_or(usize::MAX),
        };
        received_bytes = total_received;

        if !sink.send(chunk).await {
            return Ok(Outcome::Canceled { compression });
        }
    }
}

/// Cancel the query and read the rest of the response.
///
/// Returns `true` if the connection can be used again.
async fn cancel(pooled: &mut PooledConnection, compression: Compression) -> bool {
    let Ok(conn) = pooled.conn() else {
        return false;
    };

    conn.write_cancel();

    let finished = async {
        conn.flush().await.ok()?;

        loop {
            match conn.read_packet(compression.is_enabled()).await.ok()? {
                ServerPacket::EndOfStream | ServerPacket::Exception(_) => return Some(()),
                _ => {}
            }
        }
    };

    matches!(
        tokio::time::timeout(CANCEL_TIMEOUT, finished).await,
        Ok(Some(()))
    )
}
