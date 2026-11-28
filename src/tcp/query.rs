//! Execution of queries that return data.

use crate::error::{Error, Result};
use crate::native::reader::WireFormat;
use crate::native::{Block, block_encoder, to_row_binary};
use crate::query_summary::QuerySummary;
use crate::response::{Chunk, Chunks, Response};
use crate::tcp::connection::{
    Connection, ProfileInfo, Progress, QueryPacket, ServerCompression, ServerPacket,
};
use crate::tcp::pool::{Pool, PooledConnection};
use crate::tcp::profile_events::ProfileEvents;
use crate::tcp::protocol::ClientInfo;
use crate::tcp::{Request, RowFormat, changes_session, leading_words, params, trailing_format};
use crate::{Client, Compression};
use bytes::BytesMut;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
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
        let name = trailing_format(sql).unwrap_or(format);

        match RowFormat::from_name(name) {
            Some(RowFormat::RowBinary { names, types }) => Ok(Self::RowBinary { names, types }),
            Some(RowFormat::Native) => Ok(Self::Native),
            None => Err(Error::Unsupported(format!(
                "format {name} is not supported by the native TCP transport; \
                 supported formats: Native, RowBinary, RowBinaryWithNames, \
                 RowBinaryWithNamesAndTypes"
            ))),
        }
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

    let (final_summary_tx, final_summary_rx) = oneshot::channel();
    let started = start(
        client,
        request,
        sql,
        BytesConverter {
            output,
            received_header: false,
            capacity: 0,
        },
        Some(final_summary_tx),
    );

    Ok(Response::Waiting(Box::pin(async move {
        let (receiver, summary) = started.await?;
        Ok((Chunks::from_receiver(receiver, final_summary_rx), summary))
    })))
}

/// The blocks of a query result, once the query has started.
pub(crate) type BlocksFuture =
    Pin<Box<dyn Future<Output = Result<mpsc::Receiver<Result<Block>>>> + Send>>;

/// Execute a query, returning the blocks of the result (`Query::fetch_native()`).
pub(crate) fn query_blocks(client: &Client, sql: String) -> Result<BlocksFuture> {
    let started = start(client, Request::new(client)?, sql, BlockConverter, None);
    Ok(Box::pin(async move { Ok(started.await?.0) }))
}

/// Run the query in a task when the returned future is polled; the future completes
/// when the result starts.
fn start<C: Converter>(
    client: &Client,
    request: Request,
    sql: String,
    converter: C,
    final_summary_tx: Option<oneshot::Sender<QuerySummary>>,
) -> impl Future<Output = Result<(mpsc::Receiver<Result<C::Item>>, HeaderSummary)>> + Send + use<C>
{
    let pool = client.tcp.clone();
    let (header_tx, header_rx) = oneshot::channel();
    let (item_tx, item_rx) = mpsc::channel(CHANNEL_CAPACITY);

    async move {
        let mut sink = Sink::new(header_tx, item_tx);
        sink.final_summary_tx = final_summary_tx;
        tokio::spawn(run(pool, request, sql, converter, sink));

        match header_rx.await {
            Ok(Ok(summary)) => Ok((item_rx, summary)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Network("the query task stopped unexpectedly".into())),
        }
    }
}

/// Converts the blocks of the result to the items that the cursor reads.
trait Converter: Send + 'static {
    type Item: Send + 'static;

    /// Convert a block; `None` if there is nothing to send.
    fn convert(
        &mut self,
        block: Block,
        conn: &Connection,
        net_size: usize,
    ) -> Result<Option<Self::Item>>;
}

/// Converts blocks to bytes in the format that the cursor reads.
struct BytesConverter {
    output: Output,
    received_header: bool,
    /// The size of the previous chunk, to allocate the next one.
    capacity: usize,
}

impl Converter for BytesConverter {
    type Item = Chunk;

    fn convert(
        &mut self,
        block: Block,
        conn: &Connection,
        net_size: usize,
    ) -> Result<Option<Chunk>> {
        let mut data = BytesMut::with_capacity(self.capacity);

        if !std::mem::replace(&mut self.received_header, true) {
            self.output.write_header(&block, &mut data)?;
        }

        self.output
            .write_rows(&block, conn.wire_format(), &mut data)?;
        self.capacity = data.len();

        Ok((!data.is_empty()).then(|| Chunk {
            data: data.freeze(),
            net_size,
        }))
    }
}

/// Passes blocks with rows to the cursor.
struct BlockConverter;

impl Converter for BlockConverter {
    type Item = Block;

    fn convert(&mut self, block: Block, _: &Connection, _: usize) -> Result<Option<Block>> {
        Ok((block.num_rows() > 0).then_some(block))
    }
}

type HeaderSummary = Option<Box<QuerySummary>>;

/// Delivers the response to the cursor.
struct Sink<T> {
    header_tx: Option<oneshot::Sender<Result<HeaderSummary>>>,
    /// Receives the summary at the end of a successful response.
    final_summary_tx: Option<oneshot::Sender<QuerySummary>>,
    chunk_tx: mpsc::Sender<Result<T>>,
    /// The response is collected before it is sent (`wait_end_of_query = 1`).
    buffer: Option<Vec<T>>,
}

impl<T> Sink<T> {
    fn new(
        header_tx: oneshot::Sender<Result<HeaderSummary>>,
        chunk_tx: mpsc::Sender<Result<T>>,
    ) -> Self {
        Self {
            header_tx: Some(header_tx),
            final_summary_tx: None,
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
    async fn send(&mut self, chunk: T) -> bool {
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
        if let Some(final_summary_tx) = self.final_summary_tx.take() {
            // Before the end of the data, so that the cursor has it at the end.
            let _ = final_summary_tx.send(summary.clone());
        }

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
    /// An error after which the connection is in an unknown state.
    pub(crate) fn broken(error: Error) -> Self {
        Self {
            error,
            reusable: false,
        }
    }

    /// An error after which the connection can be used again, e.g. an exception of the server.
    pub(crate) fn reusable(error: Error) -> Self {
        Self {
            error,
            reusable: true,
        }
    }
}

async fn run<C: Converter>(
    pool: Arc<Pool>,
    request: Request,
    sql: String,
    mut converter: C,
    mut sink: Sink<C::Item>,
) {
    if request.wait_end_of_query {
        sink.buffer = Some(Vec::new());
    }

    let mut pooled = match pool.acquire(&request.key, request.session.clone()).await {
        Ok(pooled) => pooled,
        Err(e) => return sink.fail(e).await,
    };

    let result = match pooled.conn() {
        Ok(conn) => execute(conn, &request, sql.as_bytes(), &mut converter, &mut sink).await,
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
    set_roles(conn, &request.roles).await?;

    let (compression, compression_settings) = negotiate_compression(conn, request).await?;

    let settings: Vec<(&str, &str)> = request
        .settings
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .chain(
            compression_settings
                .iter()
                .map(|(name, value)| (*name, value.as_str())),
        )
        .collect();

    let (sql, renamed) = parameters(conn, request, sql);
    let parameters: Vec<(&str, &str)> = renamed
        .iter()
        .map(|(name, value)| (name.as_ref(), *value))
        .collect();

    write_query(
        conn,
        &QueryPacket {
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
        },
    )
    .await?;

    Ok(compression)
}

/// Write a `Query` packet and the empty block of external tables, and flush.
async fn write_query(conn: &mut Connection, packet: &QueryPacket<'_>) -> Result<(), Failure> {
    // Nothing was sent yet if the packet is invalid.
    conn.write_query(packet).map_err(Failure::reusable)?;
    send_end_of_data(conn, packet.compression).await
}

/// Send the empty block that ends the data.
pub(crate) async fn send_end_of_data(
    conn: &mut Connection,
    compression: Compression,
) -> Result<(), Failure> {
    conn.write_data(None, compression)
        .map_err(Failure::broken)?;
    conn.flush().await.map_err(Failure::broken)
}

/// A query without settings, parameters and compression for the internal queries of the client.
fn internal_query(sql: &str) -> QueryPacket<'_> {
    QueryPacket {
        query_id: "",
        client_info: ClientInfo {
            client_name: "",
            quota_key: "",
            #[cfg(feature = "opentelemetry")]
            trace_context: None,
        },
        settings: &[],
        query: sql.as_bytes(),
        parameters: &[],
        compression: Compression::None,
    }
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
    if request.parameters.is_empty() || conn.server().version_at_least(26, 7) {
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

/// The method of `compression`: the name and the ZSTD level, or `None` if compression is disabled.
fn method(compression: Compression) -> Option<(&'static str, Option<i64>)> {
    match compression {
        Compression::None => None,
        #[cfg(feature = "lz4")]
        #[allow(deprecated)]
        Compression::Lz4 | Compression::Lz4Hc(_) => Some(("LZ4", None)),
        #[cfg(feature = "zstd")]
        Compression::Zstd(level) => Some(("ZSTD", Some(i64::from(level)))),
    }
}

/// Select the compression of a query and the settings that request it.
///
/// The server compresses data with its `network_compression_method`. If the method of the client
/// is different, it is requested. A user with `readonly = 1` cannot change settings: then the method
/// of the server is used if this build can decompress it, else the query is not compressed.
async fn negotiate_compression(
    conn: &mut Connection,
    request: &Request,
) -> Result<(Compression, Vec<(&'static str, String)>), Failure> {
    let Some((method, level)) = method(request.compression) else {
        return Ok((Compression::None, Vec::new()));
    };

    if request.setting("network_compression_method").is_some() {
        // The user selected the method.
        return Ok((request.compression, Vec::new()));
    }

    let server = server_compression(conn).await?;

    let same_method = server.method.eq_ignore_ascii_case(method)
        && (level.is_none() || level == Some(server.zstd_level));

    if same_method {
        return Ok((request.compression, Vec::new()));
    }

    if !server.readonly {
        let mut settings = vec![("network_compression_method", method.to_owned())];

        if let Some(level) = level {
            settings.push(("network_zstd_compression_level", level.to_string()));
        }

        return Ok((request.compression, settings));
    }

    if server.can_decompress() {
        Ok((request.compression, Vec::new()))
    } else {
        Ok((Compression::None, Vec::new()))
    }
}

/// Get the compression settings of the server for the user of the connection.
///
/// The pool keeps the result for the server and the user.
async fn server_compression(conn: &mut Connection) -> Result<ServerCompression, Failure> {
    if let Some(known) = &conn.server_compression {
        return Ok(known.clone());
    }

    write_query(
        conn,
        &internal_query(
            "SELECT toUInt64(getSetting('readonly')), \
             toString(getSetting('network_compression_method')), \
             toInt64(getSetting('network_zstd_compression_level'))",
        ),
    )
    .await?;

    let mut result = None;

    loop {
        match conn.read_packet(false).await.map_err(Failure::broken)? {
            ServerPacket::Data(block) if block.num_rows() > 0 => {
                result = ServerCompression::from_block(&block);
            }
            ServerPacket::EndOfStream => break,
            ServerPacket::Exception(error) => return Err(Failure::reusable(error)),
            _ => {}
        }
    }

    let known = result.ok_or_else(|| {
        Failure::broken(Error::BadResponse(
            "the server did not return its compression settings".into(),
        ))
    })?;

    conn.server_compression = Some(known.clone());
    Ok(known)
}

/// Make the roles of the session of `conn` match `roles`.
async fn set_roles(conn: &mut Connection, roles: &[String]) -> Result<(), Failure> {
    if conn.roles == roles {
        return Ok(());
    }

    let mut sql = String::from("SET ROLE ");

    if roles.is_empty() {
        sql.push_str("DEFAULT");
    } else {
        for (index, role) in roles.iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }

            crate::sql::escape::identifier(role, &mut sql)
                .map_err(|e| Failure::broken(Error::Other(e.to_string().into())))?;
        }
    }

    write_query(conn, &internal_query(&sql)).await?;
    drain(conn, Compression::None, None).await?;

    conn.roles = roles.to_vec();
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
                let is_insert = leading_words(sql, 1)
                    .first()
                    .is_some_and(|word| word.eq_ignore_ascii_case(b"INSERT"));

                self.answered = true;
                self.table_columns || (is_insert && conn.server().version_at_least(22, 8))
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
            send_end_of_data(conn, compression).await?;
            continue;
        }

        match packet {
            ServerPacket::EndOfStream => return Ok(progress),
            ServerPacket::Exception(error) => {
                return Err(Failure::reusable(error));
            }
            ServerPacket::Progress(increment) => progress.add(&increment),
            _ => {}
        }
    }
}

async fn execute<C: Converter>(
    conn: &mut Connection,
    request: &Request,
    sql: &[u8],
    converter: &mut C,
    sink: &mut Sink<C::Item>,
) -> Result<Outcome, Failure> {
    let compression = send_query(conn, request, sql).await?;

    let compressed = compression.is_enabled();
    let mut data_request = DataRequest::default();

    let mut progress = Progress::default();
    let mut profile: Option<ProfileInfo> = None;
    let mut events = ProfileEvents::default();
    let mut received_header = false;
    let mut received_bytes = conn.received_bytes();

    loop {
        let packet = conn
            .read_packet(compressed)
            .await
            .map_err(Failure::broken)?;

        if data_request.check(&packet, conn, sql) {
            // The HTTP interface inserts the empty request body.
            send_end_of_data(conn, compression).await?;
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
            ServerPacket::ProfileEvents(block) => {
                events.add(&block);
                continue;
            }
            ServerPacket::EndOfStream => {
                return Ok(Outcome::Completed {
                    summary: progress.to_summary(profile.as_ref(), &events),
                });
            }
            ServerPacket::Exception(error) => {
                return Err(Failure::reusable(error));
            }
            ServerPacket::Pong | ServerPacket::Other | ServerPacket::TableColumns => continue,
        };

        if !received_header {
            received_header = true;
            sink.start(progress.to_summary(profile.as_ref(), &events));
        }

        let total_received = conn.received_bytes();
        let net_size =
            usize::try_from(total_received.saturating_sub(received_bytes)).unwrap_or(usize::MAX);
        received_bytes = total_received;

        let Some(item) = converter
            .convert(block, conn, net_size)
            .map_err(Failure::broken)?
        else {
            continue;
        };

        if !sink.send(item).await {
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
