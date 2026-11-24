//! A connection to a ClickHouse server over the native TCP protocol.

use crate::Compression;
use crate::error::{Error, Result};
use crate::native::Block;
use crate::native::block_encoder;
use crate::native::reader::{BlockReader, ChunkSource, WireFormat};
use crate::native::varuint::ParseVarUInt;
use crate::query_summary::QuerySummary;
use crate::tcp::protocol::{
    self, ClientInfo, REVISION_WITH_ADDENDUM, REVISION_WITH_CLIENT_INFO,
    REVISION_WITH_CLIENT_WRITE_INFO, REVISION_WITH_INTERSERVER_SECRET, REVISION_WITH_PARAMETERS,
    REVISION_WITH_QUOTA_KEY, REVISION_WITH_SERVER_DISPLAY_NAME,
    REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS, REVISION_WITH_SERVER_TIMEZONE,
    REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS, REVISION_WITH_VERSION_PATCH, client, server,
    write_str, write_varuint,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// A byte stream to the server: plain TCP or TLS.
pub(crate) trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

/// The maximum size of a string in a packet, the same as `DEFAULT_MAX_STRING_SIZE` of ClickHouse.
const MAX_STRING_SIZE: usize = 1 << 30;

/// The size of the uncompressed data in one compressed frame that the client sends.
const MAX_FRAME_DATA_SIZE: usize = 1 << 20;

const READ_CHUNK_SIZE: usize = 64 * 1024;

/// The marker that the server expects instead of a user name to use a JSON Web Token.
const JWT_AUTHENTICATION_MARKER: &str = " JWT AUTHENTICATION ";

/// Information that the server sends in its `Hello` packet.
#[derive(Debug, Clone)]
pub(crate) struct ServerInfo {
    pub(crate) name: String,
    pub(crate) version_major: u64,
    pub(crate) version_minor: u64,
    pub(crate) version_patch: u64,
    pub(crate) revision: u64,
    pub(crate) timezone: Option<String>,
    pub(crate) display_name: Option<String>,
}

/// Credentials and options sent in the `Hello` packet.
pub(crate) struct Hello<'a> {
    pub(crate) client_name: &'a str,
    pub(crate) database: &'a str,
    pub(crate) auth: HelloAuth<'a>,
    pub(crate) quota_key: &'a str,
}

pub(crate) enum HelloAuth<'a> {
    Password { user: &'a str, password: &'a str },
    Jwt(&'a str),
}

/// The fields of a `Query` packet.
pub(crate) struct QueryPacket<'a> {
    pub(crate) query_id: &'a str,
    pub(crate) client_info: ClientInfo<'a>,
    pub(crate) settings: &'a [(&'a str, &'a str)],
    pub(crate) query: &'a [u8],
    pub(crate) parameters: &'a [(&'a str, &'a str)],
    pub(crate) compression: Compression,
}

/// A packet that the server sent during a query.
pub(crate) enum ServerPacket {
    Data(Block),
    Progress(Progress),
    ProfileInfo(ProfileInfo),
    EndOfStream,
    Pong,
    /// The server failed to execute the query. The connection can be used again.
    Exception(Error),
    /// The description of the columns of the table of an `INSERT`.
    ///
    /// The server sends it before the structure of the table when it waits for the data.
    TableColumns,
    /// A packet that this client does not use: logs, profile events, table columns, etc.
    Other,
}

/// Progress of a query, sent as increments.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Progress {
    pub(crate) read_rows: u64,
    pub(crate) read_bytes: u64,
    pub(crate) total_rows_to_read: u64,
    pub(crate) written_rows: u64,
    pub(crate) written_bytes: u64,
    pub(crate) elapsed_ns: u64,
}

impl Progress {
    pub(crate) fn add(&mut self, other: &Progress) {
        self.read_rows = self.read_rows.saturating_add(other.read_rows);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.total_rows_to_read = self
            .total_rows_to_read
            .saturating_add(other.total_rows_to_read);
        self.written_rows = self.written_rows.saturating_add(other.written_rows);
        self.written_bytes = self.written_bytes.saturating_add(other.written_bytes);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
    }

    /// The equivalent of the `X-ClickHouse-Summary` header of the HTTP interface.
    pub(crate) fn to_summary(self, profile: Option<&ProfileInfo>) -> QuerySummary {
        let mut fields = HashMap::from([
            ("read_rows".to_owned(), self.read_rows.to_string()),
            ("read_bytes".to_owned(), self.read_bytes.to_string()),
            ("written_rows".to_owned(), self.written_rows.to_string()),
            ("written_bytes".to_owned(), self.written_bytes.to_string()),
            (
                "total_rows_to_read".to_owned(),
                self.total_rows_to_read.to_string(),
            ),
            ("elapsed_ns".to_owned(), self.elapsed_ns.to_string()),
        ]);

        if let Some(profile) = profile {
            fields.insert("result_rows".to_owned(), profile.rows.to_string());
            fields.insert("result_bytes".to_owned(), profile.bytes.to_string());
        }

        QuerySummary::from_fields(fields)
    }
}

/// Statistics of the query result.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ProfileInfo {
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

pub(crate) struct Connection {
    stream: Box<dyn Stream>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    /// The negotiated protocol revision.
    revision: u64,
    server: ServerInfo,
    /// The total number of bytes received.
    received_bytes: u64,
    /// The roles set with `SET ROLE` in the session of this connection.
    pub(crate) roles: Vec<String>,
    /// Whether the user can change `network_compression_method`, once it is known.
    pub(crate) can_change_compression: Option<bool>,
}

fn protocol_error(message: impl Into<String>) -> Error {
    Error::BadResponse(format!(
        "unexpected data from the server: {}",
        message.into()
    ))
}

fn network_error(error: std::io::Error) -> Error {
    Error::Network(Box::new(error))
}

impl Connection {
    /// Perform the handshake on `stream`.
    pub(crate) async fn handshake(stream: Box<dyn Stream>, hello: &Hello<'_>) -> Result<Self> {
        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(READ_CHUNK_SIZE),
            write_buf: BytesMut::new(),
            revision: protocol::CLIENT_REVISION,
            server: ServerInfo {
                name: String::new(),
                version_major: 0,
                version_minor: 0,
                version_patch: 0,
                revision: 0,
                timezone: None,
                display_name: None,
            },
            received_bytes: 0,
            roles: Vec::new(),
            can_change_compression: None,
        };

        let (user, password) = match hello.auth {
            HelloAuth::Password { user, password } => (user, password),
            HelloAuth::Jwt(token) => (JWT_AUTHENTICATION_MARKER, token),
        };

        let (major, minor, _) = protocol::crate_version();
        let out = &mut conn.write_buf;
        write_varuint(out, protocol::revision_varuint(client::HELLO));
        write_str(out, hello.client_name);
        write_varuint(out, major);
        write_varuint(out, minor);
        write_varuint(out, protocol::revision_varuint(protocol::CLIENT_REVISION));
        write_str(out, hello.database);
        write_str(out, user);
        write_str(out, password);
        conn.flush().await?;

        match conn.read_varuint().await? {
            server::HELLO => {}
            server::EXCEPTION => return Err(conn.read_exception().await?),
            other => {
                return Err(protocol_error(format!(
                    "expected Hello or Exception, got packet {other}"
                )));
            }
        }

        conn.server.name = conn.read_utf8().await?;
        conn.server.version_major = conn.read_varuint().await?;
        conn.server.version_minor = conn.read_varuint().await?;
        conn.server.revision = conn.read_varuint().await?;
        conn.revision = conn.server.revision.min(protocol::CLIENT_REVISION);

        if conn.revision < REVISION_WITH_CLIENT_INFO {
            return Err(Error::Unsupported(format!(
                "server protocol revision {} is too old, the minimum is {REVISION_WITH_CLIENT_INFO}",
                conn.server.revision
            )));
        }

        if conn.revision >= REVISION_WITH_SERVER_TIMEZONE {
            conn.server.timezone = Some(conn.read_utf8().await?);
        }

        if conn.revision >= REVISION_WITH_SERVER_DISPLAY_NAME {
            conn.server.display_name = Some(conn.read_utf8().await?);
        }

        conn.server.version_patch = if conn.revision >= REVISION_WITH_VERSION_PATCH {
            conn.read_varuint().await?
        } else {
            conn.server.revision
        };

        if conn.revision >= REVISION_WITH_ADDENDUM {
            let out = &mut conn.write_buf;

            if conn.revision >= REVISION_WITH_QUOTA_KEY {
                write_str(out, hello.quota_key);
            }

            conn.flush().await?;
        }

        Ok(conn)
    }

    pub(crate) fn server(&self) -> &ServerInfo {
        &self.server
    }

    /// The serialization of blocks for this server.
    pub(crate) fn wire_format(&self) -> WireFormat {
        WireFormat {
            revision: self.revision,
            variant_mode_prefix: (self.server.version_major, self.server.version_minor) >= (24, 7),
        }
    }

    /// The number of bytes received and consumed. Data that is read ahead is not counted.
    pub(crate) fn received_bytes(&self) -> u64 {
        self.received_bytes
            .saturating_sub(self.read_buf.len() as u64)
    }

    /// Check that the idle connection is still open.
    ///
    /// The server sends nothing to an idle connection, so any data or the end of the stream
    /// means that the connection cannot be used.
    pub(crate) fn is_reusable(&mut self) -> bool {
        if !self.read_buf.is_empty() || !self.write_buf.is_empty() {
            return false;
        }

        let mut buf = [0u8; 1];
        let mut buf = ReadBuf::new(&mut buf);
        let mut cx = Context::from_waker(std::task::Waker::noop());

        matches!(
            Pin::new(&mut self.stream).poll_read(&mut cx, &mut buf),
            Poll::Pending
        )
    }

    pub(crate) fn write_query(&mut self, query: &QueryPacket<'_>) -> Result<()> {
        if !query.parameters.is_empty() && self.revision < REVISION_WITH_PARAMETERS {
            return Err(Error::Unsupported(format!(
                "server protocol revision {} does not support query parameters, \
                 the minimum is {REVISION_WITH_PARAMETERS}",
                self.server.revision
            )));
        }

        if !query.settings.is_empty()
            && self.revision < REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        {
            return Err(Error::Unsupported(format!(
                "server protocol revision {} does not support settings, the minimum is \
                 {REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS}",
                self.server.revision
            )));
        }

        let revision = self.revision;
        let out = &mut self.write_buf;
        write_varuint(out, protocol::revision_varuint(client::QUERY));
        write_str(out, query.query_id);
        query.client_info.write(revision, out);
        protocol::write_settings(out, query.settings.iter().copied());

        if revision >= REVISION_WITH_INTERSERVER_SECRET {
            write_str(out, "");
        }

        write_varuint(out, protocol::revision_varuint(protocol::STAGE_COMPLETE));
        write_varuint(out, usize::from(query.compression.is_enabled()));
        write_str(out, query.query);

        if revision >= REVISION_WITH_PARAMETERS {
            protocol::write_settings(out, query.parameters.iter().copied());
        }

        Ok(())
    }

    /// Write a `Data` packet. `None` writes the empty block that ends the data.
    pub(crate) fn write_data(
        &mut self,
        block: Option<&Block>,
        compression: Compression,
    ) -> Result<()> {
        write_varuint(
            &mut self.write_buf,
            protocol::revision_varuint(client::DATA),
        );
        // The name of a temporary table; empty for the main data
        write_str(&mut self.write_buf, "");

        let mut data = BytesMut::new();
        match block {
            Some(block) => block_encoder::write_block(block, self.wire_format(), &mut data)?,
            None => block_encoder::write_empty_block(self.revision, &mut data),
        }

        if !compression.is_enabled() {
            self.write_buf.unsplit(data);
            return Ok(());
        }

        #[cfg(any(feature = "lz4", feature = "zstd"))]
        for chunk in data.chunks(MAX_FRAME_DATA_SIZE) {
            let frame = crate::insert_formatted::CompressedData::new(chunk, compression)?;
            self.write_buf.put_slice(&frame.compressed);
        }

        Ok(())
    }

    pub(crate) fn write_cancel(&mut self) {
        write_varuint(
            &mut self.write_buf,
            protocol::revision_varuint(client::CANCEL),
        );
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        if !self.write_buf.is_empty() {
            self.stream
                .write_all(&self.write_buf)
                .await
                .map_err(network_error)?;
            self.write_buf.clear();
        }

        self.stream.flush().await.map_err(network_error)
    }

    /// Read the next packet of a query.
    ///
    /// `compressed` must be the compression flag of the `Query` packet.
    ///
    /// **NOT** cancel-safe: the connection must not be used after the future is dropped.
    pub(crate) async fn read_packet(&mut self, compressed: bool) -> Result<ServerPacket> {
        let packet = self.read_varuint().await?;

        Ok(match packet {
            server::DATA => ServerPacket::Data(self.read_data_packet(compressed).await?),
            // The HTTP interface does not return totals and extremes
            // in `RowBinary` and `Native` formats either.
            server::TOTALS | server::EXTREMES => {
                self.read_data_packet(compressed).await?;
                ServerPacket::Other
            }
            server::EXCEPTION => ServerPacket::Exception(self.read_exception().await?),
            server::PROGRESS => ServerPacket::Progress(self.read_progress().await?),
            server::PONG => ServerPacket::Pong,
            server::END_OF_STREAM => ServerPacket::EndOfStream,
            server::PROFILE_INFO => ServerPacket::ProfileInfo(self.read_profile_info().await?),
            // Logs and profile events are never compressed at this revision.
            server::LOG | server::PROFILE_EVENTS => {
                self.read_data_packet(false).await?;
                ServerPacket::Other
            }
            server::TABLE_COLUMNS => {
                // Table name and the description of columns
                self.read_string().await?;
                self.read_string().await?;
                ServerPacket::TableColumns
            }
            server::PART_UUIDS => {
                let count = self.read_len().await?;
                for _ in 0..count {
                    self.read_bytes(16).await?;
                }
                ServerPacket::Other
            }
            server::TIMEZONE_UPDATE => {
                self.read_string().await?;
                ServerPacket::Other
            }
            other => return Err(protocol_error(format!("unknown packet {other}"))),
        })
    }

    async fn read_data_packet(&mut self, compressed: bool) -> Result<Block> {
        // The name of the temporary table
        self.read_string().await?;

        let format = self.wire_format();
        let mut reader = BlockReader::with_format(
            PacketSource {
                conn: self,
                compressed,
            },
            format,
        );
        // After an error, the connection is closed, so the rest of the packet does not matter.
        let block = reader.read_protocol_block().await;
        let (_, leftover) = reader.into_parts();
        let block = block?;

        if !leftover.is_empty() {
            if compressed {
                return Err(protocol_error("compressed block has trailing data"));
            }

            // Put back the data that follows the block.
            let mut read_buf = BytesMut::with_capacity(leftover.len() + self.read_buf.len());
            read_buf.put_slice(&leftover);
            read_buf.put_slice(&self.read_buf);
            self.read_buf = read_buf;
        }

        Ok(block)
    }

    async fn read_exception(&mut self) -> Result<Error> {
        let mut messages = Vec::new();

        loop {
            let code = i32::from_le_bytes(self.read_array().await?);
            let name = self.read_utf8_lossy().await?;
            let message = self.read_utf8_lossy().await?;
            let _stack_trace = self.read_string().await?;
            let [has_nested] = self.read_array().await?;

            messages.push(format_exception(code, &name, &message, &self.server));

            if has_nested == 0 {
                break;
            }
        }

        Ok(Error::BadResponse(messages.join(": ")))
    }

    async fn read_progress(&mut self) -> Result<Progress> {
        let mut progress = Progress {
            read_rows: self.read_varuint().await?,
            read_bytes: self.read_varuint().await?,
            total_rows_to_read: self.read_varuint().await?,
            ..Progress::default()
        };

        if self.revision >= REVISION_WITH_CLIENT_WRITE_INFO {
            progress.written_rows = self.read_varuint().await?;
            progress.written_bytes = self.read_varuint().await?;
        }

        if self.revision >= REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
            progress.elapsed_ns = self.read_varuint().await?;
        }

        Ok(progress)
    }

    async fn read_profile_info(&mut self) -> Result<ProfileInfo> {
        let rows = self.read_varuint().await?;
        let _blocks = self.read_varuint().await?;
        let bytes = self.read_varuint().await?;
        // applied_limit, rows_before_limit, calculated_rows_before_limit
        self.read_array::<1>().await?;
        self.read_varuint().await?;
        self.read_array::<1>().await?;

        Ok(ProfileInfo { rows, bytes })
    }

    /// Read more data from the stream into the buffer.
    async fn read_more(&mut self) -> Result<()> {
        self.read_buf.reserve(READ_CHUNK_SIZE);

        let read = self
            .stream
            .read_buf(&mut self.read_buf)
            .await
            .map_err(network_error)?;

        if read == 0 {
            return Err(Error::Network("connection closed by the server".into()));
        }

        self.received_bytes = self.received_bytes.saturating_add(read as u64);
        Ok(())
    }

    async fn ensure(&mut self, len: usize) -> Result<()> {
        while self.read_buf.len() < len {
            self.read_more().await?;
        }

        Ok(())
    }

    async fn read_varuint(&mut self) -> Result<u64> {
        loop {
            let mut input = &self.read_buf[..];

            let parsed = ParseVarUInt::default()
                .feed(&mut input)
                .map_err(|e| protocol_error(e.to_string()))?;

            if let ControlFlow::Break(value) = parsed {
                let consumed = self.read_buf.len() - input.len();
                self.read_buf.advance(consumed);
                return Ok(value);
            }

            self.read_more().await?;
        }
    }

    async fn read_len(&mut self) -> Result<usize> {
        let len = self.read_varuint().await?;

        usize::try_from(len)
            .ok()
            .filter(|&len| len <= MAX_STRING_SIZE)
            .ok_or_else(|| protocol_error(format!("length is too large: {len}")))
    }

    async fn read_bytes(&mut self, len: usize) -> Result<Bytes> {
        self.ensure(len).await?;
        Ok(self.read_buf.split_to(len).freeze())
    }

    async fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure(N).await?;
        let mut array = [0u8; N];
        self.read_buf.copy_to_slice(&mut array);
        Ok(array)
    }

    async fn read_string(&mut self) -> Result<Bytes> {
        let len = self.read_len().await?;
        self.read_bytes(len).await
    }

    async fn read_utf8(&mut self) -> Result<String> {
        let bytes = self.read_string().await?;
        String::from_utf8(bytes.into()).map_err(|e| protocol_error(e.to_string()))
    }

    async fn read_utf8_lossy(&mut self) -> Result<String> {
        Ok(String::from_utf8_lossy(&self.read_string().await?).into_owned())
    }

    /// Read one complete compressed frame and decompress it.
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    async fn read_frame(&mut self) -> Result<Bytes> {
        use crate::compression::native_framing::{META_SIZE, decompress_frame, frame_size};

        self.ensure(META_SIZE).await?;

        let meta = self
            .read_buf
            .get(..META_SIZE)
            .and_then(|meta| <&[u8; META_SIZE]>::try_from(meta).ok())
            .ok_or_else(|| protocol_error("compressed frame is too short"))?;
        let size = frame_size(meta)?;

        let frame = self.read_bytes(size).await?;
        decompress_frame(&frame)
    }

    #[cfg(not(any(feature = "lz4", feature = "zstd")))]
    async fn read_frame(&mut self) -> Result<Bytes> {
        Err(Error::Decompression(
            "the server sent compressed data, but no compression feature is enabled".into(),
        ))
    }
}

/// Format an exception like the HTTP interface:
/// `Code: 60. DB::Exception: <message>. (UNKNOWN_TABLE) (version 25.8.1)`.
fn format_exception(code: i32, name: &str, message: &str, server: &ServerInfo) -> String {
    use std::fmt::Write;

    let mut formatted = format!("Code: {code}. ");

    // The message usually starts with the name of the exception class.
    if !message.starts_with(name) {
        formatted.push_str(name);
        formatted.push_str(": ");
    }

    formatted.push_str(message.trim_end());

    if let Some(code_name) = crate::tcp::error_codes::name(code) {
        let suffix = format!("({code_name})");

        if !formatted.ends_with(&suffix) {
            if !formatted.ends_with('.') {
                formatted.push('.');
            }

            formatted.push(' ');
            formatted.push_str(&suffix);
        }
    }

    // Writing to a `String` cannot fail.
    let _ = write!(
        formatted,
        " (version {}.{}.{})",
        server.version_major, server.version_minor, server.version_patch
    );

    formatted
}

/// Provides the data of one packet to [`BlockReader`].
struct PacketSource<'a> {
    conn: &'a mut Connection,
    compressed: bool,
}

impl ChunkSource for PacketSource<'_> {
    async fn next_chunk(&mut self) -> Option<Result<Bytes>> {
        if self.compressed {
            return Some(self.conn.read_frame().await);
        }

        if self.conn.read_buf.is_empty()
            && let Err(e) = self.conn.read_more().await
        {
            return Some(Err(e));
        }

        Some(Ok(self.conn.read_buf.split().freeze()))
    }
}
