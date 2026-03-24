//! Connection management for ClickHouse native TCP protocol.
//!
//! Single-connection MVP — handles handshake, query execution, and packet
//! reading over a buffered TCP stream.

use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tokio::io::{AsyncRead, BufReader, BufWriter, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::native::protocol::{
    ChunkedProtocolMode, NativeCompressionMethod, ServerHello, DBMS_TCP_PROTOCOL_VERSION,
};
use crate::native::reader::{self, ServerPacket};
use crate::native::tcp::{self, CONN_READ_BUFFER, CONN_WRITE_BUFFER};
use crate::native::writer;

/// A single native TCP connection to ClickHouse.
pub(crate) struct NativeConnection {
    reader: BufReader<tokio::io::ReadHalf<TcpStream>>,
    writer: BufWriter<tokio::io::WriteHalf<TcpStream>>,
    server_hello: ServerHello,
    compression: NativeCompressionMethod,
    settings: Vec<(String, String)>,
    /// Set to `true` by [`crate::native::pool::PooledConnection::discard`] to
    /// prevent this connection being returned to the idle pool on drop.
    pub(crate) poisoned: bool,
}

impl NativeConnection {
    /// Connect and perform the handshake.
    pub(crate) async fn open(
        addr: &SocketAddr,
        database: &str,
        username: &str,
        password: &str,
        compression: NativeCompressionMethod,
        settings: Vec<(String, String)>,
    ) -> Result<Self> {
        let stream = tcp::connect(addr).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader = BufReader::with_capacity(CONN_READ_BUFFER, read_half);
        let mut writer = BufWriter::with_capacity(CONN_WRITE_BUFFER, write_half);

        // Send hello
        writer::send_hello(&mut writer, database, username, password).await?;

        // Read hello response
        let chunked_modes = (
            ChunkedProtocolMode::default(),
            ChunkedProtocolMode::default(),
        );
        let server_hello =
            reader::read_hello(&mut reader, DBMS_TCP_PROTOCOL_VERSION, chunked_modes).await?;

        // Send addendum
        writer::send_addendum(&mut writer, &server_hello).await?;

        Ok(Self {
            reader,
            writer,
            server_hello,
            compression,
            settings,
            poisoned: false,
        })
    }

    /// Returns `true` if this connection has been marked as broken and should
    /// not be returned to the idle pool.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Non-blocking liveness check for pool recycling.
    ///
    /// Returns `false` (connection should be discarded) if:
    /// - the connection is poisoned
    /// - the `BufReader` has unread bytes (leftover data from a previous query)
    /// - the TCP socket reports EOF (server closed the connection)
    /// - the TCP socket has unexpected data ready (protocol misalignment)
    ///
    /// Returns `true` only when the socket is clean and idle (no pending bytes).
    pub(crate) fn check_alive(&mut self) -> bool {
        if self.poisoned {
            return false;
        }
        // Leftover bytes in the read buffer mean a previous query didn't drain
        // completely — the connection is in an unknown state.
        if !self.reader.buffer().is_empty() {
            return false;
        }
        // Non-blocking poll: detect EOF or unexpected data without blocking.
        // A Pending result means the socket is idle → connection is alive.
        let mut buf = [0u8; 1];
        let mut read_buf = ReadBuf::new(&mut buf);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self.reader).poll_read(&mut cx, &mut read_buf) {
            Poll::Pending => true,   // idle — connection is healthy
            Poll::Ready(_) => false, // EOF or unexpected data — discard
        }
    }

    /// Get the server hello info.
    #[allow(unused)]
    pub(crate) fn server_hello(&self) -> &ServerHello {
        &self.server_hello
    }

    /// Negotiated server revision.
    pub(crate) fn server_revision(&self) -> u64 {
        self.server_hello.revision_version
    }

    /// Compression method in use.
    pub(crate) fn compression(&self) -> NativeCompressionMethod {
        self.compression
    }

    /// Mutable access to the write half for sending packets.
    pub(crate) fn writer_mut(&mut self) -> &mut BufWriter<tokio::io::WriteHalf<TcpStream>> {
        &mut self.writer
    }

    /// Mutable access to the read half for receiving packets.
    pub(crate) fn reader_mut(&mut self) -> &mut BufReader<tokio::io::ReadHalf<TcpStream>> {
        &mut self.reader
    }

    /// Execute a query and read all response packets until EndOfStream.
    pub(crate) async fn execute_query(&mut self, query: &str) -> Result<()> {
        self.execute_query_with("", query, &[]).await
    }

    /// Execute a query with an explicit query ID and per-query settings.
    ///
    /// `query_id` is sent verbatim in the query packet header; pass `""` to
    /// let the server generate its own ID.
    ///
    /// `extra_settings` are appended after the connection-level settings.
    /// Callers that want per-query settings to *override* client settings
    /// should perform the merge themselves before calling this method.
    pub(crate) async fn execute_query_with(
        &mut self,
        query_id: &str,
        query: &str,
        extra_settings: &[(String, String)],
    ) -> Result<()> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;

        // Merge connection-level settings with per-query overrides.
        let settings = merge_settings(&self.settings, extra_settings);

        writer::send_query(&mut self.writer, query_id, query, &settings, revision, compression).await?;
        writer::send_empty_block(&mut self.writer, compression).await?;

        loop {
            let packet =
                reader::read_packet(&mut self.reader, revision, compression).await?;
            match packet {
                ServerPacket::EndOfStream => break,
                ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Begin an INSERT operation.
    ///
    /// Sends `INSERT INTO table(cols) FORMAT Native` + an empty data block,
    /// then reads server packets until the schema Data block (0 rows) arrives.
    /// Returns the column headers `(name, type_name)` declared by the server.
    pub(crate) async fn begin_insert(
        &mut self,
        query: &str,
    ) -> Result<Vec<(String, String)>> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;

        writer::send_query(&mut self.writer, "", query, &self.settings, revision, compression).await?;
        writer::send_empty_block(&mut self.writer, compression).await?;

        loop {
            let packet =
                reader::read_packet(&mut self.reader, revision, compression).await?;
            match packet {
                reader::ServerPacket::Data(block) => {
                    return Ok(block
                        .column_headers
                        .into_iter()
                        .map(|h| (h.name, h.type_name))
                        .collect());
                }
                reader::ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => {} // skip Progress, ProfileInfo, etc.
            }
        }
    }

    /// Send one data block during an INSERT.
    ///
    /// `column_bytes` must be produced by [`crate::native::encode::encode_columns`].
    pub(crate) async fn send_insert_block(
        &mut self,
        column_bytes: &[u8],
        num_columns: usize,
        num_rows: usize,
    ) -> Result<()> {
        writer::send_data_block(
            &mut self.writer,
            num_columns,
            num_rows,
            column_bytes,
            self.compression,
        )
        .await
    }

    /// Finish an INSERT: send the empty terminator block and consume until
    /// `EndOfStream` (or surface any server exception).
    pub(crate) async fn finish_insert(&mut self) -> Result<()> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;

        writer::send_empty_block(&mut self.writer, compression).await?;

        loop {
            let packet =
                reader::read_packet(&mut self.reader, revision, compression).await?;
            match packet {
                reader::ServerPacket::EndOfStream => return Ok(()),
                reader::ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => {} // skip Progress, ProfileInfo, etc.
            }
        }
    }

    /// Send ping and wait for pong.
    pub(crate) async fn ping(&mut self) -> Result<()> {
        let revision = self.server_hello.revision_version;
        let compression = self.compression;

        writer::send_ping(&mut self.writer).await?;
        loop {
            let packet =
                reader::read_packet(&mut self.reader, revision, compression).await?;
            match packet {
                ServerPacket::Pong => return Ok(()),
                ServerPacket::Exception(err) => {
                    return Err(Error::BadResponse(err.to_string()));
                }
                _ => continue,
            }
        }
    }
}

/// Merge `base` settings with `extra`, where `extra` overrides duplicates.
///
/// Returns a `Vec` containing all entries from `base` (with any keys that also
/// appear in `extra` replaced by the `extra` value), followed by any `extra`
/// keys that were not present in `base`.
fn merge_settings(
    base: &[(String, String)],
    extra: &[(String, String)],
) -> Vec<(String, String)> {
    if extra.is_empty() {
        return base.to_vec();
    }
    let mut merged = base.to_vec();
    for (k, v) in extra {
        if let Some(slot) = merged.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            merged.push((k.clone(), v.clone()));
        }
    }
    merged
}

/// A no-op [`Waker`] used for non-blocking `poll_read` calls in `check_alive`.
///
/// The waker never schedules anything — it is used purely to drive a single
/// synchronous poll without registering for wake-up notifications.
fn noop_waker() -> Waker {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |p| RawWaker::new(p, &VTABLE), // clone
        |_| {},                        // wake
        |_| {},                        // wake_by_ref
        |_| {},                        // drop
    );
    // SAFETY: the vtable is a no-op; the data pointer is never dereferenced.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}
