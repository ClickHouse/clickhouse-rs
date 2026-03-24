//! Row cursor for native protocol query results.
//!
//! Reads native data blocks, transposes columnar data to RowBinary format,
//! and deserializes rows using the existing `rowbinary::deserialize_row` machinery.

use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::error::{Error, Result};
use crate::native::client::NativeClient;
use crate::native::pool::PooledConnection;
use crate::native::reader::ServerPacket;
use crate::row::{RowOwned, RowRead};
use crate::rowbinary;

/// A cursor that emits owned deserialized rows from a native TCP query.
///
/// `T` must be [`RowOwned`] — i.e., the deserialized value must not borrow from
/// the network buffer.  This covers the vast majority of use cases.
pub struct NativeRowCursor<T: RowOwned + RowRead> {
    client: NativeClient,
    sql: String,
    /// Query ID to send in the query packet (`""` → server generates one).
    query_id: String,
    /// Merged settings (client-level + per-query overrides) for this cursor.
    settings: Vec<(String, String)>,
    /// Buffered row bytes from already-received blocks.
    row_buf: VecDeque<Vec<u8>>,
    state: CursorState,
    _marker: PhantomData<fn() -> T>,
}

enum CursorState {
    /// Initial state — connection not yet acquired from pool.
    NotStarted,
    /// Connection open, reading packets.
    Reading(Box<PooledConnection>),
    /// EndOfStream received — no more data.
    Done,
}

impl<T: RowOwned + RowRead> Drop for NativeRowCursor<T> {
    /// Discard the connection if the stream was never fully consumed.
    ///
    /// Dropping a cursor mid-stream (e.g. after `fetch_one`) without calling
    /// `drain()` first would return a connection with unread bytes to the pool.
    /// Marking it poisoned here ensures deadpool drops it instead of recycling.
    fn drop(&mut self) {
        if let CursorState::Reading(conn) =
            std::mem::replace(&mut self.state, CursorState::Done)
        {
            // We can't async-drain here, so discard the connection.
            let mut conn = conn;
            conn.discard();
        }
    }
}

impl<T: RowOwned + RowRead> NativeRowCursor<T> {
    pub(crate) fn new(
        client: NativeClient,
        sql: String,
        query_id: String,
        settings: Vec<(String, String)>,
    ) -> Self {
        Self {
            client,
            sql,
            query_id,
            settings,
            row_buf: VecDeque::new(),
            state: CursorState::NotStarted,
            _marker: PhantomData,
        }
    }

    /// Consume all remaining packets until `EndOfStream`, allowing the
    /// underlying connection to be returned to the pool in a clean state.
    ///
    /// Must be called after a partial read (e.g. after `fetch_one` got its row)
    /// to prevent the half-read connection from being recycled with unread data.
    pub(crate) async fn drain(&mut self) -> Result<()> {
        loop {
            match &self.state {
                CursorState::Done | CursorState::NotStarted => return Ok(()),
                CursorState::Reading(_) => {}
            }
            let CursorState::Reading(conn) = &mut self.state else {
                unreachable!()
            };
            let revision = conn.server_revision();
            let compression = conn.compression();
            let packet =
                crate::native::reader::read_packet(conn.reader_mut(), revision, compression)
                    .await;
            match packet {
                Ok(ServerPacket::EndOfStream) => {
                    self.state = CursorState::Done;
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    if let CursorState::Reading(mut conn) =
                        std::mem::replace(&mut self.state, CursorState::Done)
                    {
                        conn.discard();
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Return the next deserialized row, or `None` at end of stream.
    ///
    /// `T` must be [`RowOwned`], meaning the result does not borrow from
    /// the network buffer. This is required for correctness with async streaming.
    pub async fn next(&mut self) -> Result<Option<T>> {
        loop {
            // Return a buffered row if available.
            if let Some(row_bytes) = self.row_buf.pop_front() {
                let mut slice: &[u8] = &row_bytes;
                let value = rowbinary::deserialize_row::<T>(&mut slice, None)?;
                return Ok(Some(value));
            }

            match &mut self.state {
                CursorState::Done => return Ok(None),

                CursorState::NotStarted => {
                    let mut conn = self.client.acquire().await?;
                    let revision = conn.server_revision();
                    let compression = conn.compression();
                    crate::native::writer::send_query(
                        conn.writer_mut(),
                        &self.query_id,
                        &self.sql,
                        &self.settings,
                        revision,
                        compression,
                    )
                    .await?;
                    crate::native::writer::send_empty_block(conn.writer_mut(), compression).await?;
                    self.state = CursorState::Reading(Box::new(conn));
                }

                CursorState::Reading(conn) => {
                    let revision = conn.server_revision();
                    let compression = conn.compression();
                    let packet = crate::native::reader::read_packet(
                        conn.reader_mut(),
                        revision,
                        compression,
                    )
                    .await?;

                    match packet {
                        ServerPacket::EndOfStream => {
                            self.state = CursorState::Done;
                            // Connection is returned to pool automatically when
                            // the old CursorState::Reading is dropped here.
                        }
                        ServerPacket::Data(block) => {
                            if block.num_rows > 0 {
                                self.row_buf.extend(block.row_data);
                            }
                        }
                        ServerPacket::Exception(err) => {
                            // Discard the connection — the query didn't complete
                            // cleanly; subsequent reads on this conn would be misaligned.
                            if let CursorState::Reading(mut conn) =
                                std::mem::replace(&mut self.state, CursorState::Done)
                            {
                                conn.discard();
                            }
                            return Err(Error::BadResponse(err.to_string()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
