#![doc=include_str!("../README.md")]
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Schema, SchemaRef};
use clickhouse::Client;
use clickhouse::error::Error;
use clickhouse::insert_formatted::BufInsertFormatted;
use clickhouse::query::{BytesCursor, Query};
use std::io::Write;
use std::mem;
use std::num::Saturating;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};
use tokio::io::AsyncWrite;

/// Extension methods for [`clickhouse::Client`] for use with Arrow.
pub trait ArrowClientExt {
    /// Begin inserting Arrow [`RecordBatch`]es into the target table.
    ///
    /// The request isn't begun until the first batch is written.
    ///
    /// # Errors
    /// Any [`ArrowError`][arrow_schema::ArrowError]s are wrapped as [`Error::Other`].
    fn insert_arrow(&self, table: &str) -> Result<ArrowInsert, Error>;
}

impl ArrowClientExt for Client {
    fn insert_arrow(&self, table: &str) -> Result<ArrowInsert, Error> {
        let mut escaped_table = String::new();
        clickhouse::_priv::sql_escape_identifier(table, &mut escaped_table)
            .map_err(|e| Error::Other(e.into()))?;

        let insert = self
            .insert_formatted_with(format!("INSERT INTO {escaped_table} FORMAT ArrowStream"))
            // Prevent ClickHouse from double-compressing
            .with_setting("output_format_arrow_compression_method", "none")
            // Add specific product info to let us track Arrow adoption
            .with_product_info(
                "clickhouse-ext-arrow",
                // Don't error if not building under Cargo
                option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
            )
            .buffered();

        tracing::record_all!(insert._priv_span(), db.collection.name = table);

        Ok(ArrowInsert {
            state: InsertState::NotStarted(insert),
            sent_rows: Saturating(0),
        })
    }
}

/// Extension methods for [`clickhouse::query::Query`] for use with Arrow.
pub trait ArrowQueryExt {
    /// Executes the query, returning as Arrow [`RecordBatch`]es.
    ///
    /// The resultset is streamed in the [`ArrowStream` format](https://clickhouse.com/docs/interfaces/formats/ArrowStream).
    ///
    /// # Errors
    /// Any [`ArrowError`][arrow_schema::ArrowError]s are wrapped as [`Error::Other`].
    fn fetch_arrow(self) -> Result<ArrowCursor, Error>;
}

impl ArrowQueryExt for Query {
    fn fetch_arrow(self) -> Result<ArrowCursor, Error> {
        Ok(ArrowCursor {
            cursor: self
                // Prevent ClickHouse from double-compressing
                .with_setting("output_format_arrow_compression_method", "none")
                // Add specific product info to let us track Arrow adoption
                .with_product_info(
                    "clickhouse-ext-arrow",
                    // Don't error if not building under Cargo
                    option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
                )
                .fetch_bytes("ArrowStream")?,
            buffer: Buffer::default(),
            decoder: StreamDecoder::new(),
        })
    }
}

/// Performs an `INSERT` query accepting Arrow [`RecordBatch`]es.
///
/// # Errors
/// Any [`ArrowError`][arrow_schema::ArrowError]s are wrapped as [`Error::Other`].
pub struct ArrowInsert {
    state: InsertState,
    sent_rows: Saturating<u64>,
}

enum InsertState {
    NotStarted(BufInsertFormatted),
    Started(StreamWriter<InsertWriter>),
    Finished,
}

impl ArrowInsert {
    /// Write the [Arrow Schema message] to the buffer, eagerly beginning the `INSERT` request.
    ///
    /// This is not necessary to call, but can be used to begin the request immediately
    /// if a [`Schema`] is available before the first [`RecordBatch`] is.
    /// Otherwise, the [`RecordBatch::schema()`] of the first batch is sent as the Schema message.
    ///
    /// [Arrow Schema message]: https://arrow.apache.org/docs/format/Columnar.html#schema-message
    pub fn write_schema(&mut self, schema: &Schema) -> Result<(), Error> {
        if !self.state.is_started() {
            self.state.start(schema)?;
        }

        Ok(())
    }

    /// Write an Arrow [`RecordBatch`].
    ///
    /// The batch is encoded to an internal buffer, which is flushed if it is already full.
    ///
    /// Because encoding the batch is synchronous, the buffer may need to grow to accommodate the
    /// whole batch if the connection is not ready to accept it.
    ///
    /// The buffer does not need to be manually flushed. However, this method only checks if the
    /// buffer is full _before_ encoding the `RecordBatch` to avoid waiting after each one is
    /// written, which would otherwise make this method not cancel-safe.
    ///
    /// Thus, it may be desirable to manually flush the buffer after a large `RecordBatch` if
    /// another isn't going to be written immediately following it.
    ///
    /// # Cancel-Safe
    /// The only time this method may suspend execution is _before_ `batch` is written to the buffer,
    /// so it is safe to cancel. This returns immediately after `batch` is encoded.
    ///
    /// # Note: Schema Must Not Change
    /// It is a logic error to write a batch with one given schema, and then another batch with
    /// a schema of a different shape in the same `INSERT`. Server errors or data corruption
    /// may occur as a result.
    ///
    /// For performance reasons, schema equality is not checked between batches.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        if !self.state.is_started() {
            self.state.start(batch.schema_ref())?;
        }

        let writer = self.state.writer();

        if writer.get_ref().should_flush() {
            writer.get_mut().insert.flush().await?;
        }

        writer.write(batch).map_err(wrap_arrow_err)?;
        self.sent_rows += batch.num_rows() as u64;

        Ok(())
    }

    /// Flush the buffered data without ending the request.
    ///
    /// The buffer is flushed automatically by [`Self::write()`] if it is already full.
    ///
    /// Manual flushing can be used after writing a large [`RecordBatch`] to ensure it is fully
    /// sent to the server, if another batch isn't going to be written immediately after it.
    ///
    /// # Cancel-Safe
    /// Flushing the buffer is an all-or-nothing operation.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if !self.state.is_started() {
            return Ok(());
        }

        self.state.writer().get_mut().insert.flush().await
    }

    /// Flush the remaining data and finish the `INSERT` request.
    ///
    /// # Not Cancel-Safe
    /// If this method is canceled while data is being flushed, any data left in the buffer is lost.
    ///
    /// However, because ClickHouse immediately inserts data as it is received,
    /// explicitly flushing the buffer first (with [`Self::flush()`]) _should_ avoid the risk of
    /// data loss in cancelling this call.
    ///
    /// Then, only the response status and headers of the request would be lost.
    pub async fn end(self) -> Result<(), Error> {
        let mut insert = match self.state {
            InsertState::NotStarted(insert) => insert,
            InsertState::Started(writer) => writer.into_inner().map_err(wrap_arrow_err)?.insert,
            InsertState::Finished => return Ok(()),
        };

        tracing::record_all!(
            insert._priv_span(),
            clickhouse.request.sent_rows = self.sent_rows.0,
        );

        insert.end().await
    }
}

impl InsertState {
    #[inline(always)]
    fn is_started(&self) -> bool {
        matches!(self, Self::Started(_))
    }

    fn start(&mut self, schema: &Schema) -> Result<(), Error> {
        match mem::replace(self, Self::Finished) {
            Self::NotStarted(insert) => {
                *self = Self::Started(
                    StreamWriter::try_new(InsertWriter { insert }, schema)
                        .map_err(wrap_arrow_err)?,
                );
                Ok(())
            }
            Self::Started(writer) => {
                *self = Self::Started(writer);
                Ok(())
            }
            Self::Finished => Err(Error::Other(
                "`ArrowInsert` previously returned an error".into(),
            )),
        }
    }

    #[inline]
    fn writer(&mut self) -> &mut StreamWriter<InsertWriter> {
        match self {
            Self::Started(writer) => writer,
            _ => panic!("BUG: invalid state for `ArrowInsert`"),
        }
    }
}

struct InsertWriter {
    insert: BufInsertFormatted,
}

impl InsertWriter {
    #[inline(always)]
    fn should_flush(&self) -> bool {
        self.insert.buf_len() >= self.insert.nominal_capacity()
    }
}

impl Write for InsertWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut noop_context = Context::from_waker(Waker::noop());

        // `.poll_write()` checks the buffer capacity and queues a flush if necessary and possible
        // which hopefully avoids us having too much buffered in memory at once
        if let Poll::Ready(res) = Pin::new(&mut self.insert).poll_write(&mut noop_context, buf) {
            return res;
        }

        // Connection is not yet ready to accept the data, but we have to if we don't want to block.
        self.insert.write_buffered(buf);
        Ok(buf.len())
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A cursor that emits Arrow [`RecordBatch`]es.
///
/// # Errors
/// Any [`ArrowError`][arrow_schema::ArrowError]s are wrapped as [`Error::Other`].
#[must_use = "the query is not sent until `.next().await`"]
pub struct ArrowCursor {
    cursor: BytesCursor,
    buffer: Buffer,
    decoder: StreamDecoder,
}

impl ArrowCursor {
    /// Return the response schema if it is available.
    ///
    /// The schema will not be available until the first call to [`Self::next()`] returns.
    ///
    /// If the query does not return a result set (i.e. it is not a `SELECT` or similar query),
    /// this will still be `None` after `Self::next()` returns.
    #[inline(always)]
    pub fn schema(&self) -> Option<SchemaRef> {
        self.decoder.schema()
    }

    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RecordBatch>, Error>> {
        let _span = self.cursor._priv_span().clone().entered();

        loop {
            if self.buffer.is_empty() {
                if let Some(bytes) = ready!(self.cursor.poll_next(cx))? {
                    self.buffer = bytes.into();
                    continue;
                }

                self.decoder.finish().map_err(wrap_arrow_err)?;

                return Poll::Ready(Ok(None));
            }

            // Note: some bytes may be left in `buffer` which is why we need to store it
            //
            // By design, `StreamDecoder::decode()` fully consumes the buffer unless it has a
            // `RecordBatch` to return (it copies the remaining data to an internal buffer),
            // so we don't actually need to read more until this is empty:
            // https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamDecoder.html#method.decode
            if let Some(batch) = self
                .decoder
                .decode(&mut self.buffer)
                .map_err(wrap_arrow_err)?
            {
                return Poll::Ready(Ok(Some(batch)));
            }
        }
    }

    /// Read the next batch of records. Cancel-safe.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Collect the full response into a vector of [`RecordBatch`]es.
    ///
    /// See also [`Self::collect_merged()`].
    ///
    /// # Note: Not Cancel-Safe
    /// Cancelling the `Future` created by this method will discard any record batches that have
    /// already been read.
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>, Error> {
        let mut out = Vec::new();

        while let Some(batch) = self.next().await? {
            out.push(batch);
        }

        Ok(out)
    }

    /// Collect the full response and merge it into a single [`RecordBatch`].
    ///
    /// This generally requires copying the full dataset into a new allocation, so this is
    /// less efficient than [`Self::collect()`] but may be more convenient when a single batch
    /// is preferred/expected.
    ///
    /// # Note: Not Cancel-Safe
    /// Cancelling the `Future` created by this method will discard any record batches that have
    /// already been read.
    pub async fn collect_merged(&mut self) -> Result<RecordBatch, Error> {
        let batches = self.collect().await?;

        let Some(schema) = batches.first().map(|batch| batch.schema()) else {
            return Ok(RecordBatch::new_empty(
                // Return the correct schema if we got one, but don't error if we didn't.
                self.schema().unwrap_or_else(|| Schema::empty().into()),
            ));
        };

        arrow_select::concat::concat_batches(&schema, &batches).map_err(wrap_arrow_err)
    }
}

#[cold]
fn wrap_arrow_err(e: arrow_schema::ArrowError) -> Error {
    Error::Other(e.into())
}
