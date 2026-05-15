#![doc=include_str!("../README.md")]
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Schema, SchemaRef};
use clickhouse::error::Error;
use clickhouse::insert_formatted::BufInsertFormatted;
use clickhouse::query::{BytesCursor, Query};
use clickhouse::{Client, Compression};
use std::io::Write;
use std::mem;
use std::num::Saturating;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};
use tokio::io::AsyncWrite;

/// Extension methods for [`clickhouse::Client`] for use with Arrow.
///
/// # Note: LZ4 Compression
/// LZ4 compression will be automatically downgraded to no compression if the `lz4` feature of this
/// crate is not enabled. See crate root docs for details.
pub trait ArrowClientExt {
    /// Begin inserting Arrow [`RecordBatch`]es into the target table.
    ///
    /// The request isn't begun until the first batch is written.
    fn insert_arrow(&self, table: &str) -> Result<ArrowInsert, Error>;
}

impl ArrowClientExt for Client {
    fn insert_arrow(&self, table: &str) -> Result<ArrowInsert, Error> {
        let mut escaped_table = String::new();
        clickhouse::_priv::sql_escape_identifier(table, &mut escaped_table)
            .map_err(|e| Error::Other(e.into()))?;

        let mut insert =
            self.insert_formatted_with(format!("INSERT INTO {escaped_table} FORMAT ArrowStream"));

        let compression = insert.get_compression();
        if compression.is_lz4() && !cfg!(feature = "lz4") {
            tracing::debug!(
                "`ArrowClientExt::insert_arrow()`: downgrading compression to `None`, was {compression:?}; `clickhouse-ext-arrow/lz4` feature must be enabled"
            );
            insert = insert.with_compression(Compression::None);
        }

        let insert = insert.buffered();

        tracing::record_all!(insert._priv_span(), db.collection.name = table);

        Ok(ArrowInsert {
            state: InsertState::NotStarted(insert),
            sent_rows: Saturating(0),
        })
    }
}

/// Extension methods for [`clickhouse::query::Query`] for use with Arrow.
///
/// # Note: LZ4 Compression
/// LZ4 compression will be automatically downgraded to no compression if the `lz4` feature of this
/// crate is not enabled. See crate root docs for details.
pub trait ArrowQueryExt {
    /// Executes the query, returning as Arrow [`RecordBatch`]es.
    ///
    /// The resultset is streamed in the [`ArrowStream` format](https://clickhouse.com/docs/interfaces/formats/ArrowStream).
    fn fetch_arrow(self) -> Result<ArrowCursor, Error>;
}

impl ArrowQueryExt for Query {
    fn fetch_arrow(mut self) -> Result<ArrowCursor, Error> {
        let compression = self.get_compression();
        if compression.is_lz4() && !cfg!(feature = "lz4") {
            tracing::debug!(
                "`ArrowQueryExt::fetch_arrow()`: downgrading compression to `None`, was {compression:?}; `clickhouse-ext-arrow/lz4` feature must be enabled"
            );
            self = self.with_compression(Compression::None);
        }

        Ok(ArrowCursor {
            cursor: self.fetch_bytes("ArrowStream")?,
            buffer: Buffer::default(),
            decoder: StreamDecoder::new(),
        })
    }
}

/// Performs an `INSERT` query accepting Arrow [`RecordBatch`]es.
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
    /// Write the [Arrow Schema message] to the buffer, beginning the `INSERT` request.
    ///
    /// This can be used to begin the request eagerly if a [`Schema`] is available before
    /// the first [`RecordBatch`].
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
    /// The batch is encoded to an internal buffer, which is flushed when it becomes full.
    /// Because encoding the batch is synchronous, the buffer may need to grow to accommodate the
    /// whole batch if the connection is not ready to accept it.
    ///
    /// The buffer does not need to be manually flushed.
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
    /// It is not necessary to call this method.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if !self.state.is_started() {
            return Ok(());
        }

        self.state.writer().get_mut().insert.flush().await
    }

    /// Flush the remaining data and finish the `INSERT` request.
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

        // `.poll_write()` checks the buffer size and queues a flush if necessary and possible
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
#[must_use = "the query is not sent until the cursor is polled"]
pub struct ArrowCursor {
    cursor: BytesCursor,
    buffer: Buffer,
    decoder: StreamDecoder,
}

impl ArrowCursor {
    /// Return the response schema if it is available.
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
    pub async fn collect_merged(&mut self) -> Result<RecordBatch, Error> {
        let batches = self.collect().await?;

        let Some(schema) = batches.first().map(|batch| batch.schema()) else {
            return Ok(RecordBatch::new_empty(Schema::empty().into()));
        };

        arrow_select::concat::concat_batches(&schema, &batches).map_err(wrap_arrow_err)
    }
}

#[cold]
fn wrap_arrow_err(e: arrow_schema::ArrowError) -> Error {
    Error::Other(e.into())
}
