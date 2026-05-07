use crate::Client;
use crate::cursors::RawCursor;
use crate::error::Error;
use crate::insert_formatted::{BufInsertFormatted, InsertFormatted};
use crate::query::Query;
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};
use tokio::io::AsyncWrite;

impl Client {
    /// Begin inserting Arrow [`RecordBatch`]es into the target table.
    ///
    /// The request isn't begun until the first batch is written.
    pub fn insert_arrow(&self, table: &str, schema: &Schema) -> Result<ArrowInsert, Error> {
        let mut escaped_table = String::new();
        crate::sql::escape::identifier(table, &mut escaped_table)
            .map_err(|e| Error::Other(e.into()))?;

        Ok(ArrowInsert {
            writer: StreamWriter::try_new(
                InsertWriter {
                    insert: InsertFormatted::new(
                        self,
                        format!("INSERT INTO {escaped_table} FORMAT ArrowStream"),
                        Some(table),
                    )
                    .buffered(),
                },
                schema,
            )
            .map_err(wrap_arrow_error)?,
        })
    }
}

impl Query {
    pub fn fetch_arrow(self) -> Result<ArrowCursor, Error> {
        let span = self.make_span(Some("ArrowStream"));

        let response = self.do_execute(Some("ArrowStream"))?;
        Ok(ArrowCursor {
            cursor: RawCursor::new(response),
            buffer: Buffer::default(),
            decoder: StreamDecoder::new(),
            span,
        })
    }
}

pub struct ArrowInsert {
    writer: StreamWriter<InsertWriter>,
}

impl ArrowInsert {
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), Error> {
        if self.writer.get_ref().should_flush() {
            self.writer.get_mut().insert.flush().await?;
        }

        self.writer.write(batch).map_err(wrap_arrow_error)
    }

    pub async fn end(self) -> Result<(), Error> {
        let mut writer = self.writer.into_inner().map_err(wrap_arrow_error)?;

        writer.insert.end().await
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

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct ArrowCursor {
    cursor: RawCursor,
    buffer: Buffer,
    decoder: StreamDecoder,
    span: tracing::Span,
}

impl ArrowCursor {
    #[inline(always)]
    pub fn schema(&self) -> Option<SchemaRef> {
        self.decoder.schema()
    }

    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RecordBatch>, Error>> {
        let _span = self.span.enter();

        loop {
            if self.buffer.is_empty() {
                if let Some(bytes) = ready!(self.cursor.poll_next(cx))? {
                    self.buffer = bytes.into();
                    continue;
                }

                self.decoder.finish().map_err(wrap_arrow_error)?;

                return Poll::Ready(Ok(None));
            }

            // Note: some bytes may be left in `buffer` which is why we need to store it
            if let Some(batch) = self
                .decoder
                .decode(&mut self.buffer)
                .map_err(wrap_arrow_error)?
            {
                return Poll::Ready(Ok(Some(batch)));
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<RecordBatch>, Error> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }
}

fn wrap_arrow_error(e: ArrowError) -> Error {
    Error::Other(e.into())
}
