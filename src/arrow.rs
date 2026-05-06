use crate::Client;
use crate::error::Error;
use crate::insert_formatted::{BufInsertFormatted, InsertFormatted};
use arrow_array::RecordBatch;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcWriteOptions, StreamWriter};
use arrow_schema::{ArrowError, Schema};
use bytes::BytesMut;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::io::AsyncWrite;

impl Client {
    /// Begin inserting Arrow [`RecordBatch`]es into the target table.
    ///
    /// The request isn't begun until the first batch is written.
    pub fn insert_arrow(&self, table: &str, schema: &Schema) -> Result<ArrowInsert, Error> {
        let mut escaped_table = String::new();
        crate::sql::escape::identifier(table, &mut escaped_table)?;

        Ok(ArrowInsert {
            writer: StreamWriter::try_new(InsertWriter {
                insert: InsertFormatted::new(
                    self,
                    format!("INSERT INTO {escaped_table} FORMAT ArrowStream"),
                    Some(table),
                )
                .buffered(),
            }),
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

        self.writer.write(batch)
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

        // `.poll_write()` checks the buffer size and queues a flush if necessary/possible
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
