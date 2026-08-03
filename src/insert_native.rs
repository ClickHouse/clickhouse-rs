use crate::native::Block;
use crate::native::writer::BlockWriter;
use crate::{Client, Compression, insert_formatted, sql};

pub struct InsertNative {
    writer: BlockWriter,
}

impl InsertNative {
    pub(crate) fn new(client: &Client, table_name: &str, escape: bool) -> Self {
        let mut sql = "INSERT INTO ".to_string();

        if escape {
            sql::escape::identifier(table_name, &mut sql).expect("error escaping table name");
        } else {
            sql.push_str(table_name);
        }

        sql.push_str(" FORMAT Native");

        Self {
            writer: BlockWriter::new(insert_formatted::InsertFormatted::new(
                // FIXME: use HTTP body compression instead of block-level compression
                &client.clone().with_compression(Compression::None),
                sql,
                Some(table_name),
            )),
        }
    }

    /// Send a block of data.
    ///
    /// # NOT Cancel Safe
    /// If this `async` method is canceled (i.e. by dropping the resulting `Future`),
    /// the insert is automatically aborted.
    ///
    /// This is because the block data is not sent in a single write, since that would require
    /// copying it into a separate buffer. There is no way to resynchronize the stream
    /// once a block has been partially sent. Resuming a write would corrupt the stream.
    pub async fn write(&mut self, block: &Block) -> crate::Result<()> {
        self.writer.write(block).await
    }

    pub async fn end(self) -> crate::Result<()> {
        self.writer.end().await
    }
}
