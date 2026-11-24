use crate::Client;
use crate::error::Error;
use crate::insert_formatted::InsertFormatted;
use crate::native::Block;
use crate::native::block_encoder;
use crate::native::reader::WireFormat;
use bytes::BytesMut;
use std::num::Saturating;

pub(crate) struct BlockWriter {
    insert: InsertFormatted,
    buf: BytesMut,
    sent_rows: Saturating<usize>,
}

impl BlockWriter {
    pub(crate) fn new(insert: InsertFormatted) -> Self {
        Self {
            insert,
            buf: BytesMut::with_capacity(8192),
            sent_rows: Saturating(0),
        }
    }

    pub(crate) fn insert_mut(&mut self) -> &mut InsertFormatted {
        &mut self.insert
    }

    #[cfg_attr(not(test), expect(unused))]
    pub(crate) fn expect_client(&self) -> &Client {
        self.insert.expect_client()
    }

    pub(crate) fn expect_client_mut(&mut self) -> &mut Client {
        self.insert.expect_client_mut()
    }

    pub(crate) async fn write(&mut self, block: &Block) -> Result<(), Error> {
        if block.num_rows() == 0 {
            return Err(Error::Other("attempting to write an empty block".into()));
        }

        block_encoder::write_block(block, WireFormat::NATIVE, &mut self.buf)?;

        // If canceled while sending a block, we have no way to recover.
        // We have to abort the request instead.
        let mut guard = AbortOnDrop {
            insert: &mut self.insert,
            finished: false,
        };

        guard.insert.send(self.buf.split().freeze()).await?;
        guard.finished = true;

        self.sent_rows += block.num_rows();

        Ok(())
    }

    pub(crate) async fn end(self) -> Result<(), Error> {
        tracing::record_all!(
            self.insert.span(),
            clickhouse.request.sent_rows = self.sent_rows.0,
        );

        self.insert.end().await
    }
}

struct AbortOnDrop<'a> {
    insert: &'a mut InsertFormatted,
    finished: bool,
}

impl Drop for AbortOnDrop<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.insert.abort();
        }
    }
}
