use crate::error::Error;
use crate::insert_formatted::InsertFormatted;
use crate::native::{Block, Column, Layout, LayoutKind, varuint};
use bytes::{Bytes, BytesMut};

pub struct BlockWriter {
    insert: InsertFormatted,
    buf: BytesMut,
}

impl BlockWriter {
    pub fn new(insert: InsertFormatted) -> Self {
        Self {
            insert,
            buf: BytesMut::with_capacity(8192),
        }
    }

    pub async fn write(&mut self, block: &Block) -> Result<(), Error> {
        // If canceled while writing a block, we have no way to recover.
        // We have to abort the request instead.
        let mut guard = WriteGuard {
            insert: &mut self.insert,
            buf: &mut self.buf,
            finished: false,
        };

        varuint::write(&mut guard.buf, block.columns.len());
        varuint::write(&mut guard.buf, block.num_rows);

        for column in &block.columns {
            guard.write_column(column).await?;
        }

        // We deliberately don't send the block header right away
        // since the first column header can share the same buffer.
        //
        // If `write_column()` wrote any data, this should be a no-op.
        guard.flush().await?;

        guard.finished = true;

        Ok(())
    }

    pub async fn end(mut self) -> Result<(), Error> {
        let mut guard = WriteGuard {
            insert: &mut self.insert,
            buf: &mut self.buf,
            finished: false,
        };

        guard.flush().await?;

        guard.finished = true;

        drop(guard);

        self.insert.end().await
    }
}

struct WriteGuard<'a> {
    insert: &'a mut InsertFormatted,
    buf: &'a mut BytesMut,
    finished: bool,
}

impl WriteGuard<'_> {
    async fn write_column(&mut self, column: &Column) -> Result<(), Error> {
        varuint::write(&mut self.buf, column.name.len());
        self.buf.extend_from_slice(column.name.as_bytes());

        // We have to format the data type to know exactly how long the string is,
        // though in most cases we can use a static string.
        let data_type_str = column.data_type.to_str();

        varuint::write(&mut self.buf, data_type_str.len());
        self.buf.extend_from_slice(data_type_str.as_bytes());

        self.write_layout(&column.layout).await
    }

    async fn write_layout(&mut self, layout: &Layout) -> Result<(), Error> {
        if let Some(nulls) = &layout.nulls {
            self.send(nulls.clone()).await?;
        }

        match &layout.kind {
            LayoutKind::Fixed { data, .. } => {
                self.send(data.clone()).await?;
            }
            LayoutKind::Variable { end_offsets, data } => {
                for offset in end_offsets {
                    varuint::write(&mut self.buf, *offset);
                }

                self.send(data.clone()).await?;
            }
            LayoutKind::LowCardinality(_) => {
                return Err(Error::Other(
                    "inserting LowCardinality data not yet implemented".into(),
                ));
            }
            LayoutKind::Array {
                end_indices,
                elem_layout,
            } => {
                for index in end_indices {
                    varuint::write(&mut self.buf, *index);
                }

                Box::pin(self.write_layout(elem_layout)).await?;
            }
            LayoutKind::Tuple { layouts } => {
                for layout in layouts {
                    Box::pin(self.write_layout(layout)).await?;
                }
            }
            LayoutKind::Map {
                key_val_layouts,
                end_indices,
            } => {
                for index in end_indices {
                    varuint::write(&mut self.buf, *index);
                }

                Box::pin(self.write_layout(&key_val_layouts[0])).await?;
                Box::pin(self.write_layout(&key_val_layouts[1])).await?;
            }
        }

        Ok(())
    }

    async fn send(&mut self, data: Bytes) -> Result<(), Error> {
        self.flush().await?;

        self.insert.send(data).await
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if !self.buf.is_empty() {
            self.insert.send(self.buf.split().freeze()).await?;
        }

        Ok(())
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.insert.abort();
        }
    }
}
