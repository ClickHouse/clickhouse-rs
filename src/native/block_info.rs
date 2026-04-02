//! Block metadata for ClickHouse native protocol.
//!
//! Each data block carries overflow/bucket info used by the server for
//! aggregation and distributed query routing.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::native::io::{ClickHouseRead, ClickHouseWrite};

/// Metadata about a native protocol data block.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockInfo {
    pub(crate) is_overflows: bool,
    pub(crate) bucket_num: i32,
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo {
            is_overflows: false,
            bucket_num: -1,
        }
    }
}

impl BlockInfo {
    pub(crate) async fn read_async<R: ClickHouseRead>(reader: &mut R) -> Result<Self> {
        let mut info = Self::default();
        loop {
            let field_num = reader.read_var_uint().await?;
            match field_num {
                0 => break,
                1 => {
                    info.is_overflows = reader.read_u8().await? != 0;
                }
                2 => {
                    info.bucket_num = reader.read_i32_le().await?;
                }
                n => {
                    return Err(Error::BadResponse(format!(
                        "native protocol: unknown block info field: {n}"
                    )));
                }
            }
        }
        Ok(info)
    }

    pub(crate) async fn write_async<W: ClickHouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?;
        writer
            .write_u8(if self.is_overflows { 1 } else { 0 })
            .await?;
        writer.write_var_uint(2).await?;
        writer.write_i32_le(self.bucket_num).await?;
        writer.write_var_uint(0).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[tokio::test]
    async fn test_block_info_roundtrip() {
        let info = BlockInfo {
            is_overflows: true,
            bucket_num: 42,
        };

        let mut buf = Vec::new();
        info.write_async(&mut buf).await.unwrap();

        let mut reader = Cursor::new(buf);
        let decoded = BlockInfo::read_async(&mut reader).await.unwrap();

        assert!(decoded.is_overflows);
        assert_eq!(decoded.bucket_num, 42);
    }

    #[tokio::test]
    async fn test_block_info_default_roundtrip() {
        let info = BlockInfo::default();

        let mut buf = Vec::new();
        info.write_async(&mut buf).await.unwrap();

        let mut reader = Cursor::new(buf);
        let decoded = BlockInfo::read_async(&mut reader).await.unwrap();

        assert!(!decoded.is_overflows);
        assert_eq!(decoded.bucket_num, -1);
    }
}
