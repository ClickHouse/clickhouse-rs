//! Compression/decompression for ClickHouse native protocol.
//!
//! LZ4 and ZSTD support with ClickHouse's custom frame format:
//! - 16 bytes: CityHash128 checksum
//! - 1 byte: compression method (0x82=LZ4, 0x90=ZSTD)
//! - 4 bytes: compressed size (incl. 9-byte header)
//! - 4 bytes: decompressed size
//! - N bytes: payload
//!
//! Checksum covers method+sizes+payload.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::error::{Error, Result};
use crate::native::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::protocol::NativeCompressionMethod;

/// Compress and write data in ClickHouse native chunk format.
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn compress_data<W: ClickHouseWrite>(
    writer: &mut W,
    raw: &[u8],
    compression: NativeCompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let compressed_payload = match compression {
        NativeCompressionMethod::Zstd => zstd::bulk::compress(raw, 1)
            .map_err(|e| Error::Compression(Box::new(e)))?,
        NativeCompressionMethod::Lz4 => lz4_flex::compress(raw),
        NativeCompressionMethod::None => return Ok(()),
    };

    // Build header: method(1) + compressed_size(4) + decompressed_size(4) + payload
    let mut frame = Vec::with_capacity(compressed_payload.len() + 9);
    frame.push(compression.byte());
    frame.extend_from_slice(&(compressed_payload.len() as u32 + 9).to_le_bytes());
    frame.extend_from_slice(&(decompressed_size as u32).to_le_bytes());
    frame.extend_from_slice(&compressed_payload);

    let hash = cityhash_rs::cityhash_102_128(&frame);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&frame).await?;

    Ok(())
}

/// Read and decompress a single chunk. Validates CityHash128 checksum.
pub(crate) async fn decompress_data<R: ClickHouseRead>(
    reader: &mut R,
    compression: NativeCompressionMethod,
) -> Result<Vec<u8>> {
    // Read checksum (16 bytes)
    let checksum_high = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;
    let checksum_low = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;
    let checksum = (u128::from(checksum_high) << 64) | u128::from(checksum_low);

    // Read compression header (9 bytes)
    let type_byte = reader
        .read_u8()
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;
    if type_byte != compression.byte() {
        return Err(Error::Decompression(
            format!(
                "unexpected compression algorithm for {compression}: 0x{type_byte:02x}"
            )
            .into(),
        ));
    }

    let compressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;
    let decompressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;

    // Sanity checks
    if compressed_size > 100_000_000 || decompressed_size > 1_000_000_000 {
        return Err(Error::Decompression("chunk size too large".into()));
    }

    // Build the complete compressed block for checksum validation
    let mut compressed = vec![0u8; compressed_size as usize];
    reader
        .read_exact(&mut compressed[9..])
        .await
        .map_err(|e| Error::Decompression(Box::new(e)))?;
    compressed[0] = type_byte;
    compressed[1..5].copy_from_slice(&compressed_size.to_le_bytes());
    compressed[5..9].copy_from_slice(&decompressed_size.to_le_bytes());

    // Validate checksum
    let calc_checksum = cityhash_rs::cityhash_102_128(&compressed);
    if calc_checksum != checksum {
        return Err(Error::Decompression(
            format!("checksum mismatch: expected {checksum:032x}, got {calc_checksum:032x}")
                .into(),
        ));
    }

    // Decompress
    match compression {
        NativeCompressionMethod::Lz4 => lz4_flex::decompress(&compressed[9..], decompressed_size as usize)
            .map_err(|e| Error::Decompression(Box::new(e))),
        NativeCompressionMethod::Zstd => zstd::bulk::decompress(&compressed[9..], decompressed_size as usize)
            .map_err(|e| Error::Decompression(Box::new(e))),
        NativeCompressionMethod::None => {
            Err(Error::Decompression("attempted to decompress uncompressed data".into()))
        }
    }
}

type BlockReadingFuture<'a, R> =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, &'a mut R)>> + Send + Sync + 'a>>;

/// Async reader that decompresses ClickHouse native protocol blocks on-the-fly.
pub(crate) struct DecompressionReader<'a, R: ClickHouseRead + 'static> {
    mode: NativeCompressionMethod,
    inner: Option<&'a mut R>,
    decompressed: Vec<u8>,
    position: usize,
    block_reading_future: Option<BlockReadingFuture<'a, R>>,
}

impl<'a, R: ClickHouseRead> DecompressionReader<'a, R> {
    /// Create decompressor. Reads first chunk immediately.
    pub(crate) async fn new(mode: NativeCompressionMethod, inner: &'a mut R) -> Result<Self> {
        let decompressed = decompress_data(inner, mode).await?;
        Ok(Self {
            mode,
            inner: Some(inner),
            decompressed,
            position: 0,
            block_reading_future: None,
        })
    }
}

impl<R: ClickHouseRead> AsyncRead for DecompressionReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Check if we have a pending decompression future
        if let Some(block_reading_future) = self.block_reading_future.as_mut() {
            match block_reading_future.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok((value, inner))) => {
                    drop(self.block_reading_future.take());
                    self.decompressed = value;
                    self.position = 0;
                    self.inner = Some(inner);
                }
                Poll::Ready(Err(e)) => {
                    drop(self.block_reading_future.take());
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )));
                }
            }
        }

        // Serve available data
        let available = self.decompressed.len() - self.position;
        if available > 0 {
            let to_serve = available.min(buf.remaining());
            buf.put_slice(&self.decompressed[self.position..self.position + to_serve]);
            self.position += to_serve;
            return Poll::Ready(Ok(()));
        }

        // Need more data — start reading next chunk
        if let Some(inner) = self.inner.take() {
            let mode = self.mode;
            self.block_reading_future = Some(Box::pin(async move {
                let value = decompress_data(inner, mode).await?;
                Ok((value, inner))
            }));
            return self.poll_read(cx, buf);
        }

        // EOF
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn test_roundtrip_lz4() {
        let data = b"test data for LZ4 compression".to_vec();
        let mut buffer = Vec::new();
        compress_data(&mut buffer, &data, NativeCompressionMethod::Lz4)
            .await
            .unwrap();
        assert!(!buffer.is_empty());

        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, NativeCompressionMethod::Lz4)
            .await
            .unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_roundtrip_zstd() {
        let data = b"test data for ZSTD compression".to_vec();
        let mut buffer = Vec::new();
        compress_data(&mut buffer, &data, NativeCompressionMethod::Zstd)
            .await
            .unwrap();
        assert!(!buffer.is_empty());

        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, NativeCompressionMethod::Zstd)
            .await
            .unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_none_is_noop() {
        let data = b"test data no compression".to_vec();
        let mut buffer = Vec::new();
        compress_data(&mut buffer, &data, NativeCompressionMethod::None)
            .await
            .unwrap();
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let data = b"test data for checksum validation".to_vec();
        let mut buffer = Vec::new();
        compress_data(&mut buffer, &data, NativeCompressionMethod::Lz4)
            .await
            .unwrap();

        // Corrupt the checksum
        buffer[0] ^= 0xFF;

        let mut reader = Cursor::new(buffer);
        let result = decompress_data(&mut reader, NativeCompressionMethod::Lz4).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("checksum mismatch"), "got: {err_msg}");
    }

    #[tokio::test]
    async fn test_decompression_reader_single_chunk() {
        let data = b"test data for single chunk reading".to_vec();
        let expected_len = data.len();

        let mut buffer = Vec::new();
        compress_data(&mut buffer, &data, NativeCompressionMethod::Lz4)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer);
        let mut decomp_reader =
            DecompressionReader::new(NativeCompressionMethod::Lz4, &mut reader)
                .await
                .unwrap();

        let mut result = vec![0u8; expected_len];
        decomp_reader.read_exact(&mut result).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_roundtrip_both_algorithms() {
        let original = b"This is a longer piece of test data that should compress well \
                         with both LZ4 and ZSTD algorithms"
            .to_vec();

        for compression in [NativeCompressionMethod::Lz4, NativeCompressionMethod::Zstd] {
            let mut compressed_buffer = Vec::new();
            compress_data(&mut compressed_buffer, &original, compression)
                .await
                .unwrap();

            let mut reader = Cursor::new(compressed_buffer);
            let decompressed = decompress_data(&mut reader, compression).await.unwrap();
            assert_eq!(decompressed, original, "round trip failed for {compression}");
        }
    }
}
