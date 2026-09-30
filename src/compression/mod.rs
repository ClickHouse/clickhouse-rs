#[cfg(feature = "lz4")]
pub(crate) mod lz4;
#[cfg(feature = "zstd")]
pub(crate) mod zstd;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Compression {
    /// Disables any compression.
    /// Used by default if no compression feature is enabled.
    None,
    /// Uses `LZ4` codec to (de)compress.
    /// Used by default if the `lz4` feature is enabled.
    #[cfg(feature = "lz4")]
    Lz4,
    /// Uses `LZ4HC` codec to compress and `LZ4` to decompress.
    /// High compression levels are useful in networks with low bandwidth.
    /// Affects only `INSERT`s, because others are compressed by the server.
    /// Possible levels: `[1, 12]`. Recommended level range: `[4, 9]`.
    ///
    /// Deprecated: `lz4_flex` doesn't support HC mode yet: [lz4_flex#165].
    ///
    /// [lz4_flex#165]: https://github.com/PSeitz/lz4_flex/issues/165
    #[cfg(feature = "lz4")]
    #[deprecated(note = "use `Compression::Lz4` instead")]
    Lz4Hc(i32),
    /// Uses `ZSTD` codec to (de)compress.
    /// Used by default if the `zstd` feature is enabled and `lz4` is not.
    /// The `i32` parameter specifies the compression level.
    /// Use [`Compression::zstd()`] for the default level.
    ///
    /// **Note:** Extremely high compression levels (e.g. above 19) are very
    /// CPU-intensive and likely unsuitable for real-time networked
    /// applications. They can also block the async executor for a
    /// significant amount of time. Prefer moderate levels for online usage.
    #[cfg(feature = "zstd")]
    Zstd(i32),
}

impl Default for Compression {
    #[cfg(all(not(feature = "test-util"), feature = "lz4"))]
    #[inline]
    fn default() -> Self {
        Compression::Lz4
    }

    #[cfg(all(not(feature = "test-util"), not(feature = "lz4"), feature = "zstd"))]
    #[inline]
    fn default() -> Self {
        Compression::zstd()
    }

    #[cfg(any(feature = "test-util", not(any(feature = "lz4", feature = "zstd"))))]
    #[inline]
    fn default() -> Self {
        Compression::None
    }
}

impl Compression {
    /// Creates a `Zstd` compression with the default level.
    #[cfg(feature = "zstd")]
    pub fn zstd() -> Self {
        Compression::Zstd(::zstd::DEFAULT_COMPRESSION_LEVEL)
    }

    pub(crate) fn is_enabled(&self) -> bool {
        *self != Compression::None
    }
}

/// Utils for writing ClickHouse's native compression framing (`compress=1`/`decompress=1`).
#[cfg(any(feature = "lz4", feature = "zstd"))]
mod native_framing {
    use crate::error::Error;
    use bytes::BufMut;

    pub(crate) const CHECKSUM_SIZE: usize = 16;
    pub(crate) const HEADER_SIZE: usize = 9;
    pub(crate) const META_SIZE: usize = CHECKSUM_SIZE + HEADER_SIZE;

    pub(crate) fn write_meta(
        buffer: &mut [u8],
        magic_byte: u8,
        uncompressed_len: usize,
    ) -> Result<(), Error> {
        let (mut checksum_bytes, header_and_data_bytes) = buffer.split_at_mut(CHECKSUM_SIZE);

        let compressed_len = u32::try_from(header_and_data_bytes.len()).map_err(|_| {
            Error::Compression(
                format!(
                    "compressed size of frame exceeds 4 GiB: {}",
                    header_and_data_bytes.len()
                )
                .into(),
            )
        })?;

        let uncompressed_len = u32::try_from(uncompressed_len).map_err(|_| {
            Error::Compression(
                format!("un-compressed size of frame exceeds 4 GiB: {uncompressed_len}").into(),
            )
        })?;

        // https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat#frame-format
        let mut header = &mut header_and_data_bytes[..HEADER_SIZE];
        header.put_u8(magic_byte);
        header.put_u32_le(compressed_len);
        header.put_u32_le(uncompressed_len);

        let checksum = calc_checksum(header_and_data_bytes);
        checksum_bytes.put_u128_le(checksum);

        Ok(())
    }

    fn calc_checksum(buffer: &[u8]) -> u128 {
        let hash = cityhash_rs::cityhash_102_128(buffer);
        hash.rotate_right(64)
    }
}

#[cfg(test)]
mod test_util {
    use futures_util::Stream;
    use std::cmp;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use crate::Error;
    use crate::response::Chunk;
    use bytes::Bytes;

    pub(super) trait TestDecoder {
        type Stream<S>: Stream<Item = Result<Chunk, Error>> + Unpin
        where
            S: Stream<Item = Result<Bytes, Error>> + Unpin;

        fn with_stream<S>(stream: S) -> Self::Stream<S>
        where
            S: Stream<Item = Result<Bytes, Error>> + Unpin;
    }

    pub(super) fn test_decoder<D: TestDecoder>(compressed: Bytes, uncompressed: &[u8]) {
        // Some normal and some arbitrary/weird chunk sizes to test with.
        let chunk_sizes = [32, 53, 64, 67, 128, 131, 256, 277, 384, 463, 512];

        for chunk_size in chunk_sizes {
            let mut offset = 0;

            let mut stream = D::with_stream(ChunkStream {
                data: compressed.clone(),
                chunk_size,
            });

            loop {
                match Pin::new(&mut stream).poll_next(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        assert!(
                            offset + chunk.data.len() <= uncompressed.len(),
                            "chunk length ({}) at offset {offset} exceeds expected length ({}) (chunk size {chunk_size})",
                            chunk.data.len(),
                            uncompressed.len(),
                        );

                        for (actual, (offset, expected)) in chunk
                            .data
                            .iter()
                            .zip(uncompressed.iter().enumerate().skip(offset))
                        {
                            assert_eq!(
                                actual, expected,
                                "unexpected byte in decompressed data (offset {offset}, chunk size {chunk_size})"
                            );
                        }

                        offset += chunk.data.len();
                    }
                    Poll::Ready(Some(Err(e))) => {
                        panic!("decoder returned error (chunk size {chunk_size}): {e:#}");
                    }
                    Poll::Ready(None) => {
                        assert_eq!(offset, uncompressed.len(), "");
                        break;
                    }
                    Poll::Pending => {
                        panic!(
                            "decoder returned `Poll::Pending` when underlying implementation did not"
                        );
                    }
                }
            }
        }
    }

    struct ChunkStream {
        data: Bytes,
        chunk_size: usize,
    }

    impl Stream for ChunkStream {
        type Item = Result<Bytes, Error>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = &mut *self;

            if this.data.is_empty() || this.chunk_size == 0 {
                return Poll::Ready(None);
            }

            Poll::Ready(Some(Ok(this
                .data
                .split_to(cmp::min(this.chunk_size, this.data.len())))))
        }
    }
}
