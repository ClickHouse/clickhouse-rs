use crate::response::Chunk;
use bytes::{Buf, Bytes};
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

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

const MAX_COMPRESSED_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB
const MAX_DECOMPRESSED_SIZE: usize = 4 * MAX_COMPRESSED_SIZE;

const LZ4_MAGIC: u8 = 0x82;
const ZSTD_MAGIC: u8 = 0x90;

pub(crate) struct DecompressStream<S> {
    stream: S,
    // If compression isn't enabled, we can eliminate a lot of dead code.
    #[cfg(feature = "__compression")]
    decompress: Option<decompress::State>,
}

struct FrameMeta {
    checksum: u128,
    method: u8,
    compressed_size: u32,
    decompressed_size: u32,
}

impl<S> DecompressStream<S>
where
    S: Stream<Item = crate::Result<Bytes>> + Unpin,
{
    pub(crate) fn new(stream: S, compression: Compression) -> Self {
        #[cfg(not(feature = "__compression"))]
        assert!(!compression.is_enabled());

        DecompressStream {
            stream,
            #[cfg(feature = "__compression")]
            decompress: compression.is_enabled().then(decompress::State::new),
        }
    }
}

impl<S> Stream for DecompressStream<S>
where
    S: Stream<Item = crate::Result<Bytes>> + Unpin,
{
    type Item = crate::Result<Chunk>;

    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        #[cfg(feature = "__compression")]
        if let Some(decompress) = &mut this.decompress {
            use std::task::ready;

            loop {
                if let Some(chunk) = decompress.drain()? {
                    return Poll::Ready(Some(Ok(chunk)));
                }

                if let Some(res) = ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                    decompress.feed(res?);
                    continue;
                }

                decompress.finish()?;

                return Poll::Ready(None);
            }
        }

        // No compression enabled, pass data through unprocessed
        Pin::new(&mut this.stream)
            .poll_next(cx)
            .map_ok(|data| Chunk {
                net_size: data.len(),
                data,
            })
    }
}

impl FrameMeta {
    const CHECKSUM_SIZE: usize = size_of::<u128>();

    const HEADER_SIZE: usize = 1 + size_of::<u32>() * 2; // method + compressed_size + uncompressed_size

    const TOTAL_SIZE: usize = Self::CHECKSUM_SIZE + Self::HEADER_SIZE;

    #[cfg(feature = "__compression")]
    fn try_decode(mut bytes: &[u8]) -> Option<Self> {
        (bytes.len() >= Self::TOTAL_SIZE).then(|| Self {
            checksum: bytes.get_u128_le(),
            method: bytes.get_u8(),
            compressed_size: bytes.get_u32_le(),
            decompressed_size: bytes.get_u32_le(),
        })
    }
}

#[cfg(feature = "__compression")]
fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_rs::cityhash_102_128(buffer);
    // Note (abonander): not sure why this is necessary, the checksum is documented as
    // low 8 bytes (LE), high 8 bytes (LE) which would seem to just be u128LE:
    // https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat#checksum
    // See also where it's actually encoded in `CompressedWriteBuffer`:
    // https://github.com/ClickHouse/ClickHouse/blob/7358a9e8956aaad26350ddeb086b778594c450cd/src/Compression/CompressedWriteBuffer.cpp#L44-L47
    //
    // I'm assuming the `cityhash-rs` crate returns it with the parts swapped by mistake
    // and this exists to fix that.
    hash.rotate_right(64)
}

#[cfg(feature = "__compression")]
mod decompress {
    use crate::compression::{
        FrameMeta, LZ4_MAGIC, MAX_COMPRESSED_SIZE, MAX_DECOMPRESSED_SIZE, ZSTD_MAGIC, calc_checksum,
    };
    use crate::error::Error;
    use crate::response::Chunk;
    use bytes::{Buf, Bytes, BytesMut};

    pub(crate) struct State {
        in_buffer: BytesMut,
        out_buffer: BytesMut,
        meta: Option<FrameMeta>,
    }

    impl State {
        pub(super) fn new() -> Self {
            Self {
                in_buffer: BytesMut::with_capacity(8192),
                out_buffer: BytesMut::zeroed(8192),
                meta: None,
            }
        }

        pub(super) fn feed(&mut self, bytes: Bytes) {
            self.in_buffer.extend_from_slice(&bytes);
        }

        pub(super) fn drain(&mut self) -> crate::Result<Option<Chunk>> {
            let header = match self.meta {
                Some(ref header) => header,
                None => match FrameMeta::try_decode(&self.in_buffer) {
                    Some(header) => self.meta.insert(header),
                    None => return Ok(None),
                },
            };

            // Check compression method before we allocate or anything else.
            match header.method {
                LZ4_MAGIC => {
                    if cfg!(not(feature = "lz4")) {
                        return Err(Error::decompression(
                            "compressed data frame uses Lz4, but `lz4` feature of `clickhouse` crate is not enabled",
                        ));
                    }
                }
                ZSTD_MAGIC => {
                    if cfg!(not(feature = "zstd")) {
                        return Err(Error::decompression(
                            "compressed data frame uses Zstd, but `zstd` feature of `clickhouse` crate is not enabled",
                        ));
                    }
                }
                other => {
                    return Err(Error::decompression(format!(
                        "unexpected compression method {other:#02x} for ClickHouse compressed data frame"
                    )));
                }
            }

            let compressed_size = usize::try_from(header.compressed_size)
                // Error is only possible on architectures smaller than 32-bit
                .map_err(|_| {
                    Error::decompression(format!(
                        "compressed_size of frame overflows `usize` for this platform: {}",
                        header.compressed_size
                    ))
                })?;

            if compressed_size > MAX_COMPRESSED_SIZE {
                return Err(Error::decompression(format!(
                    "compressed_size of frame exceeds safe limit ({MAX_COMPRESSED_SIZE} bytes): {compressed_size}",
                )));
            }

            let decompressed_size: usize = header.decompressed_size.try_into().map_err(|_| {
                Error::decompression(format!(
                    "decompressed_size of frame overflows `usize` for this platform: {}",
                    header.decompressed_size
                ))
            })?;

            if decompressed_size > MAX_DECOMPRESSED_SIZE {
                return Err(Error::decompression(format!(
                    "decompressed_size of frame exceeds safe limit ({MAX_DECOMPRESSED_SIZE} bytes): {decompressed_size}",
                )));
            }

            let net_size = FrameMeta::CHECKSUM_SIZE + compressed_size;

            if self.in_buffer.len() < net_size {
                return Ok(None);
            }

            // Checksum covers `(method, compressed_size, uncompressed_size, compressed_data)`
            let actual_checksum =
                calc_checksum(&self.in_buffer[FrameMeta::CHECKSUM_SIZE..][..compressed_size]);

            if header.checksum != actual_checksum {
                return Err(Error::decompression(format!(
                    "compressed data frame checksum mismatch; expected={:#032x}, actual={actual_checksum:#032x}",
                    header.checksum
                )));
            }

            self.out_buffer.resize(decompressed_size, 0);

            let compressed_data_len = compressed_size
                .checked_sub(FrameMeta::HEADER_SIZE)
                .ok_or_else(|| Error::decompression(format!("invalid compressed data frame: compressed_size ({compressed_size}) - header_size ({}) underflowed", FrameMeta::HEADER_SIZE)))?;

            let compressed_data = &self.in_buffer[FrameMeta::TOTAL_SIZE..][..compressed_data_len];

            let actual_len = match header.method {
                #[cfg(feature = "lz4")]
                LZ4_MAGIC => lz4_flex::decompress_into(compressed_data, &mut self.out_buffer)
                    .map_err(Error::decompression)?,
                #[cfg(feature = "zstd")]
                ZSTD_MAGIC => {
                    zstd::bulk::decompress_to_buffer(compressed_data, &mut self.out_buffer)
                        .map_err(Error::decompression)?
                }
                other => unreachable!("BUG: unhandled compression method {other:#02x}"),
            };

            if decompressed_size != actual_len {
                return Err(Error::decompression(format!(
                    "compressed data frame reported decompressed_size={decompressed_size}, but actual size was {actual_len}"
                )));
            }

            self.in_buffer.advance(net_size);

            // Read new frame on next call
            self.meta = None;

            Ok(Some(Chunk {
                data: self.out_buffer.split().freeze(),
                net_size,
            }))
        }

        pub(super) fn finish(&mut self) -> crate::Result<()> {
            if self.in_buffer.is_empty() {
                return Ok(());
            }

            if let Some(header) = &self.meta {
                return Err(Error::decompression(format!(
                    "incomplete compression frame at end of stream: expected {} bytes, got {}",
                    header.compressed_size,
                    self.in_buffer.len()
                )));
            }

            Err(Error::decompression(format!(
                "incomplete compression frame header at end of stream: expected {} bytes, got {}",
                FrameMeta::TOTAL_SIZE,
                self.in_buffer.len()
            )))
        }
    }
}
