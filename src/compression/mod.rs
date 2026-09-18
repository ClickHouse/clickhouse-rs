use crate::error::Error;
use crate::response::Chunk;
use bytes::{Buf, Bytes, BytesMut};
use cityhash_rs::cityhash_102_128;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

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
const LZ4_MAGIC: u8 = 0x82;
const ZSTD_MAGIC: u8 = 0x90;

pub(crate) struct DecompressStream<S> {
    stream: S,
    decompress: Option<DecompressState>,
}

pub(crate) struct DecompressState {
    in_buffer: BytesMut,
    out_buffer: BytesMut,
    header: Option<FrameHeader>,
}

struct FrameHeader {
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
        DecompressStream {
            stream,
            decompress: compression.is_enabled().then(|| DecompressState {
                in_buffer: BytesMut::with_capacity(16384),
                out_buffer: BytesMut::zeroed(16384),
                header: None,
            }),
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

        if let Some(decompress) = &mut this.decompress {
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

impl DecompressState {
    fn feed(&mut self, bytes: Bytes) {
        self.in_buffer.extend_from_slice(&bytes);
    }

    fn drain(&mut self) -> crate::Result<Option<Chunk>> {
        let header = match self.header {
            Some(ref header) => header,
            None => match FrameHeader::try_decode(&mut self.in_buffer) {
                Some(header) => self.header.insert(header),
                None => return Ok(None),
            },
        };

        // Check compression method before we allocate or anything else.
        match header.method {
            LZ4_MAGIC => if cfg!(not(feature = "lz4")) {
                return Err(Error::Decompression("compressed data frame uses Lz4, but `lz4` feature of `clickhouse` crate is not enabled".into()));
            }
            ZSTD_MAGIC => if cfg!(not(feature = "zstd")) {
                return Err(Error::Decompression("compressed data frame uses Zstd, but `zstd` feature of `clickhouse` crate is not enabled".into()));
            }
            other => {
                return Err(Error::Decompression(format!("unexpected compression method {other:#02x} for ClickHouse compressed data frame").into()))
            }
        }

        // Error is only possible on 16-bit targets
        let compressed_size: usize = header.compressed_size.try_into().map_err(|_| {
            Error::Decompression(
                format!(
                    "compressed_size of frame overflows `usize` for this platform: {}",
                    header.compressed_size
                )
                .into(),
            )
        })?;

        if compressed_size > MAX_COMPRESSED_SIZE {
            return Err(Error::Decompression(
                format!(
                    "compressed_size of frame exceeds safe limit (1 GiB): {}",
                    header.compressed_size
                )
                .into(),
            ));
        }

        let decompressed_size: usize = header.decompressed_size.try_into().map_err(|_| {
            Error::Decompression(
                format!(
                    "decompressed_size of frame overflows `usize` for this platform: {}",
                    header.decompressed_size
                )
                .into(),
            )
        })?;

        if self.in_buffer.len() < compressed_size {
            return Ok(None);
        }

        self.out_buffer.resize(decompressed_size, 0);

        let actual_size = match header.method {
            #[cfg(feature = "lz4")]
            LZ4_MAGIC => {
                lz4_flex::decompress_into(&self.in_buffer[..compressed_size], &mut self.out_buffer)
                    .map_err(|e| Error::Decompression(e.into()))?
            }
            #[cfg(feature = "zstd")]
            ZSTD_MAGIC => ::zstd::bulk::decompress_to_buffer(
                &self.in_buffer[..compressed_size],
                &mut self.out_buffer,
            )
            .map_err(|e| Error::Decompression(e.into()))?,
            other => unreachable!("BUG: unhandled compression method {other:#02x}"),
        };

        if decompressed_size != actual_size {
            return Err(Error::Decompression(format!("compressed data frame reported decompressed_size={decompressed_size}, but actual size was {actual_size}").into()));
        }

        #[cfg(feature = "__compression")]
        {
            let actual_checksum = calc_checksum(&self.out_buffer);

            if header.checksum != actual_checksum {
                return Err(Error::Decompression(format!("compressed data frame checksum mismatch; expected={:#032x}, actual={actual_checksum:#032x}", header.checksum).into()));
            }
        }

        let net_size = FrameHeader::CHECKSUM_SIZE + compressed_size;

        // Read new frame on next call
        self.header = None;

        Ok(Some(Chunk {
            data: self.out_buffer.split().freeze(),
            net_size,
        }))
    }

    fn finish(&mut self) -> crate::Result<()> {
        if self.in_buffer.is_empty() {
            // Forward jumps are generally predicted as not-taken
            return Ok(());
        }

        if let Some(header) = &self.header {
            return Err(Error::Decompression(
                format!(
                    "incomplete compression frame at end of stream: expected {} bytes, got {}",
                    header.compressed_size,
                    self.in_buffer.len()
                )
                .into(),
            ));
        }

        Err(Error::Decompression(
            format!(
                "incomplete compression frame header at end of stream: expected {} bytes, got {}",
                FrameHeader::SIZE,
                self.in_buffer.len()
            )
            .into(),
        ))
    }
}

impl FrameHeader {
    const CHECKSUM_SIZE: usize = size_of::<u128>();
    const SIZE: usize = Self::CHECKSUM_SIZE + 1 + 4 + 4; // checksum + method + compressed_size + uncompressed_size

    fn try_decode(bytes: &mut BytesMut) -> Option<Self> {
        (bytes.len() >= Self::SIZE).then(|| Self {
            checksum: bytes.get_u128_le(),
            method: bytes.get_u8(),
            compressed_size: bytes.get_u32_le(),
            decompressed_size: bytes.get_u32_le(),
        })
    }
}

#[cfg(feature = "__compression")]
fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_102_128(buffer);
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
