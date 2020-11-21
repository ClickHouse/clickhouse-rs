use std::{
    os::raw::{c_char, c_int},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use futures::{ready, stream::Stream};
use lz4::liblz4::LZ4_decompress_safe;

use crate::{
    buflist::BufList,
    error::{Error, Result},
};

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    Lz4,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "brotli")]
    Brotli,
}

impl Default for Compression {
    #[inline]
    fn default() -> Self {
        Compression::Lz4
    }
}

impl Compression {
    pub fn encoding(&self) -> Option<&'static str> {
        match self {
            Compression::None => Option::None,
            Compression::Lz4 => Option::None,
            #[cfg(feature = "gzip")]
            Compression::Gzip => Some("gzip"),
            #[cfg(feature = "zlib")]
            Compression::Zlib => Some("deflate"),
            #[cfg(feature = "brotli")]
            Compression::Brotli => Some("br"),
        }
    }
}

const MAX_COMPRESSED_SIZE: u32 = 1024 * 1024 * 1024;

pub struct Lz4Decoder<S> {
    inner: S,
    chunks: BufList<Bytes>,
    header: Option<Lz4Header>,
}

impl<S, E> Stream for Lz4Decoder<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Error>,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let header = loop {
            let size = self.chunks.remaining();
            let required_size = self
                .header
                .as_ref()
                .map_or(LZ4_HEADER_SIZE, |h| h.compressed_size as usize);

            if size < required_size {
                let inner = Pin::new(&mut self.inner);
                match ready!(inner.poll_next(cx)) {
                    Some(Ok(chunk)) => {
                        self.chunks.push(chunk);
                        continue;
                    }
                    Some(Err(err)) => return Some(Err(err.into())).into(),
                    None if size > 0 => {
                        let err = Error::Decompression("malformed data".into());
                        return Poll::Ready(Some(Err(err)));
                    }
                    None => return Poll::Ready(None),
                }
            }

            assert!(size >= required_size);

            match self.header.take() {
                Some(header) => break header,
                None => self.header = Some(self.read_header()?),
            };
        };

        let bytes = self.read_data(header)?;
        Poll::Ready(Some(Ok(bytes)))
    }

    // TODO: implement `size_hint`?
}

// - [16b] checksum
// - [ 1b] magic number (0x82)
// - [ 4b] compressed size
// - [ 4b] uncompressed size
const LZ4_HEADER_SIZE: usize = 25;

struct Lz4Header {
    checksum: u128,
    compressed_size: u32,
    uncompressed_size: u32, // TODO: use NonZeroU32
}

impl<S> Lz4Decoder<S> {
    pub fn new(stream: S) -> Self {
        Self {
            inner: stream,
            chunks: BufList::default(),
            header: None,
        }
    }

    fn read_header(&mut self) -> Result<Lz4Header> {
        assert!(self.chunks.remaining() >= LZ4_HEADER_SIZE);

        let checksum = self.chunks.get_u128_le();
        let magic = self.chunks.get_u8();
        let compressed_size = self.chunks.get_u32_le() - 9;
        let uncompressed_size = self.chunks.get_u32_le();

        if magic != 0x82 {
            return Err(Error::Decompression("incorrect magic number".into()));
        }

        if compressed_size > MAX_COMPRESSED_SIZE {
            return Err(Error::Decompression("too big compressed data".into()));
        }

        // TODO: check checksum.

        Ok(Lz4Header {
            checksum,
            compressed_size,
            uncompressed_size,
        })
    }

    fn read_data(&mut self, header: Lz4Header) -> Result<Bytes> {
        // TODO: reuse buffers.
        let mut compressed = vec![0u8; header.compressed_size as usize];
        self.chunks.copy_to_slice(&mut compressed);

        let mut uncompressed = vec![0u8; header.uncompressed_size as usize];
        let status = unsafe {
            LZ4_decompress_safe(
                compressed.as_ptr() as *const c_char,
                uncompressed.as_mut_ptr() as *mut c_char,
                header.compressed_size as c_int,
                header.uncompressed_size as c_int,
            )
        };

        if status < 0 {
            return Err(Error::Decompression(
                format!("can't decompress data: {}", status).into(),
            ));
        }

        Ok(uncompressed.into())
    }
}

#[tokio::test]
async fn it_decompress() {
    use futures::stream::{self, TryStreamExt};

    let expected = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let source = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let input = stream::iter(vec![Ok::<_, Error>(source.into())]);
    let mut decoder = Lz4Decoder::new(input);
    let actual = decoder.try_next().await.unwrap();
    assert_eq!(actual, Some(expected.into()));
}
