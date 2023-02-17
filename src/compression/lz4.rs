use std::{
    os::raw::{c_char, c_int},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{ready, stream::Stream};
use lz4::liblz4::LZ4_decompress_safe;

use crate::{
    buflist::BufList,
    error::{Error, Result},
    Compression,
};

const MAX_COMPRESSED_SIZE: u32 = 1024 * 1024 * 1024;

pub(crate) struct Lz4Decoder<S> {
    stream: S,
    chunks: BufList<Bytes>,
    meta: Option<Lz4Meta>,
    buffer: Vec<u8>,
}

impl<S> Stream for Lz4Decoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let meta = loop {
            let size = self.chunks.remaining();
            let required_size = self.meta.as_ref().map_or(LZ4_META_SIZE, |m| {
                m.compressed_size as usize - LZ4_HEADER_SIZE
            });

            if size < required_size {
                let stream = Pin::new(&mut self.stream);
                match ready!(stream.poll_next(cx)) {
                    Some(Ok(chunk)) => {
                        self.chunks.push(chunk);
                        continue;
                    }
                    Some(Err(err)) => return Some(Err(err)).into(),
                    None if size > 0 => {
                        let err = Error::Decompression("malformed data".into());
                        return Poll::Ready(Some(Err(err)));
                    }
                    None => return Poll::Ready(None),
                }
            }

            assert!(size >= required_size);

            match self.meta.take() {
                Some(meta) => break meta,
                None => self.meta = Some(self.read_meta()?),
            };
        };

        let bytes = self.read_data(meta)?;
        self.chunks.commit();
        Poll::Ready(Some(Ok(bytes)))
    }
}

// Meta = checksum + header
// - [16b] checksum
// - [ 1b] magic number (0x82)
// - [ 4b] compressed size
// - [ 4b] uncompressed size
const LZ4_CHECKSUM_SIZE: usize = 16;
const LZ4_HEADER_SIZE: usize = 9;
const LZ4_META_SIZE: usize = LZ4_CHECKSUM_SIZE + LZ4_HEADER_SIZE;
const LZ4_MAGIC: u8 = 0x82;

struct Lz4Meta {
    checksum: u128,
    compressed_size: u32,
    uncompressed_size: u32,
}

impl Lz4Meta {
    fn read(mut buffer: impl Buf) -> Result<Lz4Meta> {
        let checksum = buffer.get_u128_le();
        let magic = buffer.get_u8();
        let compressed_size = buffer.get_u32_le();
        let uncompressed_size = buffer.get_u32_le();

        if magic != LZ4_MAGIC {
            return Err(Error::Decompression("incorrect magic number".into()));
        }

        if compressed_size > MAX_COMPRESSED_SIZE {
            return Err(Error::Decompression("too big compressed data".into()));
        }

        Ok(Lz4Meta {
            checksum,
            compressed_size,
            uncompressed_size,
        })
    }

    fn write_checksum(&self, mut buffer: impl BufMut) {
        buffer.put_u128_le(self.checksum);
    }

    fn write_header(&self, mut buffer: impl BufMut) {
        buffer.put_u8(LZ4_MAGIC);
        buffer.put_u32_le(self.compressed_size);
        buffer.put_u32_le(self.uncompressed_size);
    }
}

impl<S> Lz4Decoder<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            chunks: BufList::default(),
            meta: None,
            buffer: Vec::new(),
        }
    }

    fn read_meta(&mut self) -> Result<Lz4Meta> {
        assert!(self.chunks.remaining() >= LZ4_META_SIZE);
        Lz4Meta::read(&mut self.chunks)
    }

    fn read_data(&mut self, meta: Lz4Meta) -> Result<Bytes> {
        assert!(self.chunks.remaining() >= meta.compressed_size as usize - LZ4_HEADER_SIZE);

        self.buffer.resize(meta.compressed_size as usize, 0);
        meta.write_header(&mut self.buffer[..]);

        // TODO: if we have a whole frame in one chunk, extra copying can be avoided.
        self.chunks
            .copy_to_slice(&mut self.buffer[LZ4_HEADER_SIZE..]);

        let actual_checksum = calc_checksum(&self.buffer);
        if actual_checksum != meta.checksum {
            return Err(Error::Decompression("checksum mismatch".into()));
        }

        let mut uncompressed = vec![0u8; meta.uncompressed_size as usize];
        decompress(&self.buffer[LZ4_HEADER_SIZE..], &mut uncompressed)?;
        Ok(uncompressed.into())
    }
}

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = clickhouse_rs_cityhash_sys::city_hash_128(buffer);
    u128::from(hash.hi) << 64 | u128::from(hash.lo)
}

fn decompress(compressed: &[u8], uncompressed: &mut [u8]) -> Result<()> {
    let status = unsafe {
        LZ4_decompress_safe(
            compressed.as_ptr() as *const c_char,
            uncompressed.as_mut_ptr() as *mut c_char,
            compressed.len() as c_int,
            uncompressed.len() as c_int,
        )
    };

    if status < 0 {
        return Err(Error::Decompression("can't decompress data".into()));
    }

    Ok(())
}

pub(crate) fn compress(uncompressed: &[u8], mode: Compression) -> Result<Bytes> {
    do_compress(uncompressed, mode).map_err(|err| Error::Decompression(err.into()))
}

fn do_compress(uncompressed: &[u8], mode: Compression) -> std::io::Result<Bytes> {
    let max_compressed_size = lz4::block::compress_bound(uncompressed.len())?;

    let mut buffer = BytesMut::new();
    buffer.resize(LZ4_META_SIZE + max_compressed_size, 0);

    let compressed_data_size = lz4::block::compress_to_buffer(
        uncompressed,
        Some(compression_mode(mode)),
        false,
        &mut buffer[LZ4_META_SIZE..],
    )?;

    buffer.truncate(LZ4_META_SIZE + compressed_data_size);

    let mut meta = Lz4Meta {
        checksum: 0, // will be calculated below.
        compressed_size: (LZ4_HEADER_SIZE + compressed_data_size) as u32,
        uncompressed_size: uncompressed.len() as u32,
    };

    meta.write_header(&mut buffer[LZ4_CHECKSUM_SIZE..]);
    meta.checksum = calc_checksum(&buffer[LZ4_CHECKSUM_SIZE..]);
    meta.write_checksum(&mut buffer[..]);

    Ok(buffer.freeze())
}

fn compression_mode(mode: Compression) -> lz4::block::CompressionMode {
    use lz4::block::CompressionMode;

    match mode {
        Compression::None => unreachable!(),
        Compression::Lz4 => CompressionMode::DEFAULT,
        Compression::Lz4Hc(level) => CompressionMode::HIGHCOMPRESSION(level),
    }
}

#[tokio::test]
async fn it_decompresses() {
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

    async fn test(chunks: &[&[u8]], expected: &[u8]) {
        let stream = stream::iter(
            chunks
                .iter()
                .map(|s| Bytes::copy_from_slice(s))
                .map(Ok::<_, Error>)
                .collect::<Vec<_>>(),
        );
        let mut decoder = Lz4Decoder::new(stream);
        let actual = decoder.try_next().await.unwrap();
        assert_eq!(actual.as_deref(), Some(expected));
    }

    // 1 chunk.
    test(&[&source], &expected).await;

    // 2 chunks.
    for i in 0..source.len() {
        let (left, right) = source.split_at(i);
        test(&[left, right], &expected).await;

        // 3 chunks.
        for j in i..source.len() {
            let (right_a, right_b) = right.split_at(j - i);
            test(&[left, right_a, right_b], &expected).await;
        }
    }
}

#[test]
fn it_compresses() {
    let source = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let expected = vec![
        245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0, 0, 0,
        23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105,
        110, 103, 3, 97, 98, 99,
    ];

    let actual = compress(&source, Compression::Lz4).unwrap();
    assert_eq!(actual, expected);
}
