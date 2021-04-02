use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes};
use futures::{ready, stream::Stream};
use lz4_flex::decompress;

use crate::{
    buflist::BufList,
    error::{Error, Result},
};

const MAX_COMPRESSED_SIZE: u32 = 1024 * 1024 * 1024;

pub struct Lz4Decoder<S> {
    inner: S,
    chunks: BufList<Bytes>,
    meta: Option<Lz4Meta>,
    buffer: Vec<u8>,
}

impl<S, E> Stream for Lz4Decoder<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Error>,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let meta = loop {
            let size = self.chunks.remaining();
            let required_size = self.meta.as_ref().map_or(LZ4_META_SIZE, |m| {
                m.compressed_size as usize - LZ4_HEADER_SIZE
            });

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
const LZ4_META_SIZE: usize = 25;
const LZ4_HEADER_SIZE: usize = 9;
const LZ4_MAGIC: u8 = 0x82;

struct Lz4Meta {
    checksum: u128,
    compressed_size: u32, // TODO: use NonZeroU32?
    uncompressed_size: u32,
}

impl<S> Lz4Decoder<S> {
    pub fn new(stream: S) -> Self {
        Self {
            inner: stream,
            chunks: BufList::default(),
            meta: None,
            buffer: Vec::new(),
        }
    }

    fn read_meta(&mut self) -> Result<Lz4Meta> {
        assert!(self.chunks.remaining() >= LZ4_META_SIZE);

        let checksum = self.chunks.get_u128_le();
        let magic = self.chunks.get_u8();
        let compressed_size = self.chunks.get_u32_le();
        let uncompressed_size = self.chunks.get_u32_le();

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

    fn read_data(&mut self, header: Lz4Meta) -> Result<Bytes> {
        assert!(self.chunks.remaining() >= header.compressed_size as usize - LZ4_HEADER_SIZE);

        self.buffer.resize(header.compressed_size as usize, 0);

        let compressed = &mut self.buffer[..];
        (&mut compressed[0..]).put_u8(LZ4_MAGIC);
        (&mut compressed[1..]).put_u32_le(header.compressed_size);
        (&mut compressed[5..]).put_u32_le(header.uncompressed_size);

        self.chunks
            .copy_to_slice(&mut compressed[LZ4_HEADER_SIZE..]);

        let actual_checksum = calc_checksum(compressed);
        if actual_checksum != header.checksum {
            return Err(Error::Decompression("checksum mismatch".into()));
        }

        let uncompressed = decompress(
            &compressed[LZ4_HEADER_SIZE..],
            header.uncompressed_size as usize,
        )
        .map_err(|e| Error::Decompression(Box::new(e)))?;
        Ok(uncompressed.into())
    }
}

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = clickhouse_rs_cityhash_sys::city_hash_128(buffer);
    u128::from(hash.hi) << 64 | u128::from(hash.lo)
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
        assert_eq!(actual, Some(Bytes::copy_from_slice(expected)));
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
