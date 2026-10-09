use bytes::{Bytes, BytesMut};
use futures_util::stream::Stream;
use lz4_flex::block;
use std::ops::ControlFlow;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::compression::lz4::frame::Lz4FramePushDecoder;
use crate::compression::native_framing;
use crate::{
    error::{Error, Result},
    response::Chunk,
};

mod frame;
mod header;

pub(crate) struct Lz4HttpDecoder<S> {
    stream: S,
    decoder: Lz4FramePushDecoder,
}

impl<S> Lz4HttpDecoder<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: Lz4FramePushDecoder::new(),
        }
    }
}

impl<S> Stream for Lz4HttpDecoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Note(abonander): I chose not to introduce any forced yields here
        // because Lz4 is quoted at decompressing at ~5000 MB/s,
        // so the network should almost always be the bottleneck.
        //
        // If yielding turns out to be necessary, yielding every 512 KiB decompressed
        // should be around the 100-microsecond target quoted by the Tokio devs.
        loop {
            if let ControlFlow::Break(chunk) = self.decoder.drain()? {
                return Poll::Ready(Some(Ok(chunk)));
            }

            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    self.decoder.push(res?);
                    continue;
                }
                Poll::Ready(None) => {
                    self.decoder.finish()?;
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

const LZ4_MAGIC: u8 = 0x82;

pub(crate) fn compress(uncompressed: &[u8]) -> Result<Bytes> {
    let max_compressed_size = block::get_maximum_output_size(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(native_framing::META_SIZE + max_compressed_size, 0);

    let compressed_data_size =
        block::compress_into(uncompressed, &mut buffer[native_framing::META_SIZE..])
            .map_err(|err| Error::Compression(err.into()))?;

    buffer.truncate(native_framing::META_SIZE + compressed_data_size);

    native_framing::write_meta(&mut buffer, LZ4_MAGIC, uncompressed.len())?;

    Ok(buffer.freeze())
}

#[cfg(test)]
impl super::test_util::TestDecoder for Lz4HttpDecoder<()> {
    type Stream<S>
        = Lz4HttpDecoder<S>
    where
        S: Stream<Item = std::result::Result<Bytes, Error>> + Unpin;

    fn with_stream<S>(stream: S) -> Self::Stream<S>
    where
        S: Stream<Item = std::result::Result<Bytes, Error>> + Unpin,
    {
        Lz4HttpDecoder::new(stream)
    }
}

#[cfg(test)]
mod tests {
    use crate::compression::lz4::{Lz4HttpDecoder, compress};
    use crate::compression::test_util::test_decoder;
    use crate::error::Error;
    use bytes::Bytes;
    use futures_util::stream::{self, TryStreamExt};
    use twox_hash::XxHash32;

    const UNCOMPRESSED_TAXI_TRIPS: &[u8] =
        include_bytes!("../../../tests/it/fixtures/nyc-taxi_trips_0_head_1000.tsv");

    // Each block contains its wire bytes and whether its uncompressed bit is set.
    fn http_frame(flags: u8, block_size: u8, blocks: &[(&[u8], bool)]) -> Vec<u8> {
        let descriptor = [flags, block_size];
        let mut frame = vec![0x04, 0x22, 0x4d, 0x18, flags, block_size];
        frame.push((XxHash32::oneshot(0, &descriptor) >> 8) as u8);

        for &(data, uncompressed) in blocks {
            let mut length = u32::try_from(data.len()).unwrap();
            if uncompressed {
                length |= 0x8000_0000;
            }
            frame.extend_from_slice(&length.to_le_bytes());
            frame.extend_from_slice(data);
            if flags & 0x10 != 0 {
                frame.extend_from_slice(&XxHash32::oneshot(0, data).to_le_bytes());
            }
        }

        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame
    }

    async fn assert_http_frame_error(frame: &[u8], expected: &str) {
        for split in [0, 1, frame.len() / 2, frame.len() - 1, frame.len()] {
            let chunks = [&frame[..split], &frame[split..]];
            let stream = stream::iter(
                chunks
                    .into_iter()
                    .map(|data| Ok::<_, Error>(Bytes::copy_from_slice(data))),
            );
            let error = Lz4HttpDecoder::new(stream)
                .try_fold(Vec::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk.data);
                    Ok(bytes)
                })
                .await
                .expect_err("malformed LZ4 frame must be rejected");
            assert!(matches!(error, Error::Decompression(_)));
            assert!(
                error.to_string().contains(expected),
                "unexpected error at split {split}: {error}"
            );
        }
    }

    #[test]
    fn decompress_linked_three_blocks_retains_history() {
        // Includes both full 64-KiB blocks and blocks on either side of the
        // history-window size. The final block genuinely references its peers.
        for (old_size, new_size, offset) in [
            (3, 5, 8_u16),
            (32_768, 32_768, 65_535),
            (65_536, 32_768, 65_535),
            (32_768, 65_536, 65_535),
            (65_536, 65_536, 65_535),
            (65_536, 131_072, 65_535),
            (131_072, 16_384, 65_535),
        ] {
            let old = vec![b'x'; old_size];
            let new = vec![b'y'; new_size];
            let [low, high] = offset.to_le_bytes();
            // A twelve-byte match followed by five terminal literals.
            let matched = [0x08, low, high, 0x50, b't', b'a', b'i', b'l', b'!'];
            let frame = http_frame(0x40, 0x50, &[(&old, true), (&new, true), (&matched, false)]);
            let mut expected = old;
            expected.extend_from_slice(&new);
            for _ in 0..12 {
                expected.push(expected[expected.len() - usize::from(offset)]);
            }
            expected.extend_from_slice(b"tail!");

            test_decoder::<Lz4HttpDecoder<()>>(Bytes::from(frame), &expected);
        }
    }

    #[test]
    fn decompress_concatenated_linked_frames() {
        let old = vec![b'x'; 32];
        let new = vec![b'y'; 32];
        let matched = [0x08, 1, 0, 0x50, b't', b'a', b'i', b'l', b'!'];
        let mut frames = http_frame(0x40, 0x40, &[(&old, true)]);
        frames.extend(http_frame(0x40, 0x40, &[(&new, true), (&matched, false)]));
        let mut expected = old;
        expected.extend_from_slice(&new);
        expected.extend_from_slice(&[b'y'; 12]);
        expected.extend_from_slice(b"tail!");

        test_decoder::<Lz4HttpDecoder<()>>(Bytes::from(frames), &expected);
    }

    #[tokio::test]
    async fn reject_previous_linked_frame_dictionary() {
        let matched = [0x08, 1, 0, 0x50, b't', b'a', b'i', b'l', b'!'];
        let mut frames = http_frame(0x40, 0x40, &[(b"previous frame", true)]);
        frames.extend(http_frame(0x40, 0x40, &[(&matched, false)]));

        assert_http_frame_error(&frames, "offset to copy").await;
    }

    #[tokio::test]
    async fn reject_malformed_frame_checksums() {
        let mut header = http_frame(0x60, 0x40, &[(b"hello", true)]);
        header[6] ^= 1;
        assert_http_frame_error(&header, "HeaderChecksum").await;

        let compressed = [0x50, b'h', b'e', b'l', b'l', b'o'];
        let mut block = http_frame(0x70, 0x40, &[(&compressed, false)]);
        block[7 + 4 + compressed.len()] ^= 1;
        assert_http_frame_error(&block, "block checksum mismatch").await;

        let mut content = http_frame(0x64, 0x40, &[(b"hello", true)]);
        content.extend_from_slice(&(XxHash32::oneshot(0, b"hello") ^ 1).to_le_bytes());
        assert_http_frame_error(&content, "content checksum mismatch").await;
    }

    #[test]
    fn decompress_uncompressed_block_checksum() {
        let frame = http_frame(0x50, 0x40, &[(b"hello", true), (b"world", true)]);

        test_decoder::<Lz4HttpDecoder<()>>(Bytes::from(frame), b"helloworld");
    }

    #[tokio::test]
    async fn reject_uncompressed_block_checksum() {
        let mut frame = http_frame(0x50, 0x40, &[(b"hello", true)]);
        frame[7 + 4 + 5] ^= 1;
        assert_http_frame_error(&frame, "block checksum mismatch").await;

        let truncated = &frame[..7 + 4 + 5 + 3];
        assert_http_frame_error(truncated, "unexpected EOF").await;
    }

    #[test]
    fn decompress_taxi_trips() {
        test_decoder::<Lz4HttpDecoder<()>>(
            Bytes::from_static(include_bytes!("fixtures/nyc-taxi_trips.lz4")),
            UNCOMPRESSED_TAXI_TRIPS,
        );
    }

    #[test]
    fn decompress_taxi_trips_checksum() {
        test_decoder::<Lz4HttpDecoder<()>>(
            Bytes::from_static(include_bytes!("fixtures/nyc-taxi_trips_checksum.lz4")),
            UNCOMPRESSED_TAXI_TRIPS,
        );
    }

    #[test]
    fn decompress_taxi_trips_linked() {
        test_decoder::<Lz4HttpDecoder<()>>(
            Bytes::from_static(include_bytes!("fixtures/nyc-taxi_trips_linked.lz4")),
            UNCOMPRESSED_TAXI_TRIPS,
        );
    }

    #[test]
    fn decompress_taxi_trips_linked_checksum() {
        test_decoder::<Lz4HttpDecoder<()>>(
            Bytes::from_static(include_bytes!(
                "fixtures/nyc-taxi_trips_linked_checksum.lz4"
            )),
            UNCOMPRESSED_TAXI_TRIPS,
        );
    }

    #[test]
    fn it_compresses() {
        let source = vec![
            1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97,
            98, 99,
        ];

        let expected = vec![
            245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0,
            0, 0, 23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116,
            114, 105, 110, 103, 3, 97, 98, 99,
        ];

        let actual = compress(&source).unwrap();
        assert_eq!(actual, expected);
    }
}
