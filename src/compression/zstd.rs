use std::pin::Pin;
use std::task::{Context, Poll, ready};

use crate::compression::native_framing;
use crate::error::{Error, Result};
use crate::response::Chunk;
use bytes::{Bytes, BytesMut};
use futures_util::stream::Stream;
use zstd::stream::raw::Operation;

const OUTPUT_BUFFER_SIZE: usize = 64 * 1024;

// ClickHouse native compression framing.
const ZSTD_MAGIC: u8 = 0x90;

pub(crate) fn compress(uncompressed: &[u8], level: Option<i32>) -> Result<Bytes> {
    let level = level.unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
    let max_compressed_size = zstd::zstd_safe::compress_bound(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(native_framing::META_SIZE + max_compressed_size, 0);

    let compressed_data_size = zstd::zstd_safe::compress(
        &mut buffer[native_framing::META_SIZE..],
        uncompressed,
        level,
    )
    .map_err(|code| Error::Compression(zstd::zstd_safe::get_error_name(code).into()))?;

    buffer.truncate(native_framing::META_SIZE + compressed_data_size);

    native_framing::write_meta(&mut buffer, ZSTD_MAGIC, uncompressed.len())?;

    Ok(buffer.freeze())
}

/// Streaming decoder for HTTP-level `Content-Encoding: zstd` responses.
/// Does not expect ClickHouse's native compression framing.
pub(crate) struct ZstdHttpDecoder<S> {
    stream: S,
    decoder: zstd::stream::raw::Decoder<'static>,
    input: BytesMut,
    output: BytesMut,
    stream_ended: bool,
}

impl<S> ZstdHttpDecoder<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: zstd::stream::raw::Decoder::new().expect("failed to create ZSTD decoder"),
            input: BytesMut::new(),
            output: BytesMut::zeroed(OUTPUT_BUFFER_SIZE),
            stream_ended: false,
        }
    }
}

impl<S> Stream for ZstdHttpDecoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Try to decompress any buffered input.
            if !this.input.is_empty() {
                // Ensure the output buffer has enough initialized space.
                let spare = this.output.capacity() - this.output.len();
                if spare < OUTPUT_BUFFER_SIZE {
                    this.output.reserve(OUTPUT_BUFFER_SIZE - spare);
                }
                this.output.resize(OUTPUT_BUFFER_SIZE, 0);

                let status = this
                    .decoder
                    .run_on_buffers(&this.input, &mut this.output)
                    .map_err(|err| Error::Decompression(err.into()))?;

                let net_size = status.bytes_read;
                let bytes_written = status.bytes_written;

                let _ = this.input.split_to(net_size);

                if bytes_written > 0 {
                    let data = this.output.split_to(bytes_written).freeze();
                    return Poll::Ready(Some(Ok(Chunk { data, net_size })));
                }

                // ZSTD consumed input but produced no output yet; need more data.
            }

            if this.stream_ended {
                return Poll::Ready(None);
            }

            // Pull more data from the inner stream.
            match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(chunk)) => {
                    this.input.extend_from_slice(&chunk);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    this.stream_ended = true;

                    // Flush any remaining buffered output from the decoder.
                    if !this.input.is_empty() {
                        return Poll::Ready(Some(Err(Error::Decompression(
                            "unexpected end of ZSTD stream".into(),
                        ))));
                    }

                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[cfg(test)]
async fn decode_http_zstd(chunks: &[&[u8]]) -> Result<Vec<u8>> {
    use futures_util::stream::{self, TryStreamExt};

    let stream = stream::iter(
        chunks
            .iter()
            .map(|chunk| Ok::<_, Error>(Bytes::copy_from_slice(chunk))),
    );
    let mut decoder = ZstdHttpDecoder::new(stream);
    let mut decoded = Vec::new();
    while let Some(chunk) = decoder.try_next().await? {
        decoded.extend_from_slice(&chunk.data);
    }
    Ok(decoded)
}

#[cfg(test)]
async fn assert_http_zstd_frame(compressed: &[u8], expected: &[u8]) {
    for split in 0..=compressed.len() {
        let (left, right) = compressed.split_at(split);
        let actual = decode_http_zstd(&[left, right]).await.unwrap();
        assert_eq!(actual, expected, "unexpected output at split {split}");
    }
}

#[cfg(test)]
async fn assert_http_zstd_error(compressed: &[u8]) {
    for split in 0..=compressed.len() {
        let (left, right) = compressed.split_at(split);
        let error = decode_http_zstd(&[left, right])
            .await
            .expect_err("malformed ZSTD frame must be rejected after draining to EOF");
        assert!(
            matches!(error, Error::Decompression(_)),
            "unexpected error at split {split}: {error}"
        );
    }
}

#[cfg(test)]
const HTTP_ZSTD_PLAIN: &[u8] = &[0x44, 0x33, 0x22, 0x11, 0, 0, 0, 0];

#[cfg(test)]
const HTTP_ZSTD_COMPLETE: &[u8] = &[
    0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x08, 0x41, 0, 0, 0x44, 0x33, 0x22, 0x11, 0, 0, 0, 0,
];

#[cfg(test)]
const HTTP_ZSTD_EMPTY: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd, 0x20, 0, 0x01, 0, 0];

#[tokio::test]
async fn it_decompresses_http_zstd() {
    let original = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    // Compress with raw ZSTD (no ClickHouse framing).
    let compressed = zstd::bulk::compress(&original, zstd::DEFAULT_COMPRESSION_LEVEL)
        .expect("failed to compress");

    assert_http_zstd_frame(&compressed, &original).await;
}

#[tokio::test]
async fn it_rejects_nonfinal_http_zstd_frame_at_eof() {
    assert_http_zstd_frame(HTTP_ZSTD_COMPLETE, HTTP_ZSTD_PLAIN).await;
    let decoded = decode_http_zstd(&[HTTP_ZSTD_COMPLETE]).await.unwrap();
    assert_eq!(u64::from_le_bytes(decoded.try_into().unwrap()), 0x1122_3344);

    let mut nonfinal = HTTP_ZSTD_COMPLETE.to_vec();
    nonfinal[6] = 0x40;
    assert_http_zstd_error(&nonfinal).await;
}

#[tokio::test]
async fn it_decompresses_empty_and_concatenated_http_zstd_frames() {
    assert!(decode_http_zstd(&[]).await.unwrap().is_empty());
    assert_http_zstd_frame(HTTP_ZSTD_EMPTY, &[]).await;

    let frames = [HTTP_ZSTD_COMPLETE, HTTP_ZSTD_COMPLETE].concat();
    let expected = [HTTP_ZSTD_PLAIN, HTTP_ZSTD_PLAIN].concat();
    assert_http_zstd_frame(&frames, &expected).await;

    let frames = [HTTP_ZSTD_EMPTY, HTTP_ZSTD_COMPLETE].concat();
    assert_http_zstd_frame(&frames, HTTP_ZSTD_PLAIN).await;
}

#[tokio::test]
async fn it_rejects_nonfinal_http_zstd_frame_after_empty_frame() {
    let mut nonfinal = HTTP_ZSTD_COMPLETE.to_vec();
    nonfinal[6] = 0x40;
    let frames = [HTTP_ZSTD_EMPTY, &nonfinal].concat();

    assert_http_zstd_error(&frames).await;
}

#[tokio::test]
async fn it_rejects_malformed_http_zstd_block() {
    let mut malformed = HTTP_ZSTD_COMPLETE.to_vec();
    // Block type 3 is reserved; retain the final bit and the eight-byte size.
    malformed[6] = 0x47;

    assert_http_zstd_error(&malformed).await;
}

#[tokio::test]
async fn it_validates_http_zstd_checksum() {
    let mut compressor = zstd::bulk::Compressor::new(zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
    compressor.include_checksum(true).unwrap();
    let mut compressed = compressor.compress(HTTP_ZSTD_PLAIN).unwrap();
    assert_http_zstd_frame(&compressed, HTTP_ZSTD_PLAIN).await;

    *compressed.last_mut().unwrap() ^= 1;
    assert_http_zstd_error(&compressed).await;
}

#[tokio::test]
async fn it_decompresses_large_http_zstd_frame() {
    let original: Vec<_> = (0..3 * 65_536 + 17)
        .map(|index| (index % 251) as u8)
        .collect();
    let compressed = zstd::bulk::compress(&original, zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
    assert_eq!(decode_http_zstd(&[&compressed]).await.unwrap(), original);

    for split in [1, compressed.len() / 2, compressed.len() - 1] {
        let (left, right) = compressed.split_at(split);
        assert_eq!(
            decode_http_zstd(&[left, right]).await.unwrap(),
            original,
            "unexpected large output at split {split}"
        );
    }
}

#[test]
fn it_compresses_and_decompresses() {
    let source = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let compressed = compress(&source, None).unwrap();

    // Verify the magic byte.
    assert_eq!(compressed[native_framing::CHECKSUM_SIZE], ZSTD_MAGIC);

    // Verify decompression of the payload.
    let decompressed =
        zstd::bulk::decompress(&compressed[native_framing::META_SIZE..], source.len()).unwrap();
    assert_eq!(decompressed, source);
}
