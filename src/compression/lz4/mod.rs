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
    use bytes::Bytes;

    const UNCOMPRESSED_TAXI_TRIPS: &[u8] =
        include_bytes!("../../../tests/it/fixtures/nyc-taxi_trips_0_head_1000.tsv");

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
