use crate::bytes_ext::BytesExt;
use crate::compression::lz4::header;
use crate::compression::lz4::header::{BlockInfo, BlockMode, FrameInfo};
use crate::error::Error;
use crate::response::Chunk;
use bytes::{Buf, Bytes, BytesMut};
use std::cmp;
use std::hash::Hasher;
use std::ops::ControlFlow;
use twox_hash::XxHash32;

const WINDOW_SIZE: usize = 65536;
const CHECKSUM_SIZE: usize = size_of::<u32>();

/// Push-based decoder of the Lz4 frame format.
///
/// [`lz4_flex::frame::FrameDecoder`] exists but depends on the blocking `Read` trait,
/// and its implementation is not re-entrant so returning `WouldBlock` is not enough.
pub(crate) struct Lz4FramePushDecoder {
    state: State,
    input_buffer: BytesExt,
    output_buffer: BytesMut,
    window: BytesExt,
}

enum State {
    NextFrame,
    NextBlock {
        frame: FrameInfo,
        overhead: usize,
        total_content_size: usize,
        // 256 bytes that we shouldn't store inline if we don't have to.
        content_hasher: Option<Box<XxHash32>>,
    },
    Block {
        frame: FrameInfo,
        block: BlockInfo,
        overhead: usize,
        total_content_size: usize,
        content_hasher: Option<Box<XxHash32>>,
    },
}

impl Lz4FramePushDecoder {
    pub(crate) fn new() -> Lz4FramePushDecoder {
        Self {
            state: State::NextFrame,
            input_buffer: BytesExt::default(),
            output_buffer: BytesMut::new(),
            window: BytesExt::default(),
        }
    }

    pub(crate) fn push(&mut self, chunk: Bytes) {
        self.input_buffer.extend(chunk);
    }

    pub(crate) fn drain(&mut self) -> Result<ControlFlow<Chunk, usize>, Error> {
        loop {
            match self.state {
                State::NextFrame => {
                    let mut input = self.input_buffer.slice();

                    match FrameInfo::read(&mut input) {
                        Ok(frame) => {
                            if frame.legacy_frame {
                                return Err(Error::decompression(
                                    "Lz4 frame specifies legacy format which is not supported",
                                ));
                            }

                            if let Some(dict_id) = frame.dict_id {
                                return Err(Error::decompression(format!(
                                    "Lz4 frame requests an external dictionary ({dict_id}) which is not supported"
                                )));
                            }

                            let consumed = self.input_buffer.remaining() - input.len();

                            self.state = State::NextBlock {
                                overhead: consumed,
                                total_content_size: 0,
                                content_hasher: frame.content_checksum.then(Default::default),
                                frame,
                            };
                            self.input_buffer.advance(consumed);
                        }
                        Err(header::Error::InsufficientData(len)) => {
                            return Ok(ControlFlow::Continue(len));
                        }
                        Err(other) => return Err(Error::decompression(other)),
                    }
                }
                State::NextBlock {
                    ref frame,
                    overhead,
                    total_content_size,
                    ref mut content_hasher,
                } => {
                    let mut input = self.input_buffer.slice();

                    match BlockInfo::read(&mut input) {
                        Ok(block) => {
                            let consumed = self.input_buffer.remaining() - input.len();
                            self.input_buffer.set_remaining(input.len());

                            self.state = State::Block {
                                frame: frame.clone(),
                                block,
                                overhead: overhead + consumed,
                                total_content_size,
                                content_hasher: content_hasher.take(),
                            };
                        }
                        Err(header::Error::InsufficientData(len)) => {
                            return Ok(ControlFlow::Continue(len));
                        }
                        Err(other) => return Err(Error::decompression(other)),
                    }
                }
                State::Block {
                    ref frame,
                    ref block,
                    overhead,
                    mut total_content_size,
                    ref mut content_hasher,
                } => {
                    if let BlockMode::Independent = frame.block_mode {
                        self.window.clear();
                    }

                    let max_block_size = frame.block_size.get();

                    let (input_len, data) = match *block {
                        BlockInfo::Uncompressed(len) => {
                            let len = usize::try_from(len).map_err(|_| {
                                Error::decompression(format!(
                                    "Lz4 block length out of range: {len}"
                                ))
                            })?;

                            if len > max_block_size {
                                return Err(Error::decompression(format!(
                                    "Lz4 uncompressed block size ({len}) exceeds frame's block_size ({max_block_size})"
                                )));
                            }

                            if self.input_buffer.remaining() < len {
                                return Ok(ControlFlow::Continue(
                                    len - self.input_buffer.remaining(),
                                ));
                            }

                            let content = self.input_buffer.copy_to_bytes(len);

                            if let Some(content_hasher) = content_hasher {
                                content_hasher.write(&content);
                            }

                            total_content_size += len;

                            (len, content)
                        }
                        BlockInfo::Compressed(compressed_len) => {
                            let compressed_len = usize::try_from(compressed_len).map_err(|_| {
                                Error::decompression(format!(
                                    "Lz4 block length out of range: {compressed_len}"
                                ))
                            })?;

                            if compressed_len > max_block_size {
                                return Err(Error::decompression(format!(
                                    "Lz4 compressed block size ({compressed_len}) exceeds frame's block_size ({max_block_size})"
                                )));
                            }

                            let expected_len =
                                compressed_len + if frame.block_checksums { 4 } else { 0 };

                            if self.input_buffer.remaining() < expected_len {
                                return Ok(ControlFlow::Continue(
                                    expected_len - self.input_buffer.remaining(),
                                ));
                            }

                            if max_block_size > self.output_buffer.len() {
                                // FIXME: `lz4_flex` has no way to decompress into an uninit buffer
                                // https://github.com/PSeitz/lz4_flex/pull/195
                                self.output_buffer.resize(max_block_size, 0);
                            }

                            let (compressed_data, mut rest) =
                                self.input_buffer.slice().split_at(compressed_len);

                            let len = lz4_flex::block::decompress_into_with_dict(
                                compressed_data,
                                &mut self.output_buffer,
                                self.window.slice(),
                            )
                            .map_err(Error::decompression)?;

                            if frame.block_checksums {
                                let expected_checksum = rest.try_get_u32_le().map_err(|_| {
                                    Error::decompression("expected 4-byte checksum after Lz4 block")
                                })?;

                                let actual_checksum = XxHash32::oneshot(0, compressed_data);

                                if expected_checksum != actual_checksum {
                                    return Err(Error::decompression(format!(
                                        "Lz4 block checksum mismatch; expected: {expected_checksum:#x}, actual: {actual_checksum:#x}"
                                    )));
                                }
                            }

                            self.input_buffer.advance(expected_len);

                            let content = self.output_buffer.split_to(len).freeze();

                            if let Some(content_hasher) = content_hasher {
                                content_hasher.write(&content);
                            }

                            total_content_size += len;

                            (compressed_len, content)
                        }
                        BlockInfo::EndMark => {
                            if let Some(content_hasher) = content_hasher {
                                let Ok(expected_checksum) = self.input_buffer.try_get_u32_le()
                                else {
                                    return Ok(ControlFlow::Continue(
                                        CHECKSUM_SIZE - self.input_buffer.remaining(),
                                    ));
                                };

                                let actual_checksum = content_hasher.finish_32();

                                if expected_checksum != actual_checksum {
                                    return Err(Error::decompression(format!(
                                        "Lz4 content checksum mismatch; expected: {expected_checksum:#x}, actual: {actual_checksum:#x}"
                                    )));
                                }
                            }

                            if let Some(expected_content_size) = frame.content_size
                                && expected_content_size != total_content_size as u64
                            {
                                return Err(Error::decompression(format!(
                                    "Lz4 content size mismatch; expected: {expected_content_size}, actual: {total_content_size}"
                                )));
                            }

                            self.state = State::NextFrame;
                            continue;
                        }
                    };

                    if let BlockMode::Linked = frame.block_mode {
                        let mut advance_amt =
                            (self.window.remaining() + data.len()).saturating_sub(WINDOW_SIZE);

                        self.window
                            .advance(cmp::min(advance_amt, self.window.remaining()));

                        advance_amt = advance_amt.saturating_sub(self.window.remaining());

                        self.window.extend(data.slice(advance_amt..));
                    }

                    self.state = State::NextBlock {
                        frame: frame.clone(),
                        overhead: 0,
                        total_content_size,
                        content_hasher: content_hasher.take(),
                    };

                    return Ok(ControlFlow::Break(Chunk {
                        net_size: overhead + input_len,
                        data,
                    }));
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        if self.input_buffer.remaining() != 0 {
            return Err(Error::decompression("unconsumed data in Lz4 frame buffer"));
        }

        match &self.state {
            State::NextFrame => Ok(()),
            State::NextBlock { .. } => Err(Error::decompression(
                "unexpected EOF in Lz4 frame stream: expected next block or EndMark",
            )),
            State::Block { block, .. } => match *block {
                BlockInfo::Uncompressed(len) | BlockInfo::Compressed(len) => {
                    Err(Error::decompression(format!(
                        "unexpected EOF in Lz4 frame stream: expected {len} bytes for next block, got {}",
                        self.input_buffer.remaining()
                    )))
                }
                BlockInfo::EndMark => Err(Error::decompression(format!(
                    "unexpected EOF in Lz4 frame stream: expected {CHECKSUM_SIZE} bytes for content checksum, got {}",
                    self.input_buffer.remaining()
                ))),
            },
        }
    }
}
