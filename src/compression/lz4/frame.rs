use crate::bytes_ext::BytesExt;
use crate::compression::lz4::header;
use crate::compression::lz4::header::{BlockInfo, BlockMode, FrameInfo};
use crate::error::Error;
use crate::response::Chunk;
use bytes::{Buf, Bytes, BytesMut};
use std::cmp;
use std::ops::ControlFlow;

const WINDOW_SIZE: usize = 65536;

/// Push-based decoder of the Lz4 frame format.
///
/// [`lz4_flex::frame::FrameDecoder`] exists but depends on the blocking `Read` trait,
/// and its implementation is not re-entrant so returning `WouldBlock` is not enough.
pub struct Lz4FramePushDecoder {
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
    },
    Block {
        frame: FrameInfo,
        block: BlockInfo,
        overhead: usize,
    },
}

impl Lz4FramePushDecoder {
    pub fn new() -> Lz4FramePushDecoder {
        Self {
            state: State::NextFrame,
            input_buffer: BytesExt::default(),
            output_buffer: BytesMut::new(),
            window: BytesExt::default(),
        }
    }

    pub fn push(&mut self, chunk: Bytes) {
        self.input_buffer.extend(chunk);
    }

    pub fn drain(&mut self) -> Result<ControlFlow<Chunk, usize>, Error> {
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
                                frame,
                                overhead: consumed,
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
                } => {
                    let mut input = self.input_buffer.slice();

                    match BlockInfo::read(&mut input) {
                        Ok(block) => {
                            let consumed = self.input_buffer.remaining() - input.len();

                            self.state = State::Block {
                                frame: frame.clone(),
                                block,
                                overhead: overhead + consumed,
                            };
                            self.input_buffer.set_remaining(input.len());
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

                            (len, self.input_buffer.copy_to_bytes(len))
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

                            if self.input_buffer.remaining() < compressed_len {
                                return Ok(ControlFlow::Continue(
                                    compressed_len - self.input_buffer.remaining(),
                                ));
                            }

                            if max_block_size > self.output_buffer.len() {
                                // FIXME: `lz4_flex` has no way to decompress into an uninit buffer
                                // https://github.com/PSeitz/lz4_flex/pull/195
                                self.output_buffer.resize(max_block_size, 0);
                            }

                            let len = lz4_flex::block::decompress_into_with_dict(
                                &self.input_buffer.slice()[..compressed_len],
                                &mut self.output_buffer,
                                self.window.slice(),
                            )
                            .map_err(Error::decompression)?;

                            self.input_buffer.advance(compressed_len);

                            (compressed_len, self.output_buffer.split_to(len).freeze())
                        }
                        BlockInfo::EndMark => {
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
                    };

                    return Ok(ControlFlow::Break(Chunk {
                        net_size: overhead + input_len,
                        data,
                    }));
                }
            }
        }
    }

    pub fn finish(&mut self) -> Result<(), Error> {
        if self.input_buffer.remaining() != 0 {
            return Err(Error::decompression("unconsumed data in Lz4 frame buffer"));
        }

        match &self.state {
            State::NextFrame => Ok(()),
            State::NextBlock { .. } => Err(Error::decompression(
                "unexpected EOF in Lz4 frame stream: expected next block or EndMark",
            )),
            State::Block { block, .. } => {
                let len = match *block {
                    BlockInfo::Uncompressed(len) => len,
                    BlockInfo::Compressed(len) => len,
                    BlockInfo::EndMark => unreachable!(),
                };

                Err(Error::decompression(format!(
                    "unexpected EOF in Lz4 frame stream: expected {len} bytes for next block, got {}",
                    self.input_buffer.remaining()
                )))
            }
        }
    }
}
