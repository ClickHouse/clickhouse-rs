use crate::bytes_ext::BytesExt;
use crate::compression::lz4::header;
use crate::compression::lz4::header::{BlockInfo, BlockMode, FrameInfo};
use crate::error::Error;
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
    NextBlock { frame: FrameInfo },
    InBlock { frame: FrameInfo, block: BlockInfo },
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

    pub fn drain(&mut self) -> Result<ControlFlow<Bytes, usize>, Error> {
        loop {
            match &self.state {
                State::NextFrame => {
                    let mut input = self.input_buffer.slice();

                    match FrameInfo::read(&mut input) {
                        Ok(frame) => {
                            self.state = State::NextBlock { frame };
                            self.input_buffer.set_remaining(input.len());
                        }
                        Err(header::Error::InsufficientData(len)) => {
                            return Ok(ControlFlow::Continue(len));
                        }
                        Err(other) => return Err(Error::decompression(other)),
                    }
                }
                State::NextBlock { frame } => {
                    let mut input = self.input_buffer.slice();

                    match BlockInfo::read(&mut input) {
                        Ok(block) => {
                            self.state = State::InBlock {
                                frame: frame.clone(),
                                block,
                            };
                            self.input_buffer.set_remaining(input.len());
                        }
                        Err(header::Error::InsufficientData(len)) => {
                            return Ok(ControlFlow::Continue(len));
                        }
                        Err(other) => return Err(Error::decompression(other)),
                    }
                }
                State::InBlock { frame, block } => {
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

                    if let BlockMode::Independent = frame.block_mode {
                        self.window.clear();
                    }

                    let decompressed = match *block {
                        BlockInfo::Uncompressed(len) => {
                            let len = usize::try_from(len).map_err(|_| {
                                Error::decompression(format!(
                                    "Lz4 block length out of range: {len}"
                                ))
                            })?;

                            if self.input_buffer.remaining() < len {
                                return Ok(ControlFlow::Continue(
                                    len - self.input_buffer.remaining(),
                                ));
                            }

                            self.input_buffer.copy_to_bytes(len)
                        }
                        BlockInfo::Compressed(len) => {
                            let len = usize::try_from(len).map_err(|_| {
                                Error::decompression(format!(
                                    "Lz4 block length out of range: {len}"
                                ))
                            })?;

                            if self.input_buffer.remaining() < len {
                                return Ok(ControlFlow::Continue(
                                    len - self.input_buffer.remaining(),
                                ));
                            }

                            let max_block_size = frame.block_size.get();

                            if max_block_size > self.output_buffer.len() {
                                // FIXME: `lz4_flex` has no way to decompress into an uninit buffer
                                // https://github.com/PSeitz/lz4_flex/pull/195
                                self.output_buffer.resize(max_block_size, 0);
                            }

                            let len = lz4_flex::block::decompress_into_with_dict(
                                &self.input_buffer.slice()[..len],
                                &mut self.output_buffer,
                                self.window.slice(),
                            )
                            .map_err(Error::decompression)?;

                            self.output_buffer.split_to(len).freeze()
                        }
                        BlockInfo::EndMark => {
                            self.state = State::NextFrame;
                            continue;
                        }
                    };

                    if let BlockMode::Linked = frame.block_mode {
                        let mut advance_amt = (self.window.remaining() + decompressed.len())
                            .saturating_sub(WINDOW_SIZE);

                        self.window
                            .advance(cmp::min(advance_amt, self.window.remaining()));

                        advance_amt = advance_amt.saturating_sub(self.window.remaining());

                        self.window.extend(decompressed.slice(advance_amt..));
                    }

                    self.state = State::NextBlock {
                        frame: frame.clone(),
                    };

                    return Ok(ControlFlow::Break(decompressed));
                }
            }
        }
    }
}
