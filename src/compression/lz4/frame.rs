use crate::compression::lz4::header::FrameInfo;
use crate::error::Error;
use bytes::{Bytes, BytesMut};
use lz4_flex::frame::FrameInfo;

/// Push-based decoder of the Lz4 frame format.
///
/// [`lz4_flex::frame::FrameDecoder`] exists but depends on the blocking `Read` trait,
/// and its implementation is not re-entrant so returning `WouldBlock` is not enough.
pub struct Lz4FramePushDecoder {
    current_frame: Option<FrameInfo>,
    input_buffer: BytesMut,
    output_buffer: BytesMut,
    dict: BytesMut,
}

impl Lz4FramePushDecoder {
    pub fn new() -> Lz4FramePushDecoder {
        Self {
            current_frame: None,
            input_buffer: BytesMut::with_capacity(8192),
            output_buffer: BytesMut::with_capacity(8192),
            dict: BytesMut::new(),
        }
    }

    pub fn push_chunk(&mut self, bytes: Bytes) -> Result<Bytes, Error> {}
}
