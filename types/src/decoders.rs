use crate::error::TypesError;
use crate::leb128::read_leb128;
use bytes::Buf;

#[inline]
pub(crate) fn decode_string(buffer: &mut &[u8]) -> Result<String, TypesError> {
    let length = read_leb128(buffer)? as usize;
    if length == 0 {
        return Ok("".to_string());
    }
    if buffer.remaining() < length {
        return Err(TypesError::NotEnoughData(format!(
            "decoding string, {} bytes remaining, {} bytes required",
            buffer.remaining(),
            length,
        )));
    }
    let result = String::from_utf8_lossy(&buffer.copy_to_bytes(length)).to_string();
    Ok(result)
}
