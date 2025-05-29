use crate::error::TypesError;
use crate::leb128::read_leb128;
use bytes::Buf;

#[inline]
pub(crate) fn read_string(buffer: &mut &[u8]) -> Result<String, TypesError> {
    ensure_size(buffer, 1)?;
    let length = read_leb128(buffer)? as usize;
    if length == 0 {
        return Ok("".to_string());
    }
    ensure_size(buffer, length)?;
    let result = String::from_utf8_lossy(&buffer.copy_to_bytes(length)).to_string();
    Ok(result)
}

#[inline]
pub(crate) fn ensure_size(buffer: &[u8], size: usize) -> Result<(), TypesError> {
    // println!("[ensure_size] buffer remaining: {}, required size: {}", buffer.len(), size);
    if buffer.remaining() < size {
        Err(TypesError::NotEnoughData(format!(
            "expected at least {} bytes, but only {} bytes remaining",
            size,
            buffer.remaining()
        )))
    } else {
        Ok(())
    }
}
