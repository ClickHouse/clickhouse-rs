use bytes::{Buf, BufMut};
use std::ops::ControlFlow;

/// Write a `usize` as a native `VarUInt`
#[allow(clippy::cast_possible_truncation)] // truncation is intentional here
pub(super) fn write(mut buf: impl BufMut, mut uint: usize) {
    while uint > 0x7F {
        let b = (uint as u8) | 0x80;
        buf.put_u8(b);
        uint >>= 7;
    }

    buf.put_u8(uint as u8);
}

#[derive(Default)]
pub(super) struct ParseVarUInt {
    accumulator: u64,
    shift: u32,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ParseVarUIntError {
    #[error("VarUInt repr overflowed: {accumulator:#x} byte: {byte:#02x}")]
    Overflow { accumulator: u64, byte: u8 },
    #[error("terminating byte missing in VarUInt encoding: {accumulator:#x}")]
    MissingTerminator { accumulator: u64 },
}

impl ParseVarUInt {
    pub(super) fn feed(
        &mut self,
        mut buf: impl Buf,
    ) -> Result<ControlFlow<u64>, ParseVarUIntError> {
        const MAX_LEN: usize = 10;

        for _ in 0..MAX_LEN {
            let Ok(b) = buf.try_get_u8() else {
                return Ok(ControlFlow::Continue(()));
            };

            self.accumulator |=
                (b as u64 & 0x7F)
                    .checked_shl(self.shift)
                    .ok_or(ParseVarUIntError::Overflow {
                        accumulator: self.accumulator,
                        byte: b,
                    })?;

            if b <= 0x7F {
                return Ok(ControlFlow::Break(self.accumulator));
            }

            self.shift += 7;
        }

        Err(ParseVarUIntError::MissingTerminator {
            accumulator: self.accumulator,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::native::varuint::ParseVarUInt;
    use bytes::Buf;
    use std::ops::ControlFlow;
    const ENCODED_AND_DECODED: &[(&[u8], u64)] = &[
        (&[0], 0u64),
        (&[1], 1),
        (&[127], 127),
        (&[0x80, 0x01], 1 << 7),
        (&[0x80, 0x80, 0x01], 1 << 14),
        (&[0x80, 0x80, 0x80, 0x01], 1 << 21),
        (&[0x80, 0x80, 0x80, 0x80, 0x01], 1 << 28),
        (&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F], 0xFF_FF_FF_FF_FF),
    ];

    #[test]
    fn parse_varuint() {
        let pad_len = 16usize;

        for (encoded, decoded) in ENCODED_AND_DECODED {
            // Pad with junk data that must be ignored
            let padded: Vec<_> = encoded
                .iter()
                .copied()
                .chain((0..).cycle())
                .take(pad_len)
                .collect();

            // Test feeding slices in different size chunks
            for chunk_size in 1..=padded.len() {
                let mut parser = ParseVarUInt::default();

                let mut slice = &padded[..];

                let mut last_remaining = slice.len();

                loop {
                    match parser.feed((&mut slice).take(chunk_size)) {
                        Ok(ControlFlow::Break(res)) => {
                            assert_eq!(
                                res, *decoded,
                                "invalid decoding; chunk_size: {chunk_size}, padded: {padded:?}, remaining: {slice:?}"
                            );
                            assert_eq!(
                                slice.len(),
                                padded.len() - encoded.len(),
                                "extra data consumed: {slice:?}"
                            );
                            break;
                        }
                        Ok(ControlFlow::Continue(())) => {
                            assert!(
                                !slice.is_empty(),
                                "full slice consumed without giving a result"
                            );
                            assert_ne!(
                                slice.len(),
                                last_remaining,
                                "parser failed to make progress"
                            );
                            last_remaining = slice.len();
                        }
                        Err(e) => {
                            panic!(
                                "error: {e:?}, chunk_size: {chunk_size}, padded: {padded:?}, remaining: {slice:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn write_varuint() {
        let mut buf = Vec::with_capacity(10);

        for (encoded, decoded) in ENCODED_AND_DECODED {
            buf.clear();

            super::write(&mut buf, (*decoded).try_into().unwrap());

            assert_eq!(*buf, **encoded, "decoded: {decoded}");
        }
    }
}
