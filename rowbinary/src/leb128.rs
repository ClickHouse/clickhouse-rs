use std::io::{ErrorKind, Read};

use crate::error::ColumnsParserError;

pub fn decode_leb128<R: Read>(pos: &mut usize, reader: &mut R) -> Result<u64, ColumnsParserError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    let mut buf = [0u8; 1];

    loop {
        reader.read_exact(&mut buf).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                ColumnsParserError::InvalidIntegerEncoding(*pos)
            } else {
                ColumnsParserError::IoError(e)
            }
        })?;

        *pos += 1;

        let byte = buf[0];
        result |= ((byte & 0x7f) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;

        if shift > 63 {
            return Err(ColumnsParserError::InvalidIntegerEncoding(*pos));
        }
    }

    Ok(result)
}

pub fn encode_leb128(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut val = value;

    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;

        if val != 0 {
            byte |= 0x80; // Set high bit to indicate more bytes follow
        }

        result.push(byte);

        if val == 0 {
            break;
        }
    }

    result
}

mod tests {
    #[test]
    fn test_decode_leb128() {
        let test_cases = vec![
            // (input bytes, expected value)
            (vec![0], 0),
            (vec![1], 1),
            (vec![127], 127),
            (vec![128, 1], 128),
            (vec![255, 1], 255),
            (vec![0x85, 0x91, 0x26], 624773),
            (vec![0xE5, 0x8E, 0x26], 624485),
        ];

        for (input, expected) in test_cases {
            let mut cursor = std::io::Cursor::new(input.clone());
            let mut pos = 0;
            let result = super::decode_leb128(&mut pos, &mut cursor).unwrap();
            assert_eq!(result, expected, "Failed decoding {:?}", input);
        }
    }

    #[test]
    fn test_encode_decode_leb128() {
        let test_values = vec![
            0u64,
            1,
            127,
            128,
            255,
            624773,
            624485,
            300_000,
            10_000_000,
            u32::MAX as u64,
            (u32::MAX as u64) + 1,
        ];

        for value in test_values {
            let encoded = super::encode_leb128(value);
            let mut cursor = std::io::Cursor::new(&encoded);
            let mut pos = 0;
            let decoded = super::decode_leb128(&mut pos, &mut cursor).unwrap();

            assert_eq!(
                decoded, value,
                "Failed round trip for {}: encoded as {:?}, decoded as {}",
                value, encoded, decoded
            );
        }
    }
}
