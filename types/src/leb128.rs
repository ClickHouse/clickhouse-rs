use crate::error::ParserError;
use crate::error::ParserError::NotEnoughData;
use bytes::Buf;

pub fn decode_leb128(buffer: &mut &[u8]) -> Result<u64, ParserError> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        if buffer.remaining() < 1 {
            return Err(NotEnoughData(
                "decoding LEB128, 0 bytes remaining".to_string(),
            ));
        }
        let byte = buffer.get_u8();
        value |= (byte as u64 & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 57 {
            return Err(NotEnoughData("decoding LEB128, invalid shift".to_string()));
        }
    }
    Ok(value)
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
            let result = super::decode_leb128(&mut input.as_slice()).unwrap();
            assert_eq!(result, expected, "Failed decoding {:?}", input);
        }
    }

    #[test]
    fn test_encode_decode_leb128() {
        fn encode_leb128<'a>(value: u64) -> Vec<u8> {
            let mut result = Vec::new();
            let mut val = value;
            loop {
                let mut byte = (val & 0x7f) as u8;
                val >>= 7;
                if val != 0 {
                    byte |= 0x80;
                }
                result.push(byte);
                if val == 0 {
                    break;
                }
            }
            result
        }

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
            let encoded = encode_leb128(value);
            let decoded = super::decode_leb128(&mut encoded.as_slice()).unwrap();

            assert_eq!(
                decoded, value,
                "Failed round trip for {}: encoded as {:?}, decoded as {}",
                value, encoded, decoded
            );
        }
    }
}
