use std::mem;

use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};
use uuid::Uuid;

pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = uuid.into_bytes();
    transform(&mut bytes);
    bytes.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    let mut bytes: [u8; 16] = Deserialize::deserialize(deserializer)?;
    transform(&mut bytes);
    Ok(Uuid::from_bytes(bytes))
}

/// Swaps bytes inside both 8-byte words of UUID.
/// * Input:   0 1 2 3 4 5 6 7   8 9 10 a b c  d e f
/// * Output:  7 6 5 4 3 2 1 0   f e  d c b a 10 9 8
fn transform(bytes: &mut [u8; 16]) {
    let words = unsafe { mem::transmute::<&mut [u8; 16], &mut [u64; 2]>(bytes) };
    words[0] = words[0].swap_bytes();
    words[1] = words[1].swap_bytes();
}
