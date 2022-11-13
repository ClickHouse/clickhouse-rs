//! Contains ser/de modules for different external types.

use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};

/// Handles [`std::net::Ipv4Addr`].
pub mod ipv4 {
    use std::net::Ipv4Addr;

    use super::*;

    pub fn serialize<S>(ipv4: &Ipv4Addr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u32::from(*ipv4).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Ipv4Addr, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ip: u32 = Deserialize::deserialize(deserializer)?;
        Ok(Ipv4Addr::from(ip))
    }
}

/// Handles [`::uuid::Uuid`].
#[cfg(feature = "uuid")]
pub mod uuid {
    use std::mem;

    use ::uuid::Uuid;

    use super::*;

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
}

/// Handles [`::time::OffsetDateTime`].
#[cfg(feature = "time")]
pub mod time {
    use ::time::OffsetDateTime;
    use serde::{de::Error as _, ser::Error as _};

    use super::*;

    pub mod datetime {
        use super::*;

        pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ts = dt.unix_timestamp();

            if ts < 0 || ts > i64::from(u32::MAX) {
                let msg = format!("{dt} cannot be represented as DateTime");
                return Err(S::Error::custom(msg));
            }

            (ts as u32).serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
        where
            D: Deserializer<'de>,
        {
            let ts: u32 = Deserialize::deserialize(deserializer)?;
            OffsetDateTime::from_unix_timestamp(i64::from(ts)).map_err(D::Error::custom)
        }
    }
}
