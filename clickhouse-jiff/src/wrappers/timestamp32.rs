use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::Timestamp`]
/// that corresponds to `DateTime` in ClickHouse.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timestamp32(jiff::Timestamp);

/// An error that occurs when [`jiff::Timestamp`]
/// cannot be represented as a `DateTime64` in ClickHouse.
#[derive(Debug)]
pub struct Timestamp32OutOfRange;

impl fmt::Display for Timestamp32OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Timestamp is out of range, valid values are [1970-01-01 00:00:00, 2106-02-07 06:28:15].")
    }
}

impl error::Error for Timestamp32OutOfRange {}

const FROM: jiff::Timestamp = jiff::Timestamp::UNIX_EPOCH;
const TO: jiff::Timestamp = jiff::Timestamp::constant(u32::MAX as i64, 0);

impl Timestamp32 {
    /// Convert `Timestamp32` to the underlying type.
    #[inline]
    pub fn into_inner(self) -> jiff::Timestamp {
        self.0
    }
}

impl TryFrom<jiff::Timestamp> for Timestamp32 {
    type Error = Timestamp32OutOfRange;
    fn try_from(value: jiff::Timestamp) -> Result<Self, Self::Error> {
        if value >= FROM && value <= TO {
            Ok(Self(value))
        } else {
            Err(Timestamp32OutOfRange)
        }
    }
}

impl From<Timestamp32> for jiff::Timestamp {
    #[inline]
    fn from(value: Timestamp32) -> Self {
        value.0
    }
}

impl Serialize for Timestamp32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // `FROM` is defined as the Unix epoch,
        // `TO` is defined as u32::MAX seconds since the epoch.
        //
        // Therefore, it is fine to cast as_second() to u32;
        let ts = self.0.as_second() as u32;
        ts.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde_core::Deserializer<'de>,
    {
        struct TsVisitor;

        impl<'de> Visitor<'de> for TsVisitor {
            type Value = Timestamp32;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u32 representation of a ClickHouse DateTime")
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: serde_core::de::Error,
            {
                let ts = jiff::Timestamp::from_second(v as i64)
                    .map_err(|_| E::custom("Failed to create a jiff::Timestamp"))?;
                Ok(Timestamp32(ts))
            }
        }

        deserializer.deserialize_u32(TsVisitor)
    }
}
