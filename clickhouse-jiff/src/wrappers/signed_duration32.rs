use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::SignedDuration`]
/// that corresponds to `Time` in ClickHouse.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SignedDuration32(jiff::SignedDuration);

/// An error that occurs when [`jiff::SignedDuration`]
/// cannot be represented as a `Time` in ClickHouse.
#[derive(Debug)]
pub struct SignedDuration32OutOfRange;

impl fmt::Display for SignedDuration32OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignedDuration is out of range, valid values are [-999:59:59, 999:59:59].")
    }
}

impl error::Error for SignedDuration32OutOfRange {}

const FROM: jiff::SignedDuration = jiff::SignedDuration::new(-3599999, 0);
const TO: jiff::SignedDuration = jiff::SignedDuration::new(3599999, 0);

impl SignedDuration32 {
    /// Convert `SignedDuration32` to the underlying type.
    #[inline]
    pub fn into_inner(self) -> jiff::SignedDuration {
        self.0
    }
}

impl TryFrom<jiff::SignedDuration> for SignedDuration32 {
    type Error = SignedDuration32OutOfRange;
    fn try_from(value: jiff::SignedDuration) -> Result<Self, Self::Error> {
        if value >= FROM && value <= TO {
            Ok(Self(value))
        } else {
            Err(SignedDuration32OutOfRange)
        }
    }
}

impl From<SignedDuration32> for jiff::SignedDuration {
    #[inline]
    fn from(value: SignedDuration32) -> Self {
        value.0
    }
}

impl Serialize for SignedDuration32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Possible values are [-3599999, 3599999], which do fit in an i32.
        let ts = self.0.as_secs() as i32;
        ts.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SignedDuration32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde_core::Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = SignedDuration32;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an i32 representation of a ClickHouse Time")
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde_core::de::Error,
            {
                let ts = jiff::SignedDuration::from_secs(v.into());
                Ok(SignedDuration32(ts))
            }
        }

        deserializer.deserialize_i32(DurationVisitor)
    }
}
