use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::Timestamp`]
/// that corresponds to `DateTime64` in ClickHouse.
///
/// Supported precision:
/// * Timestamp64<0> - seconds;
/// * Timestamp64<3> - milliseconds;
/// * Timestamp64<6> - microseconds;
/// * Timestamp64<9> - nanoseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timestamp64<const PRECISION: u8>(jiff::Timestamp);

/// An error that occurs when [`jiff::Timestamp`]
/// cannot be represented as a `DateTime64` in ClickHouse.
#[derive(Debug)]
pub struct Timestamp64OutOfRange;

impl fmt::Display for Timestamp64OutOfRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // f.write_str("Timestamp is out of range, valid values are [1900-01-01 00:00:00, 2229-12-31 23:59:59.99999999].") // DateTime64(8)
        f.write_str("Timestamp is out of range, valid values are [1900-01-01 00:00:00, 2262-04-11 23:47:16.854775807].")
    }
}

impl error::Error for Timestamp64OutOfRange {}

const FROM: jiff::Timestamp = jiff::Timestamp::constant(-2208988800, 0);
// const TO: jiff::Timestamp = jiff::Timestamp::constant(10413791999, 999999990); // DateTime64(8)
const TO: jiff::Timestamp = jiff::Timestamp::constant(9223372036, 854775807);

macro_rules! impl_timestamp64 {
    ($precision:literal, $ser_conv:ident, $de_conv:ident) => {
        impl TryFrom<jiff::Timestamp> for Timestamp64<$precision> {
            type Error = Timestamp64OutOfRange;
            fn try_from(value: jiff::Timestamp) -> Result<Self, Self::Error> {
                if value >= FROM && value <= TO {
                    Ok(Self(value))
                } else {
                    Err(Timestamp64OutOfRange)
                }
            }
        }

        impl From<Timestamp64<$precision>> for jiff::Timestamp {
            #[inline]
            fn from(value: Timestamp64<$precision>) -> Self {
                value.0
            }
        }

        impl Serialize for Timestamp64<$precision> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                // `TO` is defined as `from_nanosecond(i64::MAX as i128)`.
                //
                // Therefore, it's perfectly fine to cast `as_nanosecond`
                // to i64.
                let ts = self.0.$ser_conv() as i64;
                ts.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for Timestamp64<$precision> {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde_core::Deserializer<'de>,
            {
                struct TsVisitor;

                impl<'de> Visitor<'de> for TsVisitor {
                    type Value = Timestamp64<$precision>;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> fmt::Result {
                        formatter.write_str("an i64 representation of a ClickHouse DateTime64")
                    }

                    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                    where
                        E: serde_core::de::Error,
                    {
                        let ts = jiff::Timestamp::$de_conv(v.into())
                            .map_err(|_| E::custom("Failed to create a jiff::Timestamp"))?;
                        Ok(Timestamp64::<$precision>(ts))
                    }
                }

                deserializer.deserialize_i64(TsVisitor)
            }
        }
    };
}

impl_timestamp64!(0, as_second, from_second);
impl_timestamp64!(3, as_millisecond, from_millisecond);
impl_timestamp64!(6, as_microsecond, from_microsecond);
impl_timestamp64!(9, as_nanosecond, from_nanosecond);
