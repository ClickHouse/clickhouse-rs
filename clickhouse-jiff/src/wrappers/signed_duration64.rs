use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::SignedDuration`]
/// that corresponds to `Time64` in ClickHouse.
///
/// Supported precision:
/// * SignedDuration64<0> - seconds;
/// * SignedDuration64<3> - milliseconds;
/// * SignedDuration64<6> - microseconds;
/// * SignedDuration64<9> - nanoseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SignedDuration64<const PRECISION: u8>(jiff::SignedDuration);

/// An error that occurs when [`jiff::SignedDuration`]
/// cannot be represented as a `Time64` in ClickHouse.
#[derive(Debug)]
pub struct SignedDuration64OutOfRange;

impl fmt::Display for SignedDuration64OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignedDuration is out of range, valid values are [-999:59:59.999999999, 999:59:59.999999999].")
    }
}

impl error::Error for SignedDuration64OutOfRange {}

const FROM: jiff::SignedDuration = jiff::SignedDuration::new(-3599999, 999999999);
const TO: jiff::SignedDuration = jiff::SignedDuration::new(3599999, 999999999);

macro_rules! impl_signed_duration64 {
    ($precision:literal, $ser_conv:ident, $de_conv:ident) => {
        impl TryFrom<jiff::SignedDuration> for SignedDuration64<$precision> {
            type Error = SignedDuration64OutOfRange;
            fn try_from(value: jiff::SignedDuration) -> Result<Self, Self::Error> {
                if value >= FROM && value <= TO {
                    Ok(Self(value))
                } else {
                    Err(SignedDuration64OutOfRange)
                }
            }
        }

        impl From<SignedDuration64<$precision>> for jiff::SignedDuration {
            #[inline]
            fn from(value: SignedDuration64<$precision>) -> Self {
                value.0
            }
        }

        impl Serialize for SignedDuration64<$precision> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                // Even at the nanosecond precision, the range of possible values
                // is [-3599999999999999, 3599999999999999].
                //
                // Therefore, it is fine to cast as_nanos() and as_micros()
                // to i64.
                let ts = self.0.$ser_conv() as i64;
                ts.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for SignedDuration64<$precision> {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde_core::Deserializer<'de>,
            {
                struct DurationVisitor;

                impl<'de> Visitor<'de> for DurationVisitor {
                    type Value = SignedDuration64<$precision>;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("an i64 representation of a ClickHouse Time64")
                    }

                    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                    where
                        E: serde_core::de::Error,
                    {
                        let ts = jiff::SignedDuration::$de_conv(v.into());
                        Ok(SignedDuration64::<$precision>(ts))
                    }
                }

                deserializer.deserialize_i64(DurationVisitor)
            }
        }
    };
}

impl_signed_duration64!(0, as_secs, from_secs);
impl_signed_duration64!(3, as_millis, from_millis);
impl_signed_duration64!(6, as_micros, from_micros);
impl_signed_duration64!(9, as_nanos, from_nanos);
