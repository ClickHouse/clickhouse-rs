use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::civil::Date`]
/// that corresponds to `Date32` in ClickHouse.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Date32(jiff::civil::Date);

/// An error that occurs when [`jiff::civil::Date`]
/// cannot be represented as a `Date32` in ClickHouse.
#[derive(Debug)]
pub struct Date32OutOfRange;

impl fmt::Display for Date32OutOfRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Date is out of range, valid values are [1900-01-01, 2299-12-31].")
    }
}

impl error::Error for Date32OutOfRange {}

const ORIGIN: jiff::civil::Date = jiff::civil::Date::constant(1970, 1, 1);
const FROM: jiff::civil::Date = jiff::civil::Date::constant(1900, 1, 1);
const TO: jiff::civil::Date = jiff::civil::Date::constant(2299, 12, 31);

impl TryFrom<jiff::civil::Date> for Date32 {
    type Error = Date32OutOfRange;
    fn try_from(value: jiff::civil::Date) -> Result<Self, Self::Error> {
        if value >= FROM && value <= TO {
            Ok(Self(value))
        } else {
            Err(Date32OutOfRange)
        }
    }
}

impl From<Date32> for jiff::civil::Date {
    #[inline]
    fn from(value: Date32) -> Self {
        value.0
    }
}

impl Serialize for Date32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Possible values are [-25567, 120529], which do fit in an i32.
        let days = (self.0.duration_since(ORIGIN).as_hours() / 24) as i32;
        days.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Date32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde_core::Deserializer<'de>,
    {
        struct DateVisitor;

        impl<'de> Visitor<'de> for DateVisitor {
            type Value = Date32;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> fmt::Result {
                formatter.write_str("an i32 representation of a ClickHouse Date32")
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde_core::de::Error,
            {
                let date = ORIGIN + jiff::SignedDuration::from_hours(i64::from(v) * 24);
                Ok(Date32(date))
            }
        }

        deserializer.deserialize_i32(DateVisitor)
    }
}
