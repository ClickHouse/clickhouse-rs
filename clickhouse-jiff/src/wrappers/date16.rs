use core::{error, fmt};
use serde_core::{
    de::{Deserialize, Visitor},
    ser::{Serialize, Serializer},
};

/// A wrapper type for [`jiff::civil::Date`]
/// that corresponds to `Date` in ClickHouse.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Date16(jiff::civil::Date);

/// An error that occurs when [`jiff::civil::Date`]
/// cannot be represented as a `Date` in ClickHouse.
#[derive(Debug)]
pub struct Date16OutOfRange;

impl fmt::Display for Date16OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Date is out of range, valid values are [1970-01-01, 2149-06-06].")
    }
}

impl error::Error for Date16OutOfRange {}

const FROM: jiff::civil::Date = jiff::civil::Date::constant(1970, 1, 1);
const TO: jiff::civil::Date = jiff::civil::Date::constant(2149, 6, 6);

impl TryFrom<jiff::civil::Date> for Date16 {
    type Error = Date16OutOfRange;
    fn try_from(value: jiff::civil::Date) -> Result<Self, Self::Error> {
        if value >= FROM && value <= TO {
            Ok(Self(value))
        } else {
            Err(Date16OutOfRange)
        }
    }
}

impl From<Date16> for jiff::civil::Date {
    #[inline]
    fn from(value: Date16) -> Self {
        value.0
    }
}

impl Serialize for Date16 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Possible values are [0, 65535], the whole u16 range.
        let days = (self.0.duration_since(FROM).as_hours() / 24) as u16;
        days.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Date16 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde_core::Deserializer<'de>,
    {
        struct DateVisitor;

        impl<'de> Visitor<'de> for DateVisitor {
            type Value = Date16;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u16 representation of a ClickHouse Date")
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: serde_core::de::Error,
            {
                let date = FROM + jiff::SignedDuration::from_hours(i64::from(v) * 24);
                Ok(Date16(date))
            }
        }

        deserializer.deserialize_u16(DateVisitor)
    }
}
