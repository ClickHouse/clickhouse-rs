use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};

/// Macro to generate wrapper types with custom serde implementations,
/// and convenience traits: [`From`], [`AsRef`], [`AsMut`], etc.
macro_rules! clickhouse_wrapper_impl {
    (
        $wrapper_name:ident,
        $inner_type:ty,
        serialize = |$ser_val:ident, $ser_serializer:ident| $ser_body:block,
        deserialize = |$de_deserializer:ident| -> $de_result:ty $de_body:block
    ) => {
        #[derive(Debug, Clone, PartialEq)]
        #[derive(AsRef, Deref, AsMut, DerefMut, From, Into)]
        pub struct $wrapper_name(pub $inner_type);

        impl Serialize for $wrapper_name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let $ser_val = &self.0;
                let $ser_serializer = serializer;
                $ser_body
            }
        }

        impl<'de> Deserialize<'de> for $wrapper_name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let $de_deserializer = deserializer;
                let inner: $de_result = $de_body;
                Ok(Self(inner))
            }
        }
    };
}

pub mod blob {
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use serde::{Deserialize, Serialize};

    /// While being stored in ClickHouse as String or LowCardinality(String),
    /// Blob is represented as raw bytes, without any UTF-8 validation.
    #[derive(Debug, Clone, PartialEq)]
    #[derive(Serialize, Deserialize)]
    #[derive(AsRef, Deref, AsMut, DerefMut, From, Into)]
    pub struct ChBlobString(#[serde(with = "serde_bytes")] pub Vec<u8>);
}

pub mod fixed_string {
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;
    use std::fmt::Debug;

    /// FixedString is represented as raw bytes, no UTF-8 validation.
    /// It can be used with any byte array size, e.g., [u8; 16], [u8; 32], etc.
    /// The size must match the FixedString(N) definition in ClickHouse.
    ///
    /// NB: [`serde_as`] is the easiest way to serialize/deserialize fixed-size arrays,
    /// as by default, serde supports derive up to length 32.
    ///
    /// See https://docs.rs/serde_with/3.14.1/serde_with/#large-and-const-generic-arrays
    #[serde_as]
    #[derive(Debug, Clone, PartialEq)]
    #[derive(Serialize, Deserialize)]
    #[derive(AsRef, Deref, AsMut, DerefMut, From, Into)]
    pub struct ChFixedString<const N: usize>(#[serde_as(as = "[_; N]")] pub [u8; N]);
}

pub mod ipv4 {
    use super::*;
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use std::net::Ipv4Addr;

    clickhouse_wrapper_impl!(
        ChIpv4,
        Ipv4Addr,
        serialize = |ipv4, serializer| { u32::from(*ipv4).serialize(serializer) },
        deserialize = |deserializer| -> Ipv4Addr {
            let ip: u32 = Deserialize::deserialize(deserializer)?;
            Ipv4Addr::from(ip)
        }
    );
}

// NB: this is not really required, as IPv6 can be deserialized without additional attributes,
// but for consistency with IPv4 we provide the wrapper.
pub mod ipv6 {
    use super::*;
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use std::net::Ipv6Addr;

    clickhouse_wrapper_impl!(
        ChIpv6,
        Ipv6Addr,
        serialize = |ipv6, serializer| { ipv6.octets().serialize(serializer) },
        deserialize = |deserializer| -> Ipv6Addr {
            let octets: [u8; 16] = Deserialize::deserialize(deserializer)?;
            Ipv6Addr::from(octets)
        }
    );
}

#[cfg(feature = "uuid")]
pub mod uuid {
    use super::*;
    use ::uuid::Uuid;
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use serde::de::Error as DeError;

    clickhouse_wrapper_impl!(
        ChUuid,
        Uuid,
        serialize = |uuid, serializer| {
            if serializer.is_human_readable() {
                uuid.to_string().serialize(serializer)
            } else {
                let bytes = uuid.as_u64_pair();
                bytes.serialize(serializer)
            }
        },
        deserialize = |deserializer| -> Uuid {
            if deserializer.is_human_readable() {
                let uuid_str: &str = Deserialize::deserialize(deserializer)?;
                Uuid::parse_str(uuid_str).map_err(D::Error::custom)?
            } else {
                let bytes: (u64, u64) = Deserialize::deserialize(deserializer)?;
                Ok(Uuid::from_u64_pair(bytes.0, bytes.1))?
            }
        }
    );
}

#[cfg(any(feature = "time", feature = "chrono"))]
mod precision {

    // ClickHouse DateTime64 and Time64 support precision from 0 to 9 inclusive.
    // See https://clickhouse.com/docs/sql-reference/data-types/datetime64
    pub trait ValidPrecision<const N: u8> {}

    impl ValidPrecision<0> for () {}
    impl ValidPrecision<3> for () {}
    impl ValidPrecision<6> for () {}
    impl ValidPrecision<9> for () {}
}

#[cfg(any(feature = "time", feature = "chrono"))]
macro_rules! clickhouse_datetime64_wrapper_impl {
    (
        $wrapper_type:ident,
        $type:ident,
        serialize = |$ser_val:ident, $ser_serializer:ident| $ser_body:block,
        deserialize = |$de_deserializer:ident| -> $de_result:ty $de_body:block
    ) => {
        #[derive(Debug, Clone, PartialEq)]
        #[derive(AsRef, Deref, AsMut, DerefMut, From, Into)]
        pub struct $wrapper_type<const PRECISION: u8>(pub $type)
        where
            (): ValidPrecision<PRECISION>;

        impl<const PRECISION: u8> Serialize for $wrapper_type<PRECISION>
        where
            (): ValidPrecision<PRECISION>,
        {
            fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
                let $ser_val = &self.0;
                let $ser_serializer = ser;
                $ser_body
            }
        }

        impl<'de, const PRECISION: u8> Deserialize<'de> for $wrapper_type<PRECISION>
        where
            (): ValidPrecision<PRECISION>,
        {
            fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                let $de_deserializer = de;
                let inner: $de_result = $de_body;
                Ok(Self(inner))
            }
        }
    };
}

// Time crate wrappers (only when time feature is enabled)
#[cfg(feature = "time")]
pub mod time {
    use super::*;
    use ::time::{Date, Duration, OffsetDateTime, error::ComponentRange};
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use precision::*;
    use serde::de::Error as DeError;
    use serde::ser::Error as SerError;
    use std::convert::TryFrom;
    clickhouse_wrapper_impl!(
        ChDate,
        Date,
        serialize = |date, serializer| {
            const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);
            let origin = ORIGIN.unwrap();
            if *date < origin {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - origin;
            let days = elapsed.whole_days();

            u16::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> Date {
            const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);
            let days: u16 = Deserialize::deserialize(deserializer)?;
            ORIGIN.unwrap() + Duration::days(i64::from(days))
        }
    );

    clickhouse_wrapper_impl!(
        ChDate32,
        Date,
        serialize = |date, serializer| {
            const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);
            const MIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1900, 1);
            const MAX: Result<Date, ComponentRange> = Date::from_ordinal_date(2299, 365);

            if *date < MIN.unwrap() || *date > MAX.unwrap() {
                let msg = format!("{date} cannot be represented as Date32");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - ORIGIN.unwrap();
            let days = elapsed.whole_days();

            i32::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date32")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> Date {
            const ORIGIN: Result<Date, ComponentRange> = Date::from_ordinal_date(1970, 1);
            let days: i32 = Deserialize::deserialize(deserializer)?;
            ORIGIN.unwrap() + Duration::days(i64::from(days))
        }
    );

    clickhouse_wrapper_impl!(
        ChDateTime,
        OffsetDateTime,
        serialize = |dt, serializer| {
            let ts = dt.unix_timestamp();
            u32::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> OffsetDateTime {
            let ts: u32 = Deserialize::deserialize(deserializer)?;
            OffsetDateTime::from_unix_timestamp(i64::from(ts)).map_err(D::Error::custom)?
        }
    );

    // DateTime64 variants
    clickhouse_datetime64_wrapper_impl!(
        ChDateTime64,
        OffsetDateTime,
        serialize = |value, serializer| {
            let ts = if PRECISION == 0 {
                value.unix_timestamp()
            } else {
                let divisor = match PRECISION {
                    3 => 1_000_000,
                    6 => 1_000,
                    9 => 1,
                    _ => unreachable!(), // Prevented by ValidPrecision trait
                };
                let ts = value.unix_timestamp_nanos() / divisor;
                i64::try_from(ts).map_err(|_| {
                    S::Error::custom(format!(
                        "{} cannot be represented as Int64 for DateTime64 with precision {}",
                        value, PRECISION
                    ))
                })?
            };
            ts.serialize(serializer)
        },
        deserialize = |deserializer| -> OffsetDateTime {
            let mul = match PRECISION {
                0 => 1_000_000_000,
                3 => 1_000_000,
                6 => 1_000,
                9 => 1,
                _ => unreachable!(), // Prevented by ValidPrecision trait
            };

            let ts: i64 = Deserialize::deserialize(deserializer)?;
            let ts = i128::from(ts) * mul; // cannot overflow: `mul` fits in `i64`
            OffsetDateTime::from_unix_timestamp_nanos(ts).map_err(D::Error::custom)?
        }
    );

    // Time and Time64 variants for Duration
    clickhouse_wrapper_impl!(
        ChTime,
        Duration,
        serialize = |duration, serializer| {
            let total_seconds = duration.whole_seconds();
            i32::try_from(total_seconds)
                .map_err(|_| S::Error::custom(format!("{duration} cannot be represented as Time")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> Duration {
            let seconds: i32 = Deserialize::deserialize(deserializer)?;
            Duration::seconds(seconds.into())
        }
    );

    clickhouse_datetime64_wrapper_impl!(
        ChTime64,
        Duration,
        serialize = |value, serializer| {
            let total_seconds = if PRECISION == 0 {
                value.whole_seconds()
            } else {
                let ts = match PRECISION {
                    3 => value.whole_milliseconds(),
                    6 => value.whole_microseconds(),
                    9 => value.whole_nanoseconds(),
                    _ => unreachable!(), // Prevented by ValidPrecision trait
                };
                i64::try_from(ts).map_err(|_| {
                    S::Error::custom(format!(
                        "{} cannot be represented as Int64 for Time64 with precision {}",
                        value, PRECISION
                    ))
                })?
            };
            total_seconds.serialize(serializer)
        },
        deserialize = |deserializer| -> Duration {
            let seconds: i64 = Deserialize::deserialize(deserializer)?;
            match PRECISION {
                0 => Duration::seconds(seconds),
                3 => Duration::milliseconds(seconds),
                6 => Duration::microseconds(seconds),
                9 => Duration::nanoseconds(seconds),
                _ => unreachable!(), // Prevented by ValidPrecision trait
            }
        }
    );
}

#[cfg(feature = "chrono")]
pub mod chrono {
    use super::*;
    use ::chrono::{DateTime, Duration, NaiveDate, Utc};
    use derive_more::{AsMut, AsRef, Deref, DerefMut, From, Into};
    use serde::de::Error as DeError;
    use serde::ser::Error as SerError;
    use std::convert::TryFrom;

    // Date and Date32
    clickhouse_wrapper_impl!(
        ChChronoDate,
        NaiveDate,
        serialize = |date, serializer| {
            const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);
            let origin = ORIGIN.unwrap();
            if *date < origin {
                let msg = format!("{date} cannot be represented as Date");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - origin;
            let days = elapsed.num_days();

            u16::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> NaiveDate {
            const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);
            let days: u16 = Deserialize::deserialize(deserializer)?;
            ORIGIN.unwrap() + Duration::days(i64::from(days))
        }
    );

    clickhouse_wrapper_impl!(
        ChChronoDate32,
        NaiveDate,
        serialize = |date, serializer| {
            const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);
            const MIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1900, 1);
            const MAX: Option<NaiveDate> = NaiveDate::from_yo_opt(2299, 365);

            if *date < MIN.unwrap() || *date > MAX.unwrap() {
                let msg = format!("{date} cannot be represented as Date32");
                return Err(S::Error::custom(msg));
            }

            let elapsed = *date - ORIGIN.unwrap();
            let days = elapsed.num_days();

            i32::try_from(days)
                .map_err(|_| S::Error::custom(format!("{date} cannot be represented as Date32")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> NaiveDate {
            const ORIGIN: Option<NaiveDate> = NaiveDate::from_yo_opt(1970, 1);
            let days: i32 = Deserialize::deserialize(deserializer)?;
            ORIGIN.unwrap() + Duration::days(i64::from(days))
        }
    );

    // DateTime
    clickhouse_wrapper_impl!(
        ChChronoDateTime,
        DateTime<Utc>,
        serialize = |dt, serializer| {
            let ts = dt.timestamp();
            u32::try_from(ts)
                .map_err(|_| S::Error::custom(format!("{dt} cannot be represented as DateTime")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> DateTime<Utc> {
            let ts: u32 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp(i64::from(ts), 0).ok_or_else(|| {
                D::Error::custom(format!("{ts} cannot be converted to DateTime<Utc>"))
            })?
        }
    );

    // DateTime64 variants
    clickhouse_wrapper_impl!(
        ChChronoDateTime64Secs,
        DateTime<Utc>,
        serialize = |dt, serializer| {
            let ts = dt.timestamp();
            ts.serialize(serializer)
        },
        deserialize = |deserializer| -> DateTime<Utc> {
            let ts: i64 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp(ts, 0)
                .ok_or_else(|| D::Error::custom(format!("Can't create DateTime<Utc> from {ts}")))?
        }
    );

    clickhouse_wrapper_impl!(
        ChChronoDateTime64Millis,
        DateTime<Utc>,
        serialize = |dt, serializer| {
            let ts = dt.timestamp_millis();
            ts.serialize(serializer)
        },
        deserialize = |deserializer| -> DateTime<Utc> {
            let ts: i64 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp_millis(ts)
                .ok_or_else(|| D::Error::custom(format!("Can't create DateTime<Utc> from {ts}")))?
        }
    );

    clickhouse_wrapper_impl!(
        ChChronoDateTime64Micros,
        DateTime<Utc>,
        serialize = |dt, serializer| {
            let ts = dt.timestamp_micros();
            ts.serialize(serializer)
        },
        deserialize = |deserializer| -> DateTime<Utc> {
            let ts: i64 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp_micros(ts)
                .ok_or_else(|| D::Error::custom(format!("Can't create DateTime<Utc> from {ts}")))?
        }
    );

    clickhouse_wrapper_impl!(
        ChChronoDateTime64Nanos,
        DateTime<Utc>,
        serialize = |dt, serializer| {
            let ts = dt.timestamp_nanos_opt().ok_or_else(|| {
                S::Error::custom(format!("{dt} cannot be represented as DateTime64"))
            })?;
            ts.serialize(serializer)
        },
        deserialize = |deserializer| -> DateTime<Utc> {
            let ts: i64 = Deserialize::deserialize(deserializer)?;
            DateTime::<Utc>::from_timestamp_nanos(ts)
        }
    );

    // Time and Time64 for Duration
    clickhouse_wrapper_impl!(
        ChChronoTime,
        Duration,
        serialize = |time, serializer| {
            i32::try_from(time.num_seconds())
                .map_err(|_| S::Error::custom(format!("{time} cannot be represented as Time")))?
                .serialize(serializer)
        },
        deserialize = |deserializer| -> Duration {
            let seconds: i32 = Deserialize::deserialize(deserializer)?;
            Duration::seconds(seconds as i64)
        }
    );
}
