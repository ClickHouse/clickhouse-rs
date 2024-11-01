use std::fmt::Formatter;

use bytes::Buf;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize};

use crate::buffer::ensure_size;
use crate::error::{Error, Result};

#[derive(Debug, PartialEq)]
pub enum Dynamic {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    String(String),
    Array(Vec<Dynamic>),
    Nullable(Option<Box<Dynamic>>),
    // Array(Vec<Dynamic>),
    // Null,
}

impl Dynamic {
    pub(crate) fn discriminator(&self) -> u8 {
        match self {
            Dynamic::Boolean(_) => 0x2d,
            Dynamic::UInt8(_) => 0x01,
            Dynamic::UInt16(_) => 0x02,
            Dynamic::UInt32(_) => 0x03,
            Dynamic::UInt64(_) => 0x04,
            Dynamic::UInt128(_) => 0x05,
            Dynamic::Int8(_) => 0x07,
            Dynamic::Int16(_) => 0x08,
            Dynamic::Int32(_) => 0x09,
            Dynamic::Int64(_) => 0x0a,
            Dynamic::Int128(_) => 0x0b,
            Dynamic::Float32(_) => 0x0d,
            Dynamic::Float64(_) => 0x0e,
            Dynamic::String(_) => 0x15,
            Dynamic::Array(_) => 0x1e,
            _ => todo!(),
        }
    }
}

impl<'de> Deserialize<'de> for Dynamic {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DynamicVisitor;

        impl<'de> Visitor<'de> for DynamicVisitor {
            type Value = Dynamic;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a valid dynamic type")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Dynamic::Boolean(v))
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E> {
                Ok(Dynamic::Int8(v))
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E> {
                Ok(Dynamic::Int16(v))
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E> {
                Ok(Dynamic::Int32(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Dynamic::Int64(v))
            }

            fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E> {
                Ok(Dynamic::Int128(v))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E> {
                Ok(Dynamic::UInt8(v))
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E> {
                Ok(Dynamic::UInt16(v))
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E> {
                Ok(Dynamic::UInt32(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Dynamic::UInt64(v))
            }

            fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E> {
                Ok(Dynamic::UInt128(v))
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E> {
                Ok(Dynamic::Float32(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(Dynamic::Float64(v))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Dynamic::String(v))
            }

            fn visit_seq<A>(self, _seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                todo!()
            }
        }

        deserializer.deserialize_any(DynamicVisitor)
    }
}

impl Serialize for Dynamic {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Dynamic::Boolean(v) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element::<u8>(&self.discriminator())?;
                tuple.serialize_element(v)?;
                tuple.end()
            }
            Dynamic::Int8(v) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&self.discriminator())?;
                tuple.serialize_element(v)?;
                tuple.end()
            }
            Dynamic::String(v) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&self.discriminator())?;
                tuple.serialize_element(v)?;
                tuple.end()
            }
            Dynamic::Array(arr) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&self.discriminator())?;
                tuple.serialize_element(arr)?;
                tuple.end()
            }
            _ => todo!(),
        }
    }
}

#[repr(u8)]
pub(crate) enum DynamicType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    // Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    // UInt256,
    Float32,
    Float64,
    String,
    Array { value: Box<DynamicType> },
}

impl DynamicType {
    // See https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
    pub(crate) fn new(mut input: &mut &'_ [u8]) -> Result<Self> {
        ensure_size(&mut input, 1)?;
        let discriminator = input.get_u8();
        match discriminator {
            0x2d => Ok(Self::Boolean),
            0x01 => Ok(Self::UInt8),
            0x02 => Ok(Self::UInt16),
            0x03 => Ok(Self::UInt32),
            0x04 => Ok(Self::UInt64),
            0x05 => Ok(Self::UInt128),
            // 0x06 => Ok(Self::Int256),
            0x07 => Ok(Self::Int8),
            0x08 => Ok(Self::Int16),
            0x09 => Ok(Self::Int32),
            0x0a => Ok(Self::Int64),
            0x0b => Ok(Self::Int128),
            // 0x0c => Ok(Self::UInt256),
            0x0d => Ok(Self::Float32),
            0x0e => Ok(Self::Float64),
            0x15 => Ok(Self::String),
            0x1e => {
                let value = Box::new(Self::new(input)?);
                Ok(Self::Array { value })
            }
            _ => Err(Error::InvalidDataTypeBinaryEncoding(discriminator)),
        }
    }
    //
    // pub(crate) fn as_discriminator(&self) -> u8 {
    //     match self {
    //         Self::Boolean => 0x2d,
    //         Self::UInt8 => 0x01,
    //         Self::UInt16 => 0x02,
    //         Self::UInt32 => 0x03,
    //         Self::UInt64 => 0x04,
    //         Self::UInt128 => 0x05,
    //         // Self::UInt256 => 0x06,
    //         Self::Int8 => 0x07,
    //         Self::Int16 => 0x08,
    //         Self::Int32 => 0x09,
    //         Self::Int64 => 0x0a,
    //         Self::Int128 => 0x0b,
    //         // Self::Int256 => 0x0c,
    //         Self::Float32 => 0x0d,
    //         Self::Float64 => 0x0e,
    //         Self::String => 0x15,
    //         Self::Array { .. } => 0x1e,
    //     }
    // }
}
