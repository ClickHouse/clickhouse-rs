//! A dynamic value that the public `clickhouse::native` API decodes from any
//! column.
//!
//! [`Value`] implements [`Decode`], the extension point of the native API
//! for custom Rust types. It accepts every data type (`compatible` is always
//! true). The client does the column layout (null maps, `LowCardinality`
//! dictionaries, array offsets, tuples, maps) and calls `decode`,
//! `decode_null`, `decode_array`, `decode_tuple` or `decode_map` for each
//! value. For the scalar types, `decode` calls the client's own `Decode`
//! impls: of the type where the client has one (integers, floats, `bool`, IP
//! addresses, UUID), and else of the integer of the storage width (`Date`,
//! `DateTime`, `Decimal`, `Enum`, `Time`, ...). It reads the 256-bit values
//! with [`ValueReader::read_bytes_fixed`]. The renderer interprets the value
//! with the column type (for example a `Date` is the `UInt16` day number).
//!
//! `Value` cannot decode a tuple: `Decode::decode_tuple` takes a
//! `TupleIter`, which `clickhouse` does not export, so a type outside the
//! crate cannot implement that method. [`decode_column`] reads a `Tuple`,
//! `Nullable(Tuple)` or `Array(Tuple)` column (also in a
//! `SimpleAggregateFunction`) with the client's own tuple impls,
//! `(Value, Value, ...)`. A tuple inside another tuple, a map or a deeper
//! array fails with the error of the default `decode_tuple`.

use clickhouse::error::Error;
use clickhouse::native::DataTypeNode;
use clickhouse::native::decode::{Decode, ValueReader};
use clickhouse::native::{ArrayData, Column};
use std::net::{Ipv4Addr, Ipv6Addr};

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value<'a> {
    Null,
    Bool(bool),
    /// Signed integers up to 128 bits, and the types that are stored as one:
    /// `Date32`, `DateTime64`, `Time`, `Time64`, `Decimal32/64/128`,
    /// `Enum8/16`, `Interval*`.
    Int(i128),
    /// Unsigned integers up to 128 bits, and `Date` and `DateTime`.
    UInt(u128),
    /// `Int256`, `UInt256` and `Decimal256`: 32 little-endian bytes.
    Wide([u8; 32]),
    Float32(f32),
    Float64(f64),
    /// The bits of a `BFloat16`.
    BFloat16(u16),
    /// `String` and `FixedString`. A ClickHouse string can hold any bytes.
    Bytes(&'a [u8]),
    Uuid(uuid::Uuid),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Point(f64, f64),
    Array(Vec<Value<'a>>),
    Tuple(Vec<Value<'a>>),
    /// The entries in the order of the server.
    Map(Vec<(Value<'a>, Value<'a>)>),
}

/// Removes the wrappers that do not change the encoding of a single value.
/// The client gives the value of a `Nullable(T)` column with the type
/// `Nullable(T)`, and a `LowCardinality(Nullable(T))` dictionary value with
/// the type `Nullable(T)`.
pub(crate) fn unwrap_value_type(mut t: &DataTypeNode) -> &DataTypeNode {
    loop {
        t = match t {
            DataTypeNode::Nullable(inner) | DataTypeNode::LowCardinality(inner) => inner,
            DataTypeNode::SimpleAggregateFunction(_, inner) => inner,
            _ => return t,
        };
    }
}

fn int<'a, T: Decode<'a> + Into<i128>>(r: &mut ValueReader<'a>) -> Result<Value<'a>, BoxedError> {
    Ok(Value::Int(T::decode(r)?.into()))
}

fn uint<'a, T: Decode<'a> + Into<u128>>(r: &mut ValueReader<'a>) -> Result<Value<'a>, BoxedError> {
    Ok(Value::UInt(T::decode(r)?.into()))
}

impl<'a> Decode<'a> for Value<'a> {
    fn compatible(_data_type: &DataTypeNode) -> bool {
        true
    }

    fn decode(reader: &mut ValueReader<'a>) -> Result<Self, BoxedError> {
        use clickhouse::native::DataTypeNode::*;
        let data_type = unwrap_value_type(reader.data_type()).clone();
        let wide = |r: &mut ValueReader<'a>| Ok(Value::Wide(*r.read_bytes_fixed::<32>()?));
        match data_type {
            Bool => Ok(Value::Bool(bool::decode(reader)?)),
            Int8 => int::<i8>(reader),
            Int16 => int::<i16>(reader),
            Int32 => int::<i32>(reader),
            Int64 => int::<i64>(reader),
            Int128 => int::<i128>(reader),
            UInt8 => uint::<u8>(reader),
            UInt16 => uint::<u16>(reader),
            UInt32 => uint::<u32>(reader),
            UInt64 => uint::<u64>(reader),
            UInt128 => uint::<u128>(reader),
            Int256 | UInt256 => wide(reader),
            Float32 => Ok(Value::Float32(f32::decode(reader)?)),
            Float64 => Ok(Value::Float64(f64::decode(reader)?)),
            BFloat16 => Ok(Value::BFloat16(u16::decode(reader)?)),
            String | FixedString(_) => Ok(Value::Bytes(reader.native_bytes())),
            UUID => Ok(Value::Uuid(uuid::Uuid::decode(reader)?)),
            IPv4 => Ok(Value::Ipv4(Ipv4Addr::decode(reader)?)),
            IPv6 => Ok(Value::Ipv6(Ipv6Addr::decode(reader)?)),
            // Days since 1970-01-01 (UInt16), seconds since the epoch (UInt32).
            Date => uint::<u16>(reader),
            DateTime(_) => uint::<u32>(reader),
            // Days since 1970-01-01 (Int32), ticks since the epoch (Int64).
            Date32 => int::<i32>(reader),
            DateTime64(..) => int::<i64>(reader),
            // Seconds (Int32) and ticks (Int64) since 00:00:00.
            Time => int::<i32>(reader),
            Time64(_) => int::<i64>(reader),
            Interval(_) => int::<i64>(reader),
            // `clickhouse` does not re-export `DecimalType` and `EnumType`, so
            // the width comes from the precision (as in ClickHouse) and from
            // the size of the value.
            Decimal(precision, ..) => match precision {
                0..=9 => int::<i32>(reader),
                10..=18 => int::<i64>(reader),
                19..=38 => int::<i128>(reader),
                _ => wide(reader),
            },
            Enum(..) => match reader.native_bytes().len() {
                1 => int::<i8>(reader),
                _ => int::<i16>(reader),
            },
            Point => {
                let x = f64::decode(reader)?;
                let y = f64::decode(reader)?;
                Ok(Value::Point(x, y))
            }
            other => {
                Err(format!("clickhouse-test-runner: no decoder for the data type {other}").into())
            }
        }
    }

    fn decode_null(_data_type: &DataTypeNode) -> Result<Self, BoxedError> {
        Ok(Value::Null)
    }

    fn decode_array(data: ArrayData<'a>) -> Result<Self, BoxedError> {
        let values = data
            .into_reader::<Value<'a>>()?
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Value::Array(values))
    }

    fn decode_map(key_data: ArrayData<'a>, value_data: ArrayData<'a>) -> Result<Self, BoxedError> {
        let keys = key_data.into_reader::<Value<'a>>()?;
        let values = value_data.into_reader::<Value<'a>>()?;
        let entries = keys
            .zip(values)
            .map(|(k, v)| Ok((k?, v?)))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Value::Map(entries))
    }
}

/// Decodes all values of a column.
///
/// A `SimpleAggregateFunction(f, T)` column has the encoding of `T`, and the
/// client's own types accept it as `T`, so the shape comes from `T`.
pub(crate) fn decode_column(column: &Column) -> Result<Vec<Value<'_>>, Error> {
    use DataTypeNode::{Array, Nullable, Tuple};
    match column.data_type().remove_compatible_wrappers() {
        Tuple(types) => decode_tuples(column, Shape::Plain, types.len()),
        Nullable(inner) if matches!(&**inner, Tuple(_)) => {
            let Tuple(types) = &**inner else {
                unreachable!()
            };
            decode_tuples(column, Shape::Nullable, types.len())
        }
        Array(inner) if matches!(&**inner, Tuple(_)) => {
            let Tuple(types) = &**inner else {
                unreachable!()
            };
            decode_tuples(column, Shape::Array, types.len())
        }
        _ => column.iter::<Value<'_>>()?.collect(),
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Plain,
    Nullable,
    Array,
}

/// A Rust tuple of [`Value`]s, decoded by the client's own tuple impls.
trait TupleValue<'a>: Decode<'a> {
    fn into_value(self) -> Value<'a>;
}

macro_rules! elem {
    ($l:lifetime, $x:ident) => {
        Value<$l>
    };
}

macro_rules! tuple_value {
    ($($x:ident)+) => {
        impl<'a> TupleValue<'a> for ($(elem!('a, $x),)+) {
            fn into_value(self) -> Value<'a> {
                let ($($x,)+) = self;
                Value::Tuple(vec![$($x),+])
            }
        }
    };
}

tuple_value!(a);
tuple_value!(a b);
tuple_value!(a b c);
tuple_value!(a b c d);
tuple_value!(a b c d e);
tuple_value!(a b c d e f);
tuple_value!(a b c d e f g);
tuple_value!(a b c d e f g h);
tuple_value!(a b c d e f g h i);
tuple_value!(a b c d e f g h i j);
tuple_value!(a b c d e f g h i j k);
tuple_value!(a b c d e f g h i j k l);
tuple_value!(a b c d e f g h i j k l m);
tuple_value!(a b c d e f g h i j k l m n);
tuple_value!(a b c d e f g h i j k l m n o);
tuple_value!(a b c d e f g h i j k l m n o p);

fn decode_shape<'a, T: TupleValue<'a>>(
    column: &'a Column,
    shape: Shape,
) -> Result<Vec<Value<'a>>, Error> {
    match shape {
        Shape::Plain => column.iter::<T>()?.map(|r| r.map(T::into_value)).collect(),
        Shape::Nullable => column
            .iter::<Option<T>>()?
            .map(|r| r.map(|t| t.map_or(Value::Null, T::into_value)))
            .collect(),
        Shape::Array => column
            .iter::<Vec<T>>()?
            .map(|r| r.map(|v| Value::Array(v.into_iter().map(T::into_value).collect())))
            .collect(),
    }
}

fn decode_tuples(column: &Column, shape: Shape, len: usize) -> Result<Vec<Value<'_>>, Error> {
    macro_rules! by_len {
        ($($n:literal => ($($x:ident)+)),+ $(,)?) => {
            match len {
                $($n => decode_shape::<($(elem!('_, $x),)+)>(column, shape),)+
                _ => Err(Error::Unsupported(format!(
                    "clickhouse-test-runner: the client decodes tuples of at most 16 elements, \
                     got {len} in {}",
                    column.data_type()
                ))),
            }
        };
    }
    by_len!(
        1 => (a),
        2 => (a b),
        3 => (a b c),
        4 => (a b c d),
        5 => (a b c d e),
        6 => (a b c d e f),
        7 => (a b c d e f g),
        8 => (a b c d e f g h),
        9 => (a b c d e f g h i),
        10 => (a b c d e f g h i j),
        11 => (a b c d e f g h i j k),
        12 => (a b c d e f g h i j k l),
        13 => (a b c d e f g h i j k l m),
        14 => (a b c d e f g h i j k l m n),
        15 => (a b c d e f g h i j k l m n o),
        16 => (a b c d e f g h i j k l m n o p),
    )
}
