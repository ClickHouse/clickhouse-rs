use std::fmt::{self, Write};

use serde::{
    ser::{self, SerializeSeq, SerializeTuple, Serializer},
    Serialize,
};
use thiserror::Error;

use super::escape;

// === SqlSerializerError ===

#[derive(Debug, Error)]
enum SqlSerializerError {
    #[error("{0} is unsupported")]
    Unsupported(&'static str),
    #[error("{0}")]
    Custom(String),
}

impl ser::Error for SqlSerializerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<fmt::Error> for SqlSerializerError {
    fn from(err: fmt::Error) -> Self {
        Self::Custom(err.to_string())
    }
}

// === SqlSerializer ===

type Result<T = (), E = SqlSerializerError> = std::result::Result<T, E>;
type Impossible = ser::Impossible<(), SqlSerializerError>;

struct SqlSerializer<'a, W> {
    writer: &'a mut W,
}

macro_rules! unsupported {
    ($ser_method:ident($ty:ty) -> $ret:ty, $($other:tt)*) => {
        #[inline]
        fn $ser_method(self, _v: $ty) -> $ret {
            Err(SqlSerializerError::Unsupported(stringify!($ser_method)))
        }
        unsupported!($($other)*);
    };
    ($ser_method:ident($ty:ty), $($other:tt)*) => {
        unsupported!($ser_method($ty) -> Result, $($other)*);
    };
    ($ser_method:ident, $($other:tt)*) => {
        #[inline]
        fn $ser_method(self) -> Result {
            Err(SqlSerializerError::Unsupported(stringify!($ser_method)))
        }
        unsupported!($($other)*);
    };
    () => {};
}

macro_rules! forward_to_display {
    ($ser_method:ident($ty:ty), $($other:tt)*) => {
        #[inline]
        fn $ser_method(self, v: $ty) -> Result {
            write!(self.writer, "{}", &v)?;
            Ok(())
        }
        forward_to_display!($($other)*);
    };
    () => {};
}

impl<'a, W: Write> Serializer for SqlSerializer<'a, W> {
    type Error = SqlSerializerError;
    type Ok = ();
    type SerializeMap = Impossible;
    type SerializeSeq = SqlListSerializer<'a, W>;
    type SerializeStruct = Impossible;
    type SerializeStructVariant = Impossible;
    type SerializeTuple = SqlListSerializer<'a, W>;
    type SerializeTupleStruct = Impossible;
    type SerializeTupleVariant = Impossible;

    unsupported!(
        serialize_map(Option<usize>) -> Result<Impossible>,
        serialize_bytes(&[u8]),
        serialize_unit,
        serialize_unit_struct(&'static str),
    );

    forward_to_display!(
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_bool(bool),
    );

    #[inline]
    fn serialize_char(self, value: char) -> Result {
        let mut tmp = [0u8; 4];
        self.serialize_str(value.encode_utf8(&mut tmp))
    }

    #[inline]
    fn serialize_str(self, value: &str) -> Result {
        escape::string(value, self.writer)?;
        Ok(())
    }

    #[inline]
    fn serialize_seq(self, _len: Option<usize>) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('[')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ']',
        })
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<SqlListSerializer<'a, W>> {
        self.writer.write_char('(')?;
        Ok(SqlListSerializer {
            writer: self.writer,
            has_items: false,
            closing_char: ')',
        })
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, _value: &T) -> Result {
        _value.serialize(self)
    }

    #[inline]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.writer.write_str("NULL")?;
        Ok(())
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result {
        escape::string(variant, self.writer)?;
        Ok(())
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result {
        Err(SqlSerializerError::Unsupported("serialize_newtype_variant"))
    }

    #[inline]
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Impossible> {
        Err(SqlSerializerError::Unsupported("serialize_tuple_struct"))
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Impossible> {
        Err(SqlSerializerError::Unsupported("serialize_tuple_variant"))
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(SqlSerializerError::Unsupported("serialize_struct"))
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(SqlSerializerError::Unsupported("serialize_struct_variant"))
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        true
    }
}

// === SqlListSerializer ===

struct SqlListSerializer<'a, W> {
    writer: &'a mut W,
    has_items: bool,
    closing_char: char,
}

impl<'a, W: Write> SerializeSeq for SqlListSerializer<'a, W> {
    type Error = SqlSerializerError;
    type Ok = ();

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result
    where
        T: Serialize + ?Sized,
    {
        if self.has_items {
            self.writer.write_char(',')?;
        }

        self.has_items = true;

        value.serialize(SqlSerializer {
            writer: self.writer,
        })
    }

    #[inline]
    fn end(self) -> Result {
        self.writer.write_char(self.closing_char)?;
        Ok(())
    }
}

impl<'a, W: Write> SerializeTuple for SqlListSerializer<'a, W> {
    type Error = SqlSerializerError;
    type Ok = ();

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result
    where
        T: Serialize + ?Sized,
    {
        SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> Result {
        SerializeSeq::end(self)
    }
}

// === Public API ===

pub(crate) fn write_arg(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    value
        .serialize(SqlSerializer { writer })
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(v: impl Serialize) -> String {
        let mut out = String::new();
        write_arg(&mut out, &v).unwrap();
        out
    }

    #[test]
    fn it_writes_numeric_primitives() {
        assert_eq!(check(42), "42");
        assert_eq!(check(42.5), "42.5");
        assert_eq!(check(42u128), "42");
    }

    #[test]
    fn it_writes_chars() {
        assert_eq!(check('8'), "'8'");
        assert_eq!(check('\''), "'\\''");
        // TODO: assert_eq!(check('\n'), "'\\n'");
    }

    #[test]
    fn it_writes_strings() {
        assert_eq!(check("ab"), "'ab'");
        assert_eq!(check("a'b"), "'a\\'b'");
        // TODO: assert_eq!(check("a\nb"), "'a\\nb'");
    }

    #[test]
    fn it_writes_unit_variants() {
        #[derive(Serialize)]
        enum Enum {
            A,
        }
        assert_eq!(check(Enum::A), "'A'");
    }

    #[test]
    fn it_writes_newtypes() {
        #[derive(Serialize)]
        struct N(u32);
        #[derive(Serialize)]
        struct F(f64);

        assert_eq!(check(N(42)), "42");
        assert_eq!(check(F(42.5)), "42.5");
    }

    #[test]
    fn it_writes_arrays() {
        assert_eq!(check(&[42, 43][..]), "[42,43]");
        assert_eq!(check(vec![42, 43]), "[42,43]");
    }

    #[test]
    fn it_writes_tuples() {
        assert_eq!(check((42, 43)), "(42,43)");
    }

    #[test]
    fn it_fails_on_unsupported() {
        let mut out = String::new();
        assert!(write_arg(&mut out, &std::collections::HashMap::<u32, u32>::new()).is_err());
        assert!(write_arg(&mut out, &None::<u32>).is_err());
        assert!(write_arg(&mut out, &Some(42)).is_err());
        assert!(write_arg(&mut out, &()).is_err());

        #[derive(Serialize)]
        struct Unit;
        assert!(write_arg(&mut out, &Unit).is_err());

        #[derive(Serialize)]
        struct Struct {
            a: u32,
        }
        assert!(write_arg(&mut out, &Struct { a: 42 }).is_err());

        #[derive(Serialize)]
        struct TupleStruct(u32, u32);
        assert!(write_arg(&mut out, &TupleStruct(42, 42)).is_err());

        #[derive(Serialize)]
        enum Enum {
            Newtype(u32),
            Tuple(u32, u32),
            Struct { a: u32 },
        }
        assert!(write_arg(&mut out, &Enum::Newtype(42)).is_err());
        assert!(write_arg(&mut out, &Enum::Tuple(42, 42)).is_err());
        assert!(write_arg(&mut out, &Enum::Struct { a: 42 }).is_err());
    }
}
