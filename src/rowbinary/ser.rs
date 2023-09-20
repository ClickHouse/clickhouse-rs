use bytes::BufMut;
use serde::{
    ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, Serializer},
    Serialize,
};
use std::mem;

use crate::error::{Error, Result};

/// Serializes `value` using the RowBinary format and writes to `buffer`.
pub(crate) fn serialize_into(buffer: impl BufMut, value: &impl Serialize) -> Result<()> {
    let mut serializer = RowBinarySerializer { buffer };
    value.serialize(&mut serializer)?;
    Ok(())
}

/// A serializer for the RowBinary format.
///
/// See https://clickhouse.yandex/docs/en/interfaces/formats/#rowbinary for details.
struct RowBinarySerializer<B> {
    buffer: B,
}

macro_rules! impl_num {
    ($ty:ty, $ser_method:ident, $writer_method:ident) => {
        #[inline]
        fn $ser_method(self, v: $ty) -> Result<()> {
            self.buffer.$writer_method(v);
            Ok(())
        }
    };
}

impl<'a, B: BufMut> Serializer for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    impl_num!(i8, serialize_i8, put_i8);
    impl_num!(i16, serialize_i16, put_i16_le);
    impl_num!(i32, serialize_i32, put_i32_le);
    impl_num!(i64, serialize_i64, put_i64_le);
    impl_num!(i128, serialize_i128, put_i128_le);
    impl_num!(u8, serialize_u8, put_u8);
    impl_num!(u16, serialize_u16, put_u16_le);
    impl_num!(u32, serialize_u32, put_u32_le);
    impl_num!(u64, serialize_u64, put_u64_le);
    impl_num!(u128, serialize_u128, put_u128_le);
    impl_num!(f32, serialize_f32, put_f32_le);
    impl_num!(f64, serialize_f64, put_f64_le);

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.buffer.put_u8(v as _);
        Ok(())
    }

    #[inline]
    fn serialize_char(self, _v: char) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<()> where {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v.as_bytes());

        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        put_unsigned_leb128(&mut self.buffer, v.len() as u64);
        self.buffer.put_slice(v);
        Ok(())
    }

    #[inline]
    fn serialize_none(self) -> Result<()> {
        self.buffer.put_u8(1);
        Ok(())
    }

    #[inline]
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        self.buffer.put_u8(0);
        value.serialize(self)
    }

    #[inline]
    fn serialize_unit(self) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        todo!();
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.ok_or(Error::SequenceMustHaveLength)?;
        put_unsigned_leb128(&mut self.buffer, len as u64);
        Ok(self)
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!();
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        todo!();
    }

    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!();
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!();
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a, B: BufMut> SerializeStruct for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    /// In Clickhouse, when inserting in the RowBinary format:
    /// -- String is represented as a varint length (unsigned LEB128), followed by the bytes of the string.
    /// -- FixedString is represented simply as a sequence of bytes.
    /// When serializing a FixedString, we can simply serialize the String in the wrapper struct FixedString
    /// Since T is generic and the &str type already has an implementation which encodes it as LEB128,
    /// we can coerce T to a &str and put the bytes in the buffer without LEB128.
    /// * This will fail if the length of the underlying string != n for Clickhouse type FixedString(n)
    #[inline]
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        if name == "FixedString" {
            let value_str: &&String = unsafe { mem::transmute(&value) };
            self.buffer.put_slice(value_str.as_bytes());
            Ok(())
        } else {
            value.serialize(&mut **self)
        }
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> SerializeSeq for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, B: BufMut> SerializeTuple for &'a mut RowBinarySerializer<B> {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    #[inline]
    fn end(self) -> Result<()> {
        Ok(())
    }
}

fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        value != 0
    } {}
}

#[test]
fn it_serializes_unsigned_leb128() {
    let mut vec = Vec::new();

    put_unsigned_leb128(&mut vec, 624_485);

    assert_eq!(vec, [0xe5, 0x8e, 0x26]);
}
