use std::{convert::TryFrom, mem, str};

use bytes::Buf;
use serde::{
    de::{DeserializeSeed, Deserializer, SeqAccess, Visitor},
    Deserialize,
};

use crate::error::{Error, Result};

/// Deserializes a value from `buffer` with a message encoded in the RowBinary format.
pub(crate) fn deserialize_from<'de, T: Deserialize<'de>>(
    input: impl Buf,
    temp_buf: &'de mut [u8],
) -> Result<T> {
    let mut deserializer = RowBinaryDeserializer { input, temp_buf };
    T::deserialize(&mut deserializer)
}

/// A deserializer for the RowBinary format.
///
/// See https://clickhouse.yandex/docs/en/interfaces/formats/#rowbinary for details.
struct RowBinaryDeserializer<'de, B> {
    input: B,
    temp_buf: &'de mut [u8],
}

impl<'de, B: Buf> RowBinaryDeserializer<'de, B> {
    fn read_vec(&mut self, size: usize) -> Result<Vec<u8>> {
        ensure_size(&mut self.input, size)?;
        let mut vec = vec![0; size];
        self.input.copy_to_slice(&mut vec[..]);
        Ok(vec)
    }

    fn read_slice(&mut self, size: usize) -> Result<&'de [u8]> {
        ensure_size(&mut self.input, size)?;

        if self.temp_buf.len() < size {
            return Err(Error::TooSmallBuffer(size - self.temp_buf.len()));
        }

        let temp_buf = mem::take(&mut self.temp_buf);
        let (slice, rest) = temp_buf.split_at_mut(size);
        self.temp_buf = rest;
        self.input.copy_to_slice(slice);

        Ok(slice)
    }

    fn read_size(&mut self) -> Result<usize> {
        let size = get_unsigned_leb128(&mut self.input)?;
        // TODO: what about another error?
        usize::try_from(size).map_err(|_| Error::NotEnoughData)
    }
}

#[inline]
fn ensure_size(buffer: impl Buf, size: usize) -> Result<()> {
    if buffer.remaining() < size {
        Err(Error::NotEnoughData)
    } else {
        Ok(())
    }
}

macro_rules! impl_num {
    ($ty:ty, $deser_method:ident, $visitor_method:ident, $reader_method:ident) => {
        #[inline]
        fn $deser_method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            ensure_size(&mut self.input, mem::size_of::<$ty>())?;
            let value = self.input.$reader_method();
            visitor.$visitor_method(value)
        }
    };
}

impl<'de, 'a, B: Buf> Deserializer<'de> for &'a mut RowBinaryDeserializer<'de, B> {
    type Error = Error;

    #[inline]
    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        Err(Error::DeserializeAnyNotSupported)
    }

    impl_num!(i8, deserialize_i8, visit_i8, get_i8);
    impl_num!(i16, deserialize_i16, visit_i16, get_i16_le);
    impl_num!(i32, deserialize_i32, visit_i32, get_i32_le);
    impl_num!(i64, deserialize_i64, visit_i64, get_i64_le);
    impl_num!(i128, deserialize_i128, visit_i128, get_i128_le);
    impl_num!(u8, deserialize_u8, visit_u8, get_u8);
    impl_num!(u16, deserialize_u16, visit_u16, get_u16_le);
    impl_num!(u32, deserialize_u32, visit_u32, get_u32_le);
    impl_num!(u64, deserialize_u64, visit_u64, get_u64_le);
    impl_num!(u128, deserialize_u128, visit_u128, get_u128_le);
    impl_num!(f32, deserialize_f32, visit_f32, get_f32_le);
    impl_num!(f64, deserialize_f64, visit_f64, get_f64_le);

    #[inline]
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        // TODO: revise this.
        visitor.visit_unit()
    }

    #[inline]
    fn deserialize_char<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;
        match self.input.get_u8() {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        let str = str::from_utf8(slice).map_err(Error::from)?;
        visitor.visit_borrowed_str(str)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let vec = self.read_vec(size)?;
        let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
        visitor.visit_string(string)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        visitor.visit_borrowed_bytes(slice)
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        visitor.visit_byte_buf(self.read_vec(size)?)
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _enum: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        struct Access<'de, 'a, B> {
            deserializer: &'a mut RowBinaryDeserializer<'de, B>,
            len: usize,
        }

        impl<'de, 'a, B: Buf> SeqAccess<'de> for Access<'de, 'a, B> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value = DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    #[inline]
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;

        match self.input.get_u8() {
            0 => visitor.visit_some(&mut *self),
            1 => visitor.visit_none(),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let len = self.read_size()?;
        self.deserialize_tuple(len, visitor)
    }

    #[inline]
    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    #[inline]
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!();
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}

fn get_unsigned_leb128(mut buffer: impl Buf) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;

    loop {
        ensure_size(&mut buffer, 1)?;

        let byte = buffer.get_u8();
        value |= (byte as u64 & 0x7f) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift > 57 {
            // TODO: what about another error?
            return Err(Error::NotEnoughData);
        }
    }

    Ok(value)
}

#[test]
fn it_deserializes_unsigned_leb128() {
    let buf = &[0xe5, 0x8e, 0x26][..];
    assert_eq!(get_unsigned_leb128(buf).unwrap(), 624_485);
}
