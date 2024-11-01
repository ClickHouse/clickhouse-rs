use std::{convert::TryFrom, mem, str};

use crate::buffer::get_unsigned_leb128;
use crate::dynamic::DynamicType;
use crate::error::{Error, Result};
use bytes::Buf;
use serde::de::{EnumAccess, VariantAccess};
use serde::{
    de::{DeserializeSeed, Deserializer, SeqAccess, Visitor},
    Deserialize,
};

/// Deserializes a value from `input` with a row encoded in `RowBinary`.
///
/// It accepts _a reference to_ a byte slice because it somehow leads to a more
/// performant generated code than `(&[u8]) -> Result<(T, usize)>` and even
/// `(&[u8], &mut Option<T>) -> Result<usize>`.
pub(crate) fn deserialize_from<'data, T: Deserialize<'data>>(input: &mut &'data [u8]) -> Result<T> {
    let mut deserializer = RowBinaryDeserializer { input };
    T::deserialize(&mut deserializer)
}

/// A deserializer for the RowBinary format.
///
/// See https://clickhouse.com/docs/en/interfaces/formats#rowbinary for details.
struct RowBinaryDeserializer<'cursor, 'data> {
    input: &'cursor mut &'data [u8],
}

impl<'cursor, 'data> RowBinaryDeserializer<'cursor, 'data> {
    fn read_vec(&mut self, size: usize) -> Result<Vec<u8>> {
        Ok(self.read_slice(size)?.to_vec())
    }

    fn read_slice(&mut self, size: usize) -> Result<&'data [u8]> {
        ensure_size(&mut self.input, size)?;
        let slice = &self.input[..size];
        self.input.advance(size);
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
        fn $deser_method<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
            ensure_size(&mut self.input, mem::size_of::<$ty>())?;
            let value = self.input.$reader_method();
            visitor.$visitor_method(value)
        }
    };
}

impl<'cursor, 'data> Deserializer<'data> for &mut RowBinaryDeserializer<'cursor, 'data> {
    type Error = Error;

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
    fn deserialize_any<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let dynamic_type = DynamicType::new(self.input)?;
        match dynamic_type {
            DynamicType::Int8 => self.deserialize_i8(visitor),
            DynamicType::Int16 => self.deserialize_i16(visitor),
            DynamicType::Int32 => self.deserialize_i32(visitor),
            DynamicType::Int64 => self.deserialize_i64(visitor),
            DynamicType::Int128 => self.deserialize_i128(visitor),
            DynamicType::UInt8 => self.deserialize_u8(visitor),
            DynamicType::UInt16 => self.deserialize_u16(visitor),
            DynamicType::UInt32 => self.deserialize_u32(visitor),
            DynamicType::UInt64 => self.deserialize_u64(visitor),
            DynamicType::UInt128 => self.deserialize_u128(visitor),
            DynamicType::Float32 => self.deserialize_f32(visitor),
            DynamicType::Float64 => self.deserialize_f64(visitor),
            DynamicType::String => self.deserialize_string(visitor),
            DynamicType::Boolean => self.deserialize_bool(visitor),
            DynamicType::Array { .. } => self.deserialize_seq(visitor),
        }
    }

    #[inline]
    fn deserialize_unit<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // TODO: revise this.
        visitor.visit_unit()
    }

    #[inline]
    fn deserialize_char<V: Visitor<'data>>(self, _: V) -> Result<V::Value> {
        panic!("character types are unsupported: `char`");
    }

    #[inline]
    fn deserialize_bool<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;
        match self.input.get_u8() {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_str<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        let str = str::from_utf8(slice).map_err(Error::from)?;
        visitor.visit_borrowed_str(str)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let vec = self.read_vec(size)?;
        let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
        visitor.visit_string(string)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        visitor.visit_borrowed_bytes(slice)
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let size = self.read_size()?;
        visitor.visit_byte_buf(self.read_vec(size)?)
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u8(visitor)
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'data>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        struct Access<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
        }
        struct VariantDeserializer<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
        }
        impl<'data> VariantAccess<'data> for VariantDeserializer<'_, '_, 'data> {
            type Error = Error;

            fn unit_variant(self) -> Result<()> {
                Ok(())
            }

            fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
            where
                T: DeserializeSeed<'data>,
            {
                DeserializeSeed::deserialize(seed, &mut *self.deserializer)
            }

            fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
            where
                V: Visitor<'data>,
            {
                self.deserializer.deserialize_tuple(len, visitor)
            }

            fn struct_variant<V>(
                self,
                fields: &'static [&'static str],
                visitor: V,
            ) -> Result<V::Value>
            where
                V: Visitor<'data>,
            {
                self.deserializer.deserialize_tuple(fields.len(), visitor)
            }
        }

        impl<'de, 'cursor, 'data> EnumAccess<'data> for Access<'de, 'cursor, 'data> {
            type Error = Error;
            type Variant = VariantDeserializer<'de, 'cursor, 'data>;

            fn variant_seed<T>(
                self,
                seed: T,
            ) -> std::result::Result<(T::Value, Self::Variant), Self::Error>
            where
                T: DeserializeSeed<'data>,
            {
                seed.deserialize(&mut *self.deserializer).map(|v| {
                    (
                        v,
                        VariantDeserializer {
                            deserializer: self.deserializer,
                        },
                    )
                })
            }
        }
        visitor.visit_enum(Access { deserializer: self })
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'data>>(self, len: usize, visitor: V) -> Result<V::Value> {
        struct Access<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data>,
            len: usize,
        }

        impl<'de, 'cursor, 'data> SeqAccess<'data> for Access<'de, 'cursor, 'data> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'data>,
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
    fn deserialize_option<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        ensure_size(&mut self.input, 1)?;

        match self.input.get_u8() {
            0 => visitor.visit_some(&mut *self),
            1 => visitor.visit_none(),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        let len = self.read_size()?;
        self.deserialize_tuple(len, visitor)
    }

    #[inline]
    fn deserialize_map<V: Visitor<'data>>(self, _visitor: V) -> Result<V::Value> {
        panic!("maps are unsupported, use `Vec<(A, B)>` instead");
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'data>>(
        self,
        _name: &str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'data>>(
        self,
        _name: &str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    #[inline]
    fn deserialize_unit_struct<V: Visitor<'data>>(
        self,
        name: &'static str,
        _visitor: V,
    ) -> Result<V::Value> {
        panic!("unit types are unsupported: `{name}`");
    }

    #[inline]
    fn deserialize_tuple_struct<V: Visitor<'data>>(
        self,
        name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        panic!("tuple struct types are unsupported: `{name}`");
    }

    #[inline]
    fn deserialize_ignored_any<V: Visitor<'data>>(self, _visitor: V) -> Result<V::Value> {
        panic!("ignored types are unsupported");
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}
