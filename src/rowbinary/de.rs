use crate::error::{Error, Result};
use crate::rowbinary::utils::{ensure_size, get_unsigned_leb128};
use crate::rowbinary::validation::SerdeType;
use crate::rowbinary::validation::{DataTypeValidator, ValidateDataType};
use bytes::Buf;
use clickhouse_rowbinary::data_types::{Column, DataTypeHint};
use serde::de::MapAccess;
use serde::{
    de::{DeserializeSeed, Deserializer, EnumAccess, SeqAccess, VariantAccess, Visitor},
    Deserialize,
};
use std::{convert::TryFrom, mem, str};

/// Deserializes a value from `input` with a row encoded in `RowBinary`.
///
/// It accepts _a reference to_ a byte slice because it somehow leads to a more
/// performant generated code than `(&[u8]) -> Result<(T, usize)>` and even
/// `(&[u8], &mut Option<T>) -> Result<usize>`.
pub(crate) fn deserialize_from<'data, T: Deserialize<'data>>(input: &mut &'data [u8]) -> Result<T> {
    // println!("deserialize_from call");

    let mut deserializer = RowBinaryDeserializer {
        input,
        validator: (),
    };
    T::deserialize(&mut deserializer)
}

/// Similar to [`deserialize_from`], but expects a slice of [`Column`] objects
/// parsed from the beginning of `RowBinaryWithNamesAndTypes` data stream.
/// After the header, the rows format is the same as `RowBinary`.
pub(crate) fn deserialize_from_and_validate<'data, 'cursor, T: Deserialize<'data>>(
    input: &mut &'data [u8],
    columns: &'cursor [Column],
) -> Result<T> {
    let mut deserializer = RowBinaryDeserializer {
        input,
        validator: DataTypeValidator::new(columns),
    };
    T::deserialize(&mut deserializer)
}

/// A deserializer for the RowBinary(WithNamesAndTypes) format.
///
/// See https://clickhouse.com/docs/en/interfaces/formats#rowbinary for details.
pub(crate) struct RowBinaryDeserializer<'cursor, 'data, Validator = ()>
where
    Validator: ValidateDataType,
{
    pub(crate) validator: Validator,
    pub(crate) input: &'cursor mut &'data [u8],
}

impl<'data, Validator> RowBinaryDeserializer<'_, 'data, Validator>
where
    Validator: ValidateDataType,
{
    pub(crate) fn read_vec(&mut self, size: usize) -> Result<Vec<u8>> {
        Ok(self.read_slice(size)?.to_vec())
    }

    pub(crate) fn read_slice(&mut self, size: usize) -> Result<&'data [u8]> {
        ensure_size(&mut self.input, size)?;
        let slice = &self.input[..size];
        self.input.advance(size);
        Ok(slice)
    }

    pub(crate) fn read_size(&mut self) -> Result<usize> {
        let size = get_unsigned_leb128(&mut self.input)?;
        // TODO: what about another error?
        usize::try_from(size).map_err(|_| Error::NotEnoughData)
    }
}

macro_rules! impl_num {
    ($ty:ty, $deser_method:ident, $visitor_method:ident, $reader_method:ident, $serde_type:expr, $type_hints:expr) => {
        #[inline]
        fn $deser_method<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
            self.validator.validate($serde_type, $type_hints, 0)?;
            ensure_size(&mut self.input, mem::size_of::<$ty>())?;
            let value = self.input.$reader_method();
            visitor.$visitor_method(value)
        }
    };
}

impl<'data, Validator> Deserializer<'data> for &mut RowBinaryDeserializer<'_, 'data, Validator>
where
    Validator: ValidateDataType,
{
    type Error = Error;

    impl_num!(
        i8,
        deserialize_i8,
        visit_i8,
        get_i8,
        &SerdeType::I8,
        // TODO: shall we allow deserialization from boolean?
        &[DataTypeHint::Int8, DataTypeHint::Bool]
    );
    impl_num!(
        i16,
        deserialize_i16,
        visit_i16,
        get_i16_le,
        &SerdeType::I16,
        &[DataTypeHint::Int16]
    );
    impl_num!(
        i32,
        deserialize_i32,
        visit_i32,
        get_i32_le,
        &SerdeType::I32,
        &[DataTypeHint::Int32]
    );
    impl_num!(
        i64,
        deserialize_i64,
        visit_i64,
        get_i64_le,
        &SerdeType::I64,
        &[DataTypeHint::Int64]
    );
    impl_num!(
        i128,
        deserialize_i128,
        visit_i128,
        get_i128_le,
        &SerdeType::I128,
        &[DataTypeHint::Int128]
    );
    impl_num!(
        u8,
        deserialize_u8,
        visit_u8,
        get_u8,
        &SerdeType::U8,
        // TODO: shall we allow deserialization from boolean?
        &[DataTypeHint::Bool, DataTypeHint::UInt8]
    );
    impl_num!(
        u16,
        deserialize_u16,
        visit_u16,
        get_u16_le,
        &SerdeType::U16,
        &[DataTypeHint::UInt16]
    );
    impl_num!(
        u32,
        deserialize_u32,
        visit_u32,
        get_u32_le,
        &SerdeType::U32,
        &[DataTypeHint::UInt32]
    );
    impl_num!(
        u64,
        deserialize_u64,
        visit_u64,
        get_u64_le,
        &SerdeType::U64,
        &[DataTypeHint::UInt64]
    );
    impl_num!(
        u128,
        deserialize_u128,
        visit_u128,
        get_u128_le,
        &SerdeType::U128,
        &[DataTypeHint::UInt128]
    );
    impl_num!(
        f32,
        deserialize_f32,
        visit_f32,
        get_f32_le,
        &SerdeType::F32,
        &[DataTypeHint::Float32]
    );
    impl_num!(
        f64,
        deserialize_f64,
        visit_f64,
        get_f64_le,
        &SerdeType::F64,
        &[DataTypeHint::Float64]
    );

    #[inline]
    fn deserialize_any<V: Visitor<'data>>(self, _: V) -> Result<V::Value> {
        Err(Error::DeserializeAnyNotSupported)
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
        self.validator.validate(
            &SerdeType::Bool,
            &[
                DataTypeHint::Bool,
                // it is possible to deserialize from UInt8 0 or 1 as Boolean
                DataTypeHint::UInt8,
            ],
            0,
        )?;
        ensure_size(&mut self.input, 1)?;
        match self.input.get_u8() {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_str<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_str call");

        // TODO - which types to allow?
        self.validator
            .validate(&SerdeType::String, &[DataTypeHint::String], 0)?;
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        let str = str::from_utf8(slice).map_err(Error::from)?;
        visitor.visit_borrowed_str(str)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_string call");

        // TODO - which types to allow?
        self.validator
            .validate(&SerdeType::String, &[DataTypeHint::String], 0)?;
        let size = self.read_size()?;
        let vec = self.read_vec(size)?;
        let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
        visitor.visit_string(string)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_bytes call");

        // TODO - which types to allow?
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        visitor.visit_borrowed_bytes(slice)
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_byte_buf call");

        // TODO - which types to allow?
        let size = self.read_size()?;
        visitor.visit_byte_buf(self.read_vec(size)?)
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_identifier call");

        // TODO - which types to allow?
        self.deserialize_u8(visitor)
    }

    #[inline]
    fn deserialize_enum<V: Visitor<'data>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        // println!("deserialize_enum call");

        struct RowBinaryEnumAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
        }

        struct VariantDeserializer<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
        }

        impl<'data, Validator> VariantAccess<'data> for VariantDeserializer<'_, '_, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            type Error = Error;

            fn unit_variant(self) -> Result<()> {
                Err(Error::Unsupported("unit variants".to_string()))
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

        impl<'de, 'cursor, 'data, Validator> EnumAccess<'data>
            for RowBinaryEnumAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            type Error = Error;
            type Variant = VariantDeserializer<'de, 'cursor, 'data, Validator>;

            fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self::Variant), Self::Error>
            where
                T: DeserializeSeed<'data>,
            {
                let value = seed.deserialize(&mut *self.deserializer)?;
                let deserializer = VariantDeserializer {
                    deserializer: self.deserializer,
                };
                Ok((value, deserializer))
            }
        }

        // FIXME
        self.validator
            .validate(&SerdeType::Enum, &[DataTypeHint::Enum], 0)?;
        visitor.visit_enum(RowBinaryEnumAccess { deserializer: self })
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'data>>(self, len: usize, visitor: V) -> Result<V::Value> {
        // println!("deserialize_tuple call, len {}", len);

        struct RowBinaryTupleAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
            len: usize,
        }

        impl<'data, Validator> SeqAccess<'data> for RowBinaryTupleAccess<'_, '_, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
            where
                T: DeserializeSeed<'data>,
            {
                // println!("Processing value, len: {}", self.len);
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

        let inner_data_type_validator = self.validator.validate(
            &SerdeType::Tuple,
            &[
                DataTypeHint::Tuple,
                DataTypeHint::Array,
                DataTypeHint::FixedString,
                // FIXME: uncomment when there is a way to implement ReverseSeqAccess
                // DataTypeHint::IPv4,
                DataTypeHint::IPv6,
            ],
            len,
        )?;
        let mut new_self = RowBinaryDeserializer {
            input: self.input,
            validator: inner_data_type_validator,
        };
        let access = RowBinaryTupleAccess {
            deserializer: &mut new_self,
            len,
        };
        visitor.visit_seq(access)
    }

    #[inline]
    fn deserialize_option<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_option call");

        ensure_size(&mut self.input, 1)?;
        let inner_data_type_validator =
            self.validator
                .validate(&SerdeType::Option, &[DataTypeHint::Nullable], 0)?;
        match self.input.get_u8() {
            0 => visitor.visit_some(&mut RowBinaryDeserializer {
                input: self.input,
                validator: inner_data_type_validator,
            }),
            1 => visitor.visit_none(),
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!("deserialize_seq call");

        struct RowBinarySeqAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
            len: usize,
        }

        impl<'data, Validator> SeqAccess<'data> for RowBinarySeqAccess<'_, '_, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
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

        let len = self.read_size()?;
        let inner_data_type_validator =
            self.validator
                .validate(&SerdeType::Seq, &[DataTypeHint::Array], len)?;
        visitor.visit_seq(RowBinarySeqAccess {
            deserializer: &mut RowBinaryDeserializer {
                input: self.input,
                validator: inner_data_type_validator,
            },
            len,
        })
    }

    #[inline]
    fn deserialize_map<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // println!(
        //     "deserialize_map call",
        // );

        struct RowBinaryMapAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
            entries_visited: usize,
            len: usize,
        }

        impl<'data, Validator> MapAccess<'data> for RowBinaryMapAccess<'_, '_, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            type Error = Error;

            fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
            where
                K: DeserializeSeed<'data>,
            {
                if self.entries_visited >= self.len {
                    return Ok(None);
                }
                self.entries_visited += 1;
                seed.deserialize(&mut *self.deserializer).map(Some)
            }

            fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
            where
                V: DeserializeSeed<'data>,
            {
                seed.deserialize(&mut *self.deserializer)
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        let len = self.read_size()?;
        let inner_data_type_validator =
            self.validator
                .validate(&SerdeType::Map, &[DataTypeHint::Map], len)?;
        visitor.visit_map(RowBinaryMapAccess {
            deserializer: &mut RowBinaryDeserializer {
                input: self.input,
                validator: inner_data_type_validator,
            },
            entries_visited: 0,
            len,
        })
    }

    #[inline]
    fn deserialize_struct<V: Visitor<'data>>(
        self,
        name: &str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        // println!("deserialize_struct: {} (fields: {:?})", name, fields,);

        // FIXME use &'_ str, fix lifetimes
        self.validator.set_struct_name(name.to_string());

        // TODO: it should also support using HashMap to deserialize
        //  currently just copy-pasted to prevent former `deserialize_tuple` delegation
        struct RowBinaryStructAccess<'de, 'cursor, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Validator>,
            len: usize,
        }

        impl<'data, Validator> SeqAccess<'data> for RowBinaryStructAccess<'_, '_, 'data, Validator>
        where
            Validator: ValidateDataType,
        {
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

        visitor.visit_seq(RowBinaryStructAccess {
            deserializer: self,
            len: fields.len(),
        })
    }

    #[inline]
    fn deserialize_newtype_struct<V: Visitor<'data>>(
        self,
        _name: &str,
        visitor: V,
    ) -> Result<V::Value> {
        // TODO - skip validation?
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
