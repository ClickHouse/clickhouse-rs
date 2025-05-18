use crate::error::{Error, Result};
use crate::rowbinary::utils::{ensure_size, get_unsigned_leb128};
use crate::rowbinary::SerdeType;
use bytes::Buf;
use clickhouse_rowbinary::types::{Column, DataTypeHint};
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
    let mut deserializer = RowBinaryDeserializer {
        input,
        columns_validator: (),
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
        columns_validator: ColumnsValidator {
            columns,
            col_idx: 0,
            type_hint_idx: 0,
            nesting_level: 0,
        },
    };
    T::deserialize(&mut deserializer)
}

struct ColumnsValidator<'cursor> {
    columns: &'cursor [Column],
    col_idx: usize,
    type_hint_idx: usize,
    nesting_level: usize,
}

impl<'cursor> ColumnsValidator<'cursor> {
    #[inline]
    fn advance(&mut self) {
        self.col_idx += 1;
        self.type_hint_idx = 0;
    }
}

pub(crate) trait ValidateDataType {
    fn validate(
        &mut self,
        serde_type: &'static SerdeType,
        allowed: &'static [DataTypeHint],
        has_inner_type: bool,
    ) -> Result<()>;
    fn skip_next(&mut self) -> ();
    fn increase_nesting(&mut self) -> ();
    fn decrease_nesting(&mut self) -> ();
}

impl ValidateDataType for () {
    #[inline]
    fn validate(
        &mut self,
        _serde_type: &'static SerdeType,
        _allowed: &'static [DataTypeHint],
        _has_inner_type: bool,
    ) -> Result<()> {
        Ok(())
    }
    #[inline]
    fn skip_next(&mut self) -> () {}
    #[inline]
    fn increase_nesting(&mut self) -> () {}
    #[inline]
    fn decrease_nesting(&mut self) -> () {}
}

impl<'cursor> ValidateDataType for ColumnsValidator<'cursor> {
    #[inline]
    fn validate(
        &mut self,
        serde_type: &'static SerdeType,
        allowed: &'static [DataTypeHint],
        has_inner_type: bool,
    ) -> Result<()> {
        println!(
            "Validating column {}, type hint {}, serde type {}, allowed {:?}, nesting level {}",
            self.col_idx, self.type_hint_idx, serde_type, allowed, self.nesting_level
        );
        if self.col_idx >= self.columns.len() {
            return Err(Error::TooManyStructFields(
                self.columns.len(),
                self.columns.into(),
            ));
        }
        if has_inner_type {
            self.nesting_level += 1;
            println!("Increased nesting level to {}", self.nesting_level);
        }
        let current_column = &self.columns[self.col_idx];
        if self.type_hint_idx >= current_column.type_hints.len() {
            // if self.nesting_level == 0 {
            //     println!("Advancing #1");
            //     self.advance();
            // }
            println!(
                "Skipping check for column {}, type hint {}, nesting level {}",
                current_column.name, self.type_hint_idx, self.nesting_level
            );
            return Ok(());
        }
        let db_type_hint = &current_column.type_hints[self.type_hint_idx];
        if allowed.contains(db_type_hint) {
            // self.type_hint_idx += 1;
            if self.nesting_level == 0 {
                println!("Advancing #2");
                self.advance();
            } else {
                self.type_hint_idx += 1;
            }
            println!(
                "Checked column {}, type hint {}, nesting level {}",
                current_column.name, self.type_hint_idx, self.nesting_level
            );
            Ok(())
        } else {
            Err(Error::InvalidColumnDataType(
                format!("column {} {} requires deserialization from {} as {}, but serde call allowed only for {:?}",
                        current_column.name, current_column.data_type, db_type_hint, serde_type, allowed)))
        }
    }

    #[inline]
    fn skip_next(&mut self) {
        self.type_hint_idx += 1;
    }

    #[inline]
    fn increase_nesting(&mut self) {
        self.nesting_level += 1;
    }

    #[inline]
    fn decrease_nesting(&mut self) {
        println!("Decreasing nesting level from {}", self.nesting_level);
        if self.nesting_level == 1 {
            self.advance();
        } else if self.nesting_level > 0 {
            self.nesting_level -= 1;
        } else {
            panic!("decrease_nesting called when nesting level is already 0, current column index {}, type hint index {}, all columns: {:?}",
                   self.col_idx, self.type_hint_idx, self.columns);
        }
    }
}

/// A deserializer for the RowBinary format.
///
/// See https://clickhouse.com/docs/en/interfaces/formats#rowbinary for details.
pub(crate) struct RowBinaryDeserializer<'cursor, 'data, Columns = ()>
where
    Columns: ValidateDataType,
{
    pub(crate) columns_validator: Columns,
    pub(crate) input: &'cursor mut &'data [u8],
}

impl<'data, Columns> RowBinaryDeserializer<'_, 'data, Columns>
where
    Columns: ValidateDataType,
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
            self.columns_validator
                .validate($serde_type, $type_hints, false)?;
            ensure_size(&mut self.input, mem::size_of::<$ty>())?;
            let value = self.input.$reader_method();
            visitor.$visitor_method(value)
        }
    };
}

impl<'data, Columns> Deserializer<'data> for &mut RowBinaryDeserializer<'_, 'data, Columns>
where
    Columns: ValidateDataType,
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
        self.columns_validator.validate(
            &SerdeType::Bool,
            // TODO: shall we allow deserialization from integers?
            &[DataTypeHint::Bool, DataTypeHint::Int8, DataTypeHint::UInt8],
            false,
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
        // TODO - which types to allow?
        self.columns_validator
            .validate(&SerdeType::String, &[DataTypeHint::String], false)?;
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        let str = str::from_utf8(slice).map_err(Error::from)?;
        visitor.visit_borrowed_str(str)
    }

    #[inline]
    fn deserialize_string<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // TODO - which types to allow?
        self.columns_validator
            .validate(&SerdeType::String, &[DataTypeHint::String], false)?;
        let size = self.read_size()?;
        let vec = self.read_vec(size)?;
        let string = String::from_utf8(vec).map_err(|err| Error::from(err.utf8_error()))?;
        visitor.visit_string(string)
    }

    #[inline]
    fn deserialize_bytes<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // TODO - which types to allow?
        let size = self.read_size()?;
        let slice = self.read_slice(size)?;
        visitor.visit_borrowed_bytes(slice)
    }

    #[inline]
    fn deserialize_byte_buf<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        // TODO - which types to allow?
        let size = self.read_size()?;
        visitor.visit_byte_buf(self.read_vec(size)?)
    }

    #[inline]
    fn deserialize_identifier<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
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
        struct Access<'de, 'cursor, 'data, Columns>
        where
            Columns: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Columns>,
        }
        struct VariantDeserializer<'de, 'cursor, 'data, Columns>
        where
            Columns: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Columns>,
        }
        impl<'data, Columns> VariantAccess<'data> for VariantDeserializer<'_, '_, 'data, Columns>
        where
            Columns: ValidateDataType,
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

        impl<'de, 'cursor, 'data, Columns> EnumAccess<'data> for Access<'de, 'cursor, 'data, Columns>
        where
            Columns: ValidateDataType,
        {
            type Error = Error;
            type Variant = VariantDeserializer<'de, 'cursor, 'data, Columns>;

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
        self.columns_validator
            .validate(&SerdeType::Enum, &[DataTypeHint::Enum], false)?;
        visitor.visit_enum(Access { deserializer: self })
    }

    #[inline]
    fn deserialize_tuple<V: Visitor<'data>>(self, len: usize, visitor: V) -> Result<V::Value> {
        struct Access<'de, 'cursor, 'data, Columns>
        where
            Columns: ValidateDataType,
        {
            deserializer: &'de mut RowBinaryDeserializer<'cursor, 'data, Columns>,
            len: usize,
        }

        impl<'data, Columns> SeqAccess<'data> for Access<'_, '_, 'data, Columns>
        where
            Columns: ValidateDataType,
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

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    #[inline]
    fn deserialize_option<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        self.columns_validator
            .validate(&SerdeType::Option, &[DataTypeHint::Nullable], true)?;
        ensure_size(&mut self.input, 1)?;
        match self.input.get_u8() {
            0 => visitor.visit_some(&mut *self),
            1 => {
                self.columns_validator.skip_next();
                self.columns_validator.decrease_nesting();
                visitor.visit_none()
            }
            v => Err(Error::InvalidTagEncoding(v as usize)),
        }
    }

    #[inline]
    fn deserialize_seq<V: Visitor<'data>>(self, visitor: V) -> Result<V::Value> {
        self.columns_validator
            .validate(&SerdeType::Seq, &[DataTypeHint::Array], true)?;
        let len = self.read_size()?;
        let result = self.deserialize_tuple(len, visitor);
        self.columns_validator.decrease_nesting();
        result
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
