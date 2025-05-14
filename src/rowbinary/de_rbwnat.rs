use crate::error::{Error, Result};
use crate::rowbinary::de::RowBinaryDeserializer;
use clickhouse_rowbinary::types::{Column, DataTypeNode};
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt::Display;
use std::ops::Deref;
use std::rc::Rc;

pub(crate) fn deserialize_from_rbwnat<'data, 'cursor, T: Deserialize<'data>>(
    input: &mut &'data [u8],
    columns: &'cursor [Column],
) -> Result<T> {
    // println!("[RBWNAT] deserializing with names and types: {:?}, input size: {}", columns, input.len());
    let mut deserializer = RowBinaryWithNamesAndTypesDeserializer::new(input, columns)?;
    let value = T::deserialize(&mut deserializer);
    // println!("Remaining input size: {}", input.len());
    value
}

/// Serde method that delegated the value deserialization to [`Deserializer::deserialize_any`].
#[derive(Clone, Debug, PartialEq)]
enum DelegatedFrom {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Char,
    Str,
    String,
    Bytes,
    ByteBuf,
    Option,
    Unit,
    UnitStruct,
    NewtypeStruct,
    Seq,
    Tuple,
    TupleStruct,
    Map,
    Struct,
    Enum,
    Identifier,
    IgnoredAny,
}

impl Default for DelegatedFrom {
    fn default() -> Self {
        DelegatedFrom::Struct
    }
}

impl Display for DelegatedFrom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_name = match self {
            DelegatedFrom::Bool => "bool",
            DelegatedFrom::I8 => "i8",
            DelegatedFrom::I16 => "i16",
            DelegatedFrom::I32 => "i32",
            DelegatedFrom::I64 => "i64",
            DelegatedFrom::I128 => "i128",
            DelegatedFrom::U8 => "u8",
            DelegatedFrom::U16 => "u16",
            DelegatedFrom::U32 => "u32",
            DelegatedFrom::U64 => "u64",
            DelegatedFrom::U128 => "u128",
            DelegatedFrom::F32 => "f32",
            DelegatedFrom::F64 => "f64",
            DelegatedFrom::Char => "char",
            DelegatedFrom::Str => "&str",
            DelegatedFrom::String => "String",
            DelegatedFrom::Bytes => "&[u8]",
            DelegatedFrom::ByteBuf => "Vec<u8>",
            DelegatedFrom::Option => "Option<T>",
            DelegatedFrom::Unit => "()",
            DelegatedFrom::UnitStruct => "unit struct",
            DelegatedFrom::NewtypeStruct => "newtype struct",
            DelegatedFrom::Seq => "Vec<T>",
            DelegatedFrom::Tuple => "tuple",
            DelegatedFrom::TupleStruct => "tuple struct",
            DelegatedFrom::Map => "map",
            DelegatedFrom::Struct => "struct",
            DelegatedFrom::Enum => "enum",
            DelegatedFrom::Identifier => "identifier",
            DelegatedFrom::IgnoredAny => "ignored any",
        };
        write!(f, "{}", type_name)
    }
}

#[derive(Clone, Debug)]
enum DeserializerState<'cursor> {
    /// At this point, we are either processing a "simple" column (e.g., `UInt32`, `String`, etc.),
    /// or starting to process a more complex one (e.g., `Array(T)`, `Map(K, V)`, etc.).
    TopLevelColumn(&'cursor Column),
    /// Processing a column with a complex type (e.g., `Array(T)`), and we've got what `T` is.
    /// We can use this to verify the inner type definition in the struct.
    InnerDataType {
        column: &'cursor Column,
        prev_state: Rc<DeserializerState<'cursor>>,
        inner_data_type: &'cursor DataTypeNode,
    },
    /// We are done with all columns and should not try to deserialize anything else.
    EndOfStruct,
}

pub(crate) struct RowBinaryWithNamesAndTypesDeserializer<'cursor, 'data> {
    row_binary: RowBinaryDeserializer<'cursor, 'data>,
    state: DeserializerState<'cursor>,
    columns: &'cursor [Column],
    current_column_idx: usize,
    // main usage is to check if the struct field definition is compatible with the expected one
    last_delegated_from: DelegatedFrom,
    // every deserialization begins from a struct with some name
    struct_name: Option<&'static str>,
    struct_fields: Option<&'static [&'static str]>,
}

impl<'cursor, 'data> RowBinaryWithNamesAndTypesDeserializer<'cursor, 'data> {
    #[inline]
    fn new(
        input: &'cursor mut &'data [u8],
        columns: &'cursor [Column],
    ) -> Result<RowBinaryWithNamesAndTypesDeserializer<'cursor, 'data>> {
        if columns.is_empty() {
            // unlikely - should be validated by the columns parser already
            panic!("Zero columns definitions in the response");
        }
        Ok(RowBinaryWithNamesAndTypesDeserializer {
            row_binary: RowBinaryDeserializer { input },
            state: DeserializerState::TopLevelColumn(&columns[0]),
            last_delegated_from: DelegatedFrom::default(),
            current_column_idx: 0,
            struct_name: None,
            struct_fields: None,
            columns,
        })
    }

    #[inline]
    fn set_last_delegated_from(&mut self, from: DelegatedFrom) {
        if self.last_delegated_from != from {
            self.last_delegated_from = from;
        }
    }

    #[inline]
    fn set_struct_name(&mut self, name: &'static str) {
        // TODO: nested structs support?
        if self.struct_name.is_none() {
            self.struct_name = Some(name);
        }
    }

    #[inline]
    fn set_struct_fields(&mut self, fields: &'static [&'static str]) {
        // TODO: nested structs support?
        if self.struct_fields.is_none() {
            self.struct_fields = Some(fields);
        }
    }

    #[inline]
    fn advance_state(&mut self) -> Result<()> {
        match &self.state {
            DeserializerState::TopLevelColumn { .. } => {
                self.current_column_idx += 1;
                if self.current_column_idx >= self.columns.len() {
                    self.state = DeserializerState::EndOfStruct;
                } else {
                    let current_col = self.get_current_column()?;
                    self.state = DeserializerState::TopLevelColumn(current_col);
                }
            }
            DeserializerState::EndOfStruct => {
                panic!("trying to advance the current column index after full deserialization");
            }
            // skipping this when processing inner data types with more than one nesting level
            _ => {}
        }
        Ok(())
    }

    #[inline]
    fn set_inner_data_type_state(&mut self, inner_data_type: &'cursor DataTypeNode) {
        match self.state {
            DeserializerState::TopLevelColumn(column, ..)
            | DeserializerState::InnerDataType { column, .. } => {
                self.state = DeserializerState::InnerDataType {
                    prev_state: Rc::new(self.state.clone()),
                    inner_data_type,
                    column,
                }
            }
            _ => {
                panic!("to_inner called on invalid state");
            }
        }
    }

    #[inline]
    fn set_previous_state(&mut self) {
        match &self.state {
            DeserializerState::InnerDataType { prev_state, .. } => {
                self.state = prev_state.deref().clone()
            }
            _ => panic!("to_prev_state called on invalid state"),
        }
    }

    #[inline]
    fn get_current_data_type(&self) -> Result<&'cursor DataTypeNode> {
        match self.state {
            DeserializerState::TopLevelColumn(col, ..) => Ok(&col.data_type),
            DeserializerState::InnerDataType {
                inner_data_type, ..
            } => Ok(inner_data_type),
            DeserializerState::EndOfStruct => Err(Error::DeserializeCallAfterEndOfStruct),
        }
    }

    #[inline]
    fn get_current_column(&mut self) -> Result<&'cursor Column> {
        if self.current_column_idx >= self.columns.len() {
            return Err(Error::TooManyStructFields(
                self.current_column_idx,
                Vec::from(self.columns),
            ));
        }
        let col = &self.columns[self.current_column_idx];
        Ok(col)
    }

    #[inline]
    fn check_data_type_is_allowed(&mut self, allowed: &[DelegatedFrom]) -> Result<()> {
        if !allowed.contains(&self.last_delegated_from) {
            let column = self.get_current_column()?;
            let field_name = match self.struct_name {
                Some(struct_name) => format!("{}.{}", struct_name, column.name),
                None => column.name.to_string(),
            };
            let allowed_types = allowed
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            let all_columns = self
                .columns
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            let unexpected_type = self.last_delegated_from.to_string();
            Err(Error::DataTypeMismatch {
                field_name,
                allowed_types,
                unexpected_type,
                all_columns,
            })
        } else {
            Ok(())
        }
    }
}

macro_rules! rbwnat_deserialize_any {
    ($self:ident, $delegated_from:expr, $visitor:ident) => {{
        $self.set_last_delegated_from($delegated_from);
        $self.deserialize_any($visitor)
    }};
}

impl<'data> Deserializer<'data> for &mut RowBinaryWithNamesAndTypesDeserializer<'_, 'data> {
    type Error = Error;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        macro_rules! rbwnat_de_simple_with_type_check {
            ($delegate:ident, $compatible:expr) => {{
                self.check_data_type_is_allowed(&$compatible)?;
                self.row_binary.$delegate(visitor)
            }};
        }

        println!("{} state: {:?}", self.last_delegated_from, self.state);
        let data_type = self.get_current_data_type()?;
        let result = match data_type {
            DataTypeNode::Bool => rbwnat_de_simple_with_type_check!(
                deserialize_bool,
                [DelegatedFrom::Bool, DelegatedFrom::U8, DelegatedFrom::I8]
            ),
            DataTypeNode::UInt8 => {
                rbwnat_de_simple_with_type_check!(deserialize_u8, [DelegatedFrom::U8])
            }
            DataTypeNode::Int8 => {
                rbwnat_de_simple_with_type_check!(deserialize_i8, [DelegatedFrom::I8])
            }
            DataTypeNode::Int16 => {
                rbwnat_de_simple_with_type_check!(deserialize_i16, [DelegatedFrom::I16])
            }
            DataTypeNode::Int32 => {
                rbwnat_de_simple_with_type_check!(deserialize_i32, [DelegatedFrom::I32])
            }
            DataTypeNode::Int64 => {
                rbwnat_de_simple_with_type_check!(deserialize_i64, [DelegatedFrom::I64])
            }
            DataTypeNode::Int128 => {
                rbwnat_de_simple_with_type_check!(deserialize_i128, [DelegatedFrom::I128])
            }
            DataTypeNode::UInt16 => {
                rbwnat_de_simple_with_type_check!(deserialize_u16, [DelegatedFrom::U16])
            }
            DataTypeNode::UInt32 => {
                rbwnat_de_simple_with_type_check!(deserialize_u32, [DelegatedFrom::U32])
            }
            DataTypeNode::UInt64 => {
                rbwnat_de_simple_with_type_check!(deserialize_u64, [DelegatedFrom::U64])
            }
            DataTypeNode::UInt128 => {
                rbwnat_de_simple_with_type_check!(deserialize_u128, [DelegatedFrom::U128])
            }
            DataTypeNode::Float32 => {
                rbwnat_de_simple_with_type_check!(deserialize_f32, [DelegatedFrom::F32])
            }
            DataTypeNode::Float64 => {
                rbwnat_de_simple_with_type_check!(deserialize_f64, [DelegatedFrom::F64])
            }
            DataTypeNode::String => {
                rbwnat_de_simple_with_type_check!(
                    deserialize_str,
                    [DelegatedFrom::Str, DelegatedFrom::String]
                )
            }
            DataTypeNode::FixedString(len) => match self.last_delegated_from {
                DelegatedFrom::Bytes => visitor.visit_bytes(self.row_binary.read_slice(*len)?),
                DelegatedFrom::ByteBuf => visitor.visit_byte_buf(self.row_binary.read_vec(*len)?),
                _ => unreachable!(),
            },
            DataTypeNode::UUID => {
                rbwnat_de_simple_with_type_check!(
                    deserialize_str,
                    [DelegatedFrom::Str, DelegatedFrom::String]
                )
            }
            DataTypeNode::Date => {
                rbwnat_de_simple_with_type_check!(deserialize_u16, [DelegatedFrom::U16])
            }
            DataTypeNode::Date32 => {
                rbwnat_de_simple_with_type_check!(deserialize_i32, [DelegatedFrom::I32])
            }
            DataTypeNode::DateTime { .. } => {
                rbwnat_de_simple_with_type_check!(deserialize_u32, [DelegatedFrom::U32])
            }
            DataTypeNode::DateTime64 { .. } => {
                rbwnat_de_simple_with_type_check!(deserialize_i64, [DelegatedFrom::I64])
            }
            DataTypeNode::IPv4 => {
                rbwnat_de_simple_with_type_check!(deserialize_u32, [DelegatedFrom::U32])
            }
            DataTypeNode::IPv6 => self.row_binary.deserialize_tuple(16, visitor),

            DataTypeNode::Array(inner_type) => {
                self.check_data_type_is_allowed(&[DelegatedFrom::Seq])?;
                let len = self.row_binary.read_size()?;
                self.set_inner_data_type_state(inner_type);

                struct AnyArrayAccess<'de, 'cursor, 'data> {
                    deserializer: &'de mut RowBinaryWithNamesAndTypesDeserializer<'cursor, 'data>,
                    remaining: usize,
                }

                impl<'data> SeqAccess<'data> for AnyArrayAccess<'_, '_, 'data> {
                    type Error = Error;

                    fn next_element_seed<T>(
                        &mut self,
                        seed: T,
                    ) -> Result<Option<T::Value>, Self::Error>
                    where
                        T: DeserializeSeed<'data>,
                    {
                        if self.remaining == 0 {
                            return Ok(None);
                        }

                        self.remaining -= 1;
                        seed.deserialize(&mut *self.deserializer).map(Some)
                    }

                    fn size_hint(&self) -> Option<usize> {
                        Some(self.remaining)
                    }
                }

                let result = visitor.visit_seq(AnyArrayAccess {
                    deserializer: self,
                    remaining: len,
                });
                // if we are processing `Array(String)`, the state has `String` as expected type
                // revert it back to `Array(String)`
                self.set_previous_state();
                result
            }
            // DataTypeNode::Nullable(inner_type) => {
            //     self.check_data_type_is_allowed(&[DelegatedFrom::Option])?;
            //     self.set_inner_data_type_state(inner_type);
            //     self.row_binary.deserialize_option(visitor)
            // },
            _ => panic!("unsupported type for deserialize_any: {:?}", self.columns),
        };
        result.and_then(|value| {
            self.advance_state()?;
            Ok(value)
        })
    }

    #[inline]
    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Bool, visitor)
    }

    #[inline]
    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::I8, visitor)
    }

    #[inline]
    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::I16, visitor)
    }

    #[inline]
    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::I32, visitor)
    }

    #[inline]
    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::I64, visitor)
    }

    #[inline]
    fn deserialize_i128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::I128, visitor)
    }

    #[inline]
    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::U8, visitor)
    }

    #[inline]
    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::U16, visitor)
    }

    #[inline]
    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::U32, visitor)
    }

    #[inline]
    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::U64, visitor)
    }

    #[inline]
    fn deserialize_u128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::U128, visitor)
    }

    #[inline]
    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::F32, visitor)
    }

    #[inline]
    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::F64, visitor)
    }

    #[inline]
    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Char, visitor)
    }

    #[inline]
    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Str, visitor)
    }

    #[inline]
    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::String, visitor)
    }

    #[inline]
    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Bytes, visitor)
    }

    #[inline]
    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::ByteBuf, visitor)
    }

    #[inline]
    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Option, visitor)
    }

    #[inline]
    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Unit, visitor)
    }

    #[inline]
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::UnitStruct, visitor)
    }

    #[inline]
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::NewtypeStruct, visitor)
    }

    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Seq, visitor)
    }

    #[inline]
    fn deserialize_tuple<V>(
        self,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Tuple, visitor)
    }

    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::TupleStruct, visitor)
    }

    #[inline]
    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Map, visitor)
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        struct StructAccess<'de, 'cursor, 'data> {
            deserializer: &'de mut RowBinaryWithNamesAndTypesDeserializer<'cursor, 'data>,
            len: usize,
        }

        impl<'data> SeqAccess<'data> for StructAccess<'_, '_, 'data> {
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

        self.set_struct_name(name);
        self.set_struct_fields(fields);
        visitor.visit_seq(StructAccess {
            deserializer: self,
            len: fields.len(),
        })
    }

    #[inline]
    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Enum, visitor)
    }

    #[inline]
    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::Identifier, visitor)
    }

    #[inline]
    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'data>,
    {
        rbwnat_deserialize_any!(self, DelegatedFrom::IgnoredAny, visitor)
    }

    #[inline]
    fn is_human_readable(&self) -> bool {
        false
    }
}
