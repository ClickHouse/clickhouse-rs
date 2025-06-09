use crate::error::Result;
use crate::row_metadata::RowMetadata;
use crate::RowKind;
use clickhouse_types::data_types::{Column, DataTypeNode, DecimalType, EnumType};
use std::collections::HashMap;
use std::fmt::Display;

pub(crate) trait SchemaValidator: Sized {
    fn validate(
        &'_ mut self,
        serde_type: SerdeType,
    ) -> Result<Option<InnerDataTypeValidator<'_, '_>>>;
    fn validate_enum8_value(&mut self, value: i8);
    fn validate_enum16_value(&mut self, value: i16);
    fn set_next_variant_value(&mut self, value: u8);
    fn get_schema_index(&self, struct_idx: usize) -> usize;
    fn is_field_order_wrong(&self) -> bool;
}

pub(crate) struct DataTypeValidator<'cursor> {
    metadata: &'cursor RowMetadata,
    current_column_idx: usize,
}

impl<'cursor> DataTypeValidator<'cursor> {
    pub(crate) fn new(metadata: &'cursor RowMetadata) -> Self {
        Self {
            current_column_idx: 0,
            metadata,
        }
    }

    fn get_current_column(&self) -> Option<&Column> {
        if self.current_column_idx > 0 && self.current_column_idx <= self.metadata.columns.len() {
            // index is immediately moved to the next column after the root validator is called
            let schema_index = self.get_schema_index(self.current_column_idx - 1);
            Some(&self.metadata.columns[schema_index])
        } else {
            None
        }
    }

    fn get_current_column_name_and_type(&self) -> (String, &DataTypeNode) {
        self.get_current_column()
            .map(|c| (format!("{}.{}", self.metadata.name, c.name), &c.data_type))
            // both should be defined at this point
            .unwrap_or(("Struct".to_string(), &DataTypeNode::Bool))
    }

    fn panic_on_schema_mismatch<'de>(
        &'de self,
        data_type: &DataTypeNode,
        serde_type: &SerdeType,
        is_inner: bool,
    ) -> Result<Option<InnerDataTypeValidator<'de, 'cursor>>> {
        match self.metadata.kind {
            RowKind::Primitive => {
                panic!(
                    "While processing row as a primitive: attempting to deserialize \
                    ClickHouse type {} as {} which is not compatible",
                    data_type, serde_type
                )
            }
            RowKind::Vec => {
                panic!(
                    "While processing row as a vector: attempting to deserialize \
                    ClickHouse type {} as {} which is not compatible",
                    data_type, serde_type
                )
            }
            RowKind::Tuple => {
                panic!(
                    "While processing row as a tuple: attempting to deserialize \
                    ClickHouse type {} as {} which is not compatible",
                    data_type, serde_type
                )
            }
            RowKind::Struct => {
                if is_inner {
                    let (full_name, full_data_type) = self.get_current_column_name_and_type();
                    panic!(
                        "While processing column {} defined as {}: attempting to deserialize \
                        nested ClickHouse type {} as {} which is not compatible",
                        full_name, full_data_type, data_type, serde_type
                    )
                } else {
                    panic!(
                        "While processing column {}: attempting to deserialize \
                        ClickHouse type {} as {} which is not compatible",
                        self.get_current_column_name_and_type().0,
                        data_type,
                        serde_type
                    )
                }
            }
        }
    }
}

impl SchemaValidator for DataTypeValidator<'_> {
    #[inline]
    fn validate(
        &'_ mut self,
        serde_type: SerdeType,
    ) -> Result<Option<InnerDataTypeValidator<'_, '_>>> {
        match self.metadata.kind {
            // `fetch::<i32>` for a "primitive row" type
            RowKind::Primitive => {
                if self.current_column_idx == 0 && self.metadata.columns.len() == 1 {
                    let data_type = &self.metadata.columns[0].data_type;
                    validate_impl(self, data_type, &serde_type, false)
                } else {
                    panic!(
                        "Primitive row is expected to be a single value, got columns: {:?}",
                        self.metadata.columns
                    );
                }
            }
            // `fetch::<(i16, i32)>` or `fetch::<(T, u64)>` for a "tuple row" type
            RowKind::Tuple => {
                match serde_type {
                    SerdeType::Tuple(_) => Ok(Some(InnerDataTypeValidator {
                        root: self,
                        kind: InnerDataTypeValidatorKind::RootTuple(&self.metadata.columns, 0),
                    })),
                    _ => {
                        // should be unreachable
                        panic!(
                            "While processing tuple row: expected serde type Tuple(N), got {}",
                            serde_type
                        );
                    }
                }
            }
            // `fetch::<Vec<i32>>` for a "vector row" type
            RowKind::Vec => {
                let data_type = &self.metadata.columns[0].data_type;
                let kind = match data_type {
                    DataTypeNode::Array(inner_type) => {
                        InnerDataTypeValidatorKind::RootArray(inner_type)
                    }
                    _ => panic!(
                        "Expected Array type when validating root level sequence, but got {}",
                        self.metadata.columns[0].data_type
                    ),
                };
                Ok(Some(InnerDataTypeValidator { root: self, kind }))
            }
            // `fetch::<T>` for a "struct row" type, which is supposed to be the default flow
            RowKind::Struct => {
                if self.current_column_idx < self.metadata.columns.len() {
                    let current_column = &self.metadata.columns[self.current_column_idx];
                    self.current_column_idx += 1;
                    validate_impl(self, &current_column.data_type, &serde_type, false)
                } else {
                    panic!(
                        "Struct {} has more fields than columns in the database schema",
                        self.metadata.name
                    )
                }
            }
        }
    }

    #[inline]
    fn is_field_order_wrong(&self) -> bool {
        self.metadata.is_field_order_wrong()
    }

    #[inline]
    fn get_schema_index(&self, struct_idx: usize) -> usize {
        self.metadata.get_schema_index(struct_idx)
    }

    #[cold]
    #[inline(never)]
    fn validate_enum8_value(&mut self, _value: i8) {
        unreachable!()
    }

    #[cold]
    #[inline(never)]
    fn validate_enum16_value(&mut self, _value: i16) {
        unreachable!()
    }

    #[cold]
    #[inline(never)]
    fn set_next_variant_value(&mut self, _value: u8) {
        unreachable!()
    }
}

/// Having a ClickHouse `Map<K, V>` defined as a `HashMap<K, V>` in Rust, Serde will call:
/// - `deserialize_map`     for `Vec<(K, V)>`
/// - `deserialize_<key>`   suitable for `K`
/// - `deserialize_<value>` suitable for `V`
#[derive(Debug)]
pub(crate) enum MapValidatorState {
    Key,
    Value,
}

/// Having a ClickHouse `Map<K, V>` defined as `Vec<(K, V)>` in Rust, Serde will call:
/// - `deserialize_seq`     for `Vec<(K, V)>`
/// - `deserialize_tuple`   for `(K, V)`
/// - `deserialize_<key>`   suitable for `K`
/// - `deserialize_<value>` suitable for `V`
#[derive(Debug)]
pub(crate) enum MapAsSequenceValidatorState {
    Tuple,
    Key,
    Value,
}

pub(crate) struct InnerDataTypeValidator<'de, 'cursor> {
    root: &'de DataTypeValidator<'cursor>,
    kind: InnerDataTypeValidatorKind<'cursor>,
}

#[derive(Debug)]
pub(crate) enum InnerDataTypeValidatorKind<'cursor> {
    Array(&'cursor DataTypeNode),
    FixedString(usize),
    Map(&'cursor [Box<DataTypeNode>; 2], MapValidatorState),
    /// Allows supporting ClickHouse `Map<K, V>` defined as `Vec<(K, V)>` in Rust
    MapAsSequence(&'cursor [Box<DataTypeNode>; 2], MapAsSequenceValidatorState),
    Tuple(&'cursor [DataTypeNode]),
    /// This is a hack to support deserializing tuples/arrays (and not structs) from fetch calls
    RootTuple(&'cursor [Column], usize),
    RootArray(&'cursor DataTypeNode),
    Enum(&'cursor HashMap<i16, String>),
    Variant(&'cursor [DataTypeNode], VariantValidationState),
    Nullable(&'cursor DataTypeNode),
}

#[derive(Debug)]
pub(crate) enum VariantValidationState {
    Pending,
    Identifier(u8),
}

impl<'de, 'cursor> SchemaValidator for Option<InnerDataTypeValidator<'de, 'cursor>> {
    #[inline]
    fn validate(
        &mut self,
        serde_type: SerdeType,
    ) -> Result<Option<InnerDataTypeValidator<'de, 'cursor>>> {
        match self {
            None => Ok(None),
            Some(inner) => match &mut inner.kind {
                InnerDataTypeValidatorKind::Map(kv, state) => match state {
                    MapValidatorState::Key => {
                        let result = validate_impl(inner.root, &kv[0], &serde_type, true);
                        *state = MapValidatorState::Value;
                        result
                    }
                    MapValidatorState::Value => {
                        let result = validate_impl(inner.root, &kv[1], &serde_type, true);
                        *state = MapValidatorState::Key;
                        result
                    }
                },
                InnerDataTypeValidatorKind::MapAsSequence(kv, state) => {
                    match state {
                        // the first state is simply skipped, as the same validator
                        // will be called again for the Key and then the Value types
                        MapAsSequenceValidatorState::Tuple => {
                            *state = MapAsSequenceValidatorState::Key;
                            Ok(self.take())
                        }
                        MapAsSequenceValidatorState::Key => {
                            let result = validate_impl(inner.root, &kv[0], &serde_type, true);
                            *state = MapAsSequenceValidatorState::Value;
                            result
                        }
                        MapAsSequenceValidatorState::Value => {
                            let result = validate_impl(inner.root, &kv[1], &serde_type, true);
                            *state = MapAsSequenceValidatorState::Tuple;
                            result
                        }
                    }
                }
                InnerDataTypeValidatorKind::Array(inner_type) => {
                    validate_impl(inner.root, inner_type, &serde_type, true)
                }
                InnerDataTypeValidatorKind::Nullable(inner_type) => {
                    validate_impl(inner.root, inner_type, &serde_type, true)
                }
                InnerDataTypeValidatorKind::Tuple(elements_types) => {
                    match elements_types.split_first() {
                        Some((first, rest)) => {
                            *elements_types = rest;
                            validate_impl(inner.root, first, &serde_type, true)
                        }
                        None => {
                            let (full_name, full_data_type) =
                                inner.root.get_current_column_name_and_type();
                            panic!(
                                "While processing column {} defined as {}: \
                                attempting to deserialize {} while no more elements are allowed",
                                full_name, full_data_type, serde_type
                            )
                        }
                    }
                }
                InnerDataTypeValidatorKind::FixedString(_len) => {
                    Ok(None) // actually unreachable
                }
                InnerDataTypeValidatorKind::RootTuple(columns, current_index) => {
                    if *current_index < columns.len() {
                        let data_type = &columns[*current_index].data_type;
                        *current_index += 1;
                        validate_impl(inner.root, data_type, &serde_type, true)
                    } else {
                        let (full_name, full_data_type) =
                            inner.root.get_current_column_name_and_type();
                        panic!(
                            "While processing root tuple element {} defined as {}: \
                             attempting to deserialize {} while no more elements are allowed",
                            full_name, full_data_type, serde_type
                        )
                    }
                }
                InnerDataTypeValidatorKind::RootArray(inner_data_type) => {
                    validate_impl(inner.root, inner_data_type, &serde_type, true)
                }
                InnerDataTypeValidatorKind::Variant(possible_types, state) => match state {
                    VariantValidationState::Pending => {
                        unreachable!()
                    }
                    VariantValidationState::Identifier(value) => {
                        if *value as usize >= possible_types.len() {
                            let (full_name, full_data_type) =
                                inner.root.get_current_column_name_and_type();
                            panic!(
                                "While processing column {full_name} defined as {full_data_type}: \
                                 Variant identifier {value} is out of bounds, max allowed index is {}",
                                possible_types.len() - 1
                            );
                        }
                        let data_type = &possible_types[*value as usize];
                        validate_impl(inner.root, data_type, &serde_type, true)
                    }
                },
                // TODO - check enum string value correctness in the hashmap?
                //  is this even possible?
                InnerDataTypeValidatorKind::Enum(_values_map) => {
                    unreachable!()
                }
            },
        }
    }

    #[inline(always)]
    fn validate_enum8_value(&mut self, value: i8) {
        if let Some(inner) = self {
            if let InnerDataTypeValidatorKind::Enum(values_map) = &inner.kind {
                if !values_map.contains_key(&(value as i16)) {
                    let (full_name, full_data_type) = inner.root.get_current_column_name_and_type();
                    panic!(
                        "While processing column {full_name} defined as {full_data_type}: \
                         Enum8 value {value} is not present in the database schema"
                    );
                }
            }
        }
    }

    #[inline(always)]
    fn validate_enum16_value(&mut self, value: i16) {
        if let Some(inner) = self {
            if let InnerDataTypeValidatorKind::Enum(values_map) = &inner.kind {
                if !values_map.contains_key(&value) {
                    let (full_name, full_data_type) = inner.root.get_current_column_name_and_type();
                    panic!(
                        "While processing column {full_name} defined as {full_data_type}: \
                         Enum16 value {value} is not present in the database schema"
                    );
                }
            }
        }
    }

    #[inline(always)]
    fn set_next_variant_value(&mut self, value: u8) {
        if let Some(inner) = self {
            if let InnerDataTypeValidatorKind::Variant(possible_types, state) = &mut inner.kind {
                if (value as usize) < possible_types.len() {
                    *state = VariantValidationState::Identifier(value);
                } else {
                    let (full_name, full_data_type) = inner.root.get_current_column_name_and_type();
                    panic!(
                        "While processing column {full_name} defined as {full_data_type}: \
                         Variant identifier {value} is out of bounds, max allowed index is {}",
                        possible_types.len() - 1
                    );
                }
            }
        }
    }

    #[inline(always)]
    fn is_field_order_wrong(&self) -> bool {
        false
    }

    fn get_schema_index(&self, _struct_idx: usize) -> usize {
        unreachable!()
    }
}

impl Drop for InnerDataTypeValidator<'_, '_> {
    fn drop(&mut self) {
        if let InnerDataTypeValidatorKind::Tuple(elements_types) = self.kind {
            if !elements_types.is_empty() {
                let (column_name, column_type) = self.root.get_current_column_name_and_type();
                panic!(
                    "While processing column {} defined as {}: tuple was not fully deserialized; \
                    remaining elements: {}; likely, the field definition is incomplete",
                    column_name,
                    column_type,
                    elements_types
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
        }
    }
}

// TODO: is there a way to eliminate multiple branches with similar patterns?
//  static/const dispatch?
//  separate smaller inline functions?
#[inline]
fn validate_impl<'de, 'cursor>(
    root: &'de DataTypeValidator<'cursor>,
    column_data_type: &'cursor DataTypeNode,
    serde_type: &SerdeType,
    is_inner: bool,
) -> Result<Option<InnerDataTypeValidator<'de, 'cursor>>> {
    let data_type = column_data_type.remove_low_cardinality();
    match serde_type {
        SerdeType::Bool
            if data_type == &DataTypeNode::Bool || data_type == &DataTypeNode::UInt8 =>
        {
            Ok(None)
        }
        SerdeType::I8 => match data_type {
            DataTypeNode::Int8 => Ok(None),
            DataTypeNode::Enum(EnumType::Enum8, values_map) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Enum(values_map),
            })),
            _ => root.panic_on_schema_mismatch(data_type, serde_type, is_inner),
        },
        SerdeType::I16 => match data_type {
            DataTypeNode::Int16 => Ok(None),
            DataTypeNode::Enum(EnumType::Enum16, values_map) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Enum(values_map),
            })),
            _ => root.panic_on_schema_mismatch(data_type, serde_type, is_inner),
        },
        SerdeType::I32
            if data_type == &DataTypeNode::Int32
                || data_type == &DataTypeNode::Date32
                || matches!(
                    data_type,
                    DataTypeNode::Decimal(_, _, DecimalType::Decimal32)
                ) =>
        {
            Ok(None)
        }
        SerdeType::I64
            if data_type == &DataTypeNode::Int64
                || matches!(data_type, DataTypeNode::DateTime64(_, _))
                || matches!(
                    data_type,
                    DataTypeNode::Decimal(_, _, DecimalType::Decimal64)
                ) =>
        {
            Ok(None)
        }
        SerdeType::I128
            if data_type == &DataTypeNode::Int128
                || matches!(
                    data_type,
                    DataTypeNode::Decimal(_, _, DecimalType::Decimal128)
                ) =>
        {
            Ok(None)
        }
        SerdeType::U8 if data_type == &DataTypeNode::UInt8 => Ok(None),
        SerdeType::U16
            if data_type == &DataTypeNode::UInt16 || data_type == &DataTypeNode::Date =>
        {
            Ok(None)
        }
        SerdeType::U32
            if data_type == &DataTypeNode::UInt32
                || matches!(data_type, DataTypeNode::DateTime(_))
                || data_type == &DataTypeNode::IPv4 =>
        {
            Ok(None)
        }
        SerdeType::U64 if data_type == &DataTypeNode::UInt64 => Ok(None),
        SerdeType::U128 if data_type == &DataTypeNode::UInt128 => Ok(None),
        SerdeType::F32 if data_type == &DataTypeNode::Float32 => Ok(None),
        SerdeType::F64 if data_type == &DataTypeNode::Float64 => Ok(None),
        SerdeType::Str | SerdeType::String
            if data_type == &DataTypeNode::String || data_type == &DataTypeNode::JSON =>
        {
            Ok(None)
        }
        // allows to work with BLOB strings as well
        SerdeType::Bytes(_) | SerdeType::ByteBuf(_) if data_type == &DataTypeNode::String => {
            Ok(None)
        }
        SerdeType::Option => {
            if let DataTypeNode::Nullable(inner_type) = data_type {
                Ok(Some(InnerDataTypeValidator {
                    root,
                    kind: InnerDataTypeValidatorKind::Nullable(inner_type),
                }))
            } else {
                root.panic_on_schema_mismatch(data_type, serde_type, is_inner)
            }
        }
        SerdeType::Seq(_) => match data_type {
            DataTypeNode::Array(inner_type) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(inner_type),
            })),
            // A map can be defined as `Vec<(K, V)>` in the struct
            DataTypeNode::Map(kv) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::MapAsSequence(
                    kv,
                    MapAsSequenceValidatorState::Tuple,
                ),
            })),
            DataTypeNode::Ring => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::Point),
            })),
            DataTypeNode::Polygon => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::Ring),
            })),
            DataTypeNode::MultiPolygon => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::Polygon),
            })),
            DataTypeNode::LineString => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::Point),
            })),
            DataTypeNode::MultiLineString => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::LineString),
            })),
            _ => root.panic_on_schema_mismatch(data_type, serde_type, is_inner),
        },
        SerdeType::Tuple(len) => match data_type {
            DataTypeNode::FixedString(n) => {
                if n == len {
                    Ok(Some(InnerDataTypeValidator {
                        root,
                        kind: InnerDataTypeValidatorKind::FixedString(*n),
                    }))
                } else {
                    let (full_name, full_data_type) = root.get_current_column_name_and_type();
                    panic!(
                        "While processing column {} defined as {}: attempting to deserialize \
                        nested ClickHouse type {} as {}",
                        full_name, full_data_type, data_type, serde_type,
                    )
                }
            }
            DataTypeNode::Tuple(elements) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Tuple(elements),
            })),
            DataTypeNode::Array(inner_type) => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(inner_type),
            })),
            DataTypeNode::IPv6 => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(&DataTypeNode::UInt8),
            })),
            DataTypeNode::UUID => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Tuple(UUID_TUPLE_ELEMENTS),
            })),
            DataTypeNode::Point => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Tuple(POINT_TUPLE_ELEMENTS),
            })),
            _ => root.panic_on_schema_mismatch(data_type, serde_type, is_inner),
        },
        SerdeType::Map(_) => {
            if let DataTypeNode::Map(kv) = data_type {
                Ok(Some(InnerDataTypeValidator {
                    root,
                    kind: InnerDataTypeValidatorKind::Map(kv, MapValidatorState::Key),
                }))
            } else {
                panic!(
                    "Expected Map for {} call, but got {}",
                    serde_type, data_type
                )
            }
        }
        SerdeType::Enum => {
            if let DataTypeNode::Variant(possible_types) = data_type {
                Ok(Some(InnerDataTypeValidator {
                    root,
                    kind: InnerDataTypeValidatorKind::Variant(
                        possible_types,
                        VariantValidationState::Pending,
                    ),
                }))
            } else {
                panic!(
                    "Expected Variant for {} call, but got {}",
                    serde_type, data_type
                )
            }
        }

        _ => root.panic_on_schema_mismatch(
            data_type,
            serde_type,
            is_inner || matches!(column_data_type, DataTypeNode::LowCardinality { .. }),
        ),
    }
}

impl SchemaValidator for () {
    #[inline(always)]
    fn validate(
        &mut self,
        _serde_type: SerdeType,
    ) -> Result<Option<InnerDataTypeValidator<'_, '_>>> {
        Ok(None)
    }

    #[inline(always)]
    fn validate_enum8_value(&mut self, _value: i8) {}

    #[inline(always)]
    fn validate_enum16_value(&mut self, _value: i16) {}

    #[inline(always)]
    fn set_next_variant_value(&mut self, _value: u8) {}

    #[inline(always)]
    fn is_field_order_wrong(&self) -> bool {
        false
    }

    fn get_schema_index(&self, _struct_idx: usize) -> usize {
        unreachable!()
    }
}

/// Which Serde data type (De)serializer used for the given type.
/// Displays into certain Rust types for convenience in errors reporting.
/// See also: available methods in [`serde::Serializer`] and [`serde::Deserializer`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SerdeType {
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
    Str,
    String,
    Option,
    Enum,
    Bytes(usize),
    ByteBuf(usize),
    Tuple(usize),
    Seq(usize),
    Map(usize),
    // Identifier,
    // Char,
    // Unit,
    // Struct,
    // NewtypeStruct,
    // TupleStruct,
    // UnitStruct,
    // IgnoredAny,
}

impl Display for SerdeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerdeType::Bool => write!(f, "bool"),
            SerdeType::I8 => write!(f, "i8"),
            SerdeType::I16 => write!(f, "i16"),
            SerdeType::I32 => write!(f, "i32"),
            SerdeType::I64 => write!(f, "i64"),
            SerdeType::I128 => write!(f, "i128"),
            SerdeType::U8 => write!(f, "u8"),
            SerdeType::U16 => write!(f, "u16"),
            SerdeType::U32 => write!(f, "u32"),
            SerdeType::U64 => write!(f, "u64"),
            SerdeType::U128 => write!(f, "u128"),
            SerdeType::F32 => write!(f, "f32"),
            SerdeType::F64 => write!(f, "f64"),
            SerdeType::Str => write!(f, "&str"),
            SerdeType::String => write!(f, "String"),
            SerdeType::Bytes(len) => write!(f, "&[u8; {len}]"),
            SerdeType::ByteBuf(_len) => write!(f, "Vec<u8>"),
            SerdeType::Option => write!(f, "Option<T>"),
            SerdeType::Enum => write!(f, "enum"),
            SerdeType::Seq(_len) => write!(f, "Vec<T>"),
            SerdeType::Tuple(len) => write!(f, "a tuple or sequence with length {len}"),
            SerdeType::Map(_len) => write!(f, "Map<K, V>"),
            // SerdeType::Identifier => "identifier",
            // SerdeType::Char => "char",
            // SerdeType::Unit => "()",
            // SerdeType::Struct => "struct",
            // SerdeType::NewtypeStruct => "newtype struct",
            // SerdeType::TupleStruct => "tuple struct",
            // SerdeType::UnitStruct => "unit struct",
            // SerdeType::IgnoredAny => "ignored any",
        }
    }
}

const UUID_TUPLE_ELEMENTS: &[DataTypeNode; 2] = &[DataTypeNode::UInt64, DataTypeNode::UInt64];
const POINT_TUPLE_ELEMENTS: &[DataTypeNode; 2] = &[DataTypeNode::Float64, DataTypeNode::Float64];
