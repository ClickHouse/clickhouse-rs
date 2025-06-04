use crate::error::Result;
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
    fn ensure_struct_metadata(
        &'_ mut self,
        name: &'static str,
        fields: &'static [&'static str],
    ) -> bool;
    fn get_schema_index(&self, struct_idx: usize) -> usize;
}

#[derive(Debug, PartialEq)]
enum StructMetadataState {
    Pending,
    WithSeqAccess,
    WithMapAccess(Vec<usize>),
}

/// #### StructMetadata
///
/// Should reside outside the (de)serializer, so it is calculated only once per struct.
/// No lifetimes, so it does not introduce a breaking change to [`crate::cursors::RowCursor`].
///
/// #### Lifecycle
///
/// - the first call to [`crate::cursors::RowCursor::next`] creates an instance with `columns`.
/// - the first call to [`serde::Deserializer::deserialize_struct`] sets the `struct_name`,
///   and the field order is checked. If the order is different from the schema, the state is set to
///   [`StructMetadataState::WithMapAccess`], otherwise to [`StructMetadataState::WithSeqAccess`].
/// - the following calls to [`crate::cursors::RowCursor::next`] and, consequently,
///   to [`serde::Deserializer::deserialize_struct`], will re-use the same prepared instance,
///   without re-checking the fields order for every struct.
pub(crate) struct StructMetadata {
    /// Struct name is defined after the first call to [`serde::Deserializer::deserialize_struct`].
    /// If we are deserializing any other type, e.g., [`u64`], [`Vec<u8>`], etc., it is [`None`],
    /// and it affects how the validation works, see [`DataTypeValidator::validate`].
    pub(crate) struct_name: Option<&'static str>,
    /// Database schema, or columns, are parsed before the first call to (de)serializer.
    pub(crate) columns: Vec<Column>,
    /// This state determines whether we can just use [`crate::rowbinary::de::RowBinarySeqAccess`]
    /// or a more sophisticated approach with [`crate::rowbinary::de::RowBinaryStructAsMapAccess`]
    /// to support structs defined with different fields order than in the schema.
    /// Deserializing a struct as a map will be approximately 40% slower than as a sequence.
    state: StructMetadataState,
}

impl StructMetadata {
    pub(crate) fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            struct_name: None,
            state: StructMetadataState::Pending,
        }
    }

    #[inline(always)]
    pub(crate) fn check_should_use_map(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
    ) -> bool {
        match &self.state {
            StructMetadataState::WithSeqAccess => false,
            StructMetadataState::WithMapAccess(_) => true,
            StructMetadataState::Pending => {
                if self.columns.len() != fields.len() {
                    panic!(
                        "While processing struct {}: database schema has {} columns, \
                        but the struct definition has {} fields.\
                        \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                        name,
                        self.columns.len(),
                        fields.len(),
                        join_panic_schema_hint(fields),
                        join_panic_schema_hint(&self.columns),
                    );
                }
                let mut mapping = Vec::with_capacity(fields.len());
                let mut expected_index = 0;
                let mut should_use_map = false;
                for col in &self.columns {
                    if let Some(index) = fields.iter().position(|field| col.name == *field) {
                        if index != expected_index {
                            should_use_map = true
                        }
                        expected_index += 1;
                        mapping.push(index);
                    } else {
                        panic!(
                            "While processing struct {}: database schema has a column {} \
                            that was not found in the struct definition.\
                            \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                            name,
                            col,
                            join_panic_schema_hint(fields),
                            join_panic_schema_hint(&self.columns),
                        );
                    }
                }
                self.state = if should_use_map {
                    StructMetadataState::WithMapAccess(mapping)
                } else {
                    StructMetadataState::WithSeqAccess
                };
                true
            }
        }
    }

    #[inline(always)]
    pub(crate) fn get_schema_index(&self, struct_idx: usize) -> usize {
        match &self.state {
            StructMetadataState::WithMapAccess(mapping) => {
                if struct_idx < mapping.len() {
                    mapping[struct_idx]
                } else {
                    panic!(
                        "Struct {} has more fields than columns in the database schema",
                        self.struct_name.unwrap_or("Struct")
                    )
                }
            }
            // these two branches should be unreachable
            StructMetadataState::WithSeqAccess => struct_idx,
            StructMetadataState::Pending => {
                panic!(
                    "Struct metadata is not initialized yet, \
                    `ensure_struct_metadata` should be called first"
                )
            }
        }
    }
}

pub(crate) struct DataTypeValidator<'cursor> {
    metadata: &'cursor mut StructMetadata,
    current_column_idx: usize,
}

impl<'cursor> DataTypeValidator<'cursor> {
    #[inline(always)]
    pub(crate) fn new(metadata: &'cursor mut StructMetadata) -> Self {
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
            .map(|c| {
                (
                    format!("{}.{}", self.get_struct_name(), c.name),
                    &c.data_type,
                )
            })
            // both should be defined at this point
            .unwrap_or(("Struct".to_string(), &DataTypeNode::Bool))
    }

    fn get_struct_name(&self) -> String {
        // should be available at the time of the panic call
        self.metadata.struct_name.unwrap_or("Struct").to_string()
    }

    #[inline(always)]
    fn panic_on_schema_mismatch<'de>(
        &'de self,
        data_type: &DataTypeNode,
        serde_type: &SerdeType,
        is_inner: bool,
    ) -> Result<Option<InnerDataTypeValidator<'de, 'cursor>>> {
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

impl SchemaValidator for DataTypeValidator<'_> {
    #[inline]
    fn validate(
        &'_ mut self,
        serde_type: SerdeType,
    ) -> Result<Option<InnerDataTypeValidator<'_, '_>>> {
        if self.current_column_idx == 0 && self.metadata.struct_name.is_none() {
            // this allows validating and deserializing tuples from fetch calls
            Ok(Some(InnerDataTypeValidator {
                root: self,
                kind: if matches!(serde_type, SerdeType::Seq(_)) && self.metadata.columns.len() == 1
                {
                    let data_type = &self.metadata.columns[0].data_type;
                    match data_type {
                        DataTypeNode::Array(inner_type) => {
                            InnerDataTypeValidatorKind::RootArray(inner_type)
                        }
                        _ => panic!(
                            "Expected Array type when validating root level sequence, but got {}",
                            self.metadata.columns[0].data_type
                        ),
                    }
                } else {
                    InnerDataTypeValidatorKind::RootTuple(&self.metadata.columns, 0)
                },
            }))
        } else if self.current_column_idx < self.metadata.columns.len() {
            let current_column = &self.metadata.columns[self.current_column_idx];
            self.current_column_idx += 1;
            validate_impl(self, &current_column.data_type, &serde_type, false)
        } else {
            panic!(
                "Struct {} has more fields than columns in the database schema",
                self.get_struct_name()
            )
        }
    }

    #[inline(always)]
    fn ensure_struct_metadata(
        &'_ mut self,
        name: &'static str,
        fields: &'static [&'static str],
    ) -> bool {
        if self.metadata.struct_name.is_none() {
            self.metadata.struct_name = Some(name);
        }
        self.metadata.check_should_use_map(name, fields)
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

    #[inline]
    fn get_schema_index(&self, struct_idx: usize) -> usize {
        self.metadata.get_schema_index(struct_idx)
    }
}

#[derive(Debug)]
pub(crate) enum MapValidatorState {
    Key,
    Value,
    Validated,
}

#[derive(Debug)]
pub(crate) enum ArrayValidatorState {
    Pending,
    Validated,
}

pub(crate) struct InnerDataTypeValidator<'de, 'cursor> {
    root: &'de DataTypeValidator<'cursor>,
    kind: InnerDataTypeValidatorKind<'cursor>,
}

#[derive(Debug)]
pub(crate) enum InnerDataTypeValidatorKind<'cursor> {
    Array(&'cursor DataTypeNode, ArrayValidatorState),
    FixedString(usize),
    Map(
        &'cursor DataTypeNode,
        &'cursor DataTypeNode,
        MapValidatorState,
    ),
    Tuple(&'cursor [DataTypeNode]),
    /// This is a hack to support deserializing tuples/vectors (and not structs) from fetch calls
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
        // println!("[validate] Validating serde type: {}", serde_type);
        match self {
            None => Ok(None),
            Some(inner) => match &mut inner.kind {
                InnerDataTypeValidatorKind::Map(key_type, value_type, state) => match state {
                    MapValidatorState::Key => {
                        let result = validate_impl(inner.root, key_type, &serde_type, true);
                        *state = MapValidatorState::Value;
                        result
                    }
                    MapValidatorState::Value => {
                        let result = validate_impl(inner.root, value_type, &serde_type, true);
                        *state = MapValidatorState::Validated;
                        result
                    }
                    MapValidatorState::Validated => Ok(None),
                },
                InnerDataTypeValidatorKind::Array(inner_type, state) => match state {
                    ArrayValidatorState::Pending => {
                        let result = validate_impl(inner.root, inner_type, &serde_type, true);
                        *state = ArrayValidatorState::Validated;
                        result
                    }
                    // TODO: perhaps we can allow to validate the inner type more than once
                    //  avoiding e.g. issues with Array(Nullable(T)) when the first element in NULL
                    ArrayValidatorState::Validated => Ok(None),
                },
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
                        // println!("Validating variant identifier: {}", value);
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
                InnerDataTypeValidatorKind::Enum(_values_map) => {
                    todo!() // TODO - check value correctness in the hashmap
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
    fn ensure_struct_metadata(
        &mut self,
        _name: &'static str,
        _fields: &'static [&'static str],
    ) -> bool {
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

#[inline]
fn validate_impl<'de, 'cursor>(
    root: &'de DataTypeValidator<'cursor>,
    column_data_type: &'cursor DataTypeNode,
    serde_type: &SerdeType,
    is_inner: bool,
) -> Result<Option<InnerDataTypeValidator<'de, 'cursor>>> {
    // println!(
    //     "Validating data type: {} against serde type: {}",
    //     column_data_type, serde_type,
    // );
    let data_type = column_data_type.remove_low_cardinality();
    // TODO: eliminate multiple branches with similar patterns?
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
        // TODO: find use cases where this is called instead of `deserialize_tuple`
        // SerdeType::Bytes | SerdeType::ByteBuf => {
        //     if let DataTypeNode::FixedString(n) = data_type {
        //         Ok(Some(InnerDataTypeValidator::FixedString(*n)))
        //     } else {
        //         panic!(
        //             "Expected FixedString(N) for {} call, but got {}",
        //             serde_type, data_type
        //         )
        //     }
        // }
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
                kind: InnerDataTypeValidatorKind::Array(inner_type, ArrayValidatorState::Pending),
            })),
            DataTypeNode::Ring => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::Point,
                    ArrayValidatorState::Pending,
                ),
            })),
            DataTypeNode::Polygon => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::Ring,
                    ArrayValidatorState::Pending,
                ),
            })),
            DataTypeNode::MultiPolygon => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::Polygon,
                    ArrayValidatorState::Pending,
                ),
            })),
            DataTypeNode::LineString => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::Point,
                    ArrayValidatorState::Pending,
                ),
            })),
            DataTypeNode::MultiLineString => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::LineString,
                    ArrayValidatorState::Pending,
                ),
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
                kind: InnerDataTypeValidatorKind::Array(inner_type, ArrayValidatorState::Pending),
            })),
            DataTypeNode::IPv6 => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Array(
                    &DataTypeNode::UInt8,
                    ArrayValidatorState::Pending,
                ),
            })),
            DataTypeNode::UUID => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Tuple(&[
                    DataTypeNode::UInt64,
                    DataTypeNode::UInt64,
                ]),
            })),
            DataTypeNode::Point => Ok(Some(InnerDataTypeValidator {
                root,
                kind: InnerDataTypeValidatorKind::Tuple(&[
                    DataTypeNode::Float64,
                    DataTypeNode::Float64,
                ]),
            })),
            _ => root.panic_on_schema_mismatch(data_type, serde_type, is_inner),
        },
        SerdeType::Map(_) => {
            if let DataTypeNode::Map(key_type, value_type) = data_type {
                Ok(Some(InnerDataTypeValidator {
                    root,
                    kind: InnerDataTypeValidatorKind::Map(
                        key_type,
                        value_type,
                        MapValidatorState::Key,
                    ),
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
    fn ensure_struct_metadata(
        &mut self,
        _name: &'static str,
        _fields: &'static [&'static str],
    ) -> bool {
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

fn join_panic_schema_hint<T: Display>(col: &[T]) -> String {
    if col.is_empty() {
        return String::default();
    }
    col.iter()
        .map(|c| format!("- {}", c))
        .collect::<Vec<String>>()
        .join("\n")
}
