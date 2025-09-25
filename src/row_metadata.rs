use crate::Row;
use crate::row::RowKind;
use clickhouse_types::Column;
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub(crate) enum AccessType {
    WithSeqAccess,
    WithMapAccess(Vec<usize>),
}

/// Contains a vector of [`Column`] objects parsed from the beginning
/// of `RowBinaryWithNamesAndTypes` data stream.
///
/// [`RowMetadata`] should be owned outside the (de)serializer,
/// as it is calculated only once per struct. It does not have lifetimes,
/// so it does not introduce a breaking change to [`crate::cursors::RowCursor`].
pub(crate) struct RowMetadata {
    /// Database schema, or table columns, are parsed before the first call to deserializer.
    /// However, the order here depends on the usage context:
    /// * For selects, it is defined in the same order as in the database schema.
    /// * For inserts, it is adjusted to the order of fields in the struct definition.
    pub(crate) columns: Vec<Column>,
    /// This determines whether we can just use [`crate::rowbinary::de::RowBinarySeqAccess`]
    /// or a more sophisticated approach with [`crate::rowbinary::de::RowBinaryStructAsMapAccess`]
    /// to support structs defined with different fields order than in the schema.
    ///
    /// Deserializing a struct as a map can be significantly slower, but that depends
    /// on the shape of the data. In some cases, there is no noticeable difference,
    /// in others, it could be up to 2-3x slower.
    pub(crate) access_type: AccessType,
}

impl RowMetadata {
    pub(crate) fn new_for_cursor<T: Row>(columns: Vec<Column>) -> Self {
        let access_type = match T::KIND {
            RowKind::Primitive => {
                if columns.len() != 1 {
                    panic!(
                        "While processing a primitive row: \
                        expected only 1 column in the database schema, \
                        but got {} instead.\n#### All schema columns:\n{}",
                        columns.len(),
                        join_panic_schema_hint(&columns),
                    );
                }
                AccessType::WithSeqAccess // ignored
            }
            RowKind::Tuple => {
                if T::COLUMN_COUNT != columns.len() {
                    panic!(
                        "While processing a tuple row: database schema has {} columns, \
                        but the tuple definition has {} fields in total.\
                        \n#### All schema columns:\n{}",
                        columns.len(),
                        T::COLUMN_COUNT,
                        join_panic_schema_hint(&columns),
                    );
                }
                AccessType::WithSeqAccess // ignored
            }
            RowKind::Vec => {
                if columns.len() != 1 {
                    panic!(
                        "While processing a row defined as a vector: \
                        expected only 1 column in the database schema, \
                        but got {} instead.\n#### All schema columns:\n{}",
                        columns.len(),
                        join_panic_schema_hint(&columns),
                    );
                }
                AccessType::WithSeqAccess // ignored
            }
            RowKind::Struct => {
                if columns.len() != T::COLUMN_NAMES.len() {
                    panic!(
                        "While processing struct {}: database schema has {} columns, \
                        but the struct definition has {} fields.\
                        \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                        T::NAME,
                        columns.len(),
                        T::COLUMN_NAMES.len(),
                        join_panic_schema_hint(T::COLUMN_NAMES),
                        join_panic_schema_hint(&columns),
                    );
                }
                let mut mapping = Vec::with_capacity(T::COLUMN_NAMES.len());
                let mut expected_index = 0;
                let mut should_use_map = false;
                for col in &columns {
                    if let Some(index) = T::COLUMN_NAMES.iter().position(|field| col.name == *field)
                    {
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
                            T::NAME,
                            col,
                            join_panic_schema_hint(T::COLUMN_NAMES),
                            join_panic_schema_hint(&columns),
                        );
                    }
                }
                if should_use_map {
                    AccessType::WithMapAccess(mapping)
                } else {
                    AccessType::WithSeqAccess
                }
            }
        };
        Self {
            columns,
            access_type,
        }
    }

    pub(crate) fn new_for_insert<T: Row>(columns: Vec<Column>) -> Self {
        if T::KIND != RowKind::Struct {
            panic!(
                "SerializerRowMetadata can only be created for structs, \
                but got {:?} instead.\n#### All schema columns:\n{}",
                T::KIND,
                join_panic_schema_hint(&columns),
            );
        }
        if columns.len() != T::COLUMN_NAMES.len() {
            panic!(
                "While processing struct {}: database schema has {} columns, \
                but the struct definition has {} fields.\
                \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                T::NAME,
                columns.len(),
                T::COLUMN_NAMES.len(),
                join_panic_schema_hint(T::COLUMN_NAMES),
                join_panic_schema_hint(&columns),
            );
        }

        let mut result_columns: Vec<Column> = Vec::with_capacity(columns.len());
        let db_columns_lookup: HashMap<&str, &Column> =
            columns.iter().map(|col| (col.name.as_str(), col)).collect();

        for struct_column_name in T::COLUMN_NAMES {
            match db_columns_lookup.get(*struct_column_name) {
                Some(col) => result_columns.push((*col).clone()),
                None => {
                    panic!(
                        "While processing struct {}: database schema has no column named {}.\
                        \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                        T::NAME,
                        struct_column_name,
                        join_panic_schema_hint(T::COLUMN_NAMES),
                        join_panic_schema_hint(&db_columns_lookup.values().collect::<Vec<_>>()),
                    );
                }
            }
        }

        Self {
            columns: result_columns,
            access_type: AccessType::WithSeqAccess, // ignored
        }
    }

    /// Returns the index of the column in the database schema
    /// that corresponds to the field with the given index in the struct.
    ///
    /// Only makes sense for selects; for inserts, it is always the same as `struct_idx`,
    /// since we write the header with the field order defined in the struct,
    /// and ClickHouse server figures out the rest on its own.
    #[inline]
    pub(crate) fn get_schema_index(&self, struct_idx: usize) -> usize {
        match &self.access_type {
            AccessType::WithMapAccess(mapping) => {
                if struct_idx < mapping.len() {
                    mapping[struct_idx]
                } else {
                    // unreachable
                    panic!("Struct has more fields than columns in the database schema")
                }
            }
            AccessType::WithSeqAccess => struct_idx, // should be unreachable
        }
    }

    /// Returns `true` if the field order in the struct is different from the database schema.
    ///
    /// Only makes sense for selects; for inserts, it is always `false`.
    #[inline]
    pub(crate) fn is_field_order_wrong(&self) -> bool {
        matches!(self.access_type, AccessType::WithMapAccess(_))
    }
}

fn join_panic_schema_hint<T: Display>(col: &[T]) -> String {
    if col.is_empty() {
        return String::default();
    }
    col.iter()
        .map(|c| format!("- {c}"))
        .collect::<Vec<String>>()
        .join("\n")
}
