use crate::row::RowKind;
use crate::sql::Identifier;
use crate::Result;
use crate::Row;
use clickhouse_types::{parse_rbwnat_columns_header, Column};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cache for [`RowMetadata`] to avoid allocating it for the same struct more than once
/// during the application lifecycle. Key: fully qualified table name (e.g. `database.table`).
pub(crate) struct RowMetadataCache(RwLock<HashMap<String, Arc<RowMetadata>>>);

impl Default for RowMetadataCache {
    fn default() -> Self {
        RowMetadataCache(RwLock::new(HashMap::default()))
    }
}

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
    /// Database schema, or columns, are parsed before the first call to (de)serializer.
    pub(crate) columns: Vec<Column>,
    /// This determines whether we can just use [`crate::rowbinary::de::RowBinarySeqAccess`]
    /// or a more sophisticated approach with [`crate::rowbinary::de::RowBinaryStructAsMapAccess`]
    /// to support structs defined with different fields order than in the schema.
    /// (De)serializing a struct as a map will be approximately 40% slower than as a sequence.
    access_type: AccessType,
}

impl RowMetadata {
    pub(crate) fn new<T: Row>(columns: Vec<Column>) -> Self {
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

    #[inline]
    pub(crate) fn get_schema_index(&self, struct_idx: usize) -> usize {
        match &self.access_type {
            AccessType::WithMapAccess(mapping) => {
                if struct_idx < mapping.len() {
                    mapping[struct_idx]
                } else {
                    // unreachable
                    panic!("Struct has more fields than columns in the database schema",)
                }
            }
            AccessType::WithSeqAccess => struct_idx, // should be unreachable
        }
    }

    #[inline]
    pub(crate) fn is_field_order_wrong(&self) -> bool {
        matches!(self.access_type, AccessType::WithMapAccess(_))
    }
}

pub(crate) async fn get_row_metadata<T: Row>(
    client: &crate::Client,
    table_name: &str,
) -> Result<Arc<RowMetadata>> {
    let read_lock = client.row_metadata_cache.0.read().await;
    match read_lock.get(table_name) {
        Some(metadata) => Ok(metadata.clone()),
        None => {
            drop(read_lock);
            // TODO: should it be moved to a cold function?
            let mut write_lock = client.row_metadata_cache.0.write().await;
            let mut bytes_cursor = client
                .query("SELECT * FROM ? LIMIT 0")
                .bind(Identifier(table_name))
                .fetch_bytes("RowBinaryWithNamesAndTypes")?;
            let mut buffer = Vec::<u8>::new();
            while let Some(chunk) = bytes_cursor.next().await? {
                buffer.extend_from_slice(&chunk);
            }
            let columns = parse_rbwnat_columns_header(&mut buffer.as_slice())?;
            let metadata = Arc::new(RowMetadata::new::<T>(columns));
            write_lock.insert(table_name.to_string(), metadata.clone());
            Ok(metadata)
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::Client;
    use clickhouse_types::{Column, DataTypeNode};

    #[derive(Debug, Clone, PartialEq)]
    struct SystemRolesRow {
        name: String,
        id: uuid::Uuid,
        storage: String,
    }

    impl SystemRolesRow {
        fn columns() -> Vec<Column> {
            vec![
                Column::new("name".to_string(), DataTypeNode::String),
                Column::new("id".to_string(), DataTypeNode::UUID),
                Column::new("storage".to_string(), DataTypeNode::String),
            ]
        }
    }

    impl Row for SystemRolesRow {
        const NAME: &'static str = "SystemRolesRow";
        const KIND: RowKind = RowKind::Struct;
        const COLUMN_COUNT: usize = 3;
        const COLUMN_NAMES: &'static [&'static str] = &["name", "id", "storage"];
        type Value<'a> = SystemRolesRow;
    }

    #[test]
    fn get_row_metadata() {
        let metadata = RowMetadata::new::<SystemRolesRow>(SystemRolesRow::columns());
        assert_eq!(metadata.columns, SystemRolesRow::columns());
        assert_eq!(metadata.access_type, AccessType::WithSeqAccess);

        // the order is shuffled => map access
        let columns = vec![
            Column::new("id".to_string(), DataTypeNode::UUID),
            Column::new("storage".to_string(), DataTypeNode::String),
            Column::new("name".to_string(), DataTypeNode::String),
        ];
        let metadata = RowMetadata::new::<SystemRolesRow>(columns.clone());
        assert_eq!(metadata.columns, columns);
        assert_eq!(
            metadata.access_type,
            AccessType::WithMapAccess(vec![1, 2, 0]) // see COLUMN_NAMES above
        );
    }

    #[tokio::test]
    async fn cache_row_metadata() {
        let client = Client::default()
            .with_url("http://localhost:8123")
            .with_database("system");

        let metadata = super::get_row_metadata::<SystemRolesRow>(&client, "roles")
            .await
            .unwrap();

        assert_eq!(metadata.columns, SystemRolesRow::columns());
        assert_eq!(metadata.access_type, AccessType::WithSeqAccess);

        // we can now use a dummy client, cause the metadata is cached,
        // and no calls to the database will be made
        super::get_row_metadata::<SystemRolesRow>(&client.with_url("whatever"), "roles")
            .await
            .unwrap();

        assert_eq!(metadata.columns, SystemRolesRow::columns());
        assert_eq!(metadata.access_type, AccessType::WithSeqAccess);
    }
}
