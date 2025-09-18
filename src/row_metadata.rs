// FIXME: this is allowed only temporarily,
//  before the insert RBWNAT implementation is ready,
//  cause otherwise the caches are never used.
#![allow(dead_code)]
#![allow(unreachable_pub)]

use crate::row::RowKind;
use crate::sql::Identifier;
use crate::Result;
use crate::Row;
use clickhouse_types::{parse_rbwnat_columns_header, Column};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

/// Cache for [`RowMetadata`] to avoid allocating it for the same struct more than once
/// during the application lifecycle. Key: fully qualified table name (e.g. `database.table`).
type LockedRowMetadataCache = RwLock<HashMap<String, Arc<RowMetadata>>>;
static ROW_METADATA_CACHE: OnceCell<LockedRowMetadataCache> = OnceCell::const_new();

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
    let locked_cache = ROW_METADATA_CACHE
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let cache_guard = locked_cache.read().await;
    match cache_guard.get(table_name) {
        Some(metadata) => Ok(metadata.clone()),
        None => cache_row_metadata::<T>(client, table_name, locked_cache).await,
    }
}

/// Used internally to introspect and cache the table structure to allow validation
/// of serialized rows before submitting the first [`insert::Insert::write`].
async fn cache_row_metadata<T: Row>(
    client: &crate::Client,
    table_name: &str,
    locked_cache: &LockedRowMetadataCache,
) -> Result<Arc<RowMetadata>> {
    let mut bytes_cursor = client
        .query("SELECT * FROM ? LIMIT 0")
        .bind(Identifier(table_name))
        .fetch_bytes("RowBinaryWithNamesAndTypes")?;
    let mut buffer = Vec::<u8>::new();
    while let Some(chunk) = bytes_cursor.next().await? {
        buffer.extend_from_slice(&chunk);
    }
    let columns = parse_rbwnat_columns_header(&mut buffer.as_slice())?;
    let mut cache = locked_cache.write().await;
    let metadata = Arc::new(RowMetadata::new::<T>(columns));
    cache.insert(table_name.to_string(), metadata.clone());
    Ok(metadata)
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
