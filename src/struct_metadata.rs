// FIXME: this is allowed only temporarily,
//  before the insert RBWNAT implementation is ready,
//  cause otherwise the caches are never used.
#![allow(dead_code)]
#![allow(unreachable_pub)]

use crate::row::RowType;
use crate::sql::Identifier;
use crate::Result;
use crate::Row;
use clickhouse_types::{parse_rbwnat_columns_header, Column};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

/// Cache for [`StructMetadata`] to avoid allocating it for the same struct more than once
/// during the application lifecycle. Key: fully qualified table name (e.g. `database.table`).
type LockedStructMetadataCache = RwLock<HashMap<String, Arc<StructMetadata>>>;
static STRUCT_METADATA_CACHE: OnceCell<LockedStructMetadataCache> = OnceCell::const_new();

#[derive(Debug, PartialEq)]
enum AccessType {
    WithSeqAccess,
    WithMapAccess(Vec<usize>),
}

/// [`StructMetadata`] should be owned outside the (de)serializer,
/// as it is calculated only once per struct. It does not have lifetimes,
/// so it does not introduce a breaking change to [`crate::cursors::RowCursor`].
pub struct StructMetadata {
    /// See [`Row::NAME`]
    pub(crate) struct_name: &'static str,
    /// See [`Row::COLUMN_NAMES`] (currently unused)
    pub(crate) struct_fields: &'static [&'static str],
    /// See [`Row::TYPE`]
    pub(crate) row_type: RowType,
    /// Database schema, or columns, are parsed before the first call to (de)serializer.
    pub(crate) columns: Vec<Column>,
    /// This determines whether we can just use [`crate::rowbinary::de::RowBinarySeqAccess`]
    /// or a more sophisticated approach with [`crate::rowbinary::de::RowBinaryStructAsMapAccess`]
    /// to support structs defined with different fields order than in the schema.
    /// (De)serializing a struct as a map will be approximately 40% slower than as a sequence.
    access_type: AccessType,
}

impl StructMetadata {
    // FIXME: perhaps it should not be public? But it is required for mocks/provide.
    pub fn new<T: Row>(columns: Vec<Column>) -> Self {
        let struct_name = T::NAME;
        let struct_fields = T::COLUMN_NAMES;
        if columns.len() != struct_fields.len() {
            panic!(
                "While processing struct {}: database schema has {} columns, \
                but the struct definition has {} fields.\
                \n#### All struct fields:\n{}\n#### All schema columns:\n{}",
                struct_name,
                columns.len(),
                struct_fields.len(),
                join_panic_schema_hint(struct_fields),
                join_panic_schema_hint(&columns),
            );
        }
        let mut mapping = Vec::with_capacity(struct_fields.len());
        let mut expected_index = 0;
        let mut should_use_map = false;
        for col in &columns {
            if let Some(index) = struct_fields.iter().position(|field| col.name == *field) {
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
                    struct_name,
                    col,
                    join_panic_schema_hint(struct_fields),
                    join_panic_schema_hint(&columns),
                );
            }
        }
        Self {
            columns,
            struct_name,
            struct_fields,
            row_type: T::TYPE,
            access_type: if should_use_map {
                AccessType::WithMapAccess(mapping)
            } else {
                AccessType::WithSeqAccess
            },
        }
    }

    #[inline(always)]
    pub(crate) fn is_struct(&self) -> bool {
        matches!(self.row_type, RowType::Struct)
    }

    #[inline(always)]
    pub(crate) fn get_schema_index(&self, struct_idx: usize) -> usize {
        match &self.access_type {
            AccessType::WithMapAccess(mapping) => {
                if struct_idx < mapping.len() {
                    mapping[struct_idx]
                } else {
                    panic!(
                        "Struct {} has more fields than columns in the database schema",
                        self.struct_name
                    )
                }
            }
            AccessType::WithSeqAccess => struct_idx, // should be unreachable
        }
    }
}

pub(crate) async fn get_struct_metadata<T: Row>(
    client: &crate::Client,
    table_name: &str,
) -> Result<Arc<StructMetadata>> {
    let locked_cache = STRUCT_METADATA_CACHE
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let cache_guard = locked_cache.read().await;
    match cache_guard.get(table_name) {
        Some(metadata) => Ok(metadata.clone()),
        None => cache_struct_metadata::<T>(client, table_name, locked_cache).await,
    }
}

/// Used internally to introspect and cache the table structure to allow validation
/// of serialized rows before submitting the first [`insert::Insert::write`].
async fn cache_struct_metadata<T: Row>(
    client: &crate::Client,
    table_name: &str,
    locked_cache: &LockedStructMetadataCache,
) -> Result<Arc<StructMetadata>> {
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
    let metadata = Arc::new(StructMetadata::new::<T>(columns));
    cache.insert(table_name.to_string(), metadata.clone());
    Ok(metadata)
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
