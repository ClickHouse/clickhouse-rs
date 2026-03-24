//! Schema reflection for dynamic inserts.
//!
//! Fetches column definitions from `system.columns` and caches them with TTL.
//! The schema drives runtime RowBinary encoding — each column's [`ParsedType`]
//! determines how `serde_json::Value` is converted to binary.
//!
//! # Usage
//!
//! ```rust,ignore
//! use clickhouse::dynamic::schema::{fetch_dynamic_schema, DynamicSchemaCache};
//!
//! let cache = DynamicSchemaCache::new(Duration::from_secs(300));
//! let schema = fetch_dynamic_schema(&client, "mydb", "mytable").await?;
//! cache.insert("mydb.mytable", schema);
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::error::DynamicError;
use super::parsed_type::ParsedType;

/// Column definition from `system.columns`.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Raw type string from ClickHouse (e.g. "LowCardinality(Nullable(String))").
    pub raw_type: String,
    /// Parsed type with full structure.
    pub parsed_type: ParsedType,
    /// Default kind: "", "DEFAULT", "MATERIALIZED", "ALIAS", "EPHEMERAL".
    pub default_kind: String,
    /// Whether this column can be omitted from INSERT (has a server-side default).
    pub has_default: bool,
}

/// Schema for a single table — ordered list of column definitions.
#[derive(Debug, Clone)]
pub struct DynamicSchema {
    /// Fully qualified table name (database.table).
    pub table: String,
    /// Columns in position order.
    pub columns: Vec<ColumnDef>,
    /// Lookup by column name for O(1) access during encoding.
    column_index: HashMap<String, usize>,
}

impl DynamicSchema {
    /// Build from a list of column definitions.
    pub fn from_columns(table: &str, columns: Vec<ColumnDef>) -> Self {
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            table: table.to_string(),
            columns,
            column_index,
        }
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_index.get(name).map(|&i| &self.columns[i])
    }

    /// Columns that MUST appear in INSERT (no server-side default).
    pub fn required_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.has_default)
    }

    /// Columns that CAN be omitted (have DEFAULT/MATERIALIZED/ALIAS).
    pub fn optional_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| c.has_default)
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the schema has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Schema Cache
// ---------------------------------------------------------------------------

/// TTL-based schema cache with invalidation.
///
/// Thread-safe via `RwLock`. Designed to be shared across insert instances
/// via `Arc`.
pub struct DynamicSchemaCache {
    inner: RwLock<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

struct CacheEntry {
    schema: DynamicSchema,
    fetched_at: Instant,
}

impl DynamicSchemaCache {
    /// Create a new cache wrapped in `Arc`.
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
            ttl,
        })
    }

    /// Get cached schema if not expired.
    pub fn get(&self, table: &str) -> Option<DynamicSchema> {
        let guard = self.inner.read().ok()?;
        guard.get(table).and_then(|e| {
            if e.fetched_at.elapsed() < self.ttl {
                Some(e.schema.clone())
            } else {
                None
            }
        })
    }

    /// Insert or refresh a schema entry.
    pub fn insert(&self, table: &str, schema: DynamicSchema) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(
                table.to_string(),
                CacheEntry {
                    schema,
                    fetched_at: Instant::now(),
                },
            );
        }
    }

    /// Invalidate a single table (forces re-fetch on next access).
    pub fn invalidate(&self, table: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(table);
        }
    }

    /// Invalidate all cached schemas.
    pub fn invalidate_all(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
    }
}

impl std::fmt::Debug for DynamicSchemaCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.read().map(|g| g.len()).unwrap_or(0);
        f.debug_struct("DynamicSchemaCache")
            .field("ttl", &self.ttl)
            .field("entries", &count)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Schema fetch
// ---------------------------------------------------------------------------

/// Fetch table schema from `system.columns` via the unified client.
///
/// Works over both HTTP and native TCP transports.
/// Parses each column's type string into a full [`ParsedType`].
pub async fn fetch_dynamic_schema(
    client: &crate::unified::UnifiedClient,
    database: &str,
    table: &str,
) -> Result<DynamicSchema, DynamicError> {
    let full_table = format!("{database}.{table}");

    // Build query using the crate's SQL escaping
    let mut sql =
        String::from("SELECT name, type, default_kind FROM system.columns WHERE database = ");
    crate::sql::escape::string(database, &mut sql).map_err(|e| DynamicError::SchemaFetch {
        table: full_table.clone(),
        source: crate::error::Error::Custom(e.to_string()),
    })?;
    sql.push_str(" AND table = ");
    crate::sql::escape::string(table, &mut sql).map_err(|e| DynamicError::SchemaFetch {
        table: full_table.clone(),
        source: crate::error::Error::Custom(e.to_string()),
    })?;
    sql.push_str(" ORDER BY position");

    // Fetch as positional tuples to avoid derive(Row) macro issues inside the crate
    let mut cursor = client
        .query(&sql)
        .fetch::<(String, String, String)>()
        .map_err(|e| DynamicError::SchemaFetch {
            table: full_table.clone(),
            source: e,
        })?;

    let mut columns = Vec::new();
    while let Some((name, col_type, default_kind)) =
        cursor.next().await.map_err(|e| DynamicError::SchemaFetch {
            table: full_table.clone(),
            source: e,
        })?
    {
        let parsed_type = ParsedType::parse(&col_type);
        let has_default = !default_kind.is_empty();
        columns.push(ColumnDef {
            name,
            raw_type: col_type,
            parsed_type,
            default_kind,
            has_default,
        });
    }

    if columns.is_empty() {
        return Err(DynamicError::EmptySchema { table: full_table });
    }

    Ok(DynamicSchema::from_columns(&full_table, columns))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_col(name: &str, type_str: &str, default_kind: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            raw_type: type_str.to_string(),
            parsed_type: ParsedType::parse(type_str),
            default_kind: default_kind.to_string(),
            has_default: !default_kind.is_empty(),
        }
    }

    #[test]
    fn test_dynamic_schema_basic() {
        let schema = DynamicSchema::from_columns(
            "db.test",
            vec![
                make_col("id", "UInt64", ""),
                make_col("name", "String", ""),
                make_col("created_at", "DateTime64(3)", "DEFAULT"),
            ],
        );

        assert_eq!(schema.len(), 3);
        assert!(!schema.is_empty());
        assert!(schema.column("id").is_some());
        assert!(schema.column("missing").is_none());
        assert_eq!(schema.required_columns().count(), 2);
        assert_eq!(schema.optional_columns().count(), 1);
    }

    #[test]
    fn test_column_def_has_default() {
        let col = make_col("ts", "DateTime64(3)", "DEFAULT");
        assert!(col.has_default);

        let col = make_col("id", "UInt64", "");
        assert!(!col.has_default);

        let col = make_col("mv", "String", "MATERIALIZED");
        assert!(col.has_default);
    }

    #[test]
    fn test_schema_cache_basic() {
        let cache = DynamicSchemaCache::new(Duration::from_secs(300));
        let schema = DynamicSchema::from_columns("db.test", vec![make_col("id", "UInt64", "")]);

        assert!(cache.get("db.test").is_none());
        cache.insert("db.test", schema.clone());
        assert!(cache.get("db.test").is_some());

        cache.invalidate("db.test");
        assert!(cache.get("db.test").is_none());
    }

    #[test]
    fn test_schema_cache_ttl() {
        let cache = DynamicSchemaCache::new(Duration::from_millis(1));
        let schema = DynamicSchema::from_columns("db.test", vec![make_col("id", "UInt64", "")]);

        cache.insert("db.test", schema);
        // Immediately should still be cached
        assert!(cache.get("db.test").is_some());

        // After TTL expires
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get("db.test").is_none());
    }

    #[test]
    fn test_schema_cache_invalidate_all() {
        let cache = DynamicSchemaCache::new(Duration::from_secs(300));
        let schema1 = DynamicSchema::from_columns("db.t1", vec![make_col("id", "UInt64", "")]);
        let schema2 = DynamicSchema::from_columns("db.t2", vec![make_col("id", "UInt64", "")]);

        cache.insert("db.t1", schema1);
        cache.insert("db.t2", schema2);
        assert!(cache.get("db.t1").is_some());
        assert!(cache.get("db.t2").is_some());

        cache.invalidate_all();
        assert!(cache.get("db.t1").is_none());
        assert!(cache.get("db.t2").is_none());
    }
}
