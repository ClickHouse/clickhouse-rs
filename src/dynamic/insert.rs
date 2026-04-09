//! Single-table dynamic insert with automatic schema fetch and recovery.
//!
//! `DynamicInsert` fetches the table schema from `system.columns` on first use,
//! encodes `Map<String, Value>` to RowBinary, and sends via HTTP `InsertFormatted`.
//!
//! On schema mismatch errors (e.g. `ALTER TABLE ADD COLUMN`), it automatically
//! invalidates the cached schema so the next insert re-fetches. The caller's
//! retry/salvage logic handles re-sending failed rows.
//!
//! # End-to-End Efficiency
//!
//! This replaces the "push JSON, let ClickHouse parse it" approach with
//! schema-reflected binary. Your app does roughly the same work (binary encoding
//! instead of JSON serialisation), but the ClickHouse cluster does zero parsing
//! on ingest. Think total CPU across client + cluster, not just your app.

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::unified::UnifiedClient;

use super::encode::{columns_to_send, encode_dynamic_row, encode_dynamic_row_with_raw};
use super::error::DynamicError;
use super::schema::{ColumnDef, DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};

/// Dynamic insert for a single table.
///
/// Encodes `Map<String, Value>` to RowBinary using a schema fetched from
/// `system.columns`. As simple to use as JSONEachRow, but binary wire format.
///
/// # Schema Recovery
///
/// If ClickHouse rejects an insert due to schema mismatch (column added/removed,
/// type changed), call [`invalidate_schema()`][Self::invalidate_schema] and create
/// a new `DynamicInsert`. The next insert will re-fetch the schema automatically.
///
/// For automatic recovery in a pipeline context, use `DynamicBatcher` which
/// handles this transparently.
pub struct DynamicInsert {
    client: UnifiedClient,
    database: String,
    table: String,
    schema_cache: Arc<DynamicSchemaCache>,
    schema: Option<DynamicSchema>,
    /// Column list for the current INSERT (determined from first row).
    insert_columns: Option<Vec<String>>,
    /// The active HTTP insert (created lazily on first write_map).
    insert: Option<crate::insert_formatted::BufInsertFormatted>,
    rows_written: u64,
}

impl DynamicInsert {
    /// Create a new `DynamicInsert`. Schema is fetched lazily on first `write_map()`.
    pub(crate) fn new(
        client: UnifiedClient,
        database: String,
        table: String,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            client,
            database,
            table,
            schema_cache,
            schema: None,
            insert_columns: None,
            insert: None,
            rows_written: 0,
        }
    }

    /// Ensure schema is loaded (from cache or system.columns).
    async fn ensure_schema(&mut self) -> Result<(), DynamicError> {
        if self.schema.is_none() {
            let full_table = format!("{}.{}", self.database, self.table);
            let schema = if let Some(cached) = self.schema_cache.get(&full_table) {
                cached
            } else {
                let fetched =
                    fetch_dynamic_schema(&self.client, &self.database, &self.table).await?;
                self.schema_cache.insert(&full_table, fetched.clone());
                fetched
            };
            self.schema = Some(schema);
        }
        Ok(())
    }

    /// Encode and buffer a row for insert.
    ///
    /// The row is encoded to RowBinary and written to the HTTP insert buffer.
    /// On first call, fetches the schema and creates the INSERT statement.
    pub async fn write_map(&mut self, row: &Map<String, Value>) -> Result<(), DynamicError> {
        // Ensure schema is loaded
        self.ensure_schema().await?;
        let Some(schema) = self.schema.as_ref() else {
            // ensure_schema always sets self.schema on success, so this
            // branch is unreachable. Defensive check avoids .unwrap().
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "schema not available after fetch".to_string(),
            });
        };

        // On first row, determine the column list and create the INSERT.
        // The insert data path requires HTTP transport -- for native, the
        // RowBinary-to-columnar conversion is not yet wired up.
        if self.insert.is_none() {
            let cols = columns_to_send(row, schema);
            let col_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            // Escape all identifiers to prevent SQL injection.
            let mut sql = String::from("INSERT INTO ");
            crate::sql::escape::identifier(&self.database, &mut sql)
                .expect("fmt::Write on String is infallible");
            sql.push('.');
            crate::sql::escape::identifier(&self.table, &mut sql)
                .expect("fmt::Write on String is infallible");
            sql.push_str(" (");
            for (i, name) in col_names.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                crate::sql::escape::identifier(name, &mut sql)
                    .expect("fmt::Write on String is infallible");
            }
            sql.push_str(") FORMAT RowBinary");
            let mut formatted = self.client.insert_formatted_with(sql).map_err(|e| {
                DynamicError::EncodingError {
                    column: String::new(),
                    message: e.to_string(),
                }
            })?;

            // The RowBinary encoder sends JSON values as length-prefixed strings.
            // ClickHouse's default JSON RowBinary deserializer expects a structured
            // path-value binary format, which causes "Unknown type code: 0x73" errors.
            // Setting this option tells ClickHouse to accept JSON as simple strings.
            if schema.has_json_columns() {
                formatted = formatted.with_option("input_format_binary_read_json_as_string", "1");
            }

            self.insert = Some(formatted.buffered());
            self.insert_columns = Some(col_names);
        }

        // Build the column def refs for encoding based on stored column names.
        // insert_columns is always set together with insert above.
        let Some(col_names) = self.insert_columns.as_ref() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "insert columns not initialised".to_string(),
            });
        };
        let col_defs: Vec<&ColumnDef> = col_names
            .iter()
            .filter_map(|name| schema.column(name))
            .collect();

        // Encode row to RowBinary
        let rb_bytes = encode_dynamic_row(row, schema, &col_defs)?;

        // Write to the HTTP insert buffer. insert is always set above.
        let Some(insert) = self.insert.as_mut() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "insert not initialised".to_string(),
            });
        };
        insert
            .write(&rb_bytes)
            .await
            .map_err(|e| classify_error(&self.database, &self.table, e))?;

        self.rows_written += 1;
        Ok(())
    }

    /// Encode and buffer a row, with raw byte passthrough for named columns.
    ///
    /// Like [`write_map`], but `raw_columns` provides pre-encoded bytes for
    /// specific columns (e.g. `_json`). These bytes are written directly as
    /// length-prefixed strings in the RowBinary output, avoiding intermediate
    /// `Value::String` wrapping and `to_string()` re-serialisation.
    ///
    /// This enables zero-copy for columns where the caller already has the
    /// raw bytes (e.g. raw Kafka payload for a JSON column).
    pub async fn write_map_with_raw(
        &mut self,
        row: &Map<String, Value>,
        raw_columns: &[(&str, &[u8])],
    ) -> Result<(), DynamicError> {
        // Ensure schema is loaded
        self.ensure_schema().await?;
        let Some(schema) = self.schema.as_ref() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "schema not available after fetch".to_string(),
            });
        };

        // On first row, determine the column list and create the INSERT.
        if self.insert.is_none() {
            // Include raw columns in the column list even if they're not in the row map.
            let mut cols = columns_to_send(row, schema);
            for (name, _) in raw_columns {
                if !cols.iter().any(|c| c.name == *name)
                    && let Some(col_def) = schema.column(name)
                {
                    cols.push(col_def);
                }
            }
            let col_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            let mut sql = String::from("INSERT INTO ");
            crate::sql::escape::identifier(&self.database, &mut sql)
                .expect("fmt::Write on String is infallible");
            sql.push('.');
            crate::sql::escape::identifier(&self.table, &mut sql)
                .expect("fmt::Write on String is infallible");
            sql.push_str(" (");
            for (i, name) in col_names.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                crate::sql::escape::identifier(name, &mut sql)
                    .expect("fmt::Write on String is infallible");
            }
            sql.push_str(") FORMAT RowBinary");
            let mut formatted = self.client.insert_formatted_with(sql).map_err(|e| {
                DynamicError::EncodingError {
                    column: String::new(),
                    message: e.to_string(),
                }
            })?;

            if schema.has_json_columns() {
                formatted = formatted.with_option("input_format_binary_read_json_as_string", "1");
            }

            self.insert = Some(formatted.buffered());
            self.insert_columns = Some(col_names);
        }

        let Some(col_names) = self.insert_columns.as_ref() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "insert columns not initialised".to_string(),
            });
        };
        let col_defs: Vec<&ColumnDef> = col_names
            .iter()
            .filter_map(|name| schema.column(name))
            .collect();

        // Encode row to RowBinary with raw passthrough
        let rb_bytes = encode_dynamic_row_with_raw(row, schema, &col_defs, raw_columns)?;

        let Some(insert) = self.insert.as_mut() else {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "insert not initialised".to_string(),
            });
        };
        insert
            .write(&rb_bytes)
            .await
            .map_err(|e| classify_error(&self.database, &self.table, e))?;

        self.rows_written += 1;
        Ok(())
    }

    /// Flush the buffer and finalise the INSERT.
    ///
    /// Returns the number of rows written. On `SchemaMismatch` errors, the
    /// schema cache is automatically invalidated so the next insert re-fetches
    /// from `system.columns`.
    ///
    /// Since `end()` consumes `self`, use [`schema_cache()`][Self::schema_cache]
    /// before calling `end()` if you need the cache handle for custom recovery.
    pub async fn end(mut self) -> Result<u64, DynamicError> {
        if let Some(mut insert) = self.insert.take() {
            match insert.end().await {
                Ok(()) => {}
                Err(e) => {
                    let err = classify_error(&self.database, &self.table, e);
                    // Auto-invalidate on schema mismatch so the next insert
                    // re-fetches the schema without caller intervention.
                    if matches!(err, DynamicError::SchemaMismatch { .. }) {
                        self.schema_cache
                            .invalidate(&format!("{}.{}", self.database, self.table));
                    }
                    return Err(err);
                }
            }
        }
        Ok(self.rows_written)
    }

    /// Get the schema cache and table identifier for external invalidation.
    ///
    /// Use when you need to invalidate after [`end()`][Self::end] fails, since
    /// `end()` consumes `self`. Extract the cache handle *before* calling `end()`:
    ///
    /// ```ignore
    /// let (cache, db, table) = insert.schema_cache();
    /// match insert.end().await {
    ///     Ok(count) => Ok(count),
    ///     Err(DynamicError::SchemaMismatch { .. }) => {
    ///         cache.invalidate(&format!("{db}.{table}"));
    ///         // retry...
    ///     }
    ///     Err(e) => Err(e),
    /// }
    /// ```
    ///
    /// Note: `end()` already auto-invalidates on `SchemaMismatch`, so this
    /// accessor is only needed for custom recovery logic (e.g. retry loops
    /// that create a new `DynamicInsert` after invalidation).
    pub fn schema_cache(&self) -> (Arc<DynamicSchemaCache>, String, String) {
        (
            Arc::clone(&self.schema_cache),
            self.database.clone(),
            self.table.clone(),
        )
    }

    /// Invalidate the cached schema, forcing a re-fetch on next insert.
    ///
    /// Call this after a schema mismatch error before creating a new
    /// `DynamicInsert` for the same table.
    pub fn invalidate_schema(&mut self) {
        let full_table = format!("{}.{}", self.database, self.table);
        self.schema_cache.invalidate(&full_table);
        self.schema = None;
    }

    /// Number of rows written so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Get the current schema (if loaded).
    pub fn schema(&self) -> Option<&DynamicSchema> {
        self.schema.as_ref()
    }
}

/// Classify a ClickHouse error as schema mismatch or generic encoding error.
///
/// Schema mismatch covers both explicit column/type errors and data format errors
/// that indicate schema drift (e.g. sending JSON to a column that changed type,
/// or RowBinary data that no longer matches the cached schema).
fn classify_error(database: &str, table: &str, e: crate::error::Error) -> DynamicError {
    let msg = e.to_string();
    if msg.contains("UNKNOWN_IDENTIFIER")
        || msg.contains("NO_SUCH_COLUMN")
        || msg.contains("THERE_IS_NO_COLUMN")
        || msg.contains("TYPE_MISMATCH")
        || msg.contains("ILLEGAL_COLUMN")
        // Data format errors that indicate schema drift — the cached schema no
        // longer matches what the server expects, so RowBinary decoding fails.
        || msg.contains("CANNOT_PARSE")
        || msg.contains("cannot parse")
        || msg.contains("INCORRECT_DATA")
        || msg.contains("incorrect data")
        // ClickHouse error code 117 (INCORRECT_DATA) sometimes appears as a
        // numeric code prefix in error messages.
        || msg.contains("Code: 117")
    {
        DynamicError::SchemaMismatch {
            table: format!("{database}.{table}"),
            message: msg,
        }
    } else {
        DynamicError::EncodingError {
            column: String::new(),
            message: msg,
        }
    }
}
