//! Runtime schema-driven inserts for dynamic schemas.
//!
//! Use this when table schemas are not known at compile time.
//! `DynamicInsert` fetches the schema from `system.columns` and encodes
//! `Map<String, Value>` directly to RowBinary — same ease as JSONEachRow
//! but without the server-side JSON parsing overhead.
//!
//! # Three Insert Tiers
//!
//! | Tier | API | Use When |
//! |------|-----|----------|
//! | 1 | `Insert<T>` / `Inserter<T>` | Compile-time schema, `#[derive(Row)]` |
//! | 2 | `DynamicInsert` | Runtime schema, `Map<String, Value>` (this module) |
//! | 3 | `InsertFormatted` | Raw bytes, any format (JSONEachRow, CSV) |
//!
//! Tier 2 gives you the ergonomics of JSONEachRow (push any JSON map) with
//! the performance of RowBinary (ClickHouse skips JSON parsing entirely).

pub mod error;
pub mod parsed_type;
pub mod schema;

pub use error::DynamicError;
pub use parsed_type::ParsedType;
pub use schema::{ColumnDef, DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
