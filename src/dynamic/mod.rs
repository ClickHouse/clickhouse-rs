//! Runtime schema-driven inserts for dynamic schemas.
//!
//! Use this when table schemas are not known at compile time.
//! `DynamicInsert` fetches the schema from `system.columns` and encodes
//! `Map<String, Value>` directly to RowBinary — same ease as JSONEachRow
//! but without the server-side JSON parsing overhead.
//!
//! # Why This Exists — End-to-End CPU Savings
//!
//! JSONEachRow is easy: push JSON text, ClickHouse parses it. But at scale,
//! the ClickHouse cluster itself pays the CPU cost of parsing every JSON row
//! on ingest. That's not "someone else's problem" — it's your total solution
//! budget. If your ClickHouse cluster is CPU-loaded because every INSERT runs
//! through a JSON parser, that's capacity you can't use for queries.
//!
//! Schema-reflected RowBinary shifts the work to the client: fetch the schema
//! once, encode binary directly, ClickHouse receives pre-columnarised data
//! with zero parsing. The client does roughly the same work (binary encoding
//! instead of JSON serialisation), but the server does dramatically less.
//! The big picture: total CPU across client + cluster drops significantly.
//!
//! "Hey, my app works — if the CH cluster is loaded, that's the infra team's
//! problem" is exactly the mindset this module replaces. Think end-to-end.
//!
//! # Three Insert Tiers
//!
//! | Tier | API | Use When |
//! |------|-----|----------|
//! | 1 | `Insert<T>` / `Inserter<T>` | Compile-time schema, `#[derive(Row)]` |
//! | 2 | `DynamicInsert` | Runtime schema, `Map<String, Value>` (this module) |
//! | 3 | `InsertFormatted` | Raw bytes, any format (JSONEachRow, CSV) |
//!
//! Tier 2 gives you the ergonomics of Tier 3 (push any JSON map) with the
//! performance profile of Tier 1 (ClickHouse skips JSON parsing entirely).

pub mod batcher;
pub mod encode;
pub mod error;
pub mod insert;
pub mod parsed_type;
pub mod schema;

pub use batcher::{DynamicBatchConfig, DynamicBatcher, DynamicBatcherHandle};
pub use error::DynamicError;
pub use insert::DynamicInsert;
pub use parsed_type::ParsedType;
pub use schema::{ColumnDef, DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
