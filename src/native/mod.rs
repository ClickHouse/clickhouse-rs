//! ClickHouse native TCP protocol (port 9000).
//!
//! Alternative transport to the default HTTP/RowBinary path. Ported and
//! extended by HYPERI PTY LIMITED from the HyperI `clickhouse-arrow` fork.
//! API names follow the ClickHouse Go client convention.

// HyperI CTO moonlighting — ClickHouse Rust client needed love, so here we are.

pub(crate) mod async_inserter;
pub(crate) mod block_info;
pub(crate) mod callbacks;
pub(crate) mod client_info;
pub(crate) mod client;
pub(crate) mod columns;
pub(crate) mod compression;
pub(crate) mod connection;
pub(crate) mod cursor;
pub(crate) mod encode;
pub(crate) mod error_codes;
pub(crate) mod insert;
pub(crate) mod inserter;
pub(crate) mod pool;
pub(crate) mod io;
pub(crate) mod protocol;
pub(crate) mod query;
pub(crate) mod reader;
pub(crate) mod schema;
pub(crate) mod sparse;
pub(crate) mod tcp;
pub(crate) mod writer;

pub use self::async_inserter::{AsyncNativeInserter, AsyncNativeInserterConfig, AsyncNativeInserterHandle};
pub use self::client::NativeClient;
pub use self::cursor::NativeRowCursor;
pub use self::insert::NativeInsert;
pub use self::inserter::NativeInserter;
pub use self::protocol::{Progress, ProfileInfo};
pub use self::query::NativeQuery;
