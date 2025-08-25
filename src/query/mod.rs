// Suppress Clippy warning due to identical names for the folder and file (`query`)
#![allow(clippy::module_inception)]

// Declare the submodules
mod query;
mod query_flags;

// Re-export public items
pub use crate::cursors::{BytesCursor, RowCursor};
pub use query::Query;
pub use query_flags::QI;
