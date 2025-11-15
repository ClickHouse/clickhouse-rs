//! Bespoke data types for use with ClickHouse.

pub use int256::{Int256, TryFromInt256Error, TryFromUInt256Error, UInt256};

pub(crate) mod int256;
