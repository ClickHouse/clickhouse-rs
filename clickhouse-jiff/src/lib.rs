/*!

This crate provides wrappers for various types from [`jiff`]
that, at the moment, provide implementations of [`serde`]'s
`Deserialize` and `Serialize` that are compatible with [`clickhouse`].

# Available types

* [`Date16`] - wraps [`jiff::civil::Date`], corresponds to ClickHouse's [`Date`](https://clickhouse.com/docs/sql-reference/data-types/date);
* [`Date32`] - wraps [`jiff::civil::Date`], corresponds to ClickHouse's [`Date32`](https://clickhouse.com/docs/sql-reference/data-types/date32);
* [`SignedDuration32`] - wraps [`jiff::SignedDuration`], corresponds to ClickHouse's [`Time`](https://clickhouse.com/docs/sql-reference/data-types/time);
* [`SignedDuration64<precision>`](SignedDuration64) - wraps [`jiff::SignedDuration`], corresponds to ClickHouse's [`Time64(precision)`](https://clickhouse.com/docs/sql-reference/data-types/time64);
* [`Timestamp32`] - wraps [`jiff::Timestamp`], corresponds to ClickHouse's [`DateTime`](https://clickhouse.com/docs/sql-reference/data-types/datetime);
* [`Timestamp64<precision>`](Timestamp64) - wraps [`jiff::Timestamp`], corresponds to ClickHouse's [`DateTime64(precision)`](https://clickhouse.com/docs/sql-reference/data-types/datetime64).

# Warning regarding 64-bit types

It is important to make sure that the precision of a wrapper
matches that of a column.

Failure to do so **will** cause data corruption, since the official
driver does not introspect that information.

[`clickhouse`]: https://docs.rs/clickhouse
[`jiff`]: https://docs.rs/jiff
[`serde`]: https://docs.rs/serde

*/

mod wrappers;

pub use self::wrappers::*;
