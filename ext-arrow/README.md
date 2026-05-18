# `clickhouse-ext-arrow`

Extension traits for the [official ClickHouse Rust client][clickhouse-rs] adding support for the [Apache Arrow ecosystem].

## `arrow`/`clickhouse` Version Matrix

Use the following table to choose compatible versions of the `clickhouse`, `arrow`, and `clickhouse-ext-arrow` crates
(using Cargo's [default version requirement syntax][caret-versions]):

<!-- Sorting by the `clickhouse` crate version first makes it easier to maintain the table. -->

| `clickhouse` version  | `arrow` version | `clickhouse-ext-arrow` version |
|-----------------------|-----------------|--------------------------------|
| `clickhouse = "0.15"` | `arrow = "58"`  | `clickhouse-ext-arrow = "0.1"` |

Newer minor versions that are compatible with these specifications may be substituted, for example: 

```toml
[dependencies]
clickhouse = "0.15.1"
arrow = "58.3.0"
clickhouse-ext-arrow = "0.1.0"
```

[caret-versions]: https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#version-requirement-syntax

## Why a Separate Crate?

At the time of the creation of this crate, the `arrow` family of crates has an unconventionally fast release cadence, 
with [a new major version bump scheduled every three months][arrow-release-schedule].

If `arrow` was an optional feature of the `clickhouse` crate itself, we would then need to also release a new major
version linking the new version of `arrow` every three months. 

However, this would not fit in with the plans for our crate; as of writing (May 2026), we want to publish a 1.0 release 
in the near future and then continue supporting that 1.0 release for some time.

The `arrow-rs` maintainers [have discussed a slower release cadence][arrow-rs-5368] but the conclusion appears
to be that the API is still evolving and the fast pace is necessary to keep up with the rate of changes.

This leaves us two options:

### Option 1: add a separate optional feature for each new major version of `arrow`.
E.g. `arrow-56` linking `arrow = "56.0.0"`, `arrow-57` for `arrow = "57.0.0"`, etc.

This would accumulate new optional features at a rate of one per three months. After a year, that's four different 
versions we'd have to support. After two years, eight. Three years, twelve.

To ensure that the old versions still build, we'd need to have a CI pass that checks each feature. And whenever the APIs
of the versions diverged, we'd have to create a separate, nearly-identical copy of the same code and maintain them both.

This would quickly become untenable. At a certain point, we'd want to have a 2.0 release _anyway_ to deprecate
and remove the old versions.

### Option 2: use a separate crate.
By lifting Arrow support to a separate subcrate, we can decouple the `clickhouse` version from the `arrow` crate
version. A new release of the former or the latter just means one new release of the `clickhouse-ext-arrow` crate.

### What about a version range?
Using a version range for a public dependency in Cargo does not work because it is not guaranteed to unify.

For example, if we specify `arrow = ">56.0.0,<59.0.0"` and a downstream dependency specifies `arrow = "57.3.1"`,
there is nothing stopping Cargo from selecting a newer version of `arrow` for the former. APIs from the two
versions would not be compatible, resulting in compilation errors.

This approach would also have no way to deal with breaking changes to the APIs we use without additional hacks,
like generating `cfg` flags for each version at build time. This unfortunately just does not mesh with how Cargo is 
designed.

This only works when the dependency crate has a `links` directive, because Cargo does not allow more than one
version to exist in the dependency graph at a time. The [Diesel][diesel-sqlite] and [SQLx][sqlx-sqlite] (as of 0.9.0) 
crates both use this trick to support multiple versions of `libsqlite3-sys`, for example.

[clickhouse-rs]: https://github.com/clickhouse/clickhouse-rs
[Apache Arrow ecosystem]: https://docs.rs/arrow
[arrow-release-schedule]: https://github.com/apache/arrow-rs/tree/58.3.0#release-versioning-and-schedule
[arrow-rs-5368]: https://github.com/apache/arrow-rs/issues/5368
[diesel-sqlite]: https://github.com/diesel-rs/diesel/blob/2e7eb3567574f6d8660e97ab83abac1b71631e63/diesel/Cargo.toml#L53
[sqlx-sqlite]: https://github.com/launchbadge/sqlx/blob/6956cef0bf3cea81bc0a543a53d550b4dd8d56db/sqlx-sqlite/Cargo.toml#L59
