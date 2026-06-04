# Copilot Code-Review Instructions — `clickhouse-rs`

This file configures the GitHub Copilot code-review agent for the
`ClickHouse/clickhouse-rs` repository (an async ClickHouse client for Rust).

The rules below were distilled from recurring maintainer feedback on merged pull
requests. When reviewing a change, check it against these rules and flag any
violations. Keep comments concrete and actionable, and cite the relevant rule.
Do not comment on pure style/formatting that `cargo fmt` and `cargo clippy`
already enforce — call those out only when CI would fail.

## How to review

- Prioritize correctness, public-API design, backwards compatibility, and
  security over stylistic nits.
- Prefer the smallest change that fully addresses an issue. Apply YAGNI: do not
  ask for, or approve, API surface that isn't needed yet.
- When a rule is violated, reference it (e.g. "See Naming & API design") and
  suggest the concrete fix.

## Naming & API design

- Use the `with_*` prefix for all public builder/setter methods on `Client`,
  `Query`, `Insert`, and `Inserter` (e.g. `with_url`, `with_setting`,
  `with_compression`). This keeps the configuration API discoverable.
- Prefer one general method over several narrow variants (e.g. a single
  `with_roles(impl IntoIterator<Item = impl Into<String>>)` instead of
  `with_role` + `with_default_roles` + `with_roles`).
- Boolean-flag `with_*` methods must take a `bool` argument rather than being
  zero-argument toggles, so they compose with configuration structs
  (`.with_validation(config.validate)`).
- Expose raw/unsafe bypass paths with an explicit `_unescaped` suffix (e.g.
  `insert_unescaped()`); never auto-detect "magic" conditions (such as a dot in
  a table name) to switch behavior.
- Name callback/hook combinators `with_<noun>_callback` (e.g.
  `with_commit_callback`), not `with_on_<verb>`.
- Mark public enums that may gain variants — especially error enums — as
  `#[non_exhaustive]`.
- Read accessors should take `&self` (not `&mut self`); use `get_*` / `set_*`
  naming for inspection/mutation of options on `Client`.

## SQL & query safety

- Never build SQL with `format!()` in library code or examples. Use ClickHouse
  bind parameters (values and identifiers are escaped server-side). Examples
  must teach the safe pattern.
- `Client::insert()` always escapes the table name. Pre-escaped or qualified
  names (`db.table`) go through `insert_unescaped()`, where quoting is the
  caller's responsibility.

## Error handling

- Do not introduce a panic into a previously non-panicking, released code path.
  A new panic is a breaking behavior change and must wait for the next major
  version.
- Add typed variants to `Error` instead of wrapping everything in
  `Error::Other`. Remember each new variant is a SemVer hazard, so batch such
  changes at a major-version boundary.
- Deserialize unstable external data (e.g. the `X-ClickHouse-Summary` header)
  forward-compatibly: use `#[serde(default)]` and a `#[serde(flatten)]`
  catch-all map rather than a rigid struct.

## Documentation & changelog

- Every user-visible change needs a `CHANGELOG.md` entry under the correct
  heading: `Added`, `Changed`, `Fixed`, or `Removed`. `Removed` is only for
  deleted public APIs; internal-only removals go under `Changed`.
- When renaming a public API, keep the old name with `#[deprecated]` and
  document both the new method and the deprecation; schedule removal for a
  future major version.
- All public APIs must have rustdoc; the crate will enforce `missing_docs`.
  Use `[brackets]` for intra-doc links so the docs build does not break.
- Document important caveats inline on the API, not only in the changelog
  (e.g. high Zstd levels block the async executor; `summary()` needs
  `wait_end_of_query=1`). Prefer showing behavior with an example over prose.
- In examples, inline SQL strings and table names; don't extract them into
  constants or helper variables that obfuscate the flow.

## Testing

- New features and bug fixes require integration tests under `tests/it/`.
  Tests exercising experimental server features must enable the required
  settings via `.with_setting(...)` (e.g.
  `.with_setting("allow_experimental_variant_type", "1")`).
- Before submitting, changes must pass `cargo test`,
  `cargo test --no-default-features`, and `cargo test --all-features`, plus
  `cargo fmt` and `cargo clippy --all-targets`.
- Performance-sensitive changes (de/serialization, transport, buffering,
  tracing/observability) must include benchmark results from the `benches/`
  suite.
- It is acceptable to omit tests for genuinely unreachable branches when
  justified; do not add unreachable tests just to satisfy coverage.

## Tracing & observability

- `tracing` span/event messages must be static strings; put dynamic data
  (query text, error details) into span *fields* so events can be aggregated.
- Do not surface internal/private implementation failures at `WARN`/`ERROR`;
  use `DEBUG`, and avoid noisy library-level `DEBUG` events that users would
  have to suppress (e.g. expected retry/overload errors).

## Dependencies & feature flags

- External contributors must not upgrade dependencies without prior maintainer
  discussion (security policy in `CONTRIBUTING.md`).
- Use weak feature dependencies (`dep?/feature`, e.g. `arrow-ipc?/lz4`) so a
  sub-dependency is not pulled in unless its own feature is enabled.
- Keep public-API dependencies that release breaking versions on a fast cadence
  (e.g. `arrow`) in a separately versioned sub-crate, not in the core
  `clickhouse` crate.
- Remove abandoned or unused (dev-)dependencies to keep the graph clean.

## Backwards compatibility

- Never silently remove or rename public API in a minor/patch release —
  deprecate first, remove at the next major version.
- Treat panicking-behavior changes as breaking and defer them to the next major
  version.
- Avoid arbitrary hard-coded upper limits (block sizes, compression levels,
  etc.) unless ClickHouse itself imposes them; such limits become footguns for
  legitimate use cases.

## Examples

- Add runnable examples under `examples/` and update `examples/README.md` for
  major new features.
- Place format/integration examples (e.g. Arrow) under `Special cases` in
  `examples/README.md`, not in the ClickHouse data-types section.
