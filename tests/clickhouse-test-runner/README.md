# clickhouse-test-runner

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through clickhouse-rs.

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with
the `.reference` file of the test. This harness is a small Rust program that
imitates `clickhouse-client` well enough for the runner to drive it without
changes: the same flags, the same stdin and `--query` behavior, and the
`extract-from-config` shortcut.

The program sends every statement through clickhouse-rs, over HTTP (the only
protocol of clickhouse-rs). It reads each result with `Query::fetch_native()`
and the public `clickhouse::native` API: the client parses the type of each
column into a `DataTypeNode` and decodes the values into the program's own
dynamic `Value` type, which implements the public `Decode` trait. The program
then writes the values as `TabSeparated` text, directed by the column types.
Thus a test passes only when clickhouse-rs parses every type name and decodes
every value correctly.

It is a port of the
[Go](https://github.com/ClickHouse/clickhouse-go/tree/main/tests/clickhouse-test-runner)
harness, which is a port of the
[Java](https://github.com/ClickHouse/clickhouse-java/tree/main/tests/clickhouse-client)
and [JavaScript](https://github.com/ClickHouse/clickhouse-js/tree/main/tests/clickhouse-test-runner)
harnesses.

The program is a Cargo package of its own (with its own workspace, lock file
and `target/` directory). It is not a member of the root workspace and not
part of the published `clickhouse` crate.

## Run the tests locally

You need Rust, Python 3 with `jinja2`, Docker, `git` and `curl`.

1. Start the pinned ClickHouse server. The version is in
   [`clickhouse-version`](clickhouse-version):

   ```bash
   docker run -d --name clickhouse-rs-clickhouse-server -p 8123:8123 -p 9000:9000 \
     --ulimit nofile=262144:262144 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
     "clickhouse/clickhouse-server:$(tests/clickhouse-test-runner/scripts/pinned-version.sh)"
   ```

   This is the stock server configuration, as in CI. With
   `CLICKHOUSE_SKIP_USER_SETUP=1`, the `default` user of the image's
   `users.xml` has network access, access management and named-collection
   control; tests create users, roles and named collections. Do not use
   `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` instead: then the image replaces
   the user with one that has no named-collection control, and more tests
   fail. Do not use the `docker-compose.yml` of the repository either: it
   replaces the server configuration with a small one for the client tests.

2. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-test-runner/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example
   `v26.10.4.207-stable`) into `.upstream/ClickHouse`, sparse and shallow.
   The `.reference` files of that tag were made by the same server build. The
   script also generates `src/errorcodes.rs` from the same checkout. To try
   another version, give it as the argument (`fetch-upstream.sh 26.11.1.10`),
   and start the server of that version.

   The run script stops when the server and the tests are of different
   versions, because then the `.reference` files do not match. Set
   `ALLOW_VERSION_MISMATCH=1` to run anyway.

3. Run the allowlisted tests:

   ```bash
   pip install jinja2
   tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   ```

The script builds the program (`cargo build --release`), copies it to
`.bin/clickhouse` (with a `clickhouse-client` symlink), puts `.bin` first in
`PATH` for the runner only, and starts `tests/clickhouse-test`. It gives the
runner `--no-stateful --no-random-settings --no-random-merge-tree-settings`;
other arguments go to the runner after these. For example, to run one test
(also a test that is not in the allowlist):

```bash
UPSTREAM_TEST_LIST=<(echo 00007_array) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

The runner writes the output of a failed test next to the test, in
`tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless/`
(`<test>.stdout`, `.stderr` and `.diff`).

The unit tests of the harness need no server. The live tests compare the
output of the program with the server's own `TabSeparated` output, and need
the server of step 1:

```bash
(cd tests/clickhouse-test-runner && cargo test)
(cd tests/clickhouse-test-runner && CLICKHOUSE_TEST_RUNNER_LIVE=1 cargo test)
```

To run one query through the program (from the root of the repository):

```bash
cargo run --manifest-path tests/clickhouse-test-runner/Cargo.toml -- -q "SELECT 1"
```

### Environment variables

Relative paths in this table are relative to `tests/clickhouse-test-runner/`.

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_CLICKHOUSE_DIR` | `.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_REPO_URL` | the `ClickHouse/ClickHouse` repository on GitHub | For `fetch-upstream.sh`: the repository to fetch from. |
| `UPSTREAM_TEST_LIST` | `allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `KNOWN_FAILURES` | (none) | Run the known failures of these groups (comma-separated IDs, or `all`) instead of the allowlist. |
| `RETRY_FAILED` | `0` | Run the failed tests again, at most this many times. A test that passes on a retry is a warning. Do not use it to grow or check the allowlist. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | A file. Each invocation of the program adds its arguments, statements and errors to it. |
| `CLICKHOUSE_HOST` | `localhost` | The server host, for the runner and for the version check. |
| `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `9000`, `8123` | The ports of the server. The runner gives `--port=$CLICKHOUSE_PORT_TCP`; the program uses `CLICKHOUSE_PORT_HTTP` for it. |
| `CLICKHOUSE_PORT_TCP_SECURE`, `CLICKHOUSE_PORT_HTTPS` | `9440`, `8443` | The same with `--secure`. |
| `ALLOW_VERSION_MISMATCH` | `0` | `1`: run also when the server and the upstream tests are of different versions. |
| `CLICKHOUSE_TEST_RUNNER_LIVE` | (none) | `1`: `cargo test` also runs the live tests (they need the server). |
| `CLICKHOUSE_CONTAINER` | `clickhouse-rs-clickhouse-server` | For `run-with-real-client.sh`: the server container for `docker exec`. |
| `REAL_CLICKHOUSE_BINARY` | (none) | For `run-with-real-client.sh`: a `clickhouse` binary to use instead of `docker exec`. |

The scripts unset `CLICKHOUSE_DATABASE`: with it, the runner gives the client
`--database` two times, and every test fails.

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test
name (the file name without `.sql`) per line. `#` starts a comment. The script
gives the runner anchored regular expressions, so each name selects exactly
one test. (The runner itself matches each name as a substring, so
`00396_uuid` would also select `00396_uuid_v7`.)

The file has two kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs. |
| `# 00007_array  # [type-nothing]` | A **known failure**: the test does not run, because of a bug in clickhouse-rs or a gap in the shim. The ID in brackets names the group. The line is under the header of its group, in the known-failure part at the end of the file; the header describes the bug and gives a small reproduction. |

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=type-nothing tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

When a fix makes the tests of a group pass, remove the `# ` at the start and
the `# [<id>]` at the end of their lines. When all tests of a group pass,
remove the group header. A test can fail for more than one cause; it is in
the group of the first cause.

### Grow the allowlist

1. Run the candidate tests through the harness. To try all `.sql` tests:

   ```bash
   ls tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   UPSTREAM_TEST_LIST=/tmp/candidates.txt \
     tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8 --no-long --timeout 120 \
       --max-failures-chain 1000000 | tee /tmp/run.log
   ```

   `--max-failures-chain` turns off a limit of the runner: by default it
   stops after 20 failures in a row, which a list of failing tests reaches at
   once. Do not run two sweeps at the same time on one server: the memory
   pressure causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.

2. Keep only the tests that report `[ OK ]`:

   ```bash
   sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run.log | sort -u
   ```

3. Run the new tests again (two or more times, and with `-j` higher than CI
   uses) and remove the tests that do not pass every time.

4. Make sure that the tests also pass on a fresh server, because CI starts
   from one. For example, a test that reads `system.query_log` can depend on
   the queries of other tests.

**Rule:** add only tests that pass reliably. Before you remove a test or
decide not to add it, find out whether it fails because of a bug in
clickhouse-rs. The harness exists to find those bugs. See
[`AGENTS.md`](AGENTS.md) for the steps.

To compare with the real `clickhouse-client`, run the same tests through it:

```bash
UPSTREAM_TEST_LIST=<(echo 00007_array) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
```

By default the script runs `clickhouse-client` in the server container
(`docker exec`; set `CLICKHOUSE_CONTAINER` to its name). With
`REAL_CLICKHOUSE_BINARY=/path/to/clickhouse` it runs that binary. A test that
passes there and fails through clickhouse-rs shows a bug in clickhouse-rs, in
this program, or a difference of the HTTP interface (see
[`AGENTS.md`](AGENTS.md#environment-causes)).

## How the program imitates clickhouse-client

The program sends nothing that `clickhouse-client` does not send, because some
tests read their own queries from `system.query_log`.

- **Statements.** The program splits the input into statements as
  `clickhouse-client` does: at a top-level `;`, with the same-line trailing
  comment kept with its statement. It sends each statement with its `;`. The
  inline data of `INSERT ... FORMAT <name>` (other than `Values`) ends at an
  empty line. The `-- Tags:` header is not part of a statement. Every
  statement runs, with or without `--multiquery`. The input must be UTF-8:
  `Client::query_raw` takes the SQL as `&str`.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and
  `-- { error X }` (codes or names) make the error the expected result. An
  error hint in a leading comment is ignored, as upstream. Through
  clickhouse-rs the server finds all errors, so a server error can satisfy a
  `clientError` hint. `-- { echo }` and `-- { echoOn }` (in any comment) start
  to print the statements and `-- { echoOff }` stops it; a statement with a
  syntax error is not printed (`clickhouse-client` parses before it echoes).
  `-- { retry ... }` is not supported.
- **Sessions.** `SET`, `USE` and temporary tables last for the whole
  invocation: the client has one `session_id` setting
  (`Client::with_setting("session_id", ...)`), so all requests run in one
  server session, also after an error. clickhouse-rs sends the database of the
  client with each request (`Client::with_database`), so after a `USE` the
  program changes the database of the client.
- **Statement types.** Each statement is a `Client::query_raw()` query (not
  `Client::query()`, which reads `?` as a bind placeholder). Statements that
  start with `SELECT`, `WITH`, `FROM`, `SHOW`, `DESC`, `DESCRIBE`, `EXISTS`,
  `EXPLAIN`, `CHECK` or `KILL` run with `Query::fetch_native()`; all other
  statements run with `Query::execute()`. An `INSERT` with inline data goes as
  one statement through `execute()`: the HTTP request body has the query and
  the data. After `SET dialect` to another language (KQL, PRQL, ...) or
  `SET implicit_select = 1`, more statements run with `fetch_native()`: in
  another language all statements other than `SET`, and with `implicit_select`
  all statements that do not start with a DDL or DML keyword (`INSERT`,
  `CREATE`, `SET`, ...). A statement in another language goes without its
  trailing `;` (the server takes the request body as one query). The program
  splits KQL statements by the SQL rules, which is not correct for all KQL
  (known shim gap `[shim-kql-split]`).
- **Values.** The program reads each column of each block with
  `Column::iter::<Value>()`. `Value` implements
  `clickhouse::native::decode::Decode`: the client does the column layout
  (null maps, `LowCardinality` dictionaries, array offsets, maps) and calls
  `decode` for each value. `decode` calls the client's own decoders for the
  integers, floats, `Bool`, `IPv4`, `IPv6` and `UUID` (the `uuid` feature). It
  reads the other fixed-width values (`Date`, `DateTime`, `Decimal`, `Enum`,
  `Time`, ...) with the client's integer decoders of their storage width, and
  the 256-bit values with `ValueReader::read_bytes_fixed()`. A `String` is the
  raw bytes (`ValueReader::native_bytes()`). Map entries keep the order of the
  server. A custom `Decode` type cannot decode a tuple (see
  `[native-tuple-iter-not-public]` in the allowlist), so the program reads a
  `Tuple`, `Nullable(Tuple)` or `Array(Tuple)` column with the client's own
  tuple decoder, `(Value, Value, ...)`.
- **Rendering.** The program writes the values as the server writes
  `TabSeparated`: escaped strings and `\N` at the top level, quoted strings
  and `NULL` inside `Array`, `Tuple` and `Map`; the shortest float text in the
  layout of the server (`1e21`, `1.5e-7`, `nan`, `-0`); decimals without
  trailing zeros; `DateTime64(P)` with P digits; `Time` up to `999:59:59`;
  IPv6 as `formatIPv6` writes it. A `DateTime` with a time zone in its type is
  written in that zone (with the `chrono-tz` rules); a `DateTime` without a
  time zone is written in UTC, because clickhouse-rs does not tell the time
  zone of the server or the session (the CI server runs in UTC; see
  `[session-timezone]`). The program does not follow the settings that change
  the text output of the server, such as
  `output_format_decimal_trailing_zeros` (known shim gap
  `[shim-output-setting]`).
- **Output formats.** The server sends `Native`, and the program writes the
  result itself in the `TabSeparated` family (also `TSVRaw` and
  `...WithNames[AndTypes]`) or `Null`. A statement with any other output
  format fails (known shim gap `[shim-output-format]`: the program does not
  ask the server for a text format, because then the test checks only the
  transport). The program removes the `FORMAT` clause of a query, so that the
  server uses the `Native` default format of `fetch_native()`. It keeps
  `FORMAT Null` (the server sends no rows; for `EXPLAIN` the server still
  sends the plan, and the program writes it), and it keeps the clause in
  another language than SQL. The type line of `...WithNamesAndTypes` is the
  `DataTypeNode` of the client.
- **Settings.** Of the settings on the command line, only `log_comment`,
  `max_insert_threads`, `max_threads` and `send_logs_level` go to the server,
  with each query (`Query::with_setting`), as in `clickhouse-client`. A `SET`
  of the same setting in the test replaces the command-line value.
  `--param_<name>=<value>` gives a query parameter
  (`Query::with_setting("param_<name>", value)`, the raw value, as
  `clickhouse-client` sends it).
- **Errors.** An unexpected error stops the program. It writes the error to
  stderr (a server error with its code and name), and the exit code is the
  error code modulo 256 (or 1). clickhouse-rs gives a server error as the text
  of the exception (`Code: 60. DB::Exception: ...`); the program reads the
  code from it.
- **Error names.** [`src/errorcodes.rs`](src/errorcodes.rs) maps the error
  names of test hints to codes.
  [`scripts/gen-error-codes.sh`](scripts/gen-error-codes.sh), which
  `fetch-upstream.sh` runs after each checkout, generates it from
  `src/Common/ErrorCodes.cpp` of the upstream checkout (with bash, sed and
  grep; no Rust toolchain), so it always matches the tests. Commit the file
  when it changes.

## Known failures

The end of [`allowlist.txt`](allowlist.txt) lists the tests that fail
because of a bug in clickhouse-rs or a gap in this program, in groups. Each
group header describes the cause and gives a small reproduction. A test that
cannot pass for an environment cause (a cluster, ZooKeeper, server
configuration, features of `clickhouse-client` itself, the HTTP interface) is
not listed; see [`AGENTS.md`](AGENTS.md#environment-causes). The program does
not try `.sh` tests.

At the pinned version 26.10.4.207, of the 12,961 `.sql` and `.sql.j2` tests,
9,680 run and 1,153 are known failures in 26 groups: 22 client groups (854
tests) and 4 shim groups (299 tests). The other tests cannot pass for an
environment cause, are long tests (the sweep of "Grow the allowlist" gives
`--no-long`), or the runner skips them on this server (tests that need
shards or the stateful datasets). The first lines of `allowlist.txt` give the
current counts.

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml)
runs the allowlist nightly, manually, on pushes to `main` and on pull
requests that change this directory or the workflow.

- The `lint` job runs `cargo fmt -- --check`, `cargo clippy --all-targets --
  -D warnings` and the unit tests of the program. The `ci.yml` workflow does
  not build this package, because it is not a member of the root workspace.
- The `upstream-sql-tests` jobs are the shards `1..N` of the allowlist
  (`SHARD_INDEX`, `SHARD_TOTAL`), at `-j 4`. There are 4 shards of about
  2,420 tests. One shard takes about 4 minutes of tests on a 4-vCPU runner,
  plus the build of the program, the upstream checkout and the server start.
  (The full list takes about 10 minutes at `-j 6` on 32 vCPUs.) The job of
  shard 1 also runs the live tests of the program.
- The server is the stock image of the pinned version with
  `CLICKHOUSE_SKIP_USER_SETUP=1`, as in step 1 of
  [Run the tests locally](#run-the-tests-locally).
- CI does not run failed tests again (`RETRY_FAILED` is not set): no known
  client bug makes random tests fail.
- On a failure, the job uploads the `.stdout`, `.stderr` and `.diff` files
  of the failed tests and the log of the program as an artifact.
- The server version and the upstream tests are pinned together in
  [`clickhouse-version`](clickhouse-version). A new ClickHouse release
  changes nothing in CI, so a pull request never fails because of a release
  that it does not touch. A bump is a pull request of its own; see
  [`AGENTS.md`](AGENTS.md#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version
  without a commit, for example before a bump.
- When an allowlisted test is not in the checkout (upstream renamed or
  removed it), the run shows a warning with the names.
- If a shard takes much more than a few minutes, increase the number of
  shards: change the `shard` list and `SHARD_TOTAL` together.
