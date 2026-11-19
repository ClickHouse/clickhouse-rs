# Instructions for agents: upstream SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-rs. See
[`README.md`](README.md) for how it works and how to run it.

## The goal

The goal is to find bugs in clickhouse-rs. To pass more tests is not the
goal. A failed test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A
   test fails for one of three causes:
   - **The client** (clickhouse-rs, or `clickhouse-types`, which parses the
     type names). This is what the harness is for.
   - **The shim** (the program in this directory). It does not do what
     `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this
     server, or through any HTTP client (see
     [Environment causes](#environment-causes)).
2. **Do not fix a client bug in a harness change.** Report it with a small
   reproduction, and fix it in a pull request of its own, with a test in
   `tests/it/`. Then enable the tests of its group.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy
   `clickhouse-client` more exactly. Do not change the shim to hide a client
   bug, for example with another decode path or a workaround for a wrong
   value. The shim must use the client as a user does: `Query::fetch_native()`,
   `Column::iter()` and the public `clickhouse::native::decode::Decode` trait.
4. **Remove a test from the allowlist only when it cannot pass for an
   environment cause.** Write the reason in the commit message.
5. **A test that fails because of a client bug or a shim gap stays
   visible.** Do not delete it. Put it in [`allowlist.txt`](allowlist.txt)
   as a known failure, `# <test>  # [<id>]`, in the group of its cause. Each
   group has a header that describes the bug and gives a small reproduction.
   The header of a client group also has an `Issue:` line. Make a new group
   when no group fits. See the README for the format and for how to run one
   group (`KNOWN_FAILURES=<id>`).
6. **Do not list a test that cannot pass for an environment cause.**

## How to find the cause

Do these steps in this order.

1. **Run the test:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   ```

2. **Run the test through the real `clickhouse-client`** on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here,
   the cause is the client, the shim, or the HTTP interface (the real client
   uses the native protocol; see the list below).
3. **Find the statement that fails.** Set `CLICKHOUSE_CLIENT_CLI_LOG` to a
   file; the log shows each statement and the error. Read the `.diff`,
   `.stdout` and `.stderr` files next to the test.
4. **Compare the shim with the server on that one statement.** Run it
   through the shim (`cargo run --manifest-path tests/clickhouse-test-runner/Cargo.toml -- -q "<SQL>"`)
   and through `curl` with `FORMAT TSV`. Then make the statement smaller
   until it is as small as possible.
5. **Decide between the client and the shim with a small Rust program**
   that uses only the public clickhouse-rs API (`Client::query_raw`,
   `Query::fetch_native`, `Column::iter`, `DataTypeNode::new`). If the
   program also gives a wrong value or an error, the bug is in the client.
   If the program gives the correct value, the bug is in the shim.

### Environment causes

A test cannot pass through any client on the CI server when it needs one of
these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL
  sources.
- Server configuration of the upstream CI: named collections, dictionaries,
  storage policies, disks, users and grants, macros
  (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`, `UNKNOWN_POLICY`,
  `UNKNOWN_DISK`).
- Features that the server build or platform does not have
  (`SUPPORT_IS_DISABLED`).
- Stateful datasets (`--no-stateful`). (Long tests are not tried: the sweep
  of "Grow the allowlist" in the README gives `--no-long`.)
- Behavior of the HTTP interface itself, which every HTTP client has:
  - the server sends no `WITH TOTALS` row and no `extremes` in the `Native`
    format over HTTP;
  - the server sends nothing for an empty result, so the shim cannot write
    the header lines of `TSVWithNames` for an empty result;
  - an HTTP request fails when the current database of the session was
    dropped, detached or renamed, because the request names the database;
  - one request is one query, so the server does not take a multi-statement
    text;
  - the server does not check the block structure over HTTP, so duplicate
    column names (`with 3 as "1" select 1, "1"`) give no
    `AMBIGUOUS_COLUMN_NAME`;
  - the memory use of a query over HTTP can be different from TCP, so an
    expected `MEMORY_LIMIT_EXCEEDED` does not come;
  - the server logs an HTTP query differently: `system.query_log` has
    `interface = 'HTTP'`, and the thread counts, the pipeline and the stack
    traces in the logs are different; some settings (`discard_query_data`)
    have no effect over HTTP;
  - server bugs of the HTTP interface. For example, after
    `SET max_query_size = 18446744073709551615` every later request of the
    session fails as an empty query (`max_query_size + 1` becomes 0;
    ClickHouse/ClickHouse#103110).
- Features of `clickhouse-client` itself that are not part of a driver:
  `INTO OUTFILE`, `FROM INFILE`, client-side parsing of `INSERT` data, and
  client-only settings.
- The tz database. ClickHouse and the `chrono-tz` crate can give different
  historical offsets for the same zone.
- Tests that conflict with other tests of the set: a fixed table UUID, or
  server state that a test leaves behind (failpoints that a failed test did
  not disable, row policies). Such a test passes alone on a fresh server and
  fails in a full run. A failure message with "Injected failure" comes from a
  failpoint of another test. `DROP TABLE` in an `Atomic` database is
  asynchronous, so a test that drops a table and then creates or attaches a
  table with the same fixed UUID can fail with `TABLE_ALREADY_EXISTS`.
- The load of the server. The `EXPLAIN PIPELINE` and `EXPLAIN ANALYZE`
  output of a query with `max_threads` shows the threads that the server
  gives, and under load the server gives fewer (concurrency control), for
  example `Resize 1 → 3` instead of `Resize 1 → 4`. Such a test passes
  alone and fails now and then in a full run. These tests are not in the
  allowlist, because a CI runner has only 4 vCPUs.
- Memory limits of the server, when the test also fails through the real
  `clickhouse-client`.
- The host name of the server. `tests/clickhouse-test` replaces its own host
  name with `localhost` in the output of a test, so a `hostName()` value
  differs when the server has another host name
  (`03217_datetime64_constant_to_ast`). CI and step 1 of the README give the
  server container the host name of the runner (`--hostname "$(hostname)"`).
  Do not remove such a test from the allowlist: fix the server setup.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value.
- A decode error, a type name that the client does not parse, or an error
  such as `not enough data`.
- An error of the shim (`clickhouse-test-runner: ...`).

## Conditions for the allowlist

- Add a test only when it passes.
- Run new tests three times, also with a higher `-j` than CI, on a **fresh**
  server with the configuration of CI (step 1 of "Run the tests locally" in
  the README: `CLICKHOUSE_SKIP_USER_SETUP=1`). Tests change the state of the
  server (row policies, settings profiles, users). A test that passes only
  after other tests ran must not go in.
- CI does not run failed tests again (`RETRY_FAILED`), because no known
  client bug makes random tests fail. Do not use the retry when you grow or
  check the allowlist. If a client bug makes random tests fail, document it
  in its group and in the workflow before you set `RETRY_FAILED` there.
- Do not run two sweeps at the same time on one server. The memory pressure
  causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
- When a test in the allowlist starts to fail intermittently, find the cause
  first (rule 1). Remove it only when the cause is in the environment or in
  the test itself, and write the reason.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). A new ClickHouse release changes
nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own,
and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` tag upstream, normally the version of the `latest`
   image.
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A
   release changes the tests and the server output, so many tests can fail
   at the same time with no change in the client. To see whether the client
   is the cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures (`KNOWN_FAILURES=all`). Enable the tests that pass
   now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. Optionally, try the new tests of the release (see "Grow the allowlist" in
   the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `src/errorcodes.rs` with it.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output
formats, settings), update [`README.md`](README.md), this file and the
workflow in the same change. When the allowlist changes much, update the
counts in the first lines of `allowlist.txt`, in the README ("Known
failures" and "CI") and in the shard comment of the workflow. Run
`cargo fmt` and `cargo clippy --all-targets -- -D warnings` in this
directory (it is its own Cargo workspace).
