# Benchmarks

All cases are run with `cargo bench --bench <case>`.

## With a mocked server

These benchmarks are run against a mocked server, which is a simple HTTP server that responds with a fixed response. This is useful to measure the overhead of the client itself:
* `select` checks throughput of `Client::query()`.
* `insert` checks throughput of `Client::insert()` and `Client::inserter()` (if the `inserter` features is enabled).

### How to collect perf data

The crate's code runs on the thread with the name `testee`:
```bash
cargo bench --bench <name> &
perf record -p `ps -AT | grep testee | awk '{print $2}'` --call-graph dwarf,65528 --freq 5000 -g -- sleep 5
perf script > perf.script
```

Then upload the `perf.script` file to [Firefox Profiler](https://profiler.firefox.com).

## With a running ClickHouse server

These benchmarks are run against a real ClickHouse server, so it must be started:
```bash
docker compose up -d
cargo bench --bench <case>
```

Cases:
* `select_numbers` measures time of running a big SELECT query to the `system.numbers_mt` table.

### How to collect perf data

```bash
cargo bench --bench <name> &
perf record -p `ps -AT | grep <name> | awk '{print $2}'` --call-graph dwarf,65528 --freq 5000 -g -- sleep 5
perf script > perf.script
```

Then upload the `perf.script` file to [Firefox Profiler](https://profiler.firefox.com).
