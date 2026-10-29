# Benchmarking with SDB

This directory contains benchmark scenarios and configuration for running driver benchmarks using SDB.

For full SDB documentation, see:
[Full SDB README](https://github.com/scylladb-drivers-benchmarker/scylladb-drivers-benchmarker/blob/v0.1.0/README.md)

## Setup

Clone SDB and switch to the version currently used in this project:

```sh
git clone <SDB_REPO_URL>
cd <SDB_REPO_NAME>
git checkout tags/v0.1.0
cargo build --release
```

After building SDB, you can:

- run it directly using the full path, or
- create an alias for convenience (recommended)

Example:

```sh
alias sdb="/path/to/sdb/target/release/scylladb-drivers-benchmarker"
```

## Running benchmarks

Run all benchmarks from `config.yml`:

```sh
sdb -d test.db run -b config.yml
```

Run a single benchmark:

```sh
sdb -d test.db run select-small -b config.yml
```

By default, benchmarks are measured using wall-clock time.

## Plotting results

You can plot:

- a single benchmark, by passing its name
- all benchmarks that share the same backend name, by using only `--series`

> [!NOTE]
> If you specify a benchmark name in the plot command, SDB will plot **only that one benchmark**.
>
> If you do **not** specify a benchmark name, SDB will plot **all benchmarks defined in `config.yml`**.
>
> You still need to specify the backend with `--series` in all cases (including when plotting all benchmarks).
>
> If you want a subset (a few, but not all) right now, you need to temporarily comment out the other benchmark entries in `config.yml` :(
>
> I was told this should be automated with a dedicated option/command in the future.

Plot all benchmarks for the `Python-RS` backend:

```sh
sdb -d test.db plot \
  -b config.yml \
  -o result.png \
  --series Python-RS@./ \
  series
```

Plot all benchmarks for the `Python-RS-concurrent` backend:

```sh
sdb -d test.db plot \
  -b config.yml \
  -o concurrent-result.png \
  --series Python-RS-concurrent/ \
  series
```

Plot all benchmarks for the `Python-Driver` backend:

```sh
sdb -d test.db plot \
  -b config.yml \
  -o python-driver-result.png \
  --series Python-Driver@./ \
  series
```

Plot a single benchmark:

```sh
sdb -d test.db plot select-small \
  -b config.yml \
  -o select-small.png \
  --series Python-RS@./ \
  series
```

## Current benchmark set

At the moment, the benchmark suite contains:

### Regular benchmarks (7)

- insert
- complex-insert
- complex-select
- batch
- paging
- select-small
- select-large

### Concurrent benchmarks (5)

- concurrent-insert
- concurrent-select
- concurrent-complex-insert
- concurrent-complex-select
- concurrent-paging

## Backends

The benchmark suite includes two driver implementations:

- **Python-RS**: New async Python driver based on Rust
- **Python-Driver**: Legacy Cassandra Python driver

Each benchmark can be run against either backend. Use `--series Python-RS` or `--series Python-Driver` to plot results for a specific driver.

Example plotting commands for comparing drivers:

```sh
# Plot all benchmarks for Python-RS
sdb -d test.db plot -b config.yml -o python-rs-result.png --series Python-RS series

# Plot all benchmarks for Python-Driver
sdb -d test.db plot -b config.yml -o python-driver-result.png --series Python-Driver series

# Compare both drivers for a specific benchmark
sdb -d test.db plot select-small -b config.yml -o select-small-comparison.png --series Python-RS --series Python-Driver series
```

## Configuration

Benchmarks and backends are defined in `config.yml`.

Each benchmark specifies its benchmark points, for example:

- `starting-step`
- `no-steps`
- `step-progress`
- `progress-type`

Example:

```yml
- name: insert
  starting-step: 1000
  no-steps: 3
  step-progress: 5000
  progress-type: additive
```

Each backend specifies how a benchmark is executed:

```yml
  - name: Python-RS
    benchmark-name: insert
    build-command: "uv run python set-up/set_up_insert.py"
    run-command: uv run python python-rs-driver/insert.py
```

## Notes

- Run SDB from this benchmark directory.
- Benchmark results are stored in `test.db`.
- Cached results are reused between runs unless rerun mode is enabled.
- Plotting can be further customized (e.g. comparing different commits, branches, or repositories using --series options). See the full SDB README for details.
