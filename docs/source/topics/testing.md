# Testing

TBD.

## Code coverage

`make coverage` measures how much of the driver the tests exercise. It runs
`scripts/coverage.sh`, which writes its reports to `target/coverage/reports`.

### Running it locally

Start the test cluster first, as for `make test`:

```sh
make up
make coverage
```

It needs, on top of what `make test` needs:

- the `llvm-tools-preview` component of the Rust toolchain:
  `rustup component add llvm-tools-preview`;
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov), the version CI
  pins in `.github/workflows/coverage.yml`:
  `cargo install --locked cargo-llvm-cov@0.9.1`.

The script checks for its tools first and names any that are missing.
Set `COVERAGE_PYTHON` to measure with a Python other than 3.14.

### What is measured

Two suites run, in this order:

1. The native Rust tests (`make test-rust`, as CI runs them).
2. The Python tests (`pytest`, as CI runs them), against a build of the
   extension instrumented for coverage.

Every suite runs even if an earlier one fails, so the reports are always
written. The script exits non-zero if any suite or check failed. It writes
three reports:

| Report | Collector | Measures |
| --- | --- | --- |
| `python.xml` | [coverage.py](https://coverage.readthedocs.io/) | The Python package, `python/scylla`, during the Python tests. |
| `rust-native.lcov` | cargo-llvm-cov | The Rust crate, `src`, during the native Rust tests. |
| `rust-python.lcov` | cargo-llvm-cov | The Rust crate, `src`, during the Python tests. |

Both collectors count lines: coverage.py counts statements, and llvm-cov counts
the lines that hold code. Neither measures branches. A line that runs in a
child process the tests start counts too, unless that process ends without
exiting normally: a process killed by a signal or ended by `os._exit` writes no
coverage data.

These are left out:

- The tests themselves (`python/tests`, `src/tests`), the examples, and the
  build script: none of them ship as part of the driver.
- The Python type stubs (`.pyi`): they hold no code that runs.
- The Rust driver. cargo-llvm-cov picks the crates to instrument by name,
  and the driver's crate is also called `scylla`, so it is instrumented too
  and runs slower, but its files are outside the repository and left out of
  the reports.
- Other Python versions, and the free-threaded builds. The tests run on one
  Python, 3.14 with the GIL, on one Linux x86-64 host.
- Other servers. The tests run against the ScyllaDB image `make up` starts,
  the same as in CI, and the ScyllaDB releases the CCM tests pin. Cassandra
  is not measured.

### What is checked

A green run means everything counted was measured. After the suites run,
`scripts/coverage_check.py` fails the run if:

- a suite ran no tests (pytest's skipped tests do not count);
- a report is missing, measures no lines, or covers none;
- a report covers no line of the code it measures, or names files outside it;
- a report names a file git does not track, which Codecov could not match;
- the Python report leaves out a module of the package. coverage.py lists
  even the modules no test imported, so a missing module means it was lost.

The Rust crate is checked as a whole, not file by file: llvm-cov leaves out
files that hold no code at all, such as a `mod.rs` that only declares
modules. The crate is compiled as one unit, so it is instrumented as a whole
or not at all.

`python3 scripts/coverage_tests.py` tests the script and the checks. It runs
the script against stub commands, and needs nothing but Python and git.

### What a run leaves behind

- `target/coverage`: the reports, a summary, the test logs, the instrumented
  wheel and the virtualenv it is installed in.
- `target/llvm-cov-target`: the instrumented build, kept apart from the normal
  one so it does not force a rebuild of either.
- `uv.lock`, which `uv` creates as it does for `make test`. Git ignores it.

`cargo clean` removes both directories.

### In CI

`.github/workflows/coverage.yml` runs the same script on pull requests and on
pushes to `main`. It first runs `scripts/coverage_tests.py`, then starts the
cluster and runs `make coverage`. The job summary shows the result of each
check, and the reports are kept as the `coverage` artifact of the run.

The job then uploads the three reports to
[Codecov](https://codecov.io/gh/scylladb/python-rs-driver) in one upload,
which merges them. `codecov.yml` holds its settings:

- Codecov's statuses on a pull request are informational: they show the
  change in coverage, and never fail.
- Only a run in which every suite and check passed uploads. A partial report
  would become the baseline later pull requests are compared against. A commit
  without an upload is compared against the nearest ancestor that has one.
- Pull requests from forks get no upload token. Their upload is best effort,
  and a failed upload does not fail their job.

### Maintenance

Pinned versions, to update by hand:

- cargo-llvm-cov: `CARGO_LLVM_COV_VERSION` in `.github/workflows/coverage.yml`,
  and the install command above.
- The Codecov CLI: the `version` input of the upload step in
  `.github/workflows/coverage.yml`. Renovate updates the action, not this.

The upload needs the repository's Codecov upload token in the
`CODECOV_TOKEN` repository secret.

coverage.py is pinned in the `coverage` dependency group in `pyproject.toml`,
which Renovate updates.
