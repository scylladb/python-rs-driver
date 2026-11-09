#!/usr/bin/env bash
# Measures test coverage of the driver: the Python package with coverage.py and
# the Rust extension with cargo-llvm-cov. Run it as `make coverage` with the
# test cluster up (`make up`). See docs/source/topics/testing.md.
#
# Suites:
#   1. the native Rust tests (`make test-rust`), measuring the Rust code;
#   2. the Python tests, run against an instrumented build of the extension,
#      measuring both the Python package and the Rust code it calls.
#
# Every suite runs even if an earlier one failed, so the reports are always
# written. The exit status is non-zero if any suite or check failed.
#
# Leaves behind: target/coverage (reports, logs, wheel and a virtualenv) and
# target/llvm-cov-target (the instrumented build). `cargo clean` removes both.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

python_version=${COVERAGE_PYTHON:-3.14}
out=target/coverage
reports=$out/reports

die() {
    echo "coverage: $*" >&2
    exit 1
}

status=0
failed() {
    echo "coverage: $*" >&2
    status=1
}

missing=()
for tool in cargo uv make; do
    command -v "$tool" >/dev/null || missing+=("$tool")
done
if ((${#missing[@]} == 0)) && ! cargo llvm-cov --version >/dev/null 2>&1; then
    missing+=(cargo-llvm-cov)
fi
((${#missing[@]} == 0)) || die "missing ${missing[*]}; see docs/source/topics/testing.md"

# Instrument the workspace crate, and keep its build apart from the normal one.
export CARGO_TARGET_DIR=$PWD/target/llvm-cov-target
# An inherited value would send the profiles elsewhere while the reports
# look for them here.
unset CARGO_LLVM_COV_TARGET_DIR
cov_env=$(cargo llvm-cov show-env --sh --remap-path-prefix) || die "cargo llvm-cov show-env failed"
eval "$cov_env"
cargo llvm-cov clean --workspace || die "cargo llvm-cov clean failed"
rm -rf "$out" || die "cannot remove $out"
mkdir -p "$reports" "$out/python" || die "cannot create $out"

rust_report() {
    cargo llvm-cov report --remap-path-prefix --lcov --output-path "$reports/$1.lcov" ||
        failed "cannot write the $1 report"
}

echo "== Native Rust tests"
make test-rust 2>&1 | tee "$out/rust-tests.log" || failed "native Rust tests failed"
rust_report rust-native

echo "== Python tests"
export UV_PROJECT_ENVIRONMENT=$PWD/$out/venv UV_PYTHON=$python_version
uv sync --dev --group coverage --no-install-project || die "cannot create the virtualenv"
# The wheel is only installed here, so it may link the system's OpenSSL.
uv run --no-sync maturin build --profile dev --auditwheel skip --out "$out/wheel" ||
    die "cannot build the extension"
wheels=("$out"/wheel/*.whl)
[[ ${#wheels[@]} -eq 1 && -f ${wheels[0]} ]] || die "expected one wheel in $out/wheel"
uv pip install --python "$out/venv" --no-deps --reinstall "${wheels[0]}" || die "cannot install the wheel"
# Drop the native suite's profiles and those the build scripts just wrote.
cargo llvm-cov clean --profraw-only || die "cargo llvm-cov clean failed"
# That keeps the profiles merged for the native report. If the Python suite
# then wrote no profile, the next report would reuse them without an error.
rm -f "${CARGO_TARGET_DIR:?}"/*.profdata || die "cannot remove the merged profiles"
export COVERAGE_FILE=$PWD/$out/python/.coverage
uv run --no-sync coverage run -m pytest -v --junitxml="$out/pytest.xml" || failed "Python tests failed"
if uv run --no-sync coverage combine; then
    uv run --no-sync coverage xml -o "$reports/python.xml" || failed "cannot write the Python report"
else
    failed "cannot combine the Python coverage data"
fi
rust_report rust-python

echo "== Checks"
uv run --no-sync python scripts/coverage_check.py --summary "$out/summary.md" \
    "cargo-test-log:$out/rust-tests.log" \
    "pytest-junit:$out/pytest.xml" \
    "cobertura:$reports/python.xml" \
    "lcov:$reports/rust-native.lcov" \
    "lcov:$reports/rust-python.lcov" ||
    failed "coverage checks failed"

exit "$status"
