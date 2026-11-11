"""Tests for scripts/coverage.sh and scripts/coverage_check.py.

Run with `python3 scripts/coverage_tests.py`. The name keeps pytest from
collecting these with the driver's tests. They need Python 3.11 or newer (only
its standard library), git, Bash and the basic utilities listed in `UTILITIES`,
but no cluster and no Rust toolchain.

The script's tests run it against stub `cargo`, `uv` and `make` commands on a
PATH that holds nothing else but a few basic utilities, and check which
commands it ran, in which order, and how it exited.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import coverage_check

STUB = r"""#!/bin/bash
name=$(basename "$0")
call="$name $*"
echo "$call" >> "$STUB_LOG"
if [[ -n ${STUB_FAIL:-} && $call == *"$STUB_FAIL"* ]]; then
    exit 1
fi
args=("$@")
for ((i = 0; i < ${#args[@]} - 1; i++)); do
    case ${args[i]} in
        --output-path | -o) touch "${args[i + 1]}" ;;
        --out) mkdir -p "${args[i + 1]}" && touch "${args[i + 1]}/scylla.whl" ;;
    esac
done
if [[ $call == "make test-rust" ]]; then
    echo "test result: ok. 1 passed; 0 failed"
fi
if [[ $call == "cargo llvm-cov show-env"* ]]; then
    echo "${CARGO_LLVM_COV_TARGET_DIR-unset}" >> "$STUB_LOG.env"
fi
# The real report reuses a merged .profdata it finds: record whether one is there.
if [[ $call == "cargo llvm-cov report"* ]]; then
    merged=("${CARGO_TARGET_DIR:-/nonexistent}"/*.profdata)
    [[ -e ${merged[0]} ]] && state=present || state=absent
    echo "${args[-1]##*/} $state" >> "$STUB_LOG.profdata"
fi
exit 0
"""

UTILITIES = ["bash", "basename", "dirname", "mkdir", "rm", "tee", "touch"]

SUCCESS_CALLS = [
    "cargo llvm-cov --version",
    "cargo llvm-cov show-env --sh --remap-path-prefix",
    "cargo llvm-cov clean --workspace",
    "make test-rust",
    "cargo llvm-cov report --remap-path-prefix --lcov --output-path target/coverage/reports/rust-native.lcov",
    "uv sync --dev --group coverage --no-install-project",
    "uv run --no-sync maturin build --profile dev --auditwheel skip --out target/coverage/wheel",
    "uv pip install --python target/coverage/venv --no-deps --reinstall target/coverage/wheel/scylla.whl",
    "cargo llvm-cov clean --profraw-only",
    "uv run --no-sync coverage run -m pytest -v --junitxml=target/coverage/pytest.xml",
    "uv run --no-sync coverage combine",
    "uv run --no-sync coverage xml -o target/coverage/reports/python.xml",
    "cargo llvm-cov report --remap-path-prefix --lcov --output-path target/coverage/reports/rust-python.lcov",
    (
        "uv run --no-sync python scripts/coverage_check.py --summary target/coverage/summary.md"
        " cargo-test-log:target/coverage/rust-tests.log pytest-junit:target/coverage/pytest.xml"
        " cobertura:target/coverage/reports/python.xml lcov:target/coverage/reports/rust-native.lcov"
        " lcov:target/coverage/reports/rust-python.lcov"
    ),
]


class CoverageScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.repo = self.tmp / "repo"
        (self.repo / "scripts").mkdir(parents=True)
        shutil.copy(HERE / "coverage.sh", self.repo / "scripts" / "coverage.sh")
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for utility in UTILITIES:
            found = shutil.which(utility)
            assert found, utility
            (self.bin / utility).symlink_to(found)
        stub = self.tmp / "stub"
        stub.write_text(STUB)
        stub.chmod(0o755)
        for tool in ("cargo", "uv", "make"):
            (self.bin / tool).symlink_to(stub)
        self.log = self.tmp / "calls.log"

    def run_script(self, fail: str = "", **extra_env: str) -> subprocess.CompletedProcess[str]:
        env = {"PATH": str(self.bin), "STUB_LOG": str(self.log), "STUB_FAIL": fail, "HOME": str(self.tmp)}
        env.update(extra_env)
        return subprocess.run(
            ["/usr/bin/env", "bash", "scripts/coverage.sh"],
            cwd=self.repo,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    def calls(self) -> list[str]:
        if not self.log.exists():
            return []
        return [line.replace(str(self.repo) + "/", "") for line in self.log.read_text().splitlines()]

    def test_success_runs_everything_in_order(self) -> None:
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), SUCCESS_CALLS)

    def test_rust_suite_failure_still_runs_python_suite_and_checks(self) -> None:
        result = self.run_script(fail="make test-rust")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), SUCCESS_CALLS)
        self.assertIn("native Rust tests failed", result.stderr)

    def test_python_suite_failure_still_writes_reports(self) -> None:
        result = self.run_script(fail="coverage run")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), SUCCESS_CALLS)
        self.assertIn("Python tests failed", result.stderr)

    def test_combine_failure_skips_only_the_python_report(self) -> None:
        result = self.run_script(fail="coverage combine")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), [c for c in SUCCESS_CALLS if "coverage xml" not in c])

    def test_report_failure_fails(self) -> None:
        result = self.run_script(fail="rust-native.lcov")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), SUCCESS_CALLS)
        self.assertIn("cannot write the rust-native report", result.stderr)

    def test_check_failure_fails(self) -> None:
        result = self.run_script(fail="coverage_check.py")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), SUCCESS_CALLS)
        self.assertIn("coverage checks failed", result.stderr)

    def test_build_failure_stops(self) -> None:
        result = self.run_script(fail="maturin build")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot build the extension", result.stderr)
        self.assertEqual(self.calls(), SUCCESS_CALLS[:7])

    def test_native_merged_profiles_are_removed_before_the_python_suite(self) -> None:
        # cargo llvm-cov report reuses a stale .profdata when no profile was written.
        target = self.repo / "target" / "llvm-cov-target"
        target.mkdir(parents=True)
        stale = target / "python-rs-driver.profdata"
        stale.write_text("")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        # Gone when the Python suite's report is made, not merely by the end.
        states = Path(f"{self.log}.profdata").read_text().splitlines()
        self.assertEqual(states, ["rust-native.lcov present", "rust-python.lcov absent"])

    def test_inherited_profile_directory_is_ignored(self) -> None:
        # cargo-llvm-cov would write the profiles there, but the reports look in the target directory.
        result = self.run_script(CARGO_LLVM_COV_TARGET_DIR=str(self.tmp / "elsewhere"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(f"{self.log}.env").read_text().splitlines(), ["unset"])

    def test_install_failure_stops(self) -> None:
        result = self.run_script(fail="uv pip install")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot install the wheel", result.stderr)
        self.assertEqual(self.calls(), SUCCESS_CALLS[:8])

    def test_missing_tool_is_named(self) -> None:
        (self.bin / "uv").unlink()
        result = self.run_script()
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing uv; see docs/source/topics/testing.md", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_missing_cargo_llvm_cov_is_named(self) -> None:
        result = self.run_script(fail="cargo llvm-cov --version")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing cargo-llvm-cov", result.stderr)
        self.assertEqual(self.calls(), ["cargo llvm-cov --version"])


PYPROJECT = """
[tool.maturin]
python-source = "python"

[tool.coverage.run]
source_pkgs = ["scylla"]
"""

CARGO = """
[package]
name = "scylla"
"""

TRACKED = ["python/scylla/__init__.py", "python/scylla/a.py", "python/scylla/é&b.py", "src/lib.rs", "src/x.rs"]


def cobertura(*classes: tuple[str, list[int]], source: str = ".") -> str:
    body = "".join(
        f'<class filename="{name}"><lines>'
        + "".join(f'<line number="{n}" hits="{hits}"/>' for n, hits in enumerate(lines, 1))
        + "</lines></class>"
        for name, lines in classes
    )
    return (
        f'<?xml version="1.0" ?><coverage><sources><source>{source}</source></sources>'
        f"<packages><package><classes>{body}</classes></package></packages></coverage>"
    )


def lcov(*records: tuple[str, list[int]]) -> str:
    return "".join(
        f"SF:{name}\n" + "".join(f"DA:{n},{hits}\n" for n, hits in enumerate(lines, 1)) + "end_of_record\n"
        for name, lines in records
    )


GOOD_PYTHON = cobertura(
    ("python/scylla/__init__.py", []),
    ("python/scylla/a.py", [1, 0]),
    ("python/scylla/é&amp;b.py", [0]),
)


class CoverageCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        (self.root / "pyproject.toml").write_text(PYPROJECT)
        (self.root / "Cargo.toml").write_text(CARGO)
        for name in TRACKED:
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            (self.root / name).write_text("")
        self.git("init", "-q")
        self.git("add", ".")

    def git(self, *args: str) -> None:
        subprocess.run(["git", *args], cwd=self.root, check=True)

    def check(self, kind: str, content: str) -> tuple[list[str], list[str]]:
        report = self.root / f"report.{kind}"
        report.write_text(content)
        return coverage_check.run_checks([f"{kind}:{report}"], self.root)

    def assertPasses(self, kind: str, content: str) -> None:
        _, failures = self.check(kind, content)
        self.assertEqual(failures, [])

    def assertFails(self, kind: str, content: str, message: str) -> None:
        _, failures = self.check(kind, content)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn(message, failures[0])

    def test_good_reports_pass(self) -> None:
        self.assertPasses("cobertura", GOOD_PYTHON)
        self.assertPasses("lcov", lcov(("src/lib.rs", [1, 0]), ("src/x.rs", [0])))

    def test_absolute_lcov_paths_inside_the_repository_pass(self) -> None:
        self.assertPasses("lcov", lcov((f"{self.root}/src/lib.rs", [1])))

    def test_cobertura_sources_naming_the_repository_pass(self) -> None:
        for source in ("", ".", str(self.root)):
            self.assertPasses("cobertura", GOOD_PYTHON.replace("<source>.</source>", f"<source>{source}</source>"))

    def test_missing_report_fails(self) -> None:
        _, failures = coverage_check.run_checks([f"lcov:{self.root}/nothing.lcov"], self.root)
        self.assertEqual(len(failures), 1)
        self.assertIn("no report", failures[0])

    def test_empty_report_fails(self) -> None:
        self.assertFails("lcov", "", "measures no lines")
        self.assertFails("cobertura", cobertura(), "measures no lines")

    def test_report_without_covered_lines_fails(self) -> None:
        self.assertFails("lcov", lcov(("src/lib.rs", [0, 0])), "no covered lines in src/")

    def test_unit_present_but_not_covered_fails(self) -> None:
        # A name is not a measurement: the unit is listed, yet nothing in it ran.
        report = lcov(("src/lib.rs", [0]), ("python/scylla/a.py", [1]))
        self.assertFails("lcov", report, "no covered lines in src/")

    def test_file_outside_the_units_fails(self) -> None:
        self.assertFails("lcov", lcov(("src/lib.rs", [1]), ("python/scylla/a.py", [1])), "outside src")

    def test_untracked_path_fails(self) -> None:
        self.assertFails("lcov", lcov(("src/lib.rs", [1]), ("src/gone.rs", [1])), "cannot match")

    def test_path_outside_the_repository_fails(self) -> None:
        self.assertFails("lcov", lcov(("/elsewhere/src/lib.rs", [1])), "outside the repository")

    def test_unknown_cobertura_source_fails(self) -> None:
        self.assertFails("cobertura", GOOD_PYTHON.replace("<source>.</source>", "<source>/x</source>"), "<source>")
        two = GOOD_PYTHON.replace("<source>.</source>", "<source>.</source><source>/x</source>")
        self.assertFails("cobertura", two, "<source>")

    def test_python_module_missing_from_report_fails(self) -> None:
        report = cobertura(("python/scylla/__init__.py", []), ("python/scylla/a.py", [1]))
        self.assertFails("cobertura", report, "missing from the report: python/scylla/é&b.py")

    def test_unescaped_name_is_matched_against_raw_git_name(self) -> None:
        # Git would quote "é" without -z; the XML would be read raw without a parser.
        self.assertPasses("cobertura", GOOD_PYTHON)
        self.assertFails("cobertura", GOOD_PYTHON.replace("&amp;", "&amp;amp;"), "cannot match")

    def test_failing_git_fails_instead_of_skipping(self) -> None:
        shutil.rmtree(self.root / ".git")
        self.assertFails("lcov", lcov(("src/lib.rs", [1])), "git ls-files failed")

    def test_cargo_log_needs_a_passed_test(self) -> None:
        log = self.root / "rust.log"
        log.write_text("test result: ok. 0 passed; 0 failed\ntest result: ok. 0 passed; 0 failed\n")
        _, failures = coverage_check.run_checks([f"cargo-test-log:{log}"], self.root)
        self.assertIn("no Rust test passed", failures[0])
        log.write_text("test result: ok. 0 passed; 0 failed\ntest result: FAILED. 2 passed; 1 failed\n")
        rows, failures = coverage_check.run_checks([f"cargo-test-log:{log}"], self.root)
        self.assertEqual(failures, [])
        self.assertIn("2 tests passed", rows[0])

    def test_junit_needs_a_test_that_ran(self) -> None:
        junit = self.root / "junit.xml"
        junit.write_text('<testsuites><testsuite tests="3" skipped="3"/></testsuites>')
        _, failures = coverage_check.run_checks([f"pytest-junit:{junit}"], self.root)
        self.assertIn("pytest ran no tests", failures[0])
        junit.write_text('<testsuites><testsuite tests="3" skipped="2"/></testsuites>')
        _, failures = coverage_check.run_checks([f"pytest-junit:{junit}"], self.root)
        self.assertEqual(failures, [])

    def test_every_check_runs_after_a_failure(self) -> None:
        good = self.root / "good.lcov"
        good.write_text(lcov(("src/lib.rs", [1])))
        rows, failures = coverage_check.run_checks([f"lcov:{self.root}/missing.lcov", f"lcov:{good}"], self.root)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(failures), 1)

    def test_main_exit_status_and_summary(self) -> None:
        good = self.root / "good.lcov"
        good.write_text(lcov(("src/lib.rs", [1, 0])))
        summary = self.root / "summary.md"
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            self.assertEqual(coverage_check.main(["--summary", str(summary), f"lcov:{good}"]), 0)
            self.assertEqual(coverage_check.main([f"lcov:{self.root}/missing.lcov"]), 1)
        finally:
            os.chdir(cwd)
        self.assertIn("1 of 2 lines (50.0%) in 1 files", summary.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
