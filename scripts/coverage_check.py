"""Checks that a coverage run measured everything it counts.

`scripts/coverage.sh` runs this after its suites. A run passes only if every
suite ran at least one test and every report measured the code it should:

    cargo-test-log:PATH  the native Rust suite's output; at least one test passed
    pytest-junit:PATH    pytest's JUnit XML; at least one test ran, not skipped
    cobertura:PATH       coverage.py's XML report of the Python package
    lcov:PATH            cargo-llvm-cov's LCOV report of the Rust extension

Each report must name only files tracked by git, with the paths Codecov will
look up, and must have covered lines in each shipped unit. The Python report
must also list every tracked module of the package, since coverage.py lists
even the modules no test imported. The Rust crate is one compilation unit, so
it is checked as a whole: llvm-cov leaves out files with no code at all, such
as a `mod.rs` that only declares modules, so a per-file check could not tell
those from a lost file.

Only the standard library is used, so this runs on any Python 3.11+.
"""

import argparse
import re
import subprocess
import sys
import tomllib
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath


class CheckError(Exception):
    pass


@dataclass
class FileCoverage:
    lines: int = 0
    covered: int = 0


@dataclass
class Report:
    files: dict[str, FileCoverage] = field(default_factory=dict[str, FileCoverage])

    def add(self, path: str, hits: int) -> None:
        entry = self.files.setdefault(path, FileCoverage())
        entry.lines += 1
        if hits > 0:
            entry.covered += 1

    @property
    def lines(self) -> int:
        return sum(f.lines for f in self.files.values())

    @property
    def covered(self) -> int:
        return sum(f.covered for f in self.files.values())


def tracked_files(root: Path) -> set[str]:
    """The files git tracks, read from one call whose status we check."""
    result = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=False)
    if result.returncode != 0:
        raise CheckError(f"git ls-files failed: {result.stderr.decode(errors='replace').strip()}")
    files = {name.decode() for name in result.stdout.split(b"\0") if name}
    if not files:
        raise CheckError("git ls-files listed no files")
    return files


def python_units(root: Path) -> list[str]:
    """The Python packages the report must cover, from the coverage.py config."""
    config = tomllib.loads((root / "pyproject.toml").read_text())
    source = config["tool"]["maturin"]["python-source"]
    packages = config["tool"]["coverage"]["run"]["source_pkgs"]
    return [str(PurePosixPath(source, *package.split("."))) for package in packages]


def rust_units(root: Path) -> list[str]:
    """The crate source directory the report must cover, from Cargo.toml."""
    config = tomllib.loads((root / "Cargo.toml").read_text())
    lib = config.get("lib", {}).get("path", "src/lib.rs")
    return [str(PurePosixPath(lib).parent)]


def under(path: str, unit: str) -> bool:
    return path.startswith(unit + "/")


def repo_path(root: Path, name: str) -> str:
    """A report path as Codecov will match it: relative to the repository root."""
    path = Path(name)
    if path.is_absolute():
        try:
            path = path.relative_to(root)
        except ValueError:
            raise CheckError(f"{name} is outside the repository") from None
    return path.as_posix()


def parse_cobertura(path: Path, root: Path) -> Report:
    """coverage.py's XML: <class filename=...> with <line hits=...>.

    The XML parser decodes entities, so names compare as raw text.
    """
    tree = ET.parse(path)
    # With relative_files, coverage.py writes one empty <source>.
    sources = [s.text or "" for s in tree.iter("source")]
    if len(sources) != 1 or (root / sources[0]).resolve() != root:
        raise CheckError(f"<source> {sources} is not the repository; set relative_files in [tool.coverage.run]")
    report = Report()
    for cls in tree.iter("class"):
        name = repo_path(root, cls.attrib["filename"])
        report.files.setdefault(name, FileCoverage())
        for line in cls.iter("line"):
            report.add(name, int(line.attrib["hits"]))
    return report


def parse_lcov(path: Path, root: Path) -> Report:
    report = Report()
    current: str | None = None
    for number, raw in enumerate(path.read_text().splitlines(), 1):
        if raw.startswith("SF:"):
            current = repo_path(root, raw[3:])
            report.files.setdefault(current, FileCoverage())
        elif raw.startswith("DA:"):
            if current is None:
                raise CheckError(f"{path}:{number}: DA before SF")
            hits = raw[3:].split(",")[1]
            report.add(current, int(hits))
        elif raw == "end_of_record":
            current = None
    return report


def check_report(kind: str, path: Path, root: Path, tracked: set[str]) -> Report:
    if not path.is_file():
        raise CheckError("no report")
    report = parse_cobertura(path, root) if kind == "cobertura" else parse_lcov(path, root)
    if report.lines == 0:
        raise CheckError("the report measures no lines")

    unknown = sorted(name for name in report.files if name not in tracked)
    if unknown:
        raise CheckError(f"paths Codecov cannot match to the repository: {', '.join(unknown)}")

    units = python_units(root) if kind == "cobertura" else rust_units(root)
    for unit in units:
        if not any(under(name, unit) and f.covered for name, f in report.files.items()):
            raise CheckError(f"no covered lines in {unit}/")
    outside = sorted(name for name in report.files if not any(under(name, unit) for unit in units))
    if outside:
        raise CheckError(f"files outside {', '.join(units)}: {', '.join(outside)}")

    if kind == "cobertura":
        expected = {name for name in tracked if name.endswith(".py") and any(under(name, unit) for unit in units)}
        missing = sorted(expected - report.files.keys())
        if missing:
            raise CheckError(f"modules missing from the report: {', '.join(missing)}")
    return report


def check_cargo_test_log(path: Path) -> int:
    """libtest prints `test result: ok. N passed; ...` once per test binary."""
    passed = sum(int(m) for m in re.findall(r"^test result: \w+\. (\d+) passed;", path.read_text(), re.MULTILINE))
    if passed == 0:
        raise CheckError("no Rust test passed")
    return passed


def check_pytest_junit(path: Path) -> int:
    ran = 0
    for suite in ET.parse(path).iter("testsuite"):
        ran += int(suite.attrib["tests"]) - int(suite.attrib.get("skipped", 0))
    if ran == 0:
        raise CheckError("pytest ran no tests")
    return ran


def run_checks(checks: Iterable[str], root: Path) -> tuple[list[str], list[str]]:
    """Returns (summary rows, failures)."""
    rows: list[str] = []
    failures: list[str] = []
    tracked: set[str] | None = None
    for check in checks:
        kind, _, name = check.partition(":")
        path = Path(name)
        try:
            if kind == "cargo-test-log":
                rows.append(f"| `{name}` | {check_cargo_test_log(path)} tests passed |")
            elif kind == "pytest-junit":
                rows.append(f"| `{name}` | {check_pytest_junit(path)} tests ran |")
            elif kind in ("cobertura", "lcov"):
                if tracked is None:
                    tracked = tracked_files(root)
                report = check_report(kind, path, root, tracked)
                percent = 100 * report.covered / report.lines
                rows.append(
                    f"| `{name}` | {report.covered} of {report.lines} lines ({percent:.1f}%)"
                    f" in {len(report.files)} files |"
                )
            else:
                raise CheckError(f"unknown check kind {kind!r}")
        except (CheckError, OSError, ET.ParseError, KeyError, ValueError, IndexError) as error:
            failures.append(f"{check}: {error}")
            rows.append(f"| `{name}` | **failed**: {error} |")
    return rows, failures


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--summary", type=Path, help="append a Markdown table of the results here")
    parser.add_argument("checks", nargs="+", metavar="KIND:PATH")
    args = parser.parse_args(argv)

    rows, failures = run_checks(args.checks, Path.cwd())
    if args.summary:
        with args.summary.open("a") as summary:
            summary.write("| Input | Result |\n| --- | --- |\n")
            summary.writelines(row + "\n" for row in rows)
    for row in rows:
        print(row)
    for failure in failures:
        print(f"coverage check failed: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
