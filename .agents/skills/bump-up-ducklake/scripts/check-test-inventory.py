#!/usr/bin/env python3

import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DUCKLAKE = ROOT / "pg_ducklake/third_party/ducklake"
TSV = ROOT / ".agents/skills/bump-up-ducklake/unmapped-tests.tsv"
REG_SQL = ROOT / "pg_ducklake/test/regression/sql"
REG_EXPECTED = ROOT / "pg_ducklake/test/regression/expected"
ISO_SPECS = ROOT / "pg_ducklake/test/isolation/specs"
ISO_EXPECTED = ROOT / "pg_ducklake/test/isolation/expected"
REG_SCHEDULE = ROOT / "pg_ducklake/test/regression/schedule"
ISO_SCHEDULE = ROOT / "pg_ducklake/test/isolation/schedule"
REASON_CODES = {
    "pending_port",
    "unsupported",
    "inapplicable",
    "external_dependency",
    "resource_intensive",
    "platform_specific",
    "redundant",
    "blocked",
}


def fail(errors, message):
    errors.append(message)


def upstream_tests():
    output = subprocess.check_output(
        ["git", "-C", str(DUCKLAKE), "ls-files", "test/sql"], text=True
    )
    return {
        path
        for path in output.splitlines()
        if path.endswith(".test") or path.endswith(".test_slow")
    }


def schedule_entries(path):
    entries = []
    for line in path.read_text().splitlines():
        if line.startswith("test:"):
            entries.extend(line.removeprefix("test:").split())
    return entries


def mapped_tests(errors):
    mapped = {}
    sources = []
    for kind, root, expected_root, suffix in (
        ("regression", REG_SQL / "ducklake", REG_EXPECTED, ".sql"),
        ("isolation", ISO_SPECS / "ducklake", ISO_EXPECTED, ".spec"),
    ):
        for source in sorted(root.rglob(f"*{suffix}")):
            lines = source.read_text(errors="replace").splitlines()
            first = lines[0] if lines else ""
            marker = "Upstream: "
            if marker not in first:
                fail(errors, f"missing first-line Upstream marker: {source.relative_to(ROOT)}")
                continue
            upstream = first.split(marker, 1)[1].strip()
            if not upstream.startswith("test/sql/"):
                fail(errors, f"invalid Upstream marker in {source.relative_to(ROOT)}: {upstream}")
                continue
            if upstream in mapped:
                fail(errors, f"duplicate mapping for {upstream}: {mapped[upstream]} and {source.relative_to(ROOT)}")
            mapped[upstream] = str(source.relative_to(ROOT))
            name = str(source.relative_to(REG_SQL if kind == "regression" else ISO_SPECS))[: -len(suffix)]
            expected = expected_root / f"{name}.out"
            if not expected.is_file():
                fail(errors, f"missing expected output for {source.relative_to(ROOT)}")
            sources.append((kind, name, source))
    return mapped, sources


def unmapped_tests(errors):
    rows = []
    with TSV.open(newline="") as file:
        reader = csv.DictReader(file, delimiter="\t")
        expected = ["upstream_path", "reason_code", "reason"]
        if reader.fieldnames != expected:
            fail(errors, f"invalid TSV header: expected {expected}, got {reader.fieldnames}")
            return {}
        for row in reader:
            path = row["upstream_path"]
            code = row["reason_code"]
            reason = row["reason"]
            if code not in REASON_CODES:
                fail(errors, f"invalid reason_code for {path}: {code}")
            if not reason.strip() or reason.strip().lower() in {"unsupported", "not applicable", "slow", "todo"}:
                fail(errors, f"non-concrete reason for {path}: {reason!r}")
            rows.append(row)
    paths = [row["upstream_path"] for row in rows]
    if paths != sorted(paths):
        fail(errors, "unmapped-tests.tsv is not sorted by upstream_path")
    if len(paths) != len(set(paths)):
        fail(errors, "unmapped-tests.tsv contains duplicate upstream paths")
    return {row["upstream_path"]: row for row in rows}


def main():
    errors = []
    upstream = upstream_tests()
    mapped, sources = mapped_tests(errors)
    unmapped = unmapped_tests(errors)

    overlap = set(mapped) & set(unmapped)
    missing = upstream - set(mapped) - set(unmapped)
    stale_mapped = set(mapped) - upstream
    stale_unmapped = set(unmapped) - upstream
    for path in sorted(overlap):
        fail(errors, f"both mapped and unmapped: {path}")
    for path in sorted(missing):
        fail(errors, f"unaccounted upstream test: {path}")
    for path in sorted(stale_mapped):
        fail(errors, f"mapping points to absent upstream test: {path}")
    for path in sorted(stale_unmapped):
        fail(errors, f"unmapped row points to absent upstream test: {path}")

    schedules = {
        "regression": schedule_entries(REG_SCHEDULE),
        "isolation": schedule_entries(ISO_SCHEDULE),
    }
    for kind, name, source in sources:
        count = schedules[kind].count(name)
        if count != 1:
            fail(errors, f"{source.relative_to(ROOT)} has {count} schedule entries, expected 1")

    print(f"upstream={len(upstream)} mapped={len(mapped)} unmapped={len(unmapped)}")
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("DuckLake test inventory is complete and scheduled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
