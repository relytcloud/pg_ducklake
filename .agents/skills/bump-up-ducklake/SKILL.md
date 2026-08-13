---
name: bump-up-ducklake
description: Safely bump pg_ducklake's vendored DuckLake commit, rebase its patch series, account for newly added upstream SQLLogicTests, update mapped test schedules, and validate the result. Use for every pg_ducklake/third_party/ducklake bump.
---

# Bump Up DuckLake

Follow this playbook in order. Stop rather than overwrite unexplained work.

## 1. Read the repository rules

Read `AGENTS.md`, the `setup-dev` and `commit-and-pr-guidelines` skills, and
`pg_ducklake/Makefile`. Obtain an explicit target DuckLake ref and resolve the
old and new revisions to full commit SHAs.

Before changing anything, inspect the superproject and submodule status. The
DuckLake checkout is normally dirty because the Makefile applies
`third_party/ducklake-NNN-*.patch` and creates `.patched`. Do not assume all dirt
is generated: verify it against the current patch series. Never discard, clean,
stash, or overwrite unrelated user changes.

## 2. Rebase the patch series

Use a disposable DuckLake worktree, not the build checkout.

1. At the old revision, apply every patch in Makefile lexical order and commit
   each patch separately.
2. Rebase that commit stack onto the target revision. Resolve conflicts in the
   patch that owns the change.
3. If upstream absorbed a patch, prove the rebased commit is empty before
   removing it. Do not silently drop or squash patches.
4. Export the rebased commits back to the ordered patch files.
5. In a fresh worktree at the target revision, run `git apply --check` and apply
   the complete series in Makefile order. Compare its aggregate diff with the
   rebased stack.
6. Update the superproject gitlink only after the clean-room application passes.

Local pg_ducklake changes belong in patch files, never in a commit inside the
DuckLake submodule. Do not commit `.patched` or patched submodule worktree files.
Use the next unused three-digit prefix for a new patch; preserve existing patch
names and ordering unless a deliberate renumber is reviewed.

## 3. Account for upstream tests

The authoritative inventory is every tracked target-revision file matching
`test/sql/**/*.test` or `test/sql/**/*.test_slow`. Run:

```bash
python3 .agents/skills/bump-up-ducklake/scripts/check-test-inventory.py
```

The checker requires every upstream test to be exactly one of:

- mapped by an exact first-line `Upstream: test/sql/...` marker under
  `pg_ducklake/test/regression/sql/ducklake` or
  `pg_ducklake/test/isolation/specs/ducklake`; or
- listed in this skill's `unmapped-tests.tsv` with a concrete reason.

It also checks source/expected pairs and default schedule coverage. Review
`git diff --name-status -M OLD NEW -- test/sql` separately so modified, renamed,
and deleted upstream tests are not overlooked.

### Newly added tests

Map every newly added upstream test unless a genuine PostgreSQL-specific blocker
makes that impossible. Port behavior and invariants, not DuckDB syntax:

- mirror `test/sql/<path>.test` as
  `test/regression/sql/ducklake/<path>.sql`, or use an isolation spec for real
  concurrency;
- preserve the exact upstream path in the first line;
- create and review the matching expected output;
- use unique objects, deterministic ordering, dynamic IDs, and complete cleanup;
- do not bless accidental errors or weaken physical/conflict invariants;
- add the mapped name to the corresponding default schedule.

If a test cannot be mapped, add one sorted row to `unmapped-tests.tsv`. The
reason must identify the exact unavailable API, semantic mismatch, external
requirement, or redundant local coverage. Do not use only `unsupported`,
`not applicable`, `slow`, or `TODO`. `pending_port` exists only for the legacy
backlog recorded when this inventory was introduced; do not assign it to tests
added by a later bump.

TSV columns are:

- `upstream_path`: exact unique path at the target revision;
- `reason_code`: `pending_port`, `unsupported`, `inapplicable`,
  `external_dependency`, `resource_intensive`, `platform_specific`, `redundant`,
  or `blocked`;
- `reason`: one concrete ASCII line without tabs.

Remove a TSV row when its test is mapped or deleted upstream. Update paths for
upstream renames. The inventory must remain sorted by `upstream_path`.

## 4. Validate

After removing the stale `.patched` stamp, build from clean inputs. Use an
available supported `PG_CONFIG`; stop and ask if none exists. On macOS, apply
the `LIBRARY_PATH` prefix from `AGENTS.md`.

Run, at minimum:

```bash
python3 .agents/skills/bump-up-ducklake/scripts/check-test-inventory.py
PG_CONFIG=<pg_config> make check-format
PG_CONFIG=<pg_config> make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
PG_CONFIG=<pg_config> make install
PG_CONFIG=<pg_config> make check-regression
PG_CONFIG=<pg_config> make check-isolation
git diff --check
```

Run focused tests while developing, but full scheduled regression and isolation
runs are mandatory. Validate both the oldest and newest available supported
PostgreSQL versions; state clearly which matrix entries were not run.

Finally inspect `git diff --submodule=log` and status. Stage explicit paths only.
The gitlink, rebased patches, test mappings, expected outputs, schedules, and TSV
updates belong in one atomic commit. Do not commit or push unless requested.
