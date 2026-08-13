# AGENTS.md

Guidance for AI coding assistants working in this repository.

## Project Overview

This project contains the **libpgduckdb kernel** plus the PostgreSQL extensions built on it:

- `libpgduckdb/` -- the kernel (`namespace pgddb`): reusable "DuckDB inside
  a PG extension" infrastructure (DuckDB instance lifecycle, planner-offload
  primitives, PG heap scan for DuckDB, DuckdbCustomScan execution, PG-side
  wrappers). Not itself a PG extension.
- `pg_ducklake/` -- the primary extension: DuckLake lakehouse tables in
  PostgreSQL (`namespace pgducklake`). Headers in `include/`, implementation
  in `src/`, tests in `test/`.
- `pg_duckdb/` -- the original "DuckDB inside PostgreSQL" extension. We keep
  keep this extension here to ensure kernel don't break pg_duckdb, and also
  make sure different extensions with libpgduckdb can coexist.
- `examples/pg_vortex/` -- minimal example extension demonstrating kernel reuse.
- `duckdb/` -- DuckDB submodule shared by all extensions.

## Build and test

See the `setup-dev` skill for full dev environment setup and
`commit-and-pr-guidelines` for commits and pull request descriptions.

Supported PostgreSQL versions: 14-18. `PG_CONFIG` is required; usually a local
PostgreSQL is installed under the workdir (e.g.
`PG_CONFIG=$(pwd)/pg-18/bin/pg_config`) to avoid conflicts with other
worktrees. If neither a local nor a global pg is found, stop and ask the user.

The root `Makefile` defaults to pg_ducklake; other components are reached with
`make <dir>/<target>` delegation or `make -C`.

```bash
# macOS: prefix builds with LIBRARY_PATH="$(brew --prefix)/lib"
NCPU=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

PG_CONFIG=<pg_config> make -j"$NCPU"        # build pg_ducklake (default target)
PG_CONFIG=<pg_config> make install          # install pg_ducklake
PG_CONFIG=<pg_config> make installcheck     # pg_ducklake regression + isolation
PG_CONFIG=<pg_config> make check-regression TEST=basic
PG_CONFIG=<pg_config> make check-isolation TEST=concurrent_writes
PG_CONFIG=<pg_config> make check-e2e        # needs uv; optional dockerized MinIO
PG_CONFIG=<pg_config> make format           # clang-format pg_ducklake src/ include/
PG_CONFIG=<pg_config> make check-format

# Other extensions, via delegation:
PG_CONFIG=<pg_config> make pg_duckdb/all -j"$NCPU"
PG_CONFIG=<pg_config> make pg_duckdb/installcheck
PG_CONFIG=<pg_config> make examples/pg_vortex/installcheck
```

pg_ducklake tests live in `pg_ducklake/test/regression/` (SQL regression),
`test/isolation/` (concurrency specs), and `test/e2e/` (external-client
integration). Prefer regression and isolation tests to verify functionality.

## Coding rules

- Write clean, minimal code; prioritize simplicity and maintainability.
- Add comments only for non-obvious logic or essential context.
- Use ASCII only in source files, SQL tests, and expected output. Use `-`, `--`,
  `'`, and `"` instead of Unicode dashes or smart quotes.

## Comment Conventions

- Try to keep comments short. In general, comments should be one short line. Only in exceptional situations should
  comments be more than one short line. Code should be mostly self-descriptive and too many large comments make code
  harder to read and understand.
- Avoid adding comments specific to how a change was made to the code that relates to a specific issue. For example, a
  comment like "add +1 to fix an off-by-one error" is not relevant to understanding the code. Such comments related to
  specific issues that were addressed belong in a PR description or commit message, not in the code itself.

### C/C++

- Avoid `extern "C"` for symbols from the same library; put the linkage in the
  header instead. Use it only when necessary for third-party interfaces.
- Use `namespace pgducklake` for C++ extension code, never
  `namespace pg_ducklake`.
- Use `pgducklake::` when qualifying symbols outside the namespace block.
- Use C++ raw string literals (`R"(...)"`) for multiline SQL; never use
  adjacent-string concatenation for SQL queries.

### C/C++ header & include rules

PG and DuckDB headers are include-order-sensitive: PG's `elog.h`
`#define FATAL` clobbers DuckDB's `ExceptionType::FATAL`, so DuckDB headers
must precede `postgres.h`.

### Headers (`.h`/`.hpp`)

- `#pragma once` on the first line; a brief header comment only if needed.
- No `postgres.h` in headers -- get PG structs from
  `libpgduckdb/include/pgddb/pg/declarations.hpp` (add forward decls there).
  Exception: a PG type that can't be forward-declared (e.g. plain enums).
- Don't declare what `PG_FUNCTION_INFO_V1` already declares.

### Source `.cpp` include groups

Ordered groups, blank line between, alphabetical within each:

1. own/sibling module headers (`"pgducklake/..."`, or `"pgddb/..."` in the kernel)
2. C/C++ std `<...>`
3. postgres-free `"pgddb/..."` (extensions; these pull DuckDB)
4. DuckDB/DuckLake `<...>`
5. `extern "C"` { `postgres.h` first, then PG headers, then postgres-dependent
   pgddb C headers like `pgddb_ruleutils.h` }

Postgres last keeps the FATAL ordering automatic. Postgres-dependent pgddb
*C++* headers (`utility/cpp_wrapper.hpp`, `pgddb_node.hpp`) go AFTER group 5
(after the `extern "C"` block), without an explanatory comment.

`.clang-format` uses `SortIncludes: false` / `IncludeBlocks: Preserve` -- order
is manual.

### Documentation

Keep documentation near the code to reduce docs rot. Do not create separate
explanation documents or duplicate information already expressed by code. Keep
header comments current after edits; use inline comments only for non-obvious
logic.

Human documentation is organized as:

```text
README.md
  +-- pg_ducklake
      +-- sql/pg_ducklake--*.sql       all SQL objects, documented inline
      +-- docs/README.md               index of all human docs
            +-- docs/settings.md       GUCs and DuckLake options
            +-- docs/access_control.md
            +-- docs/compilation.md
```

Every new documentation file must be linked from `docs/README.md`. Keep the
following synchronized with code:

- Document SQL objects inline in `pg_ducklake--*.sql`; there is no separate
  reference document. When adding, removing, or changing a `ducklake.*` SQL
  function or procedure, update its inline comment.
- In reference documentation, order table-of-contents tables alphabetically and
  keep detailed descriptions in logical order.

## Gotchas

- **ducklake dependency / patch structure**: `pg_ducklake/third_party/ducklake`
  is a git **submodule** pinned to a community `duckdb/ducklake` commit. Our
  divergence is **not** committed into its tree -- it lives as an ordered
  series of patch files `third_party/ducklake-NNN-<desc>.patch` that the
  Makefile applies onto the pristine checkout at build time (apply-once,
  stamp-guarded via `DUCKLAKE_STAMP`; the submodule working tree is
  intentionally left dirty/patched, the gitlink stays pinned). To see what we
  changed vs. community, read those patch files. Change behavior by
  adding/editing patch files, never by committing into the submodule tree.
- **duckdb submodule**: pinned upstream source shared by all extensions; bump
  the gitlink only, never commit edits into its tree.
