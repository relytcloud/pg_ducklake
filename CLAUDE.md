This file provides guidance to AI coding assistants when working with code in this repository.

## Project Architecture

`pg_ducklake` is a PostgreSQL extension that extends `pg_duckdb` to support DuckLake, an open lakehouse format. This extension static-links `ducklake`, the official DuckDB extension, and loads it
via DuckDB's `LoadStaticExtension<T>()`.
Its C++ code should use `namespace pgducklake`.

- header files in `include/`
- implement files in `src/`
  - `src/pgducklake.cpp`: `_PG_init`, extension bootstrap
  - `src/pgducklake_duckdb.cpp`: DuckDB bridge, deferred static extension load
  - `src/pgducklake_metadata_manager.cpp`: custom DuckLake metadata manager
  - `src/pgducklake_ddl.cpp`: create/drop event triggers, DuckDDL synchronization
  - `src/pgducklake_table_am.cpp`: table access method registration
- regression tests in `test/regression/`
- isolation tests in `test/isolation/`

## Build and Test Commands

See `workflow-dev-env` skill for full dev environment setup (ccache, PostgreSQL from source, submodules, worktrees).

`PG_CONFIG` is required. Usually a local pg is installed under workdir, e.g. `PG_CONFIG=$(pwd)/pg-17/bin/pg_config`, to avoid conflicts with other worktrees. If neither local pg nor global pg is found, stop and ask user.

```bash
git submodule update --init --recursive
PG_CONFIG=<pg_config> make install
PG_CONFIG=<pg_config> make installcheck

# Run single test
PG_CONFIG=<pg_config> make installcheck TEST=basic
```

Use regression and isolation tests to verify functionality as possible.

## Code Style

### General

- Write clean, minimal code; fewer lines is better
- Prioritize simplicity for effective and maintainable software
- Only include comments that are essential to understanding functionality or convey non-obvious information
- **ASCII only** in all source files, SQL tests, and expected output — no emojis, no Unicode dashes/quotes (use `-`, `--`, `'`, `"`)

### C/C++

- Avoid using `extern "C"` to reference symbols from the same library. Instead, place it at the header file.
- Use `extern "C"` only when necessary, such as when interfacing with third-party libraries.
- Use `namespace pgducklake` for C++ extension code (do not use `namespace pg_ducklake`).
- Use `pgducklake::` when qualifying symbols outside the namespace block.
- Use C++ raw string literals (`R"(…)"`) for multiline SQL; never use adjacent-string concatenation for SQL queries.

### Imports

PostgreSQL and DuckDB headers are conflict-prone. Follow strict include order in mixed files:

1. DuckDB headers
2. DuckLake headers
3. Local `pgducklake` headers
4. PostgreSQL headers last, inside `extern "C"`, must include `<postgres.h>` at first.

**FATAL macro conflict:** PostgreSQL's `elog.h` defines `#define FATAL 22`, which clobbers DuckDB's `ExceptionType::FATAL` enum member in `duckdb/common/exception.hpp`. Any header that transitively includes both will break. The fix is include order: DuckDB's `exception.hpp` (or any header that pulls it in, e.g., `string_util.hpp`, `error_data.hpp`) must be parsed *before* `postgres.h` defines the macro. Once parsed, C++ include guards prevent re-inclusion. Watch for indirect includes -- `pgduckdb/pgduckdb_contracts.hpp` and `pgducklake/utility/cpp_wrapper.hpp` both include `postgres.h`, so any DuckDB header they transitively need must already be included earlier in the translation unit.

### Third-party Submodules

Treat `third_party/pg_duckdb` as upstream:

- Prefer additive hooks, avoid invasive edits.
- Keep diffs minimal and upstream-friendly.
- Ensure zero behavior change when hooks are unused.
- **Never change the linkage or signature of upstream functions** (e.g., do not remove `extern "C"` from functions that already exist in `duckdb/main`). Only our own additions may use C++ linkage.
- **Call pg_duckdb hooks via `pgduckdb::` from `pgduckdb/pgduckdb_contracts.hpp`** (our contract header). Upstream C-linkage functions (e.g., `RegisterDuckdbTableAm`) keep `extern "C"` and are called unqualified.
- **Our exported C++ symbols in pg_duckdb must be in `namespace pgduckdb`** to avoid name conflicts. Declare them in `include/pgduckdb/pgduckdb_contracts.hpp` under `namespace pgduckdb`.

## Docs Style

Documentation follows two axes: **AI-oriented** and **human-oriented**.

All docs must be reachable from one of two entrypoints:

### AI docs tree (`CLAUDE.md` entrypoint)

```
CLAUDE.md
  +-- .claude/skills/*          AI workflow guidance
  +-- src/*.cpp header comments  per-file purpose and usage
  +-- test/regression/           self-documenting test cases
  +-- test/isolation/            concurrency test cases
```

To avoid _Docs Rot_, keep AI docs near the code. Do NOT write separate explanation docs or duplicate what code already says. Maintain header comments after each edit. Inline comments only when logic is non-obvious.

### Human docs tree (`README.md` entrypoint)

```
README.md
  +-- docs/README.md              index of all human docs
        +-- docs/sql_objects.md   all SQL objects, functions, and procedures
        +-- docs/settings.md      GUCs and DuckLake options
        +-- docs/access_control.md
        +-- docs/compilation.md
```

Every new doc file must be linked from `docs/README.md`. Keep synced with code:

- When adding, removing, or changing a `ducklake.*` SQL function or procedure in `pg_ducklake--0.1.0.sql`, update `docs/sql_objects.md`.
- In reference docs, order TOC tables alphabetically; keep detailed descriptions in logical order.

## Miscellaneous

- When modifying multiple files, run file modification tasks in parallel whenever possible, instead of processing them sequentially
- **Never `cd` into subdirectories** in Bash commands — it changes the working directory for subsequent calls. Use subshells (`(cd third_party/pg_duckdb && git ...)`) or `pushd`/`popd` (`pushd third_party/pg_duckdb; git ...; popd`) to keep the working directory at the project root.
