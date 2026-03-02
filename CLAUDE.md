This file provides guidance to AI coding assistants when working with code in this repository.

## Project Architecture

`pg_ducklake` is a PostgreSQL extension that extends `pg_duckdb` to support DuckLake, an open lakehouse format. This extension static-links `ducklake`, the official DuckDB extension, and loads it
via DuckDB's `LoadStaticExtension<T>()`.
Its C++ code should use `namespace pgducklake`.

- header files in `include/`
- implement files in `src/`
  - `src/pgducklake.cpp`: `_PG_init`, extension bootstrap
  - `src/pgducklake_duckdb.cpp`: DuckDB bridge, static extension load
  - `src/pgducklake_metadata_manager.cpp`: custom DuckLake metadata manager
- regression tests in `test/regression/`
- isolation tests in `test/isolation/`

## Build and Test Commands

`PG_CONFIG` is required. Usually a local pg is installed under workdir, e.g. `PG_CONFIG=$(pwd)/pg-17/bin/pg_config`, to avoid conflicts with other worktrees. If neither local pg nor global pg is found, stop and ask user.

```bash
git submodule update --init --recursive
PG_CONFIG=<pg_config> make install-pg_duckdb
PG_CONFIG=<pg_config> make install
PG_CONFIG=<pg_config> make installcheck

# Run single test
PG_CONFIG=<pg_config> make installcheck TEST=basic

# Run only isolation tests
PG_CONFIG=<pg_config> make check-isolation
```

**IMPORTANT: Re-install `pg_duckdb` after modifying `third_party/pg_duckdb`.**

Use regression and isolation tests to verify functionality as possible.

## Code Style

### General

- Write clean, minimal code; fewer lines is better
- Prioritize simplicity for effective and maintainable software
- Only include comments that are essential to understanding functionality or convey non-obvious information

### C/C++

- Avoid using `extern "C"` to reference symbols from the same library. Instead, place it at the header file.
- Use `extern "C"` only when necessary, such as when interfacing with third-party libraries.
- Use `namespace pgducklake` for C++ extension code (do not use `namespace pg_ducklake`).
- Use `pgducklake::` when qualifying symbols outside the namespace block.

### Imports

PostgreSQL and DuckDB headers are conflict-prone. Follow strict include order in mixed files:

1. DuckDB headers
2. DuckLake headers
3. Local `pgducklake` headers
4. PostgreSQL headers last, inside `extern "C"`, must include `<postgres.h>` at first.

### Third-party Submodules

Treat `third_party/pg_duckdb` as upstream:

- Prefer additive hooks, avoid invasive edits.
- Keep diffs minimal and upstream-friendly.
- Ensure zero behavior change when hooks are unused.

## Docs Style

To avoid _Docs Rot_, **DO NOT write docs everywhere**. Consider replacing the documentation with:

- self-contained regression tests
- header comments of source files
- SKILLs under @.claude/skills/
- @README.md and @CLAUDE.md

Guidelines:

- Write header comments for each source file, explaining the purpose and usage of the file. Maintain comments after each edit.
- Only include comments that are essential to understanding functionality or convey non-obvious information. Otherwise, let code speak for itself.
- Only write docs about high-level concepts and design decisions if really necessary.

## Miscellaneous

- When modifying multiple files, run file modification tasks in parallel whenever possible, instead of processing them sequentially
