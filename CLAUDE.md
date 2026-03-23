This file provides guidance to AI coding assistants when working with code in this repository.

## Project Architecture

`pg_ducklake` is a PostgreSQL extension that extends `pg_duckdb` to support DuckLake, an open lakehouse format. This extension static-links `ducklake`, the official DuckDB extension, and loads it
via DuckDB's `LoadStaticExtension<T>()`.
Its C++ code should use `namespace pgducklake`.

- header files in `include/`
- implement files in `src/`
- tests in `test/`

### Lifecycle scopes

pg_ducklake has three object lifecycles:

```
PG extension           CREATE/DROP EXTENSION pg_ducklake
PG backend process     _PG_init, one per connection
  +-- DuckDB instance  [1..N via recycle_ddb]
```

PG extension and PG backend process are parallel -- the extension
creates SQL objects (table AM, functions, metadata tables), while
each backend process independently initializes its own state.

#### Entrypoints

| Scope              | Entrypoint                                            |
| ------------------ | ----------------------------------------------------- |
| PG extension       | sql/pg_ducklake--\*.sql                               |
| PG backend process | src/pgducklake.cpp (`_PG_init`)                       |
| DuckDB instance    | src/pgducklake_duckdb.cpp (`ducklake_load_extension`) |

#### 1. PG extension

Created by `CREATE EXTENSION pg_ducklake`.
Destroyed by `DROP EXTENSION pg_ducklake`.

SQL catalog objects: `ducklake` schema and metadata tables, table AM
`ducklake`, index AM `ducklake_sorted`, SQL functions and procedures,
event triggers, predefined roles.

#### 2. PG backend process

Created by `_PG_init()` when `pg_ducklake.so` is loaded in a new
backend. Destroyed on backend exit. One per connection.

C++ static variables and static class members belong here -- they
live as long as the process regardless of DuckDB instance recycling.

#### 3. DuckDB instance

Created by `DuckDBManager::Initialize()` on first DuckDB query.
Destroyed by `DuckDBManager::Reset()` via `recycle_ddb()` or backend
exit. Hook on create: `ducklake_load_extension(duckdb::DuckDB &db)`.

Everything registered on `db.instance` is lost on recycle and
re-created by `ducklake_load_extension`. Per-transaction
`PgDuckLakeMetadataManager` instances also belong here.

#### @scope convention

Each source file declares its scopes in the header comment:

```cpp
/*
 * @scope backend: register GUCs
 * @scope duckdb-instance: register wrapper macros in DuckDB catalog
 */
```

These tags are the source of truth for per-file classification.

### DDL path

DDL is executed by PostgreSQL, then `ducklake_<ddl>_trigger` is called to handle DDL operations (see @src/pgducklake_ddl.cpp).
Event triggers check events and synchronize corresponding DuckDB objects in `PGDUCKLAKE_DUCKDB_CATALOG`.

### DML path

DML containing ducklake objects is caught by `pg_duckdb` hooks (see `DuckdbInitHooks` in @third_party/pg_duckdb/src/pgduckdb_hooks.cpp).
SQL is parsed and converted to DuckDB SQL by `DuckdbPlannerHook`, then passed to DuckDB for execution. Ducklake tables are converted to `pgducklake.<schema_name>.<table_name>` (`RegisterDuckdbTableAm`).

## Build and Test Commands

See `setup-dev` skill for full dev environment setup (ccache, PostgreSQL from source, submodules, worktrees). See `commit-message-format`, `pr`, and `fix-ci` skills for git/CI workflows.

Supported PostgreSQL versions: 14, 15, 16, 17, 18.

`PG_CONFIG` is required. Usually a local pg is installed under workdir, e.g. `PG_CONFIG=$(pwd)/pg-18/bin/pg_config`, to avoid conflicts with other worktrees. If neither local pg nor global pg is found, stop and ask user.

```bash
# macOS add:
# LIBRARY_PATH="$(brew --prefix)/lib:$LIBRARY_PATH"

NCPU=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
PG_CONFIG=<pg_config> make -j"$NCPU" && make install
PG_CONFIG=<pg_config> make installcheck # build, install, and runs both regression + isolation

# Run single regression test
PG_CONFIG=<pg_config> make check-regression TEST=basic

# Run single isolation test
PG_CONFIG=<pg_config> make check-isolation TEST=concurrent_writes
```

Tests live in `test/regression/` (SQL regression) and `test/isolation/` (concurrency specs). Use regression and isolation tests to verify functionality as possible.

## Docs

See `coding-rules` skill for the full docs tree and style guide.

## Miscellaneous

- When exploring multiple files, run in parallel whenever possible, instead of processing them sequentially.
- **Never `cd` into subdirectories** in Bash commands — it changes the working directory for subsequent calls. Use subshells (`(cd third_party/pg_duckdb && git ...)`) or `pushd`/`popd` (`pushd third_party/pg_duckdb; git ...; popd`) to keep the working directory at the project root.
