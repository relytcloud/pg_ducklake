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

#### Decision guide

When adding new state, ask:

1. **Is it a C++ static variable or static class member?** Register in
   `_PG_init()` (backend process). It survives DuckDB recycle.

2. **Does it depend on the DuckDB instance?** Register on `db.instance`
   in `ducklake_load_extension()` (DuckDB instance). It will be
   re-created on recycle automatically.

3. **Is it SQL catalog state?** Put it in the extension SQL script
   (PG extension). It is created by `CREATE EXTENSION`.

### DDL path

DDL is executed by PostgreSQL, then `ducklake_<ddl>_trigger` is called to handle DDL operations (see @src/pgducklake_ddl.cpp).
Event triggers check events and synchronize corresponding DuckDB objects in `PGDUCKLAKE_DUCKDB_CATALOG`.

### DML path

DML containing ducklake objects is caught by `pg_duckdb` hooks (see `DuckdbInitHooks` in @third_party/pg_duckdb/src/pgduckdb_hooks.cpp).
SQL is parsed and converted to DuckDB SQL by `DuckdbPlannerHook`, then passed to DuckDB for execution. Ducklake tables are converted to `pgducklake.<schema_name>.<table_name>` (`RegisterDuckdbTableAm`).

### Mixed-write guard

`pg_duckdb` tracks command IDs to block mixed PostgreSQL/DuckDB writes in one transaction. DuckLake metadata flows can look like mixed writes even when they are internal extension operations.

`UnsafeCommandIdGuard` synchronizes pg_duckdb's expected command ID around these operations.

Use it in code paths where internal SPI writes or DDL-triggered DuckDB writes would otherwise trip detection.

Known use sites:

- metadata query execution path (@src/pgducklake_metadata_manager.cpp)
- create/drop trigger paths (@src/pgducklake_ddl.cpp)

Do not remove guard usage without verifying transaction-safety and mixed-write behavior end-to-end.

### Architecture guardrails

- Keep `pgducklake_duckdb.cpp` PostgreSQL-header-free.
- Keep metadata query routing through SPI unless intentionally redesigning it.
- Prefer additive changes to extension lifecycle.
- When adding static state, verify which lifecycle scope it belongs to using the decision guide above.

## Build and Test Commands

See `workflow-dev-env` skill for full dev environment setup (ccache, PostgreSQL from source, submodules, worktrees). See `workflow-commit`, `pr`, and `fix-ci` skills for git/CI workflows.

Supported PostgreSQL versions: 14, 15, 16, 17, 18.

`PG_CONFIG` is required. Usually a local pg is installed under workdir, e.g. `PG_CONFIG=$(pwd)/pg-18/bin/pg_config`, to avoid conflicts with other worktrees. If neither local pg nor global pg is found, stop and ask user.

```bash
git submodule update --init --recursive
PG_CONFIG=<pg_config> make install
PG_CONFIG=<pg_config> make installcheck # build, install, and runs both regression + isolation

# Run single regression test
PG_CONFIG=<pg_config> make check-regression TEST=basic

# Run single isolation test
PG_CONFIG=<pg_config> make check-isolation TEST=concurrent_writes

# macOS: if linker errors, add:
# LIBRARY_PATH="$(brew --prefix)/lib:$LIBRARY_PATH"
```

Tests live in `test/regression/` (SQL regression) and `test/isolation/` (concurrency specs). Use regression and isolation tests to verify functionality as possible.

## Docs

All docs are reachable from one of two entrypoints:

### AI docs tree (`CLAUDE.md` entrypoint)

```
CLAUDE.md
  +-- .claude/skills/*          AI workflow guidance
  +-- src/*.cpp header comments  per-file purpose and usage
  +-- test/regression/           self-documenting test cases
  +-- test/isolation/            concurrency test cases
```

### Human docs tree (`README.md` entrypoint)

```
README.md
  +-- docs/README.md      index of all human docs
  +-- docs/*.md           all SQL objects, functions, and procedures
```

## Miscellaneous

- When exploring multiple files, run in parallel whenever possible, instead of processing them sequentially
- **Never `cd` into subdirectories** in Bash commands — it changes the working directory for subsequent calls. Use subshells (`(cd third_party/pg_duckdb && git ...)`) or `pushd`/`popd` (`pushd third_party/pg_duckdb; git ...; popd`) to keep the working directory at the project root.
