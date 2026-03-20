---
name: pgducklake-architecture
description: "Architecture guide for pg_ducklake. Use when changing extension lifecycle, metadata manager behavior, DDL trigger flow, DuckDB bridge logic, or adding/moving static state between scopes."
user-invocable: false
---

Use this skill when a task touches architecture or behavior across PostgreSQL and DuckDB boundaries.

## Component map

- @src/pgducklake.cpp: `_PG_init`, bootstrap, extension registration wiring
- @src/pgducklake_duckdb.cpp: DuckDB bridge, deferred static extension load
- @src/pgducklake_duckdb_query.cpp: DuckDB query execution wrapper via pg_duckdb's `duckdb.raw_query()` UDF
- @src/pgducklake_metadata_manager.cpp: `PgDuckLakeMetadataManager` implementation
- @src/pgducklake_ddl.cpp: create/drop event triggers, DuckDB DDL synchronization
- @src/pgducklake_table_am.cpp: table access method registration

## Lifecycle (deferred extension load)

1. `pg_duckdb` loads (`shared_preload_libraries`).
2. `pg_ducklake` loads (`shared_preload_libraries`), triggers `_PG_init()`.
   - `RegisterDuckdbLoadExtension(...)` registers hooks on DuckDB creation.
3. `CREATE EXTENSION pg_ducklake` triggers `sql/pg_ducklake-*.sql`.
   - Creates DDL triggers, table access method, UDFs
   - Executes `ducklake_initialize`
4. During DuckDB instance (re-)initialization in each PostgreSQL process, callbacks registered by `RegisterDuckdbLoadExtension(...)` are executed.
   - `ducklake_load_extension` loads static extension `ducklake` into DuckDB, registers custom DuckLake metadata manager, and attaches pg_ducklake catalog on duckdb instance.

Reason: static extension loading requires fully initialized DuckDB instance; metadata manager registration does not.

## Lifecycle scopes

pg_ducklake has three object lifecycles:

```
PG extension           CREATE/DROP EXTENSION pg_ducklake
PG backend process     _PG_init, one per connection
  +-- DuckDB instance  [1..N via recycle_ddb]
```

PG extension and PG backend process are parallel -- the extension
creates SQL objects (table AM, functions, metadata tables), while
each backend process independently initializes its own static state.

### Entrypoints

| Scope | Entrypoint |
|-------|-----------|
| PG extension | sql/pg_ducklake--0.1.0.sql |
| PG backend process | src/pgducklake.cpp (`_PG_init`) |
| DuckDB instance | src/pgducklake_duckdb.cpp (`ducklake_load_extension`) |

### 1. PG extension

Created by `CREATE EXTENSION pg_ducklake`.
Destroyed by `DROP EXTENSION pg_ducklake`.

SQL catalog objects: `ducklake` schema and metadata tables, table AM
`ducklake`, index AM `ducklake_sorted`, SQL functions and procedures,
event triggers, predefined roles.

### 2. PG backend process

Created by `_PG_init()` when `pg_ducklake.so` is loaded in a new
backend. Destroyed on backend exit. One per connection.

C++ static variables and static class members belong here -- they
live as long as the process regardless of DuckDB instance recycling.

### 3. DuckDB instance

Created by `DuckDBManager::Initialize()` on first DuckDB query.
Destroyed by `DuckDBManager::Reset()` via `recycle_ddb()` or backend
exit. Hook on create: `ducklake_load_extension(duckdb::DuckDB &db)`.

Everything registered on `db.instance` is lost on recycle and
re-created by `ducklake_load_extension`. Per-transaction
`PgDuckLakeMetadataManager` instances also belong here.

### @scope convention

Each source file declares its scopes in the header comment:

```cpp
/*
 * @scope backend: register GUCs
 * @scope duckdb-instance: register wrapper macros in DuckDB catalog
 */
```

These tags are the source of truth for per-file classification.

### Decision guide

When adding new state, ask:

1. **Is it a C++ static variable or static class member?** Register in
   `_PG_init()` (backend process). It survives DuckDB recycle.

2. **Does it depend on the DuckDB instance?** Register on `db.instance`
   in `ducklake_load_extension()` (DuckDB instance). It will be
   re-created on recycle automatically.

3. **Is it SQL catalog state?** Put it in the extension SQL script
   (PG extension). It is created by `CREATE EXTENSION`.

## DDL path

DDL is executed by PostgreSQL, then `ducklake_<ddl>_trigger` is called to handle DDL operations (see @src/pgducklake_ddl.cpp).
Event triggers check events and synchronize corresponding DuckDB objects in `PGDUCKLAKE_DUCKDB_CATALOG`.

## DML path

DML containing ducklake objects is caught by `pg_duckdb` hooks (see `DuckdbInitHooks` in @third_party/pg_duckdb/src/pgduckdb_hooks.cpp).
SQL is parsed and converted to DuckDB SQL by `DuckdbPlannerHook`, then passed to DuckDB for execution. Ducklake tables are converted to `pgducklake.<schema_name>.<table_name>` (`RegisterDuckdbTableAm`).

## Mixed-write guard

`pg_duckdb` tracks command IDs to block mixed PostgreSQL/DuckDB writes in one transaction. DuckLake metadata flows can look like mixed writes even when they are internal extension operations.

`UnsafeCommandIdGuard` synchronizes pg_duckdb's expected command ID around these operations.

Use it in code paths where internal SPI writes or DDL-triggered DuckDB writes would otherwise trip detection.

Known use sites:

- metadata query execution path (@src/pgducklake_metadata_manager.cpp)
- create/drop trigger paths (@src/pgducklake_ddl.cpp)

Do not remove guard usage without verifying transaction-safety and mixed-write behavior end-to-end.

## Guardrails

- Keep `pgducklake_duckdb.cpp` PostgreSQL-header-free.
- Keep metadata query routing through SPI unless intentionally redesigning it.
- Prefer additive changes to extension lifecycle.
- When adding static state, verify which lifecycle scope it belongs to using the decision guide above.
