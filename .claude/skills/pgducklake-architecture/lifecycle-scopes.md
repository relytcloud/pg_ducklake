# Lifecycle Scopes

pg_ducklake has three object lifecycles.

## Scope hierarchy

```
PG extension           CREATE/DROP EXTENSION pg_ducklake
PG backend process     _PG_init, one per connection
  +-- DuckDB instance  [1..N via recycle_ddb]
```

PG extension and PG backend process are parallel -- the extension
creates SQL objects (table AM, functions, metadata tables), while
each backend process independently initializes its own static state.

## Entrypoints

| Scope | Entrypoint |
|-------|-----------|
| PG extension | sql/pg_ducklake--0.1.0.sql |
| PG backend process | src/pgducklake.cpp (`_PG_init`) |
| DuckDB instance | src/pgducklake_duckdb.cpp (`ducklake_load_extension`) |

## 1. PG extension

Created by `CREATE EXTENSION pg_ducklake`.
Destroyed by `DROP EXTENSION pg_ducklake`.

SQL catalog objects: `ducklake` schema and metadata tables, table AM
`ducklake`, index AM `ducklake_sorted`, SQL functions and procedures,
event triggers, predefined roles.

## 2. PG backend process

Created by `_PG_init()` when `pg_ducklake.so` is loaded in a new
backend. Destroyed on backend exit. One per connection.

C++ static variables and static class members belong here -- they
live as long as the process regardless of DuckDB instance recycling.

## 3. DuckDB instance

Created by `DuckDBManager::Initialize()` on first DuckDB query.
Destroyed by `DuckDBManager::Reset()` via `recycle_ddb()` or backend
exit. Hook on create: `ducklake_load_extension(duckdb::DuckDB &db)`.

Everything registered on `db.instance` is lost on recycle and
re-created by `ducklake_load_extension`. Per-transaction
`PgDuckLakeMetadataManager` instances also belong here.

## @scope convention

Each source file declares its scopes in the header comment with a
description of what it contributes to each scope:

```cpp
/*
 * @scope backend: register GUCs
 * @scope duckdb-instance: register wrapper macros in DuckDB catalog
 */
```

These tags are the source of truth for per-file classification.

## Decision guide

When adding new state, ask:

1. **Is it a C++ static variable or static class member?** Register in
   `_PG_init()` (backend process). It survives DuckDB recycle.

2. **Does it depend on the DuckDB instance?** Register on `db.instance`
   in `ducklake_load_extension()` (DuckDB instance). It will be
   re-created on recycle automatically.

3. **Is it SQL catalog state?** Put it in the extension SQL script
   (PG extension). It is created by `CREATE EXTENSION`.
