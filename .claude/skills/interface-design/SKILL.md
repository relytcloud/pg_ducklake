---
name: interface-design
description: "Decision framework for how pg_ducklake exposes features to users: PG-native vs DuckDB-native vs custom mapping. Consult before designing any new user-facing interface -- SQL functions, procedures, DDL syntax, GUCs, types, or access methods. Also consult when reviewing whether an existing interface follows project conventions."
---

# Interface Design Guide

pg_ducklake bridges two worlds: PostgreSQL's SQL catalog and DuckDB's
lakehouse engine. Every user-facing feature requires a decision about
which world owns the interface. This guide captures the decision
framework and existing precedents.

## Decision Framework

Ask these questions in order:

### 1. Does PostgreSQL have an equivalent pattern with matching semantics?

**If yes -> PG-native.** Reuse the PostgreSQL mechanism so users get
familiar syntax, tooling support (psql tab-completion, pg_dump, etc.),
and catalog integration.

Examples:
- **Tables**: Table AM (`CREATE TABLE ... USING ducklake`) -- same
  DDL, same pg_class entry, same `\d` output
- **DDL**: Event triggers intercept `CREATE/ALTER/DROP TABLE` and sync
  to DuckDB transparently -- users write standard DDL
- **Configuration**: GUCs (`SET ducklake.vacuum_delete_threshold`) --
  standard `SHOW`/`SET`, appears in `pg_settings`
- **Access control**: Predefined roles (`ducklake_superuser`,
  `ducklake_writer`, `ducklake_reader`) -- standard `GRANT`/`REVOKE`
- **DML**: Standard `INSERT/SELECT/UPDATE/DELETE/VACUUM` -- routed to
  DuckDB via pg_duckdb hooks, invisible to the user
- **Remote tables**: FDW (`CREATE FOREIGN TABLE ... SERVER ducklake_fdw`)
  -- standard FDW pattern, works with `\det`, `IMPORT FOREIGN SCHEMA`

### 2. Is it a DuckDB/DuckLake-only concept with no PG analog?

**If yes -> DuckDB-native passthrough.** Expose as a SQL function or
procedure in the `ducklake` schema that routes directly to DuckDB.
Keep the DuckDB function name and semantics.

Examples:
- `ducklake.snapshots()`, `ducklake.current_snapshot()` -- lakehouse
  versioning has no PG equivalent
- `ducklake.cleanup_old_files()` -- file-level GC on object storage
- `ducklake.time_travel(table, version_or_timestamp)` -- historical
  queries via `AT (SNAPSHOT_ID => ...)` syntax
- `ducklake.table_insertions()`, `table_deletions()`, `table_changes()`
  -- change data feed, unique to lakehouse
- `ducklake.options()`, `ducklake.set_option()` -- DuckLake catalog
  options that don't map to GUCs

### 3. Does it resemble a PG pattern but with different semantics?

**Case-by-case.** This is the hard category. The key question is:
*does mapping to the PG pattern help or mislead users?*

**Map to PG when the abstraction holds:**
- **Sort order -> Index AM** (`CREATE INDEX ... USING ducklake_sorted`):
  sorting is advisory metadata, similar to how an index is a catalog
  marker for access-path hints. The index stores no data (ambuild is
  a no-op; cost estimates are 1e10 so the planner never picks it).
  Bidirectional sync keeps pg_class and DuckDB metadata consistent.
  Users can also use `CALL ducklake.set_sort()` as an alternative.

**Don't map when it would mislead:**
- **Partition -> Procedure** (`CALL ducklake.set_partition()`):
  DuckLake partitioning is columnar data layout optimization.
  PostgreSQL's `PARTITION BY` creates child tables in an inheritance
  hierarchy -- fundamentally different structure. Mapping to PG
  partitioning would create a false catalog state. Instead, exposed
  as a procedure call.

**Decision heuristics for gray areas:**
- Does the PG mechanism create catalog entries that tools/users would
  inspect? If so, those entries must be meaningful. A dummy index
  with no data but visible in `\di` is OK because users understand
  "this table is sorted." A dummy partition hierarchy visible in
  `\d+` would be confusing because the child tables don't exist.
- Does the PG mechanism have write-side semantics that conflict?
  PG partitioning routes inserts to child tables -- DuckLake doesn't.
  PG indexes can be used by the planner -- ducklake_sorted can't
  (and explicitly prevents it with inflated costs).
- Can bidirectional sync be maintained? Sort keys sync both ways
  (PG index <-> DuckDB metadata). If sync would be fragile or lossy,
  prefer DuckDB-native.

## Routing Patterns for Functions

pg_ducklake uses four routing mechanisms, in increasing complexity:

### Pattern A: Passthrough (simplest)

PG function stub registered as `AS '$libdir/pg_duckdb', 'duckdb_only_function'`.
pg_duckdb routes to `system.main.<func_name>(args...)` in DuckDB.

Use when: function signature is identical between PG and DuckDB.

### Pattern B: Wrapper Macro

Registered in DuckDB catalog via `RegisterWrapperMacros()`. The macro
injects the catalog constant (`PGDUCKLAKE_DUCKDB_CATALOG`) so the PG
call `ducklake.snapshots()` becomes `ducklake_snapshots('pgducklake')`.

Use when: the DuckDB function needs a catalog argument that users
shouldn't have to provide.

### Pattern C: Table Function Set

Used for overloaded functions with multiple signatures. A bind function
resolves the right underlying DuckDB function and rewires arguments.

Use when: same PG function name needs multiple DuckDB signatures
(e.g., `cleanup_old_files()` vs `cleanup_old_files(interval)`).

**Special case -- bind-time function replacement**: `time_travel()` uses
a TableFunctionSet where the bind phase resolves the table's schema at
the requested snapshot via DuckDB's `AT` clause, then *replaces the
entire table function* with the resolved scan function. The execute/init
functions are never called. Use this pattern when the return schema
depends on the input arguments (e.g., schema evolution across versions).

### Pattern D: Planner Rewrite

Planner hook rewrites `regclass` arguments to `(schema_name, table_name)`
text pairs before routing to DuckDB.

Use when: the PG interface accepts `regclass` (e.g., `'my_table'::regclass`)
but DuckDB needs text schema+table. Provide both overloads: regclass
for PG-native UX, text for programmatic/DuckDB-compatible use.

## Routing Pattern for Procedures

PG procedure stub registered as `AS 'MODULE_PATHNAME', 'ducklake_only_procedure'`.
Utility hook intercepts `CALL` statements, extracts Const arguments,
converts regclass OIDs to named parameters, constructs DuckDB SQL,
and calls `ExecuteDuckDBQuery()`.

## Type Mapping

- Standard types (int, text, timestamp, etc.) map 1:1
- DuckDB-only types with no PG equivalent: create a pseudo-type in the
  `ducklake` schema, restrict to ducklake tables only, enforce in the
  create-table trigger. Precedent: `ducklake.variant` (INTERNALLENGTH
  = VARIABLE, text I/O, actual data handled by DuckDB)

## Sync Direction Reference

| Interface | Direction | Mechanism |
|-----------|-----------|-----------|
| DDL (CREATE/ALTER/DROP TABLE) | PG -> DuckDB | Event triggers |
| External DuckDB metadata changes | DuckDB -> PG | Snapshot trigger (`ducklake_snapshot_trigger`) |
| Sort keys | PG <-> DuckDB | Utility hook + snapshot trigger (bidirectional) |
| Partition keys | PG -> DuckDB | Procedure call (one-way) |
| GUCs | PG-only | DuckDB reads via C++ globals |
| COMMIT | PG -> DuckDB | Utility hook mirrors `COMMIT` |

## Return Type Convention

- Functions with a **stable, known schema** (column set does not change
  across calls): declare typed OUT parameters + `RETURNS SETOF record`.
  Precedents: `options()`, `snapshots()`, `current_snapshot()`,
  `last_committed_snapshot()`, `table_info()`, `get_partition()`,
  `get_sort()`.
- Functions whose schema **depends on input** (e.g., the queried table's
  columns): use `RETURNS SETOF duckdb.row`. Precedents: `time_travel()`,
  `table_insertions()`, `list_files()`.

Typed OUT params improve psql column headers and tooling integration.

## Known Cosmetic Decisions

These are known inconsistencies accepted as not worth a breaking change:

- `set_option(name, value, scope)` puts the table scope 3rd, while
  `set_partition(scope, ...)` and `set_sort(scope, ...)` put it 1st.
  This is forced by overload resolution (2-arg vs 3-arg set_option).
- `list_files` uses `verb_noun` naming while change-feed functions use
  `table_noun` (`table_insertions`, etc.). Both are discoverable.
- `cleanup_old_files()` and `flush_inlined_data()` are functions (not
  procedures) despite being side-effectful, because PG procedures cannot
  return result sets.

## Checklist for New Interfaces

1. Classify: PG-native, DuckDB-native, or custom mapping?
2. If PG-native: does the catalog state accurately represent reality?
3. If DuckDB-native: keep the DuckDB name; put in `ducklake` schema
4. Pick the simplest routing pattern that works (A > B > C > D)
5. If the function accepts a table reference, provide both `regclass`
   and `(schema, table)` text overloads
6. If the return schema is stable, use typed OUT parameters
7. If bidirectional sync is needed, add guard flags to prevent cycles
   (see `syncing_from_metadata`, `sort_synced_from_pg` precedents)
8. Add regression test covering the interface from PG's perspective
9. Update `sql/pg_ducklake--<version>.sql` for new SQL objects
