# Access Control for DuckLake Tables

DuckLake tables are exposed via PostgreSQL's table access method (AM), so
standard PostgreSQL privilege mechanisms (`GRANT`/`REVOKE`) apply in principle.
However, because pg_duckdb routes queries to DuckDB's execution engine, **most
DML-level permission checks are currently bypassed**.

This document describes what works, what doesn't, and the recommended setup
for multi-role environments.

## What Works

| Check | Mechanism |
|---|---|
| DDL ownership (ALTER/DROP TABLE) | Standard PostgreSQL ownership check |
| VACUUM ownership | Standard PostgreSQL ownership check |
| `duckdb_group` membership | pg_duckdb rejects DuckDB execution for non-members |
| Local filesystem access | `pg_read_server_files` / `pg_write_server_files` required for local storage |

## Known Gaps

| Gap | Root Cause |
|---|---|
| SELECT/INSERT/UPDATE/DELETE table-level permissions | pg_duckdb's planner sets `permInfos = NULL`, skipping executor-level checks |
| Column-level SELECT restrictions | Same as above |
| `ducklake.time_travel()` bypasses table-level checks | Table name is a text argument, not an RTE |

These gaps exist because pg_duckdb's `DuckdbPlanNode()` only runs
`check_view_perms_recursive()` (which checks VIEW permissions) and sets
`result->permInfos = NULL` in the `PlannedStmt`, causing the executor to skip
all relation-level permission checks.

## Role Setup

### Prerequisites

All roles that need to execute DuckDB queries must be members of `duckdb_group`
(configured via `duckdb.postgres_role`). For local file storage, roles also need
`pg_read_server_files` and `pg_write_server_files`; these are **not** needed for
S3/GCS/R2 storage.

### Three-Role Model

Despite the DML enforcement gaps, the following setup is still recommended as a
defense-in-depth measure. When pg_duckdb adds proper permission enforcement,
these grants will take effect without changes.

```sql
-- Admin: full DDL + DML
CREATE USER lake_admin IN ROLE duckdb_group, pg_read_server_files, pg_write_server_files;
GRANT ALL ON TABLE my_table TO lake_admin;

-- Writer: DML only (DDL blocked by ownership check)
CREATE USER lake_writer IN ROLE duckdb_group, pg_read_server_files, pg_write_server_files;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_table TO lake_writer;

-- Reader: SELECT only (DML not currently enforced -- known gap)
CREATE USER lake_reader IN ROLE duckdb_group, pg_read_server_files, pg_write_server_files;
GRANT SELECT ON TABLE my_table TO lake_reader;
```

### DuckLake Metadata Tables

DuckDB's SPI-based metadata manager writes to `ducklake.*` tables internally.
Non-superuser roles need DML access to these tables:

```sql
GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO lake_admin, lake_writer, lake_reader;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake TO lake_admin, lake_writer, lake_reader;
```

## Regression Test

See `test/regression/sql/access_control.sql` for a self-contained test that
verifies the current behavior, including all known gaps.

## References

- [DuckLake Access Control](https://ducklake.select/docs/stable/duckdb/guides/access_control) -- DuckLake's native ACL model (metadata-service-based)
- `third_party/pg_duckdb/src/pgduckdb_planner.cpp` -- `check_view_perms_recursive()` and `DuckdbPlanNode()`
