# Access Control for DuckLake Tables

DuckLake tables are exposed via PostgreSQL's table access method (AM), so
standard PostgreSQL privilege mechanisms (`GRANT`/`REVOKE`) apply in principle.
However, because pg_duckdb routes queries to DuckDB's execution engine, **most
DML-level permission checks are currently bypassed**.

This document describes what works, what doesn't, and the recommended setup
for multi-role environments. See also the upstream
[DuckLake Access Control guide](https://ducklake.select/docs/stable/duckdb/guides/access_control).

## What Works

| Check | Mechanism |
|---|---|
| DDL ownership (ALTER/DROP TABLE) | Standard PostgreSQL ownership check |
| VACUUM ownership | Standard PostgreSQL ownership check (VACUUM is a no-op on DuckLake tables) |
| `duckdb_group` membership | pg_duckdb rejects DuckDB execution for non-members |
| Local filesystem access | `pg_read_server_files` / `pg_write_server_files` required for local storage |

## Known Gaps

| Gap | Root Cause |
|---|---|
| SELECT/INSERT/UPDATE/DELETE table-level permissions | pg_duckdb's planner sets `permInfos = NULL`, skipping executor-level checks |
| Column-level SELECT restrictions | Same as above |
| `ducklake.time_travel()` bypasses table-level checks | Table name is a text argument, not an RTE |
| Non-superusers cannot use local file storage without explicit grants | pg_duckdb disables `LocalFileSystem` for users without `pg_read_server_files` + `pg_write_server_files`, blocking DuckLake catalog attach, reads, and writes ([#164](https://github.com/relytcloud/pg_ducklake/issues/164)) |

These gaps exist because pg_duckdb's `DuckdbPlanNode()` only runs
`check_view_perms_recursive()` (which checks VIEW permissions) and sets
`result->permInfos = NULL` in the `PlannedStmt`, causing the executor to skip
all relation-level permission checks.

## Predefined Roles

pg_ducklake creates three GROUP roles (NOLOGIN) at extension installation:

| Role | GUC | Intended access |
|---|---|---|
| `ducklake_superuser` | `ducklake.superuser_role` | Full DDL + DML on DuckLake tables |
| `ducklake_writer` | `ducklake.writer_role` | DML (SELECT/INSERT/UPDATE/DELETE) on DuckLake tables |
| `ducklake_reader` | `ducklake.reader_role` | SELECT-only on DuckLake tables |

Role names are configurable via `postgresql.conf` GUCs (set before
`CREATE EXTENSION`). Set a GUC to an empty string to skip creating that role.

All three roles are members of `duckdb_group` and have full access to the
`ducklake` metadata schema (required for DuckDB's SPI-based metadata manager).

### Usage

Create LOGIN users and grant membership in the appropriate role:

```sql
CREATE USER lake_admin IN ROLE ducklake_superuser;
CREATE USER lake_writer IN ROLE ducklake_writer;
CREATE USER lake_reader IN ROLE ducklake_reader;
```

For local file storage, also grant filesystem access:

```sql
GRANT pg_read_server_files, pg_write_server_files TO lake_admin, lake_writer, lake_reader;
```

**Without these grants, DuckLake will fail with:**
```
Permission Error: File system LocalFileSystem has been disabled by configuration
```
This happens because pg_duckdb disables DuckDB's `LocalFileSystem` for users
not in both `pg_read_server_files` and `pg_write_server_files`. The restriction
affects all local storage operations -- catalog attach, reads (SELECT), and
writes (INSERT). See [#164](https://github.com/relytcloud/pg_ducklake/issues/164).

These grants are **not** needed for S3/GCS/R2 storage.

Then grant privileges on individual tables to the predefined roles:

```sql
GRANT ALL ON TABLE my_table TO ducklake_superuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_table TO ducklake_writer;
GRANT SELECT ON TABLE my_table TO ducklake_reader;
```

> **Note:** Due to the known gaps above, DML-level grants are not yet enforced.
> This role setup is recommended as defense-in-depth; when pg_duckdb adds proper
> permission enforcement, these grants will take effect without changes.

## Regression Test

See `test/regression/sql/access_control.sql` for a self-contained test that
verifies the current behavior, including all known gaps.

## References

- [DuckLake Access Control guide](https://ducklake.select/docs/stable/duckdb/guides/access_control) -- DuckLake's native ACL model
- `third_party/pg_duckdb/src/pgduckdb_planner.cpp` -- `check_view_perms_recursive()` and `DuckdbPlanNode()`
