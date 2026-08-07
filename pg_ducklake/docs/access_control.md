# Access Control for DuckLake Tables

DuckLake tables are exposed via PostgreSQL's table access method (AM), so
standard PostgreSQL privilege mechanisms (`GRANT`/`REVOKE`) apply in principle.
INSERT permissions are enforced by PostgreSQL, including source relations in
`INSERT ... SELECT`. Other DML is routed through DuckDB and still has permission
gaps.

This document describes what works, what does not, and the recommended setup
for multi-role environments. See also the upstream
[DuckLake Access Control guide](https://ducklake.select/docs/stable/duckdb/guides/access_control).

## What Works

| Check | Mechanism |
|---|---|
| DDL ownership (ALTER/DROP TABLE) | Standard PostgreSQL ownership check |
| VACUUM ownership | Standard PostgreSQL ownership check (VACUUM is a no-op on DuckLake tables) |
| INSERT target and source permissions | Checked recursively by PostgreSQL, including cached plans and role changes |
| INSERT source RLS | Rejected because DuckDB fallback cannot preserve source policies |
| Local filesystem access | `pg_read_server_files` / `pg_write_server_files` required for local storage |

## Known Gaps

| Gap | Root Cause |
|---|---|
| SELECT/UPDATE/DELETE table-level permissions | the libpgddb planner sets `permInfos = NULL`, skipping executor-level checks |
| Column-level SELECT restrictions | Same as above |
| `ducklake.time_travel()` bypasses table-level checks | Table name is a text argument, not an RTE |
| Non-superusers cannot use local file storage without explicit grants | libpgddb disables `LocalFileSystem` for users without `pg_read_server_files` + `pg_write_server_files`, blocking DuckLake catalog attach, reads, and writes ([#164](https://github.com/relytcloud/pg_ducklake/issues/164)) |

These remaining gaps exist because libpgddb's `pgddb::PlanNode()` sets
`result->permInfos = NULL` in the `PlannedStmt`, causing the executor to skip
relation-level permission checks. pg_ducklake separately validates INSERT
permissions before planning and again at execution for cached plans.

## Predefined Roles

pg_ducklake creates three GROUP roles (NOLOGIN) at extension installation:

| Role | GUC | Intended access |
|---|---|---|
| `ducklake_superuser` | `ducklake.superuser_role` | Full DDL + DML on DuckLake tables |
| `ducklake_writer` | `ducklake.writer_role` | DML (SELECT/INSERT/UPDATE/DELETE) on DuckLake tables |
| `ducklake_reader` | `ducklake.reader_role` | SELECT-only on DuckLake tables |

Role names are configurable via `postgresql.conf` GUCs (set before
`CREATE EXTENSION`). Set a GUC to an empty string to skip creating that role.

All three roles have full access to the `ducklake` metadata schema (required
for DuckDB's SPI-based metadata manager).

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
This happens because the libpgddb kernel disables DuckDB's `LocalFileSystem` for
users not in both `pg_read_server_files` and `pg_write_server_files`. The
restriction affects all local storage operations -- catalog attach, reads
(SELECT), and writes (INSERT). See [#164](https://github.com/relytcloud/pg_ducklake/issues/164).

These grants are **not** needed for S3/GCS/R2 storage.

Then grant privileges on individual tables to the predefined roles:

```sql
GRANT ALL ON TABLE my_table TO ducklake_superuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_table TO ducklake_writer;
GRANT SELECT ON TABLE my_table TO ducklake_reader;
```

> **Note:** INSERT grants are enforced. SELECT, UPDATE, and DELETE grants remain
> defense-in-depth until the libpgddb kernel preserves their permission metadata.

## Regression Test

See `test/regression/sql/access_control.sql` for a self-contained test that
verifies INSERT enforcement and the remaining known gaps.

## References

- [DuckLake Access Control guide](https://ducklake.select/docs/stable/duckdb/guides/access_control) -- DuckLake's native ACL model
- `libpgduckdb/pgddb_planner.cpp` (repo root) -- `check_view_perms_recursive()` and `pgddb::PlanNode()`
