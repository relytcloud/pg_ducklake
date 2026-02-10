# DuckLake Inline Bypass for `INSERT ... SELECT UNNEST`

This page documents the internal performance path for batched array inserts.

Target pattern:

```sql
INSERT INTO some_ducklake_table
SELECT UNNEST($1), UNNEST($2), ...;
```

When matched, the planner creates a dedicated custom scan node and the executor writes directly through PostgreSQL SPI into DuckLake metadata/inlined tables, avoiding the normal DuckDB execution path for this statement.

## Activation Conditions

Bypass is considered only when all checks pass:

- Statement is `INSERT`.
- Target relation is a DuckLake table.
- `ducklake` option `data_inlining_row_limit` is greater than `0`.
- Target expressions resolve to `UNNEST` of external params (`$1`, `$2`, ...).
- Statement is not in an explicit transaction block.

If any check fails, planning continues with the normal path.

## Internal Execution Flow

### 1. Planner-time pattern detection

During planning, `DuckdbPlannerHook` runs `DetectInlineBypassPattern`.

- Validates statement shape and target relation.
- Resolves param indices and element/array types from `UNNEST(...)`.
- Reads the effective `data_inlining_row_limit`.

If detection succeeds, the planner does not build a DuckDB plan for this statement.

### 2. Custom plan creation

`CreateInlineBypassPlan` builds a `PlannedStmt` with a `CustomScan` node (`DuckLakeInlineBypass`).

The node serializes bypass metadata into `custom_private`, including:

- target table OID
- row limit
- schema/table name
- parameter indices
- array/element type OIDs

### 3. Executor-time parameter validation

`ExecCustomScan` reads `ParamListInfo` and validates runtime arrays:

- each referenced parameter must exist
- all arrays must have equal lengths
- row count must be within `data_inlining_row_limit`

If row count is `0`, the node returns without writes.

### 4. Metadata lookup and inlined table preparation

The executor uses SPI to:

- resolve `table_id` from DuckLake metadata
- read latest snapshot metadata (`snapshot_id`, `schema_version`, IDs)
- find or create an inlined data table entry in `ducklake_inlined_data_tables`

### 5. Data append and snapshot update

The node inserts data with one SPI statement shaped like:

```sql
INSERT INTO ducklake.<inlined_table> (...)
SELECT row_number() OVER () + $1 - 1, $2, NULL, UNNEST($3), UNNEST($4), ...;
```

Then it updates:

- `ducklake_snapshot`
- `ducklake_table_stats`
- `ducklake_snapshot_changes`

## Why This Is Faster

For the matched pattern, it avoids the extra PG->DuckDB->PG conversion loop and emits inlined rows directly from PostgreSQL array datums, reducing CPU and planning/execution overhead for array-batch ingestion.

## Operational Notes

- Enable/disable by changing `data_inlining_row_limit`:

```sql
CALL ducklake.set_option('data_inlining_row_limit', 100);  -- enable for small/medium batches
CALL ducklake.set_option('data_inlining_row_limit', 0);    -- disable
```

- Current implementation raises an error if runtime row count exceeds the limit; it does not transparently fall back once execution has entered the bypass node.
- This is a narrow, pattern-specific optimization and is intentionally conservative.

## Troubleshooting

If bypass is not selected:

- verify query shape is exactly `INSERT ... SELECT UNNEST($n), ...`
- verify the target is a DuckLake table
- verify `data_inlining_row_limit` is positive
- verify the statement is not executed inside `BEGIN ... COMMIT`

When selected, planning emits a notice similar to:

```
NOTICE: (PGDuckDB) Inline bypass: detected INSERT...SELECT UNNEST pattern for public.some_ducklake_table
```
