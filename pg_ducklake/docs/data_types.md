# Data Types

Data type support in pg_ducklake when using PostgreSQL as the DuckLake metadata
catalog.  The **Inlined Data Support** column describes how each type is handled
in inlined data tables (the in-catalog row store controlled by
`data_inlining_row_limit`).

| Group | DuckLake Type | DuckDB LogicalType | Inlined Data Support | Inlined PG Column Type |
|---|---|---|---|---|
| Primitive | `boolean` | BOOLEAN | Native | BOOLEAN |
| | `int8` | TINYINT | Native | SMALLINT |
| | `int16` | SMALLINT | Native | SMALLINT |
| | `int32` | INTEGER | Native | INTEGER |
| | `int64` | BIGINT | Native | BIGINT |
| | `uint8` | UTINYINT | Native | INTEGER |
| | `uint16` | USMALLINT | Native | INTEGER |
| | `uint32` | UINTEGER | Native | BIGINT |
| | `uint64` | UBIGINT | Standard path only | VARCHAR |
| | `int128` | HUGEINT | Standard path only | VARCHAR |
| | `uint128` | UHUGEINT | Standard path only | VARCHAR |
| | `float32` | FLOAT | Native | REAL |
| | `float64` | DOUBLE | Native | DOUBLE PRECISION |
| | `decimal(P, S)` | DECIMAL | Native | DECIMAL(P, S) |
| | `time` | TIME | Native | TIME |
| | `timetz` | TIME_TZ | Native | TIME WITH TIME ZONE |
| | `date` | DATE | Not native | VARCHAR |
| | `timestamp` | TIMESTAMP | Not native | VARCHAR |
| | `timestamptz` | TIMESTAMP_TZ | Not native | VARCHAR |
| | `timestamp_s` | TIMESTAMP_SEC | Not native | VARCHAR |
| | `timestamp_ms` | TIMESTAMP_MS | Not native | VARCHAR |
| | `timestamp_ns` | TIMESTAMP_NS | Not native | VARCHAR |
| | `interval` | INTERVAL | Native | INTERVAL |
| | `varchar` | VARCHAR | Not native | BYTEA |
| | `blob` | BLOB | Not native | BYTEA |
| | `json` | JSON | Not native | BYTEA |
| | `uuid` | UUID | Native | UUID |
| Nested | `list` | LIST | Not native | VARCHAR |
| | `struct` | STRUCT | Not native | VARCHAR |
| | `map` | MAP | Not native | VARCHAR |
| Semi-structured | `variant` | VARIANT | No inline | -- |
| Geometry | `point` | GEOMETRY | No inline | -- |
| | `linestring` | GEOMETRY | No inline | -- |
| | `polygon` | GEOMETRY | No inline | -- |
| | `multipoint` | GEOMETRY | No inline | -- |
| | `multilinestring` | GEOMETRY | No inline | -- |
| | `multipolygon` | GEOMETRY | No inline | -- |
| | `linestring_z` | GEOMETRY | No inline | -- |
| | `geometrycollection` | GEOMETRY | No inline | -- |

## Inlined Data Support Categories

- **Native**: The PG column type can represent the full DuckDB value range.
  Values are stored as-is in the inlined data table.
- **Not native**: The PG column type differs from the source type.  DuckDB
  handles read/write conversion transparently; the native writer
  (`enable_direct_insert`) converts before inserting into the inlined table.
- **Standard path only**: DuckDB inlines the type, but the native writer
  declines it and the statement falls back to the standard path.  The PG facade
  carries these as unconstrained `numeric`, which admits values the DuckLake
  type cannot hold; no PG DDL can create such a column, so no test can hold a
  native-writer conversion honest.
- **No inline**: The type does not support data inlining.  Rows are always
  written to Parquet files.

## Known Limitations

- **`varchar` with embedded null bytes**: DuckDB VARCHAR can contain null bytes
  (e.g. `'ABC' || chr(0) || '123'`), but PostgreSQL TEXT/VARCHAR cannot.
  Querying such values through pg_ducklake fails with
  `ERROR: null character not permitted`.  This is the reason the upstream
  metadata manager stores VARCHAR as BYTEA in inlined data tables.  Queries
  that go through DuckDB's Parquet read path are unaffected; the error occurs
  only when PostgreSQL processes the value (e.g. returning it to the client via
  the SPI result path).

- **`interval` with large microsecond component on PG14**: DuckLake serializes
  INTERVAL values to inlined data tables as
  `'%d months %d days %lld microseconds'`.  PostgreSQL 14's interval parser
  uses a 32-bit intermediate when parsing field values, so microsecond counts
  exceeding INT32_MAX (~2147 seconds / ~35 minutes) cause
  `ERROR: interval field value out of range`.  This is a PG14 parser bug
  (fixed in PG15+).  Workaround: keep the sub-day time component of interval
  values below ~35 minutes when using data inlining on PG14, or disable
  inlining (`data_inlining_row_limit = 0`) and use the Parquet path.

- **Nested types with `COPY FROM STDIN`**: nested columns inline as VARCHAR
  holding DuckDB's text format (`[1, 2]`), which PostgreSQL's output functions
  do not produce (`array_out` writes `{1,2}`).  `INSERT` falls back to the
  standard path, but `COPY FROM STDIN` has no fallback and rejects the
  statement: `ERROR: COPY FROM STDIN does not support column "c" of type
  integer[]`.  Only a column `COPY` can actually put a value in is rejected, so
  `COPY t (id) FROM STDIN` still works when the nested column is omitted and
  defaults to NULL.  Load nested columns with `INSERT`, which the fast path
  declines so the standard path stores them.  Disabling inlining is not a
  workaround: the utility hook routes every `COPY ... FROM STDIN` on a DuckLake
  table to the native writer, which then fails with `COPY FROM STDIN requires an
  inlined data table`.

## References

- [DuckLake Data Types Specification](https://ducklake.select/docs/stable/specification/data_types)
- Upstream type mapping: `PostgresMetadataManager::TypeIsNativelySupported` and
  `GetColumnTypeInternal` in `third_party/ducklake/src/metadata_manager/postgres_metadata_manager.cpp`
