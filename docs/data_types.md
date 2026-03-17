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
| | `uint64` | UBIGINT | Not native | VARCHAR |
| | `hugeint` | HUGEINT | Not native | VARCHAR |
| | `uhugeint` | UHUGEINT | Not native | VARCHAR |
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
| | `json` | JSON | Native | JSON |
| | `uuid` | UUID | Native | UUID |
| Nested | `list` | LIST | Not native | VARCHAR[] |
| | `struct` | STRUCT | Not native | VARCHAR |
| | `map` | MAP | Not native | VARCHAR |
| Semi-structured | `variant` | VARIANT | No inline | -- |
| Geometry | `point` | GEOMETRY | No inline | -- |
| | `linestring` | GEOMETRY | No inline | -- |
| | `polygon` | GEOMETRY | No inline | -- |
| | `multipoint` | GEOMETRY | No inline | -- |
| | `multilinestring` | GEOMETRY | No inline | -- |
| | `multipolygon` | GEOMETRY | No inline | -- |
| | `linestring z` | GEOMETRY | No inline | -- |
| | `geometrycollection` | GEOMETRY | No inline | -- |

## Inlined Data Support Categories

- **Native**: The PG column type can represent the full DuckDB value range.
  Values are stored as-is in the inlined data table.
- **Not native**: The PG column type differs from the source type.  DuckDB
  handles read/write conversion transparently; the direct insert path
  (`enable_direct_insert`) converts at the SPI boundary.
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

## References

- [DuckLake Data Types Specification](https://ducklake.select/docs/preview/specification/data_types)
- Upstream type mapping: `PostgresMetadataManager::TypeIsNativelySupported` and
  `GetColumnTypeInternal` in `third_party/ducklake/src/metadata_manager/postgres_metadata_manager.cpp`
