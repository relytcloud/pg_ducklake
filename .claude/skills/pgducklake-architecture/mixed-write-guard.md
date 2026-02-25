# Mixed-Write Guard

`pg_duckdb` tracks command IDs to block mixed PostgreSQL/DuckDB writes in one transaction. DuckLake metadata flows can look like mixed writes even when they are internal extension operations.

`UnsafeCommandIdGuard` synchronizes pg_duckdb's expected command ID around these operations.

Use it in code paths where internal SPI writes or DDL-triggered DuckDB writes would otherwise trip detection.

Known use sites:

- metadata query execution path (@src/pgducklake_metadata_manager.cpp)
- create/drop trigger paths (@src/pgducklake_ddl.cpp)

Do not remove guard usage without verifying transaction-safety and mixed-write behavior end-to-end.
