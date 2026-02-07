# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_ducklake is a PostgreSQL extension that brings native datalake capabilities into PostgreSQL, powered by DuckLake (a DuckDB lakehouse format). It enables creating/writing/querying DuckLake tables via SQL, with tables stored in columnar Parquet format and queryable from both PostgreSQL and DuckDB clients.

This project is forked from [pg_duckdb](https://github.com/duckdb/pg_duckdb) and integrates with [ducklake](https://github.com/duckdb/ducklake).

## Build Commands

```bash
# Standard build (dynamically linked)
make -j$(nproc)
make install

# Debug build (recommended for development)
DUCKDB_BUILD=Debug make -j$(nproc)
DUCKDB_BUILD=Debug make install

# Static build
DUCKDB_BUILD=ReleaseStatic make install

# Specific PostgreSQL version
PG_CONFIG=/path/to/pg_config make install

# Clean builds
make clean          # Clean pg_duckdb only
make clean-all      # Clean everything including libduckdb

# Format code (run before committing)
make format

# Lint check
make lintcheck
```

## Testing

```bash
# Run all tests
make check

# Run only PostgreSQL regression tests (pg_duckdb)
make check-regression-duckdb

# Run only DuckLake regression tests
make check-regression-ducklake

# Run only Python tests
make pycheck

# Run a specific regression test
# Edit test/regression/schedule or test/regression_ducklake/schedule
# to comment out tests you want to skip, then run the corresponding check command
```

Regression tests are in `test/regression/` (pg_duckdb) and `test/regression_ducklake/` (DuckLake-specific). Test cases are `.sql` files in `sql/` subdirectories with expected output in `expected/`.

## Architecture

### Extension Entry Point
- `src/pgduckdb.cpp`: `_PG_init()` initializes the extension via `shared_preload_libraries`

### PostgreSQL Hook System
- `src/pgduckdb_hooks.cpp`: Installs hooks for planner, executor, and explain
- Queries involving DuckDB/DuckLake tables are routed to DuckDB execution

### Query Execution Flow
1. **Planner hook** (`src/pgduckdb_planner.cpp`): Converts PostgreSQL Query to DuckDB prepared statement
2. **Custom scan node** (`src/pgduckdb_node.cpp`): Executes queries via DuckDB engine
3. **Type conversion** (`src/pgduckdb_types.cpp`): Maps between PostgreSQL and DuckDB types

### DuckDB Integration
- `src/pgduckdb_duckdb.cpp`: `DuckDBManager` singleton manages the DuckDB instance
- `src/catalog/`: Bridge between PostgreSQL catalog and DuckDB catalog

### Table Access Methods
- `src/pgduckdb_table_am.cpp`: General DuckDB table access method
- `src/ducklake/pgducklake_table_am.cpp`: DuckLake-specific table access method (for `USING ducklake`)

### Key Directories
- `src/pg/`: PostgreSQL-specific utilities (GUC, transactions, memory, permissions)
- `src/scan/`: PostgreSQL table scanning for hybrid queries
- `src/ducklake/`: DuckLake-specific functionality (DDL, metadata management)
- `third_party/duckdb/`: DuckDB submodule
- `third_party/ducklake/`: DuckLake extension submodule

## Contributing Guidelines

### CRITICAL: Code Organization and Upstream Compatibility

**This project is forked from pg_duckdb. All modifications must follow these rules:**

1. **Minimize pg_duckdb modifications**: Changes to existing pg_duckdb code must be **minimal and easy to upstream**
   - Only modify pg_duckdb files when absolutely necessary for integration
   - Keep modifications small, isolated, and well-documented
   - Consider whether changes could benefit the upstream project

2. **Separate pg_ducklake code**: pg_ducklake-specific code must **ALWAYS** be in dedicated locations:
   - `src/ducklake/` - DuckLake-specific source files
   - `include/pgduckdb/ducklake/` - DuckLake-specific headers
   - `test/regression_ducklake/` - DuckLake-specific regression tests
   - **Never mix DuckLake-specific logic into general pg_duckdb files**

3. **Prefer new files over modifications**: When adding functionality, create new files in `src/ducklake/` rather than modifying existing pg_duckdb files

## Code Style

### Error Handling (Critical)
- **Before DuckDB execution**: Use `elog(ERROR, ...)`. No C++ exceptions.
- **Inside DuckDB execution**: Use exceptions. Never use `elog(ERROR, ...)`.
- Use `PostgresFunctionGuard` when calling PostgreSQL functions from within DuckDB execution.

### C++ (DuckDB integration)
- C++17 standard
- All functions in `src/` should be in the `pgduckdb` namespace
- Use smart pointers (`unique_ptr` preferred over `shared_ptr`), never raw `new`/`delete`
- Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, etc.
- Use `idx_t` for indices/counts instead of `size_t`

### C (PostgreSQL integration)
- Use `palloc`/`palloc0` for memory allocation
- CamelCase for functions, snake_case for variables
- For SQL function implementations, use snake_case

### Formatting
- Tabs for indentation, spaces for alignment
- Max 120 columns
- Run `make format` before committing
