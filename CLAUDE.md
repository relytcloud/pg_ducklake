# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_ducklake is a PostgreSQL extension that integrates DuckLake (a lakehouse table format for DuckDB) with PostgreSQL via pg_duckdb. It uses the **statically embedded extension pattern**, where DuckLake is compiled directly into the PostgreSQL extension and registered using DuckDB's `LoadStaticExtension<T>()` API.

### Why This Approach?

There are multiple ways to integrate DuckDB extensions with PostgreSQL extensions built on pg_duckdb:

1. **SQL Interface** (simplest): Use `duckdb.raw_query()` to execute DuckDB queries via PostgreSQL's SPI. No C++ API access.
2. **Static Compilation into DuckDB**: Compile DuckLake into DuckDB itself by modifying pg_duckdb's build. Requires rebuilding pg_duckdb.
3. **Embedded Extension** (this project): Compile DuckLake into the PostgreSQL extension and register it at runtime with DuckDB's `LoadStaticExtension<T>()`.

This project chose the embedded approach because:
- **Full C++ API access** to DuckLake internals (can call `DuckLakeMetadataManager::Register` and other symbols)
- **No pg_duckdb rebuild required** (unlike static compilation into DuckDB)
- **No internet dependency** (unlike installing from DuckDB community repository)
- **Clean separation** via translation unit isolation avoids PostgreSQL/DuckDB header conflicts

## Build Commands

### Prerequisites
- PostgreSQL 14-18 installed (14+ required, 18 max supported)
- C++17 compatible compiler
- Git submodules initialized

### Standard Build Workflow

```bash
# Initialize submodules
git submodule update --init --recursive

# Build and install pg_duckdb first (required dependency)
make -C third_party/pg_duckdb PG_CONFIG=$PWD/pg-17/bin/pg_config install -j4

# Build DuckLake static library
make ducklake PG_CONFIG=$PWD/pg-17/bin/pg_config

# Build the extension
make PG_CONFIG=$PWD/pg-17/bin/pg_config

# Install the extension
make install PG_CONFIG=$PWD/pg-17/bin/pg_config
```

### Testing

```bash
# Run all regression tests
make check-regression PG_CONFIG=$PWD/pg-17/bin/pg_config

# Run a single test
make check-regression TEST=basic PG_CONFIG=$PWD/pg-17/bin/pg_config

# Clean test artifacts
make clean-regression
```

### Clean Builds

```bash
# Clean extension build artifacts
make clean

# Clean DuckLake build artifacts
make clean-ducklake

# Clean regression test output
make clean-regression
```

## Architecture

### The Split Translation Unit Pattern

PostgreSQL and DuckDB headers cannot coexist in the same translation unit due to macro conflicts (`FATAL`, `ERROR`, `Min`, `Max`, etc.). The solution: **never mix them**.

| File | Includes postgres.h | Includes duckdb.hpp | Purpose |
|------|:---:|:---:|---------|
| `src/pgducklake.cpp` | Yes | No | PostgreSQL extension entry point |
| `src/pgducklake_table_am.cpp` | Yes | No | Table access method implementation |
| `src/pgducklake_ddl.cpp` | Yes | Yes | DDL operations (uses DuckLakeManager API, careful include order) |
| `src/pgducklake_metadata_manager.cpp` | Yes | Yes | Metadata manager (uses careful include order) |
| `src/pgducklake_duckdb.cpp` | No | Yes | DuckDB bridge (DuckLakeManager implementation) |
| `include/pgducklake/pgduckdb.h` | No | No | pg_duckdb C interface (RegisterDuckdbTableAm) |
| `include/pgducklake/pgducklake_duckdb.hpp` | No | Yes | DuckLakeManager C++ API |

### Component Relationships

```
pg_ducklake.so
├── PostgreSQL-facing code (src/pgducklake*.cpp)
│   └── Implements table AM, DDL hooks, metadata operations
│   └── Calls ducklake_ensure_loaded() via extern "C"
│
├── DuckDB bridge (src/pg_ducklake_duckdb.cpp)
│   └── ducklake_ensure_loaded():
│          GetDuckDBDatabase()->LoadStaticExtension<DucklakeExtension>()
│
└── DuckLake static library (linked via -Wl,-force_load)
    └── All DuckLake implementation (49 .cpp files from third_party/ducklake/src/)
```

### Key Design Patterns

1. **Custom Metadata Manager**: `PgDuckLakeMetadataManager` inherits from `duckdb::DuckLakeMetadataManager` and overrides `Execute()` and `Query()` to run metadata queries through PostgreSQL's SPI instead of DuckDB's query engine. This is critical because:
   - DuckLake needs to query table metadata (schemas, columns, snapshots, tags)
   - This metadata lives in PostgreSQL's system catalogs, not DuckDB
   - By routing queries through SPI, DuckLake operations see the same catalog state as PostgreSQL
   - The metadata manager also handles syntax translation (DuckDB LIST/STRUCT → PostgreSQL arrays/composite types)

2. **Table Access Method**: Implements PostgreSQL's Table AM API to register "ducklake" as a storage engine via `CREATE ACCESS METHOD ducklake TYPE TABLE HANDLER ducklake._am_handler`. This allows users to write `CREATE TABLE t (...) USING ducklake`.

3. **Dynamic Symbol Resolution**: Uses `-Wl,-undefined,dynamic_lookup` (macOS) to resolve pg_duckdb symbols at runtime. pg_duckdb must be loaded via `shared_preload_libraries`.

4. **Static Extension Loading**: Uses DuckDB's `LoadStaticExtension<DucklakeExtension>()` to register DuckLake directly into pg_duckdb's DuckDB instance, accessed via `GetDuckDBDatabase()`.

## File Structure

### Core Implementation
- `src/pgducklake.cpp` - Extension initialization (`_PG_init`)
- `src/pgducklake_duckdb.cpp` - DuckDB bridge (DuckLakeManager implementation)
- `src/pgducklake_table_am.cpp` - Table access method handler (`CREATE ACCESS METHOD ducklake`)
- `src/pgducklake_metadata_manager.cpp` - Custom metadata manager using PostgreSQL SPI
- `src/pgducklake_ddl.cpp` - DDL event trigger handlers (CREATE/DROP TABLE)

### Headers
- `include/pgducklake/pgduckdb.h` - pg_duckdb C interface (`RegisterDuckdbTableAm`)
- `include/pgducklake/pgducklake_duckdb.hpp` - DuckLakeManager C++ API for DuckDB operations
- `include/pgducklake/pgducklake_metadata_manager.hpp` - Metadata manager interface
- `include/pgducklake/pgducklake_defs.hpp` - Shared constants
- `include/pgducklake/utility/cpp_wrapper.hpp` - PostgreSQL/C++ interop utilities

### Configuration
- `pg_ducklake.control` - Extension metadata (requires='pg_duckdb')
- `sql/pg_ducklake--0.1.0.sql` - Extension installation SQL (creates ducklake schema and access method)
- `Makefile` - PGXS-based build system
- `Makefile.global` - Shared compiler flags and PostgreSQL version checks

### Testing
- `test/regression/sql/` - Test SQL files
- `test/regression/expected/` - Expected test output
- `test/regression/schedule` - Test execution order
- `test/regression/regression.conf` - PostgreSQL configuration for tests (enables pg_duckdb)

## Important Build Requirements

### Hard Requirements

| Requirement | Why |
|-------------|-----|
| **ABI compatibility** | DuckLake must compile against the **exact same DuckDB headers** as pg_duckdb. Version mismatch = undefined behavior. |
| **pg_duckdb built first** | Need libduckdb and pg_duckdb's headers available before building pg_ducklake. |
| **Submodule versions pinned** | `third_party/pg_duckdb` and `third_party/ducklake` must target the same DuckDB version. |
| **Force-load DuckLake** | Use `-Wl,-force_load` (macOS) or `--whole-archive` (Linux) to ensure all DuckLake symbols are available to `LoadStaticExtension<T>()`. |

### Compiler Flags

```makefile
# PostgreSQL-facing files
PG_CPPFLAGS += -I$(CURDIR)/include $(DUCKDB_INCLUDES)
PG_CXXFLAGS += -std=c++17

# DuckDB-facing files (pg_ducklake_duckdb.cpp)
DUCKDB_CXXFLAGS = -std=c++17 -fPIC

# Linker flags
SHLIB_LINK += -Wl,-force_load,$(DUCKLAKE_STATIC_LIB)  # macOS
SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lduckdb -lstdc++
SHLIB_LINK += -Wl,-undefined,dynamic_lookup  # macOS
```

## Development Workflow

### Making Changes

1. **PostgreSQL-side changes** (table AM, DDL, catalog operations):
   - Edit `src/pgducklake*.cpp` files
   - These include PostgreSQL headers
   - Use PostgreSQL APIs (SPI, catalog access, etc.)

2. **DuckDB-side changes** (DuckLake registration, query execution):
   - Edit `src/pg_ducklake_duckdb.cpp`
   - This includes DuckDB/DuckLake headers **only**
   - Never include PostgreSQL headers here

3. **Metadata manager changes** (query routing, type conversion):
   - Edit `src/pgducklake_metadata_manager.cpp`
   - This carefully includes both PostgreSQL and DuckDB headers
   - DuckDB headers must come **before** PostgreSQL headers to avoid macro conflicts

### Adding New Functionality

When adding new DuckLake operations:

1. **If using DuckDB C++ API directly** (from DuckDB-only code):
   - Add methods to `DuckLakeManager` class in `include/pgducklake/pgducklake_duckdb.hpp`
   - Implement in `src/pgducklake_duckdb.cpp` (DuckDB-only TU)
   - Call from other DuckDB-facing code via `DuckLakeManager::`

2. **If calling DuckDB from PostgreSQL-facing code**:
   - Include `pgducklake/pgducklake_duckdb.hpp` in your file (requires DuckDB headers first)
   - Use `DuckLakeManager::GetDatabase()` or `DuckLakeManager::ExecuteQuery()`
   - See `src/pgducklake_ddl.cpp` for example (includes DuckDB headers before PostgreSQL)

3. **If using PostgreSQL SPI to run DuckDB queries**:
   - Add to `PgDuckLakeMetadataManager::Execute()` or `::Query()`
   - This runs queries through PostgreSQL's executor

### Common Pitfalls

1. **Including both postgres.h and duckdb.hpp in the same TU**: Results in macro conflicts. Use separate translation units.

2. **Forgetting to build ducklake static library**: Run `make ducklake` before `make`. The Makefile has dependencies but sometimes manual intervention helps.

3. **DuckDB version mismatch**: If pg_duckdb and ducklake target different DuckDB versions, you'll get runtime crashes or linker errors. Check submodule commits.

4. **Missing shared_preload_libraries**: pg_duckdb must be loaded at server start via `shared_preload_libraries = 'pg_duckdb'` in postgresql.conf. Otherwise `GetDuckDBDatabase()` will fail.

5. **Not force-loading DuckLake**: Without `-Wl,-force_load` (macOS) or `--whole-archive` (Linux), the linker drops unreferenced DuckLake symbols, causing `LoadStaticExtension<T>()` to fail.

## Testing Strategy

Tests use PostgreSQL's regression test framework (`pg_regress`). Each test:

1. Creates tables using `CREATE TABLE ... USING ducklake`
2. Performs INSERT/SELECT/UPDATE/DELETE operations
3. Verifies results match expected output

Test execution:
- Spins up a temporary PostgreSQL instance in `test/regression/tmp_check/`
- Loads pg_duckdb and pg_ducklake extensions
- Runs tests in schedule order
- Compares output with `test/regression/expected/*.out`

## Key Technical Details

### DuckLakeManager API

The `DuckLakeManager` class provides a high-level C++ interface for DuckDB/DuckLake operations:

```cpp
namespace pgducklake {
class DuckLakeManager {
public:
    // Ensure DuckLake extension is loaded (idempotent)
    static void EnsureLoaded();

    // Get reference to pg_duckdb's DuckDB database instance
    static duckdb::DuckDB& GetDatabase();

    // Create a new connection to the DuckDB database
    static duckdb::unique_ptr<duckdb::Connection> CreateConnection();

    // Execute a query and return success/error
    static int ExecuteQuery(const char* query, const char** errmsg_out);
};
}
```

**Implementation details:**
- Wraps pg_duckdb's low-level `GetDuckDBDatabase()` C function (returns `void*`)
- `EnsureLoaded()` uses DuckDB's `LoadStaticExtension<DucklakeExtension>()` template
- Tracks loaded state to make `EnsureLoaded()` idempotent
- Defined in `include/pgducklake/pgducklake_duckdb.hpp`, implemented in `src/pgducklake_duckdb.cpp`

### Symbol Visibility

pg_duckdb exports a minimal C interface:
- `GetDuckDBDatabase()` - Returns `duckdb::DuckDB*` as `void*`
- `RegisterDuckdbTableAm()` - Registers custom table access methods

All C++ symbols in the `pgduckdb::` namespace are hidden (`-fvisibility=hidden`). This extension:
1. Imports `GetDuckDBDatabase()` in the DuckDB-facing translation unit
2. Wraps it with `DuckLakeManager` to provide type-safe, high-level APIs
3. Never exposes raw `GetDuckDBDatabase()` to other code
