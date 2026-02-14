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

PostgreSQL and DuckDB headers cannot coexist in the same translation unit due to macro conflicts (`FATAL`, `ERROR`, `Min`, `Max`, etc.). The solution: **strict include ordering**.

| File | Includes postgres.h | Includes duckdb.hpp | Purpose |
|------|:---:|:---:|---------|
| `src/pgducklake.cpp` | Yes | No | PostgreSQL extension entry point |
| `src/pgducklake_table_am.cpp` | Yes | No | Table access method (declares RegisterDuckdbTableAm locally) |
| `src/pgducklake_ddl.cpp` | Yes | Yes | DDL operations (uses DuckLakeManager API, careful include order) |
| `src/pgducklake_metadata_manager.cpp` | Yes | Yes | Metadata manager (uses careful include order) |
| `src/pgducklake_pg_types.cpp` | Yes | Yes | Type conversion utilities (vendored, careful include order) |
| `src/pgducklake_duckdb.cpp` | No | Yes | DuckDB bridge (DuckLakeManager implementation) |
| `include/pgducklake/pgducklake_duckdb.hpp` | No | Yes | DuckLakeManager C++ API |

### Critical Include Order Rule

**All `.cpp` files MUST follow this include order** to avoid macro conflicts:

```cpp
// 1. DuckDB headers (if needed)
#include "duckdb/common/types.hpp"
#include "duckdb/main/connection.hpp"
// ... other DuckDB headers

// 2. DuckLake headers (if needed)
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
// ... other DuckLake headers

// 3. Local project headers
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_pg_types.hpp"
// ... other pgducklake headers

// 4. PostgreSQL headers LAST (always in extern "C" block)
extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
// ... other PostgreSQL headers
}
```

**Why this order:**
- DuckDB defines macros like `FATAL`, `ERROR` that conflict with PostgreSQL
- PostgreSQL headers must come last so their macro definitions take precedence
- `extern "C"` block prevents C++ name mangling for PostgreSQL C symbols
- Violating this order results in compilation errors or undefined behavior

**Never do this:**
```cpp
// WRONG - PostgreSQL headers before DuckDB
#include "postgres.h"
#include "duckdb.hpp"  // ERROR: macro conflicts

// WRONG - Missing extern "C"
#include "duckdb.hpp"
#include "postgres.h"  // ERROR: C++ tries to mangle C symbols
```

### Component Relationships

```
pg_ducklake.so
├── PostgreSQL-facing code (src/pgducklake*.cpp)
│   └── Implements table AM, DDL hooks, metadata operations
│   └── _PG_init():
│          1. ducklake_init_extension() — registers metadata manager
│          2. RegisterDuckdbLoadExtension(ducklake_load_extension) — deferred loading
│
├── DuckDB bridge (src/pgducklake_duckdb.cpp)
│   └── ducklake_init_extension():
│          DuckLakeMetadataManager::Register("pgducklake", ...)
│   └── ducklake_load_extension() (called by pg_duckdb when DuckDB is ready):
│          GetDuckDBDatabase()->LoadStaticExtension<DucklakeExtension>()
│   └── DuckLakeManager: high-level C++ API for DuckDB operations
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

4. **Deferred Static Extension Loading**: During `_PG_init()`, the metadata manager is registered eagerly, and a callback is registered via pg_duckdb's `RegisterDuckdbLoadExtension()`. The actual `LoadStaticExtension<DucklakeExtension>()` call is deferred until pg_duckdb's `DuckDBManager::Initialize()` runs, ensuring the DuckDB instance is fully configured before extensions are loaded.

5. **Mixed-Write Detection Bypass (`UnsafeCommandIdGuard`)**: pg_duckdb prevents writing to both PostgreSQL and DuckDB tables in the same transaction by tracking `next_expected_command_id`. DuckLake metadata operations (SPI writes to `ducklake_*` tables, DDL via `raw_query()`) falsely trigger this detection because they advance PostgreSQL's command counter as a side effect. The `UnsafeCommandIdGuard` RAII class in `include/pgducklake/utility/unsafe_command_id_guard.hpp` synchronizes pg_duckdb's counter with PostgreSQL's current command ID on construction and destruction, making internal metadata writes invisible to the check. Used in:
   - `CreateSPIResult()` in `src/pgducklake_metadata_manager.cpp` (metadata query execution)
   - `ducklake_create_table_trigger()` in `src/pgducklake_ddl.cpp` (DDL via `raw_query()`)
   - `ducklake_drop_trigger()` in `src/pgducklake_ddl.cpp` (DDL via `raw_query()`)

   The guard includes `postgres.h` and `access/xact.h`, so it must be included AFTER DuckDB/DuckLake headers (same positioning as `cpp_wrapper.hpp`).

6. **Vendored Type Conversion**: Rather than linking against pg_duckdb's type conversion utilities (which creates dependency cascades and header conflicts), we vendor minimal type conversion code. The vendored utilities in `src/pgducklake_pg_types.cpp` provide:
   - `DetoastPostgresDatum()` - Detoasts PostgreSQL varlena values
   - `ConvertPostgresToDuckValue()` - Converts PostgreSQL Datum to DuckDB Vector
   - `ConvertPostgresToDuckColumnType()` - Maps PostgreSQL types to DuckDB LogicalTypes

   This approach avoids:
   - Dependency cascades (pg_duckdb object files pull in many transitive dependencies)
   - Header conflicts (pg_duckdb headers use guards preventing mixing with PostgreSQL headers)
   - Symbol resolution complexity
   - Build fragility

   The vendored code (~300 lines) is well-understood type mapping logic that rarely changes.

## File Structure

### Core Implementation
- `src/pgducklake.cpp` - Extension initialization (`_PG_init`)
- `src/pgducklake_duckdb.cpp` - DuckDB bridge (DuckLakeManager implementation)
- `src/pgducklake_table_am.cpp` - Table access method handler (`CREATE ACCESS METHOD ducklake`)
- `src/pgducklake_metadata_manager.cpp` - Custom metadata manager using PostgreSQL SPI
- `src/pgducklake_ddl.cpp` - DDL event trigger handlers (CREATE/DROP TABLE)
- `src/pgducklake_pg_types.cpp` - Vendored type conversion utilities (PostgreSQL ↔ DuckDB)

### Headers
- `include/pgducklake/pgducklake_duckdb.hpp` - DuckLakeManager C++ API for DuckDB operations
- `include/pgducklake/pgducklake_metadata_manager.hpp` - Metadata manager interface
- `include/pgducklake/pgducklake_pg_types.hpp` - Type conversion utilities (vendored from pg_duckdb)
- `include/pgducklake/pgducklake_defs.hpp` - Shared constants
- `include/pgducklake/utility/cpp_wrapper.hpp` - PostgreSQL/C++ interop utilities
- `include/pgducklake/utility/unsafe_command_id_guard.hpp` - RAII guard to suppress pg_duckdb mixed-write detection for metadata operations

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

## Upstream Submodule Strategy (`third_party/pg_duckdb`)

`third_party/pg_duckdb` is an **upstream project** — we do not own it and want to minimize conflicts with upstream development. When pg_ducklake needs new capabilities from pg_duckdb, the approach is to **add hooks** rather than modify existing logic.

### Principles

1. **No invasive changes**: Never modify existing pg_duckdb functions or control flow. Add new, self-contained hook points instead.
2. **Additive only**: New code should be small, isolated additions (a registration function + a callback loop) that don't touch existing behavior.
3. **Upstream-friendly**: Design hooks so they could plausibly be upstreamed. Use `extern "C"` with `visibility("default")` so external extensions can consume them.
4. **Zero impact when unused**: If no extensions register callbacks, the hook is a no-op (empty vector iteration).

### Current Hooks

**`RegisterDuckdbLoadExtension(callback)`** (in `src/pgduckdb_duckdb.cpp`):
- Allows external extensions to register a loader function that pg_duckdb calls during `DuckDBManager::Initialize()`
- Callbacks are stored in a static `std::vector<DuckDBLoadExtension>` and invoked after the DuckDB instance is fully configured
- pg_ducklake uses this to defer `LoadStaticExtension<DucklakeExtension>()` until the DuckDB instance is ready
- Added as ~15 lines: a typedef, a vector, a registration function, and a for-loop in `Initialize()`

**`DuckdbUnsafeGetNextExpectedCommandId()` / `DuckdbUnsafeSetNextExpectedCommandId(uint32_t)`** (in `src/pgduckdb_xact.cpp`):
- Exposes get/set access to pg_duckdb's `next_expected_command_id` static variable
- Used by pg_ducklake's `UnsafeCommandIdGuard` to suppress false mixed-write detection during DuckLake metadata operations
- Named "Unsafe" because misuse can mask genuine mixed-write violations
- Added as ~20 lines: two `extern "C"` functions with `visibility("default")` after the `pgduckdb` namespace

### How to Add New Hooks

When pg_ducklake needs something new from pg_duckdb:

1. Identify the minimal hook point needed (e.g., "call me after X happens")
2. Add a registration function (`RegisterDuckdb<HookName>`) with `extern "C"` visibility in the relevant pg_duckdb source file
3. Store callbacks in a static container and invoke them at the appropriate point
4. In pg_ducklake, declare the registration function with `extern "C"` and call it during `_PG_init()`
5. Keep the pg_duckdb diff as small as possible for easy rebasing against upstream

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
   - Edit `src/pgducklake_duckdb.cpp`
   - This includes DuckDB/DuckLake headers **only**
   - Never include PostgreSQL headers here

3. **Metadata manager changes** (query routing, type conversion):
   - Edit `src/pgducklake_metadata_manager.cpp`
   - **CRITICAL**: Follow the include order rule (DuckDB → DuckLake → Local → PostgreSQL)
   - Uses vendored type conversion from `pgducklake/pgducklake_pg_types.hpp`

4. **Type conversion utilities** (PostgreSQL ↔ DuckDB type mapping):
   - Edit `src/pgducklake_pg_types.cpp`
   - **CRITICAL**: Follow the include order rule (DuckDB → DuckLake → Local → PostgreSQL)
   - Vendored from pg_duckdb to avoid dependency issues
   - Supports common types: integers, floats, text, dates, timestamps, UUID, JSON, arrays

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

1. **Wrong include order**: PostgreSQL headers MUST come last (after DuckDB/DuckLake/local headers). Violating the include order causes macro conflicts (`FATAL`, `ERROR`, `Min`, `Max`). See "Critical Include Order Rule" above.

2. **Forgetting to build ducklake static library**: Run `make ducklake` before `make`. The Makefile has dependencies but sometimes manual intervention helps.

3. **DuckDB version mismatch**: If pg_duckdb and ducklake target different DuckDB versions, you'll get runtime crashes or linker errors. Check submodule commits.

4. **Missing shared_preload_libraries**: pg_duckdb must be loaded at server start via `shared_preload_libraries = 'pg_duckdb'` in postgresql.conf. Otherwise `GetDuckDBDatabase()` will fail.

5. **Not force-loading DuckLake**: Without `-Wl,-force_load` (macOS) or `--whole-archive` (Linux), the linker drops unreferenced DuckLake symbols, causing `LoadStaticExtension<T>()` to fail.

6. **Trying to link pg_duckdb utilities directly**: Don't try to link against pg_duckdb's `.o` files for type conversion. This creates dependency cascades (pgduckdb_types.o → pgduckdb_guc.o → DuckDBManager → ...). Use the vendored utilities in `src/pgducklake_pg_types.cpp` instead.

## SQL Functions and Features

### Initialization

**`ducklake._initialize()`**
- Called automatically during `CREATE EXTENSION pg_ducklake`
- Creates data directory: `$DATADIR/pg_ducklake`
- Attaches DuckLake catalog: `ATTACH 'ducklake:pgducklake:' AS pgducklake`
- Configured with metadata schema and data path
- Can only be called during extension creation

### DDL Event Triggers

**`ducklake._create_table_trigger()`**
- Event trigger on CREATE TABLE / CREATE TABLE AS
- Auto-detects tables using ducklake access method
- Generates CREATE TABLE DDL from PostgreSQL table definition
- Creates corresponding DuckDB table in `pgducklake.schema.table` namespace
- Maps PostgreSQL types to DuckDB types using `format_type_with_typemod()`
- Supports:
  - Basic column types (int, text, numeric, timestamp, varchar, etc.)
  - NOT NULL constraints
  - Multi-column tables
- Does not yet support:
  - DEFAULT values
  - CHECK constraints
  - UNIQUE/PRIMARY KEY constraints
  - Foreign keys (explicitly rejected)
  - Temporary tables (explicitly rejected)

**`ducklake._drop_trigger()`**
- Event trigger on SQL DROP
- Queries DuckLake metadata to find corresponding DuckDB tables
- Executes `DROP TABLE IF EXISTS` in DuckDB
- Logs warnings if drop fails (doesn't block PostgreSQL DROP)

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

**Available tests:**
- `initialization` - Tests extension creation, schema setup, and basic table creation
- `ddl_triggers` - Tests automatic CREATE/DROP table triggers, data insertion/selection
- `basic` - Basic DML operations

## Implementation Notes

### DDL Trigger Architecture

The DDL trigger implementation follows these principles:

1. **No tight coupling to pg_duckdb**: Uses only `DuckLakeManager::ExecuteQuery()` API
2. **PostgreSQL catalog introspection**: Reads table structure from `pg_class`, `pg_attribute`, `pg_type`
3. **Type mapping**: Uses PostgreSQL's `format_type_with_typemod()` which produces types DuckDB understands
4. **Event trigger pattern**: Standard PostgreSQL event trigger using `pg_event_trigger_ddl_commands()` and `pg_event_trigger_dropped_objects()`

### Table Definition Generation

The `GenerateCreateTableDDL()` function:
1. Opens the relation with `AccessShareLock`
2. Builds qualified name: `pgducklake.schema.table`
3. Iterates `TupleDesc` to extract columns
4. Maps each column using `format_type_with_typemod()`
5. Adds NOT NULL constraints where applicable
6. Returns DDL string for execution

This approach avoids dependency on pg_duckdb's internal table definition parsing and keeps the code simple and maintainable.

### Mixed-Write Detection and `UnsafeCommandIdGuard`

pg_duckdb prevents writing to both PostgreSQL and DuckDB tables within the same transaction block. It does this by tracking `next_expected_command_id` in `pgduckdb_xact.cpp`: each DuckDB write claims a PostgreSQL command ID, and if the counter advances beyond the expected value, it means PostgreSQL also wrote (triggering the error).

**The problem for pg_ducklake:** DuckLake metadata operations inherently write to both systems:
- DDL triggers: PostgreSQL creates the table (advancing the command counter), then the trigger calls `duckdb.raw_query()` to create the corresponding DuckDB table
- Metadata manager: SPI executes INSERT/UPDATE/DELETE on `ducklake_*` PostgreSQL tables as part of DuckDB transactions

These are logically part of DuckDB operations, but pg_duckdb sees them as PostgreSQL writes.

**The solution:** `UnsafeCommandIdGuard` (in `include/pgducklake/utility/unsafe_command_id_guard.hpp`):
- **Constructor**: Sets `next_expected_command_id = GetCurrentCommandId(false)`, absorbing any prior PostgreSQL writes (e.g., DDL that advanced the counter before the trigger fires)
- **Destructor**: Does the same, absorbing any writes during the guard's scope (e.g., SPI metadata INSERTs/UPDATEs)
- **Error safety**: If a PostgreSQL `ereport(ERROR)` longjmps past the destructor, pg_duckdb's transaction abort handler (`XACT_EVENT_ABORT`) resets `next_expected_command_id = FirstCommandId`, so no stale state leaks

**Usage pattern:**
```cpp
{
    pgducklake::UnsafeCommandIdGuard command_id_guard;
    // PostgreSQL writes here won't trigger mixed-write detection
    ExecuteDuckDBQuery(ddl, &error_msg);
}
```

The hooks (`DuckdbUnsafeGetNextExpectedCommandId/Set`) are exported from pg_duckdb's `pgduckdb_xact.cpp` as `extern "C"` with `visibility("default")`, following the same pattern as `RegisterDuckdbLoadExtension`.

### Vendored Type Conversion

The metadata manager needs to convert PostgreSQL data to DuckDB format when executing metadata queries through SPI. Rather than depending on pg_duckdb's type conversion utilities (which would create build complexity and dependency cascades), we vendor minimal type conversion code.

**Why vendoring instead of linking:**
1. **Header conflicts**: pg_duckdb headers include `cpp_only_file.hpp` which statically asserts that `postgres.h` is not included. The metadata manager needs both.
2. **Dependency cascade**: Linking `pgduckdb_types.o` requires `pgduckdb_guc.o`, which requires `DuckDBManager`, which requires more dependencies...
3. **Symbol visibility**: pg_duckdb uses `-fvisibility=hidden` for C++ symbols, making direct linking fragile.
4. **Build simplicity**: Vendoring ~300 lines of well-understood type mapping code is simpler than managing complex linking dependencies.

**Vendored functions** (in `src/pgducklake_pg_types.cpp`):
- `DetoastPostgresDatum()` - Handles PostgreSQL TOAST (The Oversized-Attribute Storage Technique)
  - Expands compressed values
  - Fetches externally stored values
  - Handles short varlena format
  - Returns detoasted value and whether caller should free it

- `ConvertPostgresToDuckValue()` - Converts PostgreSQL Datum to DuckDB Vector
  - Supports: bool, int2/4/8, float4/8, numeric, text/varchar, date, timestamp, UUID, JSON
  - Handles PostgreSQL epoch offset (2000-01-01 vs DuckDB's 1970-01-01)
  - Falls back to string representation for unsupported types

- `ConvertPostgresToDuckColumnType()` - Maps PostgreSQL column types to DuckDB LogicalType
  - Introspects PostgreSQL type system using `get_element_type()`
  - Maps arrays to DuckDB LIST types
  - Handles multi-dimensional arrays via `attndims`

**The Critical Include Order Rule applies** (see Architecture section above):
1. DuckDB headers first (defines types like `duckdb::Vector`, `duckdb::LogicalType`)
2. DuckLake headers (if needed)
3. Local pgducklake headers (including `pgducklake/pgducklake_pg_types.hpp`)
4. PostgreSQL headers LAST in `extern "C"` block

This ordering ensures:
- DuckDB types are defined before our conversion functions need them
- PostgreSQL headers come after everything else to avoid macro pollution
- No pg_duckdb headers are included (avoiding the `cpp_only_file.hpp` assertion)
- All C symbols from PostgreSQL are correctly handled via `extern "C"`

## Key Technical Details

### DuckLakeManager API

The `DuckLakeManager` class provides a high-level C++ interface for DuckDB/DuckLake operations:

```cpp
namespace pgducklake {
class DuckLakeManager {
public:
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
- DuckLake extension loading is deferred via `RegisterDuckdbLoadExtension()` callback
- Defined in `include/pgducklake/pgducklake_duckdb.hpp`, implemented in `src/pgducklake_duckdb.cpp`

**Extension Lifecycle (Deferred Loading):**

Extension initialization uses a two-phase approach to handle the timing constraint that `_PG_init()` runs before the DuckDB instance is fully initialized:

```
1. LOAD 'pg_duckdb'              (via shared_preload_libraries)
2. CREATE EXTENSION pg_ducklake  → _PG_init():
   a. ducklake_init_extension()  → Registers PgDuckLakeMetadataManager (no DuckDB instance needed)
   b. RegisterDuckdbLoadExtension(ducklake_load_extension)  → Registers callback with pg_duckdb
3. First query using DuckDB      → DuckDBManager::Initialize():
   a. DuckDB instance fully configured
   b. pg_duckdb invokes all registered callbacks
   c. ducklake_load_extension() → db.LoadStaticExtension<DucklakeExtension>()
4. DuckLake is now registered with DuckDB's ExtensionManager
```

**Why deferred loading:**
- `_PG_init()` runs when the shared library is loaded, but `GetDuckDBDatabase()` may not return a fully initialized instance yet
- The metadata manager registration (`DuckLakeMetadataManager::Register`) is safe to call eagerly because it doesn't need a DuckDB instance
- The static extension loading (`LoadStaticExtension<T>()`) requires a fully initialized DuckDB instance, so it's deferred via the callback mechanism
- pg_duckdb's `RegisterDuckdbLoadExtension()` stores the callback and invokes it during `DuckDBManager::Initialize()`, after all DuckDB configuration is applied

**Key functions:**
- `ducklake_init_extension()` — Called once during `_PG_init()`. Registers the custom metadata manager globally.
- `ducklake_load_extension()` — Called by pg_duckdb during `DuckDBManager::Initialize()`. Loads the DuckLake static extension into the DuckDB instance.

### Symbol Visibility

pg_duckdb exports a minimal C interface:
- `GetDuckDBDatabase()` - Returns `duckdb::DuckDB*` as `void*`
- `RegisterDuckdbTableAm()` - Registers custom table access methods
- `RegisterDuckdbLoadExtension()` - Registers a callback to be invoked when a new DuckDB backend is created (used for deferred extension loading)
- `DuckdbUnsafeGetNextExpectedCommandId()` - Returns pg_duckdb's `next_expected_command_id` for mixed-write detection bypass
- `DuckdbUnsafeSetNextExpectedCommandId(uint32_t)` - Sets pg_duckdb's `next_expected_command_id`

All C++ symbols in the `pgduckdb::` namespace are hidden (`-fvisibility=hidden`). This extension:
1. Imports `GetDuckDBDatabase()` in the DuckDB-facing translation unit (`src/pgducklake_duckdb.cpp`)
2. Declares `RegisterDuckdbTableAm()` locally in the one file that needs it (`src/pgducklake_table_am.cpp`)
3. Declares `RegisterDuckdbLoadExtension()` in `src/pgducklake.cpp` to register the deferred loader
4. Declares `DuckdbUnsafeGetNextExpectedCommandId/Set` in `include/pgducklake/utility/unsafe_command_id_guard.hpp` for the mixed-write guard
5. Wraps `GetDuckDBDatabase()` with `DuckLakeManager` to provide type-safe, high-level APIs
6. Never exposes raw pg_duckdb symbols via headers
