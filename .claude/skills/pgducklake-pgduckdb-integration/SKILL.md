---
name: pgducklake-pgduckdb-integration
description: Integration contract with upstream pg_duckdb. Use when adding/changing hooks, C exports, or extension load integration points.
user-invocable: false
---

Use this skill when tasks involve `third_party/pg_duckdb` interactions.

## Priority rules

1. Treat `third_party/pg_duckdb` as upstream.
2. Prefer additive hook points over invasive control-flow edits.
3. Keep no-op behavior when callbacks are not registered.

## Hook Strategy

Preferred extension model:

1. Add registration function (`RegisterDuckdb...`) in pg_duckdb.
2. Store callback in static container.
3. Invoke callbacks at a stable lifecycle point.
4. Keep behavior unchanged when no callback exists.

Example:

```cpp
static duckdb::unordered_map<const TableAmRoutine * /*am*/, duckdb::string /*name*/> duckdb_table_ams = {
    {&duckdb_methods, "duckdb"}};

extern "C" __attribute__((visibility("default"))) bool
RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am) {
	return duckdb_table_ams.emplace(am, name).second;
}
```

This keeps integration maintainable and upstream-friendly.

## Change policy

- Keep diffs minimal and rebase-friendly.
- In pg_duckdb, use `extern "C"` exported functions for cross-extension contracts.
- In pg_ducklake, gather references to upstream contracts in header files under @include/pgduckdb/. Avoid direct references in .c/.cpp files.
- Avoid leaking pg_duckdb internal C++ symbols in public headers.
