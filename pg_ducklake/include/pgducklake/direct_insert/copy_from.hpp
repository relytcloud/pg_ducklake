#pragma once

#include "pgddb/pg/declarations.hpp"

namespace pgducklake {

// Handle a supported autocommit COPY FROM STDIN; returns rows inserted and publishes one snapshot.
uint64_t DucklakeCopyFromStdin(CopyStmt *stmt, const char *query_string);

} // namespace pgducklake
