#pragma once

#include "pgddb/pg/declarations.hpp"

namespace pgducklake {

// CREATE SECRET statements for every server on the ducklake_secret FDW, applied
// to each DuckDB connection by DuckDBManager::RefreshConnectionState.
List *ListCreateSecretQueries();

// Validate a secret defined by a SERVER (type + options) and an optional USER
// MAPPING by creating it on a throwaway connection; throws a PG error if invalid.
void ValidateSecret(const char *type, List *server_options, List *mapping_options = nullptr);

// Stash the target server's type/oid from a CREATE/ALTER FOREIGN SERVER or USER
// MAPPING node so the FDW validator (which only receives options) can reach them.
// No-op for statements unrelated to the ducklake_secret FDW.
void CaptureSecretServer(Node *parsetree);

// Register syscache callbacks that invalidate cached secrets on SERVER / USER
// MAPPING changes.
void InitSecrets();

} // namespace pgducklake
