#!/usr/bin/env bash
#
# Setup script for the DuckDB + DuckLake (standalone) data-inlining benchmark.
# Downloads one HITS partition (~140 MB), starts a plain PostgreSQL container
# as the DuckLake metadata catalog.
#
# Environment variables:
#   PG_CATALOG_IMAGE   - Docker image (default: postgres:18)
#   PG_CATALOG_PORT    - host port for catalog PG (default: 5433)
#   BENCH_HITS_FILE    - local path to hits parquet (default: /tmp/hits_0.parquet)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${PG_CATALOG_IMAGE:-postgres:18}"
PORT="${PG_CATALOG_PORT:-5433}"
HITS_FILE="${BENCH_HITS_FILE:-/tmp/hits_0.parquet}"

# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

download_hits_parquet "$HITS_FILE"

start_container "bench-pgcatalog" "$IMAGE" "$PORT"
