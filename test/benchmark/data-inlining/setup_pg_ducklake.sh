#!/usr/bin/env bash
#
# Setup script for the pg_ducklake data-inlining benchmark.
# Downloads one HITS partition (~140 MB), starts a pg_ducklake Docker
# container with the parquet mounted at /tmp/hits.parquet.
#
# Environment variables:
#   PGDUCKLAKE_IMAGE   - Docker image (default: pgducklake/pgducklake:18-main)
#   PGDUCKLAKE_PORT    - host port for PostgreSQL (default: 5432)
#   BENCH_HITS_FILE    - local path to hits parquet (default: /tmp/hits_0.parquet)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${PGDUCKLAKE_IMAGE:-pgducklake/pgducklake:18-main}"
PORT="${PGDUCKLAKE_PORT:-5432}"
HITS_FILE="${BENCH_HITS_FILE:-/tmp/hits_0.parquet}"

# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

download_hits_parquet "$HITS_FILE"

start_container "bench-pgducklake" "$IMAGE" "$PORT" \
    -v "${HITS_FILE}:/tmp/hits.parquet:ro"
