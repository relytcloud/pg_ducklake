#!/usr/bin/env bash
#
# bench_concurrent_direct_insert.sh -- Concurrent direct insert stress
#
# Spawns N parallel psql sessions, each firing M autocommit direct
# inserts (UNNEST pattern) at the same ducklake table.  Verifies the
# concurrency-safety properties enforced by DirectInsertReservation:
#
#   1. No client error.  Phase 2's retry loop hides the snapshot_id PK
#      race; if exhaustion happened the client would see SQLSTATE 40001.
#   2. User row count matches expected.  Catches lost writes.
#   3. Storage row_id uniqueness in the inlined data table.  The row_id
#      race used to allow overlapping ranges; a regression that
#      reintroduces it surfaces here even when the user count looks
#      fine.
#   4. Snapshot count matches expected.  Each direct insert produces
#      exactly one ducklake_snapshot row -- verifies the reservation
#      did not double-allocate or skip on retry.
#   5. Reports DI_R_RETRY so you can see whether concurrent contention
#      actually exercised the snapshot_id retry path.
#
# Usage:
#   PG_CONFIG=/path/to/pg_config ./bench_concurrent_direct_insert.sh
#   NUM_SESSIONS=16 INSERTS_PER_SESSION=100 BATCH_SIZE=50 \
#     ./bench_concurrent_direct_insert.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_SESSIONS=${NUM_SESSIONS:-8}
INSERTS_PER_SESSION=${INSERTS_PER_SESSION:-50}
BATCH_SIZE=${BATCH_SIZE:-100}
DATA_INLINING_ROW_LIMIT=${DATA_INLINING_ROW_LIMIT:-1000000}

TOTAL_BATCHES=$((NUM_SESSIONS * INSERTS_PER_SESSION))
TOTAL_INSERTED=$((TOTAL_BATCHES * BATCH_SIZE))

# ---------------------------------------------------------------------------
# Resolve PG binaries
# ---------------------------------------------------------------------------
if [ -z "${PG_CONFIG:-}" ]; then
  PG_CONFIG=$(command -v pg_config 2>/dev/null || true)
  if [ -z "$PG_CONFIG" ]; then
    echo "ERROR: PG_CONFIG not set and pg_config not found in PATH" >&2
    exit 1
  fi
fi

PG_BINDIR=$("$PG_CONFIG" --bindir)
INITDB="$PG_BINDIR/initdb"
PG_CTL="$PG_BINDIR/pg_ctl"
PSQL="$PG_BINDIR/psql"

# ---------------------------------------------------------------------------
# Temp cluster
# ---------------------------------------------------------------------------
BENCHDIR=$(mktemp -d "${TMPDIR:-/tmp}/bench_concurrent_di_XXXXXX")
PGDATA="$BENCHDIR/data"
PGPORT=${PGPORT:-15433}
DBNAME=bench_concurrent_di

cleanup() {
  "$PG_CTL" -D "$PGDATA" -m immediate stop 2>/dev/null || true
  rm -rf "$BENCHDIR"
}
trap cleanup EXIT

echo "Setting up temporary PostgreSQL cluster on port $PGPORT..."
"$INITDB" -D "$PGDATA" --no-locale -E UTF8 >/dev/null 2>&1

# max_connections must comfortably exceed NUM_SESSIONS + a small margin
# for the verifier psql + background workers + walwriter + bgwriter etc.
MAX_CONN=$((NUM_SESSIONS + 16))

cat >> "$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_duckdb,pg_ducklake'
port = $PGPORT
max_connections = $MAX_CONN
log_min_messages = warning
logging_collector = off
unix_socket_directories = '$BENCHDIR'
ducklake.maintenance_enabled = off
EOF

"$PG_CTL" -D "$PGDATA" -l "$BENCHDIR/pg.log" -w start >/dev/null 2>&1

run_sql() {
  "$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X -q "$@"
}
run_sql_t() {
  "$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X -At "$@"
}

"$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d postgres -X -q \
  -c "CREATE DATABASE $DBNAME;"
run_sql -c "CREATE EXTENSION pg_ducklake CASCADE;"

# ---------------------------------------------------------------------------
# Helpers (lifted from bench_direct_insert.sh)
# ---------------------------------------------------------------------------
# Build a VALUES tuple list: (1,'v1'),(2,'v2'),...,(n,'vn')
gen_values_tuples() {
  local n=$1
  seq 1 "$n" | awk -v q="'" '{printf "(%d, %sv%d%s),", $1, q, $1, q}' | sed 's/,$//'
}

# Helper that resolves the dynamic inlined-data table name and returns
# (rows, unique_row_ids) so the verifier can assert no row_id collision.
run_sql -c "CALL ducklake.set_option('data_inlining_row_limit', $DATA_INLINING_ROW_LIMIT);"
run_sql <<'EOSQL'
CREATE FUNCTION bench_concurrent_storage_check(target_table text)
RETURNS TABLE (rows bigint, unique_row_ids bigint)
LANGUAGE plpgsql AS $fn$
DECLARE
  tbl_id bigint;
  sv bigint;
  q text;
BEGIN
  SELECT t.table_id, idt.schema_version
    INTO tbl_id, sv
    FROM ducklake.ducklake_table t
    JOIN ducklake.ducklake_inlined_data_tables idt ON idt.table_id = t.table_id
   WHERE t.table_name = $1 AND t.end_snapshot IS NULL
   LIMIT 1;

  q := format('SELECT count(*)::bigint, count(DISTINCT row_id)::bigint '
              'FROM ducklake.ducklake_inlined_data_%s_%s',
              tbl_id, sv);
  RETURN QUERY EXECUTE q;
END;
$fn$;
EOSQL

run_sql -c "CREATE TABLE bench_concurrent_di (id int, val text) USING ducklake;"
run_sql -c "SELECT ducklake.reset_direct_insert_stats();"

BASELINE_SNAPSHOTS=$(run_sql_t -c "SELECT count(*) FROM ducklake.ducklake_snapshot;")

# ---------------------------------------------------------------------------
# Build the per-session script: M direct inserts of BATCH_SIZE rows each.
# Uses a literal VALUES tuple list (matches DIRECT_INSERT_VALUES).  The
# UNNEST pattern requires a PREPARE'd statement with array parameters
# to be detected as direct-insertable, but pg_duckdb's plan cache trips
# a "UNKNOWN to Postgres type" error around the 6th EXECUTE of such a
# PREPARE -- unrelated to the concurrency we want to stress.  VALUES
# avoids the bug and exercises the same DirectInsertReservation code
# path.
# ---------------------------------------------------------------------------
TUPLES=$(gen_values_tuples "$BATCH_SIZE")

{
  echo "SET ducklake.enable_direct_insert = true;"
  for _ in $(seq 1 "$INSERTS_PER_SESSION"); do
    echo "INSERT INTO bench_concurrent_di VALUES $TUPLES;"
  done
} > "$BENCHDIR/session.sql"

# ---------------------------------------------------------------------------
# Fire NUM_SESSIONS in parallel
# ---------------------------------------------------------------------------
echo ""
echo "Concurrent Direct Insert Stress"
echo "==============================="
echo "Sessions:         $NUM_SESSIONS"
echo "Inserts/session:  $INSERTS_PER_SESSION"
echo "Batch size:       $BATCH_SIZE"
echo "Total inserts:    $TOTAL_INSERTED rows in $TOTAL_BATCHES direct-insert batches"
echo ""

START=$(python3 -c 'import time; print(time.monotonic())')
for s in $(seq 1 "$NUM_SESSIONS"); do
  (
    if "$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X -q \
        -v ON_ERROR_STOP=1 -f "$BENCHDIR/session.sql" \
        > "$BENCHDIR/session_$s.out" 2>&1; then
      echo 0 > "$BENCHDIR/session_$s.rc"
    else
      echo "$?" > "$BENCHDIR/session_$s.rc"
    fi
  ) &
done
wait
END=$(python3 -c 'import time; print(time.monotonic())')
ELAPSED_MS=$(python3 -c "print(round(($END - $START) * 1000, 1))")

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
errors=0
for s in $(seq 1 "$NUM_SESSIONS"); do
  rc=$(cat "$BENCHDIR/session_$s.rc")
  if [ "$rc" -ne 0 ]; then
    echo "Session $s failed (rc=$rc):"
    sed 's/^/  /' "$BENCHDIR/session_$s.out"
    errors=$((errors + 1))
  fi
done

USER_COUNT=$(run_sql_t -c "SELECT count(*) FROM bench_concurrent_di;")
STORAGE=$(run_sql_t -c "SELECT rows || '|' || unique_row_ids FROM bench_concurrent_storage_check('bench_concurrent_di');")
STORAGE_ROWS=$(echo "$STORAGE" | cut -d'|' -f1)
STORAGE_UNIQUE=$(echo "$STORAGE" | cut -d'|' -f2)

NEW_SNAPSHOTS=$(run_sql_t -c \
  "SELECT count(*) - $BASELINE_SNAPSHOTS FROM ducklake.ducklake_snapshot;")
RETRY_COUNT=$(run_sql_t -c \
  "SELECT sum(count) FROM ducklake.direct_insert_stats() WHERE reason = 'retry';")
OK_COUNT=$(run_sql_t -c \
  "SELECT count FROM ducklake.direct_insert_stats() WHERE pattern = 'matched_values' AND reason = 'ok';")

THROUGHPUT=$(python3 -c "print(round($TOTAL_INSERTED / ($ELAPSED_MS / 1000), 0))")

# ---------------------------------------------------------------------------
# Report + verdict
# ---------------------------------------------------------------------------
fail=0
result() {
  local label=$1; local actual=$2; local expected=$3
  if [ "$actual" = "$expected" ]; then
    printf "%-22s %s == %s  PASS\n" "$label" "$actual" "$expected"
  else
    printf "%-22s %s != %s  FAIL\n" "$label" "$actual" "$expected"
    fail=1
  fi
}

echo ""
echo "Throughput:           ${THROUGHPUT} rows/sec  (${ELAPSED_MS} ms elapsed)"
echo ""
result "Failed sessions"      "$errors"          "0"
result "User row count"       "$USER_COUNT"      "$TOTAL_INSERTED"
result "Storage rows"         "$STORAGE_ROWS"    "$TOTAL_INSERTED"
result "Storage unique row_ids" "$STORAGE_UNIQUE" "$TOTAL_INSERTED"
result "Snapshots created"    "$NEW_SNAPSHOTS"   "$TOTAL_BATCHES"
result "DI matched_values ok" "$OK_COUNT"        "$TOTAL_BATCHES"
echo ""
echo "DI_R_RETRY:           $RETRY_COUNT  (>0 means concurrent contention exercised the snapshot_id retry loop)"
echo ""

if [ "$fail" -ne 0 ]; then
  echo "FAIL"
  exit 1
fi

echo "OK"
exit 0
