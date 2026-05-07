#!/usr/bin/env bash
#
# bench_concurrent_direct_insert.sh -- Concurrent direct insert stress
#
# N parallel bash sessions issue autocommit direct inserts via psql.
# When a session loses the snapshot_id PK race the executor surfaces
# SQLSTATE 40001; the per-session loop catches and retries (mirroring
# pgx / jdbc / pgbench --max-tries).  Pure shell + psql -- works on
# every PG version.
#
# Asserts:
#   - no client-visible error after retries
#   - user rows == sessions * inserts/session * batch
#   - storage row_id uniqueness (autocommit rollback property)
#   - retry path fired under contention (DI_R_RETRY > 0 when N > 1)

set -euo pipefail

NUM_SESSIONS=${NUM_SESSIONS:-8}
INSERTS_PER_SESSION=${INSERTS_PER_SESSION:-20}
BATCH_SIZE=${BATCH_SIZE:-50}
MAX_TRIES=${MAX_TRIES:-200}

TOTAL_BATCHES=$((NUM_SESSIONS * INSERTS_PER_SESSION))
TOTAL_INSERTED=$((TOTAL_BATCHES * BATCH_SIZE))

if [ -z "${PG_CONFIG:-}" ]; then
  PG_CONFIG=$(command -v pg_config 2>/dev/null) || { echo "PG_CONFIG not set" >&2; exit 1; }
fi
PG_BINDIR=$("$PG_CONFIG" --bindir)

BENCHDIR=$(mktemp -d "${TMPDIR:-/tmp}/bench_concurrent_di_XXXXXX")
PGDATA="$BENCHDIR/data"
PGPORT=${PGPORT:-15433}
DBNAME=bench_concurrent_di

cleanup() {
  "$PG_BINDIR/pg_ctl" -D "$PGDATA" -m immediate stop 2>/dev/null || true
  [ "${KEEP_BENCHDIR:-0}" = "1" ] || rm -rf "$BENCHDIR"
}
trap cleanup EXIT

"$PG_BINDIR/initdb" -D "$PGDATA" --no-locale -E UTF8 >/dev/null 2>&1
cat >> "$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_duckdb,pg_ducklake'
port = $PGPORT
max_connections = $((NUM_SESSIONS + 16))
log_min_messages = warning
unix_socket_directories = '$BENCHDIR'
ducklake.maintenance_enabled = off
EOF
"$PG_BINDIR/pg_ctl" -D "$PGDATA" -l "$BENCHDIR/pg.log" -w start >/dev/null 2>&1

PSQL="$PG_BINDIR/psql -h $BENCHDIR -p $PGPORT -d $DBNAME -X -q"
$PG_BINDIR/psql -h "$BENCHDIR" -p "$PGPORT" -d postgres -X -q -c "CREATE DATABASE $DBNAME;"
$PSQL -c "CREATE EXTENSION pg_ducklake CASCADE;" \
      -c "CALL ducklake.set_option('data_inlining_row_limit', 1000000);" \
      -c "CREATE TABLE bench_concurrent_di (id int, val text) USING ducklake;" \
      -c "SELECT ducklake.reset_direct_insert_stats();"

# Helper that resolves the dynamic inlined-data table name and reports
# (rows, unique_row_ids).  Used by the verifier below.
$PSQL <<'EOSQL'
CREATE FUNCTION storage_check() RETURNS TABLE (rows bigint, unique_row_ids bigint)
LANGUAGE plpgsql AS $fn$
DECLARE q text;
BEGIN
  SELECT format('SELECT count(*)::bigint, count(DISTINCT row_id)::bigint FROM ducklake.ducklake_inlined_data_%s_%s',
                t.table_id, idt.schema_version) INTO q
    FROM ducklake.ducklake_table t
    JOIN ducklake.ducklake_inlined_data_tables idt ON idt.table_id = t.table_id
   WHERE t.table_name = 'bench_concurrent_di' AND t.end_snapshot IS NULL;
  RETURN QUERY EXECUTE q;
END $fn$;
EOSQL

TUPLES=$(seq 1 "$BATCH_SIZE" | awk -v q="'" '{printf "(%d, %sv%d%s),", $1, q, $1, q}' | sed 's/,$//')
INSERT_SQL="INSERT INTO bench_concurrent_di VALUES $TUPLES;"

# Per-session: loop INSERTS_PER_SESSION times, retrying each batch on
# SQLSTATE 40001 (printed by psql when VERBOSITY=sqlstate).
session_runner() {
  local sid=$1 err="$BENCHDIR/err_$1"
  for _ in $(seq 1 "$INSERTS_PER_SESSION"); do
    local attempt=0
    while true; do
      if $PG_BINDIR/psql -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X -q \
           -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate \
           -c "$INSERT_SQL" >/dev/null 2>"$err"; then
        break
      fi
      if grep -qE '^ERROR:[[:space:]]+40001\b' "$err"; then
        attempt=$((attempt + 1))
        [ "$attempt" -lt "$MAX_TRIES" ] && continue
      fi
      cat "$err" >&2
      return 1
    done
  done
}

export PG_BINDIR BENCHDIR PGPORT DBNAME INSERT_SQL INSERTS_PER_SESSION MAX_TRIES
declare -a pids=()
for s in $(seq 1 "$NUM_SESSIONS"); do
  ( session_runner "$s"; echo $? > "$BENCHDIR/rc_$s" ) &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" || true; done

failed=0
for s in $(seq 1 "$NUM_SESSIONS"); do
  [ "$(cat "$BENCHDIR/rc_$s" 2>/dev/null || echo 1)" = "0" ] || failed=$((failed + 1))
done

USER_ROWS=$($PSQL -At -c "SELECT count(*) FROM bench_concurrent_di;")
STORAGE_ROWS=$($PSQL -At -c "SELECT rows FROM storage_check();")
STORAGE_UNIQUE=$($PSQL -At -c "SELECT unique_row_ids FROM storage_check();")
DI_RETRY=$($PSQL -At -c "SELECT sum(count) FROM ducklake.direct_insert_stats() WHERE reason='retry';")

fail=0
check() {
  if [ "$2" = "$3" ]; then printf "%-30s %s == %s  PASS\n" "$1" "$2" "$3"
  else printf "%-30s %s != %s  FAIL\n" "$1" "$2" "$3"; fail=1; fi
}
echo ""
check "failed sessions"           "$failed"          "0"
check "user rows"                 "$USER_ROWS"       "$TOTAL_INSERTED"
check "storage rows"              "$STORAGE_ROWS"    "$TOTAL_INSERTED"
check "storage unique row_ids"    "$STORAGE_UNIQUE"  "$TOTAL_INSERTED"
if [ "$NUM_SESSIONS" -gt 1 ]; then
  check "DI_R_RETRY > 0"          "$([ "$DI_RETRY" -gt 0 ] && echo true || echo false)" "true"
fi
echo ""
echo "DI_R_RETRY: $DI_RETRY"

[ "$fail" -eq 0 ] && echo "OK" || { echo "FAIL"; exit 1; }
