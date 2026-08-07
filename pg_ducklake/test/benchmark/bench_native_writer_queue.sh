#!/usr/bin/env bash
# Compare bounded native-writer reservation queue behavior under contention.
#
# Reproducible bounded default:
#   PG_CONFIG=$PWD/pg-18/bin/pg_config ./pg_ducklake/test/benchmark/bench_native_writer_queue.sh
#
# Environment: CLIENTS=8 DURATION=10 WARMUP_DURATION=2 REPETITIONS=3,
# BATCH_SIZE=20 HOL_DELAY_MS=500 QUEUE_CAPACITY=256 QUEUE_WAIT_MS=10,
# SEED=238 PGPORT=15435.
# Each measured run uses a fresh database and table. Modes have warmups, a
# randomized initial order followed by alternating order, and repeated runs.

set -euo pipefail

CLIENTS=${CLIENTS:-8}
DURATION=${DURATION:-10}
WARMUP_DURATION=${WARMUP_DURATION:-2}
REPETITIONS=${REPETITIONS:-3}
BATCH_SIZE=${BATCH_SIZE:-20}
HOL_DELAY_MS=${HOL_DELAY_MS:-500}
QUEUE_CAPACITY=${QUEUE_CAPACITY:-256}
QUEUE_WAIT_MS=${QUEUE_WAIT_MS:-10}
SEED=${SEED:-238}
PGPORT=${PGPORT:-15435}

if [ -z "${PG_CONFIG:-}" ]; then
  PG_CONFIG=$(command -v pg_config 2>/dev/null || true)
fi
[ -n "${PG_CONFIG:-}" ] || { echo "PG_CONFIG not set" >&2; exit 1; }
PG_BINDIR=$($PG_CONFIG --bindir)
BENCHDIR=$(mktemp -d "${TMPDIR:-/tmp}/bench_native_queue_XXXXXX")
PGDATA=$BENCHDIR/data
RESULTS=$BENCHDIR/results.csv

cleanup() {
  "$PG_BINDIR/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  [ "${KEEP_BENCHDIR:-0}" = 1 ] || rm -rf "$BENCHDIR"
}
trap cleanup EXIT

"$PG_BINDIR/initdb" -D "$PGDATA" --no-locale -E UTF8 >/dev/null
cat >>"$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_ducklake'
port = $PGPORT
unix_socket_directories = '$BENCHDIR'
max_connections = $((CLIENTS + 20))
ducklake.maintenance_enabled = off
ducklake.native_writer_reservation_queue_capacity = $QUEUE_CAPACITY
autovacuum = off
log_min_messages = warning
EOF
"$PG_BINDIR/pg_ctl" -D "$PGDATA" -l "$BENCHDIR/postgres.log" -w start >/dev/null

ADMIN=("$PG_BINDIR/psql" -h "$BENCHDIR" -p "$PGPORT" -d postgres -X -Atq -v ON_ERROR_STOP=1)
RETRY_OPTIONS="-c ducklake.native_writer_max_retry_count=1000 -c ducklake.native_writer_retry_wait_ms=1ms -c ducklake.native_writer_retry_backoff=1 -c ducklake.native_writer_reservation_queue_wait_ms=${QUEUE_WAIT_MS}ms -c ducklake.enable_metadata_sync=off"
PROBE_RETRY_OPTIONS="-c ducklake.native_writer_max_retry_count=1000 -c ducklake.native_writer_retry_wait_ms=10ms -c ducklake.native_writer_retry_backoff=1 -c ducklake.native_writer_reservation_queue_wait_ms=${QUEUE_WAIT_MS}ms -c ducklake.enable_metadata_sync=off"
TUPLES=$(seq 1 "$BATCH_SIZE" | awk '{printf "(%d, '\''payload'\''),", $1}' | sed 's/,$//')

latencies() {
  python3 - "$1" <<'PY'
import glob, math, sys
values = []
for path in glob.glob(sys.argv[1] + "*"):
    with open(path) as stream:
        for line in stream:
            if line.startswith("#"):
                continue
            fields = line.split()
            if len(fields) >= 3:
                values.append(int(fields[2]) / 1000.0)
values.sort()
def percentile(p):
    if not values:
        return 0.0
    return values[max(0, math.ceil(len(values) * p) - 1)]
print(f"{percentile(.95):.3f},{percentile(.99):.3f},{(values[-1] if values else 0):.3f}")
PY
}

wait_for_sleeping_head() {
  local db=$1 app=$2 pid=$3
  local psql=("$PG_BINDIR/psql" -h "$BENCHDIR" -p "$PGPORT" -d "$db" -X -Atq -v ON_ERROR_STOP=1)
  for _ in $(seq 1 400); do
    if [ "$("${psql[@]}" -c "SELECT count(*) FROM pg_stat_activity WHERE application_name='$app' AND wait_event='PgSleep'")" = 1 ]; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
      echo "HOL head exited before it was blocked" >&2
      return 1
    fi
    sleep 0.005
  done
  echo "HOL head was not observably blocked" >&2
  return 1
}

hol_probe() {
  local db=$1 mode=$2 kind=$3 suffix=$4
  local psql=("$PG_BINDIR/psql" -h "$BENCHDIR" -p "$PGPORT" -d "$db" -X -Atq -v ON_ERROR_STOP=1)
  local head_table="hol_head_${suffix}" follower_table app_head app_follower trigger
  if [ "$kind" = same ]; then
    follower_table=$head_table
  else
    follower_table="hol_other_${suffix}"
  fi
  app_head="queue_hol_head_${suffix}"
  app_follower="queue_hol_follower_${suffix}"
  trigger="queue_hol_trigger_${suffix}"

  "${psql[@]}" -c "CREATE TABLE $head_table (id int, payload text) USING ducklake" \
    -c "SELECT count(*) FROM ducklake.ensure_inlined_data_table('$head_table'::regclass)" >/dev/null
  if [ "$kind" = cross ]; then
    "${psql[@]}" -c "CREATE TABLE $follower_table (id int, payload text) USING ducklake" \
      -c "SELECT count(*) FROM ducklake.ensure_inlined_data_table('$follower_table'::regclass)" >/dev/null
  fi
  "${psql[@]}" -c "CREATE FUNCTION public.$trigger() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN IF current_setting('application_name') = '$app_head' THEN PERFORM pg_sleep($HOL_DELAY_MS / 1000.0); END IF; RETURN NEW; END \$\$" \
    -c "CREATE TRIGGER $trigger BEFORE INSERT ON ducklake.ducklake_snapshot FOR EACH ROW EXECUTE FUNCTION public.$trigger()" >/dev/null

  PGAPPNAME=$app_head PGOPTIONS="-c ducklake.native_writer_reservation_queue=$mode $PROBE_RETRY_OPTIONS" \
    "${psql[@]}" -c "INSERT INTO $head_table VALUES $TUPLES" >/dev/null &
  local head_pid=$!
  if ! wait_for_sleeping_head "$db" "$app_head" "$head_pid"; then
    kill "$head_pid" 2>/dev/null || true
    wait "$head_pid" 2>/dev/null || true
    return 1
  fi

  local started ended follower_pid waited=0
  started=$(python3 -c 'import time; print(time.time_ns())')
  PGAPPNAME=$app_follower PGOPTIONS="-c ducklake.native_writer_reservation_queue=$mode $PROBE_RETRY_OPTIONS" \
    "${psql[@]}" -c "INSERT INTO $follower_table VALUES $TUPLES" >/dev/null &
  follower_pid=$!
  while kill -0 "$follower_pid" 2>/dev/null; do
    if [ "$("${psql[@]}" -c "SELECT count(*) FROM pg_stat_activity WHERE application_name='$app_follower' AND wait_event_type='Extension'")" = 1 ]; then
      waited=1
    fi
    sleep 0.005
  done
  wait "$follower_pid"
  ended=$(python3 -c 'import time; print(time.time_ns())')
  wait "$head_pid"

  "${psql[@]}" -c "DROP TRIGGER $trigger ON ducklake.ducklake_snapshot" \
    -c "DROP FUNCTION public.$trigger()" >/dev/null
  python3 - "$started" "$ended" "$waited" <<'PY'
import sys
print(f"{(int(sys.argv[2]) - int(sys.argv[1])) / 1_000_000:.3f},{sys.argv[3]}")
PY
}

run_one() {
  local rep=$1 order=$2 mode=$3
  local db="bench_queue_${rep}_${mode}"
  local psql=("$PG_BINDIR/psql" -h "$BENCHDIR" -p "$PGPORT" -d "$db" -X -Atq -v ON_ERROR_STOP=1)
  "${ADMIN[@]}" -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" -c "CREATE DATABASE $db" >/dev/null
  "${psql[@]}" -c "CREATE EXTENSION pg_ducklake CASCADE" \
    -c "CALL ducklake.set_option('data_inlining_row_limit', 1000000)" >/dev/null

  local warmup="queue_warmup" measured="queue_measured"
  "${psql[@]}" -c "CREATE TABLE $warmup (id int, payload text) USING ducklake" \
    -c "SELECT count(*) FROM ducklake.ensure_inlined_data_table('$warmup'::regclass)" >/dev/null
  printf 'INSERT INTO %s VALUES %s;\n' "$warmup" "$TUPLES" >"$BENCHDIR/workload.sql"
  PGOPTIONS="-c ducklake.native_writer_reservation_queue=$mode $RETRY_OPTIONS" \
    "$PG_BINDIR/pgbench" -h "$BENCHDIR" -p "$PGPORT" -n -M prepared \
      -c "$CLIENTS" -j "$CLIENTS" -T "$WARMUP_DURATION" \
      -f "$BENCHDIR/workload.sql" "$db" >/dev/null
  "${psql[@]}" -c "DROP TABLE $warmup" \
    -c "CREATE TABLE $measured (id int, payload text) USING ducklake" \
    -c "SELECT count(*) FROM ducklake.ensure_inlined_data_table('$measured'::regclass)" \
    -c "SELECT ducklake.reset_native_writer_stats()" >/dev/null

  local inline_name wal_start wal_end log_prefix output transactions tps
  inline_name=$("${psql[@]}" -c "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it JOIN ducklake.ducklake_table t USING (table_id) WHERE t.table_name='$measured' AND t.end_snapshot IS NULL ORDER BY it.schema_version DESC LIMIT 1")
  "${psql[@]}" -c "CHECKPOINT" >/dev/null
  wal_start=$("${psql[@]}" -c "SELECT pg_current_wal_insert_lsn()")
  printf 'INSERT INTO %s VALUES %s;\n' "$measured" "$TUPLES" >"$BENCHDIR/workload.sql"
  log_prefix="$BENCHDIR/${rep}_${mode}_latency."
  output=$(PGOPTIONS="-c ducklake.native_writer_reservation_queue=$mode $RETRY_OPTIONS" \
    "$PG_BINDIR/pgbench" -h "$BENCHDIR" -p "$PGPORT" -n -M prepared \
      -c "$CLIENTS" -j "$CLIENTS" -T "$DURATION" -l --log-prefix="$log_prefix" \
      -f "$BENCHDIR/workload.sql" "$db")
  wal_end=$("${psql[@]}" -c "SELECT pg_current_wal_insert_lsn()")
  transactions=$(printf '%s\n' "$output" | awk -F': ' '/number of transactions actually processed/ {split($2, a, "/"); gsub(/ /, "", a[1]); print a[1]}' | tail -1)
  tps=$(printf '%s\n' "$output" | awk -F'= ' '/tps =/ {print $2}' | awk '{print $1}' | tail -1)
  [ -n "$transactions" ] && [ "$transactions" -gt 0 ] || { echo "$output" >&2; return 1; }

  local p95 p99 max_latency retags conflicts wal_bytes dead rows_s wal_per_tx capacity
  IFS=, read -r p95 p99 max_latency <<<"$(latencies "$log_prefix")"
  retags=$("${psql[@]}" -c "SELECT count FROM ducklake.native_writer_stats() WHERE event='rows_retagged'")
  conflicts=$("${psql[@]}" -c "SELECT count FROM ducklake.native_writer_stats() WHERE event='snapshot_claim_conflicts'")
  wal_bytes=$("${psql[@]}" -c "SELECT pg_wal_lsn_diff('$wal_end', '$wal_start')::bigint")
  "${psql[@]}" -c "ANALYZE ducklake.$inline_name" >/dev/null
  sleep 0.2
  dead=$("${psql[@]}" -c "SELECT pg_stat_get_dead_tuples('ducklake.$inline_name'::regclass)")
  capacity=$("${psql[@]}" -c "SHOW ducklake.native_writer_reservation_queue_capacity")
  rows_s=$(awk -v t="$tps" -v b="$BATCH_SIZE" 'BEGIN {printf "%.1f", t*b}')
  wal_per_tx=$(awk -v w="$wal_bytes" -v n="$transactions" 'BEGIN {printf "%.1f", w/n}')

  local probe same_ms same_wait cross_ms cross_wait
  probe=$(hol_probe "$db" "$mode" same "${rep}_${mode}_same") || return 1
  IFS=, read -r same_ms same_wait <<<"$probe"
  probe=$(hol_probe "$db" "$mode" cross "${rep}_${mode}_cross") || return 1
  IFS=, read -r cross_ms cross_wait <<<"$probe"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$rep" "$order" "$mode" "$transactions" "$tps" "$rows_s" "$p95" "$p99" "$max_latency" \
    "$retags" "$conflicts" "$wal_bytes" "$wal_per_tx" "$dead" "$capacity" \
    "$same_ms" "$same_wait" "$cross_ms" "$cross_wait" | tee -a "$RESULTS"

  "${ADMIN[@]}" -c "DROP DATABASE $db WITH (FORCE)" >/dev/null
}

printf 'repetition,order,mode,transactions,tps,rows_per_s,p95_ms,p99_ms,max_ms,retagged_rows,claim_conflicts,wal_bytes,wal_bytes_per_tx,estimated_dead_tuples,queue_capacity,same_table_hol_ms,same_table_queue_wait,cross_table_hol_ms,cross_table_queue_wait\n' | tee "$RESULTS"
first_mode=$(python3 - "$SEED" <<'PY'
import random, sys
print(random.Random(int(sys.argv[1])).choice(["off", "on"]))
PY
)
for rep in $(seq 1 "$REPETITIONS"); do
  if { [ "$first_mode" = on ] && [ $((rep % 2)) -eq 1 ]; } ||
     { [ "$first_mode" = off ] && [ $((rep % 2)) -eq 0 ]; }; then
    modes=(on off)
  else
    modes=(off on)
  fi
  order="${modes[0]}-${modes[1]}"
  for mode in "${modes[@]}"; do
    run_one "$rep" "$order" "$mode"
  done
done

python3 - "$RESULTS" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
print("\nvariability_summary")
print("mode,runs,total_transactions,tps_mean,tps_stdev,tps_cv_percent,tps_min,tps_max,cross_table_hol_mean_ms,capacity")
for mode in ("off", "on"):
    selected = [row for row in rows if row["mode"] == mode]
    tps = [float(row["tps"]) for row in selected]
    hol = [float(row["cross_table_hol_ms"]) for row in selected]
    mean = statistics.mean(tps)
    stdev = statistics.stdev(tps) if len(tps) > 1 else 0.0
    print(f"{mode},{len(selected)},{sum(int(row['transactions']) for row in selected)},"
          f"{mean:.3f},{stdev:.3f},{(100 * stdev / mean if mean else 0):.2f},"
          f"{min(tps):.3f},{max(tps):.3f},{statistics.mean(hol):.3f},{selected[0]['queue_capacity']}")
PY

echo "benchmark_dir=$BENCHDIR" >&2
