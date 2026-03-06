#!/usr/bin/env bash
#
# bench_direct_insert.sh -- Benchmark direct inline insert vs DuckDB path
#
# Measures INSERT performance for the parameterized UNNEST pattern using
# the direct insert optimization (SPI bypass) vs the standard DuckDB path
# (non-parameterized, since parameterized errors on the DuckDB path).
#
# TODO: Add benchmarks for read performance after direct insert and
#       concurrent write contention across multiple sessions.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZES=(100 1000 10000)
WARMUP=2
ITERATIONS=5

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
BENCHDIR=$(mktemp -d "${TMPDIR:-/tmp}/bench_di_XXXXXX")
PGDATA="$BENCHDIR/data"
PGPORT=${PGPORT:-15432}
DBNAME=bench_di

cleanup() {
  "$PG_CTL" -D "$PGDATA" -m immediate stop 2>/dev/null || true
  rm -rf "$BENCHDIR"
}
trap cleanup EXIT

echo "Setting up temporary PostgreSQL cluster..."
"$INITDB" -D "$PGDATA" --no-locale -E UTF8 >/dev/null 2>&1

cat >> "$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_duckdb,pg_ducklake'
port = $PGPORT
log_min_messages = warning
logging_collector = off
unix_socket_directories = '$BENCHDIR'
EOF

"$PG_CTL" -D "$PGDATA" -l "$BENCHDIR/pg.log" -w start >/dev/null 2>&1

run_sql() {
  "$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X -q "$@"
}

"$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d postgres -X -q \
  -c "CREATE DATABASE $DBNAME;"
run_sql -c "CREATE EXTENSION pg_ducklake CASCADE;"

# ---------------------------------------------------------------------------
# Array generation (uses seq + awk + paste for efficiency at large N)
# ---------------------------------------------------------------------------
gen_int_array() {
  local n=$1
  printf "ARRAY[%s]" "$(seq 1 "$n" | tr '\n' ',' | sed 's/,$//')"
}

gen_text_array() {
  local n=$1
  printf "ARRAY[%s]" "$(seq 1 "$n" | sed "s/.*/'v&'/" | tr '\n' ',' | sed 's/,$//')"
}

# ---------------------------------------------------------------------------
# Timing (parse psql \timing output)
# ---------------------------------------------------------------------------
extract_time_ms() {
  grep -oE 'Time: [0-9]+(\.[0-9]+)? ms' | tail -1 | awk '{print $2}'
}

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
echo ""
echo "Direct Insert Benchmark"
echo "======================="
echo ""
printf "%-12s %-8s %10s %12s %6s\n" "batch_size" "mode" "avg_ms" "rows/sec" "iters"
printf "%-12s %-8s %10s %12s %6s\n" "----------" "------" "--------" "----------" "-----"

declare -a speedups=()

for batch_size in "${BATCH_SIZES[@]}"; do
  echo "Generating arrays for batch_size=$batch_size..." >&2
  INT_ARRAY=$(gen_int_array "$batch_size")
  TEXT_ARRAY=$(gen_text_array "$batch_size")

  # --- Direct insert path (parameterized UNNEST via SPI bypass) ---
  run_sql \
    -c "CREATE TABLE bench_di (id INT, val TEXT) USING ducklake;" \
    -c "CALL ducklake.set_option('data_inlining_row_limit', 100000);" \
    -c "INSERT INTO bench_di VALUES (0, 'init');"

  cat > "$BENCHDIR/direct.sql" <<EOSQL
SET ducklake.enable_direct_insert = true;
PREPARE stmt (int[], text[]) AS INSERT INTO bench_di SELECT UNNEST(\$1), UNNEST(\$2);
\timing on
EXECUTE stmt($INT_ARRAY, $TEXT_ARRAY);
EOSQL

  direct_total=0
  direct_count=0
  for i in $(seq 1 $((WARMUP + ITERATIONS))); do
    output=$("$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X \
      -f "$BENCHDIR/direct.sql" 2>&1) || true
    ms=$(echo "$output" | extract_time_ms)
    if [ "$i" -gt "$WARMUP" ] && [ -n "${ms:-}" ]; then
      direct_total=$(awk "BEGIN {print $direct_total + $ms}")
      direct_count=$((direct_count + 1))
    fi
  done

  run_sql -c "DROP TABLE bench_di;"

  # --- DuckDB path (non-parameterized UNNEST, standard engine) ---
  run_sql \
    -c "CREATE TABLE bench_di (id INT, val TEXT) USING ducklake;" \
    -c "CALL ducklake.set_option('data_inlining_row_limit', 100000);" \
    -c "INSERT INTO bench_di VALUES (0, 'init');"

  cat > "$BENCHDIR/duckdb.sql" <<EOSQL
SET ducklake.enable_direct_insert = false;
\timing on
INSERT INTO bench_di SELECT UNNEST($INT_ARRAY), UNNEST($TEXT_ARRAY);
EOSQL

  duckdb_total=0
  duckdb_count=0
  for i in $(seq 1 $((WARMUP + ITERATIONS))); do
    output=$("$PSQL" -h "$BENCHDIR" -p "$PGPORT" -d "$DBNAME" -X \
      -f "$BENCHDIR/duckdb.sql" 2>&1) || true
    ms=$(echo "$output" | extract_time_ms)
    if [ "$i" -gt "$WARMUP" ] && [ -n "${ms:-}" ]; then
      duckdb_total=$(awk "BEGIN {print $duckdb_total + $ms}")
      duckdb_count=$((duckdb_count + 1))
    fi
  done

  run_sql -c "DROP TABLE bench_di;"

  # --- Compute stats ---
  if [ "$direct_count" -gt 0 ]; then
    direct_avg=$(awk "BEGIN {printf \"%.2f\", $direct_total / $direct_count}")
    direct_rps=$(awk "BEGIN {printf \"%.0f\", $batch_size / ($direct_avg / 1000)}")
  else
    direct_avg="N/A"
    direct_rps="N/A"
  fi

  if [ "$duckdb_count" -gt 0 ]; then
    duckdb_avg=$(awk "BEGIN {printf \"%.2f\", $duckdb_total / $duckdb_count}")
    duckdb_rps=$(awk "BEGIN {printf \"%.0f\", $batch_size / ($duckdb_avg / 1000)}")
  else
    duckdb_avg="N/A"
    duckdb_rps="N/A"
  fi

  printf "%-12s %-8s %10s %12s %6s\n" "$batch_size" "direct" "$direct_avg" "$direct_rps" "$ITERATIONS"
  printf "%-12s %-8s %10s %12s %6s\n" "$batch_size" "duckdb" "$duckdb_avg" "$duckdb_rps" "$ITERATIONS"

  if [ "$direct_avg" != "N/A" ] && [ "$duckdb_avg" != "N/A" ]; then
    speedup=$(awk "BEGIN {printf \"%.1f\", $duckdb_avg / $direct_avg}")
    speedups+=("${speedup}x ($batch_size)")
  fi
done

echo ""
if [ ${#speedups[@]} -gt 0 ]; then
  echo "Speedup: $(IFS=', '; echo "${speedups[*]}")"
fi
echo ""

exit 0
