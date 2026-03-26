CALL dbgen(sf=${sf});
ATTACH 'ducklake:${BENCHMARK_DIR}/ducklake.db' AS ducklake (DATA_FILES '${BENCHMARK_DIR}/ducklake_data');
COPY FROM DATABASE main TO ducklake;
