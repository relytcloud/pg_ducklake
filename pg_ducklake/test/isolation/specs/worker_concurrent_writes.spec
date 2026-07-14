# Shared-worker dispatch under concurrency: s1 writes in an in-process transaction
# while s2 dispatches autocommit writes and reads to the shared DuckDB worker (its
# DuckLake metadata commit runs back on s2's backend, inside s2's transaction).
setup
{
  CREATE TABLE iso_worker_t (id int) USING ducklake;
}

session s1
step s1_begin  { BEGIN; }
step s1_insert { INSERT INTO iso_worker_t VALUES (1); }
step s1_commit { COMMIT; }

session s2
setup { SET ducklake.use_shared_worker = on; }
step s2_insert { INSERT INTO iso_worker_t VALUES (2); }
step s2_count  { SELECT count(*) FROM iso_worker_t; }

teardown
{
  DROP TABLE iso_worker_t;
}

# Dispatched write and read while s1's transaction is open: s2's autocommit insert
# commits immediately (worker execution, backend-transaction metadata commit) and its
# dispatched read sees it; s1 commits fine afterwards (snapshot conflict retry).
permutation s1_begin s1_insert s2_insert s2_count s1_commit s2_count

# Dispatched read before and after an in-process commit: the worker read runs under
# s2's shipped snapshot, so it sees exactly what an in-process read would.
permutation s2_insert s1_begin s1_insert s2_count s1_commit s2_count
