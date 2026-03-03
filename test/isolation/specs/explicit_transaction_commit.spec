setup
{
  CREATE TABLE iso_tx_commit_t (id int) USING ducklake;
}

session s1
step s1_begin    { BEGIN; }
step s1_insert   { INSERT INTO iso_tx_commit_t VALUES (1); }
step s1_commit   { COMMIT; }
step s1_rollback { ROLLBACK; }

session s2
step s2_begin  { BEGIN; }
step s2_count  { SELECT count(*) FROM iso_tx_commit_t; }
step s2_commit { COMMIT; }

teardown
{
  DROP TABLE iso_tx_commit_t;
}

# Committed data becomes visible after commit
permutation s1_begin s1_insert s2_count s1_commit s2_count

# Rolled-back data is never visible
permutation s1_begin s1_insert s2_count s1_rollback s2_count

# Read committed: s2 in own transaction sees s1's data once s1 commits
permutation s1_begin s2_begin s1_insert s2_count s1_commit s2_count s2_commit
