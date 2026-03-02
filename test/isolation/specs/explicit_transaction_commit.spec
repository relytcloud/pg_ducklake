setup
{
  CREATE TABLE iso_tx_commit_t (id int) USING ducklake;
}

session s1
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO iso_tx_commit_t VALUES (1); }
step s1_commit { COMMIT; }

session s2
step s2_before { SELECT count(*) FROM iso_tx_commit_t; }
step s2_after { SELECT count(*) FROM iso_tx_commit_t; }

teardown
{
  DROP TABLE iso_tx_commit_t;
}

permutation s1_begin s1_insert s2_before s1_commit s2_after
