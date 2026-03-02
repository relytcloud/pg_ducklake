setup
{
  CREATE TABLE iso_conc_write_t (id int) USING ducklake;
}

session s1
step s1_begin  { BEGIN; }
step s1_insert { INSERT INTO iso_conc_write_t VALUES (1); }
step s1_count  { SELECT count(*) FROM iso_conc_write_t; }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_insert { INSERT INTO iso_conc_write_t VALUES (2); }
step s2_count  { SELECT count(*) FROM iso_conc_write_t; }
step s2_commit { COMMIT; }

teardown
{
  DROP TABLE iso_conc_write_t;
}

# Serial writes: s1 commits before s2 begins — both succeed, s2 sees s1's row
permutation s1_begin s1_insert s1_commit s2_begin s2_insert s2_count s2_commit

# Concurrent writes: both open before either commits — s1 commits first.
# s2_count sees only its own insert (snapshot isolation), s2_commit retries with new snapshot_id.
permutation s1_begin s2_begin s1_insert s2_insert s1_commit s2_count s2_commit

# Concurrent writes: both open before either commits — s2 commits first.
# s1_count sees only its own insert (snapshot isolation), s1_commit retries with new snapshot_id.
permutation s1_begin s2_begin s1_insert s2_insert s2_commit s1_count s1_commit
