# Dispatched reads see committed DuckLake data while writers run in-process.
# Separate blocks: a new schema, its table and its data committed in one
# transaction get a mis-stored schema path on main (unrelated to the worker).
setup { CREATE TABLE swc (i int) USING ducklake; }
setup { INSERT INTO swc SELECT g FROM generate_series(1, 100) g; }

teardown
{
	DROP TABLE swc;
}

session s1
setup { SET ducklake.use_shared_worker = on; }
step s1_count { SELECT count(*) FROM swc; }
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO swc VALUES (101); }
step s1_commit { COMMIT; }

session s2
setup { SET ducklake.use_shared_worker = on; }
step s2_count { SELECT count(*) FROM swc; }
step s2_sum { SELECT sum(i) FROM swc; }

session s3
setup { SET ducklake.use_shared_worker = off; }
step s3_insert { INSERT INTO swc VALUES (102); }

# Two backends share the worker.
permutation s1_count s2_count s2_sum s1_count
# A write pending in s1's transaction is invisible through the worker until it commits.
permutation s1_begin s1_insert s2_count s1_commit s2_count
# An in-process autocommit write is visible to the next dispatched reads.
permutation s3_insert s1_count s2_sum
