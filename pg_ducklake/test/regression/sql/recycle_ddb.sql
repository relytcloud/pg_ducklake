-- Verify DuckLake works after ducklake.recycle_ddb() destroys and
-- recreates the DuckDB instance (GitHub issue #81).

CREATE TABLE t (a int, b text) USING ducklake;
INSERT INTO t VALUES (1, 'before');
SELECT * FROM t;

CALL ducklake.recycle_ddb();

-- The metadata manager factory is registered in a process-global static
-- map, so it survives the recycle. The catalog must be re-attached.
SELECT * FROM t;
INSERT INTO t VALUES (2, 'after');
SELECT * FROM t ORDER BY a;

DROP TABLE t;

-- A catalog can store a non-default data path, for example after migration to
-- S3. Reattaching must read that path instead of forcing the local default.
CREATE TEMP TABLE original_ducklake_data_path AS
SELECT value FROM ducklake.ducklake_metadata WHERE key = 'data_path';

UPDATE ducklake.ducklake_metadata
SET value = '/tmp/pg_ducklake_relocated/'
WHERE key = 'data_path';

-- Recycling exercises the same attach path as a fresh backend.
CALL ducklake.recycle_ddb();
SELECT value = '/tmp/pg_ducklake_relocated/' AS relocated_data_path
FROM ducklake.options()
WHERE option_name = 'data_path';

UPDATE ducklake.ducklake_metadata
SET value = (SELECT value FROM original_ducklake_data_path)
WHERE key = 'data_path';
CALL ducklake.recycle_ddb();

SELECT options.value = original.value AS restored_data_path
FROM ducklake.options() AS options
CROSS JOIN original_ducklake_data_path AS original
WHERE options.option_name = 'data_path';

DROP TABLE original_ducklake_data_path;
