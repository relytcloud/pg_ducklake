-- Regression tests for ducklake_fdw (frozen DuckLake mode)

-- Validation: frozen_url + dbname -> error
CREATE SERVER frozen_bad_1
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/test.ducklake', dbname 'mydb');

-- Validation: frozen_url + metadata_schema -> error
CREATE SERVER frozen_bad_2
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/test.ducklake', metadata_schema 'myschema');

-- Create frozen server
CREATE SERVER frozen_space
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url '/tmp/pg_ducklake_testdata/space.ducklake');

-- Error: columns specified
CREATE FOREIGN TABLE frozen_bad_cols (id int)
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'missions');

-- Create foreign table with auto-inferred columns
CREATE FOREIGN TABLE frozen_missions ()
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'missions');

-- SELECT + ORDER BY + LIMIT
SELECT * FROM frozen_missions ORDER BY name LIMIT 5;

-- Aggregation
SELECT count(*) FROM frozen_missions;

-- READ-ONLY enforcement
INSERT INTO frozen_missions VALUES (1, 'test');
UPDATE frozen_missions SET name = 'test' WHERE mission_id = 1;
DELETE FROM frozen_missions WHERE mission_id = 1;

-- Error: non-existent table in frozen ducklake
CREATE FOREIGN TABLE frozen_nonexistent ()
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'no_such_table');

-- Cleanup
DROP FOREIGN TABLE frozen_missions;
DROP SERVER frozen_space;
