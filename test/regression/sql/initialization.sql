-- Test DuckLake initialization

-- Extension should create ducklake schema
SELECT nspname FROM pg_namespace WHERE nspname = 'ducklake';

-- Extension should create ducklake access method
SELECT amname FROM pg_am WHERE amname = 'ducklake';

-- Extension should create _initialize function
SELECT proname FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'ducklake' AND p.proname = '_initialize';

-- Extension should create _am_handler function
SELECT proname FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'ducklake' AND p.proname = '_am_handler';

-- Try creating a simple table with ducklake access method
CREATE TABLE test_init (id int, value text) USING ducklake;

-- Verify table was created with ducklake AM
SELECT relname, am.amname
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'test_init';

DROP TABLE test_init;
