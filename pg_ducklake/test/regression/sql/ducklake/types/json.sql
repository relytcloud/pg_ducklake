-- Upstream: test/sql/types/json.test
CREATE TABLE upstream_type_json (id integer, value json) USING ducklake;
SELECT count(*) FROM upstream_type_json;
INSERT INTO upstream_type_json VALUES (1, '{"key": "value"}');
SELECT id, value::text, pg_typeof(value) FROM upstream_type_json ORDER BY id;
CALL ducklake.recycle_ddb();
SELECT id, value::text, pg_typeof(value) FROM upstream_type_json ORDER BY id;
DROP TABLE upstream_type_json;
