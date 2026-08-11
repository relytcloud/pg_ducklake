-- Upstream: test/sql/types/json_alter_table.test
CREATE TABLE upstream_json_alter (
  id bigint,
  status text,
  batch_id text
) USING ducklake;
ALTER TABLE upstream_json_alter ADD COLUMN validation_errors json;
INSERT INTO upstream_json_alter VALUES
  (1, 'failed', 'batch-1', '{"field": "invalid"}');
SELECT id, status, batch_id, validation_errors::text
FROM upstream_json_alter ORDER BY id;
DROP TABLE upstream_json_alter;
