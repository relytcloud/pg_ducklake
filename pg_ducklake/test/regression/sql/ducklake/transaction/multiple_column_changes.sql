-- Upstream: test/sql/transaction/multiple_column_changes.test
-- Type and default changes on one column must both survive the transaction.

CREATE TABLE upstream_message (
    id integer NOT NULL,
    user_id integer NOT NULL
) USING ducklake;

BEGIN;
ALTER TABLE upstream_message ALTER COLUMN user_id TYPE bigint;
ALTER TABLE upstream_message ALTER COLUMN user_id SET DEFAULT 123;
COMMIT;

CALL ducklake.recycle_ddb();
INSERT INTO upstream_message (id) VALUES (1);
SELECT * FROM upstream_message;

DROP TABLE upstream_message;
