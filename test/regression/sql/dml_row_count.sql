-- Test DML completion tags report correct row counts (issue #7).
-- \set QUIET off overrides pg_regress's -q flag so psql prints tags.
\set QUIET off

CREATE TABLE rc (id int, val text) USING ducklake;

-- INSERT single row
INSERT INTO rc VALUES (1, 'one');

-- INSERT multiple rows
INSERT INTO rc VALUES (2, 'two'), (3, 'three'), (4, 'four');

-- UPDATE subset
UPDATE rc SET val = 'updated' WHERE id <= 2;

-- DELETE single row
DELETE FROM rc WHERE id = 4;

-- Verify final state
SELECT * FROM rc ORDER BY id;

DROP TABLE rc;
