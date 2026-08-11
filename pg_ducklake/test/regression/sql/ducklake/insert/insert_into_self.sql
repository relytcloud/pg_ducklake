-- Upstream: test/sql/insert/insert_into_self.test
-- Repeated INSERT ... SELECT from the target must use a stable source snapshot.

CREATE TABLE upstream_insert_self (i integer, j text) USING ducklake;

BEGIN;
INSERT INTO upstream_insert_self VALUES (1, '2'), (NULL, '3');
INSERT INTO upstream_insert_self SELECT * FROM upstream_insert_self;
INSERT INTO upstream_insert_self SELECT * FROM upstream_insert_self;
INSERT INTO upstream_insert_self SELECT * FROM upstream_insert_self;
INSERT INTO upstream_insert_self
SELECT a.i, a.j FROM upstream_insert_self a CROSS JOIN upstream_insert_self b;
COMMIT;

SELECT sum(i), sum(length(j)), count(*) FROM upstream_insert_self;

DROP TABLE upstream_insert_self;
