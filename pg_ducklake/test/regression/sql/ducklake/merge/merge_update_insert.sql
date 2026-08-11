-- Upstream: test/sql/merge/merge_update_insert.test
-- MERGE must update matches and insert nonmatches in one statement.

CREATE TABLE upstream_merge_stock (item_id integer, balance integer) USING ducklake;
INSERT INTO upstream_merge_stock VALUES (10, 2200), (20, 1900);
CREATE TABLE upstream_merge_buy (item_id integer, volume integer) USING ducklake;
INSERT INTO upstream_merge_buy VALUES (10, 1000), (30, 300);

SELECT * FROM upstream_merge_stock ORDER BY item_id;
MERGE INTO upstream_merge_stock AS s
USING upstream_merge_buy AS b ON s.item_id = b.item_id
WHEN MATCHED THEN UPDATE SET balance = s.balance + b.volume
WHEN NOT MATCHED THEN INSERT (item_id, balance) VALUES (b.item_id, b.volume);
SELECT * FROM upstream_merge_stock ORDER BY item_id;

DROP TABLE upstream_merge_buy;
DROP TABLE upstream_merge_stock;
