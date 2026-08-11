-- Upstream: test/sql/data_inlining/partition_inlining.test
CALL ducklake.set_option('data_inlining_row_limit', 20);
CREATE TABLE upstream_inline_part_move (id integer, region text, amount double precision) USING ducklake;
CALL ducklake.set_partition('upstream_inline_part_move'::regclass, 'region');
INSERT INTO upstream_inline_part_move VALUES
 (1,'east',100), (2,'east',200), (3,'west',300), (4,'west',400);
UPDATE upstream_inline_part_move SET region = 'west' WHERE id = 1;
UPDATE upstream_inline_part_move SET region = 'central' WHERE id = 2;
SELECT * FROM upstream_inline_part_move ORDER BY id;
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_part_move'::regclass);
SELECT * FROM upstream_inline_part_move ORDER BY id;
DELETE FROM upstream_inline_part_move WHERE id = 1;
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_part_move'::regclass);
SELECT * FROM upstream_inline_part_move ORDER BY id;
CREATE TABLE upstream_inline_part_year (id integer, ts timestamp, amount double precision) USING ducklake;
CALL ducklake.set_partition('upstream_inline_part_year'::regclass, 'year(ts)');
INSERT INTO upstream_inline_part_year VALUES
 (1,'2020-06-15',10), (2,'2020-11-01',20), (3,'2021-03-20',30);
UPDATE upstream_inline_part_year SET ts = '2021-06-15' WHERE id = 1;
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_part_year'::regclass);
SELECT * FROM upstream_inline_part_year ORDER BY id;
DROP TABLE upstream_inline_part_move, upstream_inline_part_year;
CALL ducklake.set_option('data_inlining_row_limit', 0);
