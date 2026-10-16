-- System columns of DuckLake tables: plain SELECT outputs are computed in PG,
-- other uses raise feature_not_supported.
CREATE TABLE sc (id int, v text) USING ducklake;
INSERT INTO sc VALUES (1, 'a'), (2, 'b');

SELECT t.*, ctid FROM sc t ORDER BY id LIMIT 1001;
SELECT id, xmin, xmax, cmin, cmax FROM sc ORDER BY id;
SELECT tableoid AS sc_oid FROM sc LIMIT 1 \gset
SELECT :sc_oid = 'sc'::regclass::oid AS tableoid_ok;
SELECT ctid, xmin, tableoid FROM sc \gdesc
SELECT ctid FROM sc;

-- Unsupported uses
SELECT tableoid::regclass, id FROM sc ORDER BY id;
SELECT ctid, count(*) FROM sc GROUP BY ctid;
SELECT id FROM sc WHERE ctid = '(0,1)';
SELECT id FROM sc ORDER BY ctid;
SELECT ctid::text FROM sc;
SELECT count(ctid) FROM sc;
SELECT id FROM sc WHERE id IN (SELECT id FROM sc WHERE xmin IS NOT NULL);
UPDATE sc SET v = 'c' WHERE ctid = '(0,1)';
DELETE FROM sc WHERE id = 1 RETURNING ctid;

-- Heap table system columns in a DuckDB-executed query
CREATE TABLE sc_heap (id int);
INSERT INTO sc_heap VALUES (1);
SELECT h.ctid, s.id FROM sc s JOIN sc_heap h USING (id);

SELECT * FROM sc ORDER BY id;
DROP TABLE sc, sc_heap;
