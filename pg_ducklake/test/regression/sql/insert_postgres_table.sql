-- INSERT INTO a heap table SELECT ... FROM a DuckLake table runs the SELECT in DuckDB

CREATE TABLE ipt_src (id int, name text, amount numeric(12,2)) USING ducklake;
INSERT INTO ipt_src VALUES (1, 'alice', 100.50), (2, 'bob', 200.75), (3, 'carol', 50.25);

CREATE TABLE ipt_dst (id int PRIMARY KEY, name varchar(5), amount numeric(12,2), note text DEFAULT 'dflt');

INSERT INTO ipt_dst (id, name, amount) SELECT id, name, amount FROM ipt_src WHERE id < 3;
SELECT * FROM ipt_dst ORDER BY id;

-- Column order differs from the table definition; RETURNING
INSERT INTO ipt_dst (amount, id, name) SELECT amount, id, name FROM ipt_src WHERE id = 3 RETURNING *;

-- ON CONFLICT
INSERT INTO ipt_dst (id, name) SELECT id, 'upd' FROM ipt_src ON CONFLICT (id) DO UPDATE SET name = excluded.name;
INSERT INTO ipt_dst (id, name) SELECT id, 'skip' FROM ipt_src ON CONFLICT DO NOTHING;
SELECT * FROM ipt_dst ORDER BY id;

-- Assignment coercion errors like a Postgres INSERT
INSERT INTO ipt_dst (id, name) SELECT id + 10, name || 'xxxxx' FROM ipt_src;

-- CTE on the INSERT, join with a heap table
TRUNCATE ipt_dst;
CREATE TABLE ipt_heap (id int, tag text);
INSERT INTO ipt_heap VALUES (1, 'one'), (2, 'two');
WITH s AS (SELECT * FROM ipt_src)
INSERT INTO ipt_dst (id, name, note) SELECT s.id, s.name, h.tag FROM s JOIN ipt_heap h ON s.id = h.id;
SELECT * FROM ipt_dst ORDER BY id;

-- Inside a transaction, visible to later Postgres reads
BEGIN;
INSERT INTO ipt_dst (id, name) SELECT id + 100, name FROM ipt_src;
SELECT count(*) FROM ipt_dst;
ROLLBACK;
SELECT count(*) FROM ipt_dst;

-- Other writes to heap tables that need DuckDB are rejected
UPDATE ipt_dst SET name = s.name FROM ipt_src s WHERE ipt_dst.id = s.id;
DELETE FROM ipt_dst USING ipt_src s WHERE ipt_dst.id = s.id;
INSERT INTO ipt_dst (id) VALUES ((SELECT max(id) FROM ipt_src));

DROP TABLE ipt_src, ipt_dst, ipt_heap;
