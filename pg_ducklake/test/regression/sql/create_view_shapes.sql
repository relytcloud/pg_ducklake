-- CREATE VIEW over query shapes that carry raw JoinExpr / SubLink nodes.
-- The utility hook parse-analyzes the view body to detect duckdb_row columns;
-- doing that on the original raw tree corrupted it (JoinExpr larg/rarg and
-- SubLink->subselect are rewritten in place), so DefineView's own parse
-- analysis then failed. These must all succeed and return correct rows.

-- Heap tables --------------------------------------------------------------
CREATE TABLE h1 (id int PRIMARY KEY, val int);
CREATE TABLE h2 (id int PRIMARY KEY, label text);
INSERT INTO h1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO h2 VALUES (1, 'a'), (2, 'b');

-- Explicit INNER JOIN
CREATE VIEW hv_join AS
    SELECT h1.id, h2.label FROM h1 JOIN h2 ON h2.id = h1.id;
SELECT * FROM hv_join ORDER BY id;

-- Explicit LEFT JOIN
CREATE VIEW hv_left_join AS
    SELECT h1.id, h2.label FROM h1 LEFT JOIN h2 ON h2.id = h1.id;
SELECT * FROM hv_left_join ORDER BY id;

-- EXISTS sublink in WHERE
CREATE VIEW hv_exists AS
    SELECT h1.id FROM h1 WHERE EXISTS (SELECT 1 FROM h2 WHERE h2.id = h1.id);
SELECT * FROM hv_exists ORDER BY id;

-- Correlated scalar subquery in the target list
CREATE VIEW hv_scalar_sub AS
    SELECT h1.id, (SELECT h2.label FROM h2 WHERE h2.id = h1.id) AS label FROM h1;
SELECT * FROM hv_scalar_sub ORDER BY id;

-- IN (SELECT ...) sublink
CREATE VIEW hv_in_sub AS
    SELECT h1.id FROM h1 WHERE h1.id IN (SELECT h2.id FROM h2);
SELECT * FROM hv_in_sub ORDER BY id;

-- CTE
CREATE VIEW hv_cte AS
    WITH c AS (SELECT id, label FROM h2)
    SELECT h1.id, c.label FROM h1 JOIN c ON c.id = h1.id;
SELECT * FROM hv_cte ORDER BY id;

-- UNION set operation
CREATE VIEW hv_union AS
    SELECT id FROM h1 UNION SELECT id FROM h2;
SELECT * FROM hv_union ORDER BY id;

-- LATERAL join
CREATE VIEW hv_lateral AS
    SELECT h1.id, sub.label
    FROM h1 LEFT JOIN LATERAL (SELECT h2.label FROM h2 WHERE h2.id = h1.id) sub ON true;
SELECT * FROM hv_lateral ORDER BY id;

-- CREATE OR REPLACE VIEW over a JOIN
CREATE OR REPLACE VIEW hv_join AS
    SELECT h1.id, h2.label, h1.val FROM h1 JOIN h2 ON h2.id = h1.id;
SELECT * FROM hv_join ORDER BY id;

-- DuckLake tables ----------------------------------------------------------
CREATE TABLE d1 (id int, val int) USING ducklake;
CREATE TABLE d2 (id int, label text) USING ducklake;
INSERT INTO d1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO d2 VALUES (1, 'a'), (2, 'b');

CREATE VIEW dv_join AS
    SELECT d1.id, d2.label FROM d1 JOIN d2 ON d2.id = d1.id;
SELECT * FROM dv_join ORDER BY id;

CREATE VIEW dv_exists AS
    SELECT d1.id FROM d1 WHERE EXISTS (SELECT 1 FROM d2 WHERE d2.id = d1.id);
SELECT * FROM dv_exists ORDER BY id;

-- Cleanup
DROP VIEW hv_join, hv_left_join, hv_exists, hv_scalar_sub, hv_in_sub,
    hv_cte, hv_union, hv_lateral, dv_join, dv_exists;
DROP TABLE h1, h2;
DROP TABLE d1, d2;
