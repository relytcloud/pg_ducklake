-- Test for INSERT ... UNNEST

CREATE TABLE insert_unnest (
    id INT
) USING ducklake;

INSERT INTO insert_unnest
SELECT * FROM UNNEST(ARRAY[1, 2, 3]);

SELECT * FROM insert_unnest ORDER BY id;

DROP TABLE insert_unnest;

CREATE TABLE insert_unnest (
    id INT,
    val TEXT
) USING ducklake;

-- Test 1: Multi-column UNNEST (zipping)
INSERT INTO insert_unnest
SELECT UNNEST(ARRAY[1, 2, 3]), UNNEST(ARRAY['a', 'b', 'c']);

SELECT * FROM insert_unnest ORDER BY id;

-- Test 2: Array Literal Handling
INSERT INTO insert_unnest
SELECT UNNEST(ARRAY[4, 5]), UNNEST(ARRAY['d', 'e']::text[]);

SELECT * FROM insert_unnest WHERE id > 3 ORDER BY id;

-- Clean up
DROP TABLE insert_unnest;
