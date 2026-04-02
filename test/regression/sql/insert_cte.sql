-- Test INSERT INTO with CTEs (WITH ... SELECT) -- issue #148

CREATE TABLE cte_src (id int, name text, amount numeric(12,2), ts timestamp) USING ducklake;
CREATE TABLE cte_dst (id int, name text, total numeric(12,2), category text, processed_at timestamp) USING ducklake;

INSERT INTO cte_src VALUES
    (1, 'alice', 100.50, '2024-01-01 10:00:00'),
    (2, 'bob',   200.75, '2024-01-02 11:00:00'),
    (3, 'alice', 50.25,  '2024-01-03 12:00:00'),
    (4, 'carol', 300.00, '2024-01-04 13:00:00'),
    (5, 'bob',   150.00, '2024-01-05 14:00:00');

-- Complex chained CTE ETL pattern
INSERT INTO cte_dst
WITH t1 AS (
    SELECT id, name, amount, ts,
           CASE WHEN amount > 200 THEN 'high'
                WHEN amount > 100 THEN 'medium'
                ELSE 'low' END AS category
    FROM cte_src
    WHERE ts >= '2024-01-01'::timestamp
),
t2 AS (
    SELECT name, SUM(amount) AS total, category, MAX(ts) AS last_ts
    FROM t1
    GROUP BY name, category
),
t3 AS (
    SELECT ROW_NUMBER() OVER (ORDER BY total DESC) AS id,
           name, total, category, last_ts AS processed_at
    FROM t2
)
SELECT * FROM t3;

SELECT * FROM cte_dst ORDER BY id;

DROP TABLE cte_dst;
DROP TABLE cte_src;
