-- Operators on ducklake.unresolved_type (r['col'] subscript results).
-- These parse on the PG side via the dummy operator stubs and execute in
-- DuckDB, where the real types resolve.

-- Binary math and prefix operators.
SELECT r['PassengerId'] + 1 AS next_id,
       r['Fare'] * 2 AS double_fare,
       r['Fare'] / 2 AS half_fare,
       -r['SibSp'] AS neg_sibsp
FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
WHERE r['PassengerId'] <= 3
ORDER BY r['PassengerId'];

-- ClickBench-style epoch arithmetic: unresolved * interval.
SELECT ('epoch'::timestamp + (r['PassengerId'] * interval '1 second'))::timestamp AS ts
FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
WHERE r['PassengerId'] = 42;

-- Comparisons with unresolved on either side.
SELECT count(*) FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
WHERE r['Age'] >= 70;
SELECT count(*) FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
WHERE 70 <= r['Age'];
SELECT count(*) FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
WHERE r['Embarked'] <> 'S' AND r['Pclass'] = 1 AND r['Fare'] > 500;

-- GROUP BY and ORDER BY directly on subscript results (hash/btree opclasses).
SELECT r['Pclass'] AS pclass, count(*) AS n
FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
GROUP BY r['Pclass']
ORDER BY r['Pclass'];
