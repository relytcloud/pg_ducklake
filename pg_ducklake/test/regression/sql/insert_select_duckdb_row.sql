-- INSERT INTO a DuckLake table ... SELECT r['col'] FROM read_csv(...) r, where read_csv() is the one
-- pg_duckdb provides when it is co-installed (the coexistence run). pg_ducklake plans such an INSERT
-- itself, and used to deparse pg_duckdb's duckdb.row as one opaque column (r(r) / r.r['col']), which
-- DuckDB rejected with array_extract(BIGINT, VARCHAR). The statement picks whichever read_csv is
-- visible, so every run executes it.

CREATE TABLE passengers (id int, name text, fare float8) USING ducklake;

DO $$
DECLARE
    fn text := 'ducklake.read_csv';
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'read_csv' AND pronamespace = 'public'::regnamespace) THEN
        fn := 'public.read_csv';
    END IF;
    EXECUTE format($q$
        INSERT INTO passengers
        SELECT r['PassengerId']::int, r['Name']::text, r['Fare']::float8
        FROM %s('/tmp/pg_ducklake_testdata/titanic.csv') r
        WHERE r['PassengerId']::int <= 100
    $q$, fn);
END $$;

SELECT count(*), min(id), max(id), round(sum(fare)::numeric, 2) FROM passengers;

-- Explicitly the ducklake function, and through a subquery.
INSERT INTO passengers
SELECT x.id, x.name, x.fare FROM (
    SELECT r['PassengerId']::int AS id, r['Name']::text AS name, r['Fare']::float8 AS fare
    FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv') r
) x WHERE x.id BETWEEN 101 AND 110;

SELECT count(*), max(id) FROM passengers;

DROP TABLE passengers;
