-- Upstream: test/sql/alter/promote_type_all.test
-- Skip: PostgreSQL lacks DuckDB's tinyint, hugeint, and unsigned integer type families.
CREATE TABLE upstream_promote_int (c smallint) USING ducklake;
ALTER TABLE upstream_promote_int ALTER COLUMN c TYPE integer;
ALTER TABLE upstream_promote_int ALTER COLUMN c TYPE bigint;
ALTER TABLE upstream_promote_int ALTER COLUMN c TYPE integer;
CREATE TABLE upstream_promote_float (c real) USING ducklake;
ALTER TABLE upstream_promote_float ALTER COLUMN c TYPE double precision;
ALTER TABLE upstream_promote_float ALTER COLUMN c TYPE real;
CREATE TABLE upstream_promote_ts (c timestamp) USING ducklake;
ALTER TABLE upstream_promote_ts ALTER COLUMN c TYPE timestamptz;
ALTER TABLE upstream_promote_ts ALTER COLUMN c TYPE timestamp;
CREATE TABLE upstream_promote_reject (c integer) USING ducklake;
ALTER TABLE upstream_promote_reject ALTER COLUMN c TYPE text;
ALTER TABLE upstream_promote_reject ALTER COLUMN c TYPE boolean;
CREATE TABLE upstream_promote_same (c integer) USING ducklake;
ALTER TABLE upstream_promote_same ALTER COLUMN c TYPE integer;
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name IN ('upstream_promote_int', 'upstream_promote_float',
                     'upstream_promote_ts', 'upstream_promote_reject', 'upstream_promote_same')
ORDER BY table_name, ordinal_position;
DROP TABLE upstream_promote_int, upstream_promote_float, upstream_promote_ts,
           upstream_promote_reject, upstream_promote_same;
