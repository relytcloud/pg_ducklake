-- Regression: COMMIT in a backend that never touched DuckLake must not crash.
-- DucklakeUtilityHook previously triggered lazy DuckDB initialization on any
-- COMMIT, causing a missing-snapshot assertion inside ReadDuckdbExtensions.
CREATE TABLE nl_commit_heap (id int);
BEGIN;
INSERT INTO nl_commit_heap VALUES (1);
COMMIT;
SELECT count(*) FROM nl_commit_heap;
DROP TABLE nl_commit_heap;
