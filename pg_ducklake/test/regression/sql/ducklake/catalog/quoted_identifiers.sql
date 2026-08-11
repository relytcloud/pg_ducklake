-- Upstream: test/sql/catalog/quoted_identifiers.test

CREATE TABLE "upstream quoted 'table' ""name""" (
    "quoted 'column' ""name""" integer
) USING ducklake;
SELECT "quoted 'column' ""name""" FROM "upstream quoted 'table' ""name""";
INSERT INTO "upstream quoted 'table' ""name""" VALUES (42);
SELECT "quoted 'column' ""name""" FROM "upstream quoted 'table' ""name""";
DROP TABLE "upstream quoted 'table' ""name""";
