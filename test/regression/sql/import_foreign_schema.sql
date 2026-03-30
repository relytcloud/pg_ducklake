-- Regression tests for IMPORT FOREIGN SCHEMA with ducklake_fdw

-- Setup: create managed DuckLake tables
CREATE TABLE ifs_orders (id int, customer text, amount float8) USING ducklake;
INSERT INTO ifs_orders VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0);

CREATE TABLE ifs_products (id int, name text, price numeric(10,2)) USING ducklake;
INSERT INTO ifs_products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99);

CREATE TABLE ifs_logs (ts timestamp, msg text) USING ducklake;
INSERT INTO ifs_logs VALUES ('2024-01-01 00:00:00', 'start');

CREATE TABLE ifs_variant (id int, v ducklake.variant) USING ducklake;
INSERT INTO ifs_variant VALUES (1, '{"name": "alice"}'), (2, '[1, 2, 3]');

-- Create FDW server
CREATE SERVER ifs_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake');

-- Create target schema for imported tables
CREATE SCHEMA ifs_target;

-- Basic IMPORT FOREIGN SCHEMA: import all tables from public
IMPORT FOREIGN SCHEMA public FROM SERVER ifs_server INTO ifs_target;

-- Verify only user tables are imported (no ducklake metadata tables)
\d ifs_target.*

-- Verify imported tables exist and are queryable
SELECT * FROM ifs_target.ifs_orders ORDER BY id;
SELECT * FROM ifs_target.ifs_products ORDER BY id;
SELECT * FROM ifs_target.ifs_logs;

-- Verify variant column is imported correctly
SELECT * FROM ifs_target.ifs_variant ORDER BY id;

-- Clean up imported tables for next test
DROP SCHEMA ifs_target CASCADE;

-- LIMIT TO: import only specific tables
CREATE SCHEMA ifs_target;
IMPORT FOREIGN SCHEMA public LIMIT TO (ifs_orders, ifs_products)
    FROM SERVER ifs_server INTO ifs_target;

-- Should exist
SELECT count(*) FROM ifs_target.ifs_orders;
SELECT count(*) FROM ifs_target.ifs_products;

-- Should not exist
SELECT count(*) FROM ifs_target.ifs_logs;

DROP SCHEMA ifs_target CASCADE;

-- EXCEPT: import all except specific tables
CREATE SCHEMA ifs_target;
IMPORT FOREIGN SCHEMA public EXCEPT (ifs_logs)
    FROM SERVER ifs_server INTO ifs_target;

-- Should exist
SELECT count(*) FROM ifs_target.ifs_orders;
SELECT count(*) FROM ifs_target.ifs_products;

-- Should not exist
SELECT count(*) FROM ifs_target.ifs_logs;

DROP SCHEMA ifs_target CASCADE;

-- Cleanup
DROP SERVER ifs_server;
DROP TABLE ifs_variant;
DROP TABLE ifs_logs;
DROP TABLE ifs_products;
DROP TABLE ifs_orders;
