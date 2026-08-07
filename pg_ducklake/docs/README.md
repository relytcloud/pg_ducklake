# pg_ducklake Documentation

## Technical Reference

- [SQL Objects](../sql/pg_ducklake--1.0.0.sql) -- all SQL objects, functions, and procedures (documented inline)
- [Data Types](data_types.md) -- DuckLake types and inlined data support
- [Settings](settings.md) -- PostgreSQL GUCs and DuckLake catalog options
- [Access Control](access_control.md) -- roles, permissions, and known gaps

## Feature Coverage

- [DuckLake Feature Coverage](ducklake_feature_coverage.md) -- upstream DuckLake features vs pg_ducklake support

## Design Proposals

- [RFC 001: PostgreSQL-Native DuckLake Writer](rfc-001-postgres-native-ducklake-writer.md) -- implemented insert-once writer and publication retry protocol

## How-to Guides

- [Foreign Data Wrapper](foreign_data_wrapper.md) -- access remote DuckLake catalogs (full DML) and frozen snapshots (read-only)
- [Building from Source](compilation.md) -- compile and install on Ubuntu and macOS
