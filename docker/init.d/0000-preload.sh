#!/bin/bash
# Enable shared_preload_libraries AFTER initdb (not in postgresql.conf.sample)
# to avoid loading extensions during initdb's post-bootstrap phase, which
# crashes on PG15+ because catalog entries don't exist yet.
echo "shared_preload_libraries='pg_duckdb,pg_ducklake'" >> "$PGDATA/postgresql.conf"
pg_ctl -D "$PGDATA" -m fast -w restart
