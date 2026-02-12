# pg_ducklake

PostgreSQL extension that integrates DuckLake with PostgreSQL via pg_duckdb.

## Documentation

See [CLAUDE.md](CLAUDE.md) for comprehensive documentation including:
- Build instructions
- Architecture overview
- Development workflow
- Testing

## Build from Source

```bash
git submodule update --init --recursive
# (Optional)
make install_pg_duckdb
make install
```
