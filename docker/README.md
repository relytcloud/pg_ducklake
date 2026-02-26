# pg_ducklake Docker Image

This image extends the official Postgres image with `pg_duckdb` and `pg_ducklake`
pre-installed and initializes both extensions in the default database.

## Local build

```bash
./scripts/docker-build.sh
```

Environment variables:

- `POSTGRES_VERSION` (default: `18`)
- `REPO` (default: `pgducklake/pgducklake`)
- `PUSH=1` to push instead of loading locally

## Usage

Use the image like the official Postgres image:

```bash
docker run -d -e POSTGRES_PASSWORD=duckdb pgducklake/pgducklake:18-main
```
