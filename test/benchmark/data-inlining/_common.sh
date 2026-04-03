#!/usr/bin/env bash
# Shared helpers for data-inlining benchmark setup scripts.

# One partition of ClickBench HITS (~140 MB, ~1M rows) -- real data, fast download.
HITS_PARTITION_URL="https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet"
HITS_PARQUET_DEFAULT="/tmp/hits_0.parquet"

ensure_docker() {
    command -v docker &>/dev/null && return
    echo "Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io
    sudo systemctl start docker
    sudo usermod -aG docker "$USER"
    echo "Docker installed. You may need to re-login for group changes."
}

ensure_uv() {
    command -v uv &>/dev/null && return
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
}

# download_hits_parquet [TARGET]
#   Downloads one ClickBench HITS partition (~140 MB) if not already present.
download_hits_parquet() {
    local target="${1:-$HITS_PARQUET_DEFAULT}"
    if [ -f "$target" ]; then
        echo "HITS parquet already at $target ($(du -h "$target" | cut -f1))"
        return 0
    fi
    echo "Downloading HITS partition to $target (~140 MB)..."
    wget --no-verbose --continue "$HITS_PARTITION_URL" -O "$target"
}

# start_container NAME IMAGE PORT [EXTRA_DOCKER_ARGS...]
#   Removes any existing container with NAME, starts a new one, and waits
#   for PostgreSQL to accept connections (up to 60 s).
start_container() {
    local name="$1" image="$2" port="$3"
    shift 3

    ensure_docker
    ensure_uv

    docker rm -f "$name" 2>/dev/null || true

    echo "Starting $name ($image) on port $port..."
    docker run -d --name "$name" \
        -p "${port}:5432" \
        -e POSTGRES_PASSWORD=duckdb \
        "$@" \
        "$image"

    echo -n "Waiting for PostgreSQL..."
    for _ in $(seq 60); do
        if docker exec "$name" pg_isready -U postgres &>/dev/null; then
            echo " ready."
            return 0
        fi
        echo -n "."
        sleep 1
    done

    echo " TIMEOUT"
    docker logs "$name" 2>&1 | tail -30
    return 1
}
