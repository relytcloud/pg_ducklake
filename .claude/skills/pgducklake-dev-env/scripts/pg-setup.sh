#!/usr/bin/env bash
# One-time setup: clone PG source and build each version into $(workdir)/pg-{version}/.
# Run from the repo root: bash scripts/pg-setup.sh
# Override with: PG_SRC_DIR=... PG_VERSIONS="17 18" bash scripts/pg-setup.sh

set -euo pipefail

WORKDIR=$(git rev-parse --show-toplevel)
PG_VERSIONS=${PG_VERSIONS:-"18"}

# Determine compiler cache
if command -v sccache >/dev/null 2>&1; then
    CACHE_CC="sccache"
elif command -v ccache >/dev/null 2>&1; then
    CACHE_CC="ccache"
else
    echo "Warning: no compiler cache (ccache/sccache) found. Build will be slow." >&2
    CACHE_CC=""
fi

# Determine PG source directory
if [ -z "${PG_SRC_DIR:-}" ]; then
    default_dir="$HOME/.dev/pg-src"
    printf "PostgreSQL source directory [%s]: " "$default_dir"
    read -r input
    PG_SRC_DIR="${input:-$default_dir}"
fi
export PG_SRC_DIR

# Suppress .pg-build/ from git status
exclude_file=$(git rev-parse --git-common-dir)/info/exclude
grep -qF '.pg-build/' "$exclude_file" 2>/dev/null || echo '.pg-build/' >> "$exclude_file"

mkdir -p "$(dirname "$PG_SRC_DIR")"

# Clone PG source as a bare repo (no checkout; worktrees provide source trees)
if [ ! -d "$PG_SRC_DIR" ]; then
    echo "==> Cloning PostgreSQL source to $PG_SRC_DIR ..."
    git clone --bare https://github.com/postgres/postgres.git "$PG_SRC_DIR"
fi

ncpu=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

for version in $PG_VERSIONS; do
    src_wt="$PG_SRC_DIR/wt/$version"
    install_dir="$WORKDIR/pg-$version"

    # Create source worktree if missing
    if [ ! -d "$src_wt" ]; then
        branch="REL_${version}_STABLE"
        echo "==> Fetching PG $version ($branch) ..."
        git -C "$PG_SRC_DIR" fetch --depth=1 origin "refs/heads/$branch:refs/heads/$branch"
        git -C "$PG_SRC_DIR" worktree add "$src_wt" "$branch"
    fi

    if [ -f "$install_dir/bin/pg_config" ]; then
        echo "==> PG $version already installed at $install_dir"
        continue
    fi

    build_dir="$WORKDIR/.pg-build/$version"
    mkdir -p "$build_dir"

    echo "==> Configuring PG $version (prefix=$install_dir) ..."
    cd "$build_dir"
    if [ -n "$CACHE_CC" ]; then
        export CC="$CACHE_CC cc" CXX="$CACHE_CC c++"
        export CMAKE_C_COMPILER_LAUNCHER="$CACHE_CC" CMAKE_CXX_COMPILER_LAUNCHER="$CACHE_CC"
    fi
    "$src_wt/configure" --prefix="$install_dir" \
        --without-icu --without-readline --without-libxml \
        >/dev/null

    echo "==> Building PG $version (j=$ncpu, cache=${CACHE_CC:-none}) ..."
    make -j"$ncpu" >/dev/null
    make install >/dev/null
    cd - >/dev/null

    echo "    Installed: $install_dir/bin/pg_config"
done

echo ""
echo "Done. Build pg_ducklake with:"
for version in $PG_VERSIONS; do
    echo "  PG_CONFIG=$WORKDIR/pg-$version/bin/pg_config make install"
done
