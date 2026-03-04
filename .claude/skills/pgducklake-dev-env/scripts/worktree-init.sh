#!/usr/bin/env bash
# Per-worktree setup: initialize submodules and install PostgreSQL.
# Run from inside the new worktree after: git worktree add <path> <branch>
# Override with: PG_SRC_DIR=... PG_VERSIONS="17 18" bash scripts/worktree-init.sh

set -euo pipefail

WORKTREE=$(git rev-parse --show-toplevel)
MAIN=$(git worktree list --porcelain | awk 'NR==1{print $2}')
PG_SRC_DIR=${PG_SRC_DIR:-"$HOME/.dev/pg-src"}
PG_VERSIONS=${PG_VERSIONS:-"18"}

if [ "$WORKTREE" = "$MAIN" ]; then
    echo "error: run this from a worktree, not the main repo. Use scripts/pg-setup.sh instead." >&2
    exit 1
fi

# Determine compiler cache
if command -v sccache >/dev/null 2>&1; then
    CACHE_CC="sccache"
elif command -v ccache >/dev/null 2>&1; then
    CACHE_CC="ccache"
else
    echo "Warning: no compiler cache found. Install ccache or sccache for faster builds." >&2
    CACHE_CC=""
fi

# Suppress .pg-build/ from git status in this worktree
exclude_file=$(git rev-parse --git-common-dir)/info/exclude
grep -qF '.pg-build/' "$exclude_file" 2>/dev/null || echo '.pg-build/' >> "$exclude_file"

# ---------------------------------------------------------------------------
# Submodules — independent clones via hardlinks (git clone --local)
# ---------------------------------------------------------------------------
echo "==> Initializing submodules ..."

clone_local() {
    local sub=$1
    local src="$MAIN/$sub"

    if [ -d "$WORKTREE/$sub" ] && { [ -d "$WORKTREE/$sub/.git" ] || [ -f "$WORKTREE/$sub/.git" ]; }; then
        echo "  $sub already initialized"
        return
    fi

    local commit
    commit=$(git ls-tree HEAD "$sub" | awk '{print $3}')

    if [ ! -d "$src" ] || { [ ! -d "$src/.git" ] && [ ! -f "$src/.git" ]; }; then
        echo "  $sub not initialized in main worktree; falling back to remote clone ..."
        local url
        url=$(git config -f "$WORKTREE/.gitmodules" "submodule.$sub.url")
        git clone "$url" "$WORKTREE/$sub" -q
    else
        git clone --local "$src" "$WORKTREE/$sub" -q
    fi

    git -C "$WORKTREE/$sub" checkout -q "$commit"
    echo "  $sub @ ${commit:0:8}"
}

clone_local third_party/pg_duckdb
clone_local third_party/ducklake

# Initialize pg_duckdb's nested duckdb submodule.
# Use main worktree's copy as the clone source to avoid re-downloading (~1GB).
DUCKDB_LOCAL="$MAIN/third_party/pg_duckdb/third_party/duckdb"
DUCKDB_DEST="$WORKTREE/third_party/pg_duckdb/third_party/duckdb"

if [ ! -d "$DUCKDB_DEST/.git" ] && [ ! -f "$DUCKDB_DEST/.git" ]; then
    if [ -d "$DUCKDB_LOCAL" ] && { [ -d "$DUCKDB_LOCAL/.git" ] || [ -f "$DUCKDB_LOCAL/.git" ]; }; then
        duckdb_commit=$(git -C "$WORKTREE/third_party/pg_duckdb" ls-tree HEAD third_party/duckdb | awk '{print $3}')
        # Point the submodule URL at the local copy so git uses hardlinks
        git -C "$WORKTREE/third_party/pg_duckdb" config \
            submodule.third_party/duckdb.url "$DUCKDB_LOCAL"
        git -C "$WORKTREE/third_party/pg_duckdb" submodule update --init third_party/duckdb -q
        echo "  third_party/pg_duckdb/third_party/duckdb @ ${duckdb_commit:0:8}"
    else
        echo "  duckdb not initialized in main worktree; running recursive submodule init (slow) ..."
        git -C "$WORKTREE/third_party/pg_duckdb" submodule update --init --recursive -q
    fi
fi

# ---------------------------------------------------------------------------
# PostgreSQL — build per worktree (ccache makes recompile near-instant)
# ---------------------------------------------------------------------------
echo "==> Installing PostgreSQL ..."

ncpu=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

for version in $PG_VERSIONS; do
    install_dir="$WORKTREE/pg-$version"
    src_wt="$PG_SRC_DIR/wt/$version"

    if [ -f "$install_dir/bin/pg_config" ]; then
        echo "  PG $version already installed"
        continue
    fi

    if [ ! -d "$src_wt" ]; then
        echo "  PG $version source not found at $src_wt; run scripts/pg-setup.sh first" >&2
        continue
    fi

    build_dir="$WORKTREE/.pg-build/$version"
    mkdir -p "$build_dir"

    echo "  Configuring PG $version (prefix=$install_dir) ..."
    cd "$build_dir"
    if [ -n "$CACHE_CC" ]; then
        export CC="$CACHE_CC cc" CXX="$CACHE_CC c++"
        export CMAKE_C_COMPILER_LAUNCHER="$CACHE_CC" CMAKE_CXX_COMPILER_LAUNCHER="$CACHE_CC"
    fi
    "$src_wt/configure" --prefix="$install_dir" \
        --without-icu --without-readline --without-libxml \
        >/dev/null

    echo "  Building PG $version (j=$ncpu, cache=${CACHE_CC:-none}) ..."
    make -j"$ncpu" >/dev/null
    make install >/dev/null
    cd - >/dev/null

    echo "  PG $version installed: $install_dir/bin/pg_config"
done

echo ""
echo "Build pg_ducklake with:"
for version in $PG_VERSIONS; do
    echo "  PG_CONFIG=$WORKTREE/pg-$version/bin/pg_config make install"
done
