---
name: pgducklake-dev-env
description: Set up and manage the local dev environment for pg_ducklake. Use whenever the user asks to set up the dev environment, create a new git worktree, initialize submodules, or install PostgreSQL versions. Trigger for phrases like "set up dev env", "create worktree", "new worktree", "init submodules", "install pg", "setup postgres", or any local environment configuration task. Also trigger proactively if the user describes a fresh clone or says they can't build/run tests.
user-invocable: true
---

Local dev for pg_ducklake requires two things:
1. **One-time**: PG source cloned and built (fast after first time via ccache)
2. **Per-worktree**: Independent submodule clones + per-worktree PG installation

## Quick Reference

| Task | Command |
|------|---------|
| First-time setup | `bash scripts/pg-setup.sh` |
| New worktree setup | `bash scripts/worktree-init.sh` (run from inside worktree) |
| Build extension | `PG_CONFIG=$(pwd)/pg-18/bin/pg_config make install` |
| Run tests | `PG_CONFIG=$(pwd)/pg-18/bin/pg_config make installcheck` |

## One-Time Setup

### 1. Install a compiler cache

ccache and sccache both work. The scripts auto-detect whichever is available.

```bash
brew install ccache        # macOS
sudo apt install ccache    # Ubuntu/Debian
```

**Critical**: duckdb and ducklake are cmake-based and ignore `CC=ccache cc`. They need
`CMAKE_C_COMPILER_LAUNCHER`, a cmake-native hook (cmake 3.4+). Add to `~/.zshrc` / `~/.bashrc`:

```bash
if command -v ccache >/dev/null; then
    export CC="ccache cc"
    export CXX="ccache c++"
    export CMAKE_C_COMPILER_LAUNCHER=ccache
    export CMAKE_CXX_COMPILER_LAUNCHER=ccache
fi
```

Without this, pg_ducklake and pg_duckdb get cache hits but the heaviest builds
(duckdb ~1GB source, ducklake) recompile from scratch every worktree.

### 2. Set up PostgreSQL source

Run from the repo root:

```bash
bash scripts/pg-setup.sh
```

This will:
- Ask for `PG_SRC_DIR` (default: `~/.dev/pg-src`) if not set
- Clone the PostgreSQL source as a bare repo
- Create a source worktree for each requested version
- Build and install PG 18 (default) to `$(repo)/pg-18/`

Override versions or source dir:
```bash
PG_SRC_DIR=/opt/pg-src PG_VERSIONS="17 18" bash scripts/pg-setup.sh
```

**Expected time**: 15–20 min first run (compilation), ~1–2 min per version on reruns (ccache).

## Per-Worktree Setup

After `git worktree add <path> <branch>`, cd into the new worktree and run:

```bash
bash scripts/worktree-init.sh
```

This will:
1. Clone `third_party/pg_duckdb` and `third_party/ducklake` independently (via `git clone --local`, hardlink-based, fast)
2. Clone the nested `third_party/duckdb` from the main worktree's copy (avoids re-downloading)
3. Build and install PG 18 to `$(worktree)/pg-18/` using the shared source and ccache

**Expected time**: ~1–5 min (ccache hits for compilation, fresh linking + install).

## How Isolation Works

- **Submodules**: Each worktree has its own clones at the correct commit. Branches can have
  different submodule versions simultaneously.
- **PG installs**: Each worktree's `pg-18/` is independently configured with its path as
  `--prefix`, so `make installcheck` in one worktree never touches another's extension files.
- **Build cache**: ccache stores compiled objects in `~/.ccache` (shared). Only linking and
  install differ between worktrees — both are fast.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_SRC_DIR` | `~/.dev/pg-src` | Where PG source lives (shared across all worktrees) |
| `PG_VERSIONS` | `18` | Space-separated list of PG versions to set up |

## Troubleshooting

**ccache not being used** — run `ccache -s` to check hit rate. If zero hits, verify `which cc` returns the ccache wrapper or that `CC=ccache cc` is being passed.

**Wrong submodule commit** — reset to what this branch expects:
```bash
git -C third_party/pg_duckdb checkout $(git ls-tree HEAD third_party/pg_duckdb | awk '{print $3}')
git -C third_party/ducklake checkout $(git ls-tree HEAD third_party/ducklake | awk '{print $3}')
```

**PG configure fails** — ensure build tools are installed:
```bash
# macOS
xcode-select --install && brew install bison flex

# Ubuntu
sudo apt install build-essential bison flex
```

**`.pg-build/` cluttering git status** — add to `.git/info/exclude`:
```
.pg-build/
```
