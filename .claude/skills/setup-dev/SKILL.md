---
name: setup-dev
description: "Dev environment setup: build tools, PostgreSQL, submodules, worktrees. Always import on EnterWorktree/ExitWorktree."
user-invocable: true
---

# Setup Dev Environment

Interactive playbook -- follow steps in order, detect current state, skip what is
already done, and present options to the user via `AskUserQuestion`.

## Principles

- **Global build cache**: ccache in `~/.ccache`, shared across all worktrees.
- **Single clone per repo**: one clone each for pg_ducklake and postgres; use
  git worktrees for parallel work.
- **Submodule sharing**: `git worktree add` on each submodule's git repo
  to share the same object store across worktrees.

---

## Step 1: Detect state

Run these checks silently to understand what is already set up:

```bash
command -v ccache                          # compiler cache
ls pg-*/bin/pg_config 2>/dev/null          # local PG installs in workdir
ls ~/.dev/pg-*/configure 2>/dev/null       # PG source worktrees
ls third_party/pg_duckdb/Makefile 2>/dev/null  # submodules initialized?
git worktree list                          # current worktree situation
```

Skip any step below that is already satisfied.

---

## Step 2: Build tools and compiler cache

### Check

```bash
command -v ccache && ccache --version
```

### If missing, ask user

Present options via `AskUserQuestion`:

| Option | macOS | Linux (apt) |
|--------|-------|-------------|
| **brew/apt** | `brew install ccache` | `sudo apt install ccache` |
| **Skip** | Continue without cache (slow builds) | |

Also check for platform build tools:
- macOS: `xcode-select -p` (if missing: `xcode-select --install`)
- Linux: `dpkg -l build-essential` (if missing: `sudo apt install build-essential bison flex`)

### Configure ccache for cmake

Check the user's shell profile:

```bash
grep CMAKE_C_COMPILER_LAUNCHER ~/.zshrc ~/.bashrc 2>/dev/null
```

If not configured, tell the user to add this block to `~/.zshrc` or `~/.bashrc`:

```bash
if command -v ccache >/dev/null; then
    export CC="ccache cc"
    export CXX="ccache c++"
    export CMAKE_C_COMPILER_LAUNCHER=ccache
    export CMAKE_CXX_COMPILER_LAUNCHER=ccache
fi
```

**Why**: duckdb and ducklake use cmake and ignore `CC=ccache cc`. Without
`CMAKE_C_COMPILER_LAUNCHER`, these heavy builds (~1GB source) recompile
from scratch in every worktree.

**Note**: if `sccache` is already installed (e.g. via Homebrew), cmake may
pick it up automatically. That works fine -- just use `sccache --show-stats`
instead of `ccache -s` to verify cache hits.

---

## Step 3: PostgreSQL (build from source)

Always build from source, per-worktree. Uses a shared bare repo so
multiple PG versions share one clone, and ccache makes rebuilds fast.

Ask the user which versions to install (usually the oldest and newest
supported, e.g. 14 and 18).

### One-time: clone PG source

Shared PG source lives at `~/.dev/pg-src` (bare repo, ~200MB):

```bash
git clone --bare https://github.com/postgres/postgres.git ~/.dev/pg-src
```

For each needed version, fetch the branch and create a source worktree:

```bash
git -C ~/.dev/pg-src fetch --depth=1 origin refs/heads/REL_<VER>_STABLE:refs/heads/REL_<VER>_STABLE
git -C ~/.dev/pg-src worktree add ~/.dev/pg-<VER> REL_<VER>_STABLE
```

### Build and install into current workdir

```bash
WT=$(git rev-parse --show-toplevel)
NCPU=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

for VER in 14 18; do
    pushd ~/.dev/pg-$VER
    ./configure --prefix="$WT/pg-$VER" \
        --without-icu --without-readline --without-libxml
    make -j"$NCPU" && make install
    popd
done
```

`PG_CONFIG` will be: `$(pwd)/pg-<VER>/bin/pg_config`

Add `pg-*/` to `.git/info/exclude` to keep git status clean:

```bash
exclude_file=$(git rev-parse --git-dir)/info/exclude
grep -qF 'pg-*/' "$exclude_file" 2>/dev/null || echo 'pg-*/' >> "$exclude_file"
```

**Time**: ~15-20 min per version on first run, ~1-2 min on reruns (ccache).

---

## Step 4: Submodules

### Main worktree (standard clone)

```bash
git submodule update --init --recursive --depth=1
```

This is all that is needed. The Makefile also auto-inits submodules on
`make install`, but explicit init is faster and gives clearer errors.

**Time**: 5-10 min on first run (duckdb submodule is large even with
shallow clone).

### Submodule worktrees (automated via hooks)

`git worktree add` on a submodule gitdir silently overwrites
`core.worktree` to point at the new worktree instead of the main one.
Stale worktree entries also break `submodule foreach --recursive`.

Two hooks in `.claude/settings.json` handle this automatically:

- **PostToolUse `EnterWorktree`** (`.claude/hooks/worktree-setup.sh`):
  repairs broken gitdirs, then creates submodule worktrees with
  `core.worktree` preservation.
- **PreToolUse `ExitWorktree`** (`.claude/hooks/worktree-cleanup.sh`):
  removes submodule worktree entries pointing to the current worktree,
  then repairs any `core.worktree` that got corrupted. Only runs when
  action is `"remove"`.

If the main worktree's submodules are not initialized, the setup hook
has nothing to iterate. Fall back to
`git submodule update --init --recursive --depth=1` in the main
worktree first (slow, downloads from remote).


### PG install for new worktree

The PG source worktrees at `~/.dev/pg-<VER>` already have compiled
object files from the initial build. Reconfigure with the new prefix
and rebuild. **`make clean` is required** because PG bakes the
`--prefix` path into binaries as rpaths — without it, old objects
with the previous worktree's rpath get reused and executables fail
at runtime with "Library not loaded".

```bash
WT=$(git rev-parse --show-toplevel)
NCPU=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

for VER in 14 18; do
    pushd ~/.dev/pg-$VER
    make clean
    ./configure --prefix="$WT/pg-$VER" \
        --without-icu --without-readline --without-libxml
    make -j"$NCPU" && make install
    popd
done
```

**Time**: ~2-5 min per version (ccache hits on recompilation).

---

## Step 5: Build and test

```bash
PG_CONFIG=<path_to_pg_config> make install
PG_CONFIG=<path_to_pg_config> make installcheck
```

**Time**: first build takes 15-30 min (duckdb cmake build dominates).
Subsequent builds with ccache take 1-5 min.

A successful `make install` ends with PGXS installing `pg_ducklake.so`
and the SQL files into the PG extension directory.

---

## Troubleshooting

**ccache not hitting** -- `ccache -s` shows zero hits. Verify:
- `echo $CC` contains `ccache`
- `echo $CMAKE_C_COMPILER_LAUNCHER` is `ccache`
- cmake is picking it up: check build logs for `ccache` in compiler command

**Wrong submodule commit** -- reset to what the branch expects:
```bash
git submodule update third_party/pg_duckdb third_party/ducklake
```

**PG configure fails** -- missing build deps:
- macOS: `xcode-select --install && brew install bison flex`
- Ubuntu: `sudo apt install build-essential bison flex libreadline-dev zlib1g-dev`

**`pg-*/` in git status** -- re-run Step 3's exclude command.
