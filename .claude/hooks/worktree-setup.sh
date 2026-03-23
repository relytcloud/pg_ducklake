#!/usr/bin/env bash
# PostToolUse hook for EnterWorktree
# Repairs submodule gitdirs and creates submodule worktrees.
set -euo pipefail

MAIN=$(git worktree list --porcelain | awk 'NR==1{print $2}')
WT=$(git rev-parse --show-toplevel)

# Skip if main worktree (not inside a secondary worktree)
[ "$MAIN" = "$WT" ] && exit 0

# Skip if .git/modules doesn't exist (no submodules initialized in main)
[ -d "$MAIN/.git/modules" ] || exit 0

# --- Repair: prune stale entries and fix broken core.worktree ---
find "$MAIN/.git/modules" -name config -not -path "*/worktrees/*" -type f | while read -r cfg; do
    gitdir=$(dirname "$cfg")
    git -C "$gitdir" worktree prune 2>/dev/null || true
    wt=$(grep 'worktree = ' "$cfg" | sed 's/.*worktree = //')
    [ -z "$wt" ] && continue
    case "$wt" in /*) abs_wt="$wt" ;;
        *) abs_wt=$(cd "$gitdir" 2>/dev/null && cd "$wt" 2>/dev/null && pwd) || abs_wt="" ;; esac
    if [ -z "$abs_wt" ] || [ ! -d "$abs_wt" ]; then
        rel=${gitdir#$MAIN/.git/modules/}
        sm_path=$(echo "$rel" | sed 's|/modules/|/|g')
        sed -i '' "s|worktree = .*|worktree = $MAIN/$sm_path|" "$cfg"
    fi
done

# --- Setup: create submodule worktrees ---
git -C "$MAIN" submodule foreach --recursive --quiet \
    'echo "$toplevel/$sm_path" "$sha1"' | while read -r abs_path commit; do
    rel_path="${abs_path#$MAIN/}"
    gitdir=$(git -C "$abs_path" rev-parse --git-dir)
    case "$gitdir" in /*) ;; *) gitdir="$abs_path/$gitdir" ;; esac
    # Skip if already populated
    [ -f "$WT/$rel_path/.git" ] && continue
    saved_wt=$(grep 'worktree = ' "$gitdir/config" | sed 's/.*worktree = //')
    rm -rf "$WT/$rel_path"
    git -C "$gitdir" worktree add --detach "$WT/$rel_path" "$commit" >/dev/null 2>&1
    sed -i '' "s|worktree = .*|worktree = $saved_wt|" "$gitdir/config"
done
