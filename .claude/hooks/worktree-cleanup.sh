#!/usr/bin/env bash
# PreToolUse hook for ExitWorktree
# Removes submodule worktree entries and repairs core.worktree.
#
# Reads tool input from stdin to check if action is "remove".
# Only runs cleanup for "remove" -- "keep" leaves everything intact.
set -euo pipefail

# Parse action from hook stdin (JSON with tool_input.action)
ACTION=$(cat | python3 -c "import sys,json; print(json.load(sys.stdin).get('tool_input',{}).get('action',''))" 2>/dev/null || echo "")
[ "$ACTION" = "remove" ] || exit 0

MAIN=$(git worktree list --porcelain | awk 'NR==1{print $2}')
WT=$(git rev-parse --show-toplevel)

# Skip if main worktree
[ "$MAIN" = "$WT" ] && exit 0

# Skip if .git/modules doesn't exist
[ -d "$MAIN/.git/modules" ] || exit 0

# --- Remove submodule worktree entries pointing to $WT ---
find "$MAIN/.git/modules" -path "*/worktrees/*/gitdir" -type f | while read -r gf; do
    target=$(cat "$gf")
    case "$target" in "$WT/"*)
        wt_entry=$(dirname "$gf")
        gitdir=$(dirname "$(dirname "$wt_entry")")
        git -C "$gitdir" worktree remove --force \
            "$(echo "$target" | sed 's|/\.git$||')" 2>/dev/null || rm -rf "$wt_entry"
    ;; esac
done

# --- Repair: fix any core.worktree that no longer resolves ---
find "$MAIN/.git/modules" -name config -not -path "*/worktrees/*" -type f | while read -r cfg; do
    gitdir=$(dirname "$cfg")
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
