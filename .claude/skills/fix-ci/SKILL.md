---
name: fix-ci
description: "Push to remote, trigger GitHub Actions, monitor, and fix CI failures."
user-invocable: true
---

# CI Push & Monitor

Commit changes, push to a remote branch, trigger GitHub Actions, monitor execution, and handle failures.

## Workflow

### 1. Analyze Changes

```bash
git status
git diff --staged
git diff
git log --oneline -10
```

### 2. Craft Commit Message

Use `workflow-commit` skill conventions to create a proper Conventional Commits message.

### 3. Commit Changes

Stage specific files (prefer `git add <files>` over `git add -A`):

```bash
git add path/to/file1 path/to/file2
git commit -m "$(cat <<'EOF'
<commit message>
EOF
)"
```

### 4. Determine Remote and Branch

```bash
git remote -v
```

Ask the user which remote and branch name if ambiguous.

```bash
# New branch
git push <remote> HEAD:<new-branch-name>

# Existing branch
git push <remote> <branch-name>
```

### 5. Trigger CI Workflow

```bash
gh workflow run <workflow-file> --repo <owner>/<repo> --ref <branch>
gh run list --repo <owner>/<repo> --workflow=<workflow-file> --limit 3
```

### 6. Monitor Workflow Progress

```bash
gh run view <run-id> --repo <owner>/<repo>
gh run watch <run-id> --repo <owner>/<repo>
```

Report status updates: when workflow starts, when jobs complete, when workflow finishes.

### 7. Handle Failures

1. **Fetch failure logs:**

```bash
gh run view <run-id> --repo <owner>/<repo> --log-failed
```

2. **Analyze the failure** -- look for build errors, test failures, timeouts, missing deps, env issues.

3. **Ask the user** via `AskUserQuestion`:
   - "Suggest fixes only" -- explain what went wrong, provide file paths and line numbers
   - "Auto-fix and re-run" -- fix, commit, push, re-trigger, continue monitoring

4. **If auto-fixing:** commit with `fix: ...`, push to same branch, re-trigger, repeat from step 6.

### 8. Success Reporting

When the workflow passes, report success with the workflow URL and any important artifacts.

## Important Notes

### Git Safety

- NEVER use destructive git commands without explicit user request
- NEVER force push to main/master
- NEVER skip git hooks (`--no-verify`) unless explicitly requested
- Prefer staging specific files over `git add -A`
- Don't commit sensitive files (`.env`, credentials, etc.)

### Iteration Limits

If auto-fixing fails more than 2 times, stop the loop, report what was tried, and ask the user to investigate.
