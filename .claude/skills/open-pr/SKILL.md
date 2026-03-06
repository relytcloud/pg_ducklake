---
name: open-pr
description: Create a new branch from local commits, push to remote, and open a GitHub PR. Trigger when the user says things like "open a PR", "push and create PR", "submit this as a PR", "create a pull request", or any variation involving branch creation + push + PR opening.
---

# Open PR

Create a branch, push, and open a GitHub pull request from local commits.

## Workflow

### 1. Gather context (run in parallel)

```bash
git status -u                         # uncommitted changes
git diff origin/main...HEAD --stat    # files changed vs base
git log --oneline origin/main..HEAD   # commits to include
git remote -v                         # available remotes
git submodule status                  # check submodule state
```

### 2. Handle submodule changes

If any submodules have modifications (dirty or new commits):

1. For each modified submodule, commit changes inside the submodule first.
2. **Ask the user** which remote/branch to push each submodule to. Never assume.
3. Push each submodule before proceeding.
4. Back in the root project, stage the updated submodule pointers (`git add <submodule-path>`).

**Critical:** The root project must only be pushed AFTER all submodules are pushed. CI clones submodules recursively and will fail if it cannot find the submodule commits.

### 3. Create and push branch

Derive a branch name from the commit subjects (e.g. `feat/snapshot-trigger`). If unsure, ask the user.

```bash
git checkout -b <branch-name>
git push -u origin <branch-name>
```

If already on a non-main branch, just push.

### 4. Open the PR

Use `gh pr create`. Write a concise title (<70 chars) and a body with Summary + Test plan sections. Use a HEREDOC for the body:

```bash
gh pr create --title "<title>" --body "$(cat <<'EOF'
## Summary
<1-3 bullet points summarizing all commits>

## Test plan
<bulleted checklist>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### 5. Report

Print the PR URL so the user can see it.

## Notes

- Never force-push to main/master.
- Prefer specific `git add` over `git add -A`.
- If there are uncommitted changes, ask the user whether to commit them first.
- If `Closes #N` applies, include it in the body.
- Always push submodules before the root project -- CI will break otherwise.
