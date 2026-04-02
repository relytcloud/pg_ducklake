---
name: pr
description: "Create branch, push, and open a GitHub PR from local commits."
user-invocable: true
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
```

### 2. Create and push branch

Derive a branch name from the commit subjects (e.g. `feat/snapshot-trigger`). If unsure, ask the user.

```bash
git checkout -b <branch-name>
git push -u origin <branch-name>
```

If already on a non-main branch, just push.

### 3. Open the PR

Use `gh pr create`. Write a concise title (<70 chars) and a body with a Summary section. Only include a Test plan section if the PR adds or changes tests. Use a HEREDOC for the body:

```bash
gh pr create --title "<title>" --body "$(cat <<'EOF'
## Summary
<1-3 bullet points summarizing all commits>

## Test plan (Optional)
<bullet points summarizing added or changed tests>

EOF
)"
```

### 4. Report

Print the PR URL so the user can see it.

## Notes

- Never force-push to main/master.
- Prefer specific `git add` over `git add -A`.
- If there are uncommitted changes, ask the user whether to commit them first.
- If `Closes #N` applies, include it in the body.
