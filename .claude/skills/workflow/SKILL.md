---
name: workflow
description: "Unified dev workflow for pg_ducklake: set up dev env, create worktrees, commit changes, open PRs, push and run CI, fix CI failures. Trigger for phrases like \"set up dev env\", \"create worktree\", \"commit\", \"open a PR\", \"push and run CI\", \"fix CI\", or any dev lifecycle task."
user-invocable: true
---

# pg_ducklake Dev Workflow

Covers the full development lifecycle. Read the relevant file for each step.

## Steps

1. **Dev environment setup** -- install build tools, PostgreSQL, submodules, worktrees.
   Read `dev-env.md` when needed.

2. **Commit** -- craft a commit message following Conventional Commits conventions.
   Read `commit.md` when needed.

3. **Open a PR** -- create branch, handle submodules, push, open GitHub PR.
   Read `open-pr.md` when needed.

4. **CI push, monitor, and fix** -- push to remote, trigger GitHub Actions, monitor, fix failures.
   Read `ci.md` when needed.
