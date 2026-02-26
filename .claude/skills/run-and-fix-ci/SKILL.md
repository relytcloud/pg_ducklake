---
name: run-and-fix-ci
description: Use this skill when the user wants to commit and push code changes to a remote Git repository, trigger GitHub Actions, and monitor their progress. Trigger this when the user says things like "push this to a new branch and run CI", "commit and test in GitHub Actions", "push to remote and check if it passes", "create a PR branch and monitor the build", or any variation involving git push + CI monitoring + potential failure handling. Also trigger if the user wants to iteratively fix CI failures.
---

# CI Push & Monitor

This skill helps you commit changes, push to a remote branch, trigger GitHub Actions, monitor their execution, and handle failures.

## When to Use This Skill

Use this skill when the user wants to:

- Commit local changes and push to a remote repository
- Trigger GitHub Actions
- Monitor workflow execution and report status
- Handle CI failures by either suggesting fixes or automatically fixing them

## Workflow Overview

Follow these steps in order:

### 1. Analyze Changes

First, understand what changed:

```bash
git status
git diff --staged
git diff
```

Also check recent commit messages to understand the project's commit style:

```bash
git log --oneline -10
```

### 2. Craft Commit Message

Use the `commit-message-format` skill to create a proper commit message following the repository's conventions (Conventional Commits format with Co-Authored-By attribution).

### 3. Commit Changes

Stage the relevant files (prefer specific files over `git add -A`):

```bash
git add path/to/file1 path/to/file2
```

Create the commit with the message from step 2, using a HEREDOC for proper formatting:

```bash
git commit -m "$(cat <<'EOF'
<commit message from step 2>
EOF
)"
```

### 4. Determine Remote and Branch

Check available remotes:

```bash
git remote -v
```

Ask the user which remote to push to if there are multiple, and what branch name to use.

If pushing to a new branch:

```bash
git push <remote> HEAD:<new-branch-name>
```

If pushing to an existing branch:

```bash
git push <remote> <branch-name>
```

### 5. Trigger CI Workflow

Use `gh` CLI:

```bash
# Trigger workflow manually
gh workflow run <workflow-file> --repo <owner>/<repo> --ref <branch>

# List recent runs to get the run ID
gh run list --repo <owner>/<repo> --workflow=<workflow-file> --limit 3
```

### 6. Monitor Workflow Progress

Poll the workflow status periodically. For GitHub Actions:

```bash
# Check status of a specific run
gh run view <run-id> --repo <owner>/<repo>

# Watch it in real-time (if supported)
gh run watch <run-id> --repo <owner>/<repo>
```

Report status updates to the user:

- When workflow starts
- When jobs complete
- When workflow finishes (success or failure)

You can also open the workflow in the browser for the user:

```bash
gh run view <run-id> --repo <owner>/<repo> --web
```

### 7. Handle Failures

If the workflow fails:

1. **Fetch the failure logs:**

```bash
gh run view <run-id> --repo <owner>/<repo> --log-failed
```

2. **Analyze the failure** - Look for:
   - Build errors (compilation, linting)
   - Test failures
   - Timeout issues
   - Missing dependencies
   - Environment/configuration problems

3. **Ask the user** how to proceed using AskUserQuestion:

```
Question: "The CI workflow failed. How would you like to proceed?"
Options:
- "Suggest fixes only" - Analyze the error and suggest what to fix (user will make changes)
- "Auto-fix and re-run" - Attempt to fix the issue automatically, commit, push, and re-trigger CI
```

4. **If suggesting fixes:**
   - Explain what went wrong
   - Provide specific file paths and line numbers
   - Suggest the exact changes needed
   - Wait for user to approve changes

5. **If auto-fixing:**
   - Make the necessary changes
   - Commit using the `commit-message-format` skill (typically `fix(ci): <description>`)
   - Push to the same branch
   - Re-trigger the workflow
   - Continue monitoring (repeat from step 6)

### 8. Success Reporting

When the workflow passes:

- Report success to the user
- Provide the workflow URL
- Mention any important artifacts or outputs
- If this was for a PR, provide the PR URL

## Important Notes

### Git Safety

- NEVER use destructive git commands without explicit user request
- NEVER force push to main/master
- NEVER skip git hooks (--no-verify) unless explicitly requested
- Prefer staging specific files rather than `git add -A`
- Don't commit sensitive files (.env, credentials, etc.)

### Parallel Operations

When checking git status, diff, and logs at the start, run these commands in parallel if they're independent.

### Iteration Limits

If auto-fixing fails more than 2 times:

- Stop the auto-fix loop
- Report to the user what you've tried
- Ask them to investigate manually or provide guidance

This prevents infinite loops of failed fixes.

## Example Usage

**User says:** "Push this to a new branch on relytcloud and make sure CI passes"

**You should:**

1. Run `git status` and `git diff` to see what changed
2. Generate a conventional commit message
3. Commit the changes
4. Ask user for branch name (or suggest one based on the changes)
5. Push to `relytcloud` remote
6. Trigger the relevant GitHub Actions workflow
7. Monitor the workflow execution
8. If it fails, ask user whether to suggest fixes or auto-fix
9. Handle accordingly and iterate until success

## Tools Required

This skill requires:

- `git` CLI
- `gh` CLI (for GitHub Actions)
- Bash tool for executing commands
- Read tool for checking log files
- Edit/Write tools if auto-fixing code
