---
name: commit-message-format
description: Use this skill whenever you need to create a git commit message for this repository. This skill defines the commit message format, style, and conventions for pg_ducklake. Always consult this before running git commit.
---

# Commit Message Format

This skill defines how to craft commit messages for the pg_ducklake repository.

## Format: Conventional Commits

All commit messages MUST follow the **Conventional Commits** format:

```
<type>: <description>

[optional body]
```

## Commit Types

Choose the appropriate type based on what changed:

- **`feat`**: New feature or functionality
- **`fix`**: Bug fix
- **`docs`**: Documentation changes only
- **`format`**: Code style formatting (whitespace, semicolons, etc.) - no logic changes
- **`refactor`**: Code refactoring without changing behavior or fixing bugs
- **`test`**: Adding or updating tests
- **`chore`**: Maintenance tasks (dependencies, build config, tooling, etc.)
- **`perf`**: Performance improvements
- **`ci`**: CI/CD configuration changes (GitHub Actions, Docker builds, etc.)

## Description

- **First line**: Clear, concise summary (preferably under 50 characters)
- Use imperative mood ("add" not "added" or "adds")
- No period at the end
- Lowercase after the colon

**Good examples:**
- `feat: add support for partitioned tables`
- `fix: handle NULL values in UPDATE statements`
- `ci: add ClickBench testing before release`

**Bad examples:**
- `feat: Added new feature.` (past tense, unnecessary period)
- `Fix bug` (no scope, too vague)
- `Update code` (not descriptive)

## Body (Optional)

Use the body to explain:

- **What** changed (if not obvious from the description)
- **Why** the change was made (context, motivation)
- **How** it was implemented (if the approach is non-obvious)
- Any **breaking changes** or migration requirements

Wrap the body at 72 characters per line.

## Examples

### Simple feature addition

```
feat: add ALTER TABLE DROP COLUMN support

Implements column dropping for DuckLake tables via online schema
evolution. Updates metadata manager to handle column removal and
data file compaction.
```

### Bug fix

```
fix: handle concurrent INSERT operations

Previously, concurrent INSERTs could cause metadata conflicts due to
race conditions. Now uses PostgreSQL XID-based locking to serialize
metadata updates.
```

### CI/CD change

```
ci: add ClickBench testing before Docker image release

Ensures Docker images pass ClickBench benchmarks on all PostgreSQL
versions (14-18) and both architectures (amd64/arm64) before promotion
to production repository.
```

### Documentation

```
docs: update compilation guide for macOS

Add instructions for Apple Silicon M1/M2 chips and clarify Xcode
command line tools requirements.
```

### Chore/maintenance

```
chore: update pg_duckdb submodule to v0.3.0

Pulls in upstream fixes for DuckDB 1.2 compatibility.
```

## How to Create the Commit

Before writing the commit message, analyze the changes:

```bash
git status
git diff --staged
git diff
```

Also check recent commits to understand the project's patterns:

```bash
git log --oneline -10
```

When committing, use a HEREDOC to ensure proper formatting:

```bash
git commit -m "$(cat <<'EOF'
<type>: <description>

<optional body>
EOF
)"
```

## What NOT to Commit

Before committing, ensure you're not including:

- Sensitive files (`.env`, `credentials.json`, API keys, passwords)
- Large binary files (unless intentional)
- Temporary files (`.log`, `.tmp`, compiled artifacts)
- User-specific config (IDE settings, unless shared team config)

Use `git add <specific-files>` rather than `git add -A` to avoid accidentally staging unwanted files.

## Git Safety Rules

- NEVER amend commits unless explicitly requested by the user
- NEVER force push to main/master branches
- NEVER skip git hooks with `--no-verify` unless explicitly requested
- NEVER create empty commits (unless using `--allow-empty` intentionally)
