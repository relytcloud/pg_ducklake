---
name: subtree-policy
description: "Subtree vs submodule policy for third_party/ dependencies. Consult when adding, removing, or updating third-party code; when deciding whether a dependency should be a subtree or submodule; when pulling upstream changes; or when troubleshooting CI sync issues. Also trigger when the user mentions subtree, submodule, third_party, or upstream merge."
user-invocable: true
---

# Subtree and Submodule Policy

## When to use

**Subtree**:
1. We maintain a fork with ongoing customizations
2. Changes are specific to pg_ducklake and won't go upstream directly
3. We need to edit the code freely without the submodule ceremony of
   committing in the submodule repo first
4. Changes pile up locally and only get contributed upstream
   occasionally or never

**Submodule**:
1. We consume the dependency as-is and only pin a version
2. Every edit would immediately go upstream as a PR
3. We're tracking upstream, not diverging

### Example: current layout

```
pg_ducklake/
  third_party/
    pg_duckdb/              <-- subtree (relytcloud/pg_duckdb)
      third_party/
        duckdb/             <-- submodule (duckdb/duckdb)
    ducklake/               <-- subtree (relytcloud/ducklake)
```

| Dependency | Type | Why |
|------------|------|-----|
| pg_duckdb | subtree | Custom hooks and C exports for pg_ducklake integration |
| ducklake | subtree | Extensible metadata manager, PG-specific customizations |
| duckdb | submodule | Used as-is; only pin version |

---

## Subtree workflow

### Sync to fork

The `sync-subtrees` CI workflow uses `git subtree push` to
incrementally push changes to our fork on every merge to `main`.
This is a regular (non-force) push -- it appends commits, preserving
history on the fork. See `.github/workflows/sync-subtrees.yaml` for
the workflow definition.

Each fork maintains a tracking PR from our branch to upstream's main,
showing the full diff of our customizations:

| Fork | Our branch | Tracking PR |
|------|------------|-------------|
| relytcloud/pg_duckdb | `pgducklake` | `pgducklake` -> upstream main |
| relytcloud/ducklake | `pg_ducklake` | `pg_ducklake` -> upstream main |

**Caveat**: `git subtree push` requires `main` to be append-only (no
rebases). If `main` is rewritten, the push will fail.

### Merge upstream changes

Pull a specific upstream commit or tag directly:

```bash
git subtree pull --prefix=third_party/ducklake \
    https://github.com/duckdb/ducklake.git <commit-or-tag> --squash
```

This fetches the remote ref, squashes it into one commit, and merges
it into the prefix directory. Resolve conflicts if our customizations
overlap with upstream changes. CI syncs the result back to the fork.

For large or complex upstream merges, merge in the fork repo first to
isolate conflict resolution, then pull the fork:

```bash
# 1. Merge upstream into the fork
git clone https://github.com/relytcloud/ducklake.git /tmp/ducklake-merge
cd /tmp/ducklake-merge
git remote add upstream https://github.com/duckdb/ducklake.git
git fetch upstream
git checkout origin/main -b merge-upstream-vX.Y
git merge upstream/vX.Y-<codename>
# resolve conflicts, push to relytcloud/ducklake:main

# 2. Pull the updated fork into pg_ducklake
git subtree pull --prefix=third_party/ducklake \
    https://github.com/relytcloud/ducklake.git main --squash
```

---

## Troubleshooting

**`git subtree push` rejected (non-fast-forward)**: Someone rebased
or force-pushed `main`. Fall back to `git subtree split` + force
push (one-time) to re-bootstrap.

**`git subtree pull` conflicts**: Upstream changes overlap with our
customizations. Resolve conflicts normally.

**Docker build fails with "not a git repository"**: Subtree code may
include a Makefile that checks for `.git/modules/*/HEAD` sentinels.
Create the sentinel files in the Dockerfile (see the pg_duckdb
workaround in `Dockerfile`).
