---
name: version-management
description: "Versioning, branch, and release policy (semantic versioning, branch-per-minor, tag-per-patch). MUST consult when: deciding a version number, cutting a release, creating a version tag, opening a release/patch branch, or backporting a bug fix."
user-invocable: true
---

# Version Management

Semantic versioning `v<major>.<minor>.<patch>`: major = breaking, minor = new
feature, patch = bug fix only. The git tag (`vX.Y.Z`) and the extension's
`default_version` always agree.

- `main` -- next unreleased version; takes anything.
- branch `vX.Y` -- one per minor line; bug fixes only, never features.
- tag `vX.Y.Z` -- one per patch; immutable. Fix a bad release with a new patch
  tag, never by re-tagging. Pushing a `v*` tag is what fires release CI.

## Cut a minor release `vX.Y.0`

Version touchpoints are already `X.Y.0` on `main` (bumped after the previous
release). Then three things:

1. Push branch: `git branch vX.Y main && git push origin vX.Y`
2. Push tag: `git tag vX.Y.0 vX.Y && git push origin vX.Y.0`  (release CI runs)
3. Bump `main` to the next dev version (`vX.(Y+1).0`, or `v(X+1).0.0` if a
   breaking change has landed) -- see touchpoints below.

## Ship a patch `vX.Y.Z` (Z > 0)

1. Land the fix on `main` first if the bug is there too; cherry-pick onto each
   affected `vX.Y` branch.
2. On the `vX.Y` branch, bump touchpoints to `X.Y.Z`.
3. Push tag `vX.Y.Z` (release CI runs).

## Version touchpoints

Per bump, in one commit:

- `pg_ducklake/pg_ducklake.control` -- `default_version`.
- `pg_ducklake/sql/pg_ducklake--<old>--<X.Y.Z>.sql` -- update script (empty
  placeholder if the release has no SQL object changes).

Keep one base install script (`pg_ducklake--1.0.0.sql`); PostgreSQL reaches
`default_version` by installing it and applying the `--<old>--<new>` chain.
