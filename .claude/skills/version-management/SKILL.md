---
name: version-management
description: "Versioning, branch, and release policy (semantic versioning, branch-per-minor, tag-per-patch). MUST consult when: deciding a version number, cutting a release, creating a version tag, opening a release/patch branch, or backporting a bug fix."
user-invocable: true
---

# Version Management

How pg_ducklake numbers versions and maps them onto git branches and tags.

## Version scheme

Versions are `v<major>.<minor>.<patch>` (semantic versioning):

- **major** -- breaking changes (incompatible SQL, catalog, or behavior).
- **minor** -- new features, backward compatible.
- **patch** -- bug fixes only, no new features and no breaking changes.

The same number appears in the git tag (prefixed `v`) and in the extension's
`default_version`. They must always agree.

## Branches and tags

| Ref | Form | Purpose | Accepts |
|-----|------|---------|---------|
| `main` | -- | the next, unreleased version | features + breaking changes + fixes |
| release branch | `v<major>.<minor>` (e.g. `v1.0`) | one per minor line | bug fixes only |
| tag | `v<major>.<minor>.<patch>` (e.g. `v1.0.1`) | one per patch release | immutable |

- **`main` is always ahead of every release branch.** After `v1.0.x` ships,
  `main` targets the next minor (`v1.1.0`) by default; it becomes the next major
  (`v2.0.0`) as soon as a breaking change merges. Decide major-vs-minor by the
  changes that have actually landed on `main`, not in advance.
- **Release branches never take features.** A `v1.0` branch only ever receives
  bug fixes destined for the next `v1.0.z` patch.
- **Tags are immutable.** Once `v1.0.0` is pushed, never move or delete it.
  Anything wrong with a release is corrected by a new patch tag, never by
  re-tagging.

## Release CI

Release CI is triggered by pushing a `v*` tag (see the `tags: ["v*"]` triggers in
`.github/workflows/build_and_test.yaml` and `docker.yaml`); a tag push fires the
full multi-version, multi-arch build and image publish. `main` pushes run a
lighter CI. **Pushing a version tag is what cuts a release** -- do it only when
the branch is ready to ship.

## Cutting a new minor release (`vX.Y.0`)

1. On `main`, set the version touchpoints (below) to `X.Y.0` and verify the build
   and tests are green.
2. Branch `vX.Y` from `main`.
3. Push tag `vX.Y.0` on `vX.Y` -> release CI runs.
4. On `main`, bump to the next development version (`vX.(Y+1).0`, or
   `v(X+1).0.0` once a breaking change is present).

## Shipping a patch (`vX.Y.Z`, `Z > 0`)

1. Land the fix on `main` first if the bug also exists there, so the next minor
   does not regress.
2. Backport (cherry-pick) it onto each affected `vX.Y` release branch. A
   release-only fix (bug absent from `main`) is made on the branch and
   forward-ported if relevant.
3. On the release branch, bump the version touchpoints to `X.Y.Z`.
4. Push tag `vX.Y.Z` -> release CI runs.

## Version touchpoints

Bump these together, in one commit, whenever the version changes:

- `pg_ducklake/pg_ducklake.control` -- `default_version`.
- `pg_ducklake/sql/pg_ducklake--<X.Y.Z>.sql` -- the install script for the new
  version.
- `pg_ducklake/sql/pg_ducklake--<old>--<X.Y.Z>.sql` -- a migration script so
  `ALTER EXTENSION pg_ducklake UPDATE` works from the previous version (required
  for every patch and minor that changes SQL objects).

## Invariants

- A tag is never moved or deleted.
- A release branch never gains a feature.
- A fix that applies to both `main` and a release branch lands on both; the
  branches must not silently diverge.
- The git tag, `default_version`, and the install-script filename agree.
