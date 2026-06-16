---
name: coding-rules
description: "Code style, include order, docs update rules, and submodule policy. MUST consult when: writing or modifying C/C++ files (include order matters), adding/removing/changing ducklake.* SQL functions or procedures (keep their inline comments in pg_ducklake--*.sql current), editing third_party/ code, or reviewing code for style."
---

# Coding Rules

## General

- Write clean, minimal code; fewer lines is better
- Prioritize simplicity for effective and maintainable software
- Only include comments that are essential to understanding functionality or convey non-obvious information
- **ASCII only** in all source files, SQL tests, and expected output -- no emojis, no Unicode dashes/quotes (use `-`, `--`, `'`, `"`)

## C/C++

- Avoid using `extern "C"` to reference symbols from the same library. Instead, place it at the header file.
- Use `extern "C"` only when necessary, such as when interfacing with third-party libraries.
- Use `namespace pgducklake` for C++ extension code (do not use `namespace pg_ducklake`).
- Use `pgducklake::` when qualifying symbols outside the namespace block.
- Use C++ raw string literals (`R"(...)"`) for multiline SQL; never use adjacent-string concatenation for SQL queries.

## Docs Style

To avoid _Docs Rot_, keep docs near the code. Do NOT write separate explanation docs or duplicate what code already says. Maintain header comments after each edit. Inline comments only when logic is non-obvious.

### Human docs tree (`README.md` entrypoint)

```
README.md
  +-- pg_ducklake
      +-- sql/pg_ducklake--*.sql       all SQL objects, documented inline
      +-- docs/README.md               index of all human docs
            +-- docs/settings.md       GUCs and DuckLake options
            +-- docs/access_control.md
            +-- docs/compilation.md
```

Every new doc file must be linked from `docs/README.md`. Keep synced with code:

- SQL objects are documented inline in `pg_ducklake--*.sql` (there is no separate
  reference doc). When adding, removing, or changing a `ducklake.*` SQL function
  or procedure, keep its inline comment current.
- In reference docs, order TOC tables alphabetically; keep detailed descriptions in logical order.

## Miscellaneous

- When modifying multiple files, run file modification tasks in parallel whenever possible, instead of processing them sequentially
