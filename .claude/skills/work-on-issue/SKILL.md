---
name: work-on-issue
description: "End-to-end workflow for working on a GitHub issue: create a worktree, fetch the issue, plan, implement, simplify, and open a PR. Use this skill whenever the user says '/work-on-issue' followed by a GitHub issue number. Also trigger when the user says things like 'work on issue #N', 'pick up issue N', 'start issue #N', or 'implement #N'."
user-invocable: true
---

# Work on Issue

End-to-end workflow that takes a GitHub issue from triage to pull request.
Accepts a GitHub issue number as argument (e.g., `#42` or `42`).

## Workflow

Execute these phases in order. Each phase has a clear checkpoint --
do not skip ahead without completing the current phase.

### Phase 1: Worktree and environment

1. Parse the issue number from the argument (strip leading `#` if present).
2. Create an isolated worktree via `EnterWorktree` (name: `issue-<number>`).
3. Run `/setup-dev` inside the new worktree.

### Phase 2: Understand the issue

```bash
gh issue view <number>
gh issue view <number> --comments
```

Summarize the issue to the user in 2-3 sentences for confirmation.

### Phase 3: Plan

Use `EnterPlanMode` to design the implementation. Explore code paths,
write a concrete plan (files to change, how to test), then
`ExitPlanMode` for user approval.

Do not write code until the user approves the plan.

### Phase 4: Implement

Code the solution following the approved plan. Build and run tests
per CLAUDE.md. Stop after 3 failed attempts and ask the user.

### Phase 5: Simplify

Run `/simplify` and apply any fixes it surfaces.

### Phase 6: User review

Present a change summary (files changed, design decisions, test
results). Wait for the user to confirm or request adjustments.

### Phase 7: Open PR

Run `/pr`. Include `Closes #<issue-number>` in the PR body.

## Notes

- If any phase fails, stop gracefully -- the worktree remains for
  manual follow-up.
- This workflow pauses for user confirmation at the plan (phase 3)
  and review (phase 6) checkpoints. Do not auto-proceed past those.
