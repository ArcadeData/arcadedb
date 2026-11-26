---
name: pr-review-surfaces
description: Use when polling, reading, or answering automated reviews on an ArcadeDB pull request (classify-pr, CodeRabbit, Codacy, Codecov) or deciding whether a PR is ready to merge - covers where each bot writes and how to re-trigger a silent review.
---

# PR review surfaces

The automated reviewers on a PR write to three different places, and a poll that reads only one of them misses
findings for hours (PR #7437):

- `gh pr view <N> --json comments` shows the ISSUE comments only: `classify-pr`, Codacy, Codecov.
- `classify-pr` (`.github/workflows/classify-pr.yml`) reviews every human PR, forks included, on open, reopen, push
  and ready-for-review. It posts one comment per head, authored by `github-actions[bot]`, whose first line is
  `<!-- classify-pr sha=<head> verdict=<lgtm|needs-changes|unsafe|none> -->`. Trust a verdict only when that `sha`
  is the PR's current head; an older comment reviews an older push. It also sets the `triage:*` label (cleared on
  every push until the new review lands), one `severity:*`, `has-issue`/`no-issue` and area labels, and adds
  `do not merge` on `unsafe`. `verdict=none` means the review failed: the comment says why (the classifier job
  failed, it returned no structured output, its decision did not reach the apply job, or the decision was not
  valid), and the apply job's audit line carries the same `reason=`. Read the run log.
- A missing review for the current head: dispatch it, `gh workflow run classify-pr.yml -f pr_number=<N> -f
  dry_run=false` (the default `dry_run=true` only logs). Never use the `@claude` trigger phrase in a comment.
- CodeRabbit posts PR REVIEWS: `gh api repos/ArcadeData/arcadedb/pulls/<N>/reviews` carries their bodies (the
  nitpicks and the "outside diff range" findings) and `gh api repos/ArcadeData/arcadedb/pulls/<N>/comments` the inline
  threads, one per actionable finding. Reply to a thread with `POST pulls/<N>/comments/<id>/replies`: CodeRabbit
  re-verifies the next push, answers in the thread and resolves it itself, and it re-reviews every push.
- A PR is ready when the latest substantive review says nothing blocks the merge, no major issue is open, every
  CodeRabbit thread is resolved and the checks are green.
