---
name: pr-review-surfaces
description: Use when polling, reading, or answering automated reviews on an ArcadeDB pull request (claude-review, CodeRabbit, Codacy, Codecov) or deciding whether a PR is ready to merge - covers where each bot writes and how to re-trigger a silent review.
---

# PR review surfaces

The automated reviewers on a PR write to three different places, and a poll that reads only one of them misses
findings for hours (PR #7437):

- `gh pr view <N> --json comments` shows the ISSUE comments only: the `claude-review` job, Codacy, Codecov.
- CodeRabbit posts PR REVIEWS: `gh api repos/ArcadeData/arcadedb/pulls/<N>/reviews` carries their bodies (the
  nitpicks and the "outside diff range" findings) and `gh api repos/ArcadeData/arcadedb/pulls/<N>/comments` the inline
  threads, one per actionable finding. Reply to a thread with `POST pulls/<N>/comments/<id>/replies`: CodeRabbit
  re-verifies the next push, answers in the thread and resolves it itself, and it re-reviews every push.
- The `claude-review` job often finishes with `permission_denials_count > 0` in its result block and posts nothing.
  `gh run rerun <runId>` gets a real review on the same commit without writing a comment; never use the `@claude`
  trigger phrase in a comment.
- A PR is ready when the latest substantive review says nothing blocks the merge, no major issue is open, every
  CodeRabbit thread is resolved and the checks are green.
