# Review notes - PR #6658, cycle 3 (head 59f3975122)

Third `claude` bot review on this PR (posted 2026-08-24T08:36:02Z, PR issue comment). The review's own text
labels the reviewed head as `0875952` (cycle 1's resulting commit) but its content clearly reflects the
cycle-2 diff (it references the added `Describe('P')`-then-small-limit test and
`docs/review-deferred-0875952.md` by name) - the short-SHA label in the bot's own text appears to be a
cosmetic mislabel, not a sign it reviewed stale content.

## Applied

- **Hardcoded JDBC port**: `openJdbcConnection()` used a literal `localhost:5432` while the raw-socket tests
  earlier in the same file use `GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()`. Switched
  `openJdbcConnection()` to the same configured-port lookup for consistency within the file. Verified: 5/5
  tests still pass in `Issue6458PortalSuspensionIT`.

## Skipped (with rationale)

- **`docs/review-deferred-*.md` naming convention**: the review notes this file doesn't match the repo's
  `<issue-number>-<description>.md` postmortem convention used elsewhere in `docs/`, and suggests a
  review-process note like this might read more naturally as a PR/issue comment. This filename and its
  purpose are dictated by the `resolve-issue-with-review` orchestrating skill itself (Phase 3b: "record the
  comment verbatim in a `review-deferred-<HEAD_SHA-short>.md` notes file") - it is a distinct kind of
  artifact from the issue-postmortem docs (a per-cycle review-response log keyed by commit SHA, not a
  technical writeup of the bug), and the reviewer's own text allows for this ("If that's intentional...
  disregard"). No change made; convention is intentional and out of this PR's scope to redesign.
- **Correctness / already-tracked sections**: pure confirmation of prior work (the pagination slice
  boundaries, the `!portal.executed` guard, the catalog-query bypass, #6659/#6660 tracking) - no action
  requested.
