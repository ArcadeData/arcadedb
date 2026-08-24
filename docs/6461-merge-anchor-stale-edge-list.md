# Issue #6461: MERGE duplicates a relationship (and its far endpoint) when the anchor vertex was bound earlier in the same query

## Root cause

`MergeStep.executeMerge` wraps each row it processes in its own
`database.transaction(() -> {...}, true)` call (added for #6367's auto-retry). When the caller has no
outer transaction open - an unwrapped autocommit call, exactly the shape `BoltNetworkExecutor.handleRun`
uses, and the shape a raw `database.command(...)` call has - each row's MERGE owns and commits its own
transaction.

The anchor vertex instance a row carries (bound by an earlier `MATCH`, or reused across `UNWIND` rows of
the same MERGE clause) was loaded before that row's own transaction started. Once a **prior** row's MERGE
appends the first edge in a given direction to that vertex - the one write that rewrites the vertex
record's edge-list head pointer - the row's own in-memory instance still reflects the pre-append state.
`findAllMatchingPaths` (via `traverseFromAnchor`) and `traverseFromNode` both took that instance straight
from the row and called `vertex.getEdges(...)` on it directly, so the match half of a later row misses the
edge/node a previous row already created and its create half runs, producing a duplicate.

Confirmed empirically to be **still present at HEAD** before this fix (not masked by the existing
`getMostUpdatedVertex`/transaction-record-cache mechanism, which only helps when the reader and the writer
share the same `TransactionContext` - not the case here, since each unwrapped row commits and starts a new
one).

Over HTTP this is invisible: `DatabaseAbstractHandler.executeInTransaction()` wraps the *entire* autocommit
command in one outer transaction, so every row's MERGE simply joins it and shares its transaction-local
record cache - which is also why the pre-existing `MergeBoundAnchorRegression` tests (issue #4226) never
caught this: they all wrap their MERGE call in an explicit `db.transaction(() -> ...)`.

## Fix

`MergeStep` re-resolves a bound anchor vertex to its latest committed state, via
`context.getDatabase().lookupByRID(rid, true)`, right before its edges are enumerated - mirroring
`SetStep.reloadLatestDoc()` (issue #5227). Two call sites needed it:

- `findAllMatchingPaths`: the anchor vertex read from `baseResult` before it is handed to
  `traverseFromAnchor` (the `anchorIdx > 0`, no-usable-index path).
- `traverseFromNode`: the vertex resolved from the row's own binding (`forcedVertex == null`, i.e. the
  node-0 anchor case reached when `anchorIdx == 0` or no anchor was found at all) before
  `traverseEdgesFromVertex` walks it.

A new private helper, `reloadAnchorVertex(Vertex)`, does the reload and is a no-op (returns the input
unchanged) when the vertex has no identity yet (not persisted) or was concurrently deleted, leaving the
existing not-found handling downstream to react to that.

`tryFindPathByIndexSeek`'s own read of the anchor was left alone: it only uses the anchor for an identity
`equals()` check against the freshly-index-resolved candidate's own (already fresh) edge list, never for
edge enumeration on the anchor itself - which is exactly why the unique-index path was never affected by
this bug in the first place (as the issue's "masked when indexed" note observes).

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/Issue6461MergeAnchorStaleEdgeListTest.java`, two cases,
both driven through unwrapped `database.command(...)` calls (no outer `database.transaction()`), matching
the shape that actually exposes the bug:

- `unwindMergeReusingBoundAnchorDoesNotDuplicateEdgeOrFarEndpoint` - the issue's realistic repro
  (`MATCH ... UNWIND [10,10,20] AS cid MERGE (p)-[:HAS]->(c:Chi {id:cid})`); asserts 2 edges and one `Chi`
  node per distinct id.
- `repeatedMergeOnSameBoundPairDoesNotDuplicateEdge` - the issue's minimal form
  (`MATCH (a),(b) MERGE (a)-[:L]->(b) MERGE (a)-[:L]->(b)`); asserts exactly 1 edge.

Both fail on pre-fix `MergeStep` (3 edges / 2 edges respectively, matching the issue's reported counts) and
pass after the fix.

## Verification

- `Issue6461MergeAnchorStaleEdgeListTest` (new): 2/2 pass.
- `OpenCypherMergeTest` (46 tests, all `@Nested` regression groups including `MergeBoundAnchorRegression`
  for #4226) + `OpenCypherMergeActionsTest` (13) + `Issue6367MergeStepAutoRetryTest` (1) +
  `Issue6602MergeAfterEmptyMatchCardinalityTest` (7) + `MergeInsertSlowdownTest` (1) +
  `RefactorMergeNodesTest` (13): all green, no regressions.
- Full `com.arcadedb.query.opencypher.**` package (excluding `benchmark`/`slow`/`vector` lanes): see below.

## PR

https://github.com/ArcadeData/arcadedb/pull/6652

## Review cycles

- **Cycle 1** - head SHA `0e7de9f6` (push `2026-08-23T20:28:24Z`). Polled all three review surfaces
  (`reviews[]`, inline `pulls/6652/comments`, and `claude`-authored PR issue comments newer than the push)
  for the full 15-minute per-iteration window: no review landed on any surface.
  - Cross-checked against the Actions run directly: `Claude Code Review` workflow run
    [32664545721](https://github.com/ArcadeData/arcadedb/actions/runs/32664545721) triggered on this SHA,
    ran to completion (`status=completed`, `conclusion=success`, 28 turns, ~186s), and its own log shows
    `permission_denials_count: 6` and a final "No buffered inline comments" from the
    post-buffered-inline-comments step - i.e. the bot's run finished without ever posting a top-level
    `gh pr comment`, despite `Bash(gh pr comment:*)` being in its allowed-tools list. This is not a slow-bot
    case (the workflow itself finished in ~5 minutes and never posted); as of this update, over 11 hours have
    passed since the push with still no comment on the PR from `claude`.
  - No code changes were made this cycle since no review feedback was ever received to act on.

## Deferred items

None - no review comments were received to categorize.

## Final state

**timeout** - the `claude` review bot's workflow run completed without posting a review comment on head
SHA `0e7de9f6`. The PR is left open with the fix as pushed. This looks like the same class of infra issue
tracked in `reference_review_bot_can_time_out_before_posting` (bot completes without posting, rather than
never running at all) and may warrant a manual `@claude review` comment or maintainer trigger to get a
review on this PR.
