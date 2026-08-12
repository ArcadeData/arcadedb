# Review feedback deferred/skipped - PR #6093, cycle 1 (reviewed commit 77013dbb6)

Source: `claude[bot]` review posted as a PR issue comment on
https://github.com/ArcadeData/arcadedb/pull/6093 at 2026-08-12T06:49:38Z.

Items 1 and 2 from that review were applied (test coverage for `apoc.refactor.cloneNodesWithRelationships`
and `apoc.do.when`; forcing the returned stream to materialize before `commit()`, plus a documented contract
on `Procedure#execute`). The two items below were explicitly flagged by the reviewer as non-blocking and are
skipped, with rationale.

## Skipped: item 3 - `apoc.do.when` always pays for a transaction, even on a pure read

> `DoWhen.isWriteProcedure()` returns `true` unconditionally, since at registration time it can't know whether
> `ifQuery`/`elseQuery` will actually write. That means every `apoc.do.when` call now begins+commits a
> transaction even when the branch taken is read-only... Not a bug and not something this PR needs to solve,
> but it's a real per-call cost for what can be an entirely read-only procedure.

**Rationale for skipping:** the reviewer explicitly stated this PR doesn't need to solve it. Making
`isWriteProcedure()` conditional on the branch actually taken would require either evaluating `condition`
before `isWriteProcedure()` is queried (which happens before argument evaluation in the current `CallStep`
flow) or deferring the auto-commit decision until after `DoWhen.execute()` has already run its nested
`database.query`/`command` dispatch - a larger restructuring of the write-procedure contract than this bug fix
warrants. `do.when`'s own nested `database.command()`/`database.query()` dispatch already has its own
correctness story independent of `CallStep`'s outer wrap (per `OpenCypherQueryEngine`'s Javadoc, a writing
`CALL` subquery auto-commits at that nested top-level call too), so the extra outer begin/commit here is a
performance cost, not a correctness gap. Worth a follow-up if `apoc.do.when` shows up in a profiling report,
not before.

## Skipped: item 4 - per-row auto-commit on chained CALL

> `UNWIND [...] AS n CALL merge.node(...)` with no explicit transaction now begins/commits once per input row.
> That matches `SetStep`'s existing per-row behavior for bulk `UNWIND ... SET`, so it's consistent rather than
> a new regression, but it does mean bulk-merging via `CALL` will pay one begin/commit (WAL flush) per row
> rather than batching.

**Rationale for skipping:** the reviewer explicitly characterized this as consistent with existing behavior,
not a regression introduced by this PR. `chainedCallAutoCommitsEachRowWithNoExplicitTransaction` in
`CallStepWriteProcedureAutoCommitTest` already documents and locks in this exact per-row behavior. Callers
doing a large bulk merge already have the documented workaround (wrap the whole `UNWIND` in an explicit
`database.begin()`/`commit()`), same as they would for bulk `UNWIND ... SET`. Batching auto-commit across rows
would be a cross-cutting change to `SetStep`/`DeleteStep`/`MergeStep`/`RemoveStep`/`ForeachStep`/`CallStep`
alike, out of scope for a bug fix targeted at #6073's missing-auto-commit gap specifically.
