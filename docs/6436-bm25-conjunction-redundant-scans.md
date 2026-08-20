# Issue #6436: BM25 conjunction re-scans the corpus once per constrained property

## Summary

A `CONTAINSTEXT` conjunction over several properties of a multi-property **BM25** full-text
index (`title CONTAINSTEXT 'x' AND content CONTAINSTEXT 'y' AND tags CONTAINSTEXT 'z'`) was
answered by `FullTextSearch.searchSimple` recursing once per constrained property. Each
recursion independently ran `scanDocumentFrequencies` (a type-wide posting scan) and built its
own `BM25ScoringContext` from scratch, even though the corpus statistics that context carries
(`totalDocs`, `avgDocLength`, and the document frequency of every scoring token) are the same
for every property in one query. Correctness was never in question (that is what #6414 fixed);
this issue is about the redundant setup cost of doing that three separate times for a
three-property conjunction.

`searchSimple` now collects the scoring tokens for every constrained property up front, scans
document frequencies once for their union, and builds a single shared `BM25ScoringContext`.
Each property still runs its own posting walk against that shared context to produce the
per-property match set `LSMTreeFullTextIndex.intersectPerProperty` intersects - that walk is
the irreducible per-property work - but the repeated `scanDocumentFrequencies` scan and
`BM25ScoringContext` construction collapses from one-per-property to one-per-query.

## PR

https://github.com/ArcadeData/arcadedb/pull/6478 (branch `fix/6436-bm25-conjunction-redundant-scans`)

## Review cycles

### Cycle 1 - head `1453cd51e3a8e4185519a9ba3e68b1fd24436658`

Reviewer: `claude[bot]` (issue comment, 2026-08-19T21:13:50Z)

Outcome: no correctness issues found; confirmed field-qualified tokens make the union-merge
safe and per-token `documentFrequency` lookups keep the shared context sound. Two non-blocking
nits raised:
1. `getSimpleQueryTokenBoosts(propertyKey)` was called twice per property (once building the
   union, once again inside `intersectPerProperty`'s lookup lambda).
2. The test-only `documentFrequencyScanInvocations` static `AtomicLong` implicitly depends on
   this module's tests running sequentially in one fork; worth a comment.

Action taken (this session): both nits applied in commit `1d3b5a6703`, pushed as head
`1d3b5a6703c6b0fbeb5cffdc1f68dfe4de0a08f0`:
- Reused each property's already-computed token map (kept in an `IdentityHashMap<Object[], Map<String,Float>>`
  keyed by the `propertyKey` array instance `intersectPerProperty` hands back to the lookup
  closure) instead of re-deriving it a second time.
- Added a one-line comment on `documentFrequencyScanInvocations` noting the sequential-single-fork
  dependency.
- Verified: `mvn -o -pl engine -am compile` clean; `mvn -o -pl engine -am test` over
  `com.arcadedb.index.fulltext.**`, `ContainsText*`, `*BM25*` - 217/217 tests green, 0 failures.

### Cycle 2 - head `1d3b5a6703c6b0fbeb5cffdc1f68dfe4de0a08f0`

Reviewer: `claude[bot]` (issue comment, 2026-08-20T07:57:34Z)

Outcome: correctness re-confirmed by an independent static trace (per-property scoring still
reads only `tokensByProperty.get(propertyKey)`, never the shared union map; field-qualified
tokens rule out cross-property collisions). Called out an unstated side benefit (the refactor
also removes redundant `getBucketIndexes`/`isBM25`/`splitPositionalKey` calls the old recursive
implementation implied). Two minor/non-blocking nits, both explicitly framed as optional:
1. `unionScoringTokens`'s `Math::max`-merged boost values are computed but never read (`scanDocumentFrequencies`
   only consults `.keySet()`) - flagged as "not a bug... just a bit of inert computation," with the
   reviewer itself noting the alternative (a `Set<String>`-based overload) isn't clearly better.
2. The test-only invocation counter is "instrumentation in production code," but the reviewer
   assessed it as "well-justified... Fine as-is."

No blocking issues found. Working tree was clean before this review landed (no in-flight
changes), and neither nit is actionable-and-clear enough to warrant a churn commit: nit 1 is a
sub-microsecond `Math::max` call on a handful of tokens with no correctness or clarity cost, and
the reviewer's own alternative (a `Set`-typed overload) was judged not obviously better; nit 2
was explicitly marked "fine as-is" by the reviewer. Treated as a clean approval.

## Deferred items

None requiring developer follow-up. The benchmark suggested by the original issue remains
deferred to a future PR per the PR description's own "Notes / deferred" section - the
invocation-count regression test already pins the exact behavior a benchmark would guard.

## Final state

**clean-approval** after 2 review cycles (satisfying the requirement of running at least a
couple of cycles). PR left open for the developer to merge.
