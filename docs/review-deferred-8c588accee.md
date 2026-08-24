# Deferred / skipped review items - PR #6654, cycle 2 (head `8c588accee`)

Source: `claude` bot review posted as a PR issue comment at 2026-08-24T07:44:40Z.
Full text: https://github.com/ArcadeData/arcadedb/pull/6654 (issue comment on this SHA).

## Applied this cycle

- **`reconcileUnresolvedLinks()` had no error isolation**, unlike every other record path in the
  file. Applied: the per-entry body is now wrapped the same way `loadDocument`/`loadVertex`/
  `loadEdge` already are in the main loop - a failure is logged, counted into `context.errors`,
  and either aborts via `ImportException` (default mode) or is skipped per `skipOnRowError`
  (rolling back just that record and continuing), instead of propagating raw and uncounted with a
  partially-committed batch behind it. See `JsonlImporterFormat.reconcileUnresolvedLinks`.
  Skip-mode periodic commit was also switched to commit every record (not just every 1000),
  matching the main loop's "one record per transaction" reasoning under `skipOnRowError`.

## Still deferred (relates to cycle 1 item #1, no new action taken)

The reviewer repeated the ask, first raised in cycle 1
(`docs/review-deferred-a0aedaa9f4.md`, item 1), for a regression test with a UNIQUE index on a
LINK/forward-reference property to pin down the transient-placeholder-RID collision risk as
"known, tested behavior."

**Why still deferred:** this cycle's error-isolation fix (above) means a collision, if it fires,
now degrades cleanly - counted `ImportException` or skip, not a raw uncounted exception - so the
*handling* half of this finding is closed. The *reproduction* half (an actual deterministic test
that forces a collision) remains impractical to add as a mechanical fix: it requires the
placeholder value (an *old*, source-database RID left unresolved) to numerically coincide with a
*new*, target-database RID already written under the same UNIQUE index. Target-database RIDs are
allocated by the bucket/page manager, not by test input, so forcing a specific new RID to equal a
specific chosen old RID is not something the test can control deterministically without reaching
into bucket-allocation internals - which would make the test fragile and coupled to allocation
implementation details rather than to the behavior under test.

Recommend resolving this the same way as cycle 1's item 1: make the design call (null placeholder
vs. documented limitation vs. two-pass resolution) first; the right regression test follows from
whichever shape is chosen, rather than being addable independent of it.

## Skipped (nitpick / acceptable as-is, with rationale)

### `docs/review-deferred-*.md` committed permanently into `docs/`

> ... worth confirming with maintainers whether capturing review back-and-forth as a permanent doc
> is the intended long-term convention versus keeping that context in the PR thread itself.

Skipped as a process question, not a code defect - flagged explicitly as "minor/non-blocking" and
addressed to maintainers rather than to this PR. This file's existence and naming convention
(`review-deferred-<head-SHA-short>.md`) come directly from the `resolve-issue-with-review`
orchestration skill driving this review loop (see its Phase 3b), not from an ad hoc choice made in
this PR - so it isn't something to unilaterally change mid-loop. Worth raising with maintainers
separately if the convention should change project-wide.

### Broad `catch (Exception e)` around `new RID(string)` in `remapLinkValue`

> `RID`'s string constructor only throws `IllegalArgumentException`/`NumberFormatException` (both
> unchecked, no other checked exceptions possible), so narrowing the catch would be slightly more
> precise, though this mirrors the "not RID-shaped, let normal conversion handle it" intent fine
> as-is.

Skipped per the reviewer's own conclusion ("fine as-is"). Confirmed `RID(String)` throws only
those two unchecked types today, but catching `Exception` here is deliberately defensive: the
comment's intent ("not a RID-shaped string ... let the normal property conversion/validation path
deal with it") holds for any future exception type that constructor might throw, not just today's
two. Narrowing would be a no-op today and a foot-gun if the constructor ever changes.

### `remapLinkProperties` always allocates a fresh `ArrayList`/`HashMap` per LIST/MAP-of-LINK property

> Minor GC churn on the import path, not a hot query path, so low priority given the project's
> performance guidance, but `reconcileUnresolvedLinks` already does the "only allocate if
> something changed" pattern next to it, so the two could be made consistent.

Skipped per the reviewer's own qualification ("low priority", "not a hot query path"). Import is a
one-shot, not-hot-path operation and the allocation is bounded by the property's own list/map
size, not the whole import; matching `reconcileUnresolvedLinks`'s conditional-allocation pattern
here would need the caller (`loadProperties`) to distinguish "nothing changed" from "everything
already resolved," which isn't free either. Worth revisiting only alongside item 5 from cycle 1
(the two methods' shared-helper follow-up), not in isolation.
