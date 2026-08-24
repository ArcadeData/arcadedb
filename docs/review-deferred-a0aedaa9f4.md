# Deferred / skipped review items - PR #6654, cycle 1 (head `a0aedaa9f4`)

Source: `claude` bot review posted as a PR issue comment at 2026-08-23T20:53:47Z.
Full text: https://github.com/ArcadeData/arcadedb/pull/6654 (issue comment on this SHA).

## Deferred (actionable, but the right fix needs a design decision)

### 1. Forward-reference LINK values can transiently collide with a UNIQUE index

> Unresolved forward-reference LINK values are temporarily written as "wrong" RIDs, which can
> collide with a UNIQUE index. `remapLinkProperties` leaves an unresolved value as the original
> source-RID string when `ridIndex` does not have a mapping yet
> (`JsonlImporterFormat.java:509-520`). `imported.fromMap()`/`Type.convert` then parses that
> string into an actual RID object (`Type.java:786`) and `save()` persists it as-is until
> `reconcileUnresolvedLinks()` fixes it up later. If that property backs a UNIQUE index, this
> intermediate (arbitrary, source-database-numbered) RID value could coincidentally collide with a
> value already written for another record's already-resolved LINK in the target database, causing
> a spurious `DuplicateKeyException` mid-import for data that would otherwise import cleanly. Rare,
> but since this is exactly the property shape (LINK + forward reference) the PR is designed to
> fix, it seems worth a regression test with a unique index on a LINK/LIST-of-LINK property to
> confirm this does not happen (or documenting it as a known limitation like the "never resolves"
> case already is).

**Why deferred rather than fixed here:** verified the mechanism is real (traced
`remapLinkValue` -> `fromMap`/`Type.convert` -> `save()` -> `reconcileUnresolvedLinks`) - an
unresolved forward reference genuinely persists the old, source-numbered RID as the live property
value until the reconciliation pass runs. Closing this properly needs a design call the developer
should make, not a mechanical patch:
- write `null` as the placeholder instead (blocked if the property is NOT NULL, and changes
  observable behavior for any reader that runs between the initial save and reconciliation), or
- pre-scan the file to resolve all forward references before any record is saved (defeats the
  streaming, single-pass design), or
- accept the current behavior and document it as a known limitation (matching how the "never
  resolves" case is already documented in `reconcileUnresolvedLinks`'s Javadoc).

Recommend: pick one of the above and, at minimum, add the regression test the reviewer suggested
(UNIQUE index on a LINK/LIST-of-LINK property with a forward reference) to pin down current
behavior either way.

## Skipped (nitpick / acceptable as-is, with rationale)

### 3. `pendingLinkReconciliation` is a plain `HashMap`, not GC-friendly like `CompressedRID2RIDIndex`

> ... this could add meaningful heap pressure on large imports. Given the project's "always bear
> in mind PERFORMANCE... prefer arrays of primitives" guidance, this is probably fine as a first
> cut for correctness, but may be worth a follow-up if large-import memory becomes a concern.

Skipped per the reviewer's own qualification ("probably fine as a first cut"). Only vertices/
documents/edges with an *unresolved forward-reference* LINK property are added to this map, not
every record, so it scales with the forward-reference count rather than with import size. Worth
revisiting only if a large-import memory profile actually shows this as a hotspot.

### 5. `remapLinkProperties` and `reconcileUnresolvedLinks` duplicate similar list/map-walking logic

> ... a shared helper (e.g. "map over list/map entries applying a per-item resolver") could reduce
> the duplication if it comes up again.

Skipped: the reviewer's own framing ("if it comes up again") marks this as optional. The two
methods operate on different value shapes (String RIDs pre-conversion vs. real `RID` objects
post-conversion) and different failure semantics (record the property name as unresolved vs. leave
the value alone), so a shared generic helper would add an abstraction layer without removing much
real duplication. Not worth the risk of the refactor this late in review.
