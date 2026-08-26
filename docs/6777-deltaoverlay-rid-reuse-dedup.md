# Issue #6777: DeltaOverlay's RID-keyed deletion dedup is unsafe across bucket slot reuse

https://github.com/ArcadeData/arcadedb/issues/6777

## Root cause

`DeltaOverlay.merge()` dedups edge deletions by the deleted edge's own RID
(`deletedEdgeRIDsPerType: Map<String, Set<RID>>`), to tell "this exact deletion event
replayed" apart from "two distinct parallel edges between the same pair, each deleted"
(#6769). RIDs are not permanently unique identities: `LocalBucket` reuses a slot freed by
a delete for a later insert ("hole reuse", #5279).

Traced the actual reachable window (the issue asked to pin this down before choosing a
fix):

- In the **live, non-compaction** merge path (`merge(delta, baseMapping)`), every
  genuinely new edge add is unconditionally recorded in `addedEdgesPerType`, keyed by its
  own RID. So a later deletion of that same RID **always** finds it there and takes the
  "withdraw the add" branch (#6775), which is safe regardless of what
  `deletedEdgeRIDsPerType` already holds. The dedup Set is never consulted for a
  same-window add+delete pair.
- The **only** way an edge's add is *not* recorded in `addedEdgesPerType` is the
  post-compaction buffered-delta re-application path (`merge(delta, baseMapping,
  baseCsrPerType)`, #4588): if the freshly built base CSR already contains the edge's
  pair (the read-committed scan crossed its bucket after the edge committed), the add is
  skipped as "already represented by the fresh base CSR", the edge is treated as part of
  the base, not tracked by identity.

Combining these: if an edge E1 is deleted (RID `r` recorded in
`deletedEdgeRIDsPerType`), and, within the same buffered-delta replay, in commit order,
a **different** edge E2 is created reusing RID `r` (physically possible only after E1's
delete freed the slot) and the fresh base CSR already reflects E2 (so its add is skipped
per #4588), then E2's own later deletion looks up `r` in `deletedEdgeRIDsPerType`, finds
it already present (from E1), and is silently dropped by `Set.add()` returning `false`.

The window is exactly the post-compaction buffered-delta replay of one compaction cycle,
narrower than "the whole overlay lifetime" the issue worried about, but real.

## Fix

When an edge add is skipped in the `baseCsrPerType != null` branch because the fresh base
CSR already represents it (the "already represented by the fresh base CSR" `continue`
path), also drop that RID from `deletedEdgeRIDsPerType` for the edge type, if present.
Going forward, that RID's dedup identity belongs to the edge the fresh base now says is
live at that physical slot, not to whatever edge originally recorded a deletion under it.
This does not disturb the earlier deletion's already-recorded exclusion budget
(`deletedEdgesPerType`, keyed by pair, not RID), only the identity-dedup Set entry that
would otherwise falsely absorb a later, unrelated deletion under the same reused RID.

## Test plan

- `DeltaOverlayCompactionDedupTest.deletionOfDifferentEdgeReusingAReplayedRidIsNotDropped()`
  (new): reproduces the exact 3-step buffered-delta sequence above: delete E1 (pair
  0→1, RID r); add E2 (pair 0→2, RID r, already in the fresh base CSR so skipped); delete
  E2 (pair 0→2, RID r). Asserts E2's deletion is recorded (count, `isEdgeDeleted`,
  `deltaEdgeCount`) without disturbing E1's own recorded exclusion.
- Full `DeltaOverlayTest` + `DeltaOverlayCompactionDedupTest` suites, to confirm the #4587
  and #6769/#6775 regression coverage stays green.
- `mvn -o -pl engine -am test -Dtest=DeltaOverlayTest,DeltaOverlayCompactionDedupTest`

## Verification

- New test `deletionOfDifferentEdgeReusingAReplayedRidIsNotDropped` confirmed the bug
  pre-fix (`isEdgeDeleted(EDGE_TYPE, 0, 2)` was `false`), and passes post-fix.
- `DeltaOverlayTest` (11) + `DeltaOverlayCompactionDedupTest` (7): all green, including the
  #4587 replay-dedup and #6769/#6775 parallel-edge/withdraw regression tests - the fix
  does not weaken any of that existing coverage.
- Full `com.arcadedb.graph.olap` package (268 tests, excluding benchmark/slow/vector
  lanes): all green.
- `mvn -o -pl engine -am compile`: clean.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/6787

## Review cycles

- Cycle 1: head `2eb1899871a5edebf2df71e7efeba014fcc3b283`. `claude` reviewed: no
  blocking issues, clean approval. `coderabbitai` flagged one actionable item: em dashes
  in `docs/6777-deltaoverlay-rid-reuse-dedup.md` (repo convention forbids them). Applied
  directly (commit `afc3c417a4`).
- Cycle 2: head `afc3c417a452110e93a97e06fa985832636beec5`. `claude` reviewed: no
  blocking issues, clean approval (traced the fix's correctness against the buffered-delta
  replay invariants, confirmed the regression test's expected values by hand). No
  actionable items from any reviewer on this head. Working tree clean, no deferred items.

## Deferred items

None.

## Status

`clean-approval`. Ready for developer merge.
