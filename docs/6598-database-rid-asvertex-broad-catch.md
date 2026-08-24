# #6598 - `DatabaseRID.asVertex(boolean)` reports every failure as "record not found"

## Root cause

`DatabaseRID.asVertex(boolean)` (`engine/src/main/java/com/arcadedb/database/DatabaseRID.java`) caught
`Exception` broadly around `database.lookupByRID(...)` and rewrapped anything that was not already a
`RecordNotFoundException` into one. Its sibling, `RID.asVertex(boolean)`, only ever caught `ClassCastException`
(a RID that names a record which exists but is not a `Vertex`). The broad catch was introduced when
`DatabaseRID` was added in 7d7d7aac6 and widened `ClassCastException` to `Exception` in the process.

Consequences of the broad catch:
- A `SecurityException` (permission denial) was reported to the caller as "record not found" - a healthy,
  present record the caller simply was not allowed to read looked like data corruption.
- A `ConcurrentModificationException` (`extends NeedRetryException`) - e.g. from `loadMultiPageRecord`
  exhausting `TX_RETRIES` on a multi-page vertex body - silently became a non-retryable
  `RecordNotFoundException`. The retry machinery never saw it, and the caller concluded the vertex was gone
  when it was merely contended.

## Affected component

`engine/src/main/java/com/arcadedb/database/DatabaseRID.java` - `asVertex(boolean)`.

## Fix

Narrowed the catch from `Exception` to `ClassCastException`, matching `RID.asVertex(boolean)` exactly (the
`RecordNotFoundException` passthrough arm is unchanged). This restores the pre-`DatabaseRID` behaviour and
removes the whole class of masking. `asDocument`/`asEdge` in the same class already have no try/catch at all,
so this also brings `asVertex` back in line with its neighbours.

Per the issue's own preference ordering, this only applies fix (1) - the narrower catch. Fix (2) (also
converting the surviving `ClassCastException` -> `RecordNotFoundException` translation into a message that
names the actual type found) is explicitly called out in the issue as a separate behaviour-changing decision
and is out of scope here.

Items 2 (`ACCESS.DELETE_RECORD`'s past-tense wire name `deletedRecord`) and 3 (a note that #6491's diagnosis
basis moved) are explicitly the "latent trap" / "pointer" halves of #6598, not "the one worth acting on" -
left untouched, as the issue itself recommends.

## Test plan

New test: `engine/src/test/java/com/arcadedb/database/Issue6598DatabaseRidAsVertexBroadCatchTest.java`

- Unit level (Mockito stub of `BasicDatabase`, isolates the catch contract from which engine code path
  happens to raise each exception today):
  - `aSecurityExceptionFromLookupSurvivesAsVertexUnwrapped` - a `SecurityException` from `lookupByRID`
    reaches the caller unwrapped, not as `RecordNotFoundException`.
  - `aRetryableConflictFromLookupSurvivesAsVertexUnwrapped` - a `ConcurrentModificationException` reaches
    the caller unwrapped and stays a `NeedRetryException`.
  - `aRecordNotFoundExceptionFromLookupIsRethrownAsIs` - unchanged passthrough behaviour.
  - `aRecordThatIsNotAVertexStillReportsAsNotFound` - the one case `asVertex()` is meant to translate (a
    `ClassCastException` because the record is not a `Vertex`) still becomes `RecordNotFoundException`.
- End-to-end: `aPermissionDenialReachedThroughAsVertexIsNotReportedAsRecordNotFound` - a real database, a
  principal granted `deleteRecord` but not `readRecord` (the exact scenario reported in the issue, reused
  from #6586's `aPrincipalWithoutReadPermissionCannotReachTheDeleteAtAll` fixture pattern), reached through
  `guarded[0].asVertex()` - the shortcut ordinary caller code uses, as opposed to `lookupByRID` directly,
  which #6586's test used specifically to sidestep this bug.

## Verification

- `mvn -pl engine -am test -Dtest=Issue6598DatabaseRidAsVertexBroadCatchTest` - RED before the fix (3 of 5
  assertions failed: the `SecurityException` and `ConcurrentModificationException` passthrough unit tests,
  plus the end-to-end permission scenario), GREEN after.
- `mvn -pl engine -am test -Dtest=Issue6598DatabaseRidAsVertexBroadCatchTest,DatabaseRIDTest,Issue6586MissingVertexDiagnosisTest,Issue6572DanglingVertexReferenceTest`
  - 32 tests, 0 failures.
- `mvn -pl engine -am test -Dtest=Issue687ClassCastEdgeCheckTest,Issue5279ConcurrentUpdateTest,RecordNotMaterialisedTest`
  - 21 tests, 0 failures.

## PR

https://github.com/ArcadeData/arcadedb/pull/6681

## Review cycles

- Cycle 1: head `5ae25f9d0b` - claude bot review (issue comment, posted 2026-08-24T15:08:23Z): "looks
  correct, safe, and ready to merge pending CI." No actionable items - two minor/non-blocking observations
  only (a note that any other `RuntimeException` besides `SecurityException`/`ConcurrentModificationException`
  now also propagates unwrapped, which is the intended, matching-`RID.asVertex()` behavior; and a sanity-check
  suggestion on the end-to-end test's `factory.close()` after `restricted.drop()`, which the reviewer itself
  judged "almost certainly fine"). Working tree stayed clean - no code change made in response.

## Deferred items

None. Both of the bot's observations were non-blocking FYI notes, not requests for a code change.

## Final state

clean-approval (cycle 1 of 4 max).
