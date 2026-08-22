# Issue #6597: gRPC insertStream ignores InsertChunk.database

## Root cause

`arcadedb-server.proto` documents `InsertChunk.database` (field 1) as REQUIRED on the first
chunk of the `insertStream` client-streaming RPC. `ArcadeDbGrpcService#insertStream`'s
`onNext` handler never read it, the line that would extract it was commented out, and
instead sourced the database name exclusively from `InsertOptions.database` (field 8 of the
nested `options` submessage). A client that followed the proto and set only
`InsertChunk.database` on the first chunk failed with `Invalid database name: name is
required`, even though it supplied exactly what the contract requires.

`graphBatchLoad`'s `onNext(GraphBatchChunk)` reads `chunk.getDatabase()` directly and does not
have this bug, so the two client-streaming ingest RPCs disagreed about where the database name
lives.

## Affected components

- `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`,
  `insertStream(StreamObserver)`, first-chunk handling inside `onNext`.

## Expected vs actual behavior

- Expected: a first `InsertChunk` with `database` set (per the proto contract) is accepted,
  regardless of whether `InsertOptions.database` is also set.
- Actual (before fix): only `InsertOptions.database` was honored; `InsertChunk.database` was
  silently ignored, producing a misleading "name is required" error that points at the wrong
  field.

## Fix

Restored reading `InsertChunk.database` on the first chunk. When present (non-empty), it is
merged into the effective `InsertOptions` used to build the `InsertContext`, taking priority
over `InsertOptions.database`, which remains as a fallback for callers that set it there
instead. This mirrors `graphBatchLoad`'s existing `chunk.getDatabase()` contract exactly as the
issue recommended, keeping the two streaming ingest RPCs consistent.

## Test plan

- New regression test `Issue6597InsertStreamChunkDatabaseIT` in
  `grpcw/src/test/java/com/arcadedb/server/grpc/`:
  - Reproduces the bug: a first chunk with `InsertChunk.database` set and `InsertOptions`
    carrying no `database` field must succeed (fails before the fix with
    `INVALID_ARGUMENT: Invalid database name: name is required`).
  - Confirms `InsertOptions.database` still works as a fallback when `InsertChunk.database` is
    empty (pre-existing behavior, must not regress).
- Existing `insertStream` regression tests (`Issue5047InsertErrorRowIndexIT`,
  `Issue5041InsertStreamTxLifecycleIT`, `Issue4656InsertStreamConflictUpdateIT`,
  `Issue4644InsertStreamThreadHopIT`, `Issue4214InsertStreamConflictIgnoreIT`,
  `Issue4198InsertStreamCommitErrorIT`, `Issue4806InsertStreamMidStreamCommitFailureIT`) re-run
  to confirm no regression, since they all set `InsertOptions.database` and must keep working
  unchanged.

## Test results

- Before the fix: `Issue6597InsertStreamChunkDatabaseIT.insertStreamMustHonorChunkLevelDatabaseWhenOptionsDatabaseIsUnset`
  failed as expected (`summary.getInserted()` was `0` instead of `1`, because
  `InsertContext` creation threw `INVALID_ARGUMENT: Invalid database name: name is required`
  internally and the stream's own catch block absorbed it into the failed-row count).
  The fallback test (`insertStreamMustStillHonorOptionsDatabaseWhenChunkDatabaseIsUnset`)
  passed both before and after, confirming it exercises pre-existing behavior only.
- After the fix: both tests in `Issue6597InsertStreamChunkDatabaseIT` pass
  (`Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`).
- Full existing `insertStream` regression suite re-run after the fix: `Tests run: 28,
  Failures: 0, Errors: 0, Skipped: 0`.
- `mvn -o -pl grpcw -am compile`: `BUILD SUCCESS`, no new warnings introduced.

## Impact analysis

The change is additive and narrowly scoped to the first-chunk branch of `insertStream`'s
`onNext`: it only changes behavior when `InsertChunk.database` is non-empty, which was
previously silently discarded. Existing callers that set `InsertOptions.database` (and leave
`InsertChunk.database` empty) are unaffected, since the new branch is a no-op for them. No
other RPC or code path is touched.

## Recommendations for monitoring or future improvements

- The client-side workaround in `@arcadedb/client-grpc` (mirroring `database` into
  `options.database` on the first chunk) can now be removed, per the issue.
- `InsertChunk.credentials` (field 2) has the same "documented on the chunk, only read from
  `InsertOptions`" shape as `database` did; it was out of scope for this fix since the issue
  reports only the `database` defect, but is worth a follow-up look if a similar client-contract
  mismatch surfaces for credentials.
