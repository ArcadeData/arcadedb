# #7370 - gRPC TimeSeriesQuery / TimeSeriesLatest cannot run inside a client transaction

Branch: `fix/7370-grpc-timeseries-query-latest-in-tx`

## Problem

`TimeSeriesQueryRequest` and `TimeSeriesLatestRequest` (#7305 / PR #7323) carry `database` and
`credentials` but no `TransactionContext`. Both handlers resolve the database directly:

```
$ grep -n "getDatabase(req.getDatabase(), req.getCredentials())" grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java
3282:      final Database db = getDatabase(req.getDatabase(), req.getCredentials());          <- timeSeriesWrite (out of scope)
3409:      final DatabaseInternal db = (DatabaseInternal) getDatabase(...);                   <- timeSeriesQuery
3638:      final DatabaseInternal db = (DatabaseInternal) getDatabase(...);                   <- timeSeriesLatest
```

So the two RPCs cannot be told which transaction they belong to, and every other read RPC on the
service can be: `executeQuery`, `streamQuery`, `lookupByRid`, and - since #7326 - the three search
RPCs all resolve one.

## Two premises of the issue that do not hold, and what is left of the bug

Both were checked with commands before the fix was written, because they are what the issue's
reasoning rests on.

### 1. A time-series append is not part of the enclosing transaction (filed as #7410)

The issue reasons from "a client that opened a gRPC transaction, appended points through some other
RPC in it and then queries the series reads outside its own transaction". There is nothing to miss.
`TimeSeriesShard.appendSamples` wraps the mutable-bucket write in its own transaction:

```
$ sed -n '246,276p' engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesShard.java
  public void appendSamples(final TimeSeriesRowSource source) throws IOException {
    final DatabaseInternal db = database.getWrappedDatabaseInstance();
    ...
          db.begin();
          ...
            db.commit();
```

and an ArcadeDB nested transaction is an independent transaction, not a savepoint:

```
$ sed -n '628,674p' engine/src/main/java/com/arcadedb/database/LocalDatabase.java
  public void begin(...) { ... if (tx.isActive()) { tx = new TransactionContext(...); current.pushTransaction(tx); } tx.begin(...); }
  public void commit()   { ... current.getLastTransaction().commit(); ... current.popIfNotLastTransaction(); }
$ sed -n '302,319p' engine/src/main/java/com/arcadedb/database/TransactionContext.java
  public Binary commit() { ... commit1stPhase(true) ... commit2ndPhase(phase1) ... }   <- publishes pages itself
```

Measured end to end by `timeSeriesAppendsAreNotPartOfTheEnclosingTransaction`: a sample appended
inside a transaction is visible to a second connection before any commit, and survives that
transaction's rollback. A DOCUMENT row inserted in the same transaction is the control - it is
invisible outside and it does disappear on rollback - so the transaction really was live.

`TimeSeriesEngine.appendSamples`' javadoc asserts the opposite ("nests into that transaction, so the
mutable-bucket page writes are published and replicated by the enclosing commit"), and `appendBatch`'s
#4957 threading decision rests on that claim. Filed as **#7410**.

### 2. The HTTP time-series routes do not bind the session transaction either (filed as #7402)

The issue says they "run through `DatabaseAbstractHandler`, which binds the session's transaction".
They do not:

```
$ grep -n "extends" server/src/main/java/com/arcadedb/server/http/handler/{Post,Get}TimeSeries*.java | grep class
PostTimeSeriesWriteHandler.java:58:public class PostTimeSeriesWriteHandler extends AbstractServerHttpHandler {
GetTimeSeriesLatestHandler.java:40:public class GetTimeSeriesLatestHandler extends AbstractServerHttpHandler {
PostTimeSeriesQueryHandler.java:47:public class PostTimeSeriesQueryHandler extends AbstractServerHttpHandler {
```

`AbstractServerHttpHandler` never reads `HttpSessionManager.ARCADEDB_SESSION_ID`; only
`DatabaseAbstractHandler.setTransactionInThreadLocal` does. The handlers say so themselves, in the
GHSA-x8mg-6r4p-87pf comment each of them carries. Filed as **#7402**.

### What is left

The observable defect, and the one this branch fixes: a request naming a transaction the server no
longer knows - reaped, committed, or invented - was read straight through and answered as if nothing
were wrong, where every other transaction-scoped RPC answers FAILED_PRECONDITION. On top of that, the
structural change the issue asks for is still right and is what makes the new `transaction` field
honest: the read runs on the transaction's own thread, against the transaction's own handle,
authorized against the transaction's real database rather than a request-supplied name.

## Invariant

> A time-series read RPC that names a live gRPC transaction runs on that transaction's own thread and
> against its own database handle, and a non-blank transaction id the server does not know is refused
> instead of silently read outside the transaction the caller believes it is inside.

## Completeness

### Enumeration

Every handler in the gRPC service, and whether it resolves a caller transaction:

```
$ python3 - <<'EOF'   # per-handler scan for resolveAuthorizedTransaction
  678 executeCommand           tx=YES     3273 timeSeriesWrite         tx=no
  998 createRecord             tx=YES     3300 timeSeriesWriteStream   tx=no
 1138 lookupByRid              tx=YES     3401 timeSeriesQuery         tx=no   <- this issue
 1210 updateRecord             tx=YES     3632 timeSeriesLatest        tx=no   <- this issue
 1401 deleteRecord             tx=YES     3842 insertBidirectional     tx=YES (in onNext)
 1505 executeQuery             tx=YES     3049 graphBatchLoad          tx=no
 2039 streamQuery              tx=YES     5086 vectorSearch            tx=YES (via searchInTransaction)
 2623 bulkInsert               tx=YES     5097 hybridSearch            tx=YES (via searchInTransaction)
 1736 beginTransaction         tx=YES     5108 fullTextSearch          tx=YES (via searchInTransaction)
EOF
```

All four time-series RPCs in the proto:

```
$ grep -n "rpc TimeSeries" grpc/src/main/proto/arcadedb-server.proto
194:  rpc TimeSeriesWrite       (TimeSeriesWriteRequest)      returns (TimeSeriesWriteSummary);
195:  rpc TimeSeriesWriteStream (stream TimeSeriesWriteChunk) returns (TimeSeriesWriteSummary);
196:  rpc TimeSeriesQuery       (TimeSeriesQueryRequest)      returns (stream TimeSeriesQueryResult);
197:  rpc TimeSeriesLatest      (TimeSeriesLatestRequest)     returns (TimeSeriesLatestResponse);
```

Every caller of the two request messages:

```
$ grep -rn "TimeSeriesQueryRequest\|TimeSeriesLatestRequest" --include="*.java" --include="*.proto" . | grep -v /target/
grpc/src/main/proto/arcadedb-server.proto:882,931          message definitions
grpc-client/.../RemoteGrpcDatabase.java:2459,2535          the only two builders in the tree
grpcw/.../ArcadeDbGrpcService.java:3401,3459,3530,3632     the two handlers + their two stream helpers
server/.../openapi/TimeSeriesApiSpec.java:46,102           an unrelated HTTP JSON schema of the same NAME
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| gRPC `TimeSeriesQuery` naming an unknown/expired transaction | yes | yes - `aTimeSeriesQueryNamingAnUnknownTransactionIsRejected` (**fails without the fix**) |
| gRPC `TimeSeriesLatest` naming an unknown/expired transaction | yes | yes - `aTimeSeriesLatestNamingAnUnknownTransactionIsRejected` (**fails without the fix**) |
| gRPC `TimeSeriesQuery` carrying a blank transaction id (= no external transaction) | yes | yes - `aBlankTransactionIdIsNotTreatedAsAnUnknownOne` |
| gRPC `TimeSeriesQuery`, raw-row branch (`streamTimeSeriesRows`) on the transaction thread | yes | yes - `timeSeriesQueryInsideATransactionReturnsTheWholeSeries` |
| ... same, read before and after a write in one transaction (page-cache staleness) | yes | yes - `aSecondQueryInTheSameTransactionSeesWhatWasAppendedBetweenThem` |
| gRPC `TimeSeriesQuery`, aggregated branch (`streamTimeSeriesBuckets`) on the transaction thread | yes | yes - `anAggregatedTimeSeriesQueryInsideATransactionReturnsTheRightBuckets` |
| gRPC `TimeSeriesQuery`, multi-message stream (`batch_size=1`) on the transaction thread | yes | yes - `aMultiMessageTimeSeriesStreamRunsToCompletionOnTheTransactionThread` |
| gRPC `TimeSeriesLatest` on the transaction thread | yes | yes - `timeSeriesLatestInsideATransactionReturnsTheNewestSample` |
| gRPC `TimeSeriesLatest` with a tag filter on the transaction thread | yes | yes - `aTagFilteredTimeSeriesLatestInsideATransactionSelectsItsOwnSeries` |
| An error raised on the transaction thread keeps its status across the executor boundary | yes | yes - `anErrorRaisedInsideTheTransactionKeepsItsStatus` |
| Both RPCs with no transaction open (the unchanged default) | yes | yes - `aQueryWithNoTransactionStillAnswersFromOutsideEveryTransaction` |
| `RemoteGrpcDatabase.timeSeriesQuery` / `.timeSeriesLatest` stamp the open transaction | yes | yes - every high-level test above goes through them |
| TimeSeries appends are not part of the enclosing transaction | **no** - #7410 | pinned by `timeSeriesAppendsAreNotPartOfTheEnclosingTransaction` |
| HTTP `/api/v1/ts` read routes ignore `arcadedb-session-id` | **no** - #7402 | pinned by `theHttpTimeSeriesRoutesDoNotYetJoinTheSessionTransaction` |
| gRPC `TimeSeriesWrite` / `TimeSeriesWriteStream` do not join a caller transaction | **no** - argued | - |
| gRPC `GraphBatchLoad` does not join a caller transaction | **no** - argued | - |

### Proof the tests can fail

With only the server-side dispatch reverted (`incomingTxId` forced to `null` in `timeSeriesQuery`,
and `null` passed to `readInTransaction` in `timeSeriesLatest`), the proto field and the client
stamping left in place:

```
Tests run: 13, Failures: 2, Errors: 0, Skipped: 0
  aTimeSeriesQueryNamingAnUnknownTransactionIsRejected
  aTimeSeriesLatestNamingAnUnknownTransactionIsRejected
```

Exactly the two tests that assert the defect, and no others - which is the honest shape of this
change: one behaviour fixed, eleven paths guarded. (That run predates the last two tests; the two
failures are the same two.)

### Reachability

`timeSeriesQuery` and `timeSeriesLatest` are the service's implementations of two RPCs declared in the
only proto in the tree; `RemoteGrpcDatabase` is the only builder of either request. Nothing is gated
by a flag, and the new branch is entered by a field the client now sets whenever a transaction is
open. The 13 tests drive the whole path against a live server over the real wire.

## Test results

```
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0 -- Issue7370GrpcTimeSeriesInTransactionIT
```

Connected suites, all green together (98 tests):

```
Issue7370GrpcTimeSeriesInTransactionIT   13
Issue7305TimeSeriesGrpcIT                13
Issue7326GrpcSearchInTransactionIT        8   (the readInTransaction rename)
Issue7306GrpcVectorSearchIT               5
TimeSeriesGrpcInsertMaterializationIT     1
RemoteGrpcDatabaseCoverageIT             32
RemoteGrpcServerIT                       13
RemoteGrpcTransactionExplicitLockIT      12
Issue4260ReloadInsideTransactionIT        1
```

`grpcw` module: `Tests run: 164, Failures: 0, Errors: 0` plus `GrpcRaftReplicationIT` (2 tests) run
separately - its fork crashed at startup on the shared run under three concurrent Maven builds, and it
passes in isolation. It references neither time series nor search (`grep -c "timeSeries\|Search"` = 0).

### One test could not be run in this environment

`Issue7305TimeSeriesGrpcAclIT` errors with `503 for URL: http://127.0.0.1:2480/api/v1/command/graph`.
`BaseGraphServerTest.command()` hard-codes `http://127.0.0.1:248<serverIndex>`, and a Homebrew
ArcadeDB 26.9.1 service is listening on `*:2480` on this machine, so the request never reaches the
test's own server. **Verified pre-existing**: reverting all three changed files to `origin/main`,
rebuilding, and running that class alone reproduces the identical error. CI, where 2480 is free, will
run it.

## Changes

| File | Change |
|---|---|
| `grpc/src/main/proto/arcadedb-server.proto` | `TransactionContext transaction` on `TimeSeriesQueryRequest` (11) and `TimeSeriesLatestRequest` (5) |
| `grpcw/.../ArcadeDbGrpcService.java` | `searchInTransaction` generalized to `readInTransaction` (now takes a checked-throwing `DatabaseRead`); `timeSeriesLatest` routes through it; `timeSeriesQuery` gets `streamQuery`'s streaming dispatch and its body is extracted into `streamTimeSeries` |
| `grpc-client/.../RemoteGrpcDatabase.java` | both TS methods stamp the connection's open transaction |
| `grpc-client/src/test/.../Issue7370GrpcTimeSeriesInTransactionIT.java` | new, 13 tests |

## Residual risk

- **#7410** - a time-series append commits itself rather than joining the caller's transaction, and
  `TimeSeriesEngine`'s javadoc says the opposite. Until that is resolved, this fix changes *where* a
  time-series read runs but not *what samples it sees*. Said plainly in the handler javadoc, in the
  test class javadoc, and in the PR body, so nobody reads more into it than was measured.
- **#7402** - the HTTP time-series read routes still ignore `arcadedb-session-id`. This branch makes
  gRPC strictly more transactional than HTTP for time series, which is the opposite asymmetry to the
  one #7305 set out to avoid.
- `TimeSeriesWrite` / `TimeSeriesWriteStream` still commit their own per-shard transactions. The issue
  puts this out of scope by name ("deliberately non-atomic - the proto documents that each
  measurement's batch commits its own shard transaction"), and the proto does document it. Given
  #7410 this is the same fact stated at the RPC level, not a separate one.
- `GraphBatchLoad` is the same shape (a bulk writer with its own commit batching) and is likewise not
  touched here.

## Adversarial pass

The orchestrator's Phase 1.5 asks for an isolated `general-purpose` subagent. **No `Task` tool is
available in this session** (`ToolSearch "select:Task"` returns no match), so the pass was run by the
author instead - which is weaker, because the point of the subagent is that it has not already been
convinced. Recorded here rather than skipped silently. What it turned up:

1. **The aggregation fan-out branch was not actually being exercised.** `aggregateMulti` fans its
   sealed reads across `shardExecutor` only when `shardCount > 1`, and `TimeSeriesTypeBuilder` defaults
   an unspecified shard count to `ASYNC_WORKER_THREADS` - so which branch the test hit depended on the
   core count of the machine running it. **Fixed here:** the test type is created with `SHARDS 4`.
   Checked at the same time that the fan-out decision does not read `isTransactionActive()` (it reads
   only `shardCount > 1 && maxBuckets > 0`), and that the mutable half stays on the calling thread,
   which is now the transaction's thread and has the DatabaseContext the comment there requires.
2. **An error raised after dispatch crosses an `ExecutionException`.** The INVALID_ARGUMENT
   `streamTimeSeriesBuckets` raises for `bucket_interval_ms <= 0`, and the NOT_FOUND from type
   resolution, are now raised on the transaction's executor thread. **Fixed here** by unwrapping in
   `timeSeriesQuery`, and covered by `anErrorRaisedInsideTheTransactionKeepsItsStatus`.
   (`GrpcErrorMapper` also unwraps, so this is belt and braces rather than a bug that was shipping -
   said plainly rather than claimed as a save.)
3. **`getDatabase`'s `validateDatabaseName` path-traversal guard is skipped on the transaction
   branch.** *Not real.* On that branch the request's `database` field is not used for anything: the
   handle is `txCtx.db`, and `authorizeTransactionAccess` deliberately authorizes against
   `txCtx.db.getName()` rather than the request-supplied name. Nothing reaches the filesystem with a
   caller-supplied string. Same shape, and same argument, as `readInTransaction`'s three existing
   search callers.
4. **`ProtocolContext` is not set on the transaction's executor thread.** *Not real for this path.*
   `grep -rn "ProtocolContext.get"` over `src/main` returns two consumers, `QueryMetricsRecorder` and
   `QueryTracer`, neither of which the time-series read path calls. `streamQuery` has had exactly this
   shape since it was written.
5. **A long stream could be reaped mid-flight.** *Not real.* `reapIdleTransactions` claims the
   transaction with `activeTransactions.remove(key, ctx)` and then submits the rollback to the same
   single-threaded executor, which is already running our task; `ThreadPoolExecutor.shutdown()` lets a
   running task finish. The stream completes and the rollback runs after it - the same guarantee
   `streamQuery` relies on.
6. **`waitUntilReady` on the transaction thread could deadlock against a slow consumer.** *Not real.*
   The wait is bounded by `arcadedb.server.grpcStreamWriteTimeoutMs` and sets `serverTimedOut`, which
   the handler turns into DEADLINE_EXCEEDED. Exercised with `batch_size=1` over five rows in
   `aMultiMessageTimeSeriesStreamRunsToCompletionOnTheTransactionThread`.
