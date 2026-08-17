# Issue #6192 — gRPC executeCommand flattens every failure into success=false

## Problem

`ArcadeDbGrpcService.executeCommandInternal` caught every exception except
`ServerIsNotTheLeaderException` and returned an `ExecuteCommandResponse` with
`success=false` and the exception's message as free text, instead of failing the
RPC. The client (`RemoteGrpcDatabase.commandInternal`) turned that back into a
single untyped `DatabaseOperationException`, so:

- `RemoteGrpcDatabase.transaction()`'s retry on `NeedRetryException` never fired
  for a command (only for a commit), because a `ConcurrentModificationException`
  raised by a command arrived client-side as `DatabaseOperationException`.
- `DuplicatedKeyException`'s index name and offending keys, already shipped on
  trailers by `GrpcErrorMapper` for every other RPC that uses it, were lost.
- Every other gRPC RPC (`createRecord`, `beginTransaction`, `commitTransaction`,
  `graphBatchLoad`) and the HTTP protocol both route failures through a typed
  error path; `executeCommand` was the odd one out.

## Fix

- `ArcadeDbGrpcService.executeCommandInternal`: the catch block no longer builds
  a `success=false` envelope for a generic failure. It always rethrows (after
  the existing best-effort rollback), same as it already did for
  `ServerIsNotTheLeaderException`.
- `ArcadeDbGrpcService.executeCommand`: the outer catch now routes every failure
  through `GrpcErrorMapper.toStatusRuntimeException(cause, "ExecuteCommand", ha())`
  and `resp.onError(...)`, instead of special-casing only the leader refusal and
  building an in-band failure response for everything else. The client's
  existing `handleGrpcException` / `GrpcClientErrorMapper` already reconstructs
  the exact engine exception type from the status + trailers, so this is a
  server-only change.
- Added a `responded` guard around the success path's `onNext`/`onCompleted` so
  the catch block never calls `onError` after a response has already been fully
  delivered (issue's point 3 - the existing code had the same latent exposure
  once the catch also called `onNext`).
- `ErrorHandlingIT.duplicateKey_throwsDuplicatedKeyException` no longer tolerates
  `DatabaseOperationException` as an acceptable outcome (the test was written to
  tolerate exactly the gap this issue describes); it now asserts
  `DuplicatedKeyException` with `getIndexName()`/`getKeys()` populated, proving
  the type (and the trailers `GrpcErrorMapper` already shipped for every other
  RPC) now survives an `executeCommand` failure over gRPC end to end.
- Five existing IT/unit tests were written to pin the pre-fix
  `success=false`-in-band behaviour and are updated to assert a
  `StatusRuntimeException` instead, each with a comment pointing at #6192:
  `Issue6183FollowerCommandRoutingIT.anOrdinaryCommandFailureSurfacesAsAGrpcError`
  (renamed from `...StillComesBackInTheResponse`),
  `Issue5040GrpcTransactionHijackIT.crossTenantExecuteCommandIsDenied`,
  `Issue4794GrpcPerDbAuthorizationIT.crossDatabaseCommandIsDenied`,
  `GrpcServerIT.executeCommandInvalidSqlReturnsError`, and
  `GrpcTransactionScriptingAuthorizationIT.readerCannotEscalateViaJsInsideExternalTransaction`
  / `.readerCannotWriteViaSqlInsideExternalTransaction`.
- `TimeSeriesGrpcHaConcurrentInsertIT` and `Issue4804GrpcCommandErrorMetricsTest`
  were checked and need no change: the former already collects both an in-band
  failure and a thrown exception into the same `errors` list, and the latter
  drives `GrpcMetricsInterceptor` directly with a hand-built response rather
  than exercising `executeCommand`, so it is independent of this change (its
  in-band-failure scenario simply no longer occurs for a real command error,
  the interceptor branch it pins stays correct for whatever might still send one).

## Explicitly out of scope (per the issue's own "points to settle")

- **Wire compatibility.** `success=false` in-band was public wire behaviour;
  this change breaks it for the next minor. The issue itself recommends
  accepting the break rather than adding a deprecation flag, since every other
  RPC already made this move - no compatibility flag was added.
- **`success` field.** Left in the proto/response as-is (always `true` on a
  response that arrives at all now); no new meaning was added to it.
- **`executeQuery`/`streamQuery`** still map unexpected failures to a bare
  `Status.INTERNAL` rather than through `GrpcErrorMapper` (issue's point 5).
  The issue flags this as "worth folding into the same change" but it is a
  distinct code path with its own existing status-preservation logic
  (`RESOURCE_EXHAUSTED`, `FAILED_PRECONDITION`, `NOT_FOUND`, etc. are already
  preserved case-by-case). Left untouched here to keep this PR's blast radius
  to the RPC the issue is titled after; worth a follow-up if the maintainer
  wants the whole service on one error path.

## Verification

All run with `-Dmaven.repo.local=<worktree>/.m2repo`:

- `mvn -pl grpcw,grpc-client -am compile` - clean compile.
- `mvn -pl grpcw,grpc-client -am test-compile` - test sources compile (covers all 6 edited test files).
- `mvn -pl grpc-client -am verify -DskipITs=false -Dit.test=ErrorHandlingIT -Dfailsafe.failIfNoSpecifiedTests=false -DskipTests`
  - `Tests run: 8, Failures: 0, Errors: 0` (includes the rewritten `duplicateKey_throwsDuplicatedKeyException`).
- `mvn -pl grpcw -am verify -DskipITs=false -Dit.test=Issue4794GrpcPerDbAuthorizationIT,GrpcServerIT -Dtest=Issue4804GrpcCommandErrorMetricsTest,GrpcErrorMapperTest -Dfailsafe.failIfNoSpecifiedTests=false -Dsurefire.failIfNoSpecifiedTests=false`
  - `GrpcErrorMapperTest`: 11/11; `Issue4804GrpcCommandErrorMetricsTest`: 3/3; `GrpcServerIT`: 30/30; `Issue4794GrpcPerDbAuthorizationIT`: 4/4.
- `mvn -pl grpcw -am verify -DskipITs=false -Dit.test=Issue5040GrpcTransactionHijackIT -Dtest=GrpcTransactionScriptingAuthorizationIT -Dfailsafe.failIfNoSpecifiedTests=false -Dsurefire.failIfNoSpecifiedTests=false`
  - `GrpcTransactionScriptingAuthorizationIT`: 3/3; `Issue5040GrpcTransactionHijackIT`: 5/5.
- `mvn -pl grpcw -am verify -DskipITs=false -Dit.test=Issue6183FollowerCommandRoutingIT -DskipTests -Dfailsafe.failIfNoSpecifiedTests=false`
  - `Issue6183FollowerCommandRoutingIT`: 2/2 (3-node Raft cluster).

All green. No test outside the files listed above referenced `ExecuteCommandResponse.getSuccess()` on a
failure path (checked with `grep -rn "getSuccess()).isFalse()"` across `grpcw`/`grpc-client` tests before
and after the fix). A full `mvn -pl grpc-client -am verify -DexcludedGroups=benchmark,vector,slow` (all
modules, no `-DskipTests`) was started as a broader sanity net but aborted partway through the unrelated
`arcadedb-engine` unit suite for time; it was not needed to validate this change; every call site that
touches `executeCommand`'s error path was located by the `getSuccess()).isFalse()` grep above and by a
full-file read of every test that calls `executeCommand`, and each one is covered by the targeted runs
listed above instead.
