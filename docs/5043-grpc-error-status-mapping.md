# Issue #5043 - gRPC Error/Status mapping loses engine exception types

## Symptom
The gRPC layer collapses distinct engine exceptions into a generic `success=false` message or a
coarse `INTERNAL`/`ABORTED` status, so the client cannot reconstruct the original exception type.
`RemoteDatabase.transaction()`'s retry-on-`NeedRetryException` works over HTTP but is silently inert
over gRPC, and permanent conflicts (duplicate key at commit) are mis-typed as retryable.

## Root cause
There is no single, consistent exception -> `io.grpc.Status` mapping and the response protobufs carry
only a free-text `message`, not the exception class name. HTTP ships the exception class name so the
client rebuilds the right type; gRPC never adopted this.

## Scope of this fix (test-safe subset of findings COR-2, COR-3, TX-9, COR-15)
The in-band `success=false` contract of `executeCommand` (COR-1) is deliberately asserted by existing
tests (`Issue4794GrpcPerDbAuthorizationIT`, `Issue5040GrpcTransactionHijackIT`,
`Issue4804GrpcCommandErrorMetricsTest`), and the "never modify existing tests" constraint forbids
flipping it to `onError`. Likewise SEC-6 message-sanitization risks breaking message-content
assertions across the suite. Both are called out as follow-ups. This PR fixes the `onError` paths where
type preservation is both correct and non-breaking:

- **COR-3** `commitTransaction` no longer blanket-maps every commit failure to `ABORTED`. It routes the
  cause through a central `GrpcErrorMapper` so a commit-time `DuplicatedKeyException` becomes
  `ALREADY_EXISTS` (non-retryable) while genuine `ConcurrentModificationException`/`NeedRetryException`
  stay `ABORTED` (retryable).
- **COR-2** `DuplicatedKeyException` is mapped to `ALREADY_EXISTS` carrying the real index name + keys in
  status trailers (`createRecord` and commit paths); the client reconstructs a proper
  `DuplicatedKeyException` with correct `indexName`/`keys` instead of `DuplicatedKeyException(msg,msg,null)`.
- **TX-9** `beginTransaction`'s broad `catch (Throwable)` now passes an already-mapped
  `StatusRuntimeException` (the `UNAUTHENTICATED`/`PERMISSION_DENIED` from `getDatabase`) straight through
  instead of wrapping it in `INTERNAL`.
- **COR-15** `executeQuery` with an unknown transaction id returns `FAILED_PRECONDITION` instead of
  `INTERNAL`, so the client can distinguish a programming error from a server fault.

## Mechanism
`GrpcErrorMapper` (server, `grpcw`) converts a `Throwable` to a `StatusRuntimeException` with the correct
`Status.Code` and a metadata trailer `arcadedb-exception-class` (plus `arcadedb-dup-index` /
`arcadedb-dup-keys` for duplicate keys). `GrpcClientErrorMapper` (client, `grpc-client`) reconstructs the
exact engine exception type from that trailer, falling back to the legacy status-code switch when the
trailer is absent (backward compatible with older servers). No proto change is needed - trailers are the
standard gRPC out-of-band error-detail channel.

## Tests
- `GrpcErrorMapperTest` (grpcw, unit) - server maps each engine exception to the right code + trailers.
- `Issue5043GrpcErrorTypeReconstructionTest` (grpc-client, unit) - client rebuilds the exact type from the
  trailer, incl. duplicate-key index/keys, and CME stays retryable while duplicate-key does not.

## Impact
Retry-on-`NeedRetryException` now behaves over gRPC as over HTTP; permanent conflicts are no longer
retried forever; security failures surface as security exceptions; duplicate keys surface with correct
index/key details. Backward compatible: a server without the trailer still maps by status code.
