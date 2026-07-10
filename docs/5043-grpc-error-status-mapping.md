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
  cause through a central `GrpcErrorMapper` so a commit-time `DuplicatedKeyException` maps to
  `ALREADY_EXISTS` while genuine `ConcurrentModificationException`/`NeedRetryException` stay `ABORTED`.
  Note: `RemoteDatabase.transaction()` retries on BOTH `NeedRetryException` and `DuplicatedKeyException`,
  so the status/type distinction does NOT gate the retry loop - the win is type fidelity: a user
  `catch (DuplicatedKeyException)` (outside `transaction()`) and the reconstructed `indexName`/`keys` now
  work over gRPC exactly as over HTTP, instead of surfacing as a generic `ConcurrentModificationException`.
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
Retry-on-`NeedRetryException` now behaves over gRPC as over HTTP; engine exception TYPES are preserved
across the wire (a duplicate key reconstructs as `DuplicatedKeyException` with correct `indexName`/`keys`
rather than a generic `ConcurrentModificationException`); security failures surface as security
exceptions. Backward compatible: a server without the trailer still maps by status code.

## PR
https://github.com/ArcadeData/arcadedb/pull/5184 (closes #5043)

## Review cycles
Reviewers: `claude` (gating, responded every cycle) and `gemini-code-assist` (gating; errored on the base
commit, COMMENTED with no feedback on cycle 2, then absent within the 15-min window on cycles 3-4).

- **Base 9a3fd4c** - claude flagged: (1) trailer-less `ALREADY_EXISTS` dropped the server message,
  (2) dup-key trailers used ASCII marshaller for arbitrary user data, (3) unused `NeedRetryException`
  import. All three fixed.
- **115340123** (cycle 1 fix) - client falls back to the server `msg` when dup trailers are absent;
  dup-index/dup-keys trailers are Base64-encoded (server) / decoded (client) so non-ASCII keys survive the
  ASCII-only metadata channel; unused import removed. Tests updated to the Base64 contract + non-ASCII
  round-trip coverage on both sides.
- **0122826a** (cycle 2 fix) - added `Issue5043GrpcDuplicatedKeyRoundTripTest` (server-backed, real wire
  round-trip: commit-time duplicate reconstructs as non-retryable `DuplicatedKeyException`); documented the
  `SecurityException`-branch reliance on `getDatabase` pre-mapping. claude review: "none blocking", only
  pre-merge item was the class-name sync guard.
- **da0ba2a7** (cycle 3 fix) - added `reconstructFromClassName_literalsMatchActualClassNames` guard test
  locking the FQ class-name string literals to the actual class names. claude cycle-4 review: "none
  blocking"; last nit (unused `ConcurrentModificationException` import) removed.

Deferred / skipped items are recorded in the local `docs/review-deferred-*.md` notes (not committed):
repointing the PRE-EXISTING `GrpcExceptionMappingTest` is deferred (constraint: never modify existing
tests); `IllegalArgumentException`/`UNAUTHENTICATED` client reconstruction, the cosmetic trailer-less dup
message, the fixed test port, and SEC-6 message sanitization are documented follow-ups.

## Final state
`max-cycles-reached` (4 cycles). claude converged to a non-blocking review with the fix and tests intact;
`gemini-code-assist` did not post within the 15-min window on the final head. Merge remains with the
developer.
