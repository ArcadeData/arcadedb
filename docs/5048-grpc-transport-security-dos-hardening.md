# #5048 - gRPC transport security & DoS hardening

## Symptom
gRPC modules had five transport-security / resource-limit weaknesses (audit 2026-07):
- SEC-3: TLS failed open - `grpc.tls.enabled=true` with a missing cert/key silently downgraded to cleartext.
- SEC-4: XDS server was hardcoded to `InsecureServerCredentials`, ignoring `grpc.tls.*`.
- SEC-5: the client attached username/password call metadata even on a plaintext channel (cleartext creds).
- SEC-7: one dedicated thread per open transaction, no cap - `beginTransaction` loop exhausts threads/memory.
- SEC-8: hardcoded `maxInboundMessageSize(256MB)` / `maxInboundMetadataSize(32MB)` overrode the smaller configurable limit.

## Root cause
Insecure fallbacks and hardcoded limits in `GrpcServerPlugin`, `ArcadeDbGrpcService` and `RemoteGrpcServer`.

## Fix
- `GrpcServerPlugin.configureStandardTls` / `configureTlsCredentials` now throw `SecurityException` instead of
  falling back to plaintext (SEC-3). `startXdsServer` derives credentials from `grpc.tls.*` (SEC-4).
- Inbound message size is left to the configured `grpc.maxMessageSize`; the hardcoded 256MB override is removed and
  metadata is capped at 16 KiB (SEC-8).
- `RemoteGrpcServer` refuses to attach credentials on a plaintext channel unless the new
  `allowInsecureCredentials` opt-in is set. Pre-existing constructors keep the historical permissive behavior for
  backward compatibility; the new 8-arg constructor lets callers opt into fail-closed (SEC-5).
- `ArcadeDbGrpcService` caps concurrently open transactions globally and per principal
  (`grpc.maxConcurrentTransactions` / `grpc.maxConcurrentTransactionsPerPrincipal`, default 1000); excess
  `beginTransaction` calls get `RESOURCE_EXHAUSTED`. Slots are released on commit/rollback/reap (SEC-7).

## Tests
- `GrpcServerTlsFailClosedTest` (SEC-3/SEC-4): standard, XDS and both modes refuse to start on TLS misconfig.
- `RemoteGrpcCredentialSecurityTest` (SEC-5): plaintext refuses creds unless opted in; secure always allows.
- `GrpcMaxConcurrentTransactionsIT` (SEC-7): 3rd concurrent begin rejected with RESOURCE_EXHAUSTED; slot freed on rollback.
- `GrpcInboundMessageSizeIT` (SEC-8): oversized request rejected under a small configured cap; small request accepted.
- Regression: existing reaper/lifecycle ITs and client tx ITs remain green.

## Impact
Fail-closed transport security for gRPC; bounded per-transaction resource usage. Client credential behavior is
backward compatible via existing constructors; secure-by-default is available on the new constructor, and a
warn-once log fires when credentials are attached over a plaintext channel.

## PR
https://github.com/ArcadeData/arcadedb/pull/5103

## Review cycles
- cycle 1 (`0e8109a`): both bots reviewed the initial commit. Addressed: per-principal slot double-release/leak on
  failure after registration; per-principal counter negative drift when the cap is disabled; int overflow of the
  now-effective inbound message size. Added `GrpcTransactionSlotReleaseIT`.
- cycle 2 (`3bbda4c`): both bots reviewed. Addressed: warn-once when attaching credentials over plaintext (SEC-5
  real-world effect); configurable `grpc.maxMetadataSize` (default 16 KiB); ownership-gated slot release to close a
  theoretical reaper race; partial-start teardown on mid-startup failure; `@Tag("slow")` on the two slow ITs;
  documented intentional non-pruning of `perPrincipalTxCount`.
- cycle 3 (`878b5db`): Gemini re-reviewed with only stale items (its naive `remove(txId)` suggestion is superseded by
  the safer ownership-gated `remove(txId, txCtx)` already applied) and one out-of-scope note on `BootstrapElection`.
  Claude did not re-review within the 15-minute budget.

## Deferred / not actioned (with rationale)
- `BootstrapElection` `System.nanoTime()` and callback-blocking notes: OUT OF SCOPE. Those `ha-raft` files come from
  a pre-existing local commit (6839cf94b) that was on local `main` but not yet pushed to `origin/main`, so this
  branch inherited it. Not part of this issue's change; left for the developer.
- Flipping the client credential default to fail-closed: would break the ~150 existing gRPC ITs that construct the
  client with `plaintext=true`; instead the opt-in default is preserved and a warn-once log added.

## Final state
timeout (Claude did not re-review the final SHA within the per-iteration budget; all actionable feedback resolved).
