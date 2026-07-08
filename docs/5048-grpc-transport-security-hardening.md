# 5048 - gRPC transport security & DoS hardening

## Symptom

The gRPC modules downgrade or leak transport security and allow resource-exhaustion DoS:

- **SEC-3** - `grpc.tls.enabled=true` with a missing/absent cert or key silently falls back to a plaintext server; operators believe TLS is on while credentials and data travel in cleartext.
- **SEC-4** - the XDS server is hardcoded to `InsecureServerCredentials.create()`, so `grpc.mode=xds`/`both` never uses TLS regardless of `grpc.tls.*`.
- **SEC-5** - the client attaches `username`/`password` call metadata (and body credentials) even on a `usePlaintext()` channel, sending the password in cleartext on every RPC.
- **SEC-7** - `beginTransaction` allocates a dedicated single-thread executor per transaction with no cap; an authenticated client can loop `beginTransaction` to exhaust threads/memory.
- **SEC-8** - `maxInboundMessageSize(256MB)` / `maxInboundMetadataSize(32MB)` override the smaller configurable `grpc.maxMessageSize` and invite memory-pressure DoS; the 32MB metadata cap is far larger than needed.

## Root cause

- SEC-3/SEC-4: TLS configuration failures return an insecure builder/credentials instead of failing closed.
- SEC-5: the client credential helpers unconditionally attach credentials without checking channel security.
- SEC-7: no per-principal or global limit on concurrently open transactions.
- SEC-8: hardcoded oversized inbound limits applied after the configurable value, and an oversized metadata cap.

## Fix

- **SEC-3** `GrpcServerPlugin.configureStandardTls` now validates the cert/key via `resolveTlsCertKey` and throws `SecurityException` (fail closed) instead of returning a plaintext builder.
- **SEC-4** `startXdsServer` derives credentials from `grpc.tls.*`: TLS enabled -> fail-closed `TlsServerCredentials`; TLS disabled -> explicit `InsecureServerCredentials` (documented as requiring mesh-level mTLS).
- **SEC-5** `RemoteGrpcServer` refuses to attach `CallCredentials` on a plaintext channel to a non-loopback host unless `allowInsecureCredentials` is set; loopback plaintext (no wire exposure) is still permitted so local usage is unaffected.
- **SEC-7** `ArcadeDbGrpcService` enforces `grpc.maxConcurrentTransactions` (global) and `grpc.maxConcurrentTransactionsPerPrincipal`; excess `beginTransaction` calls are rejected with `RESOURCE_EXHAUSTED`.
- **SEC-8** the hardcoded 256MB message override is removed so the configurable `grpc.maxMessageSize` wins; the inbound metadata cap is lowered to a configurable `grpc.maxMetadataSize` (default 16 KB).

## Tests

- `GrpcServerPluginTlsHardeningTest` - SEC-3/SEC-4 fail-closed startup on missing/absent cert (standard, xds, both modes) + SEC-8 config wiring.
- `RemoteGrpcServerInsecureCredentialsTest` - SEC-5 loopback allowed, remote plaintext refused, opt-in permits.
- `ArcadeDbGrpcServiceTransactionLimitTest` - SEC-7 global + per-principal caps and `RESOURCE_EXHAUSTED` rejection.

## Impact

gRPC modules only. TLS misconfiguration now prevents startup (both modes). Local plaintext clients are unaffected; remote plaintext clients must opt in to send credentials. Concurrent transactions are bounded. Inbound message size honors config; metadata cap lowered.

## Operator notes

- **Per-principal cap vs. single-user deployments.** `grpc.maxConcurrentTransactionsPerPrincipal` defaults to 100 while the global cap defaults to 1000. Deployments that connect every client as the same user (commonly `root`) attribute all transactions to one principal, so the per-principal cap is the effective limit and a high-concurrency connection pool holding more than 100 open transactions will receive `RESOURCE_EXHAUSTED` before reaching the global cap. The same applies to a **security-disabled server**, where every transaction maps to a single anonymous principal. Raise `grpc.maxConcurrentTransactionsPerPrincipal` (or set it to `0` to disable the per-principal bound) for such workloads.
- **Metadata cap behavioral change.** The inbound metadata cap drops from 32 MB to a 16 KB default (`grpc.maxMetadataSize`). Clients that send unusually large headers (big bearer/JWT tokens, tracing baggage) may be rejected; raise `grpc.maxMetadataSize` (KB) if needed.
- **Caps are per-listener in `both` mode.** `grpc.mode=both` starts a standard and an xds listener, each with its own service instance and transaction counters, so `grpc.maxConcurrentTransactions` is enforced per-listener rather than across both. Size the cap with that in mind when running `both`.
