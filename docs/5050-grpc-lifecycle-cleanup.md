# Issue #5050 - gRPC lifecycle & cleanup

## Symptom
Lower-severity lifecycle bugs and housekeeping across the gRPC modules:
- **CON-2** (Medium): in `grpc.mode=both`, `GrpcServerPlugin.configureServer` runs twice (once per server builder), each constructing a new `ArcadeDbGrpcService` and overwriting `this.grpcService`/`healthManager`. `stopService` closes only the last instance, so the first service's `txReaper` daemon thread and its transaction registry leak forever.
- **CON-6** (Low): `stopService` is not idempotent/concurrent-safe. The JVM shutdown hook and the plugin-lifecycle stop can run concurrently with no guard; lifecycle fields are non-volatile and also read by `getStatus()`.
- **H1** (Low): a leftover `System.out.print(...)` in `ProtoUtils.toProtoRecord` prints (and leaks) every property of every record on the client write path.
- **DOC-1** (Low): the proto `InsertOptions.TransactionMode` doc comments for `PER_STREAM` and `PER_ROW` are inverted.
- **CODE-1** (Low): `RemoteGrpcDatabase.acquireLock()` resolved the name `RemoteGrpcTransactionExplicitLock` to a private nested **stub** (no `bucket`/`type`/`lock` overrides), shadowing the fully-implemented top-level class of the same name. The top-level class is exercised by the existing `RemoteGrpcTransactionExplicitLockIT`, so it is not dead - the nested stub is the redundant one.

## Root cause
- CON-2: service construction lives inside `configureServer`, which is invoked per server builder; "both" mode calls it twice with no reuse guard.
- CON-6: no stop guard; fields not volatile.
- H1/DOC-1/CODE-1: leftover debug print / stale comments / dead file.

## Fix
- `GrpcServerPlugin`: build **one** `ArcadeDbGrpcService` and one `HealthStatusManager`, reused across both server builders (null-guard in `configureServer`); make lifecycle fields `volatile`; guard `stopService` with an `AtomicBoolean stopped` so it is idempotent under concurrent invocation.
- `ProtoUtils`: delete the `System.out.print` line.
- `arcadedb-server.proto`: correct the `PER_STREAM`/`PER_ROW`/`PER_BATCH` comments to match actual commit behavior.
- Remove the redundant private nested `RemoteGrpcTransactionExplicitLock` stub in `RemoteGrpcDatabase` so `acquireLock()` resolves to the real top-level implementation (the one already covered by `RemoteGrpcTransactionExplicitLockIT`). The issue proposed deleting the top-level file, but that file is used by an existing test and carries the actual LOCK-command logic, so the correct resolution is "make the code use it".

## Tests
- `Issue5050GrpcPluginLifecycleTest` (grpcw): "both"-mode configuration reuses one service instance (no reaper leak); `stopService` idempotent under repeated invocation.
- `Issue5050ProtoUtilsNoStdoutTest` (grpc-client): `toProtoRecord` writes nothing to stdout.

## Impact
Removes a per-restart daemon-thread + transaction-registry leak in "both" mode, a stdout data leak on the client hot path, a shutdown-window race, misleading proto docs, and a latent no-op explicit-lock. Note CODE-1 was also a latent correctness bug (the nested stub made `acquireLock()` return a no-op lock), not merely cleanup. No API/behavior change for callers.

## PR
https://github.com/ArcadeData/arcadedb/pull/5197

## Review cycles
- **Cycle 1** (`472d49a0`): gemini (medium) - make `configureServer` synchronized; claude (LGTM, 3 non-blocking) - single-use `stopped` guard, hoist stdout assert out of tx, healthManager-reuse test gap. Applied: synchronized `configureServer`, single-use lifecycle comment, test hoist. Skipped w/ rationale: stopService "at most once" note, healthManager-getter test (avoid widening API).
- **Cycle 2** (`23f8b7d8`): claude re-review - verified all fixes correct (LGTM); asked to drop the internal `review-deferred-*.md` scratch notes from the tree (also matches repo convention). Applied: removed the scratch file.
- **Cycle 3** (`ca50f229`): claude re-review - LGTM; nits: FQN in test (`Type`), `shutdownHook` volatile asymmetry, add cross-builder reuse comment. Applied: imported `Type`, made `shutdownHook` volatile, documented the shared-service invariant.
- **Cycle 4** (`65fd8352`): final polish pushed; no confirming re-review arrived within the 15-min window (the bots did not re-trigger for the trivial style/comment push). Max cycles reached.

## Final state
max-cycles-reached (converged: claude's last full review was an explicit LGTM and every actionable item is resolved). Open item for the developer at merge: the `Co-Authored-By: Claude` commit trailers - claude flagged them against CLAUDE.md, but they cannot be removed without rewriting pushed history (forbidden by the skill constraints), so strip them at squash-merge if desired.
