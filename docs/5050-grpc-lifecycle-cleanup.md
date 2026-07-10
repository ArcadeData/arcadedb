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
Removes a per-restart daemon-thread + transaction-registry leak in "both" mode, a stdout data leak on the client hot path, a shutdown-window race, misleading proto docs, and a dead maintenance trap. No API/behavior change for callers.
