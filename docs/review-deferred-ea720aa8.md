# Review notes deferred from cycle 2 (base commit ea720aa8)

Both bots (`claude`, `coderabbitai`) reviewed commit `ea720aa8fe00f9530d80b06ad68218111be1ba43`. Two
actionable findings were applied (see the commit on top of this note); the following were assessed and
intentionally skipped, not deferred to the developer - both are non-blocking per the reviewing bot itself.

## Skipped: `GrpcServerPlugin.stopService()`'s `stopped` CAS never resets (claude, cycle 2)

Both `claude` and `coderabbitai`'s cycle-1/2 reviews independently raised the same observation: once a
failed `startService()` trips the `stopped` CAS via the new cleanup call, a hypothetical retried
`startService()` on the *same* plugin instance that later succeeds would leave a live `grpcServer`/
`xdsServer`/`grpcService` that a subsequent real `stopService()` call would silently no-op on.

**Why skipped:** both reviewers themselves call this "probably a non-issue in practice" / "worth a sanity
check, not a blocker." `GrpcServerPlugin` instances are constructed fresh per server start (via reflection
from the `SERVER_PLUGINS` config string, per `ArcadeDBServer`'s plugin loading) - there is no code path in
this codebase that retries `startService()` on an already-failed, already-constructed plugin instance.
Adding a `stopped` reset would be speculative generality for a scenario the codebase does not exercise.
Worth revisiting only if a "retry gRPC startup without restarting the whole server" feature is ever added.

## Applied, not skipped

- `isIdleReaperShutdown()` now awaits real termination instead of just `isShutdown()` (coderabbitai, cycle 2).
- `Issue6756DoubleTerminateGuardTest`'s 9 cases now also `verify(resp).onCompleted()` (coderabbitai, cycle 2).
- `Issue6756ExecuteCommandMaxRowsTest.resultExactlyAtMaxRowsSucceedsWithoutThrowing` added (claude, cycle 2).
