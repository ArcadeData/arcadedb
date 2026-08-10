# Issue #5847: sendWithWatchdog() treats the millisecond socket timeout as seconds

## Root cause

`RemoteHttpComponent.sendWithWatchdog()` computed the watchdog budget as
`Math.max(timeout * 1000L, 30_000L)`, but `timeout` (backed by
`GlobalConfiguration.NETWORK_SOCKET_TIMEOUT`) is already expressed in
milliseconds everywhere else in the class (e.g. `createRequestBuilder()` uses
`Duration.ofMillis(timeout)` directly). The `* 1000L` turned the default
30,000 ms timeout into an effective 30,000,000 ms (8h20m) watchdog instead of
30 seconds, defeating the purpose of the watchdog for the case it exists to
cover: a response that never completes after headers arrive (the ordinary
per-request timeout at `createRequestBuilder()` does not catch that case).

Secondary defect in the same method: on watchdog timeout, the
`CompletableFuture` returned by `httpClient.sendAsync()` was abandoned rather
than cancelled, so the in-flight HTTP exchange kept running and buffering its
response body in memory until it finished on its own. This also invalidated
the reasoning documented on `close()`, which assumes every in-flight request
is bounded by the per-request watchdog.

## Affected components

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java`

## Fix

- Extracted the watchdog-budget arithmetic into a package-private static
  `computeWatchdogMs(int timeoutMs)` so the millisecond arithmetic is directly
  unit-testable without waiting on real timers: `Math.max(timeoutMs, 30_000L)`
  (no `* 1000L`).
- Added a package-private `sendWithWatchdog(HttpRequest, long watchdogMs)`
  overload so tests can exercise the watchdog with a short, explicit budget
  instead of always waiting out the 30 second floor.
- On watchdog timeout, the future is now cancelled (`future.cancel(true)`)
  before the `IOException` is thrown, so the exchange is torn down instead of
  draining unbounded in the background.

## Tests

Added to `network/src/test/java/com/arcadedb/remote/RemoteHttpComponentTest.java`:

- `computeWatchdogMsTreatsTimeoutAsMilliseconds` — the default 30,000 ms
  timeout must produce a 30,000 ms watchdog, not 30,000,000 ms.
- `computeWatchdogMsPreservesLargerConfiguredTimeout` — a 120,000 ms timeout
  must produce a 120,000 ms watchdog, not ~1.4 days.
- `computeWatchdogMsAppliesThirtySecondFloor` — a configured timeout below the
  floor (or 0) still yields a usable 30 second watchdog.
- `sendWithWatchdogFiresWithinConfiguredWindowNotAThousandTimesLonger` —
  behavioral: a server that accepts the connection and never responds causes
  `sendWithWatchdog()` to throw within its configured (short, explicit)
  watchdog window rather than blocking far longer, and the exception message
  reports the millisecond value actually used.

## Verification

- `mvn -pl network -am test -Dtest=RemoteHttpComponentTest`
