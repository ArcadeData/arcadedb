# Fix #4550: RemoteHttpComponent.getLeaderAddress NPE when no leader

## Summary

`RemoteHttpComponent.getLeaderAddress()` dereferences `leaderServer` at line 411-412
without a null guard. `leaderServer` is `volatile` and is set to `null` by
`reloadClusterConfiguration()` (line 550) during HA failover, and starts as `null`
when `requestClusterConfiguration()` cannot populate it (e.g. non-HA server or
network failure that triggers the fallback path at lines 459/466/511).

## Root Cause

```java
public String getLeaderAddress() {
  return leaderServer.getFirst() + ":" + leaderServer.getSecond(); // NPE when null
}
```

## Affected Components

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java` (primary fix)
- `gremlin/src/main/java/com/arcadedb/gremlin/ArcadeGraph.java` (caller hardening)

## Fix

1. `getLeaderAddress()` - snapshot the volatile field once, return `null` if null.
2. `ArcadeGraph.traversal()` - skip null leader address, still try replicas.

## Test

`RemoteHttpComponentTest` - two new unit tests using `TestableRemoteHttpComponent`
(which keeps `requestClusterConfiguration()` as a no-op so `leaderServer` starts null).

## Test Results

- `network` module: 52 tests, 0 failures (2 new regression tests added)
- `gremlin` module: 230 tests, 0 failures, 42 skipped

## Status

- [x] Analysis complete
- [x] Tests written
- [x] Fix implemented
- [x] Build verified
- [x] Tests passing

## Review Cycles

None yet.
