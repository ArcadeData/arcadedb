# Issue #4372 - RemoteHttpComponent leaderServer null-read in retry path

## Problem

`RemoteHttpComponent.httpCommand` reads `leaderServer` twice:

1. **Line ~237** (pre-loop): `connectToServer = leaderIsPreferable && leaderServer != null ? leaderServer : new Pair<>(currentServer, currentPort);`
   - Already null-safe (explicit null check).

2. **Line ~349** (retry catch block): `connectToServer = leaderServer;`
   - **NOT null-safe.** If `leaderServer` is null here, `connectToServer = null`, the for-loop
     exits prematurely and the request fails with "no server available".

Between the two reads, `reloadClusterConfiguration()` sets `leaderServer = null` temporarily
(to avoid looping back to the same leader) before calling `requestClusterConfiguration()` to
restore it. In multi-threaded usage, a concurrent call to `reloadClusterConfiguration()` from
another thread can leave `leaderServer = null` at the exact moment the second read runs.

## Root Cause

```java
// reloadClusterConfiguration() (line ~542):
leaderServer = null;  // temporarily null!
requestClusterConfiguration();  // should restore leaderServer
return leaderServer != null;  // true if restored

// httpCommand retry path (line ~349):
if (leaderIsPreferable && !currentConnectToServer.equals(leaderServer)) {
    connectToServer = leaderServer;  // BUG: can be null from concurrent thread
}
// connectToServer == null -> for-loop condition fails -> loop exits early
```

## Fix

Two changes to `RemoteHttpComponent.java`:

### 1. Volatile fields

Make `leaderServer`, `currentServer`, and `currentPort` `volatile` so writes from one thread
are immediately visible to readers in other threads:

```java
private volatile Pair<String, Integer> leaderServer;
protected volatile String currentServer;
protected volatile int currentPort;
```

### 2. Snapshot + null guard in httpCommand retry path

Snapshot `leaderServer` once after `reloadClusterConfiguration()` and add a null guard
before assigning to `connectToServer`:

```java
final Pair<String, Integer> currentConnectToServer = connectToServer;
final Pair<String, Integer> snapshotLeader = leaderServer;  // snapshot once

if (leaderIsPreferable && snapshotLeader != null && !currentConnectToServer.equals(snapshotLeader)) {
    connectToServer = snapshotLeader;
} else
    connectToServer = getNextReplicaAddress();
```

If `snapshotLeader` is null (the race occurred), the code falls through to
`getNextReplicaAddress()` which tries the next available replica instead of abandoning
the request.

## Test

`RemoteHttpComponentTest.httpCommandRetryWithReplicaWhenLeaderNulledConcurrently`:

- Creates a closed primary port (triggers IOException on iteration 0).
- Overrides `reloadClusterConfiguration()` to return `true` with `leaderServer = null`,
  simulating the race - but adds a working replica to `replicaServerList`.
- Calls `httpCommand(..., leaderIsPreferable=true, autoReconnect=true, ...)`.
- **Before fix:** loop exits with `connectToServer = null`, throws RemoteException.
- **After fix:** `getNextReplicaAddress()` returns the replica, retry succeeds, returns null
  (callback is null).

Requires `NETWORK_SAME_SERVER_ERROR_RETRIES = 2` so `maxRetry = 2` (default 0 → 1 skips retry).

## Files Changed

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java`
- `network/src/test/java/com/arcadedb/remote/RemoteHttpComponentTest.java`
