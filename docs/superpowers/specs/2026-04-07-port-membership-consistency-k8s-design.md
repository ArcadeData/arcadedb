# Port Dynamic Membership, K8s Auto-Join, and Read Consistency from apache-ratis

**Date:** 2026-04-07
**Branch:** ha-redesign
**Source:** apache-ratis branch

## Overview

Port three features from the `apache-ratis` branch to `ha-redesign`, adapted to the plugin architecture:

1. **Dynamic Membership API** - runtime add/remove peers, leadership transfer, graceful leave
2. **K8s Auto-Join** - automatic cluster discovery and join for StatefulSet scaling
3. **Wait-for-Apply Notification** - read consistency (EVENTUAL / READ_YOUR_WRITES / LINEARIZABLE)

## Approach

Faithful port of `apache-ratis` semantics, adapted to `ha-redesign`'s plugin architecture. HTTP endpoints registered via `RaftHAPlugin.registerAPI()` as individual handler classes (matching the existing `GetClusterHandler` pattern).

---

## 1. Dynamic Membership API

### 1.1 New Methods on RaftHAServer

**`addPeer(String peerId, String address)`**
- Builds updated peer list via `getLivePeers()` + new peer
- Calls `raftClient.admin().setConfiguration(newPeers)`
- Derives HTTP address from Raft address (host:raftPort -> host:httpPort) and caches in `peerHttpAddresses`
- Throws `ConfigurationException` on failure

**`removePeer(String peerId)`**
- Filters target peer from `getLivePeers()` list
- Validates peer exists (throws `ConfigurationException` if not found)
- Calls `raftClient.admin().setConfiguration(reducedList)`

**`transferLeadership(String targetPeerId, long timeoutMs)`**
- Calls `raftClient.admin().transferLeadership(RaftPeerId.valueOf(targetPeerId), timeoutMs)`
- Throws `ConfigurationException` on failure
- Note: the existing `transferLeadership(long)` method (used for crash recovery channel refresh) remains as-is; this is a new overload

**`stepDown()`**
- Iterates `getLivePeers()`, transfers leadership to first non-leader peer
- Logs failure if no peer available (does not throw)

**`leaveCluster()`**
- Skips if single-node cluster
- If leader: transfers leadership to another peer, waits up to 5s via `leaderChangeNotifier` for confirmation
- Calls `removePeer(localPeerId.toString())` to remove self
- Best-effort: errors logged but don't prevent shutdown

**`getLivePeers()`**
- Reads committed configuration from `raftServer.getDivision(groupId).getRaftConf().getCurrentPeers()`
- Falls back to static `raftGroup.getPeers()` on error

### 1.2 Leader Change Notifier

New field on `RaftHAServer`:
```java
private final Object leaderChangeNotifier = new Object();
```

`ArcadeStateMachine.notifyLeaderChanged()` calls:
```java
synchronized (leaderChangeNotifier) { leaderChangeNotifier.notifyAll(); }
```

This allows `leaveCluster()` to wait for leadership transfer without polling.

### 1.3 HTTP Handlers

All registered in `RaftHAPlugin.registerAPI()`. Each follows the `GetClusterHandler` pattern.

| Handler Class | Method | Path | Request | Response |
|---|---|---|---|---|
| `PostAddPeerHandler` | POST | `/api/v1/cluster/peer` | `{"peerId":"...","address":"..."}` | `{"result":"Peer X added"}` |
| `DeletePeerHandler` | DELETE | `/api/v1/cluster/peer/{peerId}` | - | `{"result":"Peer X removed"}` |
| `PostTransferLeaderHandler` | POST | `/api/v1/cluster/leader` | `{"peerId":"...","timeoutMs":30000}` | `{"result":"Leadership transferred to X"}` |
| `PostStepDownHandler` | POST | `/api/v1/cluster/stepdown` | - | `{"result":"Leadership step-down initiated"}` |
| `PostLeaveHandler` | POST | `/api/v1/cluster/leave` | - | `{"result":"Server X leaving cluster"}` |
| `PostVerifyDatabaseHandler` | POST | `/api/v1/cluster/verify/{database}` | - | JSON report with per-node checksums |

Error responses: 400 if HA not enabled, 400 if missing required params, 404 if peer not found.

### 1.4 verifyDatabase

Ported from `apache-ratis`'s `PostServerCommandHandler.haVerifyDatabase()`:
- Computes local CRC32 checksums via `SnapshotManager.computeFileChecksums()`
- Queries each peer's checksums via HTTP (`GET /api/v1/ha/snapshot/{database}/checksums` - new endpoint on `SnapshotHttpHandler`)
- Compares and returns JSON report of matches/mismatches per file per node

### 1.5 HAServerPlugin Interface Changes

Add default methods to `HAServerPlugin` so the server module can optionally interact with membership without a hard dependency:

```java
default void addPeer(String peerId, String address) { throw new UnsupportedOperationException(); }
default void removePeer(String peerId) { throw new UnsupportedOperationException(); }
default void transferLeadership(String targetPeerId, long timeoutMs) { throw new UnsupportedOperationException(); }
default void stepDown() { throw new UnsupportedOperationException(); }
default void leaveCluster() { throw new UnsupportedOperationException(); }
```

---

## 2. K8s Auto-Join

### 2.1 Startup Flow

In `RaftHAServer.start()`, after the Raft server is started but before the group committer begins:

1. Check `HA_K8S` config is `true`
2. Check no existing Raft storage exists (fresh pod, not a restart)
3. Call `tryAutoJoinCluster()`

Storage detection: check for `<serverRootPath>/replication/<peerId>/` directory with metadata file.

### 2.2 tryAutoJoinCluster()

1. **Jitter** - Random delay `Math.abs(localPeerId.hashCode() % 3000L)` ms to prevent thundering herd when multiple pods start simultaneously (e.g., K8s Parallel pod management policy)

2. **Probe peers** - For each peer in `raftGroup` (except self):
   - Create temporary `RaftClient` with short timeouts (3s rpc min, 5s rpc max, 5s request)
   - Build single-peer `RaftGroup` targeting just that peer
   - Call `tempClient.getGroupManagementApi(peerId).info(groupId)`
   - If cluster found:
     - Check if already a member via `conf.getPeersList()`
     - If not member: build new peer list (existing + self), call `tempClient.admin().setConfiguration(newPeers)`
     - If already member: log and return
   - Close temporary client (try-with-resources)

3. **Fallback** - If no peer reachable, log warning about potential split-brain, start as new cluster

### 2.3 Shutdown Flow

In `RaftHAPlugin.stopService()`, before stopping the Raft server:
- If `HA_K8S` is `true`, call `raftHAServer.leaveCluster()`
- Best-effort: errors logged but don't block shutdown

### 2.4 Configuration

Already exists on `ha-redesign`:
- `HA_K8S` (boolean, default `false`)
- `HA_K8S_DNS_SUFFIX` (string, for peer address resolution)

---

## 3. Wait-for-Apply Notification

### 3.1 Monitor Pattern on RaftHAServer

New field:
```java
private final Object applyNotifier = new Object();
```

New methods:

**`notifyApplied()`**
- Called by `ArcadeStateMachine.applyTransaction()` after each successful apply
- `synchronized (applyNotifier) { applyNotifier.notifyAll(); }`

**`waitForAppliedIndex(long targetIndex)`**
- Returns immediately if `targetIndex <= 0`
- Deadline = `System.currentTimeMillis() + quorumTimeout`
- Loop: `synchronized (applyNotifier) { while (getLastAppliedIndex() < targetIndex) { wait(remaining); } }`
- On timeout: logs warning, returns (degrades to EVENTUAL)
- On interrupt: restores interrupt flag, returns

**`waitForLocalApply()`**
- Gets current commit index from Raft via `getCommitIndex()`
- Uses same monitor pattern to wait for `getLastAppliedIndex() >= commitIndex`
- On timeout or error: logs and returns

**`getLastAppliedIndex()`**
- Returns `raftServer.getDivision(groupId).getInfo().getLastAppliedIndex()`
- Returns -1 on error

**`getCommitIndex()`**
- Returns `raftServer.getDivision(groupId).getInfo().getCommitIndex()`
- Returns -1 on error

### 3.2 State Machine Integration

At the end of `ArcadeStateMachine.applyTransaction()`, after updating `lastAppliedIndex`:
```java
final var raftHA = getRaftHAServer();
if (raftHA != null)
    raftHA.notifyApplied();
```

### 3.3 Read Consistency in RaftReplicatedDatabase

**New enum** in `com.arcadedb.database.Database`:
```java
enum READ_CONSISTENCY { EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE }
```

**Thread-local context:**
```java
private static final ThreadLocal<ReadConsistencyContext> READ_CONSISTENCY_CONTEXT = new ThreadLocal<>();

record ReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {}

public static void setReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) { ... }
public static void clearReadConsistencyContext() { ... }
```

**Intercept query() methods** - All three `query()` overloads call `waitForReadConsistency()` before delegating to the proxied database:

```java
private void waitForReadConsistency() {
    if (isLeader()) return;
    // read ThreadLocal, dispatch to waitForAppliedIndex or waitForLocalApply based on level
}
```

- `EVENTUAL` - no-op
- `READ_YOUR_WRITES` - calls `raftHAServer.waitForAppliedIndex(ctx.readAfterIndex)` if bookmark >= 0
- `LINEARIZABLE` - uses bookmark if available, otherwise calls `raftHAServer.waitForLocalApply()`

### 3.4 HTTP Bookmark Integration

**Commit response:** Include `X-ArcadeDB-Commit-Index` header with the Raft commit index after a successful write. This is set in the HTTP handler layer after `commit()` returns.

**Read request:** The HTTP handler reads `X-ArcadeDB-Commit-Index` from the request headers (client sends back the bookmark from the last write). Combined with the `X-ArcadeDB-Read-Consistency` header (or the server default from `HA_READ_CONSISTENCY`), it calls `RaftReplicatedDatabase.setReadConsistencyContext()` before query execution, and `clearReadConsistencyContext()` in a finally block.

### 3.5 Configuration

New `GlobalConfiguration` entry:
```
HA_READ_CONSISTENCY("arcadedb.ha.readConsistency", SCOPE.SERVER,
    "Default read consistency for follower reads: eventual, read_your_writes, linearizable",
    String.class, "read_your_writes",
    Set.of("eventual", "read_your_writes", "linearizable"))
```

---

## 4. Testing Strategy

### 4.1 Dynamic Membership Tests

**Unit tests:**
- `DynamicMembershipTest` - `addPeer()`, `removePeer()`, `getLivePeers()` with mocked Ratis admin API
- `LeaveClusterTest` - Leadership transfer before self-removal, single-node skip, timeout handling

**Integration tests:**
- `RaftDynamicMembershipIT` - 3-node cluster: add 4th peer at runtime, verify replication, remove a peer, verify cluster continues
- `RaftTransferLeadershipIT` - Transfer leadership to specific peer, verify new leader serves writes
- `RaftStepDownIT` - Leader steps down, verify another peer takes over
- `RaftLeaveClusterIT` - Node leaves gracefully, verify cluster shrinks and continues
- `RaftVerifyDatabaseIT` - Write data, call verify endpoint, confirm matching checksums

### 4.2 K8s Auto-Join Tests

**Unit test:**
- `AutoJoinClusterTest` - Jitter calculation, probe logic with mocked peers, already-member detection

**Integration test:**
- `RaftK8sAutoJoinIT` - Start 2-node cluster, start 3rd with `HA_K8S=true`, verify auto-join and replication. Stop 3rd node, verify cluster shrinks to 2.

### 4.3 Wait-for-Apply Tests

**Unit tests:**
- `WaitForApplyTest` - `waitForAppliedIndex()` with mock: notify wakes waiter, timeout degrades
- `ReadConsistencyContextTest` - Thread-local set/get/clear lifecycle

**Integration tests:**
- `RaftReadConsistencyIT` - Three scenarios: EVENTUAL, READ_YOUR_WRITES with bookmark, LINEARIZABLE
- `RaftReadConsistencyBookmarkIT` - `X-ArcadeDB-Commit-Index` header round-trip

### 4.4 HTTP Handler Tests

- One IT per new endpoint: request/response format, error cases (HA not enabled, missing params, peer not found)

---

## 5. Files Changed Summary

### New files in `ha-raft/src/main/java/com/arcadedb/server/ha/raft/`
- `PostAddPeerHandler.java`
- `DeletePeerHandler.java`
- `PostTransferLeaderHandler.java`
- `PostStepDownHandler.java`
- `PostLeaveHandler.java`
- `PostVerifyDatabaseHandler.java`

### Modified files in `ha-raft/src/main/java/com/arcadedb/server/ha/raft/`
- `RaftHAServer.java` - addPeer, removePeer, transferLeadership, stepDown, leaveCluster, getLivePeers, tryAutoJoinCluster, applyNotifier, waitForAppliedIndex, waitForLocalApply, notifyApplied, getLastAppliedIndex, getCommitIndex, leaderChangeNotifier
- `ArcadeStateMachine.java` - notifyApplied() call in applyTransaction(), leaderChangeNotifier call in notifyLeaderChanged()
- `RaftReplicatedDatabase.java` - READ_CONSISTENCY_CONTEXT thread-local, waitForReadConsistency(), query() interception, setReadConsistencyContext/clearReadConsistencyContext
- `RaftHAPlugin.java` - Register new HTTP endpoints, K8s leaveCluster on shutdown
- `SnapshotHttpHandler.java` - Add `GET /api/v1/ha/snapshot/{database}/checksums` endpoint for verifyDatabase

### Modified files in `server/`
- `HAServerPlugin.java` - Add default membership methods
- HTTP handler layer - Set/clear read consistency context, emit X-ArcadeDB-Commit-Index header

### Modified files in `engine/`
- `GlobalConfiguration.java` - Add HA_READ_CONSISTENCY
- `Database.java` - Add READ_CONSISTENCY enum

### New test files in `ha-raft/src/test/java/`
- `DynamicMembershipTest.java`
- `LeaveClusterTest.java`
- `AutoJoinClusterTest.java`
- `WaitForApplyTest.java`
- `ReadConsistencyContextTest.java`
- `RaftDynamicMembershipIT.java`
- `RaftTransferLeadershipIT.java`
- `RaftStepDownIT.java`
- `RaftLeaveClusterIT.java`
- `RaftVerifyDatabaseIT.java`
- `RaftK8sAutoJoinIT.java`
- `RaftReadConsistencyIT.java` (extend or replace existing)
- `RaftReadConsistencyBookmarkIT.java`
- HTTP handler ITs (one per new endpoint)
