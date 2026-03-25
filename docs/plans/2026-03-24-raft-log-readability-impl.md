# Raft Log Readability — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Suppress noisy Ratis logs and add clean, human-readable cluster event logging with server names.

**Design doc:** `docs/plans/2026-03-24-raft-log-readability.md`

---

## Task 1: Add Peer Display Name Map to RaftHAServer

Add a `Map<RaftPeerId, String>` that maps each peer to a display name in the format `"serverName (host:httpPort)"`.

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

**Step 1: Add field**

Add a new field alongside `httpAddresses`:

```java
private final Map<RaftPeerId, String> peerDisplayNames;
```

**Step 2: Build the map in the constructor**

After `this.localPeerId = findLocalPeerId(...)`, build the display name map. The server name pattern is `{prefix}_{index}` (e.g., `ArcadeDB_0`). Extract the prefix from the local server name, then for each peer, reconstruct the name:

```java
final String prefix = serverName.substring(0, serverName.lastIndexOf('_'));
final Map<RaftPeerId, String> displayNames = new HashMap<>(peers.size());
for (int i = 0; i < peers.size(); i++) {
  final RaftPeerId peerId = peers.get(i).getId();
  final String nodeName = prefix + "_" + i;
  final String httpAddr = this.httpAddresses.get(peerId);
  displayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
}
this.peerDisplayNames = Collections.unmodifiableMap(displayNames);
```

**Step 3: Add accessor method**

```java
public String getPeerDisplayName(final RaftPeerId peerId) {
  final String name = peerDisplayNames.get(peerId);
  return name != null ? name : peerId.toString();
}
```

**Step 4: Enhance the startup log message**

Change the existing INFO log in `start()` from:
```
RaftHAServer started: peerId='peer-0'
```
To include all node names:
```
Raft cluster joined: 3 nodes [ArcadeDB_0 (localhost:2480), ArcadeDB_1 (localhost:2481), ArcadeDB_2 (localhost:2482)]
```

After the `raftServer` is started, replace or add:
```java
LogManager.instance().log(this, Level.INFO, "Raft cluster joined: %d nodes %s", peerDisplayNames.size(), peerDisplayNames.values());
```

**Step 5: Add unit test**

In `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerTest.java`, add a test:
- Call `parsePeerList("localhost:2434:2480,localhost:2435:2481", 2434)` to get peers
- Build a display name map the same way
- Assert format is correct
- Test `getPeerDisplayName()` with known and unknown peer IDs

**Verify:** `mvn test -pl ha-raft -Dtest="RaftHAServerTest" -DskipITs`

---

## Task 2: Suppress Ratis Internal Loggers

Set all `org.apache.ratis.*` JUL loggers to WARNING before starting the Raft server.

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

**Step 1: Add suppression in `start()` method**

At the beginning of `start()`, before any Ratis operations:

```java
// Suppress verbose Ratis internal logs — operators see ArcadeDB-level cluster events instead
java.util.logging.Logger.getLogger("org.apache.ratis").setLevel(java.util.logging.Level.WARNING);
```

**Step 2: No test needed** — this is a log level configuration, verified by running integration tests and observing output.

**Verify:** Run any existing integration test (e.g., `RaftReplication2NodesIT`) and confirm Ratis election chatter is gone.

---

## Task 3: Override `notifyLeaderChanged()` in ArcadeStateMachine

Add the leadership change callback that logs clean cluster event messages.

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

**Step 1: Add import**

```java
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
```

**Step 2: Override `notifyLeaderChanged()`**

Add after `takeSnapshot()`:

```java
@Override
public void notifyLeaderChanged(final RaftGroupMemberId groupMemberId, final RaftPeerId newLeaderId) {
  super.notifyLeaderChanged(groupMemberId, newLeaderId);

  if (raftHAServer == null || newLeaderId == null)
    return;

  final String leaderName = raftHAServer.getPeerDisplayName(newLeaderId);
  LogManager.instance().log(this, Level.INFO, "Leader elected: %s", leaderName);

  if (newLeaderId.equals(raftHAServer.getLocalPeerId()))
    LogManager.instance().log(this, Level.INFO, "This node is now LEADER");
  else
    LogManager.instance().log(this, Level.INFO, "This node is now REPLICA (leader: %s)", leaderName);
}
```

**Step 3: Add unit test**

In `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java`, add a test that:
- Creates an `ArcadeStateMachine`
- Mocks `RaftHAServer` with `getPeerDisplayName()` returning `"ArcadeDB_0 (localhost:2480)"` and `getLocalPeerId()` returning the local peer ID
- Sets the mock via `setRaftHAServer()`
- Calls `notifyLeaderChanged()` with the local peer ID → verify no exception
- Calls `notifyLeaderChanged()` with a different peer ID → verify no exception
- (Log output is not asserted directly — correctness verified by integration tests)

**Verify:** `mvn test -pl ha-raft -Dtest="ArcadeStateMachineTest" -DskipITs`

---

## Task 4: Wire Replica Lag Monitoring on Leader

When this node becomes leader, start a scheduled task that queries follower commit info and feeds it to `ClusterMonitor`. Cancel when leadership is lost.

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

**Step 1: Add scheduler to RaftHAServer**

Add field:
```java
private ScheduledExecutorService lagMonitorExecutor;
```

Add methods:

```java
void startLagMonitor() {
  if (lagMonitorExecutor != null)
    return;
  lagMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
    final Thread t = new Thread(r, "arcadedb-raft-lag-monitor");
    t.setDaemon(true);
    return t;
  });
  lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag, 5, 5, TimeUnit.SECONDS);
}

void stopLagMonitor() {
  if (lagMonitorExecutor != null) {
    lagMonitorExecutor.shutdownNow();
    lagMonitorExecutor = null;
  }
}

private void checkReplicaLag() {
  try {
    final var division = raftServer.getDivision(raftGroup.getGroupId());
    final var info = division.getInfo();
    if (!info.isLeader())
      return;
    final long commitIndex = info.getLastAppliedIndex();
    clusterMonitor.updateLeaderCommitIndex(commitIndex);
    // Note: per-follower match indexes are not directly exposed by Ratis public API.
    // Future enhancement: use Ratis admin API or metrics to get per-follower progress.
  } catch (final Exception e) {
    LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
  }
}
```

**Step 2: Call from ArcadeStateMachine.notifyLeaderChanged()**

In the `notifyLeaderChanged()` override, after logging:

```java
if (newLeaderId.equals(raftHAServer.getLocalPeerId()))
  raftHAServer.startLagMonitor();
else
  raftHAServer.stopLagMonitor();
```

**Step 3: Stop in RaftHAServer.stop()**

Add `stopLagMonitor()` at the beginning of `stop()`.

**Step 4: Update ClusterMonitor log to use display names**

Modify `ClusterMonitor.updateReplicaMatchIndex()` — the replica ID passed in should already be the display name. No changes to ClusterMonitor itself, but callers should pass display names.

**Verify:** `mvn test -pl ha-raft -DskipITs`

---

## Task 5: Run Integration Tests and Verify Output

Run the full integration test suite for ha-raft and verify:
1. No Ratis election chatter in logs
2. Clean "Leader elected" and "This node is now LEADER/REPLICA" messages appear
3. "Raft cluster joined" message appears at startup
4. All existing tests still pass

**Commands:**
```bash
mvn test -pl ha-raft -DskipITs
mvn test -pl ha-raft -Dtest="RaftReplication2NodesIT"
mvn test -pl ha-raft -Dtest="RaftReplication3NodesIT"
```

Fix any issues found.
