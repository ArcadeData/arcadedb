# Port Dynamic Membership, K8s Auto-Join, and Read Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port dynamic membership API, K8s auto-join, and wait-for-apply read consistency from the `apache-ratis` branch to `ha-redesign`, adapted to the plugin architecture.

**Architecture:** All three features are implemented in the `ha-raft` plugin module. Dynamic membership methods live on `RaftHAServer`, exposed via individual HTTP handlers registered in `RaftHAPlugin.registerAPI()`. Read consistency uses a monitor pattern (`Object.wait/notifyAll`) between the state machine apply thread and query threads, with a `ThreadLocal` context set by the HTTP layer.

**Tech Stack:** Java 21, Apache Ratis 3.2.2, Undertow HTTP, JUnit 5, AssertJ

**Spec:** `docs/superpowers/specs/2026-04-07-port-membership-consistency-k8s-design.md`

---

## File Map

### New files

| File | Responsibility |
|------|---------------|
| `ha-raft/src/main/java/.../raft/PostAddPeerHandler.java` | HTTP POST `/api/v1/cluster/peer` - add a peer |
| `ha-raft/src/main/java/.../raft/DeletePeerHandler.java` | HTTP DELETE `/api/v1/cluster/peer/{id}` - remove a peer |
| `ha-raft/src/main/java/.../raft/PostTransferLeaderHandler.java` | HTTP POST `/api/v1/cluster/leader` - transfer leadership |
| `ha-raft/src/main/java/.../raft/PostStepDownHandler.java` | HTTP POST `/api/v1/cluster/stepdown` - leader steps down |
| `ha-raft/src/main/java/.../raft/PostLeaveHandler.java` | HTTP POST `/api/v1/cluster/leave` - graceful leave |
| `ha-raft/src/main/java/.../raft/PostVerifyDatabaseHandler.java` | HTTP POST `/api/v1/cluster/verify/{db}` - checksum verification |
| `ha-raft/src/test/java/.../raft/DynamicMembershipTest.java` | Unit tests for add/remove/getLivePeers |
| `ha-raft/src/test/java/.../raft/LeaveClusterTest.java` | Unit tests for leaveCluster logic |
| `ha-raft/src/test/java/.../raft/WaitForApplyTest.java` | Unit tests for wait-for-apply monitor |
| `ha-raft/src/test/java/.../raft/ReadConsistencyContextTest.java` | Unit tests for ThreadLocal context |
| `ha-raft/src/test/java/.../raft/RaftDynamicMembershipIT.java` | IT: add/remove peer at runtime |
| `ha-raft/src/test/java/.../raft/RaftTransferLeadershipIT.java` | IT: leadership transfer |
| `ha-raft/src/test/java/.../raft/RaftLeaveClusterIT.java` | IT: graceful leave |
| `ha-raft/src/test/java/.../raft/RaftVerifyDatabaseIT.java` | IT: checksum verification |
| `ha-raft/src/test/java/.../raft/RaftK8sAutoJoinIT.java` | IT: K8s auto-join and auto-leave |
| `ha-raft/src/test/java/.../raft/RaftReadConsistencyBookmarkIT.java` | IT: bookmark header round-trip |

### Modified files

| File | Changes |
|------|---------|
| `ha-raft/.../raft/RaftHAServer.java` | Add `getLivePeers()`, `addPeer()`, `removePeer()`, `transferLeadership(String,long)`, `stepDown()`, `leaveCluster()`, `tryAutoJoinCluster()`, `leaderChangeNotifier`, `applyNotifier`, `notifyApplied()`, `waitForAppliedIndex()`, `waitForLocalApply()`, `getLastAppliedIndex()`, `getCommitIndex()` |
| `ha-raft/.../raft/ArcadeStateMachine.java` | Call `raftHAServer.notifyApplied()` after apply, call `leaderChangeNotifier.notifyAll()` in `notifyLeaderChanged()` |
| `ha-raft/.../raft/RaftReplicatedDatabase.java` | Add `ReadConsistencyContext` ThreadLocal, `waitForReadConsistency()`, intercept `query()` methods, add `setReadConsistencyContext()`/`clearReadConsistencyContext()` |
| `ha-raft/.../raft/RaftHAPlugin.java` | Register new HTTP handlers, call `leaveCluster()` on K8s shutdown |
| `ha-raft/.../raft/SnapshotHttpHandler.java` | Add checksums sub-path for `verifyDatabase` |
| `server/.../HAServerPlugin.java` | Add default membership methods |
| `engine/.../database/Database.java` | Add `READ_CONSISTENCY` enum |
| `engine/.../GlobalConfiguration.java` | Add `HA_READ_CONSISTENCY` entry |

---

## Task 1: Add `READ_CONSISTENCY` enum and `HA_READ_CONSISTENCY` config

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/database/Database.java:41-44`
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java:549`

- [ ] **Step 1: Add `READ_CONSISTENCY` enum to `Database.java`**

In `engine/src/main/java/com/arcadedb/database/Database.java`, add the enum right after the existing `TRANSACTION_ISOLATION_LEVEL` enum (line 44):

```java
  enum READ_CONSISTENCY {
    EVENTUAL, READ_YOUR_WRITES, LINEARIZABLE
  }
```

- [ ] **Step 2: Add `HA_READ_CONSISTENCY` to `GlobalConfiguration.java`**

In `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`, add after the `HA_K8S_DNS_SUFFIX` entry (around line 549):

```java
  HA_READ_CONSISTENCY("arcadedb.ha.readConsistency", SCOPE.SERVER,
      "Default read consistency for follower reads: eventual, read_your_writes, linearizable",
      String.class, "read_your_writes",
      Set.of((Object[]) new String[] { "eventual", "read_your_writes", "linearizable" })),
```

- [ ] **Step 3: Build engine module to verify compilation**

Run: `cd engine && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/database/Database.java engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "feat(engine): add READ_CONSISTENCY enum and HA_READ_CONSISTENCY config"
```

---

## Task 2: Add default membership methods to `HAServerPlugin`

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/HAServerPlugin.java:82-83`

- [ ] **Step 1: Add default membership methods**

In `server/src/main/java/com/arcadedb/server/HAServerPlugin.java`, add before the closing brace (before line 84):

```java
  /**
   * Adds a new peer to the cluster at runtime.
   */
  default void addPeer(final String peerId, final String address) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Removes a peer from the cluster at runtime.
   */
  default void removePeer(final String peerId) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Transfers leadership to the specified peer.
   */
  default void transferLeadership(final String targetPeerId, final long timeoutMs) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Steps down from leadership, transferring to any available peer.
   */
  default void stepDown() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Gracefully leaves the cluster, transferring leadership first if this node is leader.
   */
  default void leaveCluster() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }
```

- [ ] **Step 2: Build server module**

Run: `cd server && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/HAServerPlugin.java
git commit -m "feat(server): add default dynamic membership methods to HAServerPlugin"
```

---

## Task 3: Add `getLivePeers()` and `leaderChangeNotifier` to `RaftHAServer`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/DynamicMembershipTest.java`

- [ ] **Step 1: Write failing test for `getLivePeers()`**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/DynamicMembershipTest.java`:

```java
package com.arcadedb.server.ha.raft;

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for dynamic membership operations on RaftHAServer.
 * Uses a real 2-node cluster to verify addPeer/removePeer/getLivePeers.
 */
class DynamicMembershipTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void getLivePeersReturnsAllConfiguredPeers() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    final Collection<RaftPeer> livePeers = raftServer.getLivePeers();
    assertThat(livePeers).hasSize(2);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ha-raft && mvn test -Dtest=DynamicMembershipTest#getLivePeersReturnsAllConfiguredPeers -pl . -q`
Expected: FAIL - method `getLivePeers()` does not exist

- [ ] **Step 3: Add `leaderChangeNotifier` field and `getLivePeers()` to `RaftHAServer`**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`, add the new field after the existing fields (after line 86):

```java
  private final Object leaderChangeNotifier = new Object();
```

Add the `getLivePeers()` method after `getLocalPeerId()` (after line 579):

```java
  /**
   * Returns the current live peers from the Raft server's committed configuration.
   * Unlike {@code raftGroup.getPeers()} which is static from construction time,
   * this reflects dynamic membership changes from addPeer/removePeer calls.
   */
  public Collection<RaftPeer> getLivePeers() {
    if (raftServer != null) {
      try {
        final var division = raftServer.getDivision(raftGroup.getGroupId());
        final var conf = division.getRaftConf();
        if (conf != null)
          return conf.getCurrentPeers();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.FINE, "Cannot read live peers from Raft server, using static list", e);
      }
    }
    return raftGroup.getPeers();
  }

  /**
   * Returns the leader change notifier object for wait/notify synchronization.
   * Used by leaveCluster() to wait for leadership transfer confirmation.
   */
  Object getLeaderChangeNotifier() {
    return leaderChangeNotifier;
  }
```

Add import for `Collection`:
```java
import java.util.Collection;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ha-raft && mvn test -Dtest=DynamicMembershipTest#getLivePeersReturnsAllConfiguredPeers -pl . -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/DynamicMembershipTest.java
git commit -m "feat(ha-raft): add getLivePeers() and leaderChangeNotifier to RaftHAServer"
```

---

## Task 4: Add `addPeer()` and `removePeer()` to `RaftHAServer`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/DynamicMembershipTest.java`

- [ ] **Step 1: Write failing test for addPeer/removePeer**

Add to `DynamicMembershipTest.java`:

```java
  @Test
  void addPeerIncreasesClusterSize() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    final int initialSize = raftServer.getLivePeers().size();

    // Add a fake peer (won't actually connect, but should succeed in the Raft config)
    raftServer.addPeer("peer-99", "localhost:19999");

    final Collection<RaftPeer> livePeers = raftServer.getLivePeers();
    assertThat(livePeers).hasSize(initialSize + 1);

    // Clean up: remove the fake peer
    raftServer.removePeer("peer-99");
    assertThat(raftServer.getLivePeers()).hasSize(initialSize);
  }

  @Test
  void removePeerThrowsForUnknownPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = getRaftPlugin(leaderIndex).getRaftHAServer();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> raftServer.removePeer("nonexistent"))
        .isInstanceOf(com.arcadedb.exception.ConfigurationException.class);
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ha-raft && mvn test -Dtest=DynamicMembershipTest -pl . -q`
Expected: FAIL - methods `addPeer()` and `removePeer()` do not exist

- [ ] **Step 3: Implement addPeer() and removePeer()**

Add to `RaftHAServer.java` after `getLivePeers()`:

```java
  /**
   * Adds a new peer to the Raft cluster at runtime.
   * Must be called on the leader.
   *
   * @param peerId  unique identifier for the new peer
   * @param address Raft gRPC address (host:port) of the new peer
   */
  public void addPeer(final String peerId, final String address) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    final List<RaftPeer> newPeers = new ArrayList<>(getLivePeers());
    newPeers.add(newPeer);

    try {
      final RaftClientReply reply = raftClient.admin().setConfiguration(newPeers);
      if (!reply.isSuccess())
        throw new com.arcadedb.exception.ConfigurationException(
            "Failed to add peer " + peerId + ": " + reply.getException());

      // Derive and cache HTTP address from the Raft address
      final int colonIdx = address.lastIndexOf(':');
      if (colonIdx > 0) {
        final String host = address.substring(0, colonIdx);
        try {
          final int raftPort = Integer.parseInt(address.substring(colonIdx + 1));
          final int httpPortOffset = getHttpPortOffset();
          httpAddresses.put(RaftPeerId.valueOf(peerId), host + ":" + (raftPort + httpPortOffset));
        } catch (final NumberFormatException ignored) {
          // Non-numeric port, skip HTTP address derivation
        }
      }

      LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster at %s", peerId, address);
    } catch (final IOException e) {
      throw new com.arcadedb.exception.ConfigurationException("Failed to add peer " + peerId, e);
    }
  }

  /**
   * Removes a peer from the Raft cluster at runtime.
   * Must be called on the leader.
   *
   * @param peerId identifier of the peer to remove
   */
  public void removePeer(final String peerId) {
    final Collection<RaftPeer> livePeers = getLivePeers();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == livePeers.size())
      throw new com.arcadedb.exception.ConfigurationException("Peer " + peerId + " not found in cluster");

    try {
      final RaftClientReply reply = raftClient.admin().setConfiguration(newPeers);
      if (!reply.isSuccess())
        throw new com.arcadedb.exception.ConfigurationException(
            "Failed to remove peer " + peerId + ": " + reply.getException());

      httpAddresses.remove(RaftPeerId.valueOf(peerId));
      LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
    } catch (final IOException e) {
      throw new com.arcadedb.exception.ConfigurationException("Failed to remove peer " + peerId, e);
    }
  }

  /**
   * Returns the offset between the Raft port and HTTP port.
   * Derived from the first peer that has both addresses configured.
   */
  private int getHttpPortOffset() {
    for (final RaftPeer peer : raftGroup.getPeers()) {
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr != null) {
        try {
          final int httpPort = Integer.parseInt(httpAddr.substring(httpAddr.lastIndexOf(':') + 1));
          final int raftPort = Integer.parseInt(
              peer.getAddress().toString().substring(peer.getAddress().toString().lastIndexOf(':') + 1));
          return httpPort - raftPort;
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return 46; // Default: 2480 - 2434
  }
```

Make the `httpAddresses` field mutable. Change the field declaration from:
```java
  private final Map<RaftPeerId, String>    httpAddresses;
```
to:
```java
  private final Map<RaftPeerId, String>    httpAddresses = new HashMap<>();
```

And in the constructor, change:
```java
    this.httpAddresses = parsed.httpAddresses();
```
to:
```java
    this.httpAddresses.putAll(parsed.httpAddresses());
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ha-raft && mvn test -Dtest=DynamicMembershipTest -pl . -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/DynamicMembershipTest.java
git commit -m "feat(ha-raft): add addPeer() and removePeer() for dynamic membership"
```

---

## Task 5: Add `transferLeadership(String, long)`, `stepDown()`, and `leaveCluster()`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/LeaveClusterTest.java`

- [ ] **Step 1: Write failing tests**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/LeaveClusterTest.java`:

```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for leadership transfer, step-down, and cluster leave operations.
 */
class LeaveClusterTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void transferLeadershipToSpecificPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final int targetIndex = leaderIndex == 0 ? 1 : 0;
    final String targetPeerId = "peer-" + targetIndex;

    leaderRaft.transferLeadership(targetPeerId, 10_000);

    // Wait briefly for the election to complete
    try { Thread.sleep(3_000); } catch (final InterruptedException ignored) {}

    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isEqualTo(targetIndex);
  }

  @Test
  void stepDownTransfersToAnotherPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    leaderRaft.stepDown();

    // Wait briefly for the election to complete
    try { Thread.sleep(3_000); } catch (final InterruptedException ignored) {}

    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isNotEqualTo(leaderIndex);
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ha-raft && mvn test -Dtest=LeaveClusterTest -pl . -q`
Expected: FAIL - methods do not exist

- [ ] **Step 3: Implement methods on RaftHAServer**

Add to `RaftHAServer.java` after `removePeer()`:

```java
  /**
   * Transfers leadership to a specific peer.
   *
   * @param targetPeerId the peer to transfer leadership to
   * @param timeoutMs    maximum time to wait for the transfer to complete
   */
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    try {
      final RaftClientReply reply = raftClient.admin().transferLeadership(
          RaftPeerId.valueOf(targetPeerId), timeoutMs);
      if (!reply.isSuccess())
        throw new com.arcadedb.exception.ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
    } catch (final IOException e) {
      throw new com.arcadedb.exception.ConfigurationException("Failed to transfer leadership to " + targetPeerId, e);
    }
  }

  /**
   * Steps down from leadership by transferring to any available peer.
   * Does not throw if no peer is available - just logs the failure.
   */
  public void stepDown() {
    final String leaderName = getLeaderName();
    for (final var peer : getLivePeers()) {
      if (!peer.getId().toString().equals(localPeerId.toString())) {
        try {
          transferLeadership(peer.getId().toString(), 10_000);
          return;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down (transfer to %s): %s", peer.getId(), e.getMessage());
        }
      }
    }
    LogManager.instance().log(this, Level.SEVERE,
        "Cannot step down: no other peer available for leadership transfer");
  }

  /**
   * Gracefully leaves the cluster. If this node is the leader, transfers
   * leadership first. Then removes self from the Raft configuration.
   * Best-effort: errors are logged but do not prevent shutdown.
   */
  public void leaveCluster() {
    if (raftServer == null || raftClient == null)
      return;

    try {
      final Collection<RaftPeer> livePeers = getLivePeers();
      if (livePeers.size() <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      // If we're the leader, transfer leadership first
      if (isLeader()) {
        for (final RaftPeer peer : livePeers) {
          if (!peer.getId().equals(localPeerId)) {
            HALog.log(this, HALog.BASIC,
                "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), 10_000);
              // Wait for leadership change notification
              final long deadline = System.currentTimeMillis() + 5_000;
              synchronized (leaderChangeNotifier) {
                while (isLeader()) {
                  final long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0)
                    break;
                  leaderChangeNotifier.wait(remaining);
                }
              }
            } catch (final Exception e) {
              HALog.log(this, HALog.BASIC,
                  "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
            }
            break;
          }
        }
      }

      // Remove self from the cluster configuration
      HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
      removePeer(localPeerId.toString());
      HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to leave cluster gracefully: %s", e.getMessage());
    }
  }
```

- [ ] **Step 4: Wire `leaderChangeNotifier` into `ArcadeStateMachine.notifyLeaderChanged()`**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`, in the `notifyLeaderChanged()` method, add after the `raftHAServer.stopLagMonitor()` call (after line 174):

```java
    // Wake up any threads waiting for leadership change (e.g. leaveCluster)
    final Object notifier = raftHAServer.getLeaderChangeNotifier();
    synchronized (notifier) {
      notifier.notifyAll();
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd ha-raft && mvn test -Dtest=LeaveClusterTest -pl . -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/LeaveClusterTest.java
git commit -m "feat(ha-raft): add transferLeadership, stepDown, leaveCluster with leader change notifier"
```

---

## Task 6: Wire `HAServerPlugin` membership methods in `RaftHAPlugin`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Implement HAServerPlugin membership methods in RaftHAPlugin**

Add to `RaftHAPlugin.java` before the `isRaftEnabled()` method:

```java
  @Override
  public void addPeer(final String peerId, final String address) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.addPeer(peerId, address);
  }

  @Override
  public void removePeer(final String peerId) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.removePeer(peerId);
  }

  @Override
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.transferLeadership(targetPeerId, timeoutMs);
  }

  @Override
  public void stepDown() {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.stepDown();
  }

  @Override
  public void leaveCluster() {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.leaveCluster();
  }
```

- [ ] **Step 2: Build to verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): wire HAServerPlugin membership methods in RaftHAPlugin"
```

---

## Task 7: Create HTTP handlers for membership operations

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/DeletePeerHandler.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostTransferLeaderHandler.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostStepDownHandler.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostLeaveHandler.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Create PostAddPeerHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostAddPeerHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostAddPeerHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String peerId = payload.optString("peerId", "");
    final String address = payload.optString("address", "");
    if (peerId.isEmpty() || address.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing required fields: peerId, address").toString());

    raftHAServer.addPeer(peerId, address);
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Peer " + peerId + " added").toString());
  }
}
```

- [ ] **Step 2: Create DeletePeerHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/DeletePeerHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class DeletePeerHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public DeletePeerHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    // Extract peerId from path: /api/v1/cluster/peer/{peerId}
    final String relativePath = exchange.getRelativePath();
    final String peerId = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    if (peerId.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing peerId in path").toString());

    raftHAServer.removePeer(peerId);
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Peer " + peerId + " removed").toString());
  }
}
```

- [ ] **Step 3: Create PostTransferLeaderHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostTransferLeaderHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostTransferLeaderHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostTransferLeaderHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String peerId = payload.optString("peerId", "");
    if (peerId.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing required field: peerId").toString());

    final long timeoutMs = payload.optLong("timeoutMs", 30_000);
    raftHAServer.transferLeadership(peerId, timeoutMs);
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Leadership transferred to " + peerId).toString());
  }
}
```

- [ ] **Step 4: Create PostStepDownHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostStepDownHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostStepDownHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostStepDownHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    raftHAServer.stepDown();
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Leadership step-down initiated").toString());
  }
}
```

- [ ] **Step 5: Create PostLeaveHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostLeaveHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostLeaveHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostLeaveHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String localPeerId = raftHAServer.getLocalPeerId().toString();
    raftHAServer.leaveCluster();
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Server " + localPeerId + " leaving cluster").toString());
  }
}
```

- [ ] **Step 6: Register handlers in RaftHAPlugin.registerAPI()**

In `RaftHAPlugin.java`, in the `registerAPI()` method, add after the snapshot endpoint registration:

```java
    routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/peer/", new DeletePeerHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leader", new PostTransferLeaderHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/stepdown", new PostStepDownHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leave", new PostLeaveHandler(httpServer, this));
    LogManager.instance().log(this, Level.INFO, "Raft cluster management endpoints registered");
```

- [ ] **Step 7: Build to verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostAddPeerHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/DeletePeerHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostTransferLeaderHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostStepDownHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostLeaveHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): add HTTP handlers for dynamic membership operations"
```

---

## Task 8: Add `PostVerifyDatabaseHandler` and checksums endpoint

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Add checksums sub-path to SnapshotHttpHandler**

In `SnapshotHttpHandler.java`, modify `handleRequest()` to detect a `/checksums` suffix on the path. After extracting `databaseName` (after line 75), add:

```java
    // Check for checksums sub-path: /api/v1/ha/snapshot/{database}/checksums
    if (databaseName.endsWith("/checksums")) {
      final String dbName = databaseName.substring(0, databaseName.length() - "/checksums".length());
      handleChecksums(exchange, dbName);
      return;
    }
```

Add the `handleChecksums` method:

```java
  private void handleChecksums(final HttpServerExchange exchange, final String databaseName) throws Exception {
    final var server = httpServer.getServer();
    if (!server.existsDatabase(databaseName)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database '" + databaseName + "' not found");
      return;
    }

    final com.arcadedb.database.DatabaseInternal db = server.getDatabase(databaseName);
    final java.io.File dbDir = new java.io.File(db.getDatabasePath());

    final java.util.Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dbDir);
    final com.arcadedb.serializer.json.JSONObject response = new com.arcadedb.serializer.json.JSONObject();
    for (final var entry : checksums.entrySet())
      response.put(entry.getKey(), entry.getValue());

    exchange.getResponseHeaders().put(io.undertow.util.Headers.CONTENT_TYPE, "application/json");
    exchange.getResponseSender().send(response.toString());
  }
```

- [ ] **Step 2: Create PostVerifyDatabaseHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PostVerifyDatabaseHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostVerifyDatabaseHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String relativePath = exchange.getRelativePath();
    final String databaseName = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    if (databaseName.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing database name in path").toString());

    if (!httpServer.getServer().existsDatabase(databaseName))
      return new ExecutionResponse(404,
          new JSONObject().put("error", "Database '" + databaseName + "' not found").toString());

    try {
      final var db = httpServer.getServer().getDatabase(databaseName);
      final File dbDir = new File(db.getDatabasePath());
      final Map<String, Long> localChecksums = SnapshotManager.computeFileChecksums(dbDir);

      final JSONObject result = new JSONObject();
      result.put("database", databaseName);
      result.put("localNode", raftHAServer.getLocalPeerId().toString());

      final com.arcadedb.serializer.json.JSONArray nodesResult = new com.arcadedb.serializer.json.JSONArray();

      final String clusterToken = httpServer.getServer().getConfiguration()
          .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);

      for (final RaftPeer peer : raftHAServer.getLivePeers()) {
        if (peer.getId().equals(raftHAServer.getLocalPeerId()))
          continue;

        final String httpAddr = raftHAServer.getPeerHttpAddress(peer.getId());
        if (httpAddr == null)
          continue;

        final JSONObject nodeResult = new JSONObject();
        nodeResult.put("peerId", peer.getId().toString());

        try {
          final String url = "http://" + httpAddr + "/api/v1/ha/snapshot/" + databaseName + "/checksums";
          final HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
          conn.setRequestMethod("GET");
          conn.setConnectTimeout(10_000);
          conn.setReadTimeout(30_000);
          if (clusterToken != null && !clusterToken.isEmpty())
            conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);

          if (conn.getResponseCode() == 200) {
            final String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            final JSONObject remoteChecksums = new JSONObject(body);

            boolean match = true;
            final com.arcadedb.serializer.json.JSONArray diffs = new com.arcadedb.serializer.json.JSONArray();
            for (final var entry : localChecksums.entrySet()) {
              if (!remoteChecksums.has(entry.getKey()) ||
                  remoteChecksums.getLong(entry.getKey()) != entry.getValue()) {
                match = false;
                diffs.put(entry.getKey());
              }
            }
            nodeResult.put("match", match);
            if (!match)
              nodeResult.put("differingFiles", diffs);
          } else {
            nodeResult.put("error", "HTTP " + conn.getResponseCode());
          }
          conn.disconnect();
        } catch (final Exception e) {
          nodeResult.put("error", e.getMessage());
        }

        nodesResult.put(nodeResult);
      }

      result.put("nodes", nodesResult);
      return new ExecutionResponse(200, result.toString());

    } catch (final Exception e) {
      return new ExecutionResponse(500,
          new JSONObject().put("error", "Verification failed: " + e.getMessage()).toString());
    }
  }
}
```

- [ ] **Step 3: Register in RaftHAPlugin.registerAPI()**

Add to `RaftHAPlugin.registerAPI()`:

```java
    routes.addPrefixPath("/api/v1/cluster/verify/", new PostVerifyDatabaseHandler(httpServer, this));
```

- [ ] **Step 4: Build to verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): add verifyDatabase endpoint with CRC32 checksum comparison"
```

---

## Task 9: Add wait-for-apply notification to `RaftHAServer`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/WaitForApplyTest.java`

- [ ] **Step 1: Write failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/WaitForApplyTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the wait-for-apply notification pattern.
 */
class WaitForApplyTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void getLastAppliedIndexIncrementsAfterWrite() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final long indexBefore = raft.getLastAppliedIndex();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ApplyTest"))
        leaderDb.getSchema().createVertexType("ApplyTest");
    });

    // After a write, the applied index should advance
    assertClusterConsistency();
    final long indexAfter = raft.getLastAppliedIndex();
    assertThat(indexAfter).isGreaterThan(indexBefore);
  }

  @Test
  void waitForAppliedIndexReturnsImmediatelyForPastIndex() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leaderIndex).getRaftHAServer();

    // Waiting for index 0 or -1 should return immediately
    final long start = System.currentTimeMillis();
    raft.waitForAppliedIndex(0);
    final long elapsed = System.currentTimeMillis() - start;
    assertThat(elapsed).isLessThan(1000);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ha-raft && mvn test -Dtest=WaitForApplyTest -pl . -q`
Expected: FAIL - methods do not exist

- [ ] **Step 3: Implement wait-for-apply methods on RaftHAServer**

Add new field after `leaderChangeNotifier`:

```java
  private final Object applyNotifier = new Object();
```

Add methods after `leaveCluster()`:

```java
  /**
   * Called by ArcadeStateMachine after applying a log entry to wake up waiters.
   */
  public void notifyApplied() {
    synchronized (applyNotifier) {
      applyNotifier.notifyAll();
    }
  }

  /**
   * Waits until the local state machine has applied at least the specified index.
   * Used for READ_YOUR_WRITES consistency: the client sends its last known commit index
   * and the follower waits until it has applied up to that point before executing a read.
   */
  public void waitForAppliedIndex(final long targetIndex) {
    if (targetIndex <= 0)
      return;
    try {
      final long deadline = System.currentTimeMillis() + quorumTimeout;
      synchronized (applyNotifier) {
        while (getLastAppliedIndex() < targetIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            LogManager.instance().log(this, Level.WARNING,
                "READ_YOUR_WRITES consistency timeout: applied=%d < target=%d (consistency degraded to EVENTUAL)",
                getLastAppliedIndex(), targetIndex);
            return;
          }
          applyNotifier.wait(remaining);
        }
      }
      HALog.log(this, HALog.TRACE, "Bookmark wait complete: applied >= target=%d", targetIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Waits until the local state machine has applied all committed entries.
   * Used for LINEARIZABLE reads on followers.
   */
  public void waitForLocalApply() {
    try {
      final long commitIndex = getCommitIndex();
      if (commitIndex <= 0)
        return;

      final long deadline = System.currentTimeMillis() + quorumTimeout;
      synchronized (applyNotifier) {
        while (getLastAppliedIndex() < commitIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            HALog.log(this, HALog.DETAILED, "waitForLocalApply timed out: applied=%d < commit=%d",
                getLastAppliedIndex(), commitIndex);
            return;
          }
          applyNotifier.wait(remaining);
        }
      }
      HALog.log(this, HALog.TRACE, "Local apply caught up: applied=%d >= commit=%d",
          getLastAppliedIndex(), commitIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      HALog.log(this, HALog.DETAILED, "waitForLocalApply failed: %s", e.getMessage());
    }
  }

  /**
   * Returns the last applied index from the Raft state machine, or -1 on error.
   */
  public long getLastAppliedIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLastAppliedIndex();
    } catch (final IOException e) {
      return -1;
    }
  }

  /**
   * Returns the commit index from the Raft state machine, or -1 on error.
   */
  public long getCommitIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getCommitIndex();
    } catch (final IOException e) {
      return -1;
    }
  }
```

- [ ] **Step 4: Wire `notifyApplied()` into `ArcadeStateMachine.applyTransaction()`**

In `ArcadeStateMachine.java`, in the `applyTransaction()` method, add after `updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());` (after line 119):

```java
      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      if (raftHAServer != null)
        raftHAServer.notifyApplied();
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd ha-raft && mvn test -Dtest=WaitForApplyTest -pl . -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/WaitForApplyTest.java
git commit -m "feat(ha-raft): add wait-for-apply notification pattern for read consistency"
```

---

## Task 10: Add read consistency to `RaftReplicatedDatabase`

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ReadConsistencyContextTest.java`

- [ ] **Step 1: Write failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ReadConsistencyContextTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the ReadConsistencyContext ThreadLocal lifecycle.
 */
class ReadConsistencyContextTest {

  @Test
  void setAndClearContext() {
    RaftReplicatedDatabase.setReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, 42);
    final var ctx = RaftReplicatedDatabase.getReadConsistencyContext();
    assertThat(ctx).isNotNull();
    assertThat(ctx.consistency()).isEqualTo(Database.READ_CONSISTENCY.READ_YOUR_WRITES);
    assertThat(ctx.readAfterIndex()).isEqualTo(42);

    RaftReplicatedDatabase.clearReadConsistencyContext();
    assertThat(RaftReplicatedDatabase.getReadConsistencyContext()).isNull();
  }

  @Test
  void contextIsNullByDefault() {
    assertThat(RaftReplicatedDatabase.getReadConsistencyContext()).isNull();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ha-raft && mvn test -Dtest=ReadConsistencyContextTest -pl . -q`
Expected: FAIL - methods do not exist

- [ ] **Step 3: Add ReadConsistencyContext and query interception to RaftReplicatedDatabase**

In `RaftReplicatedDatabase.java`, add after the `HTTP_CLIENT` field (after line 80):

```java
  /**
   * Thread-local context for read consistency. Set by the HTTP handler layer before query
   * execution and cleared in a finally block after.
   */
  public record ReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {
  }

  private static final ThreadLocal<ReadConsistencyContext> READ_CONSISTENCY_CONTEXT = new ThreadLocal<>();

  public static void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  public static void clearReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  public static ReadConsistencyContext getReadConsistencyContext() {
    return READ_CONSISTENCY_CONTEXT.get();
  }
```

Replace the three `query()` methods (lines 672-684) with:

```java
  @Override
  public ResultSet query(final String language, final String query) {
    waitForReadConsistency();
    return proxied.query(language, query);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }
```

Add the `waitForReadConsistency()` method:

```java
  /**
   * Waits for the follower to catch up to the required consistency level before executing a read.
   * On the leader, this is a no-op (data is always current).
   */
  private void waitForReadConsistency() {
    if (isLeader())
      return;

    if (raftHAServer == null)
      return;

    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency;
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (ctx.readAfterIndex >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex);
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (ctx.readAfterIndex >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex);
      else
        raftHAServer.waitForLocalApply();
    }
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ha-raft && mvn test -Dtest=ReadConsistencyContextTest -pl . -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/ReadConsistencyContextTest.java
git commit -m "feat(ha-raft): add read consistency context and query interception"
```

---

## Task 11: Add K8s auto-join and auto-leave

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Add `tryAutoJoinCluster()` to RaftHAServer**

Add to `RaftHAServer.java` after `getCommitIndex()`:

```java
  /**
   * Attempts to join an existing Raft cluster by probing known peers.
   * Called during startup when running in K8s mode with no existing Raft storage.
   * <p>
   * Uses randomized jitter to prevent thundering herd when multiple pods start simultaneously.
   */
  void tryAutoJoinCluster() {
    final long jitterMs = Math.abs(localPeerId.hashCode() % 3000L);
    if (jitterMs > 0) {
      HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing...", jitterMs);
      try {
        Thread.sleep(jitterMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        final RaftProperties tempProps = new RaftProperties();
        RaftServerConfigKeys.Rpc.setTimeoutMin(tempProps, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));

        final RaftGroup targetGroup = RaftGroup.valueOf(raftGroup.getGroupId(), peer);
        try (final RaftClient tempClient = RaftClient.newBuilder()
            .setRaftGroup(targetGroup)
            .setProperties(tempProps)
            .build()) {

          final var groupInfo = tempClient.getGroupManagementApi(peer.getId())
              .info(raftGroup.getGroupId());

          if (groupInfo != null && groupInfo.isSuccess()) {
            final var confOpt = groupInfo.getConf();
            if (confOpt.isPresent()) {
              final var conf = confOpt.get();
              boolean alreadyMember = false;
              for (final var p : conf.getPeersList())
                if (p.getId().toStringUtf8().equals(localPeerId.toString())) {
                  alreadyMember = true;
                  break;
                }

              if (!alreadyMember) {
                HALog.log(this, HALog.BASIC,
                    "K8s auto-join: adding self (%s) to existing cluster via peer %s",
                    localPeerId, peer.getId());

                RaftPeer localPeer = null;
                for (final RaftPeer p : raftGroup.getPeers())
                  if (p.getId().equals(localPeerId)) {
                    localPeer = p;
                    break;
                  }

                if (localPeer != null) {
                  final List<RaftPeer> newPeers = new ArrayList<>();
                  for (final var existingPeer : conf.getPeersList()) {
                    final String existingId = existingPeer.getId().toStringUtf8();
                    for (final RaftPeer p : raftGroup.getPeers())
                      if (p.getId().toString().equals(existingId)) {
                        newPeers.add(p);
                        break;
                      }
                  }
                  newPeers.add(localPeer);

                  final RaftClientReply joinReply = tempClient.admin().setConfiguration(newPeers);
                  if (!joinReply.isSuccess())
                    LogManager.instance().log(this, Level.WARNING,
                        "K8s auto-join: setConfiguration rejected: %s",
                        joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
                  else
                    HALog.log(this, HALog.BASIC,
                        "K8s auto-join: successfully joined cluster with %d peers", newPeers.size());
                }
              } else {
                HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
              }
            }
            return;
          }
        }
      } catch (final Exception e) {
        HALog.log(this, HALog.DETAILED,
            "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.WARNING,
        "K8s auto-join: no existing cluster found, starting as new cluster. "
            + "If other nodes exist but are unreachable, this may create a split-brain. Verify network connectivity");
  }

  /**
   * Checks if existing Raft storage exists for this peer (i.e., this is a restart, not a fresh start).
   */
  boolean hasExistingRaftStorage() {
    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    if (!storageDir.exists())
      return false;
    final File[] subdirs = storageDir.listFiles(f -> f.isDirectory() && !f.getName().equals("lost+found"));
    return subdirs != null && subdirs.length > 0;
  }
```

- [ ] **Step 2: Wire K8s auto-join into RaftHAServer.start()**

In `RaftHAServer.start()`, add after the group committer is created (after line 350):

```java
    // K8s auto-join: if running in Kubernetes with no existing storage, try to join an existing cluster
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S) && !hasExistingRaftStorage()) {
      tryAutoJoinCluster();
    }
```

Note: `hasExistingRaftStorage()` must be called before the storage directory is created/formatted by the Raft server. Move the check before `raftServer.start()` and store the result:

```java
    final boolean hadExistingStorage = hasExistingRaftStorage();
```

Then use it after the group committer:

```java
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S) && !hadExistingStorage)
      tryAutoJoinCluster();
```

- [ ] **Step 3: Wire K8s auto-leave into RaftHAPlugin.stopService()**

In `RaftHAPlugin.stopService()`, add before `raftHAServer.stop()`:

```java
    // K8s auto-leave: gracefully remove self from cluster on shutdown
    if (configuration != null && configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
      try {
        raftHAServer.leaveCluster();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "K8s auto-leave failed (best-effort): %s", e.getMessage());
      }
    }
```

- [ ] **Step 4: Build to verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): add K8s auto-join on startup and auto-leave on shutdown"
```

---

## Task 12: Integration tests for dynamic membership

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftDynamicMembershipIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftTransferLeadershipIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaveClusterIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftVerifyDatabaseIT.java`

- [ ] **Step 1: Create RaftTransferLeadershipIT**

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class RaftTransferLeadershipIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void transferLeadershipViaHttpEndpoint() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int targetIndex = leaderIndex == 0 ? 1 : 0;

    // Transfer via HTTP
    final int httpPort = 2480 + leaderIndex;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster/leader");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    final String body = new JSONObject().put("peerId", "peer-" + targetIndex).put("timeoutMs", 10_000).toString();
    conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    assertThat(conn.getResponseCode()).isEqualTo(200);
    conn.disconnect();

    // Wait for election to settle
    Thread.sleep(3_000);

    // Verify new leader can serve writes
    final Database newLeaderDb = getServerDatabase(targetIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      if (!newLeaderDb.getSchema().existsType("TransferTest"))
        newLeaderDb.getSchema().createVertexType("TransferTest");
      newLeaderDb.newVertex("TransferTest").set("value", 1).save();
    });

    assertClusterConsistency();
  }
}
```

- [ ] **Step 2: Create RaftVerifyDatabaseIT**

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class RaftVerifyDatabaseIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void verifyDatabaseReportsMatchingChecksums() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // Write some data
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("VerifyTest"))
        leaderDb.getSchema().createVertexType("VerifyTest");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("VerifyTest").set("index", i).save();
    });

    assertClusterConsistency();

    // Call verify endpoint
    final int httpPort = 2480 + leaderIndex;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster/verify/" + getDatabaseName());
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));

    assertThat(conn.getResponseCode()).isEqualTo(200);
    final String responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    final JSONObject response = new JSONObject(responseBody);

    assertThat(response.getString("database")).isEqualTo(getDatabaseName());
    final var nodes = response.getJSONArray("nodes");
    assertThat(nodes.length()).isGreaterThan(0);

    for (int i = 0; i < nodes.length(); i++) {
      final JSONObject node = nodes.getJSONObject(i);
      assertThat(node.getBoolean("match")).isTrue();
    }

    conn.disconnect();
  }
}
```

- [ ] **Step 3: Run the ITs**

Run: `cd ha-raft && mvn test -Dtest="RaftTransferLeadershipIT,RaftVerifyDatabaseIT" -pl . -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftTransferLeadershipIT.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftVerifyDatabaseIT.java
git commit -m "test(ha-raft): add ITs for leadership transfer and database verification"
```

---

## Task 13: Integration test for read consistency with bookmarks

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReadConsistencyBookmarkIT.java`

- [ ] **Step 1: Create the IT**

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests read consistency using the wait-for-apply mechanism directly
 * (not through HTTP headers, which require HTTP handler integration).
 */
class RaftReadConsistencyBookmarkIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerWaitsForAppliedIndexBeforeRead() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();

    // Write on leader
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("BookmarkTest"))
        leaderDb.getSchema().createVertexType("BookmarkTest");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newVertex("BookmarkTest").set("index", i).save();
    });

    // Get the leader's last applied index as a "bookmark"
    final long bookmark = leaderRaft.getLastAppliedIndex();
    assertThat(bookmark).isGreaterThan(0);

    // Wait on the follower for that bookmark
    followerRaft.waitForAppliedIndex(bookmark);

    // Now the follower should have all 50 records
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long count = followerDb.countType("BookmarkTest", true);
    assertThat(count).isEqualTo(50);
  }

  @Test
  void readConsistencyContextAppliesOnFollowerQueries() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();

    // Write on leader
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ContextTest"))
        leaderDb.getSchema().createVertexType("ContextTest");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++)
        leaderDb.newVertex("ContextTest").set("index", i).save();
    });

    final long bookmark = leaderRaft.getLastAppliedIndex();

    // Set read consistency context on follower thread
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    try {
      RaftReplicatedDatabase.setReadConsistencyContext(
          Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
      // The query() call should internally wait for the bookmark before executing
      final var rs = followerDb.query("sql", "SELECT count(*) as cnt FROM ContextTest");
      assertThat(rs.hasNext()).isTrue();
      final long count = rs.next().getProperty("cnt");
      assertThat(count).isEqualTo(20);
    } finally {
      RaftReplicatedDatabase.clearReadConsistencyContext();
    }
  }
}
```

- [ ] **Step 2: Run the IT**

Run: `cd ha-raft && mvn test -Dtest=RaftReadConsistencyBookmarkIT -pl . -q`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReadConsistencyBookmarkIT.java
git commit -m "test(ha-raft): add IT for read consistency with bookmark wait-for-apply"
```

---

## Task 14: Run full test suite and fix any issues

**Files:** All modified files

- [ ] **Step 1: Run all existing HA tests to check for regressions**

Run: `cd ha-raft && mvn test -pl . -q`
Expected: All tests PASS (existing + new)

- [ ] **Step 2: Fix any compilation or test failures**

If any tests fail, investigate and fix the root cause. Common issues:
- Import conflicts from new methods
- Thread timing issues in tests (add appropriate waits)
- The `httpAddresses` mutability change may affect other tests

- [ ] **Step 3: Build the full project**

Run: `mvn clean install -DskipTests -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix(ha-raft): address test regressions from membership and consistency changes"
```

---

## Task 15: Wire HTTP bookmark headers (server module)

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/DatabaseAbstractHandler.java`

This task wires the `X-ArcadeDB-Commit-Index` and `X-ArcadeDB-Read-Consistency` headers into the server's HTTP handler base class. The `RaftReplicatedDatabase.setReadConsistencyContext()` and `clearReadConsistencyContext()` methods are already available from Task 10.

- [ ] **Step 1: Add read consistency context setup in DatabaseAbstractHandler**

In `DatabaseAbstractHandler.java`, in the `execute(HttpServerExchange, ServerSecurityUser, JSONObject)` method, after the database is resolved and before the subclass `execute()` is called, add:

```java
    // Set read consistency context for HA follower reads
    final HeaderValues commitIndexHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Commit-Index");
    final HeaderValues readConsistencyHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Read-Consistency");
    if (database instanceof com.arcadedb.server.ha.raft.RaftReplicatedDatabase) {
      final String consistencyStr = readConsistencyHeader != null && !readConsistencyHeader.isEmpty()
          ? readConsistencyHeader.getFirst()
          : database.getConfiguration().getValueAsString(com.arcadedb.GlobalConfiguration.HA_READ_CONSISTENCY);
      final long bookmarkIndex = commitIndexHeader != null && !commitIndexHeader.isEmpty()
          ? Long.parseLong(commitIndexHeader.getFirst()) : -1;
      try {
        final com.arcadedb.database.Database.READ_CONSISTENCY consistency =
            com.arcadedb.database.Database.READ_CONSISTENCY.valueOf(consistencyStr.toUpperCase());
        com.arcadedb.server.ha.raft.RaftReplicatedDatabase.setReadConsistencyContext(consistency, bookmarkIndex);
      } catch (final IllegalArgumentException ignored) {
        // Invalid consistency level, skip
      }
    }
```

And in a finally block after the subclass execute returns:

```java
    com.arcadedb.server.ha.raft.RaftReplicatedDatabase.clearReadConsistencyContext();
```

Note: This introduces a compile-time reference from `server` to `ha-raft`. Since `ha-raft` has `server` as `provided` scope, this creates a circular dependency. Instead, use reflection or move the static ThreadLocal methods to a class in `server` or `engine`. The simplest approach: check `instanceof HAReplicatedDatabase` and use the `Database.READ_CONSISTENCY` enum (already in engine). The `setReadConsistencyContext` call can be made via a static method that the `ha-raft` module registers at startup.

**Alternative (recommended): Use a functional interface callback.** Add to `HAReplicatedDatabase`:

```java
  default void setReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {}
  default void clearReadConsistencyContext() {}
```

Then `RaftReplicatedDatabase` overrides these instance methods, and `DatabaseAbstractHandler` calls them on the database instance (no static reference to `ha-raft` classes needed).

- [ ] **Step 2: Add commit index response header**

After a successful write (in the response path of `PostCommandHandler`), add the commit index header:

```java
    // Emit bookmark header for read-your-writes consistency
    if (database instanceof HAReplicatedDatabase) {
      final var ha = httpServer.getServer().getHA();
      if (ha != null) {
        // The commit index can be read from the Raft state machine
        // This requires exposing getLastAppliedIndex() via HAServerPlugin
      }
    }
```

This step requires adding `getLastAppliedIndex()` to `HAServerPlugin` as a default method returning -1, overridden by `RaftHAPlugin`.

- [ ] **Step 3: Build to verify**

Run: `mvn compile -q -pl server,ha-raft`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/DatabaseAbstractHandler.java server/src/main/java/com/arcadedb/server/HAServerPlugin.java server/src/main/java/com/arcadedb/server/HAReplicatedDatabase.java
git commit -m "feat(server): wire HTTP bookmark headers for read consistency"
```

---

## Task 16: Update branch comparison documentation

**Files:**
- Modify: `docs/ha-branch-comparison.md`

- [ ] **Step 1: Update the comparison doc**

Update `docs/ha-branch-comparison.md` to reflect that dynamic membership, K8s auto-join, and read consistency have been ported to `ha-redesign`. Move these features from the "Only in apache-ratis" section to the "Shared Features" section. Update the summary section.

- [ ] **Step 2: Commit**

```bash
git add docs/ha-branch-comparison.md
git commit -m "docs: update branch comparison after porting membership, K8s, and read consistency"
```
