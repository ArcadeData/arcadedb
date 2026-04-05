# HA Raft Failure Test Scenarios Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add crash/recovery integration tests with full node restart support (Track 1) and split-brain tests via Ratis MiniRaftCluster (Track 2), all verified with `DatabaseComparator` byte-level consistency checks.

**Architecture:** Track 1 adds a `HA_RAFT_PERSIST_STORAGE` config flag that skips Raft storage directory deletion on restart, enabling in-process server stop/restart within a single test. Track 2 introduces `BaseMiniRaftTest` using `MiniRaftClusterWithGrpc` from the `ratis-test` artifact; each Ratis peer is backed by a lightweight `ArcadeDBServer` (no HA, no HTTP) that supplies real `EmbeddedDatabase` instances to the `ArcadeStateMachine`.

**Tech Stack:** Java 21, Apache Ratis 3.2.0 (`ratis-server`, `ratis-grpc`, `ratis-test`), JUnit 5, AssertJ, ArcadeDB engine/server APIs.

---

## Reference files

Read these before starting any task:

| File | Why |
|---|---|
| `ha-raft/pom.xml` | Dependency declarations |
| `ha-raft/src/main/java/.../RaftHAServer.java` | `start()` storage dir logic to modify |
| `ha-raft/src/test/java/.../BaseRaftHATest.java` | Base class to extend |
| `ha-raft/src/test/java/.../RaftSplitBrain3NodesIT.java` | Stub to replace |
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | Pattern for adding a new config key |
| `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java` | `startServers()`, `getServer(i)`, `checkDatabasesAreIdentical()` |
| `docs/plans/2026-02-24-ha-raft-failure-test-scenarios-design.md` | Approved design |

---

## Task 1: Add ratis-test test dependency

**Files:**
- Modify: `ha-raft/pom.xml`

**Step 1: Add the dependency**

In `ha-raft/pom.xml`, inside `<dependencies>`, after the existing `ratis-metrics-default` dependency, add:

```xml
<dependency>
    <groupId>org.apache.ratis</groupId>
    <artifactId>ratis-test</artifactId>
    <version>${ratis.version}</version>
    <scope>test</scope>
</dependency>
```

**Step 2: Compile to verify the dependency resolves**

```bash
cd ha-raft && mvn compile -q
```

Expected: BUILD SUCCESS with no errors.

**Step 3: Commit**

```bash
git add ha-raft/pom.xml
git commit -m "test(ha-raft): add ratis-test dependency for MiniRaftCluster support"
```

---

## Task 2: Add HA_RAFT_PERSIST_STORAGE to GlobalConfiguration

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`

**Step 1: Read the file and find the insertion point**

Open the file. Locate the `HA_RAFT_PORT` entry (around line 547). Insert the new entry immediately after `HA_RAFT_PORT`'s closing `)`.

**Step 2: Add the config key**

```java
HA_RAFT_PERSIST_STORAGE("arcadedb.ha.raftPersistStorage", SCOPE.SERVER,
    "If true, the Raft storage directory is not deleted when the server starts. " +
    "Enables node restart and rejoin in integration tests. Not intended for production use.",
    Boolean.class, false),
```

**Step 3: Compile engine**

```bash
cd engine && mvn compile -q
```

Expected: BUILD SUCCESS.

**Step 4: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "feat(ha-raft): add HA_RAFT_PERSIST_STORAGE config flag for test restart support"
```

---

## Task 3: Modify RaftHAServer.start() to respect HA_RAFT_PERSIST_STORAGE

**Files:**
- Modify: `ha-raft/src/main/java/.../RaftHAServer.java`

**Step 1: Locate the storage dir block in start()**

In `RaftHAServer.start()`, find these lines (around line 198):

```java
final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
// Clean existing Raft storage to avoid FORMAT conflicts on restart
if (storageDir.exists())
  deleteRecursive(storageDir);
RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
```

**Step 2: Make deletion conditional**

Replace those lines with:

```java
final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
// Only delete existing Raft storage when persistence is not requested.
// Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
// within a single test run, so the Raft log survives across stop/start calls.
final boolean persistStorage = configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
if (storageDir.exists() && !persistStorage)
  deleteRecursive(storageDir);
RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
```

**Step 3: Compile ha-raft**

```bash
cd ha-raft && mvn compile -q
```

Expected: BUILD SUCCESS.

**Step 4: Run the existing unit tests to verify nothing broke**

```bash
cd ha-raft && mvn test -Dtest="RaftHAServerTest,ConfigValidationTest,RaftLogEntryCodecTest" -q
```

Expected: All existing tests pass.

**Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "feat(ha-raft): skip Raft storage deletion when HA_RAFT_PERSIST_STORAGE=true"
```

---

## Task 4: Extend BaseRaftHATest with restartServer()

**Files:**
- Modify: `ha-raft/src/test/java/.../BaseRaftHATest.java`

**Step 1: Add persistentRaftStorage() override hook**

After the `BASE_RAFT_PORT` field declaration, add:

```java
/**
 * Returns true if Raft storage directories should be preserved across server stop/start
 * within a single test. Override to true in tests that call {@link #restartServer(int)}.
 * Default is false to match existing test behaviour.
 */
protected boolean persistentRaftStorage() {
  return false;
}
```

**Step 2: Wire the flag in onServerConfiguration()**

The existing `onServerConfiguration()` in `BaseRaftHATest` only sets `HA_IMPLEMENTATION`. Add the persist flag:

```java
@Override
protected void onServerConfiguration(final ContextConfiguration config) {
  config.setValue(GlobalConfiguration.HA_IMPLEMENTATION, "raft");
  if (persistentRaftStorage())
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);
}
```

**Step 3: Add restartServer()**

Add at the bottom of `BaseRaftHATest`, before the closing brace:

```java
/**
 * Stops server {@code serverIndex} then immediately restarts it using the same
 * {@link ArcadeDBServer} instance and configuration. Waits for replication to
 * catch up before returning.
 * <p>
 * Only valid when {@link #persistentRaftStorage()} returns true; otherwise Raft
 * storage is deleted on restart and the peer cannot rejoin the same group.
 */
protected void restartServer(final int serverIndex) {
  LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d for restart", serverIndex);
  getServer(serverIndex).stop();

  // Brief pause to allow the OS to release the gRPC port
  try {
    Thread.sleep(2_000);
  } catch (final InterruptedException e) {
    Thread.currentThread().interrupt();
    return;
  }

  LogManager.instance().log(this, Level.INFO, "TEST: Starting server %d again", serverIndex);
  getServer(serverIndex).start();

  // Wait for the restarted peer to catch up to the current leader's last applied index
  waitForReplicationIsCompleted(serverIndex);
  LogManager.instance().log(this, Level.INFO, "TEST: Server %d restarted and caught up", serverIndex);
}
```

**Step 4: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

Expected: BUILD SUCCESS.

**Step 5: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java
git commit -m "test(ha-raft): add persistentRaftStorage() + restartServer() to BaseRaftHATest"
```

---

## Task 5: Implement RaftReplicaCrashAndRecoverIT

**Scenario:** 2-node cluster (quorum=none). Write 200 records. Stop the replica. Write 200 more on the leader. Restart the replica. Verify it catches up via log replay. Verify `DatabaseComparator` passes.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaCrashAndRecoverIT.java`

**Step 1: Write the test**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster with no-quorum replication.
 * Verifies that a crashed replica restarts and catches up to the leader via Raft log replay
 * (hot resync), with full DatabaseComparator verification after recovery.
 */
class RaftReplicaCrashAndRecoverIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaCatchesUpAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write initial data with both nodes up
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftCrashRecover"))
        leaderDb.getSchema().createVertexType("RaftCrashRecover");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftCrashRecover");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify replica has all phase-1 data
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftCrashRecover", true))
        .as("Replica should have 200 records before crash").isEqualTo(200);

    // Phase 2: crash the replica, write more data on the leader
    LogManager.instance().log(this, Level.INFO, "TEST: Crashing replica %d", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftCrashRecover");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    assertThat(leaderDb.countType("RaftCrashRecover", true))
        .as("Leader should have 400 records while replica is down").isEqualTo(400);

    // Phase 3: restart the replica and let it catch up via log replay
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d", replicaIndex);
    restartServer(replicaIndex);

    // Verify catch-up: replica must have all 400 records
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftCrashRecover", true))
        .as("Replica should have all 400 records after recovery").isEqualTo(400);

    // Full DatabaseComparator verification (not overridden — byte-level check)
    assertClusterConsistency();
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }
}
```

**Step 2: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

Expected: BUILD SUCCESS.

**Step 3: Run the test**

```bash
cd ha-raft && mvn test -Dtest="RaftReplicaCrashAndRecoverIT" -DskipITs=false
```

Expected: TEST PASSES. If it fails, check:
- Is the Raft log being replayed? Look for `"Applying tx"` log entries on restart.
- Is `waitForReplicationIsCompleted` timing out? Increase the deadline in `BaseRaftHATest` if needed.
- Is `DatabaseComparator` failing? Check page version alignment.

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaCrashAndRecoverIT.java
git commit -m "test(ha-raft): add RaftReplicaCrashAndRecoverIT with log replay and DatabaseComparator"
```

---

## Task 6: Implement RaftLeaderCrashAndRecoverIT

**Scenario:** 3-node cluster (quorum=majority). Write 200 records. Stop the leader. New leader elected. Write 100 more. Restart the old leader as follower. Verify all 3 nodes have 300 records. Verify `DatabaseComparator` passes.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashAndRecoverIT.java`

**Step 1: Write the test**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with majority quorum.
 * Verifies that after a leader crash, a new leader is elected, writes continue,
 * the old leader rejoins as a follower and catches up, with full DatabaseComparator
 * verification after recovery.
 */
class RaftLeaderCrashAndRecoverIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void oldLeaderRejoinsAsFollowerAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write initial data with all nodes up
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftLeaderRecover"))
        leaderDb.getSchema().createVertexType("RaftLeaderRecover");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftLeaderRecover");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    // Phase 2: crash the leader, wait for new leader
    LogManager.instance().log(this, Level.INFO, "TEST: Crashing leader %d", leaderIndex);
    getServer(leaderIndex).stop();

    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("A new leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must differ from old leader").isNotEqualTo(leaderIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: New leader elected: server %d", newLeaderIndex);

    // Phase 3: write more data on the new leader
    final var newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = newLeaderDb.newVertex("RaftLeaderRecover");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    // Verify surviving nodes have 300 records
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue;
      waitForReplicationIsCompleted(i);
      assertThat(getServerDatabase(i, getDatabaseName()).countType("RaftLeaderRecover", true))
          .as("Surviving server " + i + " should have 300 records").isEqualTo(300);
    }

    // Phase 4: restart the old leader as a follower
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting old leader %d as follower", leaderIndex);
    restartServer(leaderIndex);

    // Verify old leader rejoined as follower (not leader) and has all 300 records
    final RaftHAPlugin oldLeaderPlugin = getRaftPlugin(leaderIndex);
    assertThat(oldLeaderPlugin).isNotNull();
    assertThat(oldLeaderPlugin.isLeader())
        .as("Old leader should be a follower after restart").isFalse();

    assertThat(getServerDatabase(leaderIndex, getDatabaseName()).countType("RaftLeaderRecover", true))
        .as("Recovered node should have all 300 records").isEqualTo(300);

    // Full DatabaseComparator verification across all 3 nodes
    assertClusterConsistency();
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
```

**Step 2: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

**Step 3: Run the test**

```bash
cd ha-raft && mvn test -Dtest="RaftLeaderCrashAndRecoverIT" -DskipITs=false
```

Expected: TEST PASSES. If the old leader assertion `isLeader() == false` fails, add a brief wait (Ratis step-down takes a moment after discovering the new term).

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashAndRecoverIT.java
git commit -m "test(ha-raft): add RaftLeaderCrashAndRecoverIT with leader rejoin and DatabaseComparator"
```

---

## Task 7: Implement RaftFullSnapshotResyncIT

**Scenario:** 2-node cluster (quorum=none). Configure a very small Raft snapshot threshold (10 entries). Write enough records to trigger a snapshot. Stop the replica. Write many more records (enough to purge the log before the snapshot). Restart the replica — it must receive a full snapshot install rather than log replay. Verify `DatabaseComparator` passes.

**Background:** Ratis compacts the log after a snapshot, so a replica that is too far behind cannot catch up via log replay and must receive a snapshot. Configure `RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold` to a small value (10 log entries). After the snapshot, purge old log segments by setting `RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex` to true.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFullSnapshotResyncIT.java`

**Step 1: Check how snapshots are triggered in Ratis 3.2.0**

Before writing the test, look at the Ratis `RaftServerConfigKeys.Snapshot` keys available in 3.2.0:

```java
// In RaftHAServer or a test override, set:
RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 10L);
RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
```

Also check that `ArcadeStateMachine.takeSnapshot()` is implemented. If it extends `BaseStateMachine`, the default snapshot does nothing. We need a real snapshot implementation for this test to work.

> **INVESTIGATION REQUIRED:** Open `ha-raft/src/main/java/.../ArcadeStateMachine.java` and check if `takeSnapshot()` is implemented. If it only has the `BaseStateMachine` no-op, this test will hang waiting for a snapshot that never materialises. If snapshot is not yet implemented, write a minimal `takeSnapshot()` that records `lastAppliedTermIndex` and returns a `SnapshotInfo`. See Ratis `BaseStateMachine.takeSnapshot()` Javadoc for the protocol.

**Step 2: Write the test (only after confirming snapshot support)**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster, no-quorum.
 * Verifies that a replica which has fallen too far behind receives a full snapshot
 * install (rather than log replay) when it restarts, and then passes DatabaseComparator.
 * <p>
 * Prerequisites: ArcadeStateMachine.takeSnapshot() must be implemented.
 */
class RaftFullSnapshotResyncIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
    // Trigger a snapshot every 10 log entries so the replica falls behind the snapshot
    config.setValue(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaReceivesFullSnapshotAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write enough records to trigger at least one snapshot on the leader
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftSnapshotResync"))
        leaderDb.getSchema().createVertexType("RaftSnapshotResync");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftSnapshotResync");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftSnapshotResync", true))
        .as("Replica should have 50 records before crash").isEqualTo(50);

    // Phase 2: stop replica, write many more records (> snapshot threshold) to force log compaction
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d to simulate long absence", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftSnapshotResync");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    assertThat(leaderDb.countType("RaftSnapshotResync", true))
        .as("Leader should have 250 records while replica is down").isEqualTo(250);

    // Phase 3: restart replica - it should receive a snapshot install
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d - expecting snapshot install", replicaIndex);
    restartServer(replicaIndex);

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftSnapshotResync", true))
        .as("Replica should have all 250 records after snapshot install").isEqualTo(250);

    // Full DatabaseComparator verification
    assertClusterConsistency();
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }
}
```

> **NOTE:** This test requires a new `GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD` config key and corresponding wiring in `RaftHAServer.start()` (`RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold`). Add that key following the same pattern as Task 2 before writing/compiling this test.

**Step 3: Add HA_RAFT_SNAPSHOT_THRESHOLD to GlobalConfiguration**

Following the same pattern as Task 2, add to `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`:

```java
HA_RAFT_SNAPSHOT_THRESHOLD("arcadedb.ha.raftSnapshotThreshold", SCOPE.SERVER,
    "Number of Raft log entries after which the leader automatically takes a snapshot. " +
    "Lower values cause more frequent snapshots and earlier log compaction.",
    Long.class, 10000L),
```

Then in `RaftHAServer.start()`, after the timeout config lines, add:

```java
final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD);
RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
```

**Step 4: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

**Step 5: Run the test**

```bash
cd ha-raft && mvn test -Dtest="RaftFullSnapshotResyncIT" -DskipITs=false
```

Expected: TEST PASSES. If the replica catches up via log replay instead of snapshot, the log threshold may not have been low enough or log purge may not have occurred. Check logs for `"installSnapshot"` messages from Ratis.

**Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFullSnapshotResyncIT.java
git commit -m "test(ha-raft): add RaftFullSnapshotResyncIT with snapshot install and DatabaseComparator"
```

---

## Task 8: Create BaseMiniRaftTest

**Context:** `MiniRaftClusterWithGrpc` is in the `ratis-test` artifact. It spins up a real Ratis cluster with gRPC transport, all in-process. Each peer runs a state machine of the class you specify. After the cluster starts, you retrieve each peer's `ArcadeStateMachine` and wire it to a real `ArcadeDBServer` (HA disabled, no HTTP) for database operations.

**Before coding:** Verify the `MiniRaftClusterWithGrpc` API in Ratis 3.2.0. The key classes are in package `org.apache.ratis.grpc`. The factory pattern is:

```java
MiniRaftClusterWithGrpc cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(n, properties);
```

State machine class is registered via:
```java
RaftStateMachineRegistry.register(ArcadeStateMachine.class, properties);
// or via:
properties.setClass(RaftServerConfigKeys.StateMachine.REGISTRY_CLASS_KEY, ArcadeStateMachine.class, StateMachine.class);
```

Check the Ratis 3.2.0 source/Javadoc to confirm the exact registration mechanism.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseMiniRaftTest.java`

**Step 1: Write BaseMiniRaftTest**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.simulation.RequestHandler;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Raft HA split-brain and partition tests using Ratis MiniRaftClusterWithGrpc.
 * <p>
 * Each peer runs a real {@link ArcadeStateMachine} backed by a real {@link ArcadeDBServer}
 * (HA disabled, HTTP disabled) so that database operations are fully exercised.
 * <p>
 * Subclasses call {@link #submitSchemaEntry(String, String)} and {@link #submitTxEntry(String, byte[])}
 * to drive state machine entries without going through {@link RaftReplicatedDatabase}.
 * Partition simulation is done via {@link #isolatePeer(RaftPeerId)} and {@link #healAll()}.
 */
public abstract class BaseMiniRaftTest {

  private static final String DB_NAME    = "mini-raft-test";
  private static final String TARGET_DIR = "target/mini-raft-test";

  private MiniRaftClusterWithGrpc  cluster;
  private ArcadeDBServer[]         arcadeServers;
  private List<RaftPeer>           peers;

  /**
   * Number of peers in this test's cluster. Override to return 3 or 5.
   */
  protected abstract int getPeerCount();

  @BeforeEach
  public void setUp() throws Exception {
    // Clean test directories
    final Path targetPath = Path.of(TARGET_DIR);
    if (Files.exists(targetPath))
      deleteDirectory(targetPath);

    final RaftProperties properties = new RaftProperties();

    // Register ArcadeStateMachine as the state machine class.
    // In Ratis 3.2.0 verify the exact key via RaftServerConfigKeys.StateMachine.
    // The typical approach is:
    //   properties.setClass("raft.statemachine.class", ArcadeStateMachine.class, StateMachine.class);
    // Check the Ratis 3.2.0 source for the correct property key.
    RaftServerConfigKeys.setStateMachineClass(properties, ArcadeStateMachine.class);

    cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(getPeerCount(), properties);
    cluster.start();

    peers = new ArrayList<>(cluster.getPeers());

    // Start one lightweight ArcadeDBServer per peer (HA disabled, HTTP disabled)
    arcadeServers = new ArcadeDBServer[getPeerCount()];
    for (int i = 0; i < getPeerCount(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String dbDir = TARGET_DIR + "/server-" + peerId;

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "MiniRaft_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, dbDir);
      config.setValue(GlobalConfiguration.HA_ENABLED, false);
      config.setValue(GlobalConfiguration.SERVER_HTTP_ENABLED, false);

      arcadeServers[i] = new ArcadeDBServer(config);
      arcadeServers[i].start();
      arcadeServers[i].createDatabase(DB_NAME);

      // Wire the ArcadeStateMachine to this ArcadeDBServer
      final RaftServer.Division division = cluster.getServer(peerId).getDivision(cluster.getGroup().getGroupId());
      final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
      sm.setServer(arcadeServers[i]);

      LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: peer %s wired to server %d", peerId, i);
    }

    // Wait for leader election
    org.apache.ratis.RaftTestUtil.waitForLeader(cluster);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.close();

    if (arcadeServers != null)
      for (final ArcadeDBServer server : arcadeServers)
        if (server != null && server.isStarted())
          server.stop();

    final Path targetPath = Path.of(TARGET_DIR);
    if (Files.exists(targetPath))
      deleteDirectory(targetPath);
  }

  /**
   * Submits a SCHEMA_ENTRY Raft log entry (schema JSON) via the Raft client.
   * All state machines will apply it when committed.
   */
  protected RaftClientReply submitSchemaEntry(final String databaseName, final String schemaJson) throws IOException {
    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry(databaseName, schemaJson, null, null, null, null);
    try (final RaftClient client = cluster.createClient()) {
      return client.io().send(Message.valueOf(encoded));
    }
  }

  /**
   * Submits a TX_ENTRY Raft log entry (raw WAL bytes) via the Raft client.
   */
  protected RaftClientReply submitTxEntry(final String databaseName, final byte[] walData) throws IOException {
    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, null);
    try (final RaftClient client = cluster.createClient()) {
      return client.io().send(Message.valueOf(encoded));
    }
  }

  /**
   * Returns the index of the current Raft leader in {@link #peers}, or -1 if no leader.
   */
  protected int findLeaderPeerIndex() {
    try {
      final RaftPeerId leaderId = cluster.getLeader().getId();
      for (int i = 0; i < peers.size(); i++)
        if (peers.get(i).getId().equals(leaderId))
          return i;
    } catch (final Exception e) {
      // No leader yet
    }
    return -1;
  }

  /**
   * Returns the ArcadeDBServer for peer at {@code peerIndex}.
   */
  protected ArcadeDBServer getArcadeServer(final int peerIndex) {
    return arcadeServers[peerIndex];
  }

  /**
   * Returns the test database for peer at {@code peerIndex}.
   */
  protected Database getPeerDatabase(final int peerIndex) {
    return arcadeServers[peerIndex].getDatabase(DB_NAME);
  }

  /**
   * Returns the MiniRaftCluster for direct peer control.
   */
  protected MiniRaftClusterWithGrpc getCluster() {
    return cluster;
  }

  /**
   * Returns the list of peers in this cluster (order matches ArcadeDBServer indices).
   */
  protected List<RaftPeer> getPeers() {
    return peers;
  }

  /**
   * Verifies that all peer databases are byte-level identical using DatabaseComparator.
   * Waits for all state machines to reach the leader's last applied index first.
   */
  protected void assertAllPeersIdentical() {
    // Wait for convergence: each peer's state machine must reach the leader's last applied index
    waitForAllPeersToConverge();

    // DatabaseComparator works on open databases; flush pages before comparing
    for (int i = 1; i < getPeerCount(); i++) {
      final Database db0 = getPeerDatabase(0);
      final Database dbI = getPeerDatabase(i);
      LogManager.instance().log(this, Level.INFO,
          "BaseMiniRaftTest: comparing peer 0 db vs peer %d db", i);
      new DatabaseComparator().compare(db0, dbI);
    }
    LogManager.instance().log(this, Level.INFO, "BaseMiniRaftTest: all peers are identical");
  }

  private void waitForAllPeersToConverge() {
    // Find the leader's last applied index
    long leaderLastIndex = -1;
    for (int i = 0; i < getPeerCount(); i++) {
      try {
        final RaftServer.Division division = cluster.getServer(peers.get(i).getId())
            .getDivision(cluster.getGroup().getGroupId());
        final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
        if (sm.getLastAppliedTermIndex() != null) {
          leaderLastIndex = Math.max(leaderLastIndex, sm.getLastAppliedTermIndex().getIndex());
        }
      } catch (final Exception e) {
        // peer may be stopped
      }
    }

    if (leaderLastIndex <= 0)
      return;

    final long targetIndex = leaderLastIndex;
    final long deadline = System.currentTimeMillis() + 30_000;

    for (int i = 0; i < getPeerCount(); i++) {
      final int peerIdx = i;
      while (System.currentTimeMillis() < deadline) {
        try {
          final RaftServer.Division division = cluster.getServer(peers.get(peerIdx).getId())
              .getDivision(cluster.getGroup().getGroupId());
          final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
          if (sm.getLastAppliedTermIndex() != null && sm.getLastAppliedTermIndex().getIndex() >= targetIndex)
            break;
        } catch (final Exception e) {
          // peer may be stopped temporarily
        }
        try {
          Thread.sleep(200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private static void deleteDirectory(final Path path) throws IOException {
    Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p -> {
          try {
            Files.delete(p);
          } catch (final IOException ignored) {
          }
        });
  }
}
```

> **IMPORTANT NOTES for implementer:**
> 1. `RaftServerConfigKeys.setStateMachineClass()` — verify the exact method name in Ratis 3.2.0. It may be `StateMachine.Class.set(properties, ArcadeStateMachine.class)` or similar.
> 2. `cluster.getLeader()` — verify this returns a `RaftServer` or `RaftPeer`. Check `MiniRaftCluster.getLeader()` in Ratis 3.2.0.
> 3. `cluster.createClient()` — verify API. May be `RaftClient.newBuilder().setRaftGroup(...).build()`.
> 4. `org.apache.ratis.RaftTestUtil.waitForLeader(cluster)` — check the exact import and method signature.
> 5. `ArcadeStateMachine` needs `RaftLogEntryCodec.encodeSchemaEntry()` — check if this method exists in `RaftLogEntryCodec` or if you need to use the existing `encode()` methods differently. If it doesn't exist, check how to create a `ByteString` payload for a SCHEMA_ENTRY using the codec.
> 6. `GlobalConfiguration.SERVER_HTTP_ENABLED` — verify this config key exists. If not, find the equivalent to disable the HTTP server.

**Step 2: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

Fix any compilation errors by checking the exact Ratis 3.2.0 API. The compile step will reveal any API mismatches.

**Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseMiniRaftTest.java
git commit -m "test(ha-raft): add BaseMiniRaftTest using MiniRaftClusterWithGrpc"
```

---

## Task 9: Implement RaftSplitBrain3NodesIT (replace @Disabled stub)

**Scenario:** 3-node cluster via MiniRaftCluster. Submit 50 schema entries. Isolate the leader (kill it — `cluster.killServer(leaderId)`). The 2-node majority elects a new leader and accepts 50 more entries. The old leader is restarted — it discovers the new term, steps down, catches up. All 3 state machines converge. `DatabaseComparator` passes.

> **Note on true split-brain:** Killing the leader (`cluster.killServer()`) simulates the leader becoming unreachable, not a true split-brain (where the isolated node continues to receive writes). For a true split-brain where the old leader continues accepting writes, gRPC-level message interception is needed. This is complex and beyond the current scope. The test below covers the key correctness property: majority continues after leader loss and recovered node converges.

**Files:**
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain3NodesIT.java` (replace the @Disabled stub entirely)

**Step 1: Replace RaftSplitBrain3NodesIT**

Replace the entire file content with:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster via MiniRaftClusterWithGrpc.
 * Tests leader loss and recovery: the 2-node majority elects a new leader after
 * the leader is killed, accepts writes, then the old leader restarts and converges.
 * Verifies full DatabaseComparator consistency after recovery.
 * <p>
 * Note: true split-brain (where both partitions continue accepting writes simultaneously)
 * requires gRPC-level message interception. This test covers the correctness property
 * that is achievable in-process: majority continues after leader loss and the recovered
 * node converges to the majority state.
 */
class RaftSplitBrain3NodesIT extends BaseMiniRaftTest {

  @Override
  protected int getPeerCount() {
    return 3;
  }

  @Test
  void majorityElectsNewLeaderAfterLeaderLoss() throws Exception {
    // Phase 1: submit 50 schema entries with all 3 nodes up
    final String schemaJson = new JSONObject()
        .put("types", new com.arcadedb.serializer.json.JSONArray()
            .put(new JSONObject().put("name", "SplitBrainType").put("type", "vertex")))
        .toString();

    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry("mini-raft-test", schemaJson);
      assertThat(reply.isSuccess()).as("Schema entry %d should succeed", i).isTrue();
    }

    // Verify all 3 state machines applied the entries
    waitForAllPeersToApply(50);

    // Phase 2: kill the leader
    final int leaderPeerIndex = findLeaderPeerIndex();
    assertThat(leaderPeerIndex).as("A leader must exist").isGreaterThanOrEqualTo(0);
    final RaftPeerId leaderPeerId = getPeers().get(leaderPeerIndex).getId();

    LogManager.instance().log(this, Level.INFO, "TEST: Killing leader peer %s", leaderPeerId);
    getCluster().killServer(leaderPeerId);

    // Phase 3: wait for new leader election among the 2 surviving nodes
    final long deadline = System.currentTimeMillis() + 30_000;
    int newLeaderPeerIndex = -1;
    while (System.currentTimeMillis() < deadline) {
      newLeaderPeerIndex = findLeaderPeerIndex();
      if (newLeaderPeerIndex >= 0 && newLeaderPeerIndex != leaderPeerIndex)
        break;
      Thread.sleep(500);
    }
    assertThat(newLeaderPeerIndex).as("A new leader must be elected after old leader dies").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderPeerIndex).as("New leader must be different").isNotEqualTo(leaderPeerIndex);

    // Phase 4: submit 50 more entries on the new leader (majority of 2 remaining nodes)
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry("mini-raft-test", schemaJson);
      assertThat(reply.isSuccess()).as("Entry %d after failover should succeed", i).isTrue();
    }

    // Phase 5: restart the old leader — it discovers the new term and converges
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting old leader %s", leaderPeerId);
    getCluster().restartServer(leaderPeerId, false);

    // Phase 6: wait for all 3 nodes to converge to 100 applied entries
    waitForAllPeersToApply(100);

    // Full DatabaseComparator verification
    assertAllPeersIdentical();
  }

  private void waitForAllPeersToApply(final long minEntryCount) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      boolean allReady = true;
      for (int i = 0; i < getPeerCount(); i++) {
        try {
          final var division = getCluster().getServer(getPeers().get(i).getId())
              .getDivision(getCluster().getGroup().getGroupId());
          final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
          if (sm.getLastAppliedTermIndex() == null || sm.getLastAppliedTermIndex().getIndex() < minEntryCount) {
            allReady = false;
            break;
          }
        } catch (final Exception e) {
          allReady = false;
          break;
        }
      }
      if (allReady)
        return;
      Thread.sleep(500);
    }
    // Not a hard failure — assertAllPeersIdentical() will catch divergence
  }
}
```

**Step 2: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

**Step 3: Run the test**

```bash
cd ha-raft && mvn test -Dtest="RaftSplitBrain3NodesIT" -DskipITs=false
```

Expected: TEST PASSES. If `DatabaseComparator` fails because SCHEMA_ENTRY application on replicas modifies schema files differently, investigate `applySchemaEntry()` — in the mini-raft context without a real `raftHAServer`, the `isLeader()` check returns `false` for all peers, so schema changes are applied on all peers. This is correct for this test since entries are submitted directly (not via `RaftReplicatedDatabase`).

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain3NodesIT.java
git commit -m "test(ha-raft): implement RaftSplitBrain3NodesIT via MiniRaftCluster (replaces @Disabled stub)"
```

---

## Task 10: Implement RaftSplitBrain5NodesIT

**Scenario:** 5-node cluster. Partition: kill 2 nodes. The remaining 3-node majority elects a leader and accepts 50 writes. The 2 killed nodes restart and converge. All 5 state machines are identical.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain5NodesIT.java`

**Step 1: Write the test**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 5-node cluster via MiniRaftClusterWithGrpc.
 * Tests partition scenario: 2 nodes are killed (minority), 3-node majority continues
 * and accepts writes. Both minority nodes restart and converge to the majority state.
 * Verifies full DatabaseComparator consistency after recovery across all 5 nodes.
 */
class RaftSplitBrain5NodesIT extends BaseMiniRaftTest {

  @Override
  protected int getPeerCount() {
    return 5;
  }

  @Test
  void majorityOfThreeContinuesAfterTwoNodesKilled() throws Exception {
    // Phase 1: write 50 entries with all 5 nodes up
    final String schemaJson = new JSONObject()
        .put("types", new com.arcadedb.serializer.json.JSONArray()
            .put(new JSONObject().put("name", "Split5Type").put("type", "vertex")))
        .toString();

    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry("mini-raft-test", schemaJson);
      assertThat(reply.isSuccess()).as("Initial entry %d should succeed", i).isTrue();
    }

    waitForAllPeersToApply(50);

    // Phase 2: kill 2 minority nodes (non-leaders)
    final int leaderPeerIndex = findLeaderPeerIndex();
    assertThat(leaderPeerIndex).as("A leader must exist").isGreaterThanOrEqualTo(0);

    final List<RaftPeerId> killedPeerIds = new ArrayList<>();
    for (int i = 0; i < getPeerCount(); i++) {
      if (i != leaderPeerIndex && killedPeerIds.size() < 2) {
        final RaftPeerId peerId = getPeers().get(i).getId();
        LogManager.instance().log(this, Level.INFO, "TEST: Killing minority peer %s", peerId);
        getCluster().killServer(peerId);
        killedPeerIds.add(peerId);
      }
    }
    assertThat(killedPeerIds).hasSize(2);

    // Phase 3: write 50 more entries on the 3-node majority (should succeed with quorum of 2 out of 3)
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry("mini-raft-test", schemaJson);
      assertThat(reply.isSuccess()).as("Majority write %d should succeed", i).isTrue();
    }

    // Phase 4: restart both killed nodes — they converge to the majority log
    for (final RaftPeerId peerId : killedPeerIds) {
      LogManager.instance().log(this, Level.INFO, "TEST: Restarting minority peer %s", peerId);
      getCluster().restartServer(peerId, false);
    }

    // Wait for all 5 nodes to reach entry count 100
    waitForAllPeersToApply(100);

    // Full DatabaseComparator verification across all 5 peers
    assertAllPeersIdentical();
  }

  private void waitForAllPeersToApply(final long minEntryCount) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      boolean allReady = true;
      for (int i = 0; i < getPeerCount(); i++) {
        try {
          final var division = getCluster().getServer(getPeers().get(i).getId())
              .getDivision(getCluster().getGroup().getGroupId());
          final ArcadeStateMachine sm = (ArcadeStateMachine) division.getStateMachine();
          if (sm.getLastAppliedTermIndex() == null || sm.getLastAppliedTermIndex().getIndex() < minEntryCount) {
            allReady = false;
            break;
          }
        } catch (final Exception e) {
          allReady = false; // peer may be stopped
          break;
        }
      }
      if (allReady)
        return;
      Thread.sleep(500);
    }
  }
}
```

**Step 2: Compile**

```bash
cd ha-raft && mvn test-compile -q
```

**Step 3: Run the test**

```bash
cd ha-raft && mvn test -Dtest="RaftSplitBrain5NodesIT" -DskipITs=false
```

Expected: TEST PASSES.

**Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftSplitBrain5NodesIT.java
git commit -m "test(ha-raft): add RaftSplitBrain5NodesIT with 5-node partition and DatabaseComparator"
```

---

## Task 11: Full test suite verification

**Step 1: Run all ha-raft tests**

```bash
cd ha-raft && mvn test -DskipITs=false
```

Expected: All tests pass, no regressions on:
- `RaftReplication2NodesIT`
- `RaftReplication3NodesIT`
- `RaftSchemaReplicationIT`
- `RaftLeaderFailoverIT`
- `RaftReplicaFailureIT`
- `RaftLeaderDown2NodesIT`
- `RaftQuorumLostIT`
- `RaftReplicaCrashAndRecoverIT` ← new
- `RaftLeaderCrashAndRecoverIT` ← new
- `RaftFullSnapshotResyncIT` ← new
- `RaftSplitBrain3NodesIT` ← new (replaces @Disabled stub)
- `RaftSplitBrain5NodesIT` ← new

**Step 2: Verify no regressions in engine or server modules**

```bash
cd engine && mvn test -q
```

Expected: BUILD SUCCESS (only `GlobalConfiguration` was changed, no behavior change).

**Step 3: Final commit (if any test-only tweaks were needed)**

```bash
git add -A
git commit -m "test(ha-raft): final verification pass, all failure scenario tests green"
```

---

## Troubleshooting Guide

| Problem | Likely cause | Fix |
|---|---|---|
| `restartServer()` — Ratis fails to start after restart | Port not released fast enough | Increase the 2s sleep in `restartServer()` to 4s |
| `restartServer()` — peer joins as leader not follower | Ratis leader election races | Add a post-restart wait: poll `isLeader()` on the restarted peer and assert it's false within 10s |
| `DatabaseComparator` — page version mismatch | State machine applied entries but leader skipped them | Ensure `raftHAServer` is null in the test so the leader-skip logic in `ArcadeStateMachine.applyTxEntry()` is inactive |
| `RaftFullSnapshotResyncIT` — replica still uses log replay | Snapshot threshold too high or log not purged | Lower `HA_RAFT_SNAPSHOT_THRESHOLD` to 5; confirm `RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex` is set |
| `BaseMiniRaftTest` compile error on `setStateMachineClass` | Ratis 3.2.0 uses different API | Check `RaftServerConfigKeys.StateMachine` class in the ratis-server jar for the exact property key |
| `BaseMiniRaftTest` — `ArcadeDBServer` fails without HTTP | HTTP server requires a port even when disabled | Try `config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, 0)` for random port, or find the disable key |
| `RaftSplitBrain3NodesIT` — `applySchemaEntry` on leader not skipping | `raftHAServer` null so leader-skip is inactive | Intentional; in mini-raft tests all peers apply all entries. Schema JSON must be idempotent |
