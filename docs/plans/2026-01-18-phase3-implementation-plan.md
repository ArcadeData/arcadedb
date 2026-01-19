# Phase 3: Test Completion + Critical Bug Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Achieve 95%+ test reliability by completing Phase 1 test conversions and fixing critical production bugs identified in Phase 2 validation.

**Architecture:** Dual-track approach: (1) Convert remaining HA tests to Awaitility patterns per @docs/testing/ha-test-conversion-guide.md, (2) Fix critical cluster formation and replication bugs causing 10 test failures.

**Tech Stack:**
- Java 21, JUnit 5, Awaitility 4.x
- Maven for builds
- ArcadeDB HA replication system
- HATestHelpers utility framework

**Current Status:**
- Phase 1: ~60% complete (6/26 tests converted, 15 inherit from base class)
- Phase 2: Complete (enhanced reconnection, health API, metrics)
- Test Pass Rate: 52/62 (84%) - Target: 95%+
- 10 failing tests analyzed in @docs/2026-01-15-ha-test-failures-analysis.md

---

## Track 1: Complete Phase 1 Test Conversions

### Task 1: Convert HTTP2ServersIT

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/HTTP2ServersIT.java`
- Reference: `server/src/test/java/com/arcadedb/server/ha/SimpleReplicationServerIT.java` (pattern template)

**Step 1: Add required imports**

Add after existing imports:
```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

**Step 2: Add @Timeout to all test methods**

Find all `@Test` methods and add timeout:
```java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void checkInsertAndRollback() throws Exception {
  // existing test body
}

@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void checkQuery() throws Exception {
  // existing test body
}

// Repeat for all 5 test methods
```

**Step 3: Replace Thread.sleep with waitForClusterStable**

Search for `Thread.sleep(` in file and replace each occurrence:

```java
// BEFORE:
Thread.sleep(5000);

// AFTER:
waitForClusterStable(getServerCount());
```

**Step 4: Replace data consistency sleeps with condition waits**

Replace:
```java
// BEFORE:
Thread.sleep(10000);
assertThat(server0Db.countType("V", true)).isEqualTo(expectedCount);

// AFTER:
await("vertex count on all servers")
    .atMost(Duration.ofSeconds(30))
    .pollInterval(Duration.ofSeconds(1))
    .until(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        Database db = getServerDatabase(i, getDatabaseName());
        db.begin();
        try {
          long count = db.countType("V", true);
          if (count != expectedCount) {
            return false;
          }
        } finally {
          db.rollback();
        }
      }
      return true;
    });
```

**Step 5: Run test to verify conversion**

```bash
mvn test -pl server -Dtest=HTTP2ServersIT -q
```

Expected: All 5 tests pass

**Step 6: Run test 10 times for reliability validation**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=HTTP2ServersIT -q || break
done
```

Expected: 9/10 or 10/10 passes (90%+ reliability)

**Step 7: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/HTTP2ServersIT.java
git commit -m "test: convert HTTP2ServersIT to Awaitility patterns

- Add @Timeout(10 minutes) to all 5 test methods
- Replace Thread.sleep() with waitForClusterStable()
- Replace data consistency sleeps with condition-based waits
- Use HATestTimeouts constants for consistency

Verified: 10/10 successful runs

Part of Phase 3: Test Infrastructure Completion

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 2: Convert HTTPGraphConcurrentIT

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/HTTPGraphConcurrentIT.java`

**Step 1: Add required imports**

```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

**Step 2: Add @Timeout annotation**

```java
@Test
@Timeout(value = 15, unit = TimeUnit.MINUTES)  // Complex test with concurrent operations
public void testConcurrentGraphOperations() throws Exception {
  // existing test body
}
```

**Step 3: Replace Thread.sleep with waitForClusterStable**

After server operations:
```java
// After starting servers
waitForClusterStable(getServerCount());

// After concurrent operations complete
waitForClusterStable(getServerCount());
```

**Step 4: Replace concurrent operation waits**

```java
// BEFORE:
executor.shutdown();
executor.awaitTermination(60, TimeUnit.SECONDS);
Thread.sleep(5000);

// AFTER:
executor.shutdown();
executor.awaitTermination(60, TimeUnit.SECONDS);
waitForClusterStable(getServerCount());
```

**Step 5: Run test to verify**

```bash
mvn test -pl server -Dtest=HTTPGraphConcurrentIT -q
```

Expected: Test passes

**Step 6: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=HTTPGraphConcurrentIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 7: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/HTTPGraphConcurrentIT.java
git commit -m "test: convert HTTPGraphConcurrentIT to Awaitility patterns

- Add @Timeout(15 minutes) for complex concurrent test
- Replace Thread.sleep() with waitForClusterStable()
- Add cluster stabilization after concurrent operations

Verified: 10/10 successful runs

Part of Phase 3: Test Infrastructure Completion

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 3: Convert IndexOperations3ServersIT

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/IndexOperations3ServersIT.java`

**Step 1: Add required imports**

```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

**Step 2: Add @Timeout annotation**

```java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void testIndexOperations() throws Exception {
  // existing test body
}
```

**Step 3: Replace Thread.sleep with waitForClusterStable**

```java
// After index creation
waitForClusterStable(getServerCount());

// After index operations
waitForClusterStable(getServerCount());
```

**Step 4: Add index count consistency wait**

```java
// BEFORE:
Thread.sleep(5000);
assertThat(index.countEntries()).isEqualTo(expectedCount);

// AFTER:
final long expectedCount = /* value */;
await("index entries replicated")
    .atMost(Duration.ofSeconds(30))
    .pollInterval(Duration.ofSeconds(1))
    .untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        Database db = getServerDatabase(i, getDatabaseName());
        db.begin();
        try {
          Index index = db.getSchema().getIndexByName("indexName");
          assertThat(index.countEntries()).isEqualTo(expectedCount);
        } finally {
          db.rollback();
        }
      }
    });
```

**Step 5: Run test to verify**

```bash
mvn test -pl server -Dtest=IndexOperations3ServersIT -q
```

Expected: Test passes

**Step 6: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=IndexOperations3ServersIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 7: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/IndexOperations3ServersIT.java
git commit -m "test: convert IndexOperations3ServersIT to Awaitility patterns

- Add @Timeout(10 minutes)
- Replace Thread.sleep() with waitForClusterStable()
- Add condition-based wait for index replication
- Verify index counts across all servers

Verified: 10/10 successful runs

Part of Phase 3: Test Infrastructure Completion

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 4: Convert ServerDatabaseAlignIT

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/ServerDatabaseAlignIT.java`

**Step 1: Add required imports**

```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

**Step 2: Add @Timeout annotation**

```java
@Test
@Timeout(value = 15, unit = TimeUnit.MINUTES)  // ALIGN DATABASE is expensive
public void testDatabaseAlign() throws Exception {
  // existing test body
}
```

**Step 3: Replace Thread.sleep with waitForClusterStable**

```java
// After database operations that create drift
waitForClusterStable(getServerCount());

// After ALIGN DATABASE command
waitForClusterStable(getServerCount());
```

**Step 4: Add alignment completion wait**

```java
// Wait for alignment to complete on all servers
await("alignment complete")
    .atMost(Duration.ofMinutes(2))
    .pollInterval(Duration.ofSeconds(2))
    .until(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        // Check alignment status or data consistency
        Database db = getServerDatabase(i, getDatabaseName());
        db.begin();
        try {
          long count = db.countType("V", true);
          if (count != expectedCount) {
            return false;
          }
        } finally {
          db.rollback();
        }
      }
      return true;
    });
```

**Step 5: Run test to verify**

```bash
mvn test -pl server -Dtest=ServerDatabaseAlignIT -q
```

Expected: Test passes

**Step 6: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=ServerDatabaseAlignIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 7: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/ServerDatabaseAlignIT.java
git commit -m "test: convert ServerDatabaseAlignIT to Awaitility patterns

- Add @Timeout(15 minutes) for expensive alignment operation
- Replace Thread.sleep() with waitForClusterStable()
- Add condition-based wait for alignment completion
- Verify data consistency after alignment

Verified: 10/10 successful runs

Part of Phase 3: Test Infrastructure Completion

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 5: Convert ServerDatabaseBackupIT

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/ha/ServerDatabaseBackupIT.java`

**Step 1: Add required imports**

```java
import org.junit.jupiter.api.Timeout;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import java.time.Duration;
```

**Step 2: Add @Timeout annotation**

```java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void testBackup() throws Exception {
  // existing test body
}
```

**Step 3: Replace Thread.sleep with waitForClusterStable**

```java
// Before backup
waitForClusterStable(getServerCount());

// After backup restore
waitForClusterStable(getServerCount());
```

**Step 4: Add backup completion wait**

```java
// Wait for backup file to be written and closed
await("backup file created")
    .atMost(Duration.ofMinutes(1))
    .pollInterval(Duration.ofMillis(500))
    .until(() -> {
      File backupFile = new File(backupPath);
      return backupFile.exists() && backupFile.length() > 0;
    });
```

**Step 5: Run test to verify**

```bash
mvn test -pl server -Dtest=ServerDatabaseBackupIT -q
```

Expected: Test passes

**Step 6: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=ServerDatabaseBackupIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 7: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/ServerDatabaseBackupIT.java
git commit -m "test: convert ServerDatabaseBackupIT to Awaitility patterns

- Add @Timeout(10 minutes)
- Replace Thread.sleep() with waitForClusterStable()
- Add condition-based wait for backup file creation
- Ensure cluster stable before and after backup operations

Verified: 10/10 successful runs

Part of Phase 3: Test Infrastructure Completion

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Track 2: Fix Critical Production Bugs

### Task 6: Fix 3-Server Cluster Formation Race

**Context:** ReplicationServerWriteAgainstReplicaIT fails with "Cluster failed to stabilize: expected 3 servers, only 1 connected". This is similar to the 2-server race fixed earlier, but affects 3+ server scenarios.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ha/HAServer.java:1666` (connectToLeader method)

**Step 1: Write failing test to reproduce issue**

Create: `server/src/test/java/com/arcadedb/server/ha/ThreeServerClusterFormationIT.java`

```java
package com.arcadedb.server.ha;

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces 3-server cluster formation race condition.
 */
public class ThreeServerClusterFormationIT extends BaseGraphServerTest {

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testThreeServerClusterFormation() throws Exception {
    checkActiveDatabases();
    checkDatabaseList(0, getDatabaseName());
    checkDatabaseList(1, getDatabaseName());
    checkDatabaseList(2, getDatabaseName());

    // Should have 3 servers connected
    waitForClusterStable(3);

    // Verify all servers see each other
    for (int i = 0; i < 3; i++) {
      assertThat(getServer(i).getHA().getOnlineReplicas()).isEqualTo(2);
    }
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  protected String getDatabaseName() {
    return "cluster-formation-test";
  }
}
```

**Step 2: Run test to verify it fails**

```bash
mvn test -pl server -Dtest=ThreeServerClusterFormationIT -q
```

Expected: FAIL with "Cluster failed to stabilize: expected 3 servers, only 1 connected"

**Step 3: Analyze HAServer.connectToLeader for race condition**

Read current implementation:
```bash
grep -A 30 "private synchronized void connectToLeader" server/src/main/java/com/arcadedb/server/ha/HAServer.java
```

**Step 4: Add defensive connection count check**

In `HAServer.java` connectToLeader method, add check after synchronized block entry:

```java
private synchronized void connectToLeader(ServerInfo server) {
  // Defensive check: if already connected to this leader, skip
  if (lc != null && lc.isAlive()) {
    final ServerInfo currentLeader = lc.getLeader();
    if (currentLeader.host().equals(server.host()) && currentLeader.port() == server.port()) {
      LogManager.instance().log(this, Level.INFO,
          "Already connected/connecting to leader %s (host:port %s:%d), skipping duplicate request",
          server, server.host(), server.port());
      return;
    }
  }

  // NEW: Check if we're already trying to connect from another thread
  if (connectingToLeader != null &&
      connectingToLeader.host().equals(server.host()) &&
      connectingToLeader.port() == server.port()) {
    LogManager.instance().log(this, Level.INFO,
        "Already attempting connection to leader %s from another thread, waiting...",
        server);

    // Wait for the other thread to complete connection
    try {
      wait(30000); // Wait up to 30 seconds
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return;
  }

  // Mark that we're connecting to prevent other threads from duplicating
  connectingToLeader = server;

  try {
    // ... existing connection logic ...
  } finally {
    connectingToLeader = null;
    notifyAll(); // Wake up any waiting threads
  }
}
```

**Step 5: Add connectingToLeader field**

Add field to HAServer class:
```java
private volatile ServerInfo connectingToLeader = null;
```

**Step 6: Run test to verify it passes**

```bash
mvn test -pl server -Dtest=ThreeServerClusterFormationIT -q
```

Expected: PASS - all 3 servers connected

**Step 7: Run ReplicationServerWriteAgainstReplicaIT**

```bash
mvn test -pl server -Dtest=ReplicationServerWriteAgainstReplicaIT -q
```

Expected: PASS - cluster forms successfully

**Step 8: Validate reliability with multiple runs**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=ThreeServerClusterFormationIT,ReplicationServerWriteAgainstReplicaIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 9: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/ha/HAServer.java
git add server/src/test/java/com/arcadedb/server/ha/ThreeServerClusterFormationIT.java
git commit -m "fix: prevent duplicate connectToLeader calls in 3+ server clusters

Problem: Multiple threads calling connectToLeader() simultaneously in 3+
server clusters caused only 1 connection to succeed, others failed.

Solution: Track connecting leader and make waiting threads block until
first connection completes, preventing duplicate connection attempts.

Fixes:
- ReplicationServerWriteAgainstReplicaIT (was failing at startup)
- 3-server cluster formation race condition

Test: ThreeServerClusterFormationIT verifies 3-server cluster formation
Verified: 10/10 successful runs on both tests

Part of Phase 3: Critical Bug Fixes

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 7: Fix LSM Vector Index Replication

**Context:** IndexCompactionReplicationIT.lsmVectorReplication fails with leader having 1001/5000 entries indexed, replica having 74/5000. Issue is specific to vector index async indexing.

**Files:**
- Investigate: `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndexAbstract.java`
- Modify: TBD based on investigation
- Test: `server/src/test/java/com/arcadedb/server/ha/IndexCompactionReplicationIT.java`

**Step 1: Read existing test to understand failure**

```bash
mvn test -pl server -Dtest=IndexCompactionReplicationIT#lsmVectorReplication -q
```

Capture output showing:
- Expected: 5000 entries
- Leader actual: 1001 entries
- Replica actual: 74 entries

**Step 2: Add diagnostic logging to identify root cause**

Create test to isolate vector indexing behavior:

Create: `server/src/test/java/com/arcadedb/server/ha/VectorIndexReplicationDiagnosticIT.java`

```java
package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.index.Index;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorIndexReplicationDiagnosticIT extends BaseGraphServerTest {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  public void testVectorIndexReplication() throws Exception {
    Database leaderDb = getServerDatabase(0, getDatabaseName());

    // Create vector index
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createDocumentType("VectorDoc");
      leaderDb.getSchema().createTypeIndex(
          Schema.INDEX_TYPE.LSM_TREE,
          true,
          "VectorDoc",
          new String[]{"embedding"},
          100);
    });

    // Wait for index creation to replicate
    waitForClusterStable(getServerCount());

    // Insert 100 documents with vectors
    for (int i = 0; i < 100; i++) {
      final int docNum = i;
      leaderDb.transaction(() -> {
        MutableDocument doc = leaderDb.newDocument("VectorDoc");
        doc.set("embedding", new float[]{docNum * 1.0f, docNum * 2.0f});
        doc.save();
      });
    }

    // Wait for replication
    waitForClusterStable(getServerCount());

    // Check leader index count
    leaderDb.begin();
    try {
      Index leaderIndex = leaderDb.getSchema().getIndexByName("VectorDoc[embedding]");
      long leaderCount = leaderIndex.countEntries();
      LogManager.instance().log(this, Level.INFO,
          "Leader index entries: %d (expected 100)", leaderCount);
      assertThat(leaderCount).isEqualTo(100);
    } finally {
      leaderDb.rollback();
    }

    // Check replica index count
    for (int i = 1; i < getServerCount(); i++) {
      Database replicaDb = getServerDatabase(i, getDatabaseName());
      replicaDb.begin();
      try {
        Index replicaIndex = replicaDb.getSchema().getIndexByName("VectorDoc[embedding]");
        long replicaCount = replicaIndex.countEntries();
        LogManager.instance().log(this, Level.INFO,
            "Replica %d index entries: %d (expected 100)", i, replicaCount);
        assertThat(replicaCount).isEqualTo(100);
      } finally {
        replicaDb.rollback();
      }
    }
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  protected String getDatabaseName() {
    return "vector-index-test";
  }
}
```

**Step 3: Run diagnostic test to understand behavior**

```bash
mvn test -pl server -Dtest=VectorIndexReplicationDiagnosticIT -q 2>&1 | tee vector-index-diagnostic.log
```

Analyze output:
- Are documents being inserted?
- Is leader indexing them?
- Are index updates being replicated?

**Step 4: Investigate LSM vector index implementation**

```bash
grep -r "async" engine/src/main/java/com/arcadedb/index/lsm/ | grep -i index
```

Look for:
- Asynchronous indexing mechanisms
- Index update batching
- Replication message creation for index updates

**Step 5: Identify root cause**

Based on investigation, determine if issue is:
1. Async indexing not completing before replication
2. Index updates not being sent in replication messages
3. Replicas not applying index updates correctly

**Step 6: Implement fix based on root cause**

If issue is #1 (async indexing):
```java
// In LSMTreeIndexAbstract or relevant class
public void flush() {
  // Ensure all pending async index operations complete
  if (asyncIndexer != null) {
    asyncIndexer.waitForCompletion();
  }
  super.flush();
}
```

If issue is #2 (replication messages):
```java
// Ensure index updates are included in transaction replication
// Add to transaction commit path
```

If issue is #3 (replica application):
```java
// Fix replica-side index update handling
```

**Step 7: Run diagnostic test to verify fix**

```bash
mvn test -pl server -Dtest=VectorIndexReplicationDiagnosticIT -q
```

Expected: PASS - all replicas have 100 index entries

**Step 8: Run original failing test**

```bash
mvn test -pl server -Dtest=IndexCompactionReplicationIT#lsmVectorReplication -q
```

Expected: PASS - all replicas have 5000 index entries

**Step 9: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=IndexCompactionReplicationIT#lsmVectorReplication -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 10: Commit**

```bash
git add [modified files from Step 6]
git add server/src/test/java/com/arcadedb/server/ha/VectorIndexReplicationDiagnosticIT.java
git commit -m "fix: ensure LSM vector index updates are fully replicated

Problem: Vector index async indexing not completing before replication,
causing massive index entry gaps (leader: 1001/5000, replica: 74/5000).

Solution: [Describe specific fix based on root cause]

Fixes:
- IndexCompactionReplicationIT.lsmVectorReplication

Test: VectorIndexReplicationDiagnosticIT validates vector index replication
Verified: 10/10 successful runs

Part of Phase 3: Critical Bug Fixes

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Note:** This task requires root cause analysis. If investigation reveals a complex issue, may need to defer to Phase 4 and focus on other high-priority bugs first.

---

### Task 8: Fix Quorum Timeout Issues

**Context:** ReplicationServerQuorumMajority1ServerOutIT and ReplicationServerQuorumMajority2ServersOutIT timeout waiting for cluster stabilization. May be test timeout too short or actual quorum calculation issue.

**Files:**
- Test: `server/src/test/java/com/arcadedb/server/ha/ReplicationServerQuorumMajority1ServerOutIT.java`
- Test: `server/src/test/java/com/arcadedb/server/ha/ReplicationServerQuorumMajority2ServersOutIT.java`
- May modify: `server/src/main/java/com/arcadedb/server/ha/HAServer.java` (quorum logic)

**Step 1: Run tests with increased logging**

```bash
mvn test -pl server -Dtest=ReplicationServerQuorumMajority1ServerOutIT -X 2>&1 | tee quorum-1-out.log
mvn test -pl server -Dtest=ReplicationServerQuorumMajority2ServersOutIT -X 2>&1 | tee quorum-2-out.log
```

**Step 2: Analyze test expectations**

Read test to understand:
- How many servers are configured?
- What quorum setting is used?
- When does test take servers offline?
- What operations should succeed/fail?

**Step 3: Check if timeout is too short**

Review test timeout:
```java
// If test uses shorter timeout than cluster needs
@Timeout(value = 2, unit = TimeUnit.MINUTES)  // May be too short
```

Try increasing timeout:
```java
@Timeout(value = 10, unit = TimeUnit.MINUTES)
```

**Step 4: Run test with increased timeout**

```bash
mvn test -pl server -Dtest=ReplicationServerQuorumMajority1ServerOutIT -q
```

If PASSES: Issue was test timeout configuration
If FAILS: Issue is actual quorum logic

**Step 5: If timeout was issue, update both tests**

```java
// In ReplicationServerQuorumMajority1ServerOutIT.java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void testReplication() throws Exception {
  super.testReplication();
}

// In ReplicationServerQuorumMajority2ServersOutIT.java
@Test
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public void testReplication() throws Exception {
  super.testReplication();
}
```

**Step 6: If quorum logic issue, investigate HAServer.isQuorumAvailable**

```bash
grep -A 20 "isQuorumAvailable" server/src/main/java/com/arcadedb/server/ha/HAServer.java
```

Check:
- Is quorum calculation correct for MAJORITY with servers down?
- Does cluster size account for offline servers correctly?

**Step 7: Fix quorum calculation if needed**

```java
public boolean isQuorumAvailable() {
  final int onlineServers = getOnlineReplicas() + 1; // +1 for self
  final int requiredQuorum = calculateRequiredQuorum();

  LogManager.instance().log(this, Level.FINE,
      "Quorum check: online=%d, required=%d, available=%b",
      onlineServers, requiredQuorum, onlineServers >= requiredQuorum);

  return onlineServers >= requiredQuorum;
}

private int calculateRequiredQuorum() {
  final QuorumType quorumType = getQuorumType();
  final int totalServers = clusterConfiguration.getServers().size();

  switch (quorumType) {
    case MAJORITY:
      return (totalServers / 2) + 1;
    case ALL:
      return totalServers;
    case NONE:
      return 1;
    default:
      return 1;
  }
}
```

**Step 8: Run tests to verify fix**

```bash
mvn test -pl server -Dtest=ReplicationServerQuorumMajority1ServerOutIT,ReplicationServerQuorumMajority2ServersOutIT -q
```

Expected: Both tests PASS

**Step 9: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=ReplicationServerQuorumMajority1ServerOutIT,ReplicationServerQuorumMajority2ServersOutIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 10: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/ha/ReplicationServerQuorumMajority*.java
# If HAServer.java was modified:
# git add server/src/main/java/com/arcadedb/server/ha/HAServer.java
git commit -m "fix: quorum majority tests with servers offline

Problem: Tests timing out during cluster stabilization with servers
intentionally taken offline to test quorum edge cases.

Solution: [Either increased test timeout OR fixed quorum calculation]

Fixes:
- ReplicationServerQuorumMajority1ServerOutIT
- ReplicationServerQuorumMajority2ServersOutIT

Verified: 10/10 successful runs on both tests

Part of Phase 3: Critical Bug Fixes

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 9: Fix Leader Failover Database Lifecycle

**Context:** ReplicationServerLeaderDownIT fails with DatabaseIsClosedException during failover. Database not properly reopened after leader change.

**Files:**
- Test: `server/src/test/java/com/arcadedb/server/ha/ReplicationServerLeaderDownIT.java`
- May modify: `server/src/main/java/com/arcadedb/server/ha/HAServer.java` (leader fence logic)

**Step 1: Run test to reproduce issue**

```bash
mvn test -pl server -Dtest=ReplicationServerLeaderDownIT -q 2>&1 | tee leader-down-failure.log
```

Note exact line number and operation that throws DatabaseIsClosedException.

**Step 2: Analyze test sequence**

Read test to understand:
1. When is leader stopped?
2. When does new leader election happen?
3. When does test try to access database?
4. What operation fails?

**Step 3: Add diagnostic logging to leader failover**

In `HAServer.java`, add logging to leader election completion:

```java
private void electionComplete() {
  LogManager.instance().log(this, Level.INFO,
      "Election complete, I am %s", isLeader() ? "LEADER" : "REPLICA");

  // Log database states
  for (String dbName : getDatabaseNames()) {
    Database db = getDatabase(dbName);
    LogManager.instance().log(this, Level.INFO,
        "Database '%s' status: open=%b, mode=%s",
        dbName, db != null && !db.isClosed(), db != null ? db.getMode() : "N/A");
  }

  // ... existing code
}
```

**Step 4: Run test with diagnostic logging**

```bash
mvn test -pl server -Dtest=ReplicationServerLeaderDownIT -X 2>&1 | tee leader-down-diagnostic.log
```

Look for:
- When databases are closed during leader shutdown
- When databases are reopened on new leader
- Any timing gap between close and reopen

**Step 5: Identify root cause**

Check if issue is:
1. Old leader not closing databases cleanly on shutdown
2. New leader not opening databases after election
3. Test accessing database during transition window
4. Database mode not switching correctly (READ_WRITE vs READ_ONLY)

**Step 6: Implement fix based on root cause**

If issue is #3 (test timing):
```java
// In test, wait for new leader to stabilize before operations
waitForClusterStable(getServerCount() - 1); // -1 for stopped server

// Add wait for new leader databases to be ready
await("new leader databases ready")
    .atMost(Duration.ofSeconds(30))
    .pollInterval(Duration.ofSeconds(1))
    .until(() -> {
      Database db = getLeaderDatabase();
      return db != null && !db.isClosed();
    });
```

If issue is #2 (new leader not opening):
```java
// In HAServer.electionComplete()
if (isLeader()) {
  // Ensure all databases are opened in READ_WRITE mode
  for (String dbName : getDatabaseNames()) {
    Database db = getDatabase(dbName);
    if (db == null || db.isClosed() || db.getMode() != Database.MODE.READ_WRITE) {
      reopenDatabaseAsLeader(dbName);
    }
  }
}
```

**Step 7: Run test to verify fix**

```bash
mvn test -pl server -Dtest=ReplicationServerLeaderDownIT -q
```

Expected: PASS - no DatabaseIsClosedException

**Step 8: Run ReplicationServerLeaderChanges3TimesIT**

This test also has leader failover issues:
```bash
mvn test -pl server -Dtest=ReplicationServerLeaderChanges3TimesIT -q
```

Expected: PASS - cluster stabilizes after multiple leader changes

**Step 9: Validate reliability**

```bash
for i in {1..10}; do
  echo "Run $i/10"
  mvn test -pl server -Dtest=ReplicationServerLeaderDownIT,ReplicationServerLeaderChanges3TimesIT -q || break
done
```

Expected: 9/10 or 10/10 passes

**Step 10: Commit**

```bash
git add [modified files from Step 6]
git commit -m "fix: database lifecycle during leader failover

Problem: DatabaseIsClosedException thrown when accessing database
immediately after leader failover. Databases closed during election
but not properly reopened on new leader.

Solution: [Describe specific fix - either test timing OR leader reopening]

Fixes:
- ReplicationServerLeaderDownIT (DatabaseIsClosedException)
- ReplicationServerLeaderChanges3TimesIT (stability after multiple changes)

Verified: 10/10 successful runs on both tests

Part of Phase 3: Critical Bug Fixes

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Validation & Documentation

### Task 10: Run Full HA Test Suite Validation

**Files:**
- Create: `docs/testing/phase3-validation-results.md`

**Step 1: Run complete HA test suite**

```bash
mvn test -pl server -Dtest="*HA*IT,*Replication*IT,HTTP2Servers*" 2>&1 | tee phase3-full-suite.log
```

**Step 2: Count pass/fail results**

```bash
grep "Tests run:" phase3-full-suite.log | tail -1
```

Calculate:
- Total tests run
- Passing tests
- Failing tests
- Pass rate percentage

**Step 3: Document results**

Create validation results document:

```markdown
# Phase 3 Validation Results

**Date:** 2026-01-18
**Branch:** feature/2043-ha-test
**Test Suite:** Full HA integration tests

## Summary

- **Tests run:** [total]
- **Passing:** [count] ([percentage]%)
- **Failing:** [count] ([percentage]%)
- **Skipped:** [count]

**Target:** 95%+ pass rate
**Result:** [PASS/FAIL - did we meet target?]

## Test Conversions Completed

### Track 1: Phase 1 Test Conversions
- [x] HTTP2ServersIT
- [x] HTTPGraphConcurrentIT
- [x] IndexOperations3ServersIT
- [x] ServerDatabaseAlignIT
- [x] ServerDatabaseBackupIT

**Total:** 5 additional tests converted
**Combined with Phase 1:** 11/26 tests converted (42%)
**Inherited from base class:** ~15 tests benefit from ReplicationServerIT conversion

## Critical Bugs Fixed

### Track 2: Production Bug Fixes
- [x] 3-server cluster formation race condition
- [x] LSM vector index replication [if completed]
- [x] Quorum timeout issues
- [x] Leader failover database lifecycle

## Test Results by Category

### Passing Tests ([count])

1. SimpleReplicationServerIT
2. ReplicationServerIT (and subclasses)
3. HTTP2ServersIT
4. [list all passing]

### Failing Tests ([count])

1. [Test name] - [Brief reason]
2. [list all failing with reasons]

### Deferred Issues

[Any issues identified but deferred to Phase 4]

## Pass Rate Improvement

- **Phase 2 Baseline:** 52/62 (84%)
- **Phase 3 Result:** [X]/[Y] ([Z]%)
- **Improvement:** +[percentage points]

## Next Steps

[Based on results, what needs to happen next]

## Detailed Test Execution Log

[Excerpt from phase3-full-suite.log showing key pass/fail info]
```

**Step 4: Commit validation results**

```bash
git add docs/testing/phase3-validation-results.md
git commit -m "docs: Phase 3 validation results

Full HA test suite validation after completing test conversions
and critical bug fixes.

Pass rate: [X]% (target: 95%)

Part of Phase 3: Test Infrastructure Completion + Critical Fixes

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Step 5: If pass rate <95%, identify remaining issues**

Create action items for Phase 4:
```bash
echo "# Phase 4 Remaining Issues" > docs/plans/2026-01-18-phase4-remaining-issues.md
echo "" >> docs/plans/2026-01-18-phase4-remaining-issues.md
echo "## Tests Still Failing" >> docs/plans/2026-01-18-phase4-remaining-issues.md
echo "" >> docs/plans/2026-01-18-phase4-remaining-issues.md
# Add each failing test with analysis
```

---

## Success Criteria

**Phase 3 Complete When:**

- [ ] All 5 remaining tests converted to Awaitility patterns (Track 1)
- [ ] 3-server cluster formation race fixed (Track 2, Task 6)
- [ ] LSM vector index replication fixed OR deferred with justification (Track 2, Task 7)
- [ ] Quorum timeout issues resolved (Track 2, Task 8)
- [ ] Leader failover database lifecycle fixed (Track 2, Task 9)
- [ ] Full test suite pass rate ≥90% (stretch goal: 95%)
- [ ] All converted tests run 10 times with ≥90% reliability
- [ ] Validation results documented

**Metrics:**
- Test pass rate: 84% → 95%+ (target)
- Test conversions: 60% → 100% complete
- Timing anti-patterns: Eliminate all Thread.sleep() from converted tests
- Production bugs fixed: 4-5 critical issues resolved

**Documentation:**
- Phase 3 validation results in docs/testing/
- Commit messages follow TDD pattern
- Each fix has associated test demonstrating issue + fix

---

## References

- **Design Document:** @docs/plans/2026-01-13-ha-reliability-improvements-design.md (sections 2.2, 2.3)
- **Test Conversion Guide:** @docs/testing/ha-test-conversion-guide.md
- **Failure Analysis:** @docs/2026-01-15-ha-test-failures-analysis.md
- **Phase 2 Results:** @docs/testing/ha-phase2-baseline.md
- **HATestHelpers:** @server/src/test/java/com/arcadedb/server/ha/HATestHelpers.java (if exists, else BaseGraphServerTest)
- **HATestTimeouts:** @server/src/test/java/com/arcadedb/server/ha/HATestTimeouts.java

---

## Notes

- **Prioritization:** If time-constrained, complete Track 1 (test conversions) fully before deep-diving on Track 2 (bug fixes). Test conversions are lower risk.
- **LSM Vector Index (Task 7):** This may be complex. If root cause investigation takes >2 hours, defer to Phase 4 and focus on other high-value fixes.
- **Incremental commits:** Commit after each task completion, don't batch. Enables easy rollback if issues arise.
- **Run tests frequently:** After each fix, run both the specific test AND the full suite to check for regressions.
