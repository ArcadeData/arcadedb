# 3-Phase Commit Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `RaftReplicatedDatabase.commit()` to release the database read lock during Raft replication, improving concurrent write throughput.

**Architecture:** Split the single `executeInReadLock()` block into three phases: (1) capture WAL under read lock, (2) replicate via Raft with no lock held, (3) apply locally under read lock. On Phase 2 failure, rollback. On Phase 3 failure, step down so a follower with correct state becomes leader.

**Tech Stack:** Java 21, Apache Ratis 3.2.2, JUnit 5, AssertJ

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `ha-raft/src/main/java/.../RaftReplicatedDatabase.java` | Modify (lines 139-214) | Refactor `commit()` into 3-phase pattern, add `ReplicationPayload` record |
| `ha-raft/src/test/java/.../Raft3PhaseCommitIT.java` | Create | Integration test for 3-phase commit correctness and concurrency |

---

### Task 1: Add the `ReplicationPayload` record

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:73`

- [ ] **Step 1: Add the private record inside `RaftReplicatedDatabase`**

Add this record right after the class declaration and before the existing `schemaWalBuffer` field (line 74):

```java
  /**
   * Carries transaction state between Phase 1 (WAL capture under lock) and
   * Phase 2/3 (replication without lock, then local apply under lock).
   */
  private record ReplicationPayload(
      TransactionContext tx,
      TransactionContext.TransactionPhase1 phase1,
      byte[] walData,
      Map<Integer, Integer> bucketDeltas
  ) {}
```

- [ ] **Step 2: Verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
git commit -m "refactor(ha-raft): add ReplicationPayload record for 3-phase commit"
```

---

### Task 2: Refactor `commit()` into 3-phase pattern

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:139-214`

This is the core change. Replace the single `executeInReadLock()` block (lines 172-213) with three separate sections.

- [ ] **Step 1: Write the failing integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Raft3PhaseCommitIT.java`:

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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests that the 3-phase commit works correctly under concurrent load.
 * Verifies that multiple writers can make progress simultaneously
 * (the lock is released during Raft replication).
 */
class Raft3PhaseCommitIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Runs multiple concurrent insert transactions against the leader.
   * With the old single-lock commit, these would serialize completely.
   * With 3-phase commit, Phase 1 and Phase 3 still serialize (read lock),
   * but the Raft replication phase runs without any lock.
   * <p>
   * This test verifies correctness: all records must be present on all replicas.
   */
  @Test
  void concurrentWritersReplicateCorrectly() throws Exception {
    final int leaderIndex = 0;
    executeCommand(leaderIndex, "sql", "CREATE document TYPE ConcurrentDoc");
    waitForReplicationIsCompleted(leaderIndex);

    final int THREADS = 8;
    final int INSERTS_PER_THREAD = 50;
    final AtomicInteger successCount = new AtomicInteger();

    final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    final List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < THREADS; t++) {
      final int threadId = t;
      futures.add(executor.submit(() -> {
        for (int i = 0; i < INSERTS_PER_THREAD; i++) {
          try {
            executeCommand(leaderIndex, "sql",
                "INSERT INTO ConcurrentDoc SET threadId = " + threadId + ", seq = " + i);
            successCount.incrementAndGet();
          } catch (final Exception e) {
            fail("Insert failed: thread=" + threadId + " seq=" + i + " error=" + e.getMessage());
          }
        }
      }));
    }

    for (final Future<?> f : futures)
      f.get(120, TimeUnit.SECONDS);

    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    final int expectedTotal = THREADS * INSERTS_PER_THREAD;
    assertThat(successCount.get()).isEqualTo(expectedTotal);

    // Verify all records replicated to every node
    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++) {
      final JSONObject result = executeCommand(i, "sql", "SELECT count(*) as cnt FROM ConcurrentDoc");
      final long count = result.getJSONObject("result").getJSONArray("records")
          .getJSONObject(0).getLong("cnt");
      assertThat(count)
          .withFailMessage("Server %d has %d records, expected %d", i, count, expectedTotal)
          .isEqualTo(expectedTotal);
    }
  }

  /**
   * Verifies that a basic insert-and-read flow still works after the 3-phase refactor.
   * This is a simple smoke test to catch any regression in the commit path.
   */
  @Test
  void basicInsertReplicates() throws Exception {
    final int leaderIndex = 0;
    executeCommand(leaderIndex, "sql", "CREATE document TYPE BasicDoc");
    waitForReplicationIsCompleted(leaderIndex);

    executeCommand(leaderIndex, "sql", "INSERT INTO BasicDoc SET name = 'test1', value = 42");
    waitForReplicationIsCompleted(leaderIndex);

    for (int i = 0; i < getServerCount(); i++) {
      final JSONObject result = executeCommand(i, "sql", "SELECT FROM BasicDoc WHERE name = 'test1'");
      final int count = result.getJSONObject("result").getJSONArray("records").length();
      assertThat(count)
          .withFailMessage("Server %d should have 1 BasicDoc record but has %d", i, count)
          .isEqualTo(1);
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it passes with the current implementation (baseline)**

Run: `cd ha-raft && mvn test -Dtest=Raft3PhaseCommitIT -pl . -q`
Expected: Both tests PASS (they test correctness, not lock behavior - they should work with either implementation)

- [ ] **Step 3: Refactor `commit()` into 3 phases**

Replace lines 172-213 of `RaftReplicatedDatabase.java` (the second `proxied.executeInReadLock()` block in `commit()`) with:

```java
    // --- PHASE 1 (read lock): capture WAL bytes and delta ---
    final ReplicationPayload payload = proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();
      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        if (phase1 != null) {
          final byte[] walData = phase1.result.toByteArray();
          final Map<Integer, Integer> bucketDeltas = new HashMap<>(tx.getBucketRecordDelta());
          return new ReplicationPayload(tx, phase1, walData, bucketDeltas);
        }

        // Read-only transaction: nothing to replicate.
        tx.reset();
        return null;
      } catch (final NeedRetryException | TransactionException e) {
        rollback();
        throw e;
      } catch (final Exception e) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (phase 1)", e);
      } finally {
        current.popIfNotLastTransaction();
      }
    });

    // Read-only transaction: nothing more to do.
    if (payload == null)
      return;

    // --- REPLICATION (no lock held): send WAL to Raft and wait for quorum ---
    try {
      final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), payload.walData(), payload.bucketDeltas());
      final RaftHAServer raft = requireRaftServer();
      raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
    } catch (final ArcadeDBException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Error on commit distributed transaction (replication)", e);
    }

    // --- PHASE 2 (read lock): quorum reached, apply locally ---
    proxied.executeInReadLock(() -> {
      try {
        payload.tx().commit2ndPhase(payload.phase1());

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed AFTER successful Raft replication (db=%s). "
                + "Stepping down to prevent stale reads. Error: %s", getName(), e.getMessage());
        try {
          if (raftHAServer != null && raftHAServer.isLeader())
            raftHAServer.stepDown();
        } catch (final Exception stepDownEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down after phase 2 failure (db=%s). Manual restart required.", getName());
        }
        throw e;
      }
      return null;
    });
```

Also add the missing `HashMap` import if not already present. Check line 64 - `java.util.*` is already imported, so no additional import needed.

- [ ] **Step 4: Verify compilation**

Run: `cd ha-raft && mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 5: Run the new test**

Run: `cd ha-raft && mvn test -Dtest=Raft3PhaseCommitIT -pl . -q`
Expected: Both tests PASS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/Raft3PhaseCommitIT.java
git commit -m "refactor(ha-raft): split commit() into 3-phase pattern for concurrent write throughput

Phase 1 (read lock): capture WAL bytes and delta
Replication (no lock): submit to Raft group committer, wait for quorum
Phase 2 (read lock): apply pages locally

On Phase 2 failure, leader steps down so a follower with correct state
takes over. The stepped-down node self-heals via Raft log replay."
```

---

### Task 3: Run existing HA tests to verify no regressions

**Files:**
- No changes, verification only

- [ ] **Step 1: Run basic replication tests**

Run: `cd ha-raft && mvn test -Dtest="RaftReplication2NodesIT,RaftReplication3NodesIT" -pl . -q`
Expected: All tests PASS

- [ ] **Step 2: Run schema replication tests**

Run: `cd ha-raft && mvn test -Dtest="RaftReplicationChangeSchemaIT,RaftSchemaReplicationIT" -pl . -q`
Expected: All tests PASS

- [ ] **Step 3: Run concurrent and write-forwarding tests**

Run: `cd ha-raft && mvn test -Dtest="RaftHTTPGraphConcurrentIT,RaftReplicationWriteAgainstReplicaIT" -pl . -q`
Expected: All tests PASS

- [ ] **Step 4: Run crash recovery tests**

Run: `cd ha-raft && mvn test -Dtest="RaftLeaderCrashAndRecoverIT,RaftReplicaCrashAndRecoverIT,RaftLeaderFailoverIT" -pl . -q`
Expected: All tests PASS

- [ ] **Step 5: Run remaining HA integration tests**

Run: `cd ha-raft && mvn test -Dtest="RaftHTTP2ServersIT,RaftHTTP2ServersCreateReplicatedDatabaseIT,RaftIndexOperations3ServersIT,RaftQuorumLostIT,RaftReadConsistencyIT,RaftReadConsistencyBookmarkIT,RaftFullSnapshotResyncIT,RaftIndexCompactionReplicationIT,RaftReplicationMaterializedViewIT" -pl . -q`
Expected: All tests PASS

---

### Task 4: Update the branch comparison doc

**Files:**
- Modify: `docs/ha-branch-comparison.md`

- [ ] **Step 1: Update the comparison doc**

In `docs/ha-branch-comparison.md`, update section 4.1 (Command Forwarding) to note that the 3-phase commit pattern has now been ported. Also update section 9 (Summary) to remove the lock-release pattern as a difference.

Specifically, add a row to the "Shared Features" table in section 3:

```markdown
| 3-phase commit | Phase 1 (lock: capture WAL) -> Replication (no lock) -> Phase 2 (lock: apply locally). Leader steps down on Phase 2 failure |
```

And in section 5 "Features Unique to Each Branch", remove any mention of the 3-phase commit from the `apache-ratis` unique features if present.

- [ ] **Step 2: Commit**

```bash
git add docs/ha-branch-comparison.md
git commit -m "docs: update branch comparison after 3-phase commit port"
```
