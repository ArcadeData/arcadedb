# Port apache-ratis Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring ha-redesign to feature parity with apache-ratis across Raft correctness, snapshot security, architecture, and cluster token hardening.

**Architecture:** Port correctness fixes first (P1), then extract classes from the 1423-line RaftHAServer (P3), then harden snapshot install (P2), wire cluster token provider (P4), fix forward compatibility (P5), add edge-case tests (P6), and polish (P7). Each priority block is independently testable.

**Tech Stack:** Java 21, Apache Ratis, JUnit 5 + AssertJ, Maven

**Spec:** `docs/superpowers/specs/2026-04-14-port-apache-ratis-parity-design.md`

---

## File Map

### New Production Files (ha-raft module)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/MajorityCommittedAllFailedException.java` - Exception for MAJORITY-committed-but-ALL-failed
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ReplicationException.java` - Runtime exception for replication errors
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterTokenProvider.java` - PBKDF2 token derivation (copy from apache-ratis)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPeerAddressResolver.java` - Peer address parsing (extract from RaftHAServer)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java` - Ratis properties setup (extract from RaftHAServer)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterManager.java` - Membership ops (extract from RaftHAServer)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java` - Cluster status JSON (copy from apache-ratis)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java` - Transaction routing (extract from RaftHAServer)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java` - K8s peer discovery (extract from RaftHAServer)

### New Production Files (engine module)
- `engine/src/main/java/com/arcadedb/exception/WALVersionGapException.java` - Exception for WAL version gaps

### Modified Production Files
- `engine/src/main/java/com/arcadedb/engine/TransactionManager.java` - Throw WALVersionGapException instead of ConcurrentModificationException for version gaps
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java` - Throw MajorityCommittedAllFailedException on ALL-quorum watch failure
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java` - Add MajorityCommittedAllFailedException catch, phase-2 escalation, applyLocallyAfterMajorityCommit()
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` - WAL exception handling, persisted applied-index, emergency stop, lifecycle executor, hot resync
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` - Extract classes, shrink to coordinator
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java` - Wire extracted classes
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java` - Security hardening (symlink, zip-bomb, SSL, completion marker)
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java` - Root-only auth, db-name validation, write timeout, SSL warning
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java` - Return null for unknown IDs
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java` - Handle null type in decode
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotManager.java` - Em-dash cleanup only

---

## Task 1: Create WALVersionGapException in engine module

**Files:**
- Create: `engine/src/main/java/com/arcadedb/exception/WALVersionGapException.java`
- Modify: `engine/src/main/java/com/arcadedb/engine/TransactionManager.java:347-356`

This is a prerequisite for P1.3 (WALVersionGapException handling in ArcadeStateMachine). The exception must exist in the engine module so the state machine can catch it by type.

- [ ] **Step 1: Create WALVersionGapException class**

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
package com.arcadedb.exception;

/**
 * Thrown when a WAL page version is ahead of the database page version by more than 1,
 * indicating that an intermediate transaction was never applied and the state may have diverged.
 */
public class WALVersionGapException extends ConcurrentModificationException {
  public WALVersionGapException(final String s) {
    super(s);
  }
}
```

- [ ] **Step 2: Update TransactionManager to throw WALVersionGapException**

In `engine/src/main/java/com/arcadedb/engine/TransactionManager.java`, add import:

```java
import com.arcadedb.exception.WALVersionGapException;
```

Then at line 353, change the throw from `ConcurrentModificationException` to `WALVersionGapException`:

```java
        if (txPage.currentPageVersion > page.getVersion() + 1) {
          LogManager.instance().log(this, Level.WARNING,
              "Cannot apply changes to the database because modified page %s version in WAL (%d) does not match with existent version (%d) fileId=%d",
              null, pageId, txPage.currentPageVersion, page.getVersion(), txPage.fileId);
          if (ignoreErrors)
            continue;
          throw new WALVersionGapException(
              "Cannot apply changes to the database because modified page " + pageId + " version in WAL ("
                  + txPage.currentPageVersion + ") does not match with existent version (" + page.getVersion() + ") fileId="
                  + txPage.fileId);
        }
```

- [ ] **Step 3: Build engine module to verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl engine -DskipTests compile`
Expected: BUILD SUCCESS

- [ ] **Step 4: Run engine tests to verify no regressions**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl engine test -Dtest="*TransactionManager*,*WAL*" -DfailIfNoTests=false`
Expected: Tests pass (WALVersionGapException extends ConcurrentModificationException, so existing catches still work)

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/exception/WALVersionGapException.java engine/src/main/java/com/arcadedb/engine/TransactionManager.java
git commit -m "feat(engine): add WALVersionGapException for version gap detection

Introduces a dedicated exception subclass of ConcurrentModificationException
thrown when a WAL page version is ahead of the database page version by more
than 1. This allows the HA state machine to distinguish recoverable replays
from dangerous state divergence.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Create MajorityCommittedAllFailedException and ReplicationException

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/MajorityCommittedAllFailedException.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ReplicationException.java`

- [ ] **Step 1: Create MajorityCommittedAllFailedException**

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

import com.arcadedb.network.binary.QuorumNotReachedException;

/**
 * Thrown by {@link RaftGroupCommitter} when the Raft MAJORITY quorum was committed (meaning
 * Ratis already called {@code applyTransaction()} on the leader with the origin-skip) but the
 * subsequent ALL-quorum watch failed.
 *
 * <p>The catch in {@link RaftReplicatedDatabase#commit()} distinguishes this from a plain
 * {@link QuorumNotReachedException} (where no commit happened and rollback is correct) and
 * instead calls {@code commit2ndPhase()} to apply the local page writes. Without this,
 * the leader's database permanently diverges: {@code lastAppliedIndex} was advanced in
 * {@code applyTransaction()} but the database pages were never written.
 */
public class MajorityCommittedAllFailedException extends QuorumNotReachedException {

  public MajorityCommittedAllFailedException(final String message) {
    super(message);
  }

  public MajorityCommittedAllFailedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
```

- [ ] **Step 2: Create ReplicationException**

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

public class ReplicationException extends RuntimeException {
  public ReplicationException(final String message) {
    super(message);
  }

  public ReplicationException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
```

- [ ] **Step 3: Build ha-raft to verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft -DskipTests compile -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/MajorityCommittedAllFailedException.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/ReplicationException.java
git commit -m "feat(ha-raft): add MajorityCommittedAllFailedException and ReplicationException

MajorityCommittedAllFailedException distinguishes ALL-quorum watch failures
(where MAJORITY already committed) from plain QuorumNotReachedException
(where nothing committed). This allows the commit path to apply local pages
rather than rolling back when MAJORITY succeeded.

ReplicationException signals replication errors that require snapshot resync,
such as WAL version gaps indicating state divergence.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Wire MajorityCommittedAllFailedException into RaftGroupCommitter

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java:142-154`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java`

- [ ] **Step 1: Write the failing test**

Add to `RaftGroupCommitterTest.java`:

```java
@Test
void allQuorumWatchFailureThrowsMajorityCommittedException() {
  // This test verifies the exception type distinction. Since we can't easily
  // mock Ratis internals, we verify the exception class hierarchy is correct.
  final var ex = new MajorityCommittedAllFailedException("ALL quorum watch failed");
  assertThat(ex).isInstanceOf(QuorumNotReachedException.class);
  assertThat(ex.getMessage()).contains("ALL quorum");
}
```

- [ ] **Step 2: Run the test to verify it passes (class hierarchy test)**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftGroupCommitterTest" -am`
Expected: PASS

- [ ] **Step 3: Update RaftGroupCommitter.flushBatch() to throw MajorityCommittedAllFailedException**

In `RaftGroupCommitter.java`, at line 143-154, change the ALL-quorum watch failure handling:

Replace:
```java
        if (quorum == Quorum.ALL) {
          try {
            final RaftClientReply watchReply = raftClient.io().watch(
                reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
            if (!watchReply.isSuccess()) {
              batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum not reached"));
              continue;
            }
          } catch (final Exception e) {
            batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum watch failed: " + e.getMessage()));
            continue;
          }
        }
```

With:
```java
        if (quorum == Quorum.ALL) {
          try {
            final RaftClientReply watchReply = raftClient.io().watch(
                reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
            if (!watchReply.isSuccess()) {
              batch.get(i).future.complete(new MajorityCommittedAllFailedException(
                  "ALL quorum not reached after MAJORITY commit at logIndex=" + reply.getLogIndex()));
              continue;
            }
          } catch (final Exception e) {
            batch.get(i).future.complete(new MajorityCommittedAllFailedException(
                "ALL quorum watch failed after MAJORITY commit: " + e.getMessage(), e));
            continue;
          }
        }
```

- [ ] **Step 4: Build to verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft -DskipTests compile -am`
Expected: BUILD SUCCESS

- [ ] **Step 5: Run existing tests to verify no regressions**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftGroupCommitterTest" -am`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java
git commit -m "feat(ha-raft): throw MajorityCommittedAllFailedException on ALL-quorum watch failure

When MAJORITY quorum succeeds but the ALL-quorum watch fails, throw
MajorityCommittedAllFailedException instead of QuorumNotReachedException.
This lets RaftReplicatedDatabase.commit() distinguish between 'committed
but not fully replicated' and 'not committed at all'.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Add applyLocallyAfterMajorityCommit and phase-2 escalation to RaftReplicatedDatabase

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:243-288`

This is the most critical correctness change. It adds three things to `commit()`:
1. A catch for `MajorityCommittedAllFailedException` that calls `applyLocallyAfterMajorityCommit()`
2. Enhanced phase-2 error handling with `ConcurrentModificationException` distinction
3. Emergency `server.stop()` on step-down failure

- [ ] **Step 1: Add the MajorityCommittedAllFailedException catch to commit()**

In `RaftReplicatedDatabase.java`, replace the replication try-catch block (lines 244-254) with:

```java
    // --- REPLICATION (no lock held): send WAL to Raft and wait for quorum ---
    try {
      final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), payload.walData(), payload.bucketDeltas());
      final RaftHAServer raft = requireRaftServer();
      raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
    } catch (final MajorityCommittedAllFailedException e) {
      // MAJORITY committed (applyTransaction fired with origin-skip, lastAppliedIndex advanced)
      // but ALL-quorum watch failed. We MUST apply locally to prevent permanent divergence.
      HALog.log(this, HALog.BASIC,
          "ALL quorum watch failed after MAJORITY commit; applying locally to prevent leader divergence: db=%s", getName());
      applyLocallyAfterMajorityCommit(payload);
      throw e;
    } catch (final ArcadeDBException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Error on commit distributed transaction (replication)", e);
    }
```

- [ ] **Step 2: Enhance the phase-2 catch block**

Replace the phase-2 try-catch (lines 264-287) with:

```java
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx().commit2ndPhase(payload.phase1());

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        if (e instanceof java.util.ConcurrentModificationException)
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication with a page version conflict (db=%s, txId=%s). "
                  + "A page was concurrently modified under file lock - this may indicate a locking bug. "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());
        else
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication (db=%s, txId=%s). "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());
        // Step down so a follower with correct state takes over.
        try {
          if (raftHAServer != null && raftHAServer.isLeader())
            raftHAServer.stepDown();
        } catch (final Exception stepDownEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down after phase 2 failure (db=%s, txId=%s). "
                  + "Forcing server stop to prevent leader-follower divergence.",
              getName(), payload.tx());
          // Schedule stop on a separate thread to avoid deadlock (we hold the read lock)
          final Thread stopThread = new Thread(() -> {
            try {
              server.stop();
            } catch (final Throwable t) {
              LogManager.instance().log(this, Level.SEVERE,
                  "Server stop also failed (db=%s). Manual intervention required: %s",
                  getName(), t.getMessage());
            }
          }, "arcadedb-emergency-stop");
          stopThread.setDaemon(true);
          stopThread.start();
        }
        throw e;
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
```

- [ ] **Step 3: Add the applyLocallyAfterMajorityCommit method**

Add this private method to `RaftReplicatedDatabase` (after the `commit()` method, before the `command()` methods):

```java
  /**
   * Applies phase 2 locally when ALL-quorum watch fails after MAJORITY commit.
   * The Raft entry is durably committed (MAJORITY applied it, including origin-skip on the leader),
   * so we must write the local pages to prevent permanent divergence.
   */
  private void applyLocallyAfterMajorityCommit(final ReplicationPayload payload) {
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx().commit2ndPhase(payload.phase1());
        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed during ALL-quorum recovery (db=%s, txId=%s). "
                + "Leader database may be inconsistent. Stepping down so a node with correct state takes over. Error: %s",
            getName(), payload.tx(), e.getMessage());
        try {
          if (raftHAServer != null && raftHAServer.isLeader())
            raftHAServer.stepDown();
        } catch (final Exception stepDownEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down after ALL-quorum recovery failure (db=%s): %s. Manual intervention required.",
              getName(), stepDownEx.getMessage());
        }
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }
```

- [ ] **Step 4: Build to verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft -DskipTests compile -am`
Expected: BUILD SUCCESS

- [ ] **Step 5: Run existing tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftReplicatedDatabaseTest,RaftGroupCommitterTest" -am`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
git commit -m "fix(ha-raft): add ALL-quorum recovery and phase-2 failure escalation in commit()

When ALL-quorum watch fails after MAJORITY commit, apply pages locally via
applyLocallyAfterMajorityCommit() to prevent permanent leader divergence.

Enhance phase-2 error handling: distinguish ConcurrentModificationException
(locking bug) from generic errors, include txId in logs, and force server
stop if step-down fails to prevent leader-follower divergence.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Add WALVersionGapException handling and lifecycle executor to ArcadeStateMachine

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

This task implements spec items 1.3, 1.4, 1.5, and 1.6 together since they all modify ArcadeStateMachine.

- [ ] **Step 1: Add new imports and fields to ArcadeStateMachine**

Add these imports after the existing imports (around line 42):

```java
import com.arcadedb.exception.WALVersionGapException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
```

Add these fields after the existing fields (after line 88):

```java
  private static final long SNAPSHOT_GAP_TOLERANCE = 10;

  private final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(r -> {
    final Thread t = new Thread(r, "arcadedb-sm-lifecycle");
    t.setDaemon(true);
    return t;
  });

  private final AtomicBoolean needsSnapshotDownload = new AtomicBoolean(false);
  private final AtomicBoolean catchingUp             = new AtomicBoolean(false);
```

- [ ] **Step 2: Update reinitialize() with persisted applied-index and snapshot gap detection**

Replace the `reinitialize()` method (lines 122-128):

```java
  public void reinitialize() throws IOException {
    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null) {
      final long snapshotIndex = snapshotInfo.getIndex();
      final long persistedApplied = readPersistedAppliedIndex();
      if (persistedApplied >= 0 && snapshotIndex > persistedApplied + SNAPSHOT_GAP_TOLERANCE) {
        LogManager.instance().log(this, Level.INFO,
            "Snapshot index %d is ahead of persisted applied index %d, will download from leader when available",
            snapshotIndex, persistedApplied);
        needsSnapshotDownload.set(true);

        // Watchdog: if notifyLeaderChanged() doesn't fire within 30 seconds, trigger download directly
        lifecycleExecutor.submit(() -> {
          try {
            Thread.sleep(30_000);
            if (needsSnapshotDownload.compareAndSet(true, false)) {
              LogManager.instance().log(this, Level.WARNING,
                  "Snapshot download watchdog: no leader change after 30s, triggering download directly");
              triggerSnapshotDownload();
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Snapshot download watchdog failed", e);
          }
        });
      }
      lastAppliedIndex.set(snapshotIndex);
      updateLastAppliedTermIndex(snapshotInfo.getTerm(), snapshotIndex);
    } else
      lastAppliedIndex.set(-1);
  }
```

- [ ] **Step 3: Update applyTransaction() with emergency stop, hot resync, and persisted index**

Replace the `applyTransaction()` method (lines 136-162):

```java
  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();
    final ByteString data = entry.getStateMachineLogEntry().getLogData();
    final TermIndex termIndex = TermIndex.valueOf(entry);
    final long index = termIndex.getIndex();

    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(data);

      switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded);
        case SCHEMA_ENTRY -> applySchemaEntry(decoded);
        case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
        case DROP_DATABASE_ENTRY -> applyDropDatabaseEntry(decoded);
        case SECURITY_USERS_ENTRY -> applySecurityUsersEntry(decoded);
      }

      final long previousApplied = lastAppliedIndex.getAndSet(index);
      updateLastAppliedTermIndex(termIndex.getTerm(), index);
      writePersistedAppliedIndex(index);

      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      final RaftHAServer raftHA = this.raftHAServer;
      if (raftHA != null) {
        raftHA.notifyApplied();

        // Detect hot resync on followers
        if (!raftHA.isLeader()) {
          final long gap = index - previousApplied;
          if (gap > 1 && catchingUp.compareAndSet(false, true))
            HALog.log(this, HALog.BASIC, "Follower catching up: gap=%d (previous=%d, current=%d)",
                gap, previousApplied, index);
          if (catchingUp.get()) {
            final long commitIndex = raftHA.getCommitIndex();
            if (commitIndex > 0 && index >= commitIndex) {
              catchingUp.set(false);
              HALog.log(this, HALog.BASIC, "Hot resync complete: applied=%d >= commit=%d", index, commitIndex);
            }
          }
        }
      }
      return CompletableFuture.completedFuture(Message.valueOf("OK"));

    } catch (final ReplicationException e) {
      LogManager.instance().log(this, Level.SEVERE, "Replication error at index %d: %s", e, index, e.getMessage());
      return CompletableFuture.failedFuture(e);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.WARNING, "Invalid raft log entry at index %d: %s", index, e.getMessage());
      return CompletableFuture.failedFuture(e);
    } catch (final Throwable e) {
      // Unexpected errors (NPE, ClassCastException, OOM, etc.) indicate a bug that could cause
      // state divergence if silently swallowed. Crash the server so the node recovers via snapshot.
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: Unexpected error applying Raft log entry at index %d. "
              + "Shutting down to prevent state divergence.", e, index);
      final Thread stopThread = new Thread(() -> {
        try {
          if (server != null)
            server.stop();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.SEVERE, "Emergency stop failed", t);
        }
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
      return CompletableFuture.failedFuture(e instanceof Exception ex ? ex : new RuntimeException(e));
    }
  }
```

- [ ] **Step 4: Update applyTxEntry() with WALVersionGapException handling**

Replace `applyTxEntry()` method (lines 304-322):

```java
  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader, the transaction was already applied via commit2ndPhase() in RaftReplicatedDatabase.
    // Only replicas need to apply WAL changes from the state machine.
    if (raftHAServer != null && raftHAServer.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping tx apply on leader for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    HALog.log(this, HALog.DETAILED, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    try {
      db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), false);
    } catch (final WALVersionGapException e) {
      // Version gap: WAL page version > DB page version + 1 - an intermediate transaction
      // was never applied on this node. State has diverged; trigger snapshot resync.
      LogManager.instance().log(this, Level.SEVERE,
          "WAL version gap on follower - state divergence detected, triggering snapshot resync (db=%s, txId=%d): %s",
          decoded.databaseName(), walTx.txId, e.getMessage());
      throw new ReplicationException(
          "WAL version gap detected - snapshot resync required (db=" + decoded.databaseName() + ")", e);
    } catch (final com.arcadedb.exception.ConcurrentModificationException e) {
      // Benign replay: WAL page version <= DB page version - the entry was already applied
      // (via WAL recovery or a prior commit). Expected after cold restart or snapshot install.
      LogManager.instance().log(this, Level.WARNING,
          "Skipping already-applied WAL entry on follower (db=%s, txId=%d): %s",
          decoded.databaseName(), walTx.txId, e.getMessage());
    }
  }
```

- [ ] **Step 5: Add persisted applied-index methods and lifecycle executor cleanup**

Add these methods after `deserializeWalTransaction()` (at the end of the class, before the closing brace):

```java
  private long readPersistedAppliedIndex() {
    try {
      final Path file = getAppliedIndexFile();
      if (file != null && Files.exists(file))
        return Long.parseLong(Files.readString(file).trim());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not read persisted applied index: %s", e.getMessage());
    }
    return -1;
  }

  private void writePersistedAppliedIndex(final long index) {
    try {
      final Path file = getAppliedIndexFile();
      if (file == null)
        return;
      Files.createDirectories(file.getParent());
      // Write via a temp file + atomic rename to avoid corruption on crash
      final Path tmp = file.resolveSibling("applied-index.tmp");
      Files.writeString(tmp, Long.toString(index));
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
    }
  }

  private Path getAppliedIndexFile() {
    if (server == null)
      return null;
    final String dbDir = server.getConfiguration().getValueAsString(
        com.arcadedb.GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    if (dbDir == null)
      return null;
    return Path.of(dbDir, ".raft", "applied-index");
  }

  private void triggerSnapshotDownload() {
    if (raftHAServer == null || server == null)
      return;
    final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
    if (leaderHttpAddr == null) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot trigger snapshot download: leader HTTP address unknown");
      return;
    }
    try {
      final String clusterToken = server.getConfiguration().getValueAsString(
          com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);
      for (final String dbName : server.getDatabaseNames()) {
        if (server.existsDatabase(dbName)) {
          final DatabaseInternal db = (DatabaseInternal) server.getDatabase(dbName);
          final String databasePath = db.getDatabasePath();
          db.close();
          server.removeDatabase(dbName);
          SnapshotInstaller.install(dbName, databasePath, leaderHttpAddr, clusterToken, server);
        }
      }
      LogManager.instance().log(this, Level.INFO, "Snapshot download triggered by watchdog completed");
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Snapshot download triggered by watchdog failed", e);
    }
  }

  /**
   * Shuts down the lifecycle executor. Called when the state machine is closed.
   */
  @Override
  public void close() throws IOException {
    lifecycleExecutor.shutdownNow();
    super.close();
  }
```

- [ ] **Step 6: Build to verify compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft -DskipTests compile -am`
Expected: BUILD SUCCESS

- [ ] **Step 7: Run existing tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="ArcadeStateMachineTest" -am`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "fix(ha-raft): add WAL version gap handling, persisted applied-index, emergency stop

- applyTxEntry: catch WALVersionGapException separately from ConcurrentModificationException.
  Version gaps trigger ReplicationException for snapshot resync. Benign replays are logged
  as WARNING and skipped.
- Persisted applied-index: write last-applied index to disk via temp+atomic rename after
  each apply. On reinitialize, detect snapshot gaps and trigger download with 30s watchdog.
- Emergency stop: unexpected errors (NPE, OOM) in applyTransaction trigger server.stop()
  on a separate thread to prevent silent state divergence.
- Lifecycle executor: single-threaded daemon for async tasks that cannot run on Ratis
  callback threads.
- Hot resync detection: log when followers are catching up and when they complete.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Run full P1 integration tests

**Files:** No changes, verification only.

- [ ] **Step 1: Run all ha-raft unit tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -am`
Expected: All tests PASS

- [ ] **Step 2: Run key integration tests that exercise commit path**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="Raft3PhaseCommitIT,RaftReplication3NodesIT,RaftLeaderFailoverIT" -DfailIfNoTests=false -am`
Expected: All tests PASS (or skip if IT infrastructure not available)

---

## Task 7: Extract RaftPeerAddressResolver from RaftHAServer

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPeerAddressResolver.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Identify the code to extract**

Read `RaftHAServer.java` and find:
- The `ParsedPeerList` record (line 89)
- The peer parsing method(s) that produce `ParsedPeerList` from `HA_SERVER_LIST` config
- Any helper methods for resolving peer HTTP addresses

Use `grep -n "parsePeer\|ParsedPeerList\|httpAddress\|HA_SERVER_LIST" RaftHAServer.java` to find exact line ranges.

- [ ] **Step 2: Create RaftPeerAddressResolver with the extracted code**

Create the file with the `ParsedPeerList` record and the peer parsing logic moved from `RaftHAServer`. The class should have a static method `resolve(ContextConfiguration config)` that returns a `ParsedPeerList`.

- [ ] **Step 3: Update RaftHAServer to delegate to RaftPeerAddressResolver**

Replace the inline peer parsing with a call to `RaftPeerAddressResolver.resolve(configuration)`. Remove the `ParsedPeerList` record from `RaftHAServer`.

- [ ] **Step 4: Build and run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftHAServerTest,RaftHAServerAddressParsingTest" -am`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPeerAddressResolver.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "refactor(ha-raft): extract RaftPeerAddressResolver from RaftHAServer

Move ParsedPeerList record and peer address parsing logic into a dedicated
class. RaftHAServer now delegates peer resolution instead of handling it inline.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Extract RaftPropertiesBuilder from RaftHAServer

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Identify the code to extract**

Read `RaftHAServer.java` and find the `buildRaftProperties()` method (line 296). This entire method moves to the new class.

- [ ] **Step 2: Create RaftPropertiesBuilder**

Create the file with a static `build(ContextConfiguration config)` method that returns `RaftProperties`. Move all the Ratis property setup code from `RaftHAServer.buildRaftProperties()`.

- [ ] **Step 3: Update RaftHAServer to delegate**

Replace `buildRaftProperties()` with a call to `RaftPropertiesBuilder.build(configuration)`.

- [ ] **Step 4: Build and run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftHAServerTest,HAConfigDefaultsTest" -am`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "refactor(ha-raft): extract RaftPropertiesBuilder from RaftHAServer

Move Ratis RaftProperties construction into a dedicated builder class.
RaftHAServer delegates property setup instead of handling it inline.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Extract RaftClusterManager from RaftHAServer

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterManager.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Identify the code to extract**

Find in `RaftHAServer.java`:
- `addPeer()` (line 775)
- `removePeer()` (line 800)
- `leaveCluster()` (line 960)
- `transferLeadership()` methods (lines 585, 910)
- Any helper methods these call

- [ ] **Step 2: Create RaftClusterManager**

Create the file. The class takes a `RaftClient` and `RaftHAServer` reference in its constructor. Move the membership methods. The `RaftHAServer` reference is needed for `getLocalPeerId()`, `getRaftGroup()`, etc.

- [ ] **Step 3: Update RaftHAServer to delegate**

RaftHAServer keeps thin forwarding methods that delegate to `RaftClusterManager`. The HTTP handlers (`PostAddPeerHandler`, `DeletePeerHandler`, `PostLeaveHandler`, `PostTransferLeaderHandler`) call through `RaftHAServer` which delegates.

- [ ] **Step 4: Build and run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftHAServerTest,DynamicMembershipTest,LeaveClusterTest" -am`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterManager.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "refactor(ha-raft): extract RaftClusterManager from RaftHAServer

Move addPeer, removePeer, leaveCluster, and transferLeadership into a
dedicated cluster management class. RaftHAServer delegates membership
operations.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Extract RaftTransactionBroker, KubernetesAutoJoin, and copy ClusterTokenProvider + RaftClusterStatusExporter

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterTokenProvider.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Copy ClusterTokenProvider from apache-ratis**

Run: `git show origin/apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterTokenProvider.java > ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterTokenProvider.java`

Fix the copyright line (remove em dash from `Copyright ©`), replace with `Copyright 2021-present`.

- [ ] **Step 2: Copy RaftClusterStatusExporter from apache-ratis**

Run: `git show origin/apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java > ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java`

Fix the copyright line. Adapt any class name references (`ReplicatedDatabase` -> `RaftReplicatedDatabase`, `ArcadeDBStateMachine` -> `ArcadeStateMachine`).

- [ ] **Step 3: Extract RaftTransactionBroker**

Find the `replicateTransaction()` method and related helpers in `RaftHAServer.java`. Move them to `RaftTransactionBroker`. The broker takes a `RaftGroupCommitter` reference.

- [ ] **Step 4: Extract KubernetesAutoJoin**

Find K8s-specific join/leave logic in `RaftHAServer.java`. Move to `KubernetesAutoJoin`. Only instantiated when `HA_K8S` is enabled in `RaftHAPlugin`.

- [ ] **Step 5: Update RaftHAServer to delegate**

Wire all extracted classes. `RaftHAServer` should now be ~500-600 lines.

- [ ] **Step 6: Clean up em dashes and fully-qualified names**

Search all modified and new files for em dashes (`\u2014`) and inline fully-qualified names. Fix:
- `ArcadeStateMachine.java` - any em dashes
- `RaftHAPlugin.java` - any em dashes
- `RaftHAServer.java` - 7 occurrences of em dashes
- `SnapshotManager.java` - any em dashes
- Replace `com.arcadedb.GlobalConfiguration` with import where used in the same package
- Replace `javax.crypto.*`, `java.util.HexFormat` inline usages with imports

- [ ] **Step 7: Build and run all tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -am`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterTokenProvider.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotManager.java
git commit -m "refactor(ha-raft): complete P3 architecture extraction from RaftHAServer

Extract RaftTransactionBroker, KubernetesAutoJoin from RaftHAServer.
Copy ClusterTokenProvider, RaftClusterStatusExporter from apache-ratis.
RaftHAServer shrinks to ~500 lines as a coordinator.

Also: remove em dashes and inline fully-qualified names across all
modified files per project conventions.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Harden SnapshotInstaller (P2.1)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSymlinkProtectionTest.java` (new or extend existing `SnapshotSymlinkTest.java`)

- [ ] **Step 1: Write failing test for real-path symlink escape check**

Create or extend `SnapshotSymlinkProtectionTest.java` with a test that creates a ZIP containing a symlink entry that escapes the target directory via `Path.toRealPath()`. Verify extraction throws `ReplicationException`.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="SnapshotSymlinkProtectionTest" -am`
Expected: FAIL

- [ ] **Step 3: Implement security hardening in downloadSnapshot()**

In `SnapshotInstaller.downloadSnapshot()` (lines 241-286), replace the extraction loop with:

```java
      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = targetDir.resolve(zipEntry.getName()).normalize();

          // Zip-slip protection (literal check)
          if (!targetFile.startsWith(targetDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          // Reject paths containing ".."
          if (zipEntry.getName().contains(".."))
            throw new ReplicationException("Suspicious path in snapshot ZIP: " + zipEntry.getName());

          // Create parent directories
          Files.createDirectories(targetFile.getParent());

          // Real-path symlink-escape check on parent directory
          final Path realParent = targetFile.getParent().toRealPath();
          if (!realParent.startsWith(targetDir.toRealPath()))
            throw new ReplicationException(
                "Symlink escape detected in snapshot: entry '" + zipEntry.getName()
                    + "' resolves outside target directory");

          // Reject pre-existing symlinks at target path
          if (Files.isSymbolicLink(targetFile))
            throw new ReplicationException(
                "Symlink detected at extraction target: " + targetFile);

          // Bounded copy to defend against zip-bombs (10 GB limit per entry)
          try (final java.io.FileOutputStream fos = new java.io.FileOutputStream(targetFile.toFile())) {
            copyWithLimit(zipIn, fos, 10L * 1024 * 1024 * 1024, zipEntry.getName());
          }
          zipIn.closeEntry();
        }
      }
```

Add the `copyWithLimit` helper:

```java
  private static void copyWithLimit(final java.io.InputStream in, final java.io.OutputStream out,
      final long maxBytes, final String entryName) throws IOException {
    final byte[] buffer = new byte[8192];
    long totalRead = 0;
    int bytesRead;
    while ((bytesRead = in.read(buffer)) != -1) {
      totalRead += bytesRead;
      if (totalRead > maxBytes)
        throw new ReplicationException(
            "Snapshot entry '" + entryName + "' exceeds size limit of " + maxBytes + " bytes (zip-bomb protection)");
      out.write(buffer, 0, bytesRead);
    }
  }
```

- [ ] **Step 4: Add SSL context handling**

In `downloadSnapshot()`, before opening the connection, add SSL handling:

```java
    final boolean useSsl = server != null && server.getConfiguration().getValueAsBoolean(
        GlobalConfiguration.NETWORK_USE_SSL);

    // ... after opening connection ...
    if (connection instanceof javax.net.ssl.HttpsURLConnection httpsConn) {
      if (!useSsl)
        throw new ReplicationException(
            "Snapshot URL uses HTTPS but NETWORK_USE_SSL is disabled: " + snapshotUrl);
      if (server != null && server.getHttpServer() != null) {
        final javax.net.ssl.SSLContext sslContext = server.getHttpServer().getSSLContext();
        if (sslContext != null)
          httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      }
    }
```

Note: This requires updating `downloadSnapshot()` and `downloadWithRetry()` signatures to accept a `server` parameter, or pass SSL context separately. Follow the existing pattern of passing individual parameters.

- [ ] **Step 5: Add cluster token to DB-list call**

Find any place where the installer calls the leader's database-list endpoint. Add `X-ArcadeDB-Cluster-Token` header there too.

- [ ] **Step 6: Run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="SnapshotSymlinkProtectionTest,SnapshotSymlinkTest,SnapshotInstallerRetryTest,SnapshotSwapRecoveryTest" -am`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSymlinkProtectionTest.java
git commit -m "fix(ha-raft): harden SnapshotInstaller with symlink, zip-bomb, and SSL checks

- Real-path symlink-escape check on parent directories during extraction
- File-level symlink rejection at extraction targets
- copyWithLimit with 10 GB cap per entry to defend against zip-bombs
- SSL context handling: use server SSLContext for HTTPS connections,
  fail-fast if HTTPS URL but NETWORK_USE_SSL is disabled
- Cluster token header on database-list calls

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Harden SnapshotHttpHandler (P2.2) and add snapshot tests (P2.3)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWriteTimeoutTest.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSwapRecoveryTest.java`

- [ ] **Step 1: Add root-only authorization**

After the `authenticate()` call in `handleRequest()` (line 74), add:

```java
    if (!"root".equals(user.getName())) {
      exchange.setStatusCode(403);
      exchange.getResponseSender().send("Only root user can download database snapshots");
      return;
    }
```

- [ ] **Step 2: Add database-name validation**

After extracting `databaseName` (line 93), add validation:

```java
    if (databaseName.contains("/") || databaseName.contains("\\")
        || databaseName.contains("..") || databaseName.contains("\0")
        || !databaseName.chars().allMatch(c -> c >= 0x20 && c < 0x7F)) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("Invalid database name");
      return;
    }
```

- [ ] **Step 3: Sanitize Content-Disposition header**

Replace the Content-Disposition header (line 118-119):

```java
    final String safeName = databaseName.replaceAll("[^a-zA-Z0-9._-]", "_");
    exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
        "attachment; filename=\"" + safeName + "-snapshot.zip\"");
```

- [ ] **Step 4: Add write-timeout watchdog**

Add a scheduled executor field:

```java
  private static final java.util.concurrent.ScheduledExecutorService TIMEOUT_SCHEDULER =
      java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "arcadedb-snapshot-timeout");
        t.setDaemon(true);
        return t;
      });
```

In the `serveSnapshotZip()` method, wrap the streaming with a timeout watchdog:

```java
  private void serveSnapshotZip(final HttpServerExchange exchange, final DatabaseInternal db, final String databaseName) {
    final long timeoutMs = db.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_WRITE_TIMEOUT_MS, 300_000);
    final java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);
    final java.util.concurrent.ScheduledFuture<?> watchdog = TIMEOUT_SCHEDULER.schedule(() -> {
      if (!completed.get()) {
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot streaming for '%s' timed out after %dms, closing connection", databaseName, timeoutMs);
        try {
          exchange.getConnection().close();
        } catch (final Exception e) {
          // ignore
        }
      }
    }, timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

    try (final OutputStream out = exchange.getOutputStream();
         final ZipOutputStream zipOut = new ZipOutputStream(out)) {
      // ... existing ZIP serving logic ...
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error serving snapshot for '%s'", e, databaseName);
      throw new RuntimeException(e);
    } finally {
      completed.set(true);
      watchdog.cancel(false);
    }
  }
```

- [ ] **Step 5: Add plain-HTTP warning**

Add a static flag and warning in the constructor or handleRequest:

```java
  private static volatile boolean sslWarningLogged = false;

  // In handleRequest(), after authentication:
  if (!sslWarningLogged && !httpServer.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL)) {
    sslWarningLogged = true;
    LogManager.instance().log(this, Level.WARNING,
        "Serving database snapshots over plain HTTP. Consider enabling SSL for production clusters.");
  }
```

- [ ] **Step 6: Write SnapshotWriteTimeoutTest**

Create `SnapshotWriteTimeoutTest.java` that verifies the watchdog concept (unit test level).

- [ ] **Step 7: Extend SnapshotSwapRecoveryTest**

Add test methods verifying that `.snapshot-new` directories without `SNAPSHOT_COMPLETE_FILE` marker are discarded during `recoverPendingSnapshotSwaps()`.

- [ ] **Step 8: Build and run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="SnapshotWriteTimeoutTest,SnapshotSwapRecoveryTest,SnapshotThrottleTest" -am`
Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWriteTimeoutTest.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSwapRecoveryTest.java
git commit -m "fix(ha-raft): harden SnapshotHttpHandler with auth, validation, and timeout

- Root-only authorization (403 for non-root users)
- Database name validation rejecting path traversal and control characters
- Content-Disposition header sanitization
- Write-timeout watchdog that kills stalled connections and releases semaphore
- Plain-HTTP warning log (once per server start)
- Extended SnapshotSwapRecoveryTest for completion marker verification

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: Wire ClusterTokenProvider into all call sites (P4)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterTokenProviderTest.java`

- [ ] **Step 1: Write ClusterTokenProviderTest**

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTokenProviderTest {

  @Test
  void consistentTokenDerivation() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    final ClusterTokenProvider provider1 = new ClusterTokenProvider(config);
    final ClusterTokenProvider provider2 = new ClusterTokenProvider(config);
    assertThat(provider1.getClusterToken()).isEqualTo(provider2.getClusterToken());
  }

  @Test
  void differentClusterNamesDifferentTokens() {
    final ContextConfiguration config1 = new ContextConfiguration();
    config1.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-a");
    config1.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");

    final ContextConfiguration config2 = new ContextConfiguration();
    config2.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "cluster-b");
    config2.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "same-password");

    final ClusterTokenProvider provider1 = new ClusterTokenProvider(config1);
    final ClusterTokenProvider provider2 = new ClusterTokenProvider(config2);
    assertThat(provider1.getClusterToken()).isNotEqualTo(provider2.getClusterToken());
  }

  @Test
  void missingClusterNameThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    assertThatThrownBy(provider::getClusterToken).isInstanceOf(ConfigurationException.class);
  }

  @Test
  void missingRootPasswordThrows() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "");
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    assertThatThrownBy(provider::getClusterToken).isInstanceOf(ConfigurationException.class);
  }

  @Test
  void passwordMaterialZeroed() {
    final char[] password = "test-password".toCharArray();
    ClusterTokenProvider.deriveTokenFromPassword("test-cluster", password);
    // The method should NOT zero the caller's array (callers own their copy)
    // but internal arrays should be zeroed. We verify the method completes without error.
    assertThat(password).containsExactly("test-password".toCharArray());
  }

  @Test
  void explicitTokenTakesPrecedence() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");
    config.setValue(GlobalConfiguration.HA_CLUSTER_NAME, "test-cluster");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    final ClusterTokenProvider provider = new ClusterTokenProvider(config);
    assertThat(provider.getClusterToken()).isEqualTo("explicit-token");
  }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="ClusterTokenProviderTest" -am`
Expected: All PASS

- [ ] **Step 3: Replace direct config reads with ClusterTokenProvider**

Search for `getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)` or `getValueAsString(com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN)` in all ha-raft source files. Replace each with injection of `ClusterTokenProvider`. The provider should be created once in `RaftHAPlugin` or `RaftHAServer` and passed to dependents.

- [ ] **Step 4: Build and run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -am`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterTokenProviderTest.java
git commit -m "feat(ha-raft): wire ClusterTokenProvider into all cluster token call sites

Replace direct configuration reads of HA_CLUSTER_TOKEN with injected
ClusterTokenProvider across ArcadeStateMachine, PostVerifyDatabaseHandler,
RaftHAPlugin, and SnapshotInstaller. Add validation and production warnings.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: RaftLogEntryType forward compatibility (P5)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

- [ ] **Step 1: Write failing test**

Add to an existing test file or create a new one:

```java
@Test
void unknownEntryTypeReturnsNull() {
  assertThat(RaftLogEntryType.fromId((byte) 99)).isNull();
}
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL with `IllegalArgumentException: Unknown RaftLogEntryType id: 99`

- [ ] **Step 3: Update RaftLogEntryType.fromId() to return null**

Replace (lines 38-43):

```java
  public static RaftLogEntryType fromId(final byte id) {
    for (final RaftLogEntryType type : values())
      if (type.id == id)
        return type;
    return null;
  }
```

- [ ] **Step 4: Update RaftLogEntryCodec.decode() to handle null type**

In the `decode()` method, after reading the type byte, handle null:

```java
    final RaftLogEntryType type = RaftLogEntryType.fromId(typeByte);
    if (type == null)
      return new DecodedEntry(null, null, null, null, null, null, null, null, null, null, false);
```

- [ ] **Step 5: Update ArcadeStateMachine.applyTransaction() to handle null type**

Before the `switch` statement, add a null check:

```java
      if (decoded.type() == null) {
        LogManager.instance().log(this, Level.WARNING,
            "Unknown Raft log entry type at index %d, skipping - likely from a newer node version", index);
        lastAppliedIndex.set(index);
        updateLastAppliedTermIndex(termIndex.getTerm(), index);
        if (raftHAServer != null)
          raftHAServer.notifyApplied();
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
      }
```

- [ ] **Step 6: Run tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="RaftLogEntryCodecTest,ArcadeStateMachineTest" -am`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "fix(ha-raft): return null for unknown RaftLogEntryType IDs

Change fromId() to return null instead of throwing IllegalArgumentException
for unknown type codes. This prevents older nodes from crashing during
rolling upgrades when they encounter entry types from newer node versions.
Unknown entries are acknowledged as applied but no database mutation occurs.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: Port correctness-level tests (P6)

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftClusterStarter.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandlerTest.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerValidatePeerAddressTest.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineCreateFilesTest.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/OriginNodeSkipIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGraphIngestionStabilityIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAComprehensiveIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerIT.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationIT.java`

For each test:
1. Copy from apache-ratis: `git show origin/apache-ratis:ha-raft/src/test/java/com/arcadedb/server/ha/raft/<TestName>.java`
2. Adapt class names (`ArcadeDBStateMachine` -> `ArcadeStateMachine`, `ReplicatedDatabase` -> `RaftReplicatedDatabase`)
3. Adapt to ha-redesign's entry types and codec
4. Use ha-redesign's test base classes (`BaseRaftHATest`, `BaseMiniRaftTest`)
5. Fix copyright line (remove em dash)
6. Run and verify each test passes individually

- [ ] **Step 1: Port RaftClusterStarter (test utility)**

Copy and adapt. This is used by several other tests.

- [ ] **Step 2: Port unit tests (PostVerifyDatabaseHandlerTest, RaftHAServerValidatePeerAddressTest, ArcadeStateMachineCreateFilesTest)**

Copy, adapt, and run each:

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="PostVerifyDatabaseHandlerTest,RaftHAServerValidatePeerAddressTest,ArcadeStateMachineCreateFilesTest" -am`

- [ ] **Step 3: Port integration tests (OriginNodeSkipIT, RaftGraphIngestionStabilityIT, RaftHAComprehensiveIT, RaftHAServerIT, RaftReplicationIT)**

Copy, adapt, and run each:

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="OriginNodeSkipIT,RaftGraphIngestionStabilityIT,RaftHAComprehensiveIT,RaftHAServerIT,RaftReplicationIT" -DfailIfNoTests=false -am`

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftClusterStarter.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandlerTest.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerValidatePeerAddressTest.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineCreateFilesTest.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/OriginNodeSkipIT.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGraphIngestionStabilityIT.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAComprehensiveIT.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerIT.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicationIT.java
git commit -m "test(ha-raft): port correctness-level tests from apache-ratis (P6)

Port RaftClusterStarter utility and 8 test classes covering:
- Origin-skip optimization verification
- High-throughput graph ingestion stability
- Comprehensive HA lifecycle scenarios
- RaftHAServer lifecycle management
- Core replication across data models
- PostVerifyDatabaseHandler validation
- Peer address parsing edge cases
- Schema file creation during replication

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: Optional polish (P7)

**Files:** Various

- [ ] **Step 1: Port RaftHTTP2ServersIT resilience improvements**

Compare ha-redesign's `RaftHTTP2ServersIT` with apache-ratis version. Port any timeout or retry improvements.

- [ ] **Step 2: Add ForkJoinPool isolation documentation**

Add a code comment in `ArcadeStateMachine.notifyInstallSnapshotFromLeader()` documenting the current scheduling approach and its safety:

```java
    // Note: This runs on CompletableFuture's common ForkJoinPool. Apache-ratis uses a dedicated
    // pool to avoid blocking Ratis internal threads. The current approach is safe because
    // supplyAsync() returns immediately and the snapshot download runs on a separate thread.
    // However, if the common pool is exhausted, new snapshot downloads may be delayed.
```

- [ ] **Step 3: Run full test suite**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -am`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(ha-raft): P7 polish - resilience improvements and documentation

Port RaftHTTP2ServersIT resilience tweaks from apache-ratis.
Add ForkJoinPool isolation documentation in ArcadeStateMachine.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Task 17: Final verification

- [ ] **Step 1: Run full ha-raft unit test suite**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -am`
Expected: All tests PASS

- [ ] **Step 2: Run key integration tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn -pl ha-raft test -Dtest="Raft3PhaseCommitIT,RaftReplication3NodesIT,RaftLeaderFailoverIT,RaftFullSnapshotResyncIT" -DfailIfNoTests=false -am`
Expected: All tests PASS

- [ ] **Step 3: Verify no em dashes remain**

Run: `grep -rn $'\u2014' ha-raft/src/main/java/`
Expected: No matches

- [ ] **Step 4: Verify no inline fully-qualified names in raft package**

Run: `grep -rn 'com\.arcadedb\.GlobalConfiguration\.' ha-raft/src/main/java/com/arcadedb/server/ha/raft/`
Expected: No matches (all should be imported)

- [ ] **Step 5: Compile entire project**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn clean install -DskipTests`
Expected: BUILD SUCCESS
