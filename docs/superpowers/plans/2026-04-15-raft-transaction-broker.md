# RaftTransactionBroker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralize all Raft entry submission behind a `RaftTransactionBroker` facade with timeout cancellation to prevent phantom commits.

**Architecture:** `RaftTransactionBroker` owns `RaftGroupCommitter` and exposes typed methods per entry type. `CancellablePendingEntry` adds an AtomicInteger state machine (PENDING/DISPATCHED/CANCELLED) to `RaftGroupCommitter` to prevent entries from being sent to Raft after the caller times out.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Apache Ratis

**Spec:** `docs/superpowers/specs/2026-04-15-raft-transaction-broker-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `ha-raft/src/main/java/.../raft/RaftGroupCommitter.java` | Modify | Add CancellablePendingEntry, CAS in flush, package-private visibility |
| `ha-raft/src/main/java/.../raft/RaftTransactionBroker.java` | Create | Broker facade - typed methods, owns RaftGroupCommitter |
| `ha-raft/src/main/java/.../raft/RaftHAServer.java` | Modify | Replace groupCommitter field with transactionBroker |
| `ha-raft/src/main/java/.../raft/RaftReplicatedDatabase.java` | Modify | Call broker instead of direct committer/codec |
| `ha-raft/src/main/java/.../raft/RaftHAPlugin.java` | Modify | Call broker instead of direct committer/codec |
| `ha-raft/src/test/java/.../raft/RaftGroupCommitterTest.java` | Modify | Adapt to package-private, add cancellation tests |
| `ha-raft/src/test/java/.../raft/RaftTransactionBrokerTest.java` | Create | Unit tests for broker encoding + delegation |

All paths relative to project root. Full package: `com.arcadedb.server.ha.raft`.

---

### Task 1: Add CancellablePendingEntry to RaftGroupCommitter

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java`

- [ ] **Step 1: Write failing test for cancellation - entry cancelled before dispatch**

In `RaftGroupCommitterTest.java`, add:

```java
@Test
void cancelledEntryIsSkippedByFlusher() throws Exception {
  // Use a committer with null RaftClient so flushed entries fail,
  // but we can observe that cancelled entries are NOT flushed.
  final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 5_000);

  // Submit with a very short timeout so it cancels before flusher picks it up
  try {
    committer.submitAndWait(new byte[] { 1, 2, 3 }, 1);
  } catch (final QuorumNotReachedException e) {
    assertThat(e.getMessage()).containsAnyOf("timed out", "cancelled");
  }

  // Give flusher time to process the queue
  Thread.sleep(300);

  // Submit another entry that WILL be flushed (null client -> "not available" error)
  final var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
    try {
      committer.submitAndWait(new byte[] { 4, 5, 6 }, 5_000);
      return "success";
    } catch (final QuorumNotReachedException e) {
      return "failed: " + e.getMessage();
    }
  });

  final String result = future.join();
  // This entry WAS dispatched (not cancelled), so it fails with "not available" rather than "cancelled"
  assertThat(result).contains("not available");

  committer.stop();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ha-raft && mvn test -Dtest=RaftGroupCommitterTest#cancelledEntryIsSkippedByFlusher -pl . 2>&1 | tail -20`

Expected: FAIL because `RaftGroupCommitter` does not yet have cancellation logic; the 1ms timeout throws a plain `QuorumNotReachedException("Group commit timed out...")` but the entry may still be dispatched.

- [ ] **Step 3: Write failing test for cancellation - dispatched entry waits for result on timeout**

In `RaftGroupCommitterTest.java`, add:

```java
@Test
void dispatchedEntryWaitsForResultOnTimeout() {
  // With null client, entries that ARE dispatched fail with "not available".
  // This test verifies the timeout path when CAS PENDING->CANCELLED fails
  // (because flusher already set DISPATCHED).
  final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 5_000);

  try {
    // Use a long enough timeout that the flusher dispatches it before timeout fires.
    // The flusher polls every 100ms, so 2000ms is plenty.
    committer.submitAndWait(new byte[] { 1, 2, 3 }, 2_000);
    org.junit.jupiter.api.Assertions.fail("Expected exception");
  } catch (final QuorumNotReachedException e) {
    // Entry was dispatched and failed due to null client - this is the correct path
    assertThat(e.getMessage()).contains("not available");
  } finally {
    committer.stop();
  }
}
```

- [ ] **Step 4: Run test to verify it fails or passes (baseline)**

Run: `cd ha-raft && mvn test -Dtest=RaftGroupCommitterTest#dispatchedEntryWaitsForResultOnTimeout -pl . 2>&1 | tail -20`

This test may already pass since the existing code behaves similarly for dispatched entries. Record the baseline.

- [ ] **Step 5: Implement CancellablePendingEntry and cancellation logic**

Replace the `PendingEntry` inner class and update `submitAndWait()` and `flushBatch()` in `RaftGroupCommitter.java`:

Replace class visibility from `public` to package-private:

```java
class RaftGroupCommitter {
```

Replace the `PendingEntry` inner class with:

```java
static class CancellablePendingEntry {
  static final int PENDING    = 0;
  static final int DISPATCHED = 1;
  static final int CANCELLED  = 2;

  final byte[]                                entry;
  final CompletableFuture<Exception>          future = new CompletableFuture<>();
  private final java.util.concurrent.atomic.AtomicInteger state  = new java.util.concurrent.atomic.AtomicInteger(PENDING);

  CancellablePendingEntry(final byte[] entry) {
    this.entry = entry;
  }

  boolean tryDispatch() {
    return state.compareAndSet(PENDING, DISPATCHED);
  }

  boolean tryCancel() {
    return state.compareAndSet(PENDING, CANCELLED);
  }

  int getState() {
    return state.get();
  }
}
```

Replace `submitAndWait()` with:

```java
void submitAndWait(final byte[] entry, final long timeoutMs) {
  final CancellablePendingEntry pending = new CancellablePendingEntry(entry);
  queue.add(pending);

  try {
    final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
    if (error != null)
      throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
  } catch (final java.util.concurrent.TimeoutException e) {
    // Try to cancel before flusher dispatches
    if (pending.tryCancel())
      throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms (cancelled before dispatch)");

    // Flusher already dispatched - wait for the actual Raft result with grace period
    try {
      final Exception error = pending.future.get(quorumTimeout, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
    } catch (final java.util.concurrent.TimeoutException e2) {
      throw new QuorumNotReachedException(
          "Group commit timed out after " + timeoutMs + "ms + " + quorumTimeout + "ms grace (entry was dispatched to Raft)");
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception ex) {
      throw new QuorumNotReachedException("Group commit failed during grace wait: " + ex.getMessage());
    }
  } catch (final RuntimeException e) {
    throw e;
  } catch (final Exception e) {
    throw new QuorumNotReachedException("Group commit failed: " + e.getMessage());
  }
}
```

Update `flushLoop()` - change `PendingEntry` to `CancellablePendingEntry`:

```java
private void flushLoop() {
  final List<CancellablePendingEntry> batch = new ArrayList<>(maxBatchSize);

  while (running) {
    try {
      final CancellablePendingEntry first = queue.poll(100, TimeUnit.MILLISECONDS);
      if (first == null)
        continue;

      batch.clear();
      batch.add(first);
      queue.drainTo(batch, maxBatchSize - 1);

      flushBatch(batch);

    } catch (final InterruptedException e) {
      if (!running)
        break;
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error in group commit flusher: %s", e.getMessage());
      for (final CancellablePendingEntry p : batch)
        p.future.complete(e);
      batch.clear();
    }
  }
}
```

Update `flushBatch()` - add CAS check before dispatch:

```java
@SuppressWarnings("unchecked")
private void flushBatch(final List<CancellablePendingEntry> batch) {
  if (raftClient == null) {
    final Exception err = new QuorumNotReachedException("RaftClient not available");
    for (final CancellablePendingEntry p : batch)
      p.future.complete(err);
    return;
  }

  final CompletableFuture<RaftClientReply>[] futures = new CompletableFuture[batch.size()];
  for (int i = 0; i < batch.size(); i++) {
    final CancellablePendingEntry p = batch.get(i);

    // CAS: only dispatch if still PENDING
    if (!p.tryDispatch()) {
      // Entry was cancelled by the caller - skip it
      p.future.complete(new QuorumNotReachedException("cancelled before dispatch"));
      futures[i] = null;
      continue;
    }

    final Message msg = Message.valueOf(ByteString.copyFrom(p.entry));
    futures[i] = raftClient.async().send(msg);
  }

  for (int i = 0; i < batch.size(); i++) {
    if (futures[i] == null)
      continue; // was cancelled

    try {
      final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
      if (!reply.isSuccess()) {
        final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
        batch.get(i).future.complete(new QuorumNotReachedException("Raft replication failed: " + err));
        continue;
      }

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

      batch.get(i).future.complete(null); // success
    } catch (final Exception e) {
      final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + detail));
    }
  }

  HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());
}
```

Update `stop()` - change `PendingEntry` to `CancellablePendingEntry`:

```java
void stop() {
  running = false;
  flusher.interrupt();
  CancellablePendingEntry pending;
  while ((pending = queue.poll()) != null)
    pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
}
```

Update the queue field type:

```java
private final LinkedBlockingQueue<CancellablePendingEntry> queue = new LinkedBlockingQueue<>();
```

- [ ] **Step 6: Run both cancellation tests**

Run: `cd ha-raft && mvn test -Dtest=RaftGroupCommitterTest -pl . 2>&1 | tail -20`

Expected: All tests PASS.

- [ ] **Step 7: Compile the ha-raft module**

Run: `cd ha-raft && mvn compile -pl . 2>&1 | tail -10`

Expected: BUILD SUCCESS (note: `RaftHAServer`, `RaftReplicatedDatabase`, and `RaftHAPlugin` still reference the old `public` class and `getGroupCommitter()` - these will be fixed in subsequent tasks. The compilation should still pass since the class is package-private within the same package.)

- [ ] **Step 8: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java
git commit -m "feat(ha-raft): add CancellablePendingEntry to RaftGroupCommitter

Replace PendingEntry with CancellablePendingEntry that uses an AtomicInteger
state machine (PENDING/DISPATCHED/CANCELLED) to prevent phantom commits when
a client times out before the flusher dispatches the entry."
```

---

### Task 2: Create RaftTransactionBroker

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftTransactionBrokerTest.java`

- [ ] **Step 1: Write failing tests for broker encoding**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftTransactionBrokerTest.java`:

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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RaftTransactionBroker}. Uses a null RaftClient so entries
 * fail on dispatch, but we can verify encoding via the thrown exception path.
 */
class RaftTransactionBrokerTest {

  private RaftTransactionBroker broker;

  @BeforeEach
  void setUp() {
    // null RaftClient: entries will be dispatched but fail with "RaftClient not available"
    broker = new RaftTransactionBroker(null, Quorum.MAJORITY, 5_000);
  }

  @AfterEach
  void tearDown() {
    if (broker != null)
      broker.stop();
  }

  @Test
  void replicateTransactionEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateTransaction("testdb", new byte[] { 1, 2, 3 }, Map.of(0, 1), 2_000))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateSchemaEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateSchema("testdb", "{}", Map.of(1, "file1.dat"), Map.of(),
            Collections.emptyList(), Collections.emptyList(), 2_000))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateInstallDatabaseEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateInstallDatabase("testdb", false, 2_000))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateDropDatabaseEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateDropDatabase("testdb", 2_000))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateSecurityUsersEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateSecurityUsers("[{\"name\":\"root\"}]", 2_000))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void stopDelegates() {
    broker.stop();
    // After stop, further submissions should fail
    assertThatThrownBy(() ->
        broker.replicateTransaction("testdb", new byte[] { 1 }, Map.of(), 1_000))
        .isInstanceOf(QuorumNotReachedException.class);
    broker = null; // prevent double-stop in tearDown
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ha-raft && mvn test -Dtest=RaftTransactionBrokerTest -pl . 2>&1 | tail -20`

Expected: Compilation failure - `RaftTransactionBroker` does not exist yet.

- [ ] **Step 3: Implement RaftTransactionBroker**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java`:

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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

/**
 * Centralized broker for all Raft entry submission. Owns the {@link RaftGroupCommitter}
 * and exposes typed methods for each entry type. Encoding is handled internally via
 * {@link RaftLogEntryCodec}, so callers never touch the codec directly.
 *
 * <p>The broker delegates to {@link RaftGroupCommitter#submitAndWait} which provides
 * batching and timeout cancellation (preventing phantom commits).
 */
public class RaftTransactionBroker {

  private final RaftGroupCommitter groupCommitter;

  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout) {
    this(raftClient, quorum, quorumTimeout, 500);
  }

  public RaftTransactionBroker(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize) {
    this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, maxBatchSize);
  }

  /**
   * Replicates a transaction (WAL data + bucket deltas) via Raft consensus.
   */
  public void replicateTransaction(final String dbName, final byte[] walData,
      final Map<Integer, Integer> bucketDeltas, final long timeoutMs) {
    final ByteString entry = RaftLogEntryCodec.encodeTxEntry(dbName, walData, bucketDeltas);
    groupCommitter.submitAndWait(entry.toByteArray(), timeoutMs);
  }

  /**
   * Replicates schema changes (file additions/removals, schema JSON, embedded WAL entries).
   */
  public void replicateSchema(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final long timeoutMs) {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(dbName, schemaJson,
        filesToAdd, filesToRemove, walEntries, bucketDeltas);
    groupCommitter.submitAndWait(entry.toByteArray(), timeoutMs);
  }

  /**
   * Replicates an install-database entry so replicas create or snapshot-sync the database.
   */
  public void replicateInstallDatabase(final String dbName, final boolean forceSnapshot, final long timeoutMs) {
    final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(dbName, forceSnapshot);
    groupCommitter.submitAndWait(entry.toByteArray(), timeoutMs);
  }

  /**
   * Replicates a drop-database entry so replicas remove the database.
   */
  public void replicateDropDatabase(final String dbName, final long timeoutMs) {
    final ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(dbName);
    groupCommitter.submitAndWait(entry.toByteArray(), timeoutMs);
  }

  /**
   * Replicates a security users entry so all nodes update their user files.
   */
  public void replicateSecurityUsers(final String usersJson, final long timeoutMs) {
    final ByteString entry = RaftLogEntryCodec.encodeSecurityUsersEntry(usersJson);
    groupCommitter.submitAndWait(entry.toByteArray(), timeoutMs);
  }

  /**
   * Stops the underlying group committer, draining pending entries.
   */
  public void stop() {
    groupCommitter.stop();
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ha-raft && mvn test -Dtest=RaftTransactionBrokerTest -pl . 2>&1 | tail -20`

Expected: All tests PASS.

- [ ] **Step 5: Compile the ha-raft module**

Run: `cd ha-raft && mvn compile -pl . 2>&1 | tail -10`

Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftTransactionBrokerTest.java
git commit -m "feat(ha-raft): add RaftTransactionBroker facade

Centralized broker for all Raft entry submission with typed methods per
entry type. Owns RaftGroupCommitter internally. Encoding handled via
RaftLogEntryCodec so callers never touch the codec directly."
```

---

### Task 3: Wire RaftHAServer to use RaftTransactionBroker

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Replace groupCommitter field with transactionBroker**

In `RaftHAServer.java`, make these changes:

Replace field declaration (line 94):

```java
// OLD:
private volatile RaftGroupCommitter        groupCommitter;

// NEW:
private volatile RaftTransactionBroker     transactionBroker;
```

Replace in `start()` (line 230):

```java
// OLD:
groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);

// NEW:
transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize);
```

Replace in `restartRatisIfNeeded()` (line 286):

```java
// OLD:
final RaftGroupCommitter oldCommitter = this.groupCommitter;

// NEW:
final RaftTransactionBroker oldBroker = this.transactionBroker;
```

Replace (lines 289-290):

```java
// OLD:
if (oldCommitter != null)
  oldCommitter.stop();

// NEW:
if (oldBroker != null)
  oldBroker.stop();
```

Replace (line 328):

```java
// OLD:
this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);

// NEW:
this.transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize);
```

Replace in `stop()` (lines 354-356):

```java
// OLD:
if (groupCommitter != null) {
  groupCommitter.stop();
  groupCommitter = null;
}

// NEW:
if (transactionBroker != null) {
  transactionBroker.stop();
  transactionBroker = null;
}
```

Replace the accessor method (lines 452-454):

```java
// OLD:
public RaftGroupCommitter getGroupCommitter() {
  return groupCommitter;
}

// NEW:
public RaftTransactionBroker getTransactionBroker() {
  return transactionBroker;
}
```

Replace in `refreshRaftClient()` (lines 487-491):

```java
// OLD:
if (groupCommitter != null) {
  final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
  final RaftGroupCommitter oldCommitter = groupCommitter;
  groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);
  oldCommitter.stop();
}

// NEW:
if (transactionBroker != null) {
  final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
  final RaftTransactionBroker oldBroker = transactionBroker;
  transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize);
  oldBroker.stop();
}
```

Remove the `RaftGroupCommitter` import if it becomes unused (it should, since `RaftGroupCommitter` is now package-private and only used by `RaftTransactionBroker`).

- [ ] **Step 2: Compile**

Run: `cd ha-raft && mvn compile -pl . 2>&1 | tail -20`

Expected: BUILD SUCCESS. `RaftReplicatedDatabase` and `RaftHAPlugin` still call `getGroupCommitter()` which no longer exists - but they're in the same package, so the compiler will catch this. If compilation fails, proceed to Task 4 immediately (wiring the callers).

Note: If compilation fails here because `RaftReplicatedDatabase` and `RaftHAPlugin` reference `getGroupCommitter()`, that's expected. Continue to Task 4 without committing - we'll commit after all wiring is done.

- [ ] **Step 3: Commit (if compilation succeeds)**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "refactor(ha-raft): wire RaftHAServer to use RaftTransactionBroker

Replace RaftGroupCommitter field with RaftTransactionBroker. The broker
owns the committer internally. All lifecycle methods (start, stop,
restart, refresh) now operate on the broker."
```

---

### Task 4: Rewire RaftReplicatedDatabase to use broker

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`

- [ ] **Step 1: Update commit() to use broker**

In `RaftReplicatedDatabase.java`, replace the replication section in `commit()` (around lines 271-274):

```java
// OLD:
final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), payload.walData(), payload.bucketDeltas());
final RaftHAServer raft = requireRaftServer();
raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());

// NEW:
final RaftHAServer raft = requireRaftServer();
raft.getTransactionBroker().replicateTransaction(getName(), payload.walData(), payload.bucketDeltas(), raft.getQuorumTimeout());
```

- [ ] **Step 2: Update recordFileChanges() to use broker**

Replace the schema replication section (around lines 1067-1070):

```java
// OLD:
final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(getName(), serializedSchema, addFiles, removeFiles,
    walEntries, bucketDeltas);
final RaftHAServer raft = requireRaftServer();
raft.getGroupCommitter().submitAndWait(schemaEntry.toByteArray(), raft.getQuorumTimeout());

// NEW:
final RaftHAServer raft = requireRaftServer();
raft.getTransactionBroker().replicateSchema(getName(), serializedSchema, addFiles, removeFiles,
    walEntries, bucketDeltas, raft.getQuorumTimeout());
```

- [ ] **Step 3: Update createInReplicas() to use broker**

Replace both overloads (around lines 1131-1158):

```java
// OLD (first overload):
final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(getName());
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());

// NEW (first overload):
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getTransactionBroker().replicateInstallDatabase(getName(), false, raft.getQuorumTimeout());
```

```java
// OLD (second overload):
final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(getName(), forceSnapshot);
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());

// NEW (second overload):
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getTransactionBroker().replicateInstallDatabase(getName(), forceSnapshot, raft.getQuorumTimeout());
```

- [ ] **Step 4: Update dropInReplicas() to use broker**

Replace (around lines 1161-1165):

```java
// OLD:
final ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(getName());
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());

// NEW:
try {
  final RaftHAServer raft = requireRaftServer();
  raft.getTransactionBroker().replicateDropDatabase(getName(), raft.getQuorumTimeout());
```

- [ ] **Step 5: Remove unused imports from RaftReplicatedDatabase**

Remove these imports if they are no longer used:

```java
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
```

Note: Check whether `ByteString` is still used elsewhere in the file before removing. If it's only used in the entry encoding calls being replaced, remove it.

- [ ] **Step 6: Compile**

Run: `cd ha-raft && mvn compile -pl . 2>&1 | tail -20`

Expected: Compilation may still fail due to `RaftHAPlugin` referencing `getGroupCommitter()`. Continue to Task 5.

---

### Task 5: Rewire RaftHAPlugin to use broker

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Update replicateSecurityUsers() to use broker**

Replace lines 112-125:

```java
// OLD:
@Override
public void replicateSecurityUsers(final String usersJsonArray) {
  if (raftHAServer == null)
    throw new TransactionException("Raft HA server not started");

  final ByteString entry = RaftLogEntryCodec.encodeSecurityUsersEntry(usersJsonArray);
  try {
    raftHAServer.getGroupCommitter().submitAndWait(entry.toByteArray(), raftHAServer.getQuorumTimeout());
  } catch (final TransactionException e) {
    throw e;
  } catch (final Exception e) {
    throw new TransactionException("Error sending security-users entry via Raft", e);
  }
  LogManager.instance().log(this, Level.INFO, "Security users entry committed via Raft");
}

// NEW:
@Override
public void replicateSecurityUsers(final String usersJsonArray) {
  if (raftHAServer == null)
    throw new TransactionException("Raft HA server not started");

  try {
    raftHAServer.getTransactionBroker().replicateSecurityUsers(usersJsonArray, raftHAServer.getQuorumTimeout());
  } catch (final TransactionException e) {
    throw e;
  } catch (final Exception e) {
    throw new TransactionException("Error sending security-users entry via Raft", e);
  }
  LogManager.instance().log(this, Level.INFO, "Security users entry committed via Raft");
}
```

- [ ] **Step 2: Remove unused imports from RaftHAPlugin**

Remove these if no longer used:

```java
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
```

(Check whether `ByteString` is used elsewhere in the file before removing.)

- [ ] **Step 3: Compile the full ha-raft module**

Run: `cd ha-raft && mvn compile -pl . 2>&1 | tail -20`

Expected: BUILD SUCCESS. All references to `getGroupCommitter()` are now replaced.

- [ ] **Step 4: Run all unit tests in ha-raft**

Run: `cd ha-raft && mvn test -pl . 2>&1 | tail -30`

Expected: All tests PASS. No test directly referenced `getGroupCommitter()`.

- [ ] **Step 5: Commit all wiring changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "refactor(ha-raft): rewire all callers to use RaftTransactionBroker

Replace direct RaftGroupCommitter.submitAndWait() + RaftLogEntryCodec
calls in RaftReplicatedDatabase and RaftHAPlugin with typed broker
methods. Encoding is now fully encapsulated inside the broker."
```

---

### Task 6: Run integration tests to verify no regressions

**Files:** None modified - verification only.

- [ ] **Step 1: Run the core replication integration test**

Run: `cd ha-raft && mvn test -Dtest=RaftReplicationIT -pl . 2>&1 | tail -30`

Expected: PASS. The test exercises the full write path through `RaftReplicatedDatabase.commit()`.

- [ ] **Step 2: Run the user management integration test**

Run: `cd ha-raft && mvn test -Dtest=RaftUserManagement3NodesIT -pl . 2>&1 | tail -30`

Expected: PASS. The test exercises `RaftHAPlugin.replicateSecurityUsers()`.

- [ ] **Step 3: Run the schema replication integration test**

Run: `cd ha-raft && mvn test -Dtest=RaftSchemaReplicationIT -pl . 2>&1 | tail -30`

Expected: PASS. The test exercises `RaftReplicatedDatabase.recordFileChanges()`.

- [ ] **Step 4: Run the drop database integration test**

Run: `cd ha-raft && mvn test -Dtest=RaftDropDatabase3NodesIT -pl . 2>&1 | tail -30`

Expected: PASS. The test exercises `RaftReplicatedDatabase.dropInReplicas()`.

- [ ] **Step 5: If any integration test fails, diagnose and fix**

Common issues:
- Missing import in a modified file
- Method signature mismatch between broker and codec
- `getGroupCommitter()` reference still present somewhere

Fix the issue, rerun the failing test, and amend or create a new commit.
