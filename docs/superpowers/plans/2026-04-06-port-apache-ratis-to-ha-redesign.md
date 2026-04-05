# Port apache-ratis Improvements to ha-redesign - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port performance and correctness features (group committer, WAL compression, snapshot install, quorum enum, HALog, Studio UI) from the `apache-ratis` branch into the `ha-redesign` branch's `ha-raft/` module.

**Architecture:** Each item lands as one or more commits on `ha-redesign`. Clean break on all wire/disk formats (no backward compatibility). TDD: tests first, implementation second. Group committer is the throughput headline; snapshot install is the correctness headline.

**Tech Stack:** Java 21, Apache Ratis 3.2.0, LZ4 compression (via existing `CompressionFactory`), JUnit 5 + AssertJ, Undertow HTTP handlers.

**Spec:** `docs/superpowers/specs/2026-04-06-port-apache-ratis-to-ha-redesign-design.md`

---

### Task 0: Baseline Benchmark

**Files:**
- Read: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAInsertBenchmark.java`

- [ ] **Step 1: Run the existing benchmark and record baseline numbers**

```bash
cd ha-raft
mvn test -Dtest=RaftHAInsertBenchmark -pl . -DfailIfNoTests=false 2>&1 | tee ../results/benchmark-baseline.txt
```

Expected: benchmark completes, output shows throughput for single-node, 3-node sync, 3-node async scenarios. Save the output file for later comparison.

- [ ] **Step 2: Commit baseline results**

```bash
git add results/benchmark-baseline.txt
git commit -m "perf(ha-raft): capture group-commit baseline benchmark numbers"
```

---

### Task 1: HALog Utility

Moved ahead of other items because every subsequent task uses `HALog` for logging.

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HALog.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HALogTest.java`
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java:~574`

- [ ] **Step 1: Add `HA_LOG_VERBOSE` config entry to GlobalConfiguration**

In `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`, after the `HA_RAFT_SNAPSHOT_THRESHOLD` entry (around line 577), add:

```java
  HA_LOG_VERBOSE("arcadedb.ha.logVerbose", SCOPE.SERVER,
      "HA verbose logging level: 0=off, 1=basic (elections, leader changes), 2=detailed (replication, forwarding), 3=trace (every state machine apply)",
      Integer.class, 0),
```

- [ ] **Step 2: Write the failing test for HALog**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HALogTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HALogTest {

  @AfterEach
  void resetLogLevel() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
  }

  @Test
  void isEnabledReturnsFalseWhenLevelIsZero() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    assertThat(HALog.isEnabled(HALog.BASIC)).isFalse();
    assertThat(HALog.isEnabled(HALog.DETAILED)).isFalse();
    assertThat(HALog.isEnabled(HALog.TRACE)).isFalse();
  }

  @Test
  void isEnabledRespectsConfiguredLevel() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(2);
    assertThat(HALog.isEnabled(HALog.BASIC)).isTrue();
    assertThat(HALog.isEnabled(HALog.DETAILED)).isTrue();
    assertThat(HALog.isEnabled(HALog.TRACE)).isFalse();
  }

  @Test
  void logDoesNotThrowWhenDisabled() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(0);
    HALog.log(this, HALog.BASIC, "should not appear: %s", "test");
    // No exception = pass
  }

  @Test
  void logDoesNotThrowWhenEnabled() {
    GlobalConfiguration.HA_LOG_VERBOSE.setValue(3);
    HALog.log(this, HALog.TRACE, "trace message: %s %d", "test", 42);
    // No exception = pass
  }
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd ha-raft && mvn test -Dtest=HALogTest -pl .
```

Expected: FAIL - `HALog` class does not exist yet.

- [ ] **Step 4: Write HALog implementation**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HALog.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * HA verbose logging utility. Controlled by arcadedb.ha.logVerbose (0=off, 1=basic, 2=detailed, 3=trace).
 */
public final class HALog {

  /** Level 1: election, leader changes, replication start/complete, peer add/remove. */
  public static final int BASIC = 1;
  /** Level 2: command forwarding, WAL replication details, schema changes. */
  public static final int DETAILED = 2;
  /** Level 3: every state machine operation, entry parsing, serialization. */
  public static final int TRACE = 3;

  private HALog() {
  }

  public static boolean isEnabled(final int level) {
    return GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger() >= level;
  }

  public static void log(final Object caller, final int level, final String message, final Object... args) {
    if (GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger() >= level)
      LogManager.instance().log(caller, Level.INFO, "[HA-" + level + "] " + message, null, args);
  }

  public static void log(final Object caller, final int level, final String message, final Throwable exception,
      final Object... args) {
    if (GlobalConfiguration.HA_LOG_VERBOSE.getValueAsInteger() >= level)
      LogManager.instance().log(caller, Level.INFO, "[HA-" + level + "] " + message, exception, args);
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd ha-raft && mvn test -Dtest=HALogTest -pl .
```

Expected: PASS - all 4 tests green.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/HALog.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/HALogTest.java \
       engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "feat(ha-raft): add HALog verbose logging utility with configurable levels"
```

---

### Task 2: Quorum Enum

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/Quorum.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/QuorumTest.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java:112-131`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/QuorumTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuorumTest {

  @Test
  void parseMajority() {
    assertThat(Quorum.parse("majority")).isEqualTo(Quorum.MAJORITY);
    assertThat(Quorum.parse("MAJORITY")).isEqualTo(Quorum.MAJORITY);
  }

  @Test
  void parseAll() {
    assertThat(Quorum.parse("all")).isEqualTo(Quorum.ALL);
    assertThat(Quorum.parse("ALL")).isEqualTo(Quorum.ALL);
  }

  @Test
  void parseInvalidThrows() {
    assertThatThrownBy(() -> Quorum.parse("none"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("none");
  }

  @Test
  void parseEmptyThrows() {
    assertThatThrownBy(() -> Quorum.parse(""))
        .isInstanceOf(ConfigurationException.class);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ha-raft && mvn test -Dtest=QuorumTest -pl .
```

Expected: FAIL - `Quorum` class does not exist.

- [ ] **Step 3: Write Quorum enum**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/Quorum.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.ConfigurationException;

/**
 * Quorum modes supported for Raft HA replication.
 */
public enum Quorum {
  MAJORITY,
  ALL;

  public static Quorum parse(final String value) {
    return switch (value.toLowerCase()) {
      case "majority" -> MAJORITY;
      case "all" -> ALL;
      default -> throw new ConfigurationException(
          "Unsupported HA quorum '" + value + "'. Valid values: 'majority', 'all'");
    };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd ha-raft && mvn test -Dtest=QuorumTest -pl .
```

Expected: PASS.

- [ ] **Step 5: Wire Quorum into RaftHAServer and RaftHAPlugin**

In `RaftHAServer.java`, add fields and accessor:

```java
// Add to fields (after clusterMonitor):
private final Quorum quorum;
private final long   quorumTimeout;
```

In the constructor, after `this.clusterMonitor = ...`, add:

```java
this.quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));
this.quorumTimeout = configuration.getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
```

Add accessors:

```java
public Quorum getQuorum() {
  return quorum;
}

public long getQuorumTimeout() {
  return quorumTimeout;
}
```

In `RaftHAPlugin.java`, replace `validateConfiguration()` method (lines 112-131) with:

```java
private void validateConfiguration() {
  final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
  if (serverList == null || serverList.isEmpty())
    throw new RuntimeException("HA_SERVER_LIST must be configured for Raft HA");

  // Validate quorum early - will throw ConfigurationException for invalid values
  final Quorum quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));

  final int serverCount = serverList.split(",").length;
  if (quorum == Quorum.ALL && serverCount > 3)
    LogManager.instance().log(this, Level.WARNING,
        "HA_QUORUM=ALL with %d nodes: every node must acknowledge writes. A single slow node will throttle the cluster.", serverCount);
}
```

- [ ] **Step 6: Run existing tests to verify nothing breaks**

```bash
cd ha-raft && mvn test -Dtest="QuorumTest,RaftHAPluginTest,RaftHAServerTest,ConfigValidationTest" -pl .
```

Expected: PASS. Note: `ConfigValidationTest` or `RaftHAPluginTest` may have tests that rely on `"NONE"` quorum - fix those to use `"majority"` instead (clean break).

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/Quorum.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/QuorumTest.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(ha-raft): add Quorum enum replacing string-based HA_QUORUM config"
```

---

### Task 3: RaftGroupCommitter

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:128-172`
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`

- [ ] **Step 1: Add `HA_RAFT_GROUP_COMMIT_BATCH_SIZE` config**

In `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`, after `HA_LOG_VERBOSE`, add:

```java
  HA_RAFT_GROUP_COMMIT_BATCH_SIZE("arcadedb.ha.raftGroupCommitBatchSize", SCOPE.SERVER,
      "Maximum number of Raft log entries to batch in a single group commit flush. Higher values improve throughput under concurrent load.",
      Integer.class, 500),
```

- [ ] **Step 2: Write the failing unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.network.binary.QuorumNotReachedException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftGroupCommitterTest {

  @Test
  void stopDrainsQueueWithErrors() {
    // Create a committer with no RaftClient (null) - entries will fail on flush
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 10_000);

    // Submit an entry in a background thread
    final var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[]{1, 2, 3}, 5_000);
        return "success";
      } catch (final QuorumNotReachedException e) {
        return "failed: " + e.getMessage();
      }
    });

    // Give the background thread time to enqueue
    try { Thread.sleep(200); } catch (final InterruptedException ignored) {}

    // Stop should drain the queue and complete all futures with errors
    committer.stop();

    final String result = future.join();
    assertThat(result).startsWith("failed:");
  }
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd ha-raft && mvn test -Dtest=RaftGroupCommitterTest -pl .
```

Expected: FAIL - `RaftGroupCommitter` does not exist.

- [ ] **Step 4: Write RaftGroupCommitter implementation**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Groups multiple Raft entries into batched submissions to amortize gRPC round-trip cost.
 * Each transaction enqueues its entry and blocks; a background flusher collects all pending
 * entries and sends them via pipelined async calls, then notifies all waiting threads.
 */
public class RaftGroupCommitter {

  private final RaftClient                          raftClient;
  private final Quorum                              quorum;
  private final long                                quorumTimeout;
  private final int                                 maxBatchSize;
  private final LinkedBlockingQueue<PendingEntry>   queue    = new LinkedBlockingQueue<>();
  private final Thread                              flusher;
  private volatile boolean                          running  = true;

  public RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout) {
    this(raftClient, quorum, quorumTimeout, 500);
  }

  public RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize) {
    this.raftClient = raftClient;
    this.quorum = quorum;
    this.quorumTimeout = quorumTimeout;
    this.maxBatchSize = maxBatchSize;
    this.flusher = new Thread(this::flushLoop, "arcadedb-raft-group-committer");
    this.flusher.setDaemon(true);
    this.flusher.start();
  }

  /**
   * Enqueues a Raft entry and blocks until it is committed by the cluster.
   */
  public void submitAndWait(final byte[] entry, final long timeoutMs) {
    final PendingEntry pending = new PendingEntry(entry);
    queue.add(pending);

    try {
      final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
    } catch (final java.util.concurrent.TimeoutException e) {
      throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms");
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new QuorumNotReachedException("Group commit failed: " + e.getMessage());
    }
  }

  public void stop() {
    running = false;
    flusher.interrupt();
    PendingEntry pending;
    while ((pending = queue.poll()) != null)
      pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
  }

  private void flushLoop() {
    final List<PendingEntry> batch = new ArrayList<>(maxBatchSize);

    while (running) {
      try {
        final PendingEntry first = queue.poll(100, TimeUnit.MILLISECONDS);
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
        for (final PendingEntry p : batch)
          p.future.complete(e);
        batch.clear();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void flushBatch(final List<PendingEntry> batch) {
    if (raftClient == null) {
      final Exception err = new QuorumNotReachedException("RaftClient not available");
      for (final PendingEntry p : batch)
        p.future.complete(err);
      return;
    }

    final CompletableFuture<RaftClientReply>[] futures = new CompletableFuture[batch.size()];
    for (int i = 0; i < batch.size(); i++) {
      final Message msg = Message.valueOf(ByteString.copyFrom(batch.get(i).entry));
      futures[i] = raftClient.async().send(msg);
    }

    for (int i = 0; i < batch.size(); i++) {
      try {
        final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (reply.isSuccess()) {
          batch.get(i).future.complete(null);

          if (quorum == Quorum.ALL) {
            try {
              final RaftClientReply watchReply = raftClient.io().watch(
                  reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
              if (!watchReply.isSuccess())
                batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum not reached"));
            } catch (final Exception e) {
              batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum watch failed: " + e.getMessage()));
            }
          }
        } else {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
          batch.get(i).future.complete(new QuorumNotReachedException("Raft replication failed: " + err));
        }
      } catch (final Exception e) {
        batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + e.getMessage()));
      }
    }

    HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());
  }

  private static class PendingEntry {
    final byte[]                       entry;
    final CompletableFuture<Exception> future = new CompletableFuture<>();

    PendingEntry(final byte[] entry) {
      this.entry = entry;
    }
  }
}
```

- [ ] **Step 5: Run unit test to verify it passes**

```bash
cd ha-raft && mvn test -Dtest=RaftGroupCommitterTest -pl .
```

Expected: PASS.

- [ ] **Step 6: Wire RaftGroupCommitter into RaftHAServer**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`:

Add field:

```java
private RaftGroupCommitter groupCommitter;
```

At the end of `start()` method, after `raftClient = RaftClient.newBuilder()...build();` and the log message, add:

```java
final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);
```

In `stop()` method, before `if (raftClient != null)`, add:

```java
if (groupCommitter != null) {
  groupCommitter.stop();
  groupCommitter = null;
}
```

In `refreshRaftClient()`, after the new `raftClient` is built, replace the group committer:

```java
if (groupCommitter != null) {
  groupCommitter.stop();
  final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
  groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);
}
```

Add accessor:

```java
public RaftGroupCommitter getGroupCommitter() {
  return groupCommitter;
}
```

- [ ] **Step 7: Replace direct Raft sends in RaftReplicatedDatabase with group committer**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`:

Replace the commit path (lines 138-147):

```java
// OLD:
final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), walData, bucketDeltas);
final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(entry));
if (!reply.isSuccess())
  throw new TransactionException("Raft consensus failed for transaction commit on database '" + getName() + "'");
```

With:

```java
final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), walData, bucketDeltas);
raftHAServer.getGroupCommitter().submitAndWait(entry.toByteArray(), raftHAServer.getQuorumTimeout());
```

Replace the schema send path (lines 819-822):

```java
// OLD:
final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(schemaEntry));
if (!reply.isSuccess())
  throw new TransactionException("Raft consensus failed for schema change on database '" + getName() + "'");
```

With:

```java
raftHAServer.getGroupCommitter().submitAndWait(schemaEntry.toByteArray(), raftHAServer.getQuorumTimeout());
```

Replace the install-database send path (lines 888-890):

```java
// OLD:
final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(entry));
if (!reply.isSuccess())
  throw new TransactionException("Raft consensus failed while installing database '" + getName() + "' on replicas");
```

With:

```java
raftHAServer.getGroupCommitter().submitAndWait(entry.toByteArray(), raftHAServer.getQuorumTimeout());
```

Remove the `import org.apache.ratis.protocol.RaftClientReply;` and `import org.apache.ratis.protocol.Message;` if no longer needed.

- [ ] **Step 8: Add Ratis config tuning to RaftHAServer.start()**

In `RaftHAServer.start()`, after the existing snapshot threshold config (line 267), add:

```java
// AppendEntries batching: allow multiple entries per gRPC call to followers
RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, org.apache.ratis.util.SizeInBytes.valueOf("4MB"));
RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 256);

// Log segment and write buffer sizes
RaftServerConfigKeys.Log.setSegmentSizeMax(properties, org.apache.ratis.util.SizeInBytes.valueOf("64MB"));
RaftServerConfigKeys.Log.setWriteBufferSize(properties, org.apache.ratis.util.SizeInBytes.valueOf("8MB"));

// Leader lease: consistent reads without round-trip
RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
```

Add import: `import org.apache.ratis.util.SizeInBytes;`

- [ ] **Step 9: Run full ha-raft unit + integration tests**

```bash
cd ha-raft && mvn test -pl .
```

Expected: PASS. All existing tests should work since the group committer is transparent (same semantics, just batched).

- [ ] **Step 10: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftGroupCommitterTest.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java \
       engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "feat(ha-raft): add RaftGroupCommitter for batched Raft submissions"
```

---

### Task 4: Log Entry Compression

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java`

- [ ] **Step 1: Write a failing test for compressed roundtrip**

Add to `RaftLogEntryCodecTest.java`:

```java
@Test
void roundTripTxEntryCompressesWalData() {
  // Create a WAL-like payload with repetitive data that compresses well
  final byte[] walData = new byte[4096];
  for (int i = 0; i < walData.length; i++)
    walData[i] = (byte) (i % 10);

  final Map<Integer, Integer> bucketDeltas = Map.of(0, 5);

  final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("testdb", walData, bucketDeltas);
  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
  assertThat(decoded.walData()).isEqualTo(walData);
  assertThat(decoded.bucketRecordDelta()).isEqualTo(bucketDeltas);

  // Verify compression actually reduced size (repetitive data compresses well)
  // Encoded size should be smaller than raw walData + overhead
  assertThat(encoded.size()).isLessThan(walData.length);
}

@Test
void roundTripSchemaEntryWithEmbeddedWalCompresses() {
  final byte[] fakeWal = new byte[2048];
  java.util.Arrays.fill(fakeWal, (byte) 42);
  final Map<Integer, Integer> fakeDelta = Map.of(1, 5);

  final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb",
      "{\"schemaVersion\": 1}",
      Map.of(1, "User_0"),
      Map.of(),
      List.of(fakeWal),
      List.of(fakeDelta));

  final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

  assertThat(decoded.walEntries()).hasSize(1);
  assertThat(decoded.walEntries().get(0)).isEqualTo(fakeWal);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ha-raft && mvn test -Dtest=RaftLogEntryCodecTest -pl .
```

Expected: the new `roundTripTxEntryCompressesWalData` test will fail on the `assertThat(encoded.size()).isLessThan(walData.length)` assertion because compression is not yet implemented.

- [ ] **Step 3: Add compression to RaftLogEntryCodec**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`:

Add import:

```java
import com.arcadedb.compression.CompressionFactory;
```

In `encodeTxEntry()`, replace the WAL data writing section:

```java
// OLD:
dos.writeInt(walData.length);
dos.write(walData);
```

With:

```java
// Compress WAL data
final byte[] compressed = CompressionFactory.getDefault().compress(walData);
dos.writeInt(walData.length);       // uncompressed length (for decompression)
dos.writeInt(compressed.length);    // compressed length
dos.write(compressed);
```

In `decodeTxEntry()`, replace the WAL data reading:

```java
// OLD:
final int walLength = dis.readInt();
final byte[] walData = new byte[walLength];
dis.readFully(walData);
```

With:

```java
final int uncompressedLength = dis.readInt();
final int compressedLength = dis.readInt();
final byte[] compressed = new byte[compressedLength];
dis.readFully(compressed);
final byte[] walData = CompressionFactory.getDefault().decompress(compressed, uncompressedLength);
```

In `encodeSchemaEntry()`, replace each embedded WAL entry write in the walEntries loop:

```java
// OLD:
dos.writeInt(walData.length);
dos.write(walData);
```

With:

```java
final byte[] compressedWal = CompressionFactory.getDefault().compress(walData);
dos.writeInt(walData.length);         // uncompressed length
dos.writeInt(compressedWal.length);   // compressed length
dos.write(compressedWal);
```

In `decodeSchemaEntry()`, replace the WAL read inside the loop:

```java
// OLD:
final int walLength = dis.readInt();
final byte[] walData = new byte[walLength];
dis.readFully(walData);
```

With:

```java
final int walUncompressedLen = dis.readInt();
final int walCompressedLen = dis.readInt();
final byte[] walCompressed = new byte[walCompressedLen];
dis.readFully(walCompressed);
final byte[] walData = CompressionFactory.getDefault().decompress(walCompressed, walUncompressedLen);
```

- [ ] **Step 4: Run all codec tests**

```bash
cd ha-raft && mvn test -Dtest=RaftLogEntryCodecTest -pl .
```

Expected: PASS - all existing roundtrip tests still pass (they encode+decode, so compression/decompression is transparent), plus new compression-specific tests pass.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLogEntryCodecTest.java
git commit -m "feat(ha-raft): add LZ4 compression for WAL data in Raft log entries"
```

---

### Task 5: ArcadeStateMachine Upgrade (SimpleStateMachineStorage + Election Metrics)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/GetClusterHandler.java`

- [ ] **Step 1: Write failing tests for election metrics**

Add to `ArcadeStateMachineTest.java`:

```java
@Test
void electionCountStartsAtZero() {
  final ArcadeStateMachine sm = new ArcadeStateMachine();
  assertThat(sm.getElectionCount()).isZero();
  assertThat(sm.getLastElectionTime()).isZero();
}

@Test
void notifyLeaderChangedIncrementsElectionCount() {
  final ArcadeStateMachine sm = new ArcadeStateMachine();
  final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
      RaftPeerId.valueOf("peer-0"), RaftGroupId.valueOf(UUID.randomUUID()));

  sm.notifyLeaderChanged(memberId, RaftPeerId.valueOf("peer-1"));

  assertThat(sm.getElectionCount()).isEqualTo(1);
  assertThat(sm.getLastElectionTime()).isGreaterThan(0);
}

@Test
void multipleLeaderChangesIncrementCount() {
  final ArcadeStateMachine sm = new ArcadeStateMachine();
  final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
      RaftPeerId.valueOf("peer-0"), RaftGroupId.valueOf(UUID.randomUUID()));

  sm.notifyLeaderChanged(memberId, RaftPeerId.valueOf("peer-1"));
  sm.notifyLeaderChanged(memberId, RaftPeerId.valueOf("peer-2"));
  sm.notifyLeaderChanged(memberId, RaftPeerId.valueOf("peer-0"));

  assertThat(sm.getElectionCount()).isEqualTo(3);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ha-raft && mvn test -Dtest=ArcadeStateMachineTest -pl .
```

Expected: FAIL - `getElectionCount()` and `getLastElectionTime()` do not exist.

- [ ] **Step 3: Implement state machine upgrade**

In `ArcadeStateMachine.java`, replace the storage/tracking fields:

Remove:
- `private static final String LAST_APPLIED_FILE = "arcade-last-applied.txt";`
- `private volatile TermIndex lastAppliedTermIndex;`
- `private volatile Path lastAppliedFile;`
- The entire `persistLastApplied()` method

Add imports:

```java
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import java.util.concurrent.atomic.AtomicLong;
```

Add new fields:

```java
private final SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
private final AtomicLong                lastAppliedIndex = new AtomicLong(-1);
private final AtomicLong                electionCount    = new AtomicLong(0);
private volatile long                   lastElectionTime = 0;
private final long                      startTime        = System.currentTimeMillis();
```

Replace `initialize()`:

```java
@Override
public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
  super.initialize(raftServer, groupId, raftStorage);
  storage.init(raftStorage);
  reinitialize();
  LogManager.instance().log(this, Level.INFO, "ArcadeStateMachine initialized (groupId=%s)", groupId);
}
```

Add `reinitialize()` and `getStateMachineStorage()`:

```java
public void reinitialize() throws IOException {
  final var snapshotInfo = storage.getLatestSnapshot();
  if (snapshotInfo != null)
    lastAppliedIndex.set(snapshotInfo.getIndex());
  else
    lastAppliedIndex.set(-1);
}

@Override
public StateMachineStorage getStateMachineStorage() {
  return storage;
}
```

Update `applyTransaction()` to track index via atomic long:

Replace:

```java
lastAppliedTermIndex = termIndex;
persistLastApplied(termIndex);
```

With:

```java
lastAppliedIndex.set(termIndex.getIndex());
updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());
```

Replace `getLastAppliedTermIndex()` - use the parent class implementation by removing the override entirely (the `updateLastAppliedTermIndex` call above is sufficient).

Update `notifyLeaderChanged()` to add election metrics:

After the existing `super.notifyLeaderChanged(...)` call and before the null check, add:

```java
electionCount.incrementAndGet();
lastElectionTime = System.currentTimeMillis();
```

Add accessor methods:

```java
public long getElectionCount() {
  return electionCount.get();
}

public long getLastElectionTime() {
  return lastElectionTime;
}

public long getStartTime() {
  return startTime;
}
```

Update `takeSnapshot()`:

```java
@Override
public long takeSnapshot() {
  final long currentIndex = lastAppliedIndex.get();
  if (currentIndex < 0)
    return RaftLog.INVALID_LOG_INDEX;
  LogManager.instance().log(this, Level.FINE, "ArcadeStateMachine: snapshot checkpoint at index %d", currentIndex);
  return currentIndex;
}
```

- [ ] **Step 4: Update GetClusterHandler to include election metrics**

In `GetClusterHandler.java`, in the `execute()` method, after `response.put("leaderHttpAddress", ...)` and before the peers loop, add:

```java
final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
response.put("electionCount", stateMachine.getElectionCount());
response.put("lastElectionTime", stateMachine.getLastElectionTime());
response.put("uptime", System.currentTimeMillis() - stateMachine.getStartTime());
```

- [ ] **Step 5: Fix any test references to removed methods**

The `ArcadeStateMachineTest.getLastAppliedTermIndexInitiallyNull` test may need updating since we removed the override. Check if the base class returns null by default (it should).

- [ ] **Step 6: Run all state machine and handler tests**

```bash
cd ha-raft && mvn test -Dtest="ArcadeStateMachineTest,GetClusterHandlerIT" -pl .
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/GetClusterHandler.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineTest.java
git commit -m "feat(ha-raft): upgrade ArcadeStateMachine to SimpleStateMachineStorage with election metrics"
```

---

### Task 6: Snapshot Install (SnapshotHttpHandler + notifyInstallSnapshotFromLeader)

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFullSnapshotResyncIT.java`

- [ ] **Step 1: Add getPeerHttpAddress to RaftHAServer**

In `RaftHAServer.java`, add:

```java
/**
 * Returns the HTTP address for a peer, or null if not configured.
 */
public String getPeerHttpAddress(final RaftPeerId peerId) {
  return httpAddresses.get(peerId);
}
```

- [ ] **Step 2: Add Ratis snapshot notification config to RaftHAServer.start()**

In `RaftHAServer.start()`, after the existing snapshot threshold config:

```java
// Disable Ratis built-in snapshot transfer; use notification mode
// so ArcadeDB controls the snapshot transfer via HTTP
RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
```

- [ ] **Step 3: Write SnapshotHttpHandler**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * HTTP handler serving a consistent database snapshot as a ZIP file.
 * When a follower falls behind the compacted Raft log, it downloads
 * the database from the leader via this endpoint.
 *
 * Endpoint: GET /api/v1/ha/snapshot/{database}
 */
public class SnapshotHttpHandler implements HttpHandler {

  private final HttpServer httpServer;

  public SnapshotHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    if (exchange.isInIoThread()) {
      exchange.dispatch(this);
      return;
    }

    final ServerSecurityUser user = authenticate(exchange);
    if (user == null) {
      exchange.setStatusCode(401);
      exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic realm=\"ArcadeDB\"");
      exchange.getResponseSender().send("Unauthorized");
      return;
    }

    final String databaseName = exchange.getQueryParameters().get("database").getFirst();
    if (databaseName == null || databaseName.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("Missing 'database' parameter");
      return;
    }

    final var server = httpServer.getServer();
    if (!server.existsDatabase(databaseName)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database '" + databaseName + "' not found");
      return;
    }

    LogManager.instance().log(this, Level.INFO, "Serving database snapshot for '%s'...", databaseName);

    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/zip");
    exchange.getResponseHeaders().put(Headers.CONTENT_DISPOSITION,
        "attachment; filename=\"" + databaseName + "-snapshot.zip\"");
    exchange.startBlocking();

    final DatabaseInternal db = server.getDatabase(databaseName);

    db.executeInReadLock(() -> {
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        try (final OutputStream out = exchange.getOutputStream();
             final ZipOutputStream zipOut = new ZipOutputStream(out)) {

          final File configFile = ((LocalDatabase) db.getEmbedded()).getConfigurationFile();
          if (configFile.exists())
            addFileToZip(zipOut, configFile);

          final File schemaFile = ((LocalSchema) db.getSchema()).getConfigurationFile();
          if (schemaFile.exists())
            addFileToZip(zipOut, schemaFile);

          final Collection<ComponentFile> files = db.getFileManager().getFiles();
          for (final ComponentFile file : new ArrayList<>(files))
            if (file != null)
              addFileToZip(zipOut, file.getOSFile());

          zipOut.finish();
          HALog.log(this, HALog.BASIC, "Database snapshot for '%s' sent successfully", databaseName);

        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error serving snapshot for '%s'", e, databaseName);
          throw new RuntimeException(e);
        }
      });
      return null;
    });
  }

  private ServerSecurityUser authenticate(final HttpServerExchange exchange) {
    final HeaderValues authHeader = exchange.getRequestHeaders().get(Headers.AUTHORIZATION);
    if (authHeader == null || authHeader.isEmpty())
      return null;

    final String auth = authHeader.getFirst();
    if (auth.startsWith("Basic ")) {
      final String decoded = new String(Base64.getDecoder().decode(auth.substring(6)));
      final int colonPos = decoded.indexOf(':');
      if (colonPos > 0) {
        final String userName = decoded.substring(0, colonPos);
        final String password = decoded.substring(colonPos + 1);
        try {
          return httpServer.getServer().getSecurity().authenticate(userName, password, null);
        } catch (final Exception e) {
          return null;
        }
      }
    }
    return null;
  }

  private void addFileToZip(final ZipOutputStream zipOut, final File inputFile) throws Exception {
    if (!inputFile.exists())
      return;
    final ZipEntry entry = new ZipEntry(inputFile.getName());
    zipOut.putNextEntry(entry);
    try (final FileInputStream fis = new FileInputStream(inputFile)) {
      fis.transferTo(zipOut);
    }
    zipOut.closeEntry();
  }
}
```

- [ ] **Step 4: Register snapshot endpoint in RaftHAPlugin**

In `RaftHAPlugin.java`, in `registerAPI()` method, after the `/api/v1/cluster` registration:

```java
routes.addPrefixPath("/api/v1/ha/snapshot/", new SnapshotHttpHandler(httpServer));
LogManager.instance().log(this, Level.INFO, "Raft snapshot endpoint registered at /api/v1/ha/snapshot/{database}");
```

Add import: `import io.undertow.server.handlers.PathHandler;` (already present).

- [ ] **Step 5: Implement notifyInstallSnapshotFromLeader in ArcadeStateMachine**

In `ArcadeStateMachine.java`, add imports:

```java
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import com.arcadedb.database.LocalDatabase;
```

Add the override:

```java
@Override
public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
    final RaftProtos.RoleInfoProto roleInfoProto, final TermIndex firstTermIndexInLog) {

  LogManager.instance().log(this, Level.INFO,
      "Snapshot installation requested from leader (firstLogIndex=%s). Starting full resync...", firstTermIndexInLog);

  return CompletableFuture.supplyAsync(() -> {
    try {
      final RaftPeerId leaderId = RaftPeerId.valueOf(
          roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
      final String leaderHttpAddr = raftHAServer.getPeerHttpAddress(leaderId);

      if (leaderHttpAddr == null)
        throw new RuntimeException("Cannot determine leader HTTP address for snapshot download");

      final String clusterToken = server.getConfiguration().getValueAsString(
          com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);

      for (final String dbName : server.getDatabaseNames()) {
        LogManager.instance().log(this, Level.INFO,
            "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);
        installDatabaseSnapshot(dbName, leaderHttpAddr, clusterToken);
      }

      LogManager.instance().log(this, Level.INFO, "Full resync from leader completed");
      return firstTermIndexInLog;

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
      throw new RuntimeException("Error during Raft snapshot installation", e);
    }
  });
}

private void installDatabaseSnapshot(final String databaseName, final String leaderHttpAddr,
    final String clusterToken) throws IOException {

  final String snapshotUrl = "http://" + leaderHttpAddr + "/api/v1/ha/snapshot/" + databaseName;
  HALog.log(this, HALog.BASIC, "Downloading snapshot from %s", snapshotUrl);

  final java.net.HttpURLConnection connection;
  try {
    connection = (java.net.HttpURLConnection) new java.net.URI(snapshotUrl).toURL().openConnection();
  } catch (final java.net.URISyntaxException e) {
    throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
  }
  connection.setRequestMethod("GET");
  connection.setConnectTimeout(30_000);
  connection.setReadTimeout(300_000);

  // Authenticate with cluster token
  if (clusterToken != null && !clusterToken.isEmpty()) {
    final String auth = java.util.Base64.getEncoder().encodeToString(
        ("root:" + clusterToken).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + auth);
  }

  try {
    final int responseCode = connection.getResponseCode();
    if (responseCode != 200)
      throw new IOException("Failed to download snapshot: HTTP " + responseCode);

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
    final String databasePath = db.getDatabasePath();
    db.close();

    try (final java.util.zip.ZipInputStream zipIn = new java.util.zip.ZipInputStream(
        connection.getInputStream())) {
      java.util.zip.ZipEntry zipEntry;
      while ((zipEntry = zipIn.getNextEntry()) != null) {
        final java.io.File targetFile = new java.io.File(databasePath, zipEntry.getName());

        // Zip-slip protection
        if (!targetFile.getCanonicalPath().startsWith(new java.io.File(databasePath).getCanonicalPath()))
          throw new IOException("Zip slip detected in snapshot: " + zipEntry.getName());

        try (final java.io.FileOutputStream fos = new java.io.FileOutputStream(targetFile)) {
          zipIn.transferTo(fos);
        }
        zipIn.closeEntry();
      }
    }

    // Delete stale WAL files
    final java.io.File dbDir = new java.io.File(databasePath);
    final java.io.File[] walFiles = dbDir.listFiles((dir, name) -> name.endsWith(".wal"));
    if (walFiles != null)
      for (final java.io.File walFile : walFiles)
        walFile.delete();

    HALog.log(this, HALog.BASIC, "Snapshot for '%s' installed successfully", databaseName);

  } finally {
    connection.disconnect();
  }
}
```

- [ ] **Step 6: Enable the RaftFullSnapshotResyncIT test**

In `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFullSnapshotResyncIT.java`:

Remove the `@Disabled` annotation (line 53). Update the config to use `"majority"` instead of `"none"` for quorum (line 64):

```java
config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
```

- [ ] **Step 7: Run the snapshot resync test**

```bash
cd ha-raft && mvn test -Dtest=RaftFullSnapshotResyncIT -pl .
```

Expected: PASS - the test should now work end-to-end.

- [ ] **Step 8: Run full ha-raft test suite**

```bash
cd ha-raft && mvn test -pl .
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFullSnapshotResyncIT.java
git commit -m "feat(ha-raft): wire snapshot install to Ratis with HTTP-based database transfer"
```

---

### Task 7: HALog Callsite Migration

Replace `Level.FINE` debug logging in ha-raft classes with `HALog` calls.

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

- [ ] **Step 1: Replace Level.FINE logs in ArcadeStateMachine**

Replace all `LogManager.instance().log(this, Level.FINE, ...)` calls in `ArcadeStateMachine.java` with appropriate `HALog.log()` calls:

- `"Skipping tx apply on leader..."` -> `HALog.log(this, HALog.TRACE, ...)`
- `"Applying tx %d to database..."` -> `HALog.log(this, HALog.DETAILED, ...)`
- `"Skipping schema apply on leader..."` -> `HALog.log(this, HALog.TRACE, ...)`
- `"Applying schema entry..."` -> `HALog.log(this, HALog.DETAILED, ...)`
- `"Applied %d buffered WAL..."` -> `HALog.log(this, HALog.DETAILED, ...)`
- `"Applied schema change..."` -> `HALog.log(this, HALog.DETAILED, ...)`
- `"Database '%s' already present..."` -> `HALog.log(this, HALog.TRACE, ...)`
- Snapshot checkpoint log -> `HALog.log(this, HALog.BASIC, ...)`

- [ ] **Step 2: Replace Level.FINE logs in RaftReplicatedDatabase**

Replace `LogManager.instance().log(this, Level.FINE, ...)` calls:

- Schema replication log -> `HALog.log(this, HALog.DETAILED, ...)`

- [ ] **Step 3: Replace Level.FINE logs in RaftHAServer**

Replace `LogManager.instance().log(this, Level.FINE, ...)` calls:

- Lag check error -> `HALog.log(this, HALog.TRACE, ...)`

- [ ] **Step 4: Run tests**

```bash
cd ha-raft && mvn test -pl .
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "refactor(ha-raft): migrate debug logging to HALog with configurable verbosity"
```

---

### Task 8: Studio Cluster Monitor UI

**Files:**
- Modify: `studio/src/main/resources/static/cluster.html`
- Modify: `studio/src/main/resources/static/js/studio-cluster.js`

- [ ] **Step 1: Extract the apache-ratis Studio changes**

```bash
git diff main..apache-ratis -- studio/src/main/resources/static/cluster.html > /tmp/cluster-html.patch
git diff main..apache-ratis -- studio/src/main/resources/static/js/studio-cluster.js > /tmp/cluster-js.patch
```

- [ ] **Step 2: Apply the patches to ha-redesign**

```bash
git apply --3way /tmp/cluster-html.patch
git apply --3way /tmp/cluster-js.patch
```

If the patches don't apply cleanly, manually port the changes by reading the diff and applying them. The key changes are:
- `cluster.html`: replace the simple server info div with a topology diagram, leader badge, follower cards with lag meters, election count display
- `studio-cluster.js`: add polling for `/api/v1/cluster`, parse the JSON response, render leader/follower topology, show election count and uptime

- [ ] **Step 3: Review for API compatibility**

Check that the JS code references the correct JSON field names from `GetClusterHandler`:
- `implementation`, `clusterName`, `localPeerId`, `isLeader`, `leaderId`, `leaderHttpAddress`, `peers`, `electionCount`, `lastElectionTime`, `uptime`

Fix any mismatched field names (apache-ratis may have used slightly different names).

- [ ] **Step 4: Commit**

```bash
git add studio/src/main/resources/static/cluster.html \
       studio/src/main/resources/static/js/studio-cluster.js
git commit -m "feat(studio): port enhanced cluster monitor UI from apache-ratis branch"
```

---

### Task 9: Test Ports (ReadConsistencyIT)

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReadConsistencyIT.java`

- [ ] **Step 1: Write the ReadConsistencyIT test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReadConsistencyIT.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that reads on follower nodes return data consistent with
 * the leader's committed state (linearizable reads via leader lease).
 */
class RaftReadConsistencyIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerReadsAreConsistentWithLeaderWrites() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Find a follower
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Write on the leader
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ReadConsistency"))
        leaderDb.getSchema().createVertexType("ReadConsistency");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = leaderDb.newVertex("ReadConsistency");
        v.set("index", i);
        v.save();
      }
    });

    // Wait for replication
    assertClusterConsistency();

    // Read from follower
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long count = followerDb.countType("ReadConsistency", true);
    assertThat(count).as("Follower should see all 100 records written on leader").isEqualTo(100);

    // Write more on leader, verify follower catches up
    leaderDb.transaction(() -> {
      for (int i = 100; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("ReadConsistency");
        v.set("index", i);
        v.save();
      }
    });

    assertClusterConsistency();

    final long countAfter = followerDb.countType("ReadConsistency", true);
    assertThat(countAfter).as("Follower should see all 200 records after second batch").isEqualTo(200);
  }
}
```

- [ ] **Step 2: Run the test**

```bash
cd ha-raft && mvn test -Dtest=RaftReadConsistencyIT -pl .
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReadConsistencyIT.java
git commit -m "test(ha-raft): add RaftReadConsistencyIT for linearizable read verification"
```

---

### Task 10: Full Validation

- [ ] **Step 1: Run the complete ha-raft test suite**

```bash
cd ha-raft && mvn test -pl .
```

Expected: ALL PASS.

- [ ] **Step 2: Run the e2e-ha chaos suite**

```bash
cd e2e-ha && mvn test -pl .
```

Expected: ALL PASS (NetworkPartitionIT, PacketLossIT, RollingRestartIT, SplitBrainIT, etc.).

- [ ] **Step 3: Run the benchmark and compare with baseline**

```bash
cd ha-raft && mvn test -Dtest=RaftHAInsertBenchmark -pl . -DfailIfNoTests=false 2>&1 | tee ../results/benchmark-after-group-commit.txt
```

Compare `results/benchmark-after-group-commit.txt` against `results/benchmark-baseline.txt`. The 3-node async throughput should show meaningful improvement from group commit batching.

- [ ] **Step 4: Build entire project to verify no cross-module breakage**

```bash
mvn clean install -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit benchmark results**

```bash
git add results/benchmark-after-group-commit.txt
git commit -m "perf(ha-raft): capture post-group-commit benchmark numbers"
```
