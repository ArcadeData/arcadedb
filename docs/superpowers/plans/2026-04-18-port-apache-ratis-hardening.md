# Port apache-ratis Hardening to ha-redesign - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port 6 feature areas from the `apache-ratis` branch (15 commits from 2026-04-17) to `ha-redesign`, adapting code to ha-redesign's refactored class structure.

**Architecture:** Execute in dependency order: Task 1 (GlobalConfiguration + edge-case fixes) first, then Task 2 (step-down hardening), then Tasks 3-6 in parallel (linearizable reads, snapshot hardening, idempotency cache, gRPC security). Each task is independently testable. All code adapts apache-ratis implementations to ha-redesign's naming (ArcadeStateMachine, RaftReplicatedDatabase, HAServerPlugin, SnapshotManager).

**Tech Stack:** Java 21, Apache Ratis 3.2.2, JUnit 5 + AssertJ, Maven

**Spec:** `docs/superpowers/specs/2026-04-18-port-apache-ratis-hardening-design.md`

**Reference branch:** Use `git show apache-ratis:<path>` to read source implementations.

---

## File Map

### New Production Files
- `server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java` - HTTP response cache for retry deduplication
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java` - gRPC transport filter for peer IP validation
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java` - Ratis gRPC customizer for installing filters

### New Test Files
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLinearizableReadIT.java` - Linearizable read integration test
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashBetweenCommitAndApplyIT.java` - Phase-2 crash test
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotCompressionRatioTest.java` - Decompression bomb unit test
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWatchdogTimeoutTest.java` - Watchdog floor computation test
- `server/src/test/java/com/arcadedb/server/http/IdempotencyCacheTest.java` - Cache unit tests
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilterTest.java` - Filter unit tests

### Modified Production Files
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - Add 8 new HA config keys
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` - Linearizable read methods + gRPC filter wiring
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java` - Step-down retry + linearizable read dispatch
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` - Configurable watchdog timeout
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java` - Compression ratio check + stale DB cleanup + leader refresh
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java` - Write timeout watchdog + HTTP warning
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java` - Accept GrpcServices.Customizer
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java` - Null type handling
- `server/src/main/java/com/arcadedb/server/HAServerPlugin.java` - Linearizable read interface methods
- `server/src/main/java/com/arcadedb/server/http/HttpServer.java` - IdempotencyCache lifecycle
- `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java` - X-Request-Id interception
- `network/src/main/java/com/arcadedb/remote/RemoteSchema.java` - Concurrent init safety

---

## Task 1: GlobalConfiguration + Edge-Case Fixes (Section 6 + config keys)

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java:634-636`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java`
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteSchema.java`

This task adds all new config entries and ports the small edge-case fixes that other tasks depend on.

- [ ] **Step 1: Add new GlobalConfiguration entries**

Insert after `HA_CLIENT_ELECTION_RETRY_DELAY_MS` (line 634) and before the `// POSTGRES` comment (line 636):

```java
  HA_STOP_SERVER_ON_REPLICATION_FAILURE("arcadedb.ha.stopServerOnReplicationFailure", SCOPE.SERVER,
      "If true, stops the JVM after exhausting step-down retries on a phase-2 replication failure. "
          + "If false, logs CRITICAL but leaves the server running (useful for debugging).",
      Boolean.class, true),

  HA_SNAPSHOT_WRITE_TIMEOUT("arcadedb.ha.snapshotWriteTimeout", SCOPE.SERVER,
      "Timeout in milliseconds for writing a snapshot to a follower. "
          + "If the transfer stalls beyond this duration, the connection is force-closed to free the semaphore slot.",
      Long.class, 300_000L),

  HA_SNAPSHOT_WATCHDOG_TIMEOUT("arcadedb.ha.snapshotWatchdogTimeout", SCOPE.SERVER,
      "Delay in milliseconds before the snapshot-gap watchdog triggers a download. "
          + "Floored at 4x HA_ELECTION_TIMEOUT_MAX to avoid premature firing on WAN clusters.",
      Long.class, 30_000L),

  HA_SNAPSHOT_GAP_TOLERANCE("arcadedb.ha.snapshotGapTolerance", SCOPE.SERVER,
      "Maximum acceptable gap between the snapshot index and persisted applied index before triggering a snapshot download.",
      Long.class, 10L),

  HA_SNAPSHOT_MAX_ENTRY_SIZE("arcadedb.ha.snapshotMaxEntrySize", SCOPE.SERVER,
      "Maximum uncompressed size in bytes for a single entry in a snapshot ZIP file. Protects against decompression bombs.",
      Long.class, 10_737_418_240L),

  HA_IDEMPOTENCY_CACHE_TTL_MS("arcadedb.ha.idempotencyCacheTtlMs", SCOPE.SERVER,
      "Time-to-live in milliseconds for entries in the HTTP idempotency cache.",
      Long.class, 60_000L),

  HA_IDEMPOTENCY_CACHE_MAX_ENTRIES("arcadedb.ha.idempotencyCacheMaxEntries", SCOPE.SERVER,
      "Maximum number of entries in the HTTP idempotency cache. Oldest entry is evicted when full.",
      Integer.class, 10_000),

  HA_GRPC_ALLOWLIST_REFRESH_MS("arcadedb.ha.grpcAllowlistRefreshMs", SCOPE.SERVER,
      "Rate-limiting interval in milliseconds for DNS re-resolution in the gRPC peer address allowlist filter.",
      Long.class, 30_000L),
```

- [ ] **Step 2: Port RaftLogEntryCodec null type handling**

Read the current `decode()` method in `RaftLogEntryCodec.java`. The method at line ~249 already handles `type == null` by returning a DecodedEntry with all nulls. Verify this by reading the file. If the null handling is already present (check line 249: `if (type == null)`), this sub-step is already done.

Also check `RaftLogEntryType.fromId()` - it should return null for unknown IDs instead of throwing. Read `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java` and verify it handles unknown byte values gracefully.

- [ ] **Step 3: Port RemoteSchema concurrent init safety**

Read the apache-ratis version for the exact changes:
```bash
git diff ha-redesign..apache-ratis -- network/src/main/java/com/arcadedb/remote/RemoteSchema.java
```

Apply these changes to `network/src/main/java/com/arcadedb/remote/RemoteSchema.java`:
1. Make `types` and `buckets` fields `volatile`
2. Make `reload()` method `synchronized`
3. Build new maps locally in `reload()`, then publish atomically via volatile field assignment
4. Use double-checked locking in `checkSchemaIsLoaded()`

- [ ] **Step 4: Port PostVerifyDatabaseHandler improvements**

Read the apache-ratis diff:
```bash
git diff ha-redesign..apache-ratis -- ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java
```

Apply the improvements from the diff to `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java`. Focus on:
- Improved error handling and validation
- Better logging for verification failures
- Any structural refactoring (the diff shows ~243 lines changed)

- [ ] **Step 5: Port KubernetesAutoJoin edge-case fixes**

Read the apache-ratis diff:
```bash
git diff ha-redesign..apache-ratis -- ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java
```

Apply the edge-case DNS resolution fixes to `ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java`.

- [ ] **Step 6: Port RetryStep fix**

Read the apache-ratis diff:
```bash
git diff ha-redesign..apache-ratis -- engine/src/main/java/com/arcadedb/query/sql/executor/RetryStep.java
```

The change adds `TimeoutException` to the catch clause alongside `NeedRetryException` because `TransactionManager`'s file-lock timeout during commit is transient and should be retried. Apply the fix.

- [ ] **Step 7: Build and verify**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests -pl engine,network,ha-raft,server
```

Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java \
       engine/src/main/java/com/arcadedb/query/sql/executor/RetryStep.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/PostVerifyDatabaseHandler.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java \
       network/src/main/java/com/arcadedb/remote/RemoteSchema.java
git commit -m "feat(ha): add config keys and edge-case fixes for apache-ratis port"
```

---

## Task 2: Step-Down Retry on Phase-2 Failure (Section 2)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:295-389`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashBetweenCommitAndApplyIT.java`

- [ ] **Step 1: Add TEST_POST_REPLICATION_HOOK field**

Add a static field near the top of `RaftReplicatedDatabase` class (after the existing static fields, around line 115):

```java
  /**
   * Test-only fault-injection hook. Fires after Raft replication succeeds but BEFORE phase-2
   * commit runs. Set to a non-null Consumer to simulate leader crash in this narrow window.
   * Always null in production.
   */
  static volatile Consumer<String> TEST_POST_REPLICATION_HOOK = null;
```

Add the import at the top of the file:
```java
import java.util.function.Consumer;
```

- [ ] **Step 2: Insert hook call after replication, before phase-2**

In the `commit()` method, after the replication try-catch block (line ~287) and before the phase-2 section (line ~288 `// --- PHASE 2`), insert:

```java
    // Test-only fault injection: simulate leader crash between replication and phase-2
    final Consumer<String> postReplicationHook = TEST_POST_REPLICATION_HOOK;
    if (postReplicationHook != null)
      postReplicationHook.accept(getName());
```

- [ ] **Step 3: Replace single step-down with retry loop in phase-2 catch block**

Replace the phase-2 catch block (lines 303-339) with a retry loop. The current code calls `stepDown()` once and spawns emergency stop on failure. Replace the entire catch block body (inside `catch (final Exception e)`) with:

```java
        if (e instanceof java.util.ConcurrentModificationException)
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication with a page version conflict (db=%s, txId=%s). "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());
        else
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication (db=%s, txId=%s). "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());

        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
        throw e;
```

- [ ] **Step 4: Extract recoverLeadershipAfterPhase2Failure as a separate method with retry loop**

Replace the existing step-down logic (currently inlined in the catch block) with a new private method. Place it after the `applyLocallyAfterMajorityCommit` method (after line 389):

```java
  private static final int    STEP_DOWN_MAX_RETRIES = 3;
  private static final long   STEP_DOWN_RETRY_DELAY_MS = 500;

  /**
   * Attempts to step down after a phase-2 failure. Retries up to {@link #STEP_DOWN_MAX_RETRIES}
   * times with {@link #STEP_DOWN_RETRY_DELAY_MS} delay between attempts. If all attempts fail
   * and {@code HA_STOP_SERVER_ON_REPLICATION_FAILURE} is true, stops the server on a separate
   * thread to avoid deadlock (caller may hold read lock).
   */
  private void recoverLeadershipAfterPhase2Failure(final String txDescription) {
    if (raftHAServer == null || !raftHAServer.isLeader())
      return;

    for (int attempt = 1; attempt <= STEP_DOWN_MAX_RETRIES; attempt++) {
      try {
        raftHAServer.stepDown();
        LogManager.instance().log(this, Level.WARNING,
            "Step-down succeeded on attempt %d/%d after phase-2 failure (db=%s, tx=%s)",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription);
        return;
      } catch (final Exception stepDownEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "Step-down attempt %d/%d failed after phase-2 failure (db=%s, tx=%s): %s",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription, stepDownEx.getMessage());
        if (attempt < STEP_DOWN_MAX_RETRIES) {
          try {
            Thread.sleep(STEP_DOWN_RETRY_DELAY_MS);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    // All step-down attempts failed
    final boolean stopServer = server.getConfiguration()
        .getValueAsBoolean(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE);
    if (stopServer) {
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). "
              + "Forcing server stop to prevent leader-follower divergence.",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
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
    } else {
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). "
              + "HA_STOP_SERVER_ON_REPLICATION_FAILURE=false, server continues in degraded state.",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
    }
  }
```

- [ ] **Step 5: Update applyLocallyAfterMajorityCommit to use the same retry method**

Replace the step-down logic in `applyLocallyAfterMajorityCommit()` (lines 364-383) with a call to the new method:

```java
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed during ALL-quorum recovery (db=%s, txId=%s). "
                + "Stepping down so a node with correct state takes over. Error: %s",
            getName(), payload.tx(), e.getMessage());
        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
```

Remove the duplicated try/catch for stepDown and the emergency stop thread from this method.

- [ ] **Step 6: Build and verify**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests -pl ha-raft
```

Expected: BUILD SUCCESS

- [ ] **Step 7: Write integration test for phase-2 crash window**

Read the apache-ratis version for reference:
```bash
git show apache-ratis:ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashBetweenCommitAndApplyIT.java
```

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashBetweenCommitAndApplyIT.java`. The test should:
1. Start a 3-node cluster
2. Set `RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK` to throw a RuntimeException (simulating crash)
3. Perform a write on the leader
4. Verify the hook fires and causes phase-2 to fail
5. Verify the leader steps down
6. Verify data is consistent on the new leader (followers applied the entry via Raft)
7. Clean up: set `TEST_POST_REPLICATION_HOOK = null` in a finally block

Adapt the test to use `BaseRaftHATest` (ha-redesign's test base class) instead of whatever base class apache-ratis uses.

- [ ] **Step 8: Run the test**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft -Dtest=RaftLeaderCrashBetweenCommitAndApplyIT
```

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderCrashBetweenCommitAndApplyIT.java
git commit -m "feat(ha): add step-down retry loop and TEST_POST_REPLICATION_HOOK for phase-2 failure"
```

---

## Task 3: Linearizable Reads on Followers (Section 1)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java:745-862`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:902-926`
- Modify: `server/src/main/java/com/arcadedb/server/HAServerPlugin.java:109-132`

- [ ] **Step 1: Add imports to RaftHAServer**

Add these imports to `RaftHAServer.java` (after the existing Ratis imports around line 36):

```java
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
```

- [ ] **Step 2: Add fetchReadIndex method to RaftHAServer**

Add after `getCurrentTerm()` method (after line ~775, before `getFollowerStates()`):

```java
  /**
   * Issues a Ratis {@code sendReadOnly} call and returns the committed log index.
   * This confirms the leader's lease is valid (leader-side) or returns the leader's
   * current committed index (follower-side).
   *
   * @param expectSelfIsLeader true when called from the leader (error = leadership lost),
   *                           false when called from a follower (error = leader unreachable)
   * @return the committed log index confirmed by the Raft quorum
   */
  public long fetchReadIndex(final boolean expectSelfIsLeader) {
    try {
      final RaftClientReply reply = raftClient.io().sendReadOnly(Message.EMPTY);
      if (reply.isSuccess())
        return reply.getLogIndex();

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (expectSelfIsLeader) {
          // We thought we were leader but we're not - throw with redirect hint
          final String leaderAddr = suggestedLeader != null ? suggestedLeader.getId().toString() : null;
          throw new ServerIsNotTheLeaderException("Lost leadership during ReadIndex", leaderAddr);
        }
        throw new ReplicationException("ReadIndex failed: leader unavailable");
      }
      throw new ReplicationException("ReadIndex failed: " + reply);
    } catch (final ServerIsNotTheLeaderException e) {
      throw e;
    } catch (final ReplicationException e) {
      throw e;
    } catch (final IOException e) {
      throw new ReplicationException("ReadIndex RPC failed: " + e.getMessage(), e);
    }
  }
```

Add the missing import if needed:
```java
import com.arcadedb.server.http.handler.ServerIsNotTheLeaderException;
```

- [ ] **Step 3: Add ensureLinearizableRead method (leader-side)**

Add after `fetchReadIndex()`:

```java
  /**
   * Ensures linearizable read consistency on the leader.
   * Issues a ReadIndex RPC to confirm the leader lease is still valid,
   * then waits for the local state machine to apply up to the confirmed index.
   */
  public void ensureLinearizableRead() {
    final long readIndex = fetchReadIndex(true);
    // Verify we're still leader after the ReadIndex confirmation
    if (!isLeader())
      throw new ServerIsNotTheLeaderException("Lost leadership after ReadIndex confirmation", getLeaderHttpAddress());
    waitForAppliedIndex(readIndex);
  }
```

- [ ] **Step 4: Add ensureLinearizableFollowerRead method (follower-side)**

Add after `ensureLinearizableRead()`:

```java
  /**
   * Ensures linearizable read consistency on a follower.
   * Queries the leader's current committed index via {@code sendReadOnly},
   * then waits for the local state machine to catch up to that index.
   * Cost: one follower-to-leader RTT + leader's quorum heartbeat + local apply lag.
   */
  public void ensureLinearizableFollowerRead() {
    final long readIndex = fetchReadIndex(false);
    waitForAppliedIndex(readIndex);
  }
```

- [ ] **Step 5: Add interface methods to HAServerPlugin**

Add before the `replicateSecurityUsers` method (before line 120 in `HAServerPlugin.java`):

```java
  /**
   * Ensures linearizable read consistency on the leader by confirming the Raft lease.
   */
  default void ensureLinearizableRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }

  /**
   * Ensures linearizable read consistency on a follower by querying the leader's committed index.
   */
  default void ensureLinearizableFollowerRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }
```

- [ ] **Step 6: Wire linearizable reads into RaftReplicatedDatabase.waitForReadConsistency()**

Replace the LINEARIZABLE branch in `waitForReadConsistency()` (lines 920-924) with:

```java
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (isLeader())
        raftHAServer.ensureLinearizableRead();
      else
        raftHAServer.ensureLinearizableFollowerRead();
    }
```

Also remove the early `if (isLeader()) return;` guard at line 902-903 since linearizable reads on leader now need the ReadIndex check.

The updated method should be:

```java
  private void waitForReadConsistency() {
    if (raftHAServer == null)
      return;

    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency();
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (!isLeader() && ctx.readAfterIndex() >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex());
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (isLeader())
        raftHAServer.ensureLinearizableRead();
      else
        raftHAServer.ensureLinearizableFollowerRead();
    }
  }
```

- [ ] **Step 7: Build and verify**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests -pl engine,server,ha-raft
```

Expected: BUILD SUCCESS

- [ ] **Step 8: Run existing read consistency tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft -Dtest="*ReadConsistency*"
```

Expected: PASS (existing tests should still work)

- [ ] **Step 9: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java \
       server/src/main/java/com/arcadedb/server/HAServerPlugin.java
git commit -m "feat(ha): add linearizable reads on followers via ReadIndex protocol"
```

---

## Task 4: Snapshot Hardening (Section 3)

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java:63-309`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:215-361`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:100,144-176`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotCompressionRatioTest.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWatchdogTimeoutTest.java`

### 4A: SnapshotHttpHandler Write Timeout Watchdog

- [ ] **Step 1: Add watchdog executor to SnapshotHttpHandler**

Read the apache-ratis implementation for reference:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java
```

Add fields after the `CONCURRENCY_SEMAPHORE` declaration (line ~64):

```java
  private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
      Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "arcadedb-snapshot-watchdog");
        t.setDaemon(true);
        return t;
      });

  private static volatile boolean plainHttpWarningLogged = false;
```

Add the import:
```java
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
```

- [ ] **Step 2: Add write timeout and plain HTTP warning to handleRequest**

Inside the `try` block that holds the semaphore (after semaphore acquire, before the snapshot streaming code), add the plain HTTP warning:

```java
      // Warn once about plain HTTP
      if (!exchange.getConnection().getSslSession() != null && !plainHttpWarningLogged) {
        plainHttpWarningLogged = true;
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot served over plain HTTP. Configure HTTPS to encrypt snapshot data in transit.");
      }
```

Actually, check the Undertow API for SSL detection. The correct check is:

```java
      if (exchange.getRequestScheme().equals("http") && !plainHttpWarningLogged) {
        plainHttpWarningLogged = true;
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot served over plain HTTP. Configure HTTPS to encrypt snapshot data in transit.");
      }
```

Then wrap the snapshot streaming code with a watchdog. Before the ZIP output stream write loop, arm the watchdog:

```java
      final long writeTimeoutMs = GlobalConfiguration.HA_SNAPSHOT_WRITE_TIMEOUT.getValueAsLong();
      final var connection = exchange.getConnection();
      final ScheduledFuture<?> watchdog = TIMEOUT_SCHEDULER.schedule(() -> {
        LogManager.instance().log(SnapshotHttpHandler.class, Level.WARNING,
            "Snapshot write timeout after %dms, force-closing connection for database '%s'",
            writeTimeoutMs, databaseName);
        try {
          connection.close();
        } catch (final Exception e) {
          // Best effort
        }
      }, writeTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
```

After the streaming completes (in the finally block), cancel the watchdog:

```java
      watchdog.cancel(false);
```

Read the exact location in the current `SnapshotHttpHandler.java` to determine where to place these. The watchdog must be armed before `streamDatabaseAsZip()` or the equivalent streaming call, and cancelled in the finally.

- [ ] **Step 3: Build and verify**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests -pl ha-raft
```

Expected: BUILD SUCCESS

### 4B: SnapshotInstaller Compression Ratio Check

- [ ] **Step 4: Add CountingInputStream inner class to SnapshotInstaller**

Read the apache-ratis implementation for reference:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java
```

Add at the bottom of `SnapshotInstaller.java` (before the closing brace):

```java
  /**
   * FilterInputStream that tracks the number of raw (compressed) bytes consumed.
   * Used to compute per-entry compression ratios for decompression bomb detection.
   */
  static final class CountingInputStream extends java.io.FilterInputStream {
    private long count;

    CountingInputStream(final java.io.InputStream in) {
      super(in);
    }

    long getCount() {
      return count;
    }

    void resetCount() {
      count = 0;
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b >= 0)
        count++;
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int n = super.read(b, off, len);
      if (n > 0)
        count += n;
      return n;
    }

    @Override
    public long skip(final long n) throws IOException {
      final long skipped = super.skip(n);
      if (skipped > 0)
        count += skipped;
      return skipped;
    }
  }
```

- [ ] **Step 5: Add compression ratio constants and check to copyWithLimit**

Add constants near the top of the class:

```java
  private static final int  MAX_COMPRESSION_RATIO = 200;
  private static final long MIN_RATIO_CHECK_BYTES = 64 * 1024; // 64KB
```

Modify `copyWithLimit()` (line 349) to accept an optional `CountingInputStream` parameter and add ratio checking:

```java
  private static void copyWithLimit(final InputStream in, final OutputStream out, final long maxBytes,
      final String entryName, final CountingInputStream counter) throws IOException {
    final byte[] buffer = new byte[8192];
    long total = 0;
    int len;
    while ((len = in.read(buffer)) > 0) {
      total += len;
      if (total > maxBytes)
        throw new IOException("Snapshot entry '" + entryName + "' exceeds max size of " + maxBytes + " bytes (decompression bomb?)");
      // Check compression ratio if we have enough compressed data
      if (counter != null && counter.getCount() >= MIN_RATIO_CHECK_BYTES) {
        final long ratio = total / Math.max(1, counter.getCount());
        if (ratio > MAX_COMPRESSION_RATIO)
          throw new IOException("Snapshot entry '" + entryName + "' has suspicious compression ratio " + ratio
              + ":1 (limit " + MAX_COMPRESSION_RATIO + ":1, decompression bomb?)");
      }
      out.write(buffer, 0, len);
    }
  }
```

Update all callers of `copyWithLimit()` to pass the `CountingInputStream` (or null if not available). Find the call sites in the ZIP extraction loop and wrap the ZipInputStream with a CountingInputStream.

- [ ] **Step 6: Write compression ratio unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotCompressionRatioTest.java`:

```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import java.io.*;
import static org.assertj.core.api.Assertions.*;

class SnapshotCompressionRatioTest {

  @Test
  void countingInputStreamTracksBytes() throws IOException {
    final byte[] data = new byte[1024];
    final var counting = new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(data));
    final byte[] buf = new byte[256];
    counting.read(buf);
    assertThat(counting.getCount()).isEqualTo(256);
    counting.read(buf);
    assertThat(counting.getCount()).isEqualTo(512);
  }

  @Test
  void countingInputStreamResetWorks() throws IOException {
    final byte[] data = new byte[512];
    final var counting = new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(data));
    counting.read(new byte[100]);
    assertThat(counting.getCount()).isEqualTo(100);
    counting.resetCount();
    assertThat(counting.getCount()).isEqualTo(0);
  }
}
```

- [ ] **Step 7: Run test**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft -Dtest=SnapshotCompressionRatioTest
```

Expected: PASS

### 4C: SnapshotInstaller Leader Refresh and Stale DB Cleanup

- [ ] **Step 7.1: Add leader address refresh to downloadWithRetry**

In `SnapshotInstaller.java`, modify the `downloadWithRetry()` method to refresh the leader address on each retry attempt. Read the apache-ratis version for the exact pattern:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java | grep -A20 "leaderAddr\|refreshLeader\|getLeaderHTTP"
```

The idea: before each retry iteration, resolve the current leader's HTTP address (it may have changed due to election). This requires passing a callback or reference to `RaftHAServer` that can provide the current leader address. On apache-ratis this is done by calling `raftHA.getLeaderHTTPAddress()` per attempt in the retry loop.

- [ ] **Step 7.2: Add syncDatabasesFromLeader method**

Read the apache-ratis implementation:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java | grep -B5 -A30 "syncDatabasesFromLeader"
```

Add the method to `SnapshotInstaller.java`. It should:
1. HTTP GET the leader's database list endpoint
2. Compare with local databases
3. Drop local databases not present on leader (with logging)
4. Call `install()` for databases that need snapshot sync

### 4D: ArcadeStateMachine Configurable Watchdog Timeout

- [ ] **Step 8: Replace hardcoded 30s with configurable timeout**

In `ArcadeStateMachine.java`, replace the hardcoded `SNAPSHOT_GAP_TOLERANCE` constant (line 100):

```java
  // Old:
  private static final long SNAPSHOT_GAP_TOLERANCE = 10;

  // New:
  private static final int WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER = 4;
```

In `reinitialize()` (line 150), replace the hardcoded gap tolerance:

```java
  // Old:
  if (persistedApplied >= 0 && snapshotIndex > persistedApplied + SNAPSHOT_GAP_TOLERANCE) {

  // New:
  final long gapTolerance = server != null ?
      server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE) :
      GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE.getValueAsLong();
  if (persistedApplied >= 0 && snapshotIndex > persistedApplied + gapTolerance) {
```

Replace the hardcoded `30_000` sleep (line 159) with the computed timeout:

```java
  // Old:
  Thread.sleep(30_000);

  // New:
  Thread.sleep(computeSnapshotWatchdogTimeoutMs());
```

And update the log message to show the actual timeout:

```java
  // Old:
  "Snapshot download watchdog: no leader change after 30s, triggering download directly"

  // New:
  "Snapshot download watchdog: no leader change after %dms, triggering download directly",
  computeSnapshotWatchdogTimeoutMs()
```

- [ ] **Step 9: Add computeSnapshotWatchdogTimeoutMs method**

Add after `reinitialize()`:

```java
  long computeSnapshotWatchdogTimeoutMs() {
    final long configured = server != null ?
        server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT) :
        GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.getValueAsLong();
    final int electionTimeoutMax = server != null ?
        server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX) :
        GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getValueAsInteger();
    final long floor = (long) Math.max(0, electionTimeoutMax) * WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER;
    return Math.max(configured, floor);
  }
```

- [ ] **Step 10: Write watchdog timeout unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWatchdogTimeoutTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SnapshotWatchdogTimeoutTest {

  @AfterEach
  void resetDefaults() {
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(30_000L);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(
        GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getDefValue());
  }

  @Test
  void watchdogUsesConfiguredValueWhenAboveFloor() {
    // With default election timeout (e.g., 2000ms), floor = 4*2000 = 8000ms
    // Configured 30000 > 8000 so configured wins
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(30_000L);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(2000);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.computeSnapshotWatchdogTimeoutMs()).isEqualTo(30_000L);
  }

  @Test
  void watchdogFlooredByElectionTimeout() {
    // Configured 5000 but floor = 4*5000 = 20000, so floor wins
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(5_000L);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(5000);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.computeSnapshotWatchdogTimeoutMs()).isEqualTo(20_000L);
  }
}
```

Note: This test creates an `ArcadeStateMachine` without a server, so `computeSnapshotWatchdogTimeoutMs()` reads from global config defaults. The method handles null server by reading defaults directly. Verify this works or adjust the test to use the static GlobalConfiguration values directly.

- [ ] **Step 11: Run tests and build**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft -Dtest="SnapshotCompressionRatioTest,SnapshotWatchdogTimeoutTest"
```

Expected: PASS

- [ ] **Step 12: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotCompressionRatioTest.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotWatchdogTimeoutTest.java
git commit -m "feat(ha): snapshot hardening - write timeout watchdog, compression ratio check, configurable watchdog"
```

---

## Task 5: IdempotencyCache (Section 4)

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/HttpServer.java:114-153`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java:78-180`
- Create: `server/src/test/java/com/arcadedb/server/http/IdempotencyCacheTest.java`

- [ ] **Step 1: Create IdempotencyCache class**

Read the apache-ratis implementation:
```bash
git show apache-ratis:server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java
```

Create `server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java` based on the apache-ratis version, adapting as needed. The key structure:

```java
package com.arcadedb.server.http;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdempotencyCache {

  public static final String HEADER_REQUEST_ID = "X-Request-Id";

  private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();
  private final long ttlMs;
  private final int  maxEntries;

  public IdempotencyCache(final long ttlMs, final int maxEntries) {
    this.ttlMs = ttlMs;
    this.maxEntries = maxEntries;
  }

  public CachedEntry get(final String requestId) {
    if (requestId == null)
      return null;
    final CachedEntry entry = cache.get(requestId);
    if (entry == null)
      return null;
    if (System.currentTimeMillis() - entry.timestampMs > ttlMs) {
      cache.remove(requestId);
      return null;
    }
    return entry;
  }

  public void putSuccess(final String requestId, final int statusCode, final String body,
      final byte[] binary, final String principal) {
    if (requestId == null || statusCode < 200 || statusCode >= 300)
      return;
    if (cache.size() >= maxEntries)
      evictOldest();
    cache.put(requestId, new CachedEntry(statusCode, body, binary, principal, System.currentTimeMillis()));
  }

  public void cleanupExpired() {
    final long now = System.currentTimeMillis();
    cache.entrySet().removeIf(e -> now - e.getValue().timestampMs > ttlMs);
  }

  public int size() {
    return cache.size();
  }

  private void evictOldest() {
    String oldestKey = null;
    long oldestTime = Long.MAX_VALUE;
    for (final Map.Entry<String, CachedEntry> e : cache.entrySet()) {
      if (e.getValue().timestampMs < oldestTime) {
        oldestTime = e.getValue().timestampMs;
        oldestKey = e.getKey();
      }
    }
    if (oldestKey != null)
      cache.remove(oldestKey);
  }

  public static class CachedEntry {
    public final int    statusCode;
    public final String body;
    public final byte[] binary;
    public final String principal;
    public final long   timestampMs;

    CachedEntry(final int statusCode, final String body, final byte[] binary,
        final String principal, final long timestampMs) {
      this.statusCode = statusCode;
      this.body = body;
      this.binary = binary;
      this.principal = principal;
      this.timestampMs = timestampMs;
    }
  }
}
```

- [ ] **Step 2: Write IdempotencyCache unit tests**

Create `server/src/test/java/com/arcadedb/server/http/IdempotencyCacheTest.java`:

```java
package com.arcadedb.server.http;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class IdempotencyCacheTest {

  @Test
  void putAndGetSuccessfulResponse() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess("req-1", 200, "{\"ok\":true}", null, "admin");

    final var entry = cache.get("req-1");
    assertThat(entry).isNotNull();
    assertThat(entry.statusCode).isEqualTo(200);
    assertThat(entry.body).isEqualTo("{\"ok\":true}");
    assertThat(entry.principal).isEqualTo("admin");
  }

  @Test
  void nonSuccessResponseNotCached() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess("req-err", 500, "error", null, "admin");
    assertThat(cache.get("req-err")).isNull();
  }

  @Test
  void nullRequestIdIgnored() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 100);
    cache.putSuccess(null, 200, "body", null, "admin");
    assertThat(cache.size()).isEqualTo(0);
  }

  @Test
  void expiredEntryRemoved() throws InterruptedException {
    final IdempotencyCache cache = new IdempotencyCache(50, 100); // 50ms TTL
    cache.putSuccess("req-exp", 200, "body", null, "admin");
    Thread.sleep(100);
    assertThat(cache.get("req-exp")).isNull();
  }

  @Test
  void evictsOldestWhenFull() {
    final IdempotencyCache cache = new IdempotencyCache(60_000, 2);
    cache.putSuccess("req-1", 200, "a", null, "admin");
    cache.putSuccess("req-2", 200, "b", null, "admin");
    cache.putSuccess("req-3", 200, "c", null, "admin");
    // req-1 should have been evicted
    assertThat(cache.get("req-1")).isNull();
    assertThat(cache.get("req-3")).isNotNull();
  }

  @Test
  void cleanupExpiredRemovesStaleEntries() throws InterruptedException {
    final IdempotencyCache cache = new IdempotencyCache(50, 100);
    cache.putSuccess("req-a", 200, "a", null, "admin");
    cache.putSuccess("req-b", 200, "b", null, "admin");
    Thread.sleep(100);
    cache.cleanupExpired();
    assertThat(cache.size()).isEqualTo(0);
  }
}
```

- [ ] **Step 3: Run tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl server -Dtest=IdempotencyCacheTest
```

Expected: PASS

- [ ] **Step 4: Wire IdempotencyCache into HttpServer lifecycle**

Add field to `HttpServer.java` (after line 122):

```java
  private final    IdempotencyCache       idempotencyCache;
  private final    ScheduledExecutorService idempotencyCleanup;
```

Add imports:
```java
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
```

In the constructor (after `leaderProxy` init, around line 132):

```java
    this.idempotencyCache = new IdempotencyCache(
        server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_TTL_MS),
        server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_IDEMPOTENCY_CACHE_MAX_ENTRIES));
    this.idempotencyCleanup = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-idempotency-cleanup");
      t.setDaemon(true);
      return t;
    });
    this.idempotencyCleanup.scheduleAtFixedRate(idempotencyCache::cleanupExpired, 30, 30, TimeUnit.SECONDS);
```

Add getter:

```java
  public IdempotencyCache getIdempotencyCache() {
    return idempotencyCache;
  }
```

In `stopService()` (after `authSessionManager.close()`, around line 152):

```java
    idempotencyCleanup.shutdownNow();
```

- [ ] **Step 5: Wire X-Request-Id interception into AbstractServerHttpHandler**

In `handleRequest()`, after the authentication block and payload parsing (around line 177, before calling `execute()`), add the cache check:

```java
      // Idempotency cache: replay cached response for POST retries with X-Request-Id
      final String requestId = exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID);
      if (requestId != null && "POST".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
        final IdempotencyCache cache = httpServer.getIdempotencyCache();
        final IdempotencyCache.CachedEntry cached = cache.get(requestId);
        if (cached != null) {
          // Verify principal matches
          final String currentPrincipal = user != null ? user.getName() : null;
          if (cached.principal == null || cached.principal.equals(currentPrincipal)) {
            exchange.setStatusCode(cached.statusCode);
            exchange.getResponseSender().send(cached.body != null ? cached.body : "");
            return;
          }
        }
      }
```

After the `execute()` call and successful response (around line 179, after `response.send(exchange)`), cache the result:

```java
      if (response != null) {
        response.send(exchange);
        // Cache successful POST responses for idempotency
        if (requestId != null && "POST".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
          final String principal = user != null ? user.getName() : null;
          httpServer.getIdempotencyCache().putSuccess(requestId, exchange.getStatusCode(),
              response.getBody(), null, principal);
        }
      }
```

Note: This requires adjusting the existing `response.send(exchange)` logic. Read the `ExecutionResponse` class to understand what `getBody()` returns and adapt accordingly. The response body may need to be captured before sending.

- [ ] **Step 6: Build and verify**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests -pl server
```

Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/IdempotencyCache.java \
       server/src/main/java/com/arcadedb/server/http/HttpServer.java \
       server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java \
       server/src/test/java/com/arcadedb/server/http/IdempotencyCacheTest.java
git commit -m "feat(ha): add HTTP idempotency cache for POST request deduplication"
```

---

## Task 6: PeerAddressAllowlistFilter + RaftGrpcServicesCustomizer (Section 5)

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilterTest.java`

- [ ] **Step 1: Create PeerAddressAllowlistFilter**

Read the apache-ratis implementation:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java
```

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java` based on the apache-ratis version. The key structure:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.ServerTransportFilter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

class PeerAddressAllowlistFilter extends ServerTransportFilter {

  private static final Set<String> LOOPBACK = Set.of("127.0.0.1", "::1", "0:0:0:0:0:0:0:1");

  private final String                    serverList;
  private final long                      refreshIntervalMs;
  private final AtomicReference<Set<String>> allowedIps = new AtomicReference<>(Set.of());
  private volatile long                   lastRefreshTime = 0;

  PeerAddressAllowlistFilter(final String serverList, final long refreshIntervalMs) {
    this.serverList = serverList;
    this.refreshIntervalMs = refreshIntervalMs;
    resolveNow();
  }

  @Override
  public Attributes transportReady(final Attributes transportAttrs) {
    final SocketAddress remote = transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (remote instanceof InetSocketAddress) {
      final String ip = ((InetSocketAddress) remote).getAddress().getHostAddress();
      if (LOOPBACK.contains(ip))
        return super.transportReady(transportAttrs);
      if (!allowedIps.get().contains(ip)) {
        // Cache miss: try refresh (rate-limited)
        if (System.currentTimeMillis() - lastRefreshTime > refreshIntervalMs)
          resolveNow();
        if (!allowedIps.get().contains(ip)) {
          LogManager.instance().log(this, Level.WARNING,
              "Rejected gRPC connection from non-peer address: %s", ip);
          throw new io.grpc.StatusRuntimeException(
              io.grpc.Status.PERMISSION_DENIED.withDescription("Not in peer allowlist: " + ip));
        }
      }
    }
    return super.transportReady(transportAttrs);
  }

  void resolveNow() {
    lastRefreshTime = System.currentTimeMillis();
    final Set<String> resolved = new HashSet<>();
    for (final String host : extractPeerHosts(serverList)) {
      try {
        for (final InetAddress addr : InetAddress.getAllByName(host))
          resolved.add(addr.getHostAddress());
      } catch (final UnknownHostException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Failed to resolve peer host '%s' for gRPC allowlist: %s", host, e.getMessage());
      }
    }
    allowedIps.set(Set.copyOf(resolved));
  }

  static Set<String> extractPeerHosts(final String serverList) {
    if (serverList == null || serverList.isBlank())
      return Set.of();
    final Set<String> hosts = new HashSet<>();
    for (final String entry : serverList.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;
      // Strip IPv6 brackets: [::1]:port -> ::1
      if (trimmed.startsWith("[")) {
        final int closeBracket = trimmed.indexOf(']');
        if (closeBracket > 0)
          trimmed = trimmed.substring(1, closeBracket);
      } else {
        // host:port -> host
        final int lastColon = trimmed.lastIndexOf(':');
        if (lastColon > 0)
          trimmed = trimmed.substring(0, lastColon);
      }
      hosts.add(trimmed);
    }
    return hosts;
  }
}
```

- [ ] **Step 2: Create RaftGrpcServicesCustomizer**

Read the apache-ratis implementation:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java
```

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java`:

```java
package com.arcadedb.server.ha.raft;

import io.grpc.ServerTransportFilter;
import io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.server.GrpcServices;

class RaftGrpcServicesCustomizer implements GrpcServices.Customizer {

  private final ServerTransportFilter[] filters;

  RaftGrpcServicesCustomizer(final ServerTransportFilter... filters) {
    this.filters = filters;
  }

  @Override
  public void customize(final NettyServerBuilder builder) {
    for (final ServerTransportFilter filter : filters)
      builder.addTransportFilter(filter);
  }
}
```

Note: Verify the exact `GrpcServices.Customizer` interface signature from the Ratis 3.2.2 API. The `customize` method may accept different parameters. Read the Ratis javadoc or source for the exact signature.

- [ ] **Step 3: Wire into RaftPropertiesBuilder**

Modify `RaftPropertiesBuilder.build()` to accept an optional customizer. Read the current file first:
```bash
cat ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java
```

Change the method signature and add customizer wiring. The exact mechanism depends on Ratis 3.2.2 API - the customizer may be set via `GrpcConfigKeys` or passed to the `Parameters` object. Read the apache-ratis RaftHAServer.start() to see how it was wired there:
```bash
git show apache-ratis:ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java | grep -A5 -B5 "Customizer\|customiz"
```

Add the customizer parameter and wiring based on what the apache-ratis branch does.

- [ ] **Step 4: Wire into RaftHAServer.start()**

In `RaftHAServer.start()`, before building the Ratis server, create the filter and customizer:

```java
    // gRPC peer address allowlist
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final long refreshMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_ALLOWLIST_REFRESH_MS);
    final PeerAddressAllowlistFilter allowlistFilter = new PeerAddressAllowlistFilter(serverList, refreshMs);
    final RaftGrpcServicesCustomizer grpcCustomizer = new RaftGrpcServicesCustomizer(allowlistFilter);
```

Pass `grpcCustomizer` to the properties builder or register it with the Ratis server builder as appropriate.

- [ ] **Step 5: Write PeerAddressAllowlistFilter unit tests**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilterTest.java`:

```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.assertj.core.api.Assertions.*;

class PeerAddressAllowlistFilterTest {

  @Test
  void extractPeerHostsFromServerList() {
    final Set<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("host1:2480,host2:2480,host3:2480");
    assertThat(hosts).containsExactlyInAnyOrder("host1", "host2", "host3");
  }

  @Test
  void extractPeerHostsHandlesIPv6() {
    final Set<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("[::1]:2480,[fe80::1]:2480");
    assertThat(hosts).containsExactlyInAnyOrder("::1", "fe80::1");
  }

  @Test
  void extractPeerHostsHandlesEmptyInput() {
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(null)).isEmpty();
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("")).isEmpty();
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts("  ")).isEmpty();
  }

  @Test
  void extractPeerHostsHandlesMixedFormats() {
    final Set<String> hosts = PeerAddressAllowlistFilter.extractPeerHosts("192.168.1.1:2480,[::1]:2480,myhost:2480");
    assertThat(hosts).containsExactlyInAnyOrder("192.168.1.1", "::1", "myhost");
  }
}
```

- [ ] **Step 6: Run tests and build**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft -Dtest=PeerAddressAllowlistFilterTest
```

Expected: PASS

```bash
mvn clean install -DskipTests -pl ha-raft
```

Expected: BUILD SUCCESS

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGrpcServicesCustomizer.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPropertiesBuilder.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
       ha-raft/src/test/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilterTest.java
git commit -m "feat(ha): add gRPC peer address allowlist filter for Raft port security"
```

---

## Task 7: Final Verification

- [ ] **Step 1: Full build**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn clean install -DskipTests
```

Expected: BUILD SUCCESS across all modules

- [ ] **Step 2: Run all HA tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl ha-raft
```

Expected: All tests PASS

- [ ] **Step 3: Run server tests**

```bash
cd /Users/frank/projects/arcade/worktrees/ha-redesign
mvn test -pl server -Dtest="IdempotencyCacheTest,*HttpHandler*"
```

Expected: All tests PASS
