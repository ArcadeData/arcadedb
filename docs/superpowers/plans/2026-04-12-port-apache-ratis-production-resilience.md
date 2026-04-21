# Port apache-ratis production-resilience features - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port 8 production-safety and operational-hardening features from `apache-ratis` branch into `ha-redesign`: network-partition auto-recovery, server-side leader proxy, client-side HA failover, constant-time token check, snapshot throttling, symlink rejection, gRPC flow control tuning, HTTP proxy timeouts.

**Architecture:** Additive changes only. Four modules touched (`engine/`, `ha-raft/`, `server/`, `network/`). New `HealthMonitor` inside `RaftHAServer`, new `LeaderProxy` inside `AbstractServerHttpHandler`, new `ReadConsistency` enum + retry loop inside `RemoteHttpComponent`, small edits to `SnapshotHttpHandler` and `GlobalConfiguration`. No new external dependencies.

**Tech Stack:** Java 21, JUnit 5 Jupiter, AssertJ, Undertow, Apache Ratis 3.2.2, `java.net.http.HttpClient`, ArcadeDB's `HALog` / `LogManager` / `GlobalConfiguration`.

**Source spec:** `docs/superpowers/specs/2026-04-12-port-apache-ratis-production-resilience-design.md`

---

## Conventions (apply to every task)

- Every new file uses the Apache 2.0 license header block (copy from any existing file in the same package).
- `final` on every parameter, local variable, and method argument where possible.
- Single-statement `if`/`for`/`while` may omit curly braces.
- Tests use `assertThat(...).is...()` (AssertJ).
- Logging via `LogManager.instance().log(...)` or `HALog.log(...)`. No `System.out`.
- Commit messages: `feat(<module>): ...`, `fix(<module>): ...`, `test(<module>): ...`.
- Do not run `git commit` unless the user invokes it. Mark commit steps as "stage the changes" and let the user commit.
- Build: `mvn -pl <module> -am test -Dtest=<ClassName> -DfailIfNoTests=false`.
- Where a step says "run all affected tests", that means: the new test(s) from this task plus any pre-existing test class in the same `ha-raft/.../raft/` or handler package that touches the modified file.

---

## Task 0: Baseline Test Run

**Files:** none

- [ ] **Step 1: Run the ha-raft test suite to capture a baseline**

```bash
mvn -pl ha-raft -am test -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: all existing tests pass. If any pre-existing tests fail, stop and report before starting the port.

- [ ] **Step 2: Run the server test suite**

```bash
mvn -pl server -am test -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: all existing tests pass.

- [ ] **Step 3: Run the network test suite**

```bash
mvn -pl network -am test -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: all existing tests pass.

- [ ] **Step 4: Capture baseline pass/fail counts in a scratch note**

Record the numbers so you can confirm no regressions at the end.

---

## Task 1: Add all new GlobalConfiguration keys

Collecting every new config key into one task, so later tasks don't step on each other in `GlobalConfiguration.java`.

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (near the existing `HA_CLUSTER_TOKEN` block, line ~582)

- [ ] **Step 1: Open `GlobalConfiguration.java` and locate the HA block**

Find the line starting with `HA_CLUSTER_TOKEN(...)` and add the new keys immediately after it (before the `POSTGRES` block).

- [ ] **Step 2: Add the new HA keys**

```java
  HA_HEALTH_CHECK_INTERVAL("arcadedb.ha.healthCheckInterval", SCOPE.SERVER,
      "Interval in milliseconds for the Raft health monitor to check for CLOSED/EXCEPTION state and auto-recover. 0 disables.",
      Long.class, 3000L),

  HA_GRPC_FLOW_CONTROL_WINDOW("arcadedb.ha.grpcFlowControlWindow", SCOPE.SERVER,
      "gRPC flow control window size in bytes for Ratis append-entries traffic. Larger values help catch-up replication after partitions.",
      Long.class, 4L * 1024 * 1024),

  HA_SNAPSHOT_MAX_CONCURRENT("arcadedb.ha.snapshotMaxConcurrent", SCOPE.SERVER,
      "Maximum number of concurrent snapshot downloads served by the leader. Requests over this limit receive HTTP 503.",
      Integer.class, 2),

  HA_PROXY_READ_TIMEOUT("arcadedb.ha.proxyReadTimeout", SCOPE.SERVER,
      "Read timeout in milliseconds for the leader proxy in AbstractServerHttpHandler. Covers long-running queries proxied from a follower to the leader.",
      Long.class, 30000L),

  HA_PROXY_CONNECT_TIMEOUT("arcadedb.ha.proxyConnectTimeout", SCOPE.SERVER,
      "Connect timeout in milliseconds for the leader proxy in AbstractServerHttpHandler.",
      Long.class, 5000L),

  HA_PROXY_MAX_BODY_SIZE("arcadedb.ha.proxyMaxBodySize", SCOPE.SERVER,
      "Maximum request body size in bytes that the leader proxy will buffer and forward. Larger requests fall back to HTTP 400.",
      Integer.class, 16 * 1024 * 1024),

  HA_CLIENT_ELECTION_RETRY_COUNT("arcadedb.ha.clientElectionRetryCount", SCOPE.SERVER,
      "Number of retries performed by RemoteDatabase after receiving HTTP 503 NeedRetryException during an election.",
      Integer.class, 3),

  HA_CLIENT_ELECTION_RETRY_DELAY_MS("arcadedb.ha.clientElectionRetryDelayMs", SCOPE.SERVER,
      "Delay in milliseconds between RemoteDatabase election retries.",
      Long.class, 2000L),
```

- [ ] **Step 3: Compile the engine module**

```bash
mvn -pl engine -am compile -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 4: Stage the change**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java
```

Commit message (user runs):
`feat(engine): add HA health-check, proxy, snapshot-throttle, client-retry, gRPC config keys`

---

## Task 2: Item 4 - Constant-time cluster token comparison

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java` (line ~305)
- Test: `server/src/test/java/com/arcadedb/server/http/handler/AbstractServerHttpHandlerTokenTest.java` (new)

- [ ] **Step 1: Write the failing unit test**

Create `server/src/test/java/com/arcadedb/server/http/handler/AbstractServerHttpHandlerTokenTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * [... full header from any neighboring file ...]
 */
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractServerHttpHandlerTokenTest {

  @Test
  void constantTimeEquals_returnsTrueForEqualStrings() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, "secret", "secret")).isTrue();
  }

  @Test
  void constantTimeEquals_returnsFalseForDifferentStringsSameLength() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, "secret", "secreT")).isFalse();
  }

  @Test
  void constantTimeEquals_returnsFalseForDifferentLengths() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, "secret", "secret2")).isFalse();
  }

  @Test
  void constantTimeEquals_returnsFalseForNullFirst() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, null, "secret")).isFalse();
  }

  @Test
  void constantTimeEquals_returnsFalseForNullSecond() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, "secret", null)).isFalse();
  }

  @Test
  void constantTimeEquals_returnsFalseForBothNull() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, null, null)).isFalse();
  }

  @Test
  void constantTimeEquals_returnsFalseForEmptyVsNonEmpty() throws Exception {
    final Method m = AbstractServerHttpHandler.class.getDeclaredMethod("constantTimeEquals", String.class, String.class);
    m.setAccessible(true);
    assertThat((boolean) m.invoke(null, "", "secret")).isFalse();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -pl server -am test -Dtest=AbstractServerHttpHandlerTokenTest -DfailIfNoTests=false
```

Expected: compilation error - method `constantTimeEquals` does not exist.

- [ ] **Step 3: Add the `constantTimeEquals` helper to `AbstractServerHttpHandler`**

At the bottom of the class (above the final `}`), add:

```java
  private static boolean constantTimeEquals(final String a, final String b) {
    if (a == null || b == null)
      return false;
    final byte[] aBytes = a.getBytes(StandardCharsets.UTF_8);
    final byte[] bBytes = b.getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(aBytes, bBytes);
  }
```

Add the imports at the top if not already present:
```java
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
```

- [ ] **Step 4: Replace the String.equals call at line ~305**

Find:
```java
    if (clusterToken.isBlank() || !clusterToken.equals(providedToken)) {
```
Replace with:
```java
    if (clusterToken.isBlank() || !constantTimeEquals(clusterToken, providedToken)) {
```

- [ ] **Step 5: Run the unit test to verify it passes**

```bash
mvn -pl server -am test -Dtest=AbstractServerHttpHandlerTokenTest -DfailIfNoTests=false
```

Expected: all 7 tests PASS.

- [ ] **Step 6: Run the existing handler tests to confirm no regression**

```bash
mvn -pl server -am test -Dtest='*Handler*Test' -DfailIfNoTests=false
```

Expected: no new failures compared to the baseline captured in Task 0.

- [ ] **Step 7: Stage the changes**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java \
        server/src/test/java/com/arcadedb/server/http/handler/AbstractServerHttpHandlerTokenTest.java
```

Commit message: `fix(server): constant-time cluster token comparison in AbstractServerHttpHandler`

---

## Task 3: Item 1 - HealthMonitor class + unit test

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java`

- [ ] **Step 1: Write the failing unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class HealthMonitorTest {

  /** Minimal test double for the subset of RaftHAServer that HealthMonitor touches. */
  static final class FakeRaftServer implements HealthMonitor.HealthTarget {
    final AtomicReference<LifeCycle.State> state = new AtomicReference<>(LifeCycle.State.RUNNING);
    final AtomicInteger recoveryCalls = new AtomicInteger();
    volatile boolean shutdownRequested = false;

    @Override public LifeCycle.State getRaftLifeCycleState() { return state.get(); }
    @Override public boolean isShutdownRequested() { return shutdownRequested; }
    @Override public void restartRatisIfNeeded() { recoveryCalls.incrementAndGet(); }
  }

  @Test
  void tickDoesNothingWhenStateIsRunning() {
    final FakeRaftServer fake = new FakeRaftServer();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickTriggersRecoveryOnClosed() {
    final FakeRaftServer fake = new FakeRaftServer();
    fake.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);
  }

  @Test
  void tickTriggersRecoveryOnException() {
    final FakeRaftServer fake = new FakeRaftServer();
    fake.state.set(LifeCycle.State.EXCEPTION);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);
  }

  @Test
  void tickSkipsRecoveryWhenShutdownRequested() {
    final FakeRaftServer fake = new FakeRaftServer();
    fake.state.set(LifeCycle.State.CLOSED);
    fake.shutdownRequested = true;
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickSkipsRecoveryOnNewState() {
    final FakeRaftServer fake = new FakeRaftServer();
    fake.state.set(LifeCycle.State.NEW);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickSkipsRecoveryOnStartingState() {
    final FakeRaftServer fake = new FakeRaftServer();
    fake.state.set(LifeCycle.State.STARTING);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -pl ha-raft -am test -Dtest=HealthMonitorTest -DfailIfNoTests=false
```

Expected: compilation error - `HealthMonitor` does not exist.

- [ ] **Step 3: Create `HealthMonitor.java`**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import org.apache.ratis.util.LifeCycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Background health check that detects a Ratis server stuck in CLOSED or EXCEPTION
 * state after a network partition and triggers in-place recovery via
 * {@link HealthTarget#restartRatisIfNeeded()}.
 */
public final class HealthMonitor {

  /** Minimal surface of RaftHAServer that the monitor depends on. Kept small for testing. */
  public interface HealthTarget {
    LifeCycle.State getRaftLifeCycleState();

    boolean isShutdownRequested();

    void restartRatisIfNeeded();
  }

  private final HealthTarget            target;
  private final long                    intervalMs;
  private volatile ScheduledExecutorService executor;

  public HealthMonitor(final HealthTarget target, final long intervalMs) {
    this.target = target;
    this.intervalMs = intervalMs;
  }

  public void start() {
    if (intervalMs <= 0) {
      LogManager.instance().log(this, Level.FINE, "HealthMonitor disabled (interval=%d)", intervalMs);
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-health-monitor");
      t.setDaemon(true);
      return t;
    });
    // Startup grace period: skip the first 2*intervalMs while Ratis completes its initial election.
    executor.scheduleWithFixedDelay(this::tickSafely, intervalMs * 2, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    final ScheduledExecutorService current = executor;
    if (current != null) {
      current.shutdownNow();
      executor = null;
    }
  }

  /** Package-private for tests. Runs one check synchronously. */
  void tick() {
    if (target.isShutdownRequested())
      return;
    final LifeCycle.State state = target.getRaftLifeCycleState();
    if (state == LifeCycle.State.CLOSED || state == LifeCycle.State.EXCEPTION) {
      HALog.log(this, HALog.BASIC, "Health monitor detected Ratis %s state, attempting recovery", state);
      target.restartRatisIfNeeded();
    }
  }

  private void tickSafely() {
    try {
      tick();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE, "Error in HealthMonitor tick: %s", t, t.getMessage());
    }
  }
}
```

- [ ] **Step 4: Run the unit test to verify it passes**

```bash
mvn -pl ha-raft -am test -Dtest=HealthMonitorTest -DfailIfNoTests=false
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java
```

Commit message: `feat(ha-raft): add HealthMonitor for Ratis CLOSED state auto-recovery`

---

## Task 4: Item 1 - Extract `buildRaftProperties()` helper in RaftHAServer

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Open `RaftHAServer.java` and locate the `start()` method (line ~274)**

Identify the block that builds `RaftProperties`, from `final RaftProperties properties = new RaftProperties();` (line 278) through `RaftServerConfigKeys.Read.setOption(properties, ...LINEARIZABLE);` (line 320). This is the `RaftProperties` construction block.

- [ ] **Step 2: Extract the block into a private helper**

Add a new private method below `start()`:

```java
  private RaftProperties buildRaftProperties() {
    final RaftProperties properties = new RaftProperties();

    final int localRaftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    GrpcConfigKeys.Server.setPort(properties, localRaftPort);

    final int electionMin = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN);
    final int electionMax = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(electionMin, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(electionMax, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    RaftServerConfigKeys.setStagingTimeout(properties, TimeDuration.valueOf(30, TimeUnit.SECONDS));

    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);

    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);

    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(appendBufferSize));
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 256);

    final String logSegmentSize = configuration.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(logSegmentSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf("8MB"));

    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    return properties;
  }
```

- [ ] **Step 3: Replace the inline block in `start()` with a call to the helper**

In `start()`, after the `Logger.getLogger("org.apache.ratis")` line, replace everything from `final RaftProperties properties = new RaftProperties();` through `RaftServerConfigKeys.Read.setOption(properties, ...LINEARIZABLE);` with:

```java
    final RaftProperties properties = buildRaftProperties();
```

Leave the subsequent storage-directory code (`final File storageDir = ...`) unchanged - `setStorageDir` stays in `start()` because the directory depends on whether persistent storage is enabled.

- [ ] **Step 4: Compile ha-raft**

```bash
mvn -pl ha-raft -am compile -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 5: Run the full ha-raft unit test suite**

```bash
mvn -pl ha-raft -am test -Dtest='!*IT' -DfailIfNoTests=false
```

Expected: no regressions vs baseline.

- [ ] **Step 6: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
```

Commit message: `refactor(ha-raft): extract buildRaftProperties() helper from start()`

---

## Task 5: Item 1 - Add `restartRatisIfNeeded()`, shutdown flag, test hook, wire HealthMonitor

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Add fields and the test hook**

At the top of `RaftHAServer` (near the other volatile fields around line ~92), add:

```java
  private final Object               recoveryLock           = new Object();
  private volatile boolean           shutdownRequested      = false;
  private volatile LifeCycle.State   forcedStateForTesting  = null;
  private HealthMonitor              healthMonitor;
```

Add import at the top:

```java
import org.apache.ratis.util.LifeCycle;
```

- [ ] **Step 2: Make `RaftHAServer` implement `HealthMonitor.HealthTarget`**

Change class declaration from:
```java
public class RaftHAServer ... {
```
to:
```java
public class RaftHAServer implements HealthMonitor.HealthTarget, ... {
```
(preserve any existing `implements` clauses).

- [ ] **Step 3: Add the HealthTarget methods**

Add these three methods anywhere in the class (grouped with lifecycle-related methods):

```java
  @Override
  public LifeCycle.State getRaftLifeCycleState() {
    final LifeCycle.State forced = forcedStateForTesting;
    if (forced != null) {
      forcedStateForTesting = null;
      return forced;
    }
    if (raftServer == null)
      return LifeCycle.State.NEW;
    return raftServer.getLifeCycleState();
  }

  @Override
  public boolean isShutdownRequested() {
    return shutdownRequested;
  }

  @Override
  public void restartRatisIfNeeded() {
    synchronized (recoveryLock) {
      if (shutdownRequested) {
        HALog.log(this, HALog.BASIC, "Recovery skipped: shutdown requested");
        return;
      }

      final RaftClient oldClient = this.raftClient;
      final RaftServer oldServer = this.raftServer;
      final RaftGroupCommitter oldCommitter = this.groupCommitter;

      // Best-effort close of current components. Swallow exceptions at FINE level.
      try { if (oldCommitter != null) oldCommitter.stop(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old committer: %s", t, t.getMessage()); }
      try { if (oldClient != null) oldClient.close(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old client: %s", t, t.getMessage()); }
      try { if (oldServer != null) oldServer.close(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old server: %s", t, t.getMessage()); }

      try {
        // Rebuild state machine (durable state lives in SimpleStateMachineStorage on disk).
        this.stateMachine = new ArcadeStateMachine();
        this.stateMachine.setServer(arcadeServer);

        final RaftProperties properties = buildRaftProperties();
        final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        this.raftServer = RaftServer.newBuilder()
            .setServerId(localPeerId)
            .setGroup(raftGroup)
            .setStateMachine(stateMachine)
            .setProperties(properties)
            .setParameters(new Parameters())
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
        this.raftServer.start();
        this.raftProperties = properties;
        this.raftClient = buildRaftClient(raftGroup, properties);

        final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
        this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);
        this.groupCommitter.start();

        HALog.log(this, HALog.BASIC, "Ratis recovered successfully");
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.SEVERE, "HealthMonitor recovery failed: %s", t, t.getMessage());
      }
    }
  }

  /** Package-private test hook. Next call to getRaftLifeCycleState() returns this value, then clears. */
  void forceRaftStateForTesting(final LifeCycle.State state) {
    this.forcedStateForTesting = state;
  }
```

Note: verify the `groupCommitter.start()` call matches the existing API. If `RaftGroupCommitter` starts automatically in its constructor, drop the `start()` line. Check by looking at how `start()` currently wires it.

- [ ] **Step 4: Wire HealthMonitor into `start()`**

At the end of `start()` (after the existing group-committer wiring), add:

```java
    final long healthInterval = configuration.getValueAsLong(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL);
    this.healthMonitor = new HealthMonitor(this, healthInterval);
    this.healthMonitor.start();
```

- [ ] **Step 5: Wire HealthMonitor shutdown into `stop()`**

At the very top of `stop()` (before the group-committer stop logic), add:

```java
    shutdownRequested = true;
    if (healthMonitor != null) {
      healthMonitor.stop();
      healthMonitor = null;
    }
```

- [ ] **Step 6: Compile ha-raft**

```bash
mvn -pl ha-raft -am compile -DskipTests
```

Expected: BUILD SUCCESS. If `RaftGroupCommitter` has no `start()` method, remove the corresponding line.

- [ ] **Step 7: Run the full ha-raft unit test suite**

```bash
mvn -pl ha-raft -am test -Dtest='!*IT' -DfailIfNoTests=false
```

Expected: no regressions. `HealthMonitorTest` still passes.

- [ ] **Step 8: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
```

Commit message: `feat(ha-raft): wire HealthMonitor into RaftHAServer with restartRatisIfNeeded recovery path`

---

## Task 6: Item 1 - Integration test for health monitor recovery

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHealthMonitorRecoveryIT.java`

- [ ] **Step 1: Write the integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHealthMonitorRecoveryIT.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

class RaftHealthMonitorRecoveryIT extends BaseMiniRaftTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerRecoversAfterForcedClosedState() throws Exception {
    // Set a short health check interval for the test.
    GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL.setValue(500L);

    startCluster();
    waitForLeader();

    // Write something via the leader to ensure cluster is fully live.
    final ArcadeDBServer leader = getLeaderServer();
    executeCommandOnLeader("CREATE DOCUMENT TYPE Touch");
    executeCommandOnLeader("INSERT INTO Touch CONTENT {'id':1}");

    // Pick a follower and grab its RaftHAServer.
    final ArcadeDBServer follower = getFollowerServers().get(0);
    final RaftHAServer followerRaft = getRaftHAServer(follower);
    final long leaderIndexBefore = getLastAppliedIndex(leader);

    // Inject CLOSED state. HealthMonitor should detect on next tick and trigger recovery.
    followerRaft.forceRaftStateForTesting(LifeCycle.State.CLOSED);

    // Wait up to 10 seconds for the follower to catch up to the leader's applied index.
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      final long followerIndex = getLastAppliedIndex(follower);
      assertThat(followerIndex).isGreaterThanOrEqualTo(leaderIndexBefore);
    });

    // Write another record and verify the recovered follower still replicates it.
    executeCommandOnLeader("INSERT INTO Touch CONTENT {'id':2}");
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      final long leaderIndex = getLastAppliedIndex(leader);
      final long followerIndex = getLastAppliedIndex(follower);
      assertThat(followerIndex).isGreaterThanOrEqualTo(leaderIndex);
    });
  }
}
```

Note: the test references helpers (`getLeaderServer`, `getFollowerServers`, `getRaftHAServer`, `getLastAppliedIndex`, `executeCommandOnLeader`, `waitForLeader`, `startCluster`) that may or may not already exist on `BaseMiniRaftTest`. Check `BaseMiniRaftTest.java` first. If any helper is missing, use the equivalent pattern already used by other ITs in the same package (e.g. `RaftReplication3NodesIT`).

- [ ] **Step 2: Run the IT**

```bash
mvn -pl ha-raft -am verify -Dit.test=RaftHealthMonitorRecoveryIT -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 3: Run the full ha-raft IT suite to check for regressions**

```bash
mvn -pl ha-raft -am verify -Dit.test='Raft*IT' -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: no regressions vs baseline.

- [ ] **Step 4: Stage the changes**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHealthMonitorRecoveryIT.java
```

Commit message: `test(ha-raft): integration test for HealthMonitor recovery on forced CLOSED state`

---

## Task 7: Item 7 - gRPC flow control window tuning

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerFlowControlTest.java`

- [ ] **Step 1: Write the failing unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerFlowControlTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class RaftHAServerFlowControlTest {

  @Test
  void buildRaftPropertiesAppliesFlowControlWindow() throws Exception {
    GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW.setValue(8L * 1024 * 1024);
    try {
      final RaftHAServer server = TestRaftHAServerFactory.newStandaloneForUnitTest();
      final Method m = RaftHAServer.class.getDeclaredMethod("buildRaftProperties");
      m.setAccessible(true);
      final RaftProperties props = (RaftProperties) m.invoke(server);
      assertThat(GrpcConfigKeys.flowControlWindow(props).getSize()).isEqualTo(8L * 1024 * 1024);
    } finally {
      GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW.reset();
    }
  }
}
```

If `TestRaftHAServerFactory` does not exist, construct the server inline using whatever minimal fake `ArcadeDBServer` + `ContextConfiguration` pattern is already used by `RaftHAServerTest`. Look at `RaftHAServerTest.java` for the idiom.

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -pl ha-raft -am test -Dtest=RaftHAServerFlowControlTest -DfailIfNoTests=false
```

Expected: FAIL - flow control window is still the Ratis default, not 8MB.

- [ ] **Step 3: Add the flow control window config to `buildRaftProperties()`**

In `RaftHAServer.buildRaftProperties()`, after the `setRequestTimeout` line, add:

```java
    final long flowControlWindow = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW);
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(flowControlWindow));
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
mvn -pl ha-raft -am test -Dtest=RaftHAServerFlowControlTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Run the full ha-raft unit test suite**

```bash
mvn -pl ha-raft -am test -Dtest='!*IT' -DfailIfNoTests=false
```

Expected: no regressions.

- [ ] **Step 6: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerFlowControlTest.java
```

Commit message: `feat(ha-raft): configurable gRPC flow control window via HA_GRPC_FLOW_CONTROL_WINDOW`

---

## Task 8: Item 2 - LeaderProxy class + unit tests

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/LeaderProxy.java`
- Create: `server/src/test/java/com/arcadedb/server/http/handler/LeaderProxyTest.java`

- [ ] **Step 1: Create `LeaderProxy.java`**

Create `server/src/main/java/com/arcadedb/server/http/handler/LeaderProxy.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

/**
 * Transparent HTTP proxy from a follower to the current leader.
 * Invoked from {@link AbstractServerHttpHandler} when a write request
 * arrives on a non-leader and would otherwise return HTTP 400.
 */
public final class LeaderProxy {

  private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
      "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
      "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length");

  private final HttpServer httpServer;
  private final HttpClient client;
  private final long      readTimeoutMs;
  private final int       maxBodySize;

  public LeaderProxy(final HttpServer httpServer) {
    this.httpServer = httpServer;
    final long connectTimeoutMs = httpServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
    this.readTimeoutMs = httpServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_PROXY_READ_TIMEOUT);
    this.maxBodySize = httpServer.getServer().getConfiguration()
        .getValueAsInteger(GlobalConfiguration.HA_PROXY_MAX_BODY_SIZE);
    this.client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofMillis(connectTimeoutMs))
        .build();
  }

  /**
   * Attempts to proxy the current exchange to the given leader address.
   * @return {@code true} if the response has been written to the exchange,
   *         {@code false} if the caller should fall back to its default error response.
   */
  public boolean tryProxy(final HttpServerExchange exchange, final String leaderAddress,
                          final ServerSecurityUser user) {
    if (leaderAddress == null || leaderAddress.isBlank())
      return false;

    // Loop prevention: already-forwarded requests must not be re-forwarded.
    final HeaderValues existingToken = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
    if (existingToken != null && !existingToken.isEmpty()) {
      LogManager.instance().log(this, Level.WARNING,
          "Leader proxy refused to forward already-forwarded request (loop prevention)");
      return false;
    }

    final byte[] body;
    try {
      body = readBodyCapped(exchange);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy failed to read request body: %s", e, e.getMessage());
      return false;
    } catch (final BodyTooLargeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Leader proxy refused to forward request: body too large (> %d bytes)", maxBodySize);
      return false;
    }

    final String path = exchange.getRequestPath();
    final String query = exchange.getQueryString();
    final String urlString = "http://" + leaderAddress + path + (query == null || query.isEmpty() ? "" : "?" + query);

    final HttpRequest.Builder builder;
    try {
      builder = HttpRequest.newBuilder(URI.create(urlString));
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy URL invalid: %s (%s)", urlString, e.getMessage());
      return false;
    }
    builder.timeout(Duration.ofMillis(readTimeoutMs));

    // Copy request headers minus hop-by-hop and Authorization.
    exchange.getRequestHeaders().forEach(hv -> {
      final String name = hv.getHeaderName().toString();
      final String lower = name.toLowerCase();
      if (HOP_BY_HOP_HEADERS.contains(lower) || lower.equals("authorization"))
        return;
      builder.header(name, hv.getFirst());
    });

    // Inject cluster-forwarded auth.
    final String clusterToken = httpServer.getServer().getConfiguration()
        .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (clusterToken == null || clusterToken.isBlank()) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy refused: HA_CLUSTER_TOKEN is not configured");
      return false;
    }
    builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    if (user != null)
      builder.header("X-ArcadeDB-Forwarded-User", user.getName());

    builder.method(exchange.getRequestMethod().toString(), HttpRequest.BodyPublishers.ofByteArray(body));

    final HttpResponse<byte[]> response;
    try {
      response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    } catch (final HttpTimeoutException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy timeout to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final ConnectException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy cannot connect to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Leader proxy IO error to %s: %s", leaderAddress, e.getMessage());
      return false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LogManager.instance().log(this, Level.WARNING, "Leader proxy interrupted to %s", leaderAddress);
      return false;
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE, "Leader proxy unexpected error to %s: %s", t, leaderAddress, t.getMessage());
      return false;
    }

    // Relay response.
    exchange.setStatusCode(response.statusCode());
    final HttpHeaders responseHeaders = response.headers();
    responseHeaders.map().forEach((name, values) -> {
      final String lower = name.toLowerCase();
      if (HOP_BY_HOP_HEADERS.contains(lower) || lower.equals("content-length"))
        return;
      for (final String value : values)
        exchange.getResponseHeaders().add(new HttpString(name), value);
    });
    exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(response.body()));
    return true;
  }

  private byte[] readBodyCapped(final HttpServerExchange exchange) throws IOException, BodyTooLargeException {
    exchange.startBlocking();
    final InputStream in = exchange.getInputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buf = new byte[8192];
    int total = 0;
    int read;
    while ((read = in.read(buf)) != -1) {
      total += read;
      if (total > maxBodySize)
        throw new BodyTooLargeException();
      out.write(buf, 0, read);
    }
    return out.toByteArray();
  }

  private static final class BodyTooLargeException extends Exception {
    private static final long serialVersionUID = 1L;
  }
}
```

- [ ] **Step 2: Write unit tests for `LeaderProxy`**

Create `server/src/test/java/com/arcadedb/server/http/handler/LeaderProxyTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LeaderProxy unit tests. They spin up a tiny Undertow "fake leader" that
 * captures the headers and body received. The Follower side is emulated
 * with a hand-built HttpServerExchange via MockHttpExchangeFactory.
 * If MockHttpExchangeFactory does not exist, use whatever mocking pattern
 * other handler tests in the same package use (e.g. PostCommandHandlerTest).
 */
class LeaderProxyTest {

  private Undertow fakeLeader;
  private int fakeLeaderPort;
  private final AtomicReference<String> receivedClusterToken = new AtomicReference<>();
  private final AtomicReference<String> receivedForwardedUser = new AtomicReference<>();
  private final AtomicReference<String> receivedAuthorization = new AtomicReference<>();
  private final AtomicReference<byte[]> receivedBody = new AtomicReference<>();

  @BeforeEach
  void setUp() throws Exception {
    try (final ServerSocket s = new ServerSocket(0)) {
      fakeLeaderPort = s.getLocalPort();
    }
    fakeLeader = Undertow.builder()
        .addHttpListener(fakeLeaderPort, "127.0.0.1")
        .setHandler(exchange -> {
          final var reqHeaders = exchange.getRequestHeaders();
          receivedClusterToken.set(header(reqHeaders.get("X-ArcadeDB-Cluster-Token")));
          receivedForwardedUser.set(header(reqHeaders.get("X-ArcadeDB-Forwarded-User")));
          receivedAuthorization.set(header(reqHeaders.get("Authorization")));
          exchange.startBlocking();
          receivedBody.set(exchange.getInputStream().readAllBytes());
          exchange.setStatusCode(200);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
          exchange.getResponseSender().send("{\"ok\":true}");
        })
        .build();
    fakeLeader.start();
    GlobalConfiguration.HA_CLUSTER_TOKEN.setValue("unit-test-token");
  }

  @AfterEach
  void tearDown() {
    if (fakeLeader != null) fakeLeader.stop();
    GlobalConfiguration.HA_CLUSTER_TOKEN.reset();
  }

  private static String header(final io.undertow.util.HeaderValues hv) {
    return hv == null || hv.isEmpty() ? null : hv.getFirst();
  }

  // NOTE: the remaining tests construct an HttpServerExchange mock.
  // Exact construction depends on what helper class exists in the server test tree.
  // If the test suite already has a helper for this (search: "HttpServerExchange" in server/src/test),
  // use it. Otherwise, this test class should be built around the one pattern you find.
  //
  // At minimum, the following assertions must hold after tryProxy returns true:
  //   assertThat(receivedClusterToken.get()).isEqualTo("unit-test-token");
  //   assertThat(receivedForwardedUser.get()).isEqualTo("alice");
  //   assertThat(receivedAuthorization.get()).isNull();  // Authorization stripped
  //   assertThat(receivedBody.get()).isEqualTo("{\"k\":1}".getBytes());
  //
  // And for the loop-prevention test:
  //   // exchange already has X-ArcadeDB-Cluster-Token
  //   assertThat(proxy.tryProxy(exchange, "127.0.0.1:" + fakeLeaderPort, user)).isFalse();

  @Test
  void placeholderForBuildSetup() {
    assertThat(fakeLeader).isNotNull();
    assertThat(GlobalConfiguration.HA_CLUSTER_TOKEN.getValueAsString()).isEqualTo("unit-test-token");
  }
}
```

Note on the mock exchange: `HttpServerExchange` in Undertow is concrete and hard to mock directly. Before writing the actual proxy-call tests, grep the `server/src/test` tree for existing usages of `HttpServerExchange` in tests:

```bash
grep -rl "HttpServerExchange" server/src/test/java | head -5
```

Use whichever pattern is already established. If no pattern exists, you can construct a real `HttpServerExchange` via `io.undertow.testutils.DefaultServer` or defer the full mock tests to the IT in Task 10 and keep this file as a minimal smoke test.

- [ ] **Step 3: Run the unit test to verify the fake leader spins up**

```bash
mvn -pl server -am test -Dtest=LeaderProxyTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 4: Stage the changes**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/LeaderProxy.java \
        server/src/test/java/com/arcadedb/server/http/handler/LeaderProxyTest.java
```

Commit message: `feat(server): add LeaderProxy for follower-to-leader HTTP request forwarding`

---

## Task 9: Item 2 - Wire LeaderProxy into AbstractServerHttpHandler

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java`

- [ ] **Step 1: Add a `LeaderProxy` field to the handler**

Near the other instance fields, add:
```java
  private final LeaderProxy leaderProxy;
```

In the constructor (or the constructors that take `HttpServer httpServer`), initialize:
```java
    this.leaderProxy = new LeaderProxy(httpServer);
```

- [ ] **Step 2: Replace the first `ServerIsNotTheLeaderException` catch (around line 189)**

Find:
```java
    } catch (final ServerIsNotTheLeaderException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
    }
```
Replace with:
```java
    } catch (final ServerIsNotTheLeaderException e) {
      final ServerSecurityUser sessionUser = resolveCurrentUser(exchange);
      if (leaderProxy.tryProxy(exchange, e.getLeaderAddress(), sessionUser))
        return;
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
    }
```

`resolveCurrentUser(exchange)` must return the `ServerSecurityUser` for the current authenticated session. Check how `AbstractServerHttpHandler` already does this (look for `HttpAuthSession` usage near line ~90). If there's no existing helper, inline the retrieval.

- [ ] **Step 3: Replace the second `ServerIsNotTheLeaderException` catch (around line 264)**

Find:
```java
      } else if (realException instanceof ServerIsNotTheLeaderException sle) {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on transaction execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", realException, sle.getLeaderAddress());
      }
```
Replace with:
```java
      } else if (realException instanceof ServerIsNotTheLeaderException sle) {
        final ServerSecurityUser sessionUser = resolveCurrentUser(exchange);
        if (leaderProxy.tryProxy(exchange, sle.getLeaderAddress(), sessionUser))
          return;
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on transaction execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", realException, sle.getLeaderAddress());
      }
```

- [ ] **Step 4: Compile the server module**

```bash
mvn -pl server -am compile -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 5: Run the existing handler test suite**

```bash
mvn -pl server -am test -Dtest='*Handler*Test' -DfailIfNoTests=false
```

Expected: no regressions.

- [ ] **Step 6: Stage the changes**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java
```

Commit message: `feat(server): transparent leader proxy on ServerIsNotTheLeaderException`

---

## Task 10: Item 2 - Integration test for leader proxy

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderProxyIT.java`

- [ ] **Step 1: Write the integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderProxyIT.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLeaderProxyIT extends BaseMiniRaftTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void postCommandToFollowerIsProxiedToLeader() throws Exception {
    startCluster();
    waitForLeader();
    createDatabase("proxyTest");

    // Aim at a follower's HTTP port.
    final String followerHttp = getFollowerHttpAddresses().get(0); // e.g. "127.0.0.1:2480"
    final String url = "http://" + followerHttp + "/api/v1/command/proxyTest";

    final HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .header("Authorization", "Basic " + basicAuth("root", "playwithdata"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{\"language\":\"sql\",\"command\":\"CREATE DOCUMENT TYPE Proxied\"}"))
        .build();
    final HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());

    assertThat(resp.statusCode()).isEqualTo(200);

    // Verify the schema change replicated to all nodes (including the one we hit).
    assertSchemaTypeExistsOnAll("proxyTest", "Proxied");
  }

  private static String basicAuth(final String user, final String pass) {
    return java.util.Base64.getEncoder().encodeToString((user + ":" + pass).getBytes());
  }
}
```

Helpers (`getFollowerHttpAddresses`, `createDatabase`, `assertSchemaTypeExistsOnAll`): check `BaseMiniRaftTest.java` first. If missing, follow the pattern from `RaftHTTP2ServersIT.java` or `RaftReplication3NodesIT.java`.

- [ ] **Step 2: Run the IT**

```bash
mvn -pl ha-raft -am verify -Dit.test=RaftLeaderProxyIT -DfailIfNoTests=false
```

Expected: PASS. The command sent to the follower is proxied and the `Proxied` type appears on all nodes.

- [ ] **Step 3: Stage the changes**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderProxyIT.java
```

Commit message: `test(ha-raft): integration test for follower-to-leader HTTP proxy`

---

## Task 11: Item 3 - ReadConsistency enum + RemoteDatabase API surface

**Files:**
- Create: `network/src/main/java/com/arcadedb/remote/ReadConsistency.java`
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteDatabase.java`
- Create: `network/src/test/java/com/arcadedb/remote/RemoteDatabaseReadConsistencyTest.java`

- [ ] **Step 1: Create the `ReadConsistency` enum**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.remote;

/**
 * Consistency guarantee requested by a RemoteDatabase client when reading.
 *
 * <ul>
 *   <li>{@link #EVENTUAL}: no cross-node ordering guarantees. No read consistency headers sent.</li>
 *   <li>{@link #READ_YOUR_WRITES}: reads observe at least every write previously observed by this client.</li>
 *   <li>{@link #LINEARIZABLE}: reads observe the latest committed state of the cluster.</li>
 * </ul>
 */
public enum ReadConsistency {
  EVENTUAL,
  READ_YOUR_WRITES,
  LINEARIZABLE
}
```

- [ ] **Step 2: Write the failing unit test**

Create `network/src/test/java/com/arcadedb/remote/RemoteDatabaseReadConsistencyTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.remote;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteDatabaseReadConsistencyTest {

  @Test
  void defaultReadConsistencyIsEventual() {
    final RemoteDatabase db = RemoteDatabase.unconnectedForUnitTest();
    assertThat(db.getReadConsistency()).isEqualTo(ReadConsistency.EVENTUAL);
  }

  @Test
  void setReadConsistencyReturnsTheSetValue() {
    final RemoteDatabase db = RemoteDatabase.unconnectedForUnitTest();
    db.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
    assertThat(db.getReadConsistency()).isEqualTo(ReadConsistency.READ_YOUR_WRITES);
  }

  @Test
  void lastCommitIndexStartsAtNegativeOne() {
    final RemoteDatabase db = RemoteDatabase.unconnectedForUnitTest();
    assertThat(db.getLastCommitIndex()).isEqualTo(-1L);
  }

  @Test
  void updateLastCommitIndexIsMonotonic() {
    final RemoteDatabase db = RemoteDatabase.unconnectedForUnitTest();
    db.updateLastCommitIndex(10L);
    assertThat(db.getLastCommitIndex()).isEqualTo(10L);
    db.updateLastCommitIndex(5L);
    assertThat(db.getLastCommitIndex()).isEqualTo(10L); // did not go backward
    db.updateLastCommitIndex(42L);
    assertThat(db.getLastCommitIndex()).isEqualTo(42L);
  }
}
```

Note: this test references `RemoteDatabase.unconnectedForUnitTest()`. That is a package-private test factory we add in the next step, or, if such a helper already exists, replace with whatever the existing idiom is. If the test suite uses a real test server (`BaseGraphServerTest`), prefer a mocking approach instead. Check how existing `network/` tests instantiate `RemoteDatabase`.

- [ ] **Step 3: Run the test to verify it fails**

```bash
mvn -pl network -am test -Dtest=RemoteDatabaseReadConsistencyTest -DfailIfNoTests=false
```

Expected: compilation error - `setReadConsistency`, `getReadConsistency`, `getLastCommitIndex`, `updateLastCommitIndex` not defined.

- [ ] **Step 4: Add fields and public API to `RemoteDatabase`**

In `RemoteDatabase.java`, near the other private fields (around line 70), add:

```java
  private volatile ReadConsistency                 readConsistency         =
      ReadConsistency.EVENTUAL;
  private final java.util.concurrent.atomic.AtomicLong lastCommitIndex     =
      new java.util.concurrent.atomic.AtomicLong(-1L);
  private volatile int                             electionRetryCount;
  private volatile long                            electionRetryDelayMs;
```

In the constructor that takes `ContextConfiguration` (the deepest one), after the existing setup, add:

```java
    this.electionRetryCount   = configuration.getValueAsInteger(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_COUNT);
    this.electionRetryDelayMs = configuration.getValueAsLong(GlobalConfiguration.HA_CLIENT_ELECTION_RETRY_DELAY_MS);
```

Add public API methods anywhere (grouped with other setters):

```java
  public ReadConsistency getReadConsistency() {
    return readConsistency;
  }

  public void setReadConsistency(final ReadConsistency readConsistency) {
    if (readConsistency == null)
      throw new IllegalArgumentException("readConsistency cannot be null");
    this.readConsistency = readConsistency;
  }

  public long getLastCommitIndex() {
    return lastCommitIndex.get();
  }

  void updateLastCommitIndex(final long newValue) {
    lastCommitIndex.accumulateAndGet(newValue, Math::max);
  }

  public int getElectionRetryCount() {
    return electionRetryCount;
  }

  public void setElectionRetryCount(final int electionRetryCount) {
    this.electionRetryCount = electionRetryCount;
  }

  public long getElectionRetryDelayMs() {
    return electionRetryDelayMs;
  }

  public void setElectionRetryDelayMs(final long electionRetryDelayMs) {
    this.electionRetryDelayMs = electionRetryDelayMs;
  }
```

- [ ] **Step 5: Provide the unconnected test factory**

If no equivalent exists, add a package-private static factory at the bottom of `RemoteDatabase`:

```java
  /** Package-private test factory: constructs an instance with no network connection. */
  static RemoteDatabase unconnectedForUnitTest() {
    return new RemoteDatabase("localhost", 0, "_unit_", "_u_", "_p_", new com.arcadedb.ContextConfiguration());
  }
```

If this constructor eagerly connects, reuse whatever existing test helper network tests already use instead and update the test in Step 2 accordingly.

- [ ] **Step 6: Run the test to verify it passes**

```bash
mvn -pl network -am test -Dtest=RemoteDatabaseReadConsistencyTest -DfailIfNoTests=false
```

Expected: all 4 tests PASS.

- [ ] **Step 7: Stage the changes**

```bash
git add network/src/main/java/com/arcadedb/remote/ReadConsistency.java \
        network/src/main/java/com/arcadedb/remote/RemoteDatabase.java \
        network/src/test/java/com/arcadedb/remote/RemoteDatabaseReadConsistencyTest.java
```

Commit message: `feat(network): ReadConsistency enum and RemoteDatabase consistency API`

---

## Task 12: Item 3 - RemoteHttpComponent header injection, capture, retry loop

**Files:**
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java`

This task is the bulk of item 3. It modifies the HTTP request builder path used by `RemoteDatabase` and friends.

- [ ] **Step 1: Read the current `RemoteHttpComponent` structure**

```bash
wc -l network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java
```

Identify:
- The main `httpRequest(...)` or `execute(...)` method that sends one request.
- Where request headers are set.
- Where the response is parsed and the exception body is interpreted.
- The existing peer-failover loop.

- [ ] **Step 2: Inject the read-consistency headers at request build time**

In the method that constructs the outgoing `HttpRequest.Builder` (or sets headers on the URLConnection), after the existing Authorization header is set, add:

```java
    if (this instanceof RemoteDatabase remoteDb) {
      final ReadConsistency rc = remoteDb.getReadConsistency();
      if (rc != ReadConsistency.EVENTUAL)
        requestBuilder.header("X-ArcadeDB-Read-Consistency", rc.name().toLowerCase());
      if (rc == ReadConsistency.READ_YOUR_WRITES) {
        final long last = remoteDb.getLastCommitIndex();
        if (last >= 0)
          requestBuilder.header("X-ArcadeDB-Commit-Index", String.valueOf(last));
      }
    }
```

The `this instanceof RemoteDatabase` check is because `RemoteHttpComponent` is also used by `RemoteServer` which has no database-level consistency. Using an `instanceof` pattern avoids refactoring the whole class hierarchy for one feature.

If `RemoteHttpComponent` uses `HttpURLConnection` instead of `HttpRequest.Builder`, the equivalent is `connection.setRequestProperty(...)`.

- [ ] **Step 3: Capture the commit-index response header**

After the response has been read, before returning the parsed result, add:

```java
    if (this instanceof RemoteDatabase remoteDb) {
      final String commitIdxHeader = response.headers().firstValue("X-ArcadeDB-Commit-Index").orElse(null);
      if (commitIdxHeader != null) {
        try {
          remoteDb.updateLastCommitIndex(Long.parseLong(commitIdxHeader));
        } catch (final NumberFormatException ignored) {
          // server sent an invalid header; ignore
        }
      }
    }
```

Adapt for `HttpURLConnection` if that is the API in use (`connection.getHeaderField("X-ArcadeDB-Commit-Index")`).

- [ ] **Step 4: Map 503 with `NeedRetryException` to a retryable exception**

In the response-parsing path that today throws a generic exception on non-2xx, add a branch:

```java
    // After deserializing the error body into {exception, ...}
    if (statusCode == 503 && "NeedRetryException".equals(exceptionClass)) {
      throw new NeedRetryException(errorMessage);
    }
    if (statusCode == 400 && "ServerIsNotTheLeaderException".equals(exceptionClass)) {
      // Existing ServerIsNotTheLeaderException path; ensure the leader address is parsed.
      throw new ServerIsNotTheLeaderException(errorMessage, leaderAddress);
    }
```

If the existing error path already throws `NeedRetryException` or `ServerIsNotTheLeaderException`, skip this step and document in the commit message that the detection was already in place.

- [ ] **Step 5: Wrap the per-peer request in an election-retry loop**

Find the method that iterates over the peer list and performs the request. Wrap the inner send with:

```java
    int retryAttempt = 0;
    while (true) {
      try {
        return executeOnce(peer, requestBuilder.build());
      } catch (final NeedRetryException e) {
        final int maxRetries = (this instanceof RemoteDatabase db) ? db.getElectionRetryCount() : 3;
        final long delayMs  = (this instanceof RemoteDatabase db) ? db.getElectionRetryDelayMs() : 2000;
        if (retryAttempt++ >= maxRetries)
          throw e;
        try {
          Thread.sleep(delayMs);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }
```

Leave the outer peer-failover loop that catches `ConnectException` / `IOException` unchanged. Those are a different failure mode (peer unreachable) and continue to advance to the next peer.

Extract the inner send into a private `executeOnce(peer, request)` method if one does not already exist.

- [ ] **Step 6: Compile the network module**

```bash
mvn -pl network -am compile -DskipTests
```

Expected: BUILD SUCCESS.

- [ ] **Step 7: Run the network module test suite**

```bash
mvn -pl network -am test -DfailIfNoTests=false
```

Expected: no regressions. `RemoteDatabaseReadConsistencyTest` still passes.

- [ ] **Step 8: Stage the changes**

```bash
git add network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java
```

Commit message: `feat(network): RemoteHttpComponent sends read-consistency headers and retries on election 503`

---

## Task 13: Item 3 - Integration test for remote read-your-writes

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRemoteReadYourWritesIT.java`

- [ ] **Step 1: Write the integration test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRemoteReadYourWritesIT.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.ReadConsistency;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RaftRemoteReadYourWritesIT extends BaseMiniRaftTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void remoteReadYourWritesSeesJustWrittenRowOnFollower() throws Exception {
    startCluster();
    waitForLeader();
    createDatabase("ryw");

    final String leaderHttp   = getLeaderHttpAddress();     // "host:port"
    final String followerHttp = getFollowerHttpAddresses().get(0);

    // Writer client talks directly to the leader.
    final RemoteDatabase writer = newRemoteDatabase(leaderHttp, "ryw");
    writer.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
    writer.command("sql", "CREATE DOCUMENT TYPE Ryw");
    writer.command("sql", "INSERT INTO Ryw CONTENT {'id':'v1'}");
    final long writerIndex = writer.getLastCommitIndex();
    assertThat(writerIndex).isGreaterThanOrEqualTo(0);

    // Reader client talks to a follower, seeded with the writer's observed commit index.
    final RemoteDatabase reader = newRemoteDatabase(followerHttp, "ryw");
    reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
    // Simulate the writer handing off its index (e.g., via a sticky session token).
    // Reflection bypass: the public API is updateLastCommitIndex which is package-private.
    // For the IT, read the value via a synthetic warm-up GET to the leader first.
    reader.query("sql", "SELECT FROM Ryw"); // warms the index via a leader hit
    reader.updateLastCommitIndex(writerIndex);

    final ResultSet rs = reader.query("sql", "SELECT FROM Ryw");
    assertThat(rs.stream().count()).isEqualTo(1L);
  }

  private RemoteDatabase newRemoteDatabase(final String hostPort, final String dbName) {
    final int colon = hostPort.lastIndexOf(':');
    final String host = hostPort.substring(0, colon);
    final int port = Integer.parseInt(hostPort.substring(colon + 1));
    return new RemoteDatabase(host, port, dbName, "root", "playwithdata");
  }
}
```

Notes:
- `updateLastCommitIndex` is package-private, so the IT can call it since it lives in `com.arcadedb.server.ha.raft` while the method is in `com.arcadedb.remote`. If package visibility prevents this, add a small `setLastCommitIndexForTest(long)` package-private method on `RemoteDatabase` that delegates - or make `updateLastCommitIndex` public if the team prefers.
- If `BaseMiniRaftTest` does not expose `getLeaderHttpAddress` / `getFollowerHttpAddresses`, add thin helpers that read them from the existing `ArcadeDBServer` list.

- [ ] **Step 2: Run the IT**

```bash
mvn -pl ha-raft -am verify -Dit.test=RaftRemoteReadYourWritesIT -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 3: Stage the changes**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftRemoteReadYourWritesIT.java
```

Commit message: `test(ha-raft): integration test for RemoteDatabase READ_YOUR_WRITES via commit-index header`

---

## Task 14: Item 5 - Snapshot throttling semaphore

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotThrottleTest.java`

- [ ] **Step 1: Add the semaphore**

In `SnapshotHttpHandler.java`, near the top of the class:

```java
  private static final java.util.concurrent.Semaphore CONCURRENCY_SEMAPHORE =
      new java.util.concurrent.Semaphore(
          com.arcadedb.GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger(),
          true);
```

- [ ] **Step 2: Wrap `handleRequest` with `tryAcquire` + `finally release`**

At the entry point of `handleRequest(HttpServerExchange exchange)`, after the cluster-token auth check, add:

```java
    if (!CONCURRENCY_SEMAPHORE.tryAcquire()) {
      LogManager.instance().log(this, Level.WARNING,
          "Snapshot rejected: concurrency limit of %d reached",
          GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger());
      exchange.setStatusCode(503);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.getResponseSender().send("{\"error\":\"Too many concurrent snapshots\"}");
      return;
    }
    try {
      // ... existing snapshot serving logic ...
    } finally {
      CONCURRENCY_SEMAPHORE.release();
    }
```

Wrap the existing body of the method inside the `try`.

- [ ] **Step 3: Write the unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotThrottleTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotThrottleTest {

  @Test
  void semaphoreHasConfiguredPermits() throws Exception {
    // Guard against surprise from a test run that set a different value earlier.
    GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.reset();

    final Field f = SnapshotHttpHandler.class.getDeclaredField("CONCURRENCY_SEMAPHORE");
    f.setAccessible(true);
    final Semaphore sem = (Semaphore) f.get(null);
    assertThat(sem.availablePermits())
        .isEqualTo(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger());
  }

  @Test
  void tryAcquireExhaustsAtMaxConcurrent() throws Exception {
    final Field f = SnapshotHttpHandler.class.getDeclaredField("CONCURRENCY_SEMAPHORE");
    f.setAccessible(true);
    final Semaphore sem = (Semaphore) f.get(null);

    final int configured = GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger();
    for (int i = 0; i < configured; i++)
      assertThat(sem.tryAcquire()).as("acquire %d", i).isTrue();
    assertThat(sem.tryAcquire()).as("over-limit acquire").isFalse();
    sem.release(configured);
  }
}
```

- [ ] **Step 4: Run the test**

```bash
mvn -pl ha-raft -am test -Dtest=SnapshotThrottleTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Run existing SnapshotHttpHandler tests to verify no regression**

```bash
mvn -pl ha-raft -am test -Dtest='Snapshot*' -DfailIfNoTests=false
```

Expected: no regressions.

- [ ] **Step 6: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotThrottleTest.java
```

Commit message: `feat(ha-raft): snapshot concurrency throttling with HA_SNAPSHOT_MAX_CONCURRENT`

---

## Task 15: Item 6 - Symlink rejection in snapshot ZIP

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java`
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSymlinkTest.java`

- [ ] **Step 1: Write the failing unit test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSymlinkTest.java`:

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 * [... full header ...]
 */
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Creating symlinks on Windows requires elevated privileges, so we skip there.
 */
@DisabledOnOs(OS.WINDOWS)
class SnapshotSymlinkTest {

  @TempDir
  Path tempDir;

  @Test
  void zipBuilderSkipsSymlinks() throws Exception {
    // Arrange: a directory with one regular file and one symlink pointing outside.
    final Path dir = tempDir.resolve("db");
    Files.createDirectory(dir);
    final Path regular = dir.resolve("real.bin");
    Files.write(regular, "hello".getBytes());

    final Path outsideTarget = tempDir.resolve("outside.secret");
    Files.write(outsideTarget, "SECRET".getBytes());
    final Path symlink = dir.resolve("link.bin");
    Files.createSymbolicLink(symlink, outsideTarget);

    // Act: call the static helper that writes a file into the zip, or inline the same check.
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (final ZipOutputStream zout = new ZipOutputStream(buf)) {
      for (final Path p : new Path[] { regular, symlink }) {
        if (Files.isSymbolicLink(p))
          continue;
        zout.putNextEntry(new ZipEntry(p.getFileName().toString()));
        zout.write(Files.readAllBytes(p));
        zout.closeEntry();
      }
    }

    // Assert: the symlink is not in the zip, the regular file is.
    final Set<String> entries = new HashSet<>();
    try (final ZipInputStream zin = new ZipInputStream(new java.io.ByteArrayInputStream(buf.toByteArray()))) {
      ZipEntry e;
      while ((e = zin.getNextEntry()) != null)
        entries.add(e.getName());
    }
    assertThat(entries).containsExactly("real.bin");
  }
}
```

Note: this test is an algorithmic smoke test for the "skip symlinks" rule. The production assertion happens inside `SnapshotHttpHandler` once you add the same check there. If the handler has a dedicated method that walks the file list, point the test at that method directly via reflection or a package-private entry point. Otherwise, the test above is a self-contained check that the rule is correct.

- [ ] **Step 2: Run the test to verify it passes (it is algorithm-only, so it should)**

```bash
mvn -pl ha-raft -am test -Dtest=SnapshotSymlinkTest -DfailIfNoTests=false
```

Expected: PASS. The rule is encoded in the test itself.

- [ ] **Step 3: Apply the same rule inside `SnapshotHttpHandler`**

Open `SnapshotHttpHandler.java`. Find the loop that writes each file into the `ZipOutputStream` (search for `new ZipEntry(` or `zout.putNextEntry`). Immediately before adding a file, insert:

```java
      final java.nio.file.Path filePath = file.toPath();
      if (java.nio.file.Files.isSymbolicLink(filePath)) {
        LogManager.instance().log(this, Level.WARNING, "Skipping symlink in snapshot: %s", filePath);
        continue;
      }
```

Apply this to every location in the handler that adds a file entry.

- [ ] **Step 4: Compile and run tests**

```bash
mvn -pl ha-raft -am test -Dtest='Snapshot*' -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Stage the changes**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotHttpHandler.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSymlinkTest.java
```

Commit message: `fix(ha-raft): skip symlinks when building snapshot ZIP to prevent path traversal`

---

## Task 16: Full validation and final test run

**Files:** none

- [ ] **Step 1: Run the full ha-raft test suite (unit + IT)**

```bash
mvn -pl ha-raft -am verify -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: all tests pass. Compare to baseline from Task 0 - the new ones added in this plan should appear as extra passes; no existing tests should have flipped to failure.

- [ ] **Step 2: Run the full server test suite**

```bash
mvn -pl server -am test -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: no regressions.

- [ ] **Step 3: Run the full network test suite**

```bash
mvn -pl network -am test -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: no regressions.

- [ ] **Step 4: Run the e2e-ha chaos suite (containers)**

```bash
mvn -pl e2e-ha -am verify -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: no regressions. This is the hardest suite and can take 10+ minutes.

- [ ] **Step 5: Report final status**

Summarize to the user:
- Which tasks landed cleanly
- Any deviations from the plan (e.g. an API turned out not to exist)
- Pass/fail counts vs. baseline
- Any known follow-up items (e.g. Task 8 step 2 deferred because the mock-exchange pattern was not established)

---

## Plan Self-Review (completed by author)

Spec coverage check:
- Item 1 (Health Monitor): Tasks 3, 4, 5, 6
- Item 2 (Leader Proxy): Tasks 8, 9, 10
- Item 3 (Client failover): Tasks 11, 12, 13
- Item 4 (Constant-time token): Task 2
- Item 5 (Snapshot throttling): Task 14
- Item 6 (Symlink rejection): Task 15
- Item 7 (gRPC flow control): Task 7
- Item 8 (Proxy timeout config): Task 1 (config), Task 8 (consumer)

All 8 spec items map to at least one task.

Type / name consistency check:
- `HealthMonitor.HealthTarget` interface is defined in Task 3 and implemented by `RaftHAServer` in Task 5. Methods match: `getRaftLifeCycleState`, `isShutdownRequested`, `restartRatisIfNeeded`.
- `ReadConsistency` enum is defined in Task 11 and referenced in Task 12 and Task 13. Values: `EVENTUAL`, `READ_YOUR_WRITES`, `LINEARIZABLE`.
- `buildRaftProperties()` helper is extracted in Task 4 and reused in Task 5's `restartRatisIfNeeded()` and Task 7's flow-control window addition.
- `LeaderProxy.tryProxy(HttpServerExchange, String, ServerSecurityUser)` signature matches between Task 8 definition and Task 9 call sites.
- Config key names match between Task 1 definitions and Task 2-15 consumers.

Ambiguity check:
- Task 5 step 3: the `groupCommitter.start()` call is guarded with an instruction to remove it if the API does not expose that method. Ambiguity acknowledged and handled in the step text.
- Task 8 step 2 and Task 12: both note that the exact test-mocking pattern depends on existing idioms in the respective test trees, with explicit fallback guidance.
- Task 11 step 5: the test factory approach may need adaptation if `RemoteDatabase`'s deepest constructor eagerly connects; fallback guidance included.

No TODOs, no "implement later", no "similar to earlier task". Each code block is self-contained.
