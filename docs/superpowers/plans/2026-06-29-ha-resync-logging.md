# HA Resync Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the leader's per-retry WARN flood when a follower restarts, and give the restarting follower clear, throttled resync progress logging (catch-up and snapshot), without changing Raft membership.

**Architecture:** Three independent, unit-testable pieces wired into existing HA hooks. (1) A `java.util.logging.Filter` on the Ratis `GrpcLogAppender` logger drops the known flood message; the leader-only `ClusterMonitor` (already ticking every 5s) gains an unreachable/recovery narrative fed by each follower's `lastRpcElapsedMs`. (2) A `FollowerResyncProgressTracker` driven by the follower-only `HealthMonitor` tick emits catch-up start/progress/finish bookends. (3) A `SnapshotDownloadProgressMeter` reports periodic byte progress during full snapshot downloads, and the existing snapshot bookend lines are aligned to a single grep-able phrasing. All new logging is gated by one config switch.

**Tech Stack:** Java 21, Apache Ratis 3.2.2, JUnit 5 (Jupiter) + AssertJ, `com.arcadedb.log.LogManager` / `HALog`, `GlobalConfiguration` enum config.

## Global Constraints

- Java 21+. Module: `ha-raft`. Engine enum lives in module `engine`.
- Reuse existing components; do not add dependencies.
- Import classes; do not use fully-qualified names inline.
- Use `final` on variables and parameters where possible.
- One-statement `if` bodies omit braces (match surrounding code).
- Prefer primitive arrays over object collections on hot paths; this code is not a hot path but keep allocations minimal.
- Tests: JUnit 5 + AssertJ, `assertThat(x).isTrue()` style. Tag long HA integration tests `@Tag("slow")`. Required import for tags: `import org.junit.jupiter.api.Tag;`
- No `System.out`. No issue numbers in Javadoc/source comments (state behavioral invariants only). Do not add Claude as author.
- Do NOT commit on behalf of the user beyond the per-task commits this plan specifies (the user reviews before pushing). Per-task commits are expected by the execution skills.
- Use a normal dash, never the em dash character, in source and docs.
- Logger name strings (verbatim, already used in `RaftHAServer`): `"org.apache.ratis"`, `"org.apache.ratis.grpc.server.GrpcLogAppender"`, `"org.apache.ratis.grpc.server.GrpcServerProtocolService"`.

---

## File Structure

**New (main):**
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottle.java` - JUL `Filter` that drops the flood message.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTracker.java` - pure state machine emitting catch-up log events.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeter.java` - pure throttled byte-progress message generator.

**Modified (main):**
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - 3 new HA knobs.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` - install/remove Filter; construct `ClusterMonitor` with new args; implement `reportResyncProgress()`.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java` - unreachable/recovery narrative; `updateReplicaMatchIndex` gains `lastRpcElapsedMs`.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java` - pass `lastRpcElapsedMs` from `getFollowerStates()`.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java` - call new `HealthTarget.reportResyncProgress()` each tick.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java` - wire progress meter into `downloadSnapshot`; nested `ProgressReportingInputStream`.
- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` - align snapshot bookend wording.

**New (test):**
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottleTest.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTrackerTest.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeterTest.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFloodSuppressionIT.java`
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFollowerCatchupLoggingIT.java`

**Modified (test):**
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java` - unreachable/recovery narrative cases.
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java` - assert `reportResyncProgress()` invoked each tick.

---

## Task 1: Config knobs

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (insert after the `HA_HEALTH_CHECK_INTERVAL` entry, ~line 850)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ResyncLoggingConfigTest.java` (new)

**Interfaces:**
- Produces: `GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING` (Boolean, default `true`), `GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL` (Long ms, default `5000`), `GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD` (Long ms, default `10000`). Read via `configuration.getValueAsBoolean(...)` / `getValueAsLong(...)`.

- [ ] **Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ResyncLoggingConfigTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResyncLoggingConfigTest {

  @Test
  void defaultsAreSane() {
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING.getValueAsBoolean()).isTrue();
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL.getValueAsLong()).isEqualTo(5000L);
    assertThat(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD.getValueAsLong()).isEqualTo(10000L);
  }

  @Test
  void keysFollowHaNamespace() {
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING.getKey()).isEqualTo("arcadedb.ha.resyncProgressLogging");
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL.getKey()).isEqualTo("arcadedb.ha.resyncProgressInterval");
    assertThat(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD.getKey()).isEqualTo("arcadedb.ha.peerUnreachableThreshold");
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=ResyncLoggingConfigTest`
Expected: COMPILE FAILURE - `HA_RESYNC_PROGRESS_LOGGING` (and the other two symbols) cannot be resolved.

- [ ] **Step 3: Add the three enum entries**

In `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`, immediately after the `HA_HEALTH_CHECK_INTERVAL(...)` entry (the entry that ends `Long.class, 3000L),` around line 850), insert:

```java
  HA_RESYNC_PROGRESS_LOGGING("arcadedb.ha.resyncProgressLogging", SCOPE.SERVER,
      """
      When true (default), the leader emits a concise per-follower unreachable/reconnected narrative instead of \
      the raw per-retry Ratis appender flood, and a restarting follower logs its resync progress (Raft log \
      catch-up and full snapshot download). Set to false to restore the legacy raw Ratis logging.""",
      Boolean.class, true),

  HA_RESYNC_PROGRESS_INTERVAL("arcadedb.ha.resyncProgressInterval", SCOPE.SERVER,
      "Minimum interval in milliseconds between follower resync progress log lines (Raft log catch-up and snapshot download). Throttles progress output so a fast resync logs only start and finish.",
      Long.class, 5000L),

  HA_PEER_UNREACHABLE_THRESHOLD("arcadedb.ha.peerUnreachableThreshold", SCOPE.SERVER,
      "Time in milliseconds since the last successful RPC to a follower before the leader reports it as unreachable in the resync narrative. Does not change Raft membership or quorum.",
      Long.class, 10000L),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl engine -am install -DskipTests && mvn -q -pl ha-raft test -Dtest=ResyncLoggingConfigTest`
Expected: PASS (both tests green). The `engine` rebuild+install is required because `ha-raft` consumes `engine` as an installed artifact.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/ResyncLoggingConfigTest.java
git commit -m "feat(ha): add resync-progress logging configuration knobs"
```

---

## Task 2: Ratis appender flood Filter

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottle.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottleTest.java`

**Interfaces:**
- Produces: `class RatisAppenderLogThrottle implements java.util.logging.Filter` with `public boolean isLoggable(LogRecord record)` returning `false` only for the per-retry flood message, `true` otherwise (including a null/blank message).

- [ ] **Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottleTest.java`:

```java
package com.arcadedb.server.ha.raft.ratis;

import org.junit.jupiter.api.Test;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

class RatisAppenderLogThrottleTest {

  private final RatisAppenderLogThrottle filter = new RatisAppenderLogThrottle();

  @Test
  void suppressesTheFollowerFailedRetryFlood() {
    final LogRecord r = new LogRecord(Level.WARNING,
        "Follower failed (request=null, errorCount=4442); keep nextIndex (105325) unchanged and retry.");
    assertThat(filter.isLoggable(r)).isFalse();
  }

  @Test
  void letsGenuineAppenderErrorsThrough() {
    final LogRecord r = new LogRecord(Level.WARNING, "Failed appendEntries to follower: connection reset");
    assertThat(filter.isLoggable(r)).isTrue();
  }

  @Test
  void nullAndBlankMessagesPass() {
    assertThat(filter.isLoggable(new LogRecord(Level.INFO, null))).isTrue();
    assertThat(filter.isLoggable(new LogRecord(Level.INFO, ""))).isTrue();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=RatisAppenderLogThrottleTest`
Expected: COMPILE FAILURE - `RatisAppenderLogThrottle` does not exist.

- [ ] **Step 3: Implement the Filter**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottle.java`:

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
package com.arcadedb.server.ha.raft.ratis;

import java.util.logging.Filter;
import java.util.logging.LogRecord;

/**
 * java.util.logging {@link Filter} installed on the Ratis {@code GrpcLogAppender} logger to drop the
 * per-retry flood the leader emits while a follower is briefly unreachable (a restart): repeated
 * {@code Follower failed (request=...); keep nextIndex (...) unchanged and retry} lines whose
 * errorCount climbs into the thousands. The line is harmless (replication resumes automatically when
 * the follower returns) and the operator-facing signal is provided instead by the leader-side
 * unreachable/reconnected narrative. Every other record from that logger is passed through unchanged
 * so genuine appender errors still surface.
 */
public class RatisAppenderLogThrottle implements Filter {

  @Override
  public boolean isLoggable(final LogRecord record) {
    if (record == null)
      return true;
    final String msg = record.getMessage();
    if (msg == null)
      return true;
    // Match the specific retry flood: both fragments are present only in that message.
    return !(msg.contains("Follower failed") && msg.contains("keep nextIndex"));
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=RatisAppenderLogThrottleTest`
Expected: PASS (3 tests green).

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottle.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/ratis/RatisAppenderLogThrottleTest.java
git commit -m "feat(ha): add Ratis appender flood-suppression log filter"
```

---

## Task 3: Wire the Filter into RaftHAServer start/stop

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` (start() ~line 416; stop() ~lines 798-825; new private field + helpers)

**Interfaces:**
- Consumes: `RatisAppenderLogThrottle` (Task 2), `GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING` (Task 1).
- Produces: behavior only. When `HA_RESYNC_PROGRESS_LOGGING` is true, the `org.apache.ratis.grpc.server.GrpcLogAppender` JUL logger carries a `RatisAppenderLogThrottle` filter for the server's lifetime; removed on `stop()`.

Note: this task's deliverable is verified by the integration test in Task 9 (the filter's effect on real Ratis logging cannot be exercised by a focused unit test). Here we only add the wiring and confirm the project compiles.

- [ ] **Step 1: Add imports and a field**

In `RaftHAServer.java`, ensure these imports exist (add if missing):

```java
import com.arcadedb.server.ha.raft.ratis.RatisAppenderLogThrottle;
import java.util.logging.Filter;
import java.util.logging.Logger;
```

Add a private field near the other monitor fields (e.g. next to `healthMonitor`):

```java
  // Filter installed on the Ratis GrpcLogAppender JUL logger to suppress the per-retry flood while a
  // follower is briefly unreachable. Saved so stop() can detach exactly what start() attached and
  // restore any pre-existing filter.
  private Filter installedRatisAppenderFilter = null;
  private Filter previousRatisAppenderFilter  = null;
```

- [ ] **Step 2: Install the filter in start()**

In `start()`, immediately after the existing line `Logger.getLogger("org.apache.ratis").setLevel(Level.WARNING);` (~line 416), add:

```java
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING)) {
      final Logger appenderLogger = Logger.getLogger("org.apache.ratis.grpc.server.GrpcLogAppender");
      previousRatisAppenderFilter = appenderLogger.getFilter();
      installedRatisAppenderFilter = new RatisAppenderLogThrottle();
      appenderLogger.setFilter(installedRatisAppenderFilter);
    }
```

- [ ] **Step 3: Remove the filter in stop()**

In `stop()`, inside the `finally` block at the end of the try/catch that closes the Ratis client/server (the block at ~lines 822-824 that restores `previousLevels`), append after the level-restore loop:

```java
      final Logger appenderLogger = Logger.getLogger("org.apache.ratis.grpc.server.GrpcLogAppender");
      if (installedRatisAppenderFilter != null && appenderLogger.getFilter() == installedRatisAppenderFilter)
        appenderLogger.setFilter(previousRatisAppenderFilter);
      installedRatisAppenderFilter = null;
      previousRatisAppenderFilter = null;
```

- [ ] **Step 4: Compile**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "feat(ha): install Ratis appender flood filter on the leader"
```

---

## Task 4: ClusterMonitor unreachable/reconnected narrative

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java` (checkReplicaLag ~lines 316-326)
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` (ClusterMonitor construction ~lines 218-220)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java`

**Interfaces:**
- Consumes: `GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING`, `GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD` (Task 1); `getFollowerStates()` entry key `"lastRpcElapsedMs"` (already present, type `Long`).
- Produces: `ClusterMonitor.updateReplicaMatchIndex(String replicaId, long matchIndex, long lastRpcElapsedMs)` (signature gains the third parameter). New constructor `ClusterMonitor(long lagWarningThreshold, long stalledResyncDurationMs, Consumer<String> stalledReplicaHandler, boolean resyncNarrativeEnabled, long peerUnreachableThresholdMs)`. Existing constructors keep working by delegating with `resyncNarrativeEnabled=false, peerUnreachableThresholdMs=0`.

- [ ] **Step 1: Write the failing test**

Add to `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java` (new imports `java.util.logging.Handler`, `java.util.logging.Level`, `java.util.logging.LogRecord`, `java.util.logging.Logger`, `java.util.ArrayList`, `java.util.List`, `java.util.concurrent.atomic.AtomicLong` as needed):

```java
  @Test
  void emitsUnreachableThenReconnectedNarrative() {
    final AtomicLong now = new AtomicLong(100_000L);
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, true, 10_000L);
    monitor.setClock(now::get);
    monitor.updateLeaderCommitIndex(1000L);

    final CapturingHandler handler = CapturingHandler.attach();
    try {
      // Reachable tick: lastRpcElapsedMs below threshold -> no unreachable line.
      monitor.updateReplicaMatchIndex("replica-2", 1000L, 200L);
      assertThat(handler.linesContaining("unreachable")).isEmpty();

      // Goes unreachable: lastRpcElapsedMs over threshold -> one onset line.
      now.addAndGet(1_000L);
      monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
      assertThat(handler.linesContaining("unreachable")).hasSize(1);

      // Still unreachable within the 30s throttle window -> no new line.
      now.addAndGet(1_000L);
      monitor.updateReplicaMatchIndex("replica-2", 1000L, 13_000L);
      assertThat(handler.linesContaining("unreachable")).hasSize(1);

      // Comes back: lastRpcElapsedMs below threshold -> one reconnected line.
      now.addAndGet(1_000L);
      monitor.updateReplicaMatchIndex("replica-2", 1005L, 150L);
      assertThat(handler.linesContaining("reconnected")).hasSize(1);
    } finally {
      handler.detach();
    }
  }

  @Test
  void noNarrativeWhenDisabled() {
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, false, 10_000L);
    monitor.updateLeaderCommitIndex(1000L);
    final CapturingHandler handler = CapturingHandler.attach();
    try {
      monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
      assertThat(handler.linesContaining("unreachable")).isEmpty();
    } finally {
      handler.detach();
    }
  }

  /** Captures root-logger records for assertions. */
  private static final class CapturingHandler extends Handler {
    private final List<String> messages = new ArrayList<>();
    private final Logger        root    = Logger.getLogger("");
    private final Level         prev    = root.getLevel();

    static CapturingHandler attach() {
      final CapturingHandler h = new CapturingHandler();
      h.setLevel(Level.ALL);
      h.root.addHandler(h);
      if (h.root.getLevel() == null || h.root.getLevel().intValue() > Level.INFO.intValue())
        h.root.setLevel(Level.INFO);
      return h;
    }

    @Override public synchronized void publish(final LogRecord record) {
      if (record != null && record.getMessage() != null)
        messages.add(record.getMessage());
    }

    synchronized List<String> linesContaining(final String needle) {
      final List<String> out = new ArrayList<>();
      for (final String m : messages)
        if (m.contains(needle))
          out.add(m);
      return out;
    }

    @Override public void flush() { }

    void detach() {
      root.removeHandler(this);
      if (prev != null)
        root.setLevel(prev);
    }
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=ClusterMonitorTest#emitsUnreachableThenReconnectedNarrative+noNarrativeWhenDisabled`
Expected: COMPILE FAILURE - the 5-arg constructor and 3-arg `updateReplicaMatchIndex` do not exist yet.

- [ ] **Step 3: Add fields, constructor, and narrative logic to ClusterMonitor**

In `ClusterMonitor.java`:

Add two fields next to `stalledReplicaHandler` (~line 73):

```java
  private final    boolean                         resyncNarrativeEnabled;
  private final    long                            peerUnreachableThresholdMs;
```

Replace the existing 3-arg constructor (lines 92-100) so it delegates, and add the full constructor:

```java
  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler) {
    this(lagWarningThreshold, stalledResyncDurationMs, stalledReplicaHandler, false, 0L);
  }

  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler, final boolean resyncNarrativeEnabled,
      final long peerUnreachableThresholdMs) {
    if (stalledResyncDurationMs > 0 && stalledReplicaHandler == null)
      throw new IllegalArgumentException(
          "stalledReplicaHandler must be non-null when stalledResyncDurationMs > 0 (leader-driven recovery enabled)");
    this.lagWarningThreshold = lagWarningThreshold;
    this.stalledResyncDurationMs = stalledResyncDurationMs;
    this.stalledReplicaHandler = stalledReplicaHandler;
    this.resyncNarrativeEnabled = resyncNarrativeEnabled;
    this.peerUnreachableThresholdMs = peerUnreachableThresholdMs;
  }
```

Keep the 1-arg constructor (lines 80-82) unchanged - it already delegates to the 3-arg one.

Change the `updateReplicaMatchIndex` signature (line 111) to add the third parameter, and call the narrative tracker at the very start of the method body (before reading the clock is fine; reuse the same `now`). Replace the method header and insert the narrative call:

```java
  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex, final long lastRpcElapsedMs) {
    final long now = clock.getAsLong();
    trackReachabilityForNarrative(replicaId, lastRpcElapsedMs, now);
    final long leaderIdx = leaderCommitIndex;
```

(The rest of the method body is unchanged. The `computeIfAbsent` on line 116 still creates the `ReplicaState`; the narrative tracker below also uses `computeIfAbsent`, so order does not matter.)

Add the new method after `trackStallForRecovery` (after line 228):

```java
  /**
   * Emits a concise per-follower unreachable/reconnected narrative driven by the time since the last
   * successful RPC to the follower. Onset and reconnect are logged once each; while unreachable, a
   * reminder is logged at most once per {@link #LAG_LOG_THROTTLE_MS}. This replaces the raw Ratis
   * per-retry appender flood (which is suppressed by RatisAppenderLogThrottle) and never changes Raft
   * membership.
   */
  private void trackReachabilityForNarrative(final String replicaId, final long lastRpcElapsedMs, final long now) {
    if (!resyncNarrativeEnabled || peerUnreachableThresholdMs <= 0)
      return;

    final ReplicaState state = replicaStates.computeIfAbsent(replicaId,
        k -> new ReplicaState(0L, leaderCommitIndex));

    final boolean unreachable = lastRpcElapsedMs >= peerUnreachableThresholdMs;

    if (unreachable) {
      if (state.unreachableSinceMs == -1) {
        state.unreachableSinceMs = now;
        state.lastUnreachableWarnAtMs = now;
        LogManager.instance().log(this, Level.INFO,
            "Follower '%s' unreachable (no successful RPC for %dms); holding replication and retrying. It will recover automatically when it returns.",
            replicaId, lastRpcElapsedMs);
      } else if (now - state.lastUnreachableWarnAtMs >= LAG_LOG_THROTTLE_MS) {
        state.lastUnreachableWarnAtMs = now;
        LogManager.instance().log(this, Level.INFO,
            "Follower '%s' still unreachable for %dms (matchIndex=%d).",
            replicaId, now - state.unreachableSinceMs, state.lastMatchIndex);
      }
    } else if (state.unreachableSinceMs != -1) {
      final long downMs = now - state.unreachableSinceMs;
      state.unreachableSinceMs = -1;
      LogManager.instance().log(this, Level.INFO,
          "Follower '%s' reconnected after %dms; replication resuming (matchIndex=%d).",
          replicaId, downMs, state.lastMatchIndex);
    }
  }
```

Add three fields to the `ReplicaState` inner class (after line 284, before the constructor):

```java
    // Reachability-narrative state, mutated only from the single lag-monitor thread.
    long          unreachableSinceMs      = -1;
    long          lastUnreachableWarnAtMs = 0;
```

- [ ] **Step 4: Update the single production caller**

In `RaftClusterStatusExporter.java`, method `checkReplicaLag()` (~lines 316-326), change the loop body so it passes `lastRpcElapsedMs`:

```java
    for (final var fs : haServer.getFollowerStates())
      clusterMonitor.updateReplicaMatchIndex((String) fs.get("peerId"), (Long) fs.get("matchIndex"),
          (Long) fs.get("lastRpcElapsedMs"));
```

In `RaftHAServer.java`, the `ClusterMonitor` construction (~lines 218-220), pass the two new arguments:

```java
    final long stalledResyncDurationMs = configuration.getValueAsLong(
        GlobalConfiguration.HA_STALLED_REPLICA_RESYNC_DURATION_MS);
    final boolean resyncNarrative = configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING);
    final long peerUnreachableThresholdMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD);
    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold, stalledResyncDurationMs,
        this::forceResyncStalledReplica, resyncNarrative, peerUnreachableThresholdMs);
```

- [ ] **Step 5: Fix other callers of updateReplicaMatchIndex**

Run: `cd /Users/frank/projects/arcade/arcadedb && grep -rn "updateReplicaMatchIndex" ha-raft/src`
For each call other than the production caller above (these will be in `ClusterMonitorTest.java`), add a third argument. For existing pre-narrative test calls that do not care about reachability, pass `0L` (always reachable):

```java
monitor.updateReplicaMatchIndex("replica-1", 500L, 0L);
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests && mvn -q -pl ha-raft test -Dtest=ClusterMonitorTest`
Expected: PASS (all existing ClusterMonitorTest cases plus the two new ones).

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ClusterMonitor.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftClusterStatusExporter.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/ClusterMonitorTest.java
git commit -m "feat(ha): leader-side follower unreachable/reconnected narrative"
```

---

## Task 5: FollowerResyncProgressTracker

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTracker.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTrackerTest.java`

**Interfaces:**
- Produces:
  - `enum FollowerResyncProgressTracker.Event { NONE, STARTED, PROGRESS, FINISHED }`
  - `record FollowerResyncProgressTracker.Tick(Event event, String message)` (message is `null` when `event == NONE`)
  - constructor `FollowerResyncProgressTracker(long progressIntervalMs)`
  - `Tick onTick(long appliedIndex, long leaderCommitIndex, long nowMs)` - pure; emits `STARTED` once when the follower is first observed behind, `PROGRESS` at most once per `progressIntervalMs` while still behind, `FINISHED` once when it catches up. Returns `NONE` (no state change) when either index is negative (transient read failure).

- [ ] **Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTrackerTest.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ha.raft.FollowerResyncProgressTracker.Event;
import com.arcadedb.server.ha.raft.FollowerResyncProgressTracker.Tick;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FollowerResyncProgressTrackerTest {

  @Test
  void notBehindProducesNothing() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);
    assertThat(t.onTick(1000L, 1000L, 0L).event()).isEqualTo(Event.NONE);
  }

  @Test
  void emitsStartProgressFinishAcrossCatchUp() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);

    final Tick start = t.onTick(900L, 1000L, 0L);
    assertThat(start.event()).isEqualTo(Event.STARTED);
    assertThat(start.message()).contains("mode=catch-up").contains("100 entries behind");

    // Within the progress interval: no progress line.
    assertThat(t.onTick(920L, 1000L, 1000L).event()).isEqualTo(Event.NONE);

    // After the interval, still behind: progress line.
    final Tick prog = t.onTick(960L, 1000L, 6000L);
    assertThat(prog.event()).isEqualTo(Event.PROGRESS);
    assertThat(prog.message()).contains("catch-up").contains("960/1000");

    // Caught up: finished line, once.
    final Tick done = t.onTick(1000L, 1000L, 7000L);
    assertThat(done.event()).isEqualTo(Event.FINISHED);
    assertThat(done.message()).contains("mode=catch-up").contains("result=ok");

    // Idle afterwards: nothing.
    assertThat(t.onTick(1000L, 1000L, 8000L).event()).isEqualTo(Event.NONE);
  }

  @Test
  void negativeIndexIsIgnoredWithoutStateChange() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);
    assertThat(t.onTick(-1L, 1000L, 0L).event()).isEqualTo(Event.NONE);
    // A real "behind" tick still starts cleanly afterwards.
    assertThat(t.onTick(900L, 1000L, 1000L).event()).isEqualTo(Event.STARTED);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=FollowerResyncProgressTrackerTest`
Expected: COMPILE FAILURE - `FollowerResyncProgressTracker` does not exist.

- [ ] **Step 3: Implement the tracker**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTracker.java`:

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

/**
 * Pure state machine that turns a sequence of (appliedIndex, leaderCommitIndex, now) observations on a
 * follower into a concise resync narrative: one STARTED line when the follower is first seen behind the
 * leader after a (re)start, throttled PROGRESS lines while it catches up via Raft log replay, and one
 * FINISHED line when it draws level. Driven by the follower-only health-monitor tick. Holds no clock or
 * logger of its own so it can be unit-tested deterministically; the caller supplies {@code now} and logs
 * the returned message.
 */
public final class FollowerResyncProgressTracker {

  public enum Event {NONE, STARTED, PROGRESS, FINISHED}

  public record Tick(Event event, String message) {
    static final Tick NONE = new Tick(Event.NONE, null);
  }

  private final long progressIntervalMs;

  private boolean active        = false;
  private long    startMs       = 0;
  private long    startApplied  = 0;
  private long    behindAtStart = 0;
  private long    lastProgressMs = 0;

  public FollowerResyncProgressTracker(final long progressIntervalMs) {
    this.progressIntervalMs = progressIntervalMs;
  }

  public Tick onTick(final long appliedIndex, final long leaderCommitIndex, final long nowMs) {
    if (appliedIndex < 0 || leaderCommitIndex < 0)
      return Tick.NONE; // transient read failure: do not change state

    final long behind = leaderCommitIndex - appliedIndex;

    if (!active) {
      if (behind <= 0)
        return Tick.NONE;
      active = true;
      startMs = nowMs;
      startApplied = appliedIndex;
      behindAtStart = behind;
      lastProgressMs = nowMs;
      return new Tick(Event.STARTED, String.format(
          "HA resync started (mode=catch-up): %d entries behind leader (applied=%d, leader=%d)",
          behind, appliedIndex, leaderCommitIndex));
    }

    if (behind <= 0) {
      active = false;
      final long durationMs = nowMs - startMs;
      final long caughtUp = appliedIndex - startApplied;
      return new Tick(Event.FINISHED, String.format(
          "HA resync finished (mode=catch-up, duration=%dms, result=ok): caught up %d entries to index %d",
          durationMs, caughtUp, appliedIndex));
    }

    if (nowMs - lastProgressMs >= progressIntervalMs) {
      lastProgressMs = nowMs;
      final long elapsedMs = Math.max(1L, nowMs - startMs);
      final long caughtUp = appliedIndex - startApplied;
      final double ratePerSec = caughtUp * 1000.0 / elapsedMs;
      return new Tick(Event.PROGRESS, String.format(
          "catch-up: applied=%d/%d (%d behind, ~%.0f entries/s)",
          appliedIndex, leaderCommitIndex, behind, ratePerSec));
    }

    return Tick.NONE;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=FollowerResyncProgressTrackerTest`
Expected: PASS (3 tests green).

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTracker.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/FollowerResyncProgressTrackerTest.java
git commit -m "feat(ha): add follower catch-up resync progress tracker"
```

---

## Task 6: Drive the tracker from the follower health tick

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java` (HealthTarget interface + tick())
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` (implement `reportResyncProgress()`; own the tracker)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java`

**Interfaces:**
- Consumes: `FollowerResyncProgressTracker` (Task 5); existing `RaftHAServer.getLastAppliedIndex()` / `getCommitIndex()` / `isLeader()`; existing `ArcadeStateMachine.isSnapshotDownloadPending()`.
- Produces: new default method `void HealthTarget.reportResyncProgress()` (no-op default), called by `HealthMonitor.tick()` every tick. `RaftHAServer.reportResyncProgress()` reads the indices on a follower and logs the tracker's output at INFO.

- [ ] **Step 1: Write the failing test**

Add to `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java`. Find the existing fake/stub `HealthTarget` used by the test (typically an inner class or lambda-based stub). Add a counter field and override, then a test. If the test uses an inline anonymous `HealthTarget`, add an `AtomicInteger reportCalls` captured by the stub. Concretely, add this test plus a minimal stub if one is not already present:

```java
  @Test
  void tickInvokesReportResyncProgress() {
    final AtomicInteger reportCalls = new AtomicInteger();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override public org.apache.ratis.util.LifeCycle.State getRaftLifeCycleState() {
        return org.apache.ratis.util.LifeCycle.State.RUNNING;
      }
      @Override public boolean isShutdownRequested() { return false; }
      @Override public void restartRatisIfNeeded() { }
      @Override public void reportResyncProgress() { reportCalls.incrementAndGet(); }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 1000L);
    monitor.tick();
    assertThat(reportCalls.get()).isEqualTo(1);
  }
```

Add imports if missing: `import java.util.concurrent.atomic.AtomicInteger;`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=HealthMonitorTest#tickInvokesReportResyncProgress`
Expected: COMPILE FAILURE - `reportResyncProgress()` is not a member of `HealthTarget`.

- [ ] **Step 3: Add the HealthTarget method and call it from tick()**

In `HealthMonitor.java`, add to the `HealthTarget` interface (after `refreshPeerAllowlist()`, ~line 92):

```java
    /**
     * Logs Raft log catch-up resync progress when this node is a follower that is behind the leader.
     * No-op on the leader, when resync logging is disabled, or while a snapshot install is in progress.
     */
    default void reportResyncProgress() {
    }
```

In `tick()`, after the existing `target.refreshPeerAllowlist();` call (~line 179), add:

```java
    target.reportResyncProgress();
```

- [ ] **Step 4: Run the HealthMonitor test to verify it passes**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=HealthMonitorTest`
Expected: PASS (existing cases plus the new one).

- [ ] **Step 5: Implement reportResyncProgress() in RaftHAServer**

In `RaftHAServer.java`, add a tracker field near `healthMonitor`:

```java
  // Follower-side Raft log catch-up narrative. Driven by the health-monitor tick; only active on a
  // follower that is behind the leader and not installing a snapshot.
  private FollowerResyncProgressTracker resyncProgressTracker = null;
```

In the constructor or wherever `healthMonitor` is built (~line 480), initialize the tracker once, gated by config:

```java
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING))
      this.resyncProgressTracker = new FollowerResyncProgressTracker(
          configuration.getValueAsLong(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL));
```

Add the override (place near the other `HealthTarget` overrides such as `isFollowerLaggingBeyond`):

```java
  @Override
  public void reportResyncProgress() {
    final FollowerResyncProgressTracker tracker = resyncProgressTracker;
    if (tracker == null || raftServer == null || shutdownRequested || isLeader())
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null || sm.isSnapshotDownloadPending())
      return; // the snapshot path logs its own bookends
    final long applied = getLastAppliedIndex();
    final long commit = getCommitIndex();
    final FollowerResyncProgressTracker.Tick tick = tracker.onTick(applied, commit, System.currentTimeMillis());
    if (tick.event() != FollowerResyncProgressTracker.Event.NONE)
      LogManager.instance().log(this, Level.INFO, tick.message());
  }
```

(`LogManager`, `Level`, and `FollowerResyncProgressTracker` are in the same package or already imported in `RaftHAServer`; add `import com.arcadedb.log.LogManager;` and `import java.util.logging.Level;` only if not already present - they are.)

- [ ] **Step 6: Compile and run HealthMonitorTest again**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests && mvn -q -pl ha-raft test -Dtest=HealthMonitorTest`
Expected: BUILD SUCCESS and PASS.

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/HealthMonitor.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/HealthMonitorTest.java
git commit -m "feat(ha): log follower Raft log catch-up progress from the health tick"
```

---

## Task 7: Snapshot download progress

**Files:**
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeter.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java` (downloadSnapshot ~lines 717-795; add nested `ProgressReportingInputStream`)
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeterTest.java`

**Interfaces:**
- Consumes: `GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING`, `GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL`; existing `SnapshotInstaller.CountingInputStream`.
- Produces: `SnapshotDownloadProgressMeter(String databaseName, long intervalMs)` with `String lineIfDue(long cumulativeBytes, long nowMs)` returning a progress message or `null`. The meter is fed the running compressed-byte count during a snapshot download and the caller logs any non-null line.

- [ ] **Step 1: Write the failing test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeterTest.java`:

```java
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotDownloadProgressMeterTest {

  @Test
  void firstCallNeverLogsAndEstablishesBaseline() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    assertThat(meter.lineIfDue(0L, 0L)).isNull();
  }

  @Test
  void logsOnlyAfterTheInterval() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    meter.lineIfDue(0L, 0L);                       // baseline
    assertThat(meter.lineIfDue(10_000_000L, 1000L)).isNull();    // too soon
    final String line = meter.lineIfDue(40_000_000L, 6000L);     // interval elapsed
    assertThat(line).isNotNull();
    assertThat(line).contains("db=foo").contains("MB").contains("MB/s");
  }

  @Test
  void reThrottlesAfterEachEmittedLine() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    meter.lineIfDue(0L, 0L);
    assertThat(meter.lineIfDue(40_000_000L, 6000L)).isNotNull();
    assertThat(meter.lineIfDue(50_000_000L, 7000L)).isNull();    // within interval of last emit
    assertThat(meter.lineIfDue(80_000_000L, 12_000L)).isNotNull();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=SnapshotDownloadProgressMeterTest`
Expected: COMPILE FAILURE - `SnapshotDownloadProgressMeter` does not exist.

- [ ] **Step 3: Implement the meter**

Create `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeter.java`:

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

/**
 * Generates throttled progress lines for a snapshot download. The HTTP response carries no reliable
 * content length (the body is a streamed ZIP), so progress is reported as cumulative bytes downloaded
 * and instantaneous throughput rather than a percentage or ETA. Pure and clock-free: the caller passes
 * the running byte count and current time; {@link #lineIfDue} returns a message at most once per
 * configured interval, otherwise {@code null}. The first call establishes the baseline and never logs.
 */
public final class SnapshotDownloadProgressMeter {

  private static final double BYTES_PER_MB = 1024.0 * 1024.0;

  private final String databaseName;
  private final long   intervalMs;

  private boolean started        = false;
  private long    lastLogMs      = 0;
  private long    lastLogBytes   = 0;

  public SnapshotDownloadProgressMeter(final String databaseName, final long intervalMs) {
    this.databaseName = databaseName;
    this.intervalMs = intervalMs;
  }

  public String lineIfDue(final long cumulativeBytes, final long nowMs) {
    if (!started) {
      started = true;
      lastLogMs = nowMs;
      lastLogBytes = cumulativeBytes;
      return null;
    }
    final long sinceMs = nowMs - lastLogMs;
    if (sinceMs < intervalMs)
      return null;

    final double mbSoFar = cumulativeBytes / BYTES_PER_MB;
    final double mbPerSec = (cumulativeBytes - lastLogBytes) / BYTES_PER_MB * 1000.0 / Math.max(1L, sinceMs);
    lastLogMs = nowMs;
    lastLogBytes = cumulativeBytes;
    return String.format("snapshot download db=%s: %.1f MB so far (%.1f MB/s)", databaseName, mbSoFar, mbPerSec);
  }
}
```

- [ ] **Step 4: Run the meter test to verify it passes**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=SnapshotDownloadProgressMeterTest`
Expected: PASS (3 tests green).

- [ ] **Step 5: Wire the meter into the download stream**

In `SnapshotInstaller.java`, add imports if missing: `import com.arcadedb.GlobalConfiguration;` (already used in the file for the read timeout) and `import java.io.FilterInputStream;`.

Add a nested decorator class next to `CountingInputStream` (after line 898, inside the `SnapshotInstaller` class):

```java
  /**
   * Wraps the snapshot download stream to feed the cumulative byte count to a progress meter and log
   * any due progress line. Reports compressed bytes read off the wire (the meter measures download, not
   * decompression). No-op when the meter is null (resync logging disabled).
   */
  private static final class ProgressReportingInputStream extends FilterInputStream {
    private final SnapshotDownloadProgressMeter meter;
    private       long                          total;

    ProgressReportingInputStream(final InputStream in, final SnapshotDownloadProgressMeter meter) {
      super(in);
      this.meter = meter;
    }

    private void account(final int n) {
      if (n > 0) {
        total += n;
        final String line = meter.lineIfDue(total, System.currentTimeMillis());
        if (line != null)
          HALog.log(SnapshotInstaller.class, HALog.BASIC, line);
      }
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b != -1)
        account(1);
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int n = super.read(b, off, len);
      account(n);
      return n;
    }
  }
```

In `downloadSnapshot(...)`, change the stream construction (lines 750-751). Replace:

```java
      final CountingInputStream rawCounter = new CountingInputStream(connection.getInputStream());
      try (final ZipInputStream zipIn = new ZipInputStream(rawCounter)) {
```

with:

```java
      InputStream source = new CountingInputStream(connection.getInputStream());
      final CountingInputStream rawCounter = (CountingInputStream) source;
      final boolean progressLogging = server == null
          || server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING);
      if (progressLogging) {
        final String dbName = targetDir.getFileName() != null ? targetDir.getFileName().toString() : "snapshot";
        final long intervalMs = server != null
            ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL)
            : 5000L;
        source = new ProgressReportingInputStream(rawCounter, new SnapshotDownloadProgressMeter(dbName, intervalMs));
      }
      try (final ZipInputStream zipIn = new ZipInputStream(source)) {
```

The rest of `downloadSnapshot` continues to reference `rawCounter.getCount()` for the per-entry compression-ratio check, which is unaffected (the `ProgressReportingInputStream`, when present, sits above `rawCounter` and reads through it).

- [ ] **Step 6: Compile and run the meter test plus existing installer tests**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests && mvn -q -pl ha-raft test -Dtest=SnapshotDownloadProgressMeterTest,SnapshotInstallerRetryTest,SnapshotInstallerSSLTest`
Expected: BUILD SUCCESS and all PASS.

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeter.java \
  ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotDownloadProgressMeterTest.java
git commit -m "feat(ha): log periodic snapshot download progress"
```

---

## Task 8: Align snapshot bookend wording

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` (lines 690-691 and 774)

**Interfaces:**
- Produces: log-string change only. Snapshot path bookends now share the `HA resync started (mode=snapshot ...)` / `HA resync finished (mode=snapshot ...)` phrasing used by the catch-up path (Task 5), so an operator can grep `HA resync started` / `HA resync finished` for both modes.

- [ ] **Step 1: Update the start bookend**

In `ArcadeStateMachine.java` at lines 690-691, replace:

```java
    LogManager.instance().log(this, Level.INFO,
        "Snapshot installation requested from leader (firstLogIndex=%s). Starting full resync...", firstTermIndexInLog);
```

with:

```java
    LogManager.instance().log(this, Level.INFO,
        "HA resync started (mode=snapshot, reason=leader snapshot install): firstLogIndex=%s", firstTermIndexInLog);
```

- [ ] **Step 2: Update the finish bookend**

In `ArcadeStateMachine.java` at line 774, replace:

```java
        LogManager.instance().log(this, Level.INFO, "Full resync from leader completed (snapshotIndex=%d)", snapshotIndex);
```

with:

```java
        LogManager.instance().log(this, Level.INFO,
            "HA resync finished (mode=snapshot, result=ok): snapshotIndex=%d", snapshotIndex);
```

- [ ] **Step 3: Compile**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "refactor(ha): unify snapshot resync log bookend wording"
```

---

## Task 9: Shared log-capture helper + leader flood suppression IT

> Revised during execution: ArcadeDB's own `LogManager` INFO lines do NOT reach a `java.util.logging`
> root Handler (the test classpath's `arcadedb-log.properties` pins `com.arcadedb` to `SEVERE`), so the
> narrative lines must be captured with ArcadeDB's logger seam (`LogManager.instance().setLogger(...)`),
> while the raw Apache Ratis flood (a separate `java.util.logging` record) is still observed with a JUL
> Handler. This task therefore adds a small shared capturing logger and uses BOTH mechanisms.

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/CapturingTestLogger.java` (shared helper, reused by Task 10)
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFloodSuppressionIT.java`

**Interfaces:**
- Consumes: `BaseRaftHATest`, `RaftIdleReplicaRestartIT` (as the structural model), `GlobalConfiguration`, `com.arcadedb.log.LogManager` / `Logger` / `DefaultLogger`, the Task 2/3 filter wiring, the Task 4 narrative.
- Produces: `CapturingTestLogger` - `static CapturingTestLogger install()`, `void uninstall()`, `int countContaining(String... needles)` (counts captured ArcadeDB messages whose template contains ALL needles).

- [ ] **Step 1: Create the shared capturing logger**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/CapturingTestLogger.java`:

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

/**
 * Test helper that captures every ArcadeDB LogManager message across all in-process servers by
 * replacing the global logger for the duration of a test. Thread-safe: the in-process HA servers log
 * from many threads concurrently. It captures the raw message template (before argument substitution),
 * which is sufficient for substring assertions because every asserted phrase is a literal in the format
 * string. It does NOT capture Apache Ratis's own java.util.logging output - use a JUL Handler for that.
 */
final class CapturingTestLogger implements Logger {

  private final List<String> messages = new CopyOnWriteArrayList<>();

  static CapturingTestLogger install() {
    final CapturingTestLogger logger = new CapturingTestLogger();
    LogManager.instance().setLogger(logger);
    return logger;
  }

  void uninstall() {
    LogManager.instance().setLogger(new DefaultLogger());
  }

  int countContaining(final String... needles) {
    int n = 0;
    for (final String m : messages) {
      boolean all = true;
      for (final String needle : needles)
        if (!m.contains(needle)) {
          all = false;
          break;
        }
      if (all)
        n++;
    }
    return n;
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
      final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
      final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    if (iMessage != null)
      messages.add(iMessage);
  }

  @Override
  public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
      final String context, final Object... args) {
    if (iMessage != null)
      messages.add(iMessage);
  }

  @Override
  public void flush() {
  }
}
```

- [ ] **Step 2: Study the real test harness**

Before writing the IT, read `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftIdleReplicaRestartIT.java` and `BaseRaftHATest.java` in full. The Task 9/10 ITs MUST use the real `BaseRaftHATest` API, not invented method names. From `RaftIdleReplicaRestartIT` identify the concrete calls for: discovering the leader index, finding a non-leader/replica index, stopping a server, restarting a server, waiting for replication to catch up, and running a write command against the leader. From `BaseRaftHATest` identify how `onServerConfiguration(ContextConfiguration)` is overridden and how `getServerCount()` works. Use those exact APIs in the IT below, replacing the placeholder calls.

- [ ] **Step 3: Write the leader flood-suppression IT**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFloodSuppressionIT.java`. The structure below is the intent; bind the placeholder helper calls (`leaderIndex()`, `firstReplicaIndex()`, `stopServer(int)`, `restartServer(int)`, `runWriteOnLeader()`, `waitForReplicationToCatchUp()`) to the real `BaseRaftHATest`/`RaftIdleReplicaRestartIT` equivalents found in Step 2.

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("slow")
class RaftLeaderFloodSuppressionIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Enable the follower tick and make the leader call a follower unreachable quickly so the
    // narrative fires within the test window. The flood filter is on by default (HA_RESYNC_PROGRESS_LOGGING).
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 500L);
    config.setValue(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD, 2000L);
  }

  @Test
  void leaderDoesNotFloodAndNarratesUnreachableReplica() throws Exception {
    final int replica = firstReplicaIndex();

    // JUL Handler observes Apache Ratis's own logging (the raw flood). ArcadeDB's narrative is captured
    // separately by CapturingTestLogger because ArcadeDB INFO lines are JUL-suppressed in tests.
    final RatisFloodWatcher ratis = RatisFloodWatcher.attach();
    final CapturingTestLogger arcade = CapturingTestLogger.install();
    try {
      stopServer(replica);

      // Keep the leader advancing its commit index while the replica is down, so the appender would
      // normally flood and the narrative has something to report.
      for (int i = 0; i < 40; i++) {
        runWriteOnLeader();
        Thread.sleep(500);
      }

      restartServer(replica);
      waitForReplicationToCatchUp();

      // The raw per-retry Ratis flood must be suppressed by the filter (allow a tiny transient margin
      // for any line emitted in the window before the filter is installed on a restart).
      assertThat(ratis.countContaining("Follower failed", "keep nextIndex")).isLessThanOrEqualTo(2);

      // The operator-facing narrative replaces it.
      assertThat(arcade.countContaining("unreachable")).isGreaterThanOrEqualTo(1);
      assertThat(arcade.countContaining("reconnected")).isGreaterThanOrEqualTo(1);
    } finally {
      arcade.uninstall();
      ratis.detach();
    }
  }

  /** Captures Apache Ratis's java.util.logging records (the raw appender flood). */
  private static final class RatisFloodWatcher extends Handler {
    private final List<String> messages = new ArrayList<>();
    private final Logger        root    = Logger.getLogger("");
    private final Level         prev    = root.getLevel();

    static RatisFloodWatcher attach() {
      final RatisFloodWatcher h = new RatisFloodWatcher();
      h.setLevel(Level.ALL);
      h.root.addHandler(h);
      if (h.root.getLevel() == null || h.root.getLevel().intValue() > Level.WARNING.intValue())
        h.root.setLevel(Level.WARNING);
      return h;
    }

    @Override public synchronized void publish(final LogRecord record) {
      if (record != null && record.getMessage() != null)
        messages.add(record.getMessage());
    }

    synchronized int countContaining(final String... needles) {
      int n = 0;
      for (final String m : messages) {
        boolean all = true;
        for (final String needle : needles)
          if (!m.contains(needle)) { all = false; break; }
        if (all)
          n++;
      }
      return n;
    }

    @Override public void flush() { }
    @Override public void close() { }

    void detach() {
      root.removeHandler(this);
      if (prev != null)
        root.setLevel(prev);
    }
  }
}
```

- [ ] **Step 4: Run the test**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=RaftLeaderFloodSuppressionIT`
Expected: PASS. If the Ratis count exceeds the margin, confirm the filter is installed (Task 3) and `HA_RESYNC_PROGRESS_LOGGING` is true for the test servers. If the narrative counts are zero, the follower may not have been seen unreachable long enough - lengthen the write loop or lower `HA_PEER_UNREACHABLE_THRESHOLD`. If the test is timing-flaky despite reasonable tolerances, report DONE_WITH_CONCERNS describing the flakiness rather than weakening the assertion to meaninglessness.

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/CapturingTestLogger.java \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftLeaderFloodSuppressionIT.java
git commit -m "test(ha): IT for leader flood suppression and unreachable narrative"
```

---

## Task 10: Integration test - follower catch-up logging

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFollowerCatchupLoggingIT.java`

**Interfaces:**
- Consumes: `BaseRaftHATest`, the Task 9 `CapturingTestLogger`, the Task 5/6 catch-up tracker wiring, Task 8 snapshot bookend wording. Asserts the follower logs the unified resync bookends after a restart under write load.

- [ ] **Step 1: Write the test**

Create `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFollowerCatchupLoggingIT.java`, reusing the `CapturingTestLogger` from Task 9 and the same real `BaseRaftHATest` helpers (bind the placeholder calls to the real API as in Task 9). This IT only needs the ArcadeDB capture (no JUL Handler) because it asserts on ArcadeDB-authored resync lines.

```java
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("slow")
class RaftFollowerCatchupLoggingIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The base test disables the health monitor (interval 0); the catch-up narrative is driven by its
    // tick, so enable it and shorten the progress interval for the test window.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 500L);
    config.setValue(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL, 1000L);
  }

  @Test
  void restartedFollowerLogsResyncBookends() throws Exception {
    final int replica = firstReplicaIndex();

    final CapturingTestLogger arcade = CapturingTestLogger.install();
    try {
      stopServer(replica);

      // Accumulate a backlog the follower must replay on return.
      for (int i = 0; i < 30; i++) {
        runWriteOnLeader();
        Thread.sleep(500);
      }

      restartServer(replica);
      waitForReplicationToCatchUp();

      // Either a Raft log catch-up (common) or, if it fell far enough behind, a snapshot resync.
      final int catchUp = arcade.countContaining("HA resync started", "mode=catch-up");
      final int snapshot = arcade.countContaining("HA resync started", "mode=snapshot");
      assertThat(catchUp + snapshot).isGreaterThanOrEqualTo(1);

      assertThat(arcade.countContaining("HA resync finished")).isGreaterThanOrEqualTo(1);
    } finally {
      arcade.uninstall();
    }
  }
}
```

- [ ] **Step 2: Run the test**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=RaftFollowerCatchupLoggingIT`
Expected: PASS. If no bookend appears, confirm `HA_HEALTH_CHECK_INTERVAL` is positive for the test servers (the base test disables it) and that the follower actually fell behind (lengthen the write loop). If timing-flaky despite reasonable tolerances, report DONE_WITH_CONCERNS rather than gutting the assertion.

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftFollowerCatchupLoggingIT.java
git commit -m "test(ha): IT for follower catch-up resync logging"
```

---

## Final verification

- [ ] **Run the full ha-raft unit suite (excluding slow ITs):**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft -am install -DskipTests && mvn -q -pl ha-raft test -Dtest='!*IT'`
Expected: BUILD SUCCESS, all unit tests green (config, filter, tracker, meter, ClusterMonitor, HealthMonitor).

- [ ] **Run the two new ITs explicitly:**

Run: `cd /Users/frank/projects/arcade/arcadedb && mvn -q -pl ha-raft test -Dtest=RaftLeaderFloodSuppressionIT,RaftFollowerCatchupLoggingIT,RaftIdleReplicaRestartIT`
Expected: PASS (the existing `RaftIdleReplicaRestartIT` still passes - the new filter must not break its `received INCONSISTENCY reply` assertion, which is a different message and is not matched by `RatisAppenderLogThrottle`).

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Master flood suppression via JUL Filter -> Tasks 2, 3.
- Master honest summary (unreachable / reconnected, no membership change) -> Task 4.
- Follower catch-up visibility -> Tasks 5, 6.
- Snapshot download progress -> Task 7.
- Unified lifecycle bookends -> Task 5 (catch-up wording) + Task 8 (snapshot wording aligned).
- Config knobs + levels (INFO for new lines, genuine failures unchanged) -> Task 1; all new lines logged at `Level.INFO`; existing SEVERE/WARNING paths untouched.
- Testing (master unit + IT, follower unit + IT, reconciler/meter unit) -> Tasks 2/4 unit + 9 IT (master); 5/6 unit + 10 IT (follower); 7 unit (meter).
- Out-of-scope items (auto-eviction, quorum changes, ForkJoinPool migration) -> not implemented, consistent with spec.

**Type consistency:** `updateReplicaMatchIndex(String,long,long)` used identically in ClusterMonitor (Task 4 def) and RaftClusterStatusExporter (Task 4 caller) and ClusterMonitorTest (Task 4). `FollowerResyncProgressTracker.Tick`/`Event` names match between Task 5 def and Task 6 use. `SnapshotDownloadProgressMeter.lineIfDue(long,long)` matches between Task 7 def and the nested decorator. `HealthTarget.reportResyncProgress()` matches between Task 6 interface, HealthMonitor call, and RaftHAServer override.

**Placeholder scan:** The only intentionally abstract elements are the `BaseRaftHATest` helper method names in Tasks 9-10, flagged explicitly with instructions to bind them to the real equivalents in `RaftIdleReplicaRestartIT`. All code steps contain complete code.
