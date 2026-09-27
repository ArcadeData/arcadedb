# HA Chaos Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A long-running, seeded chaos test (`HaChaosIT`) that drives randomized process, network and freeze faults
against one 3- or 5-node Raft cluster under continuous write load, checking a client-side ledger of acknowledged writes
after every step.

**Architecture:** A small harness in `e2e-ha` package `com.arcadedb.containers.ha.chaos`. Pure logic (config, ledger,
invariant checker, fault selection, checkpoint, runner, report) talks to the cluster only through three interfaces
(`NodeControl`, `NodeReader`, `LoadGenerator`), so all of it is unit-tested with fakes and no containers. `HaChaosIT`
wires the Docker/Toxiproxy-backed implementations on top of the existing `ContainersTestTemplate`. Everything is tagged
`chaos`, excluded from the nightly HA job, and run by a new workflow.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Awaitility, Testcontainers (docker-java API), toxiproxy-java, JDK
`HttpURLConnection` and `com.sun.net.httpserver.HttpServer` (tests), ArcadeDB embedded engine (schema test), GitHub
Actions.

**Spec:** `docs/superpowers/specs/2026-09-23-ha-chaos-test-design.md`

## Global Constraints

- Worktree: `/Users/frank/projects/arcade/arcadedb/.worktrees/feat/ha-chaos-test`. Run every command from its root.
- **Do not commit.** The maintainer commits after review (project CLAUDE.md). Each task ends with `git add` of its files
  instead of a commit.
- No new dependencies. Everything used is already on the `e2e-ha` test classpath.
- All new code is test code under `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/`, except one overload in
  `load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java` and the workflow file.
- Every new Java file starts with this header, verbatim:

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
```

  (The code blocks below omit it to save space; add it to every file.)
- Style: `final` on locals and parameters where possible; single-statement `if` without braces; import classes, never
  fully qualified names; JSON via `com.arcadedb.serializer.json.JSONObject` / `JSONArray`; assertions in the
  `assertThat(x).isTrue()` form; no `System.out`; no author tags naming Claude; no em dash characters in docs.
- `e2e-ha` has surefire disabled (`skipTests=true`). Every test class in this plan is therefore named `*IT` and tagged
  `@Tag("chaos")` at class level, so failsafe runs it and the nightly job excludes it.
- **One-time prerequisite** (and again after Task 10 touches `load-tests`):
  `./mvnw install -DskipTests -pl e2e-ha -am -q`
- **Test command** for a single class (called `RUN <Class>` below):
  `./mvnw verify -Pintegration -pl e2e-ha -Dfailsafe.excludedGroups= -Dit.test=<Class>`
  Read the result from Maven's `Results:` block, not from report files.

## Review Focus

1. **A write sent to a paused leader and processed after unpause** (the response is lost, the write may commit late):
   expected to be `UNKNOWN`, resolved at a later checkpoint, and never a false I1 or I3. Pinned by
   `InvariantCheckerIT.lateCommitIsNotAViolation` (Task 3) and `WorkloadIT.readTimeoutIsUnknown` (Task 6).
2. **A restarted container published on a different host port**: expected that writers and the checkpoint reader follow
   the new port. Pinned by the Task 10 smoke run restricted to `kill,stop` (Step 10.6).
3. **A node whose row count is an exact multiple of the page size**: expected that the scan fetches one more empty page,
   stops, and adds no duplicates. Pinned by `HttpNodeReaderIT.exactMultipleOfPageSizeTerminates` (Task 7).
4. **A Docker or Toxiproxy error in the middle of a step**: expected to be reported as `HARNESS`, never as a cluster
   safety bug. Pinned by `ChaosRunnerIT.harnessErrorIsNotReportedAsAClusterBug` (Task 9).
5. **A writer stuck on a paused node for its full read timeout**: expected that the availability check does not call the
   cluster unavailable because the only writer was blocked. Pinned by
   `ChaosRunnerIT.availabilityWindowIsNeverShorterThanAWriterTimeout` (Task 9).

---

### Task 1: Module wiring and `ChaosConfig`

**Files:**
- Modify: `e2e-ha/pom.xml` (add a `<properties>` block after `<packaging>jar</packaging>`)
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfig.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfigIT.java`

**Interfaces:**
- Produces: `record ChaosConfig(long seed, int nodes, Duration duration, int maxSteps, int writers, Map<String,Integer> faultWeights, Duration holdMin, Duration holdMax, Duration calmMin, Duration calmMax, Duration convergenceTimeout, Duration electionTimeout, Duration availabilityGrace)`;
  `static ChaosConfig fromProperties(Properties)`; `static Map<String,Integer> parseFaults(String)`;
  `static final List<String> ALL_FAULTS`; `String faultsSpec()`; `String replayCommand()`. `maxSteps == 0` means unlimited.

- [ ] **Step 1.1: Exclude the `chaos` tag from the module's default failsafe run**

In `e2e-ha/pom.xml`, directly after `<packaging>jar</packaging>`, add:

```xml
    <properties>
        <!-- HaChaosIT and its harness tests run only from ha-chaos-tests.yml, never in the nightly HA job.
             Override with -Dfailsafe.excludedGroups= to run them. -->
        <failsafe.excludedGroups>chaos</failsafe.excludedGroups>
    </properties>
```

- [ ] **Step 1.2: Write the failing test**

`ChaosConfigIT.java`:

```java
package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

@Tag("chaos")
class ChaosConfigIT {

  static Properties props(final String... keyValues) {
    final Properties properties = new Properties();
    for (int i = 0; i < keyValues.length; i += 2)
      properties.setProperty(keyValues[i], keyValues[i + 1]);
    return properties;
  }

  @Test
  void defaults() {
    final ChaosConfig config = ChaosConfig.fromProperties(props());
    assertThat(config.nodes()).isEqualTo(3);
    assertThat(config.writers()).isEqualTo(4);
    assertThat(config.duration()).isEqualTo(Duration.ofMinutes(20));
    assertThat(config.maxSteps()).isZero();
    assertThat(config.faultWeights()).containsOnlyKeys(ChaosConfig.ALL_FAULTS.toArray(new String[0]));
    assertThat(config.faultWeights().values()).containsOnly(1);
    assertThat(config.holdMin()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.holdMax()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.calmMin()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.calmMax()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.convergenceTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(config.electionTimeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.availabilityGrace()).isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  void explicitSeedAndNodes() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.seed", "42", "chaos.nodes", "5"));
    assertThat(config.seed()).isEqualTo(42L);
    assertThat(config.nodes()).isEqualTo(5);
  }

  @Test
  void missingSeedIsRandom() {
    assertThat(ChaosConfig.fromProperties(props()).seed()).isNotEqualTo(ChaosConfig.fromProperties(props()).seed());
  }

  @Test
  void weightedFaults() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.faults", " kill:3, pause "));
    assertThat(config.faultWeights()).containsExactly(entry("kill", 3), entry("pause", 1));
    assertThat(config.faultsSpec()).isEqualTo("kill:3,pause:1");
  }

  @Test
  void rejectsInvalidValues() {
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.nodes", "4")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.nodes");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.faults", "nuke")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nuke");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.faults", "kill:0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("weight");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.holdMin", "PT30S", "chaos.holdMax", "PT10S")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.hold");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.writers", "0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.writers");
  }

  @Test
  void replayCommandCarriesTheDecisions() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.seed", "42", "chaos.faults", "kill:3,pause"));
    assertThat(config.replayCommand())
        .contains("-Dit.test=HaChaosIT")
        .contains("-Dfailsafe.excludedGroups=")
        .contains("-Dchaos.seed=42")
        .contains("-Dchaos.faults=kill:3,pause:1")
        .contains("-Dchaos.holdMin=PT10S");
  }
}
```

- [ ] **Step 1.3: Run it to verify it fails**

Run: `RUN ChaosConfigIT`
Expected: compilation failure, `cannot find symbol ... ChaosConfig`.

- [ ] **Step 1.4: Implement `ChaosConfig`**

```java
package com.arcadedb.containers.ha.chaos;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Immutable configuration of a chaos run, read from {@code chaos.*} system properties. The seed drives every random
 * decision of the run, so {@link #replayCommand()} reproduces the same fault sequence.
 */
public record ChaosConfig(long seed, int nodes, Duration duration, int maxSteps, int writers,
                          Map<String, Integer> faultWeights, Duration holdMin, Duration holdMax, Duration calmMin,
                          Duration calmMax, Duration convergenceTimeout, Duration electionTimeout,
                          Duration availabilityGrace) {

  public static final List<String> ALL_FAULTS = List.of("kill", "stop", "rolling", "pause", "isolate", "split", "latency",
      "loss");

  public ChaosConfig {
    if (nodes != 3 && nodes != 5)
      throw new IllegalArgumentException("chaos.nodes must be 3 or 5, got " + nodes);
    if (writers < 1)
      throw new IllegalArgumentException("chaos.writers must be >= 1, got " + writers);
    if (maxSteps < 0)
      throw new IllegalArgumentException("chaos.maxSteps must be >= 0 (0 = unlimited), got " + maxSteps);
    if (duration.isNegative() || duration.isZero())
      throw new IllegalArgumentException("chaos.duration must be positive, got " + duration);
    requireRange("chaos.hold", holdMin, holdMax);
    requireRange("chaos.calm", calmMin, calmMax);
    if (faultWeights.isEmpty())
      throw new IllegalArgumentException("chaos.faults selects no fault");
    for (final String fault : faultWeights.keySet())
      if (!ALL_FAULTS.contains(fault))
        throw new IllegalArgumentException("Unknown fault '" + fault + "', valid faults: " + ALL_FAULTS);
    faultWeights = Collections.unmodifiableMap(new LinkedHashMap<>(faultWeights));
  }

  public static ChaosConfig fromProperties(final Properties properties) {
    final String seedValue = properties.getProperty("chaos.seed", "").trim();
    final long seed = seedValue.isEmpty() ? ThreadLocalRandom.current().nextLong() : Long.parseLong(seedValue);
    return new ChaosConfig(seed,
        integer(properties, "chaos.nodes", 3),
        duration(properties, "chaos.duration", "PT20M"),
        integer(properties, "chaos.maxSteps", 0),
        integer(properties, "chaos.writers", 4),
        parseFaults(properties.getProperty("chaos.faults", "")),
        duration(properties, "chaos.holdMin", "PT10S"),
        duration(properties, "chaos.holdMax", "PT60S"),
        duration(properties, "chaos.calmMin", "PT10S"),
        duration(properties, "chaos.calmMax", "PT30S"),
        duration(properties, "chaos.convergenceTimeout", "PT2M"),
        duration(properties, "chaos.electionTimeout", "PT60S"),
        duration(properties, "chaos.availabilityGrace", "PT20S"));
  }

  /**
   * Parses {@code kill:3,pause} into an ordered name-to-weight map; a blank spec enables every fault with weight 1.
   */
  public static Map<String, Integer> parseFaults(final String spec) {
    final Map<String, Integer> weights = new LinkedHashMap<>();
    if (spec == null || spec.isBlank()) {
      for (final String fault : ALL_FAULTS)
        weights.put(fault, 1);
      return weights;
    }
    for (final String token : spec.split(",")) {
      final String trimmed = token.trim();
      if (trimmed.isEmpty())
        continue;
      final int colon = trimmed.indexOf(':');
      final String name = colon < 0 ? trimmed : trimmed.substring(0, colon).trim();
      final int weight = colon < 0 ? 1 : Integer.parseInt(trimmed.substring(colon + 1).trim());
      if (weight < 1)
        throw new IllegalArgumentException("Fault weight must be >= 1: '" + trimmed + "'");
      weights.merge(name, weight, Integer::sum);
    }
    return weights;
  }

  public String faultsSpec() {
    final StringJoiner joiner = new StringJoiner(",");
    faultWeights.forEach((name, weight) -> joiner.add(name + ":" + weight));
    return joiner.toString();
  }

  public String replayCommand() {
    return "./mvnw verify -Pintegration -pl e2e-ha -Dit.test=HaChaosIT -Dfailsafe.excludedGroups="
        + " -Dchaos.seed=" + seed
        + " -Dchaos.nodes=" + nodes
        + " -Dchaos.duration=" + duration
        + " -Dchaos.maxSteps=" + maxSteps
        + " -Dchaos.writers=" + writers
        + " -Dchaos.faults=" + faultsSpec()
        + " -Dchaos.holdMin=" + holdMin
        + " -Dchaos.holdMax=" + holdMax
        + " -Dchaos.calmMin=" + calmMin
        + " -Dchaos.calmMax=" + calmMax;
  }

  private static void requireRange(final String name, final Duration min, final Duration max) {
    if (min.isNegative() || max.compareTo(min) < 0)
      throw new IllegalArgumentException(name + "Min/" + name + "Max must satisfy 0 <= min <= max, got " + min + " / " + max);
  }

  private static int integer(final Properties properties, final String name, final int defaultValue) {
    final String value = properties.getProperty(name, "").trim();
    return value.isEmpty() ? defaultValue : Integer.parseInt(value);
  }

  private static Duration duration(final Properties properties, final String name, final String defaultValue) {
    final String value = properties.getProperty(name, "").trim();
    return Duration.parse(value.isEmpty() ? defaultValue : value);
  }
}
```

- [ ] **Step 1.5: Run it to verify it passes**

Run: `RUN ChaosConfigIT`
Expected: `Tests run: 6, Failures: 0, Errors: 0`.

- [ ] **Step 1.6: Verify the nightly job would skip it**

Run: `./mvnw verify -Pintegration -pl e2e-ha -Dit.test=ChaosConfigIT -Dit.failIfNoSpecifiedTests=false`
(no `-Dfailsafe.excludedGroups=` override)
Expected: `ChaosConfigIT` does not appear in `Results:` / `Tests run: 0`. This proves the pom property excludes the tag.

- [ ] **Step 1.7: Stage**

```bash
git add e2e-ha/pom.xml e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfig.java \
        e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/ChaosConfigIT.java
```

---

### Task 2: `Ledger`

**Files:**
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/Ledger.java`
- Test: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/LedgerIT.java`

**Interfaces:**
- Produces: byte constants `IN_FLIGHT=0, ACKED=1, FAILED=2, UNKNOWN=3, ACKED_LATE=4, LOST_UNKNOWN=5`;
  `Ledger(int writers)`; `int writers()`; `static long key(int writer, long seq)`; `static int writerOf(long key)`;
  `static long seqOf(long key)`; `static String format(long key)` (`"w3-17"`); `static String name(byte outcome)`;
  `long reserve(int writer, boolean pair)` (returns the key, outcome `IN_FLIGHT`); `void record(long key, byte outcome)`;
  `byte outcome(long key)`; `boolean isPair(long key)`; `int size(int writer)`; `long count(byte outcome)`;
  `long randomAckedKey(int writer, Random random)` (`-1` when none found).

- [ ] **Step 2.1: Write the failing test**

```java
package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("chaos")
class LedgerIT {

  @Test
  void keyRoundTrip() {
    final long key = Ledger.key(7, (1L << 39) + 5);
    assertThat(Ledger.writerOf(key)).isEqualTo(7);
    assertThat(Ledger.seqOf(key)).isEqualTo((1L << 39) + 5);
    assertThat(Ledger.format(Ledger.key(3, 17))).isEqualTo("w3-17");
  }

  @Test
  void reserveIsPerWriterAndStartsInFlight() {
    final Ledger ledger = new Ledger(2);
    assertThat(ledger.reserve(0, false)).isEqualTo(Ledger.key(0, 0));
    assertThat(ledger.reserve(0, true)).isEqualTo(Ledger.key(0, 1));
    assertThat(ledger.reserve(1, false)).isEqualTo(Ledger.key(1, 0));
    assertThat(ledger.size(0)).isEqualTo(2);
    assertThat(ledger.outcome(Ledger.key(0, 1))).isEqualTo(Ledger.IN_FLIGHT);
    assertThat(ledger.isPair(Ledger.key(0, 1))).isTrue();
    assertThat(ledger.isPair(Ledger.key(0, 0))).isFalse();
  }

  @Test
  void recordKeepsThePairFlag() {
    final Ledger ledger = new Ledger(1);
    final long key = ledger.reserve(0, true);
    ledger.record(key, Ledger.UNKNOWN);
    ledger.record(key, Ledger.ACKED_LATE);
    assertThat(ledger.outcome(key)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(ledger.isPair(key)).isTrue();
    assertThat(ledger.count(Ledger.ACKED_LATE)).isEqualTo(1);
    assertThat(ledger.count(Ledger.UNKNOWN)).isZero();
  }

  @Test
  void unknownKeysAreRejected() {
    final Ledger ledger = new Ledger(1);
    assertThatThrownBy(() -> ledger.record(Ledger.key(0, 0), Ledger.ACKED)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.outcome(Ledger.key(4, 0))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void randomAckedKeyOnlyReturnsAcknowledgedKeysOfThatWriter() {
    final Ledger ledger = new Ledger(2);
    assertThat(ledger.randomAckedKey(0, new Random(1))).isEqualTo(-1);
    final long acked = ledger.reserve(0, false);
    ledger.record(acked, Ledger.ACKED);
    final long failed = ledger.reserve(0, false);
    ledger.record(failed, Ledger.FAILED);
    final long other = ledger.reserve(1, false);
    ledger.record(other, Ledger.ACKED);
    final Random random = new Random(3);
    for (int i = 0; i < 100; i++)
      assertThat(ledger.randomAckedKey(0, random)).isIn(acked, -1L);
  }

  @Test
  void growsBeyondTheInitialCapacityUnderConcurrentWriters() throws InterruptedException {
    final Ledger ledger = new Ledger(4);
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final int writer = w;
      threads.add(Thread.ofPlatform().start(() -> {
        for (int i = 0; i < 10_000; i++)
          ledger.record(ledger.reserve(writer, i % 5 == 0), Ledger.ACKED);
      }));
    }
    for (final Thread thread : threads)
      thread.join();
    assertThat(ledger.count(Ledger.ACKED)).isEqualTo(40_000);
    assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
    assertThat(ledger.size(3)).isEqualTo(10_000);
  }
}
```

- [ ] **Step 2.2: Run it to verify it fails**

Run: `RUN LedgerIT` - Expected: compilation failure, `cannot find symbol ... Ledger`.

- [ ] **Step 2.3: Implement `Ledger`**

```java
package com.arcadedb.containers.ha.chaos;

import java.util.Arrays;
import java.util.Random;

/**
 * Client-side record of every write the workload attempted and what the cluster answered. Keys pack the writer index
 * and its sequence number into one {@code long}; outcomes are one byte per operation in per-writer arrays, so a soak
 * run with millions of operations stays light on the garbage collector.
 */
public final class Ledger {
  public static final byte IN_FLIGHT    = 0;
  public static final byte ACKED        = 1;
  public static final byte FAILED       = 2;
  public static final byte UNKNOWN      = 3;
  public static final byte ACKED_LATE   = 4;
  public static final byte LOST_UNKNOWN = 5;

  static final         int  SEQ_BITS     = 40;
  static final         long SEQ_MASK     = (1L << SEQ_BITS) - 1;
  private static final byte PAIR_FLAG    = 0x10;
  private static final byte OUTCOME_MASK = 0x0F;
  private static final int  PICK_TRIES   = 8;

  private final Lane[] lanes;

  public Ledger(final int writers) {
    lanes = new Lane[writers];
    for (int i = 0; i < writers; i++)
      lanes[i] = new Lane();
  }

  public int writers() {
    return lanes.length;
  }

  public static long key(final int writer, final long seq) {
    return ((long) writer << SEQ_BITS) | seq;
  }

  public static int writerOf(final long key) {
    return (int) (key >>> SEQ_BITS);
  }

  public static long seqOf(final long key) {
    return key & SEQ_MASK;
  }

  public static String format(final long key) {
    return "w" + writerOf(key) + "-" + seqOf(key);
  }

  public static String name(final byte outcome) {
    return switch (outcome) {
      case IN_FLIGHT -> "IN_FLIGHT";
      case ACKED -> "ACKED";
      case FAILED -> "FAILED";
      case UNKNOWN -> "UNKNOWN";
      case ACKED_LATE -> "ACKED_LATE";
      case LOST_UNKNOWN -> "LOST_UNKNOWN";
      default -> "?" + outcome;
    };
  }

  /**
   * Reserves the writer's next sequence number. The operation stays {@link #IN_FLIGHT} until {@link #record}.
   */
  public long reserve(final int writer, final boolean pair) {
    return key(writer, lanes[writer].reserve(pair));
  }

  public void record(final long key, final byte outcome) {
    lane(key).set((int) seqOf(key), outcome);
  }

  public byte outcome(final long key) {
    return (byte) (lane(key).get((int) seqOf(key)) & OUTCOME_MASK);
  }

  public boolean isPair(final long key) {
    return (lane(key).get((int) seqOf(key)) & PAIR_FLAG) != 0;
  }

  public int size(final int writer) {
    return lanes[writer].size();
  }

  public long count(final byte outcome) {
    long total = 0;
    for (final Lane lane : lanes)
      total += lane.count(outcome);
    return total;
  }

  /**
   * Picks a random acknowledged key of the writer, or -1 when a few random probes find none.
   */
  public long randomAckedKey(final int writer, final Random random) {
    final Lane lane = lanes[writer];
    final int size = lane.size();
    if (size == 0)
      return -1;
    for (int i = 0; i < PICK_TRIES; i++) {
      final int seq = random.nextInt(size);
      final int outcome = lane.get(seq) & OUTCOME_MASK;
      if (outcome == ACKED || outcome == ACKED_LATE)
        return key(writer, seq);
    }
    return -1;
  }

  private Lane lane(final long key) {
    final int writer = writerOf(key);
    if (key < 0 || writer >= lanes.length)
      throw new IllegalArgumentException("Unknown writer in key " + format(key));
    return lanes[writer];
  }

  private static final class Lane {
    private byte[] ops = new byte[1024];
    private int    size;

    synchronized int reserve(final boolean pair) {
      if (size == ops.length)
        ops = Arrays.copyOf(ops, size * 2);
      ops[size] = pair ? PAIR_FLAG : 0;
      return size++;
    }

    synchronized void set(final int seq, final byte outcome) {
      check(seq);
      ops[seq] = (byte) ((ops[seq] & PAIR_FLAG) | outcome);
    }

    synchronized byte get(final int seq) {
      check(seq);
      return ops[seq];
    }

    synchronized int size() {
      return size;
    }

    synchronized long count(final byte outcome) {
      long count = 0;
      for (int i = 0; i < size; i++)
        if ((ops[i] & OUTCOME_MASK) == outcome)
          ++count;
      return count;
    }

    private void check(final int seq) {
      if (seq < 0 || seq >= size)
        throw new IllegalArgumentException("Sequence " + seq + " was never reserved");
    }
  }
}
```

- [ ] **Step 2.4: Run it to verify it passes**

Run: `RUN LedgerIT` - Expected: `Tests run: 6, Failures: 0, Errors: 0`.

- [ ] **Step 2.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/Ledger.java \
        e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/LedgerIT.java
```

---

### Task 3: Invariants (`ResultKind`, `Violation`, `NodeSnapshot`, `InvariantChecker`)

**Files:**
- Create: `.../chaos/ResultKind.java`, `.../chaos/Violation.java`, `.../chaos/NodeSnapshot.java`, `.../chaos/InvariantChecker.java`
- Test: `.../chaos/InvariantCheckerIT.java`

(`.../chaos/` = `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/` here and below.)

**Interfaces:**
- Consumes: `Ledger` (Task 2).
- Produces: `enum ResultKind { PASS, SAFETY, AVAILABILITY, HARNESS }`;
  `record Violation(ResultKind kind, String invariant, String message, long[] keys)` with `String describe()`;
  `NodeSnapshot(Ledger)` with `void add(long key, int edges)`, `boolean present(int writer, int seq)`,
  `boolean hasEdge(int writer, int seq)`, `long rows()`, `long[] phantoms()`, `long[] duplicates()`, `long[] multiEdge()`,
  `long[] diff(NodeSnapshot other, int limit)`;
  `InvariantChecker(Ledger)` with `static final int MAX_KEYS = 100`, `List<Violation> check(NodeSnapshot)`,
  `long lateCommits()`. Invariant ids: `I1`, `I2`, `I3`, `I5`, and `QUIESCE` (harness). I4 is the resolution of
  `UNKNOWN` performed by `check`.

- [ ] **Step 3.1: Write the failing test**

```java
package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class InvariantCheckerIT {
  private final Ledger           ledger  = new Ledger(2);
  private final InvariantChecker checker = new InvariantChecker(ledger);

  private long op(final int writer, final boolean pair, final byte outcome) {
    final long key = ledger.reserve(writer, pair);
    ledger.record(key, outcome);
    return key;
  }

  /** A snapshot holding the given keys, each with the edge count its ledger entry expects. */
  private NodeSnapshot snapshot(final long... keys) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    for (final long key : keys)
      snapshot.add(key, ledger.isPair(key) ? 1 : 0);
    return snapshot;
  }

  @Test
  void consistentClusterPassesAndResolvesUnknowns() {
    final long acked = op(0, false, Ledger.ACKED);
    final long pair = op(1, true, Ledger.ACKED);
    op(0, false, Ledger.FAILED);
    final long unknownPresent = op(0, false, Ledger.UNKNOWN);
    final long unknownAbsent = op(1, false, Ledger.UNKNOWN);

    assertThat(checker.check(snapshot(acked, pair, unknownPresent))).isEmpty();
    assertThat(ledger.outcome(unknownPresent)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(ledger.outcome(unknownAbsent)).isEqualTo(Ledger.LOST_UNKNOWN);
  }

  @Test
  void lostAcknowledgedWriteIsI1() {
    final long acked = op(0, false, Ledger.ACKED);
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).hasSize(1);
    assertThat(violations.getFirst().invariant()).isEqualTo("I1");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(violations.getFirst().keys()).containsExactly(acked);
  }

  @Test
  void rowsNoOperationProducedAreI2() {
    op(0, false, Ledger.ACKED);
    final NodeSnapshot snapshot = snapshot(Ledger.key(0, 0));
    snapshot.add(Ledger.key(0, 999), 0);
    snapshot.add(Ledger.key(7, 0), 0);
    final List<Violation> violations = checker.check(snapshot);
    assertThat(violations).extracting(Violation::invariant).containsExactly("I2");
    assertThat(violations.getFirst().keys()).containsExactlyInAnyOrder(Ledger.key(0, 999), Ledger.key(7, 0));
  }

  @Test
  void duplicateRowIsI2() {
    final long acked = op(0, false, Ledger.ACKED);
    final List<Violation> violations = checker.check(snapshot(acked, acked));
    assertThat(violations).extracting(Violation::invariant).containsExactly("I2");
    assertThat(violations.getFirst().keys()).containsExactly(acked);
  }

  @Test
  void resurrectedFailureIsI3() {
    final long failed = op(0, false, Ledger.FAILED);
    final List<Violation> violations = checker.check(snapshot(failed));
    assertThat(violations).extracting(Violation::invariant).containsExactly("I3");
  }

  @Test
  void aKeyOnceSeenMustNeverDisappear() {
    final long unknown = op(0, false, Ledger.UNKNOWN);
    assertThat(checker.check(snapshot(unknown))).isEmpty();
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).extracting(Violation::invariant).containsExactly("I1");
    assertThat(violations.getFirst().keys()).containsExactly(unknown);
  }

  @Test
  void lateCommitIsNotAViolation() {
    final long unknown = op(0, false, Ledger.UNKNOWN);
    assertThat(checker.check(snapshot())).isEmpty();
    assertThat(ledger.outcome(unknown)).isEqualTo(Ledger.LOST_UNKNOWN);
    assertThat(checker.check(snapshot(unknown))).isEmpty();
    assertThat(ledger.outcome(unknown)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(checker.lateCommits()).isEqualTo(1);
  }

  @Test
  void partialTransactionsAreI5() {
    final long pairWithoutEdge = op(0, true, Ledger.ACKED);
    final long singleWithEdge = op(0, false, Ledger.ACKED);
    final long pairWithTwoEdges = op(1, true, Ledger.ACKED);
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    snapshot.add(pairWithoutEdge, 0);
    snapshot.add(singleWithEdge, 1);
    snapshot.add(pairWithTwoEdges, 2);
    final List<Violation> violations = checker.check(snapshot);
    assertThat(violations).extracting(Violation::invariant).containsExactly("I5");
    assertThat(violations.getFirst().keys()).containsExactlyInAnyOrder(pairWithTwoEdges, pairWithoutEdge, singleWithEdge);
  }

  @Test
  void operationStillInFlightIsAHarnessError() {
    ledger.reserve(0, false);
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).extracting(Violation::invariant).containsExactly("QUIESCE");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.HARNESS);
  }

  @Test
  void reportedKeysAreCappedButTheCountIsNot() {
    for (int i = 0; i < 150; i++)
      op(0, false, Ledger.ACKED);
    final Violation violation = checker.check(snapshot()).getFirst();
    assertThat(violation.keys()).hasSize(InvariantChecker.MAX_KEYS);
    assertThat(violation.message()).startsWith("150 ");
  }

  @Test
  void diffFindsPresenceAndEdgeDifferences() {
    final long a = op(0, false, Ledger.ACKED);
    final long b = op(1, false, Ledger.ACKED);
    assertThat(snapshot(a, b).diff(snapshot(a), 10)).containsExactly(b);
    assertThat(snapshot(a, b).diff(snapshot(a, b), 10)).isEmpty();
    final NodeSnapshot withEdge = new NodeSnapshot(ledger);
    withEdge.add(a, 1);
    withEdge.add(b, 0);
    assertThat(snapshot(a, b).diff(withEdge, 10)).containsExactly(a);
  }
}
```

- [ ] **Step 3.2: Run it to verify it fails**

Run: `RUN InvariantCheckerIT` - Expected: compilation failure (`NodeSnapshot`, `InvariantChecker`, `Violation` missing).

- [ ] **Step 3.3: Implement the four classes**

`ResultKind.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Outcome category of a chaos run. SAFETY means data was lost or diverged; AVAILABILITY means the cluster did not
 * recover in time; HARNESS means the test infrastructure itself failed and says nothing about the cluster.
 */
public enum ResultKind {
  PASS, SAFETY, AVAILABILITY, HARNESS
}
```

`Violation.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * One violated invariant, with up to {@link InvariantChecker#MAX_KEYS} offending ledger keys.
 */
public record Violation(ResultKind kind, String invariant, String message, long[] keys) {
  public String describe() {
    return invariant + " (" + kind + "): " + message;
  }
}
```

`NodeSnapshot.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.Arrays;
import java.util.BitSet;

/**
 * The {@code ChaosOp} rows one node holds, as per-writer bit sets indexed by sequence number. Rows that match no ledger
 * entry, appear twice, or carry more than one {@code NEXT} edge are kept aside for the checker.
 */
public final class NodeSnapshot {
  private final Ledger   ledger;
  private final BitSet[] present;
  private final BitSet[] withEdge;
  private final Keys     phantoms   = new Keys();
  private final Keys     duplicates = new Keys();
  private final Keys     multiEdge  = new Keys();
  private       long     rows;

  public NodeSnapshot(final Ledger ledger) {
    this.ledger = ledger;
    present = new BitSet[ledger.writers()];
    withEdge = new BitSet[ledger.writers()];
    for (int i = 0; i < present.length; i++) {
      present[i] = new BitSet();
      withEdge[i] = new BitSet();
    }
  }

  public void add(final long key, final int edges) {
    ++rows;
    final int writer = Ledger.writerOf(key);
    final long seq = Ledger.seqOf(key);
    if (key < 0 || writer >= present.length || seq >= ledger.size(writer)) {
      phantoms.add(key);
      return;
    }
    final int index = (int) seq;
    if (present[writer].get(index)) {
      duplicates.add(key);
      return;
    }
    present[writer].set(index);
    if (edges > 0)
      withEdge[writer].set(index);
    if (edges > 1)
      multiEdge.add(key);
  }

  public boolean present(final int writer, final int seq) {
    return present[writer].get(seq);
  }

  public boolean hasEdge(final int writer, final int seq) {
    return withEdge[writer].get(seq);
  }

  public long rows() {
    return rows;
  }

  public long[] phantoms() {
    return phantoms.toArray();
  }

  public long[] duplicates() {
    return duplicates.toArray();
  }

  public long[] multiEdge() {
    return multiEdge.toArray();
  }

  /**
   * Keys whose presence or edge state differs between the two snapshots, up to {@code limit}.
   */
  public long[] diff(final NodeSnapshot other, final int limit) {
    final Keys out = new Keys();
    for (int w = 0; w < present.length && out.size() < limit; w++) {
      final BitSet differing = (BitSet) present[w].clone();
      differing.xor(other.present[w]);
      final BitSet edges = (BitSet) withEdge[w].clone();
      edges.xor(other.withEdge[w]);
      differing.or(edges);
      for (int s = differing.nextSetBit(0); s >= 0 && out.size() < limit; s = differing.nextSetBit(s + 1))
        out.add(Ledger.key(w, s));
    }
    final long[] mine = phantoms.sorted();
    final long[] theirs = other.phantoms.sorted();
    for (final long key : mine)
      if (out.size() < limit && Arrays.binarySearch(theirs, key) < 0)
        out.add(key);
    for (final long key : theirs)
      if (out.size() < limit && Arrays.binarySearch(mine, key) < 0)
        out.add(key);
    return out.toArray();
  }

  private static final class Keys {
    private long[] values = new long[16];
    private int    size;

    void add(final long key) {
      if (size == values.length)
        values = Arrays.copyOf(values, size * 2);
      values[size++] = key;
    }

    int size() {
      return size;
    }

    long[] toArray() {
      return Arrays.copyOf(values, size);
    }

    long[] sorted() {
      final long[] copy = toArray();
      Arrays.sort(copy);
      return copy;
    }
  }
}
```

`InvariantChecker.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Checks one converged node snapshot against the ledger:
 * <ul>
 *   <li>I1 every acknowledged (or once seen) write is present;</li>
 *   <li>I2 every row was produced by a recorded operation, exactly once;</li>
 *   <li>I3 no write rejected before append is present;</li>
 *   <li>I4 each UNKNOWN write is resolved to ACKED_LATE or LOST_UNKNOWN. A LOST_UNKNOWN write that appears later is a
 *       legitimate late Raft commit, re-resolved to ACKED_LATE and counted, not a violation;</li>
 *   <li>I5 a pair transaction's edge exists if and only if its vertex does.</li>
 * </ul>
 */
public final class InvariantChecker {
  public static final int MAX_KEYS = 100;

  private final Ledger ledger;
  private       long   lateCommits;

  public InvariantChecker(final Ledger ledger) {
    this.ledger = ledger;
  }

  public long lateCommits() {
    return lateCommits;
  }

  public List<Violation> check(final NodeSnapshot snapshot) {
    final Collector lost = new Collector();
    final Collector phantom = new Collector();
    final Collector resurrected = new Collector();
    final Collector atomicity = new Collector();
    final Collector inFlight = new Collector();

    for (final long key : snapshot.phantoms())
      phantom.add(key);
    for (final long key : snapshot.duplicates())
      phantom.add(key);
    for (final long key : snapshot.multiEdge())
      atomicity.add(key);

    for (int w = 0; w < ledger.writers(); w++) {
      final int size = ledger.size(w);
      for (int s = 0; s < size; s++) {
        final long key = Ledger.key(w, s);
        final boolean present = snapshot.present(w, s);
        switch (ledger.outcome(key)) {
          case Ledger.ACKED, Ledger.ACKED_LATE -> {
            if (!present)
              lost.add(key);
          }
          case Ledger.FAILED -> {
            if (present)
              resurrected.add(key);
          }
          case Ledger.UNKNOWN -> ledger.record(key, present ? Ledger.ACKED_LATE : Ledger.LOST_UNKNOWN);
          case Ledger.LOST_UNKNOWN -> {
            if (present) {
              ledger.record(key, Ledger.ACKED_LATE);
              ++lateCommits;
            }
          }
          default -> inFlight.add(key);
        }
        if (present && ledger.isPair(key) != snapshot.hasEdge(w, s))
          atomicity.add(key);
      }
    }

    final List<Violation> violations = new ArrayList<>();
    lost.report(violations, ResultKind.SAFETY, "I1", "acknowledged writes are missing");
    phantom.report(violations, ResultKind.SAFETY, "I2", "rows were not produced by any recorded operation, or appear twice");
    resurrected.report(violations, ResultKind.SAFETY, "I3", "writes rejected before append are present");
    atomicity.report(violations, ResultKind.SAFETY, "I5", "transactions were applied partially (vertex and edge disagree)");
    inFlight.report(violations, ResultKind.HARNESS, "QUIESCE", "operations were still in flight at the checkpoint");
    return violations;
  }

  private static final class Collector {
    private final long[] keys = new long[MAX_KEYS];
    private       int    kept;
    private       long   count;

    void add(final long key) {
      ++count;
      if (kept < MAX_KEYS)
        keys[kept++] = key;
    }

    void report(final List<Violation> out, final ResultKind kind, final String invariant, final String what) {
      if (count > 0)
        out.add(new Violation(kind, invariant, count + " " + what, Arrays.copyOf(keys, kept)));
    }
  }
}
```

- [ ] **Step 3.4: Run it to verify it passes**

Run: `RUN InvariantCheckerIT` - Expected: `Tests run: 11, Failures: 0, Errors: 0`.

- [ ] **Step 3.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{ResultKind,Violation,NodeSnapshot,InvariantChecker,InvariantCheckerIT}.java
```

---

### Task 4: `ChaosSchema` (SQL validated against the embedded engine)

**Files:**
- Create: `.../chaos/ChaosSchema.java`
- Test: `.../chaos/ChaosSchemaIT.java`

**Interfaces:**
- Consumes: `Ledger.key/writerOf/seqOf` (Task 2).
- Produces: `DATABASE = "chaos"`, `PAGE_SIZE = 20_000`, `List<String> DDL`, `INSERT_SINGLE` (sql), `INSERT_PAIR`
  (sqlscript), `COUNT_OPS`, `COUNT_EDGES` (both return one row with long column `c`), `static String page(int size)`
  (param `last`, returns columns `id` and `e`), `static Map<String,Object> singleParams(long key)`,
  `static Map<String,Object> pairParams(long key, long target)`.

- [ ] **Step 4.1: Write the failing test**

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the exact statements the workload and the checkpoint send over HTTP against an embedded database, so a syntax
 * or semantics mistake shows up here instead of as a mysterious failure 20 minutes into a container run.
 */
@Tag("chaos")
class ChaosSchemaIT {
  private static final String PATH = "./target/chaos-schema-it";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    factory = new DatabaseFactory(PATH);
    database = factory.create();
    for (final String ddl : ChaosSchema.DDL)
      database.command("sql", ddl);
  }

  @AfterEach
  void tearDown() {
    database.drop();
    factory.close();
  }

  private void single(final long key) {
    database.transaction(() -> database.command("sql", ChaosSchema.INSERT_SINGLE, ChaosSchema.singleParams(key)));
  }

  private void pair(final long key, final long target) {
    database.command("sqlscript", ChaosSchema.INSERT_PAIR, ChaosSchema.pairParams(key, target));
  }

  private List<long[]> page(final long last, final int size) {
    final List<long[]> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", ChaosSchema.page(size), Map.of("last", last))) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(new long[] { ((Number) row.getProperty("id")).longValue(), ((Number) row.getProperty("e")).longValue() });
      }
    }
    return rows;
  }

  private long count(final String sql) {
    try (final ResultSet resultSet = database.query("sql", sql)) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }

  @Test
  void ddlIsIdempotent() {
    for (final String ddl : ChaosSchema.DDL)
      database.command("sql", ddl);
    assertThat(database.getSchema().existsType("ChaosOp")).isTrue();
    assertThat(database.getSchema().existsType("NEXT")).isTrue();
  }

  @Test
  void singleInsertHasNoEdge() {
    final long key = Ledger.key(0, 0);
    single(key);
    assertThat(page(-1, ChaosSchema.PAGE_SIZE)).containsExactly(new long[] { key, 0 });
  }

  @Test
  void pairInsertCreatesOneOutgoingEdge() {
    final long target = Ledger.key(0, 0);
    final long pairKey = Ledger.key(1, 0);
    single(target);
    pair(pairKey, target);
    assertThat(page(-1, ChaosSchema.PAGE_SIZE)).containsExactly(new long[] { target, 0 }, new long[] { pairKey, 1 });
    assertThat(count(ChaosSchema.COUNT_OPS)).isEqualTo(2);
    assertThat(count(ChaosSchema.COUNT_EDGES)).isEqualTo(1);
  }

  @Test
  void pageResumesAfterTheLastKeyInKeyOrder() {
    single(Ledger.key(1, 0));
    single(Ledger.key(0, 2));
    single(Ledger.key(0, 0));
    single(Ledger.key(0, 1));
    assertThat(page(Ledger.key(0, 0), ChaosSchema.PAGE_SIZE)).extracting(row -> row[0])
        .containsExactly(Ledger.key(0, 1), Ledger.key(0, 2), Ledger.key(1, 0));
  }

  @Test
  void pageSizeLimitsRows() {
    for (int i = 0; i < 5; i++)
      single(Ledger.key(0, i));
    assertThat(page(-1, 2)).hasSize(2);
  }

  @Test
  void duplicateKeyIsRejected() {
    final long key = Ledger.key(0, 0);
    single(key);
    assertThatThrownBy(() -> single(key)).hasStackTraceContaining("DuplicatedKeyException");
  }
}
```

- [ ] **Step 4.2: Run it to verify it fails**

Run: `RUN ChaosSchemaIT` - Expected: compilation failure, `cannot find symbol ... ChaosSchema`.

- [ ] **Step 4.3: Implement `ChaosSchema`**

```java
package com.arcadedb.containers.ha.chaos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema and statements of the chaos workload. {@code id} is the ledger key; {@code w} and {@code s} repeat the writer
 * and sequence for humans reading a dump. A pair operation adds, in the same transaction, a {@code NEXT} edge from the
 * new vertex to an earlier acknowledged one, so a partially applied transaction shows up as a vertex without its edge.
 * If the target were missing on the leader the edge would not be created; the checker then reports I1 for the target
 * and I5 for the new vertex, both real bugs because the target was acknowledged.
 */
public final class ChaosSchema {
  public static final String DATABASE  = "chaos";
  public static final int    PAGE_SIZE = 20_000;

  public static final List<String> DDL = List.of(
      "CREATE VERTEX TYPE ChaosOp IF NOT EXISTS",
      "CREATE PROPERTY ChaosOp.id IF NOT EXISTS LONG",
      "CREATE INDEX IF NOT EXISTS ON ChaosOp (id) UNIQUE",
      "CREATE EDGE TYPE NEXT IF NOT EXISTS");

  public static final String INSERT_SINGLE = "INSERT INTO ChaosOp SET id = :id, w = :w, s = :s, pair = false";

  public static final String INSERT_PAIR = """
      BEGIN;
      LET a = CREATE VERTEX ChaosOp SET id = :id, w = :w, s = :s, pair = true;
      LET b = SELECT FROM ChaosOp WHERE id = :target;
      CREATE EDGE NEXT FROM $a TO $b;
      COMMIT;""";

  public static final String COUNT_OPS   = "SELECT count(*) AS c FROM ChaosOp";
  public static final String COUNT_EDGES = "SELECT count(*) AS c FROM NEXT";

  private ChaosSchema() {
  }

  public static String page(final int size) {
    return "SELECT id, out('NEXT').size() AS e FROM ChaosOp WHERE id > :last ORDER BY id LIMIT " + size;
  }

  public static Map<String, Object> singleParams(final long key) {
    final Map<String, Object> params = new HashMap<>();
    params.put("id", key);
    params.put("w", Ledger.writerOf(key));
    params.put("s", Ledger.seqOf(key));
    return params;
  }

  public static Map<String, Object> pairParams(final long key, final long target) {
    final Map<String, Object> params = singleParams(key);
    params.put("target", target);
    return params;
  }
}
```

- [ ] **Step 4.4: Run it to verify it passes**

Run: `RUN ChaosSchemaIT` - Expected: `Tests run: 6, Failures: 0, Errors: 0`.
If a statement is rejected by the parser, fix the statement in `ChaosSchema` (not the test): the test encodes the
behavior the harness relies on.

- [ ] **Step 4.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{ChaosSchema,ChaosSchemaIT}.java
```

---

### Task 5: Faults (`ClusterState`, `NodeControl`, `ChaosFailure`, `Fault` and implementations, `FaultPicker`)

**Files:**
- Create: `.../chaos/ClusterState.java`, `NodeControl.java`, `ChaosFailure.java`, `Fault.java`, `Targets.java`,
  `NodeFault.java`, `SplitFault.java`, `RollingRestartFault.java`, `ToxicFault.java`, `FaultPicker.java`
- Create (test support): `.../chaos/FakeNodeControl.java`
- Test: `.../chaos/FaultsIT.java`

**Interfaces:**
- Consumes: `ResultKind` (Task 3), `ChaosConfig.parseFaults` (Task 1).
- Produces:
  - `ClusterState(int nodes)`; `enum NodeState { UP, DOWN, PAUSED, ISOLATED, DEGRADED }`; `int size()`; `int minority()`;
    `NodeState state(int)`; `void set(int, NodeState)`; `int impairedCount()`; `boolean canImpair(int additional)`.
  - `interface NodeControl`: `void kill(int)`, `void stopGracefully(int)`, `void start(int)`, `void pause(int)`,
    `void unpause(int)`, `void disconnect(int)`, `void reconnect(int)`, `void addLatency(int node, int latencyMs, int jitterMs)`,
    `void addLoss(int node, float toxicity)`, `void clearToxics(int)` (all `throws Exception`); `int findLeader()`
    (`-1` when unknown); `boolean awaitLeader(Duration timeout)`.
  - `ChaosFailure(ResultKind kind, String message) extends RuntimeException`, `ResultKind kind()`.
  - `interface Fault`: `String name()`, `boolean canApply(ClusterState)`,
    `String inject(ClusterState, NodeControl, Random) throws Exception` (returns a target description starting with the
    role, e.g. `"LEADER [2]"`), `void heal(ClusterState, NodeControl) throws Exception`, `boolean expectsWritesAvailable()`.
  - `FaultPicker(Map<String,Integer> weights, Duration electionTimeout)`; `Fault pick(ClusterState, Random)` (`null` when
    nothing applies; always consumes exactly one `nextInt`).

- [ ] **Step 5.1: Write the fake and the failing test**

`FakeNodeControl.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Records every cluster operation as {@code "op:node"} so tests can assert on the sequence of actions.
 */
final class FakeNodeControl implements NodeControl {
  final List<String>     calls            = new ArrayList<>();
  int                    leader           = 0;
  boolean                leaderAvailable  = true;
  RuntimeException       failure;

  private void log(final String call) {
    if (failure != null)
      throw failure;
    calls.add(call);
  }

  @Override
  public void kill(final int node) {
    log("kill:" + node);
  }

  @Override
  public void stopGracefully(final int node) {
    log("stop:" + node);
  }

  @Override
  public void start(final int node) {
    log("start:" + node);
  }

  @Override
  public void pause(final int node) {
    log("pause:" + node);
  }

  @Override
  public void unpause(final int node) {
    log("unpause:" + node);
  }

  @Override
  public void disconnect(final int node) {
    log("disconnect:" + node);
  }

  @Override
  public void reconnect(final int node) {
    log("reconnect:" + node);
  }

  @Override
  public void addLatency(final int node, final int latencyMs, final int jitterMs) {
    log("latency:" + node + ":" + latencyMs + ":" + jitterMs);
  }

  @Override
  public void addLoss(final int node, final float toxicity) {
    log("loss:" + node + ":" + toxicity);
  }

  @Override
  public void clearToxics(final int node) {
    log("clearToxics:" + node);
  }

  @Override
  public int findLeader() {
    return leaderAvailable ? leader : -1;
  }

  @Override
  public boolean awaitLeader(final Duration timeout) {
    calls.add("awaitLeader");
    return leaderAvailable;
  }
}
```

`FaultsIT.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("chaos")
class FaultsIT {

  private static int firstIn(final ClusterState state, final NodeState wanted) {
    for (int i = 0; i < state.size(); i++)
      if (state.state(i) == wanted)
        return i;
    return -1;
  }

  @Test
  void minorityRule() {
    final ClusterState three = new ClusterState(3);
    assertThat(three.minority()).isEqualTo(1);
    assertThat(three.canImpair(1)).isTrue();
    assertThat(three.canImpair(2)).isFalse();
    three.set(0, NodeState.DOWN);
    assertThat(three.canImpair(1)).isFalse();

    final ClusterState five = new ClusterState(5);
    assertThat(five.minority()).isEqualTo(2);
    assertThat(five.canImpair(2)).isTrue();
    assertThat(five.canImpair(3)).isFalse();
  }

  @Test
  void killTakesOneNodeDownOnThreeNodesAndHealRestartsIt() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      final NodeFault fault = new NodeFault(NodeFault.Kind.KILL);
      fault.inject(state, control, new Random(seed));
      assertThat(state.impairedCount()).isEqualTo(1);
      final int down = firstIn(state, NodeState.DOWN);
      assertThat(control.calls).contains("kill:" + down);
      fault.heal(state, control);
      assertThat(state.impairedCount()).isZero();
      assertThat(control.calls).contains("start:" + down);
    }
  }

  @Test
  void fiveNodesNeverLoseTheMajority() throws Exception {
    boolean sawTwo = false;
    for (long seed = 0; seed < 200; seed++)
      for (final NodeFault.Kind kind : NodeFault.Kind.values()) {
        final ClusterState state = new ClusterState(5);
        final NodeFault fault = new NodeFault(kind);
        fault.inject(state, new FakeNodeControl(), new Random(seed));
        assertThat(state.impairedCount()).isBetween(1, 2);
        sawTwo |= state.impairedCount() == 2;
        fault.heal(state, new FakeNodeControl());
        assertThat(state.impairedCount()).isZero();
      }
    assertThat(sawTwo).isTrue();
  }

  @Test
  void leaderRoleTargetsTheLeaderAndFollowerRoleNeverDoes() throws Exception {
    boolean sawLeader = false;
    boolean sawFollower = false;
    for (long seed = 0; seed < 100; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      control.leader = 2;
      final String description = new NodeFault(NodeFault.Kind.PAUSE).inject(state, control, new Random(seed));
      if (description.startsWith("LEADER")) {
        assertThat(state.state(2)).isEqualTo(NodeState.PAUSED);
        sawLeader = true;
      } else {
        assertThat(state.state(2)).isEqualTo(NodeState.UP);
        sawFollower = true;
      }
    }
    assertThat(sawLeader).isTrue();
    assertThat(sawFollower).isTrue();
  }

  @Test
  void splitIsolatesTheLeaderSideMinority() throws Exception {
    final ClusterState five = new ClusterState(5);
    final FakeNodeControl control = new FakeNodeControl();
    control.leader = 4;
    final SplitFault split = new SplitFault();
    split.inject(five, control, new Random(1));
    assertThat(five.state(4)).isEqualTo(NodeState.ISOLATED);
    assertThat(five.impairedCount()).isEqualTo(2);
    split.heal(five, control);
    assertThat(control.calls).contains("reconnect:4");
    assertThat(five.impairedCount()).isZero();

    final ClusterState three = new ClusterState(3);
    control.leader = 1;
    new SplitFault().inject(three, control, new Random(1));
    assertThat(three.impairedCount()).isEqualTo(1);
    assertThat(three.state(1)).isEqualTo(NodeState.ISOLATED);
  }

  @Test
  void rollingRestartRestartsEveryNodeInOrder() throws Exception {
    final ClusterState state = new ClusterState(3);
    final FakeNodeControl control = new FakeNodeControl();
    new RollingRestartFault(Duration.ofSeconds(1)).inject(state, control, new Random(1));
    assertThat(control.calls).containsSubsequence("stop:0", "start:0", "awaitLeader", "stop:1", "start:1", "awaitLeader",
        "stop:2", "start:2", "awaitLeader");
    assertThat(state.impairedCount()).isZero();
  }

  @Test
  void rollingRestartWithoutALeaderIsAnAvailabilityFailure() {
    final FakeNodeControl control = new FakeNodeControl();
    control.leaderAvailable = false;
    assertThatThrownBy(() -> new RollingRestartFault(Duration.ofSeconds(1)).inject(new ClusterState(3), control, new Random(1)))
        .isInstanceOfSatisfying(ChaosFailure.class, e -> assertThat(e.kind()).isEqualTo(ResultKind.AVAILABILITY));
  }

  @Test
  void latencyStaysWithinBoundsAndHealClearsToxics() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final ClusterState state = new ClusterState(3);
      final FakeNodeControl control = new FakeNodeControl();
      final ToxicFault fault = new ToxicFault(ToxicFault.Variant.LATENCY);
      fault.inject(state, control, new Random(seed));
      final String[] call = control.calls.getFirst().split(":");
      assertThat(call[0]).isEqualTo("latency");
      assertThat(Integer.parseInt(call[2])).isBetween(200, 2000);
      assertThat(state.state(Integer.parseInt(call[1]))).isEqualTo(NodeState.DEGRADED);
      fault.heal(state, control);
      assertThat(control.calls).contains("clearToxics:" + call[1]);
      assertThat(state.impairedCount()).isZero();
    }
  }

  @Test
  void lossToxicityStaysWithinBounds() throws Exception {
    for (long seed = 0; seed < 50; seed++) {
      final FakeNodeControl control = new FakeNodeControl();
      new ToxicFault(ToxicFault.Variant.LOSS).inject(new ClusterState(3), control, new Random(seed));
      final float toxicity = Float.parseFloat(control.calls.getFirst().split(":")[2]);
      assertThat(toxicity).isBetween(0.05f, 0.20f);
    }
  }

  private static List<String> script(final long seed) throws Exception {
    final Random random = new Random(seed);
    final ClusterState state = new ClusterState(5);
    final FakeNodeControl control = new FakeNodeControl();
    control.leader = 1;
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults(""), Duration.ofSeconds(1));
    for (int step = 0; step < 30; step++) {
      final Fault fault = picker.pick(state, random);
      control.calls.add("fault:" + fault.name());
      fault.inject(state, control, random);
      fault.heal(state, control);
    }
    return control.calls;
  }

  @Test
  void sameSeedSameDecisions() throws Exception {
    assertThat(script(42)).isEqualTo(script(42));
    assertThat(script(42)).isNotEqualTo(script(43));
  }

  @Test
  void weightsAreRespected() {
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults("kill:9,pause:1"), Duration.ofSeconds(1));
    final ClusterState state = new ClusterState(3);
    final Random random = new Random(7);
    int kills = 0;
    for (int i = 0; i < 10_000; i++)
      if (picker.pick(state, random).name().equals("kill"))
        ++kills;
    assertThat(kills).isBetween(8_500, 9_500);
  }

  @Test
  void noApplicableFaultReturnsNull() {
    final FaultPicker picker = new FaultPicker(ChaosConfig.parseFaults("split"), Duration.ofSeconds(1));
    final ClusterState state = new ClusterState(3);
    state.set(0, NodeState.DOWN);
    assertThat(picker.pick(state, new Random(1))).isNull();
  }
}
```

- [ ] **Step 5.2: Run it to verify it fails**

Run: `RUN FaultsIT` - Expected: compilation failure (`NodeControl`, `ClusterState`, fault classes missing).

- [ ] **Step 5.3: Implement the fault model**

`ClusterState.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.Arrays;

/**
 * What the runner has done to each node. Only the runner thread reads or writes it. A fault may impair at most a
 * minority of nodes, counting nodes already impaired, so the cluster always keeps a possible majority.
 */
public final class ClusterState {
  public enum NodeState {UP, DOWN, PAUSED, ISOLATED, DEGRADED}

  private final NodeState[] states;

  public ClusterState(final int nodes) {
    states = new NodeState[nodes];
    Arrays.fill(states, NodeState.UP);
  }

  public int size() {
    return states.length;
  }

  public int minority() {
    return (states.length - 1) / 2;
  }

  public NodeState state(final int node) {
    return states[node];
  }

  public void set(final int node, final NodeState state) {
    states[node] = state;
  }

  public int impairedCount() {
    int count = 0;
    for (final NodeState state : states)
      if (state != NodeState.UP)
        ++count;
    return count;
  }

  public boolean canImpair(final int additional) {
    return impairedCount() + additional <= minority();
  }

  @Override
  public String toString() {
    return Arrays.toString(states);
  }
}
```

`NodeControl.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.time.Duration;

/**
 * Everything a fault can do to the cluster. The container-backed implementation lives in {@code HaChaosIT}; tests use
 * {@code FakeNodeControl}.
 */
public interface NodeControl {
  void kill(int node) throws Exception;

  void stopGracefully(int node) throws Exception;

  /** Starts a stopped or killed node and returns once it answers its health check. */
  void start(int node) throws Exception;

  void pause(int node) throws Exception;

  void unpause(int node) throws Exception;

  void disconnect(int node) throws Exception;

  void reconnect(int node) throws Exception;

  /** Adds latency to the node's inbound Raft traffic. */
  void addLatency(int node, int latencyMs, int jitterMs) throws Exception;

  /** Drops the node's inbound Raft connections with the given probability. */
  void addLoss(int node, float toxicity) throws Exception;

  void clearToxics(int node) throws Exception;

  /** @return the index of the current leader, or -1 when no node reports being leader */
  int findLeader();

  /** @return true when a leader was elected and every node knows it within the timeout */
  boolean awaitLeader(Duration timeout);
}
```

`ChaosFailure.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Stops a chaos run with a categorized result.
 */
public final class ChaosFailure extends RuntimeException {
  private final ResultKind kind;

  public ChaosFailure(final ResultKind kind, final String message) {
    super(message);
    this.kind = kind;
  }

  public ResultKind kind() {
    return kind;
  }
}
```

`Fault.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.Random;

/**
 * One kind of failure. {@link #inject} picks targets from the seeded random and impairs them; {@link #heal} restores
 * exactly those targets. Only one fault is active at a time.
 */
public interface Fault {
  String name();

  boolean canApply(ClusterState state);

  /** @return the role and targets, for the step log, e.g. {@code "LEADER [2]"} */
  String inject(ClusterState state, NodeControl control, Random random) throws Exception;

  void heal(ClusterState state, NodeControl control) throws Exception;

  /** True when a majority stays connected, so acknowledged writes must keep flowing once a leader is elected. */
  boolean expectsWritesAvailable();
}
```

`Targets.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Target selection shared by the faults. Every method consumes the random the same way for a given cluster size, so
 * one seed yields one sequence of decisions whoever the leader happens to be.
 */
final class Targets {
  enum Role {LEADER, FOLLOWER, ANY}

  private Targets() {
  }

  static Role role(final Random random) {
    return random.nextBoolean() ? Role.LEADER : Role.FOLLOWER;
  }

  /** One node, or two when the random asks for two and the cluster can afford it. */
  static int count(final ClusterState state, final Random random) {
    final boolean wantTwo = random.nextBoolean();
    return wantTwo && state.canImpair(2) ? 2 : 1;
  }

  static int[] pick(final ClusterState state, final NodeControl control, final Random random, final Role role,
      final int count) {
    final List<Integer> candidates = new ArrayList<>();
    for (int i = 0; i < state.size(); i++)
      if (state.state(i) == NodeState.UP)
        candidates.add(i);
    Collections.shuffle(candidates, random);

    final int leader = role == Role.ANY ? -1 : control.findLeader();
    if (leader >= 0 && candidates.remove(Integer.valueOf(leader)) && role == Role.LEADER)
      candidates.addFirst(leader);

    if (candidates.size() < count)
      throw new ChaosFailure(ResultKind.HARNESS, "Need " + count + " " + role + " targets, UP candidates: " + candidates);
    final int[] targets = new int[count];
    for (int i = 0; i < count; i++)
      targets[i] = candidates.get(i);
    return targets;
  }
}
```

`NodeFault.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;

import java.util.Arrays;
import java.util.Random;

/**
 * Faults that act on whole nodes: SIGKILL, graceful stop, freeze ({@code docker pause}) and network isolation. The
 * target is the leader or a follower with equal probability; on 5 nodes it may be two nodes.
 */
public final class NodeFault implements Fault {
  public enum Kind {
    KILL("kill", NodeState.DOWN), STOP("stop", NodeState.DOWN), PAUSE("pause", NodeState.PAUSED),
    ISOLATE("isolate", NodeState.ISOLATED);

    private final String    faultName;
    private final NodeState impaired;

    Kind(final String faultName, final NodeState impaired) {
      this.faultName = faultName;
      this.impaired = impaired;
    }
  }

  private final Kind  kind;
  private       int[] targets = new int[0];

  public NodeFault(final Kind kind) {
    this.kind = kind;
  }

  @Override
  public String name() {
    return kind.faultName;
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.canImpair(1);
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    final Targets.Role role = Targets.role(random);
    final int count = Targets.count(state, random);
    targets = Targets.pick(state, control, random, role, count);
    for (final int node : targets) {
      switch (kind) {
      case KILL -> control.kill(node);
      case STOP -> control.stopGracefully(node);
      case PAUSE -> control.pause(node);
      case ISOLATE -> control.disconnect(node);
      }
      state.set(node, kind.impaired);
    }
    return role + " " + Arrays.toString(targets);
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      switch (kind) {
      case KILL, STOP -> control.start(node);
      case PAUSE -> control.unpause(node);
      case ISOLATE -> control.reconnect(node);
      }
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
```

`SplitFault.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;

import java.util.Arrays;
import java.util.Random;

/**
 * Cuts the leader plus enough followers to form the largest minority off the network, forcing an election on the
 * majority side. On 3 nodes this is the leader alone. The disconnected nodes cannot reach each other either.
 */
public final class SplitFault implements Fault {
  private int[] targets = new int[0];

  @Override
  public String name() {
    return "split";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.impairedCount() == 0;
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    targets = Targets.pick(state, control, random, Targets.Role.LEADER, state.minority());
    for (final int node : targets) {
      control.disconnect(node);
      state.set(node, NodeState.ISOLATED);
    }
    return "LEADER-side minority " + Arrays.toString(targets);
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      control.reconnect(node);
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
```

`RollingRestartFault.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;

import java.time.Duration;
import java.util.Random;

/**
 * Gracefully restarts every node in index order, waiting for a leader after each one. The whole fault happens inside
 * {@link #inject}; {@link #heal} has nothing left to do.
 */
public final class RollingRestartFault implements Fault {
  private final Duration electionTimeout;

  public RollingRestartFault(final Duration electionTimeout) {
    this.electionTimeout = electionTimeout;
  }

  @Override
  public String name() {
    return "rolling";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.impairedCount() == 0;
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    for (int node = 0; node < state.size(); node++) {
      control.stopGracefully(node);
      state.set(node, NodeState.DOWN);
      control.start(node);
      state.set(node, NodeState.UP);
      if (!control.awaitLeader(electionTimeout))
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "No leader within " + electionTimeout + " after restarting node " + node + " during a rolling restart");
    }
    return "ALL in index order";
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) {
    // every node was already restarted by inject
  }

  @Override
  public boolean expectsWritesAvailable() {
    return false;
  }
}
```

`ToxicFault.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.containers.ha.chaos.ClusterState.NodeState;

import java.util.Arrays;
import java.util.Random;

/**
 * Degrades the inbound Raft traffic of one node (two on 5 nodes) through Toxiproxy: 200-2000 ms latency with 25%
 * jitter, or connection drops with 5-20% probability.
 */
public final class ToxicFault implements Fault {
  public enum Variant {LATENCY, LOSS}

  private final Variant variant;
  private       int[]   targets = new int[0];

  public ToxicFault(final Variant variant) {
    this.variant = variant;
  }

  @Override
  public String name() {
    return variant == Variant.LATENCY ? "latency" : "loss";
  }

  @Override
  public boolean canApply(final ClusterState state) {
    return state.canImpair(1);
  }

  @Override
  public String inject(final ClusterState state, final NodeControl control, final Random random) throws Exception {
    final int count = Targets.count(state, random);
    targets = Targets.pick(state, control, random, Targets.Role.ANY, count);
    final StringBuilder description = new StringBuilder("ANY ").append(Arrays.toString(targets));
    for (final int node : targets) {
      if (variant == Variant.LATENCY) {
        final int latencyMs = 200 + random.nextInt(1801);
        control.addLatency(node, latencyMs, latencyMs / 4);
        description.append(' ').append(latencyMs).append("ms");
      } else {
        final float toxicity = (5 + random.nextInt(16)) / 100f;
        control.addLoss(node, toxicity);
        description.append(' ').append(toxicity);
      }
      state.set(node, NodeState.DEGRADED);
    }
    return description.toString();
  }

  @Override
  public void heal(final ClusterState state, final NodeControl control) throws Exception {
    for (final int node : targets) {
      control.clearToxics(node);
      state.set(node, NodeState.UP);
    }
    targets = new int[0];
  }

  @Override
  public boolean expectsWritesAvailable() {
    return true;
  }
}
```

`FaultPicker.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Weighted, seeded choice among the enabled faults that can apply to the current cluster state.
 */
public final class FaultPicker {
  private final List<Fault> faults = new ArrayList<>();
  private final int[]       weights;

  public FaultPicker(final Map<String, Integer> faultWeights, final Duration electionTimeout) {
    weights = new int[faultWeights.size()];
    int i = 0;
    for (final Map.Entry<String, Integer> entry : faultWeights.entrySet()) {
      faults.add(create(entry.getKey(), electionTimeout));
      weights[i++] = entry.getValue();
    }
  }

  static Fault create(final String name, final Duration electionTimeout) {
    return switch (name) {
      case "kill" -> new NodeFault(NodeFault.Kind.KILL);
      case "stop" -> new NodeFault(NodeFault.Kind.STOP);
      case "pause" -> new NodeFault(NodeFault.Kind.PAUSE);
      case "isolate" -> new NodeFault(NodeFault.Kind.ISOLATE);
      case "split" -> new SplitFault();
      case "rolling" -> new RollingRestartFault(electionTimeout);
      case "latency" -> new ToxicFault(ToxicFault.Variant.LATENCY);
      case "loss" -> new ToxicFault(ToxicFault.Variant.LOSS);
      default -> throw new IllegalArgumentException("Unknown fault: " + name);
    };
  }

  /**
   * @return a fault that can apply, or null when none can. Always consumes exactly one draw from the random.
   */
  public Fault pick(final ClusterState state, final Random random) {
    int total = 0;
    for (int i = 0; i < faults.size(); i++)
      if (faults.get(i).canApply(state))
        total += weights[i];
    final int roll = random.nextInt(Math.max(total, 1));
    if (total == 0)
      return null;
    int cumulative = 0;
    for (int i = 0; i < faults.size(); i++) {
      if (!faults.get(i).canApply(state))
        continue;
      cumulative += weights[i];
      if (roll < cumulative)
        return faults.get(i);
    }
    throw new IllegalStateException("Weighted pick fell through: roll " + roll + " of " + total);
  }
}
```

- [ ] **Step 5.4: Run it to verify it passes**

Run: `RUN FaultsIT` - Expected: `Tests run: 12, Failures: 0, Errors: 0`.

- [ ] **Step 5.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{ClusterState,NodeControl,ChaosFailure,Fault,Targets,NodeFault,SplitFault,RollingRestartFault,ToxicFault,FaultPicker,FakeNodeControl,FaultsIT}.java
```

---

### Task 6: Workload (`Endpoint`, `Endpoints`, `ChaosHttp`, `OpOutcome`, `LoadGenerator`, `Workload`)

**Files:**
- Create: `.../chaos/Endpoint.java`, `Endpoints.java`, `ChaosHttp.java`, `OpOutcome.java`, `LoadGenerator.java`, `Workload.java`
- Test: `.../chaos/WorkloadIT.java`

**Interfaces:**
- Consumes: `ChaosConfig` (Task 1), `Ledger` (Task 2), `ChaosSchema` (Task 4).
- Produces:
  - `record Endpoint(String host, int port)`; `interface Endpoints { int size(); Endpoint endpoint(int node); }`.
  - `ChaosHttp.post(String host, int port, String path, String json, int connectTimeoutMs, int readTimeoutMs) throws IOException`
    returning `record Response(int status, String body)`; throws `ChaosHttp.NotSentException` (an `IOException`) when the
    TCP connection could not be opened.
  - `OpOutcome.fromStatus(int)`, `OpOutcome.fromException(IOException)` returning a ledger outcome byte.
  - `interface LoadGenerator extends AutoCloseable { void start(); void quiesce() throws InterruptedException; void resume(); long acked(); void close(); }`
    (`quiesce` and `resume` must be called from the same thread; `close` is idempotent).
  - `Workload(ChaosConfig, Ledger, Endpoints, String database)` and
    `Workload(ChaosConfig, Ledger, Endpoints, String database, int connectTimeoutMs, int readTimeoutMs, int thinkMs)`.

- [ ] **Step 6.1: Write the failing test**

```java
package com.arcadedb.containers.ha.chaos;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Tag("chaos")
class WorkloadIT {
  private final AtomicInteger status   = new AtomicInteger(200);
  private final AtomicInteger delayMs  = new AtomicInteger();
  private final AtomicInteger requests = new AtomicInteger();
  private final List<String>  bodies   = new CopyOnWriteArrayList<>();
  private       HttpServer    server;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", exchange -> {
      bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      requests.incrementAndGet();
      try {
        Thread.sleep(delayMs.get());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.sendResponseHeaders(status.get(), -1);
      exchange.close();
    });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private static Endpoints single(final int port) {
    final Endpoint endpoint = new Endpoint("127.0.0.1", port);
    return new Endpoints() {
      @Override
      public int size() {
        return 1;
      }

      @Override
      public Endpoint endpoint(final int node) {
        return endpoint;
      }
    };
  }

  private Workload workload(final Ledger ledger, final Endpoints endpoints, final int readTimeoutMs) {
    final ChaosConfig config = ChaosConfig.fromProperties(ChaosConfigIT.props("chaos.seed", "1", "chaos.writers", "2"));
    return new Workload(config, ledger, endpoints, ChaosSchema.DATABASE, 1_000, readTimeoutMs, 1);
  }

  @Test
  void successfulWritesAreAcked() {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.ACKED) >= 50);
    }
    assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
    assertThat(ledger.count(Ledger.UNKNOWN) + ledger.count(Ledger.FAILED)).isZero();
  }

  @Test
  void serverErrorsAreUnknown() {
    status.set(503);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.UNKNOWN) >= 20);
    }
    assertThat(ledger.count(Ledger.ACKED)).isZero();
  }

  @Test
  void authenticationRejectionIsFailed() {
    status.set(401);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.FAILED) >= 20);
    }
  }

  @Test
  void refusedConnectionIsFailed() throws IOException {
    final int closedPort;
    try (final ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(closedPort), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.FAILED) >= 20);
    }
    assertThat(ledger.count(Ledger.UNKNOWN)).isZero();
  }

  @Test
  void readTimeoutIsUnknown() {
    delayMs.set(1_000);
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 200)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> ledger.count(Ledger.UNKNOWN) >= 3);
    }
    assertThat(ledger.count(Ledger.ACKED)).isZero();
  }

  @Test
  void quiesceStopsTrafficUntilResume() throws InterruptedException {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() >= 10);
      workload.quiesce();
      final int frozen = requests.get();
      assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
      Thread.sleep(500);
      assertThat(requests.get()).isEqualTo(frozen);
      workload.resume();
      await().atMost(Duration.ofSeconds(10)).until(() -> requests.get() > frozen);
    }
  }

  @Test
  void payloadsCarryTheSchemaStatementsAndParameters() {
    final Ledger ledger = new Ledger(2);
    try (final Workload workload = workload(ledger, single(server.getAddress().getPort()), 2_000)) {
      workload.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> ledger.count(Ledger.ACKED) >= 200);
    }
    assertThat(bodies).anySatisfy(body -> assertThat(body).contains("\"language\":\"sql\"").contains("INSERT INTO ChaosOp"));
    assertThat(bodies).anySatisfy(body -> assertThat(body).contains("\"language\":\"sqlscript\"").contains("\"target\""));
  }

  @Test
  void outcomeClassification() {
    assertThat(OpOutcome.fromStatus(200)).isEqualTo(Ledger.ACKED);
    assertThat(OpOutcome.fromStatus(204)).isEqualTo(Ledger.ACKED);
    assertThat(OpOutcome.fromStatus(401)).isEqualTo(Ledger.FAILED);
    assertThat(OpOutcome.fromStatus(403)).isEqualTo(Ledger.FAILED);
    for (final int code : new int[] { 400, 409, 500, 503 })
      assertThat(OpOutcome.fromStatus(code)).isEqualTo(Ledger.UNKNOWN);
    assertThat(OpOutcome.fromException(new ChaosHttp.NotSentException(new ConnectException("refused")))).isEqualTo(Ledger.FAILED);
    assertThat(OpOutcome.fromException(new SocketTimeoutException("read"))).isEqualTo(Ledger.UNKNOWN);
  }
}
```

- [ ] **Step 6.2: Run it to verify it fails**

Run: `RUN WorkloadIT` - Expected: compilation failure (`Workload`, `Endpoint`, `ChaosHttp` missing).

- [ ] **Step 6.3: Implement the workload classes**

`Endpoint.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Host-reachable HTTP address of one node.
 */
public record Endpoint(String host, int port) {
}
```

`Endpoints.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Current HTTP address of each node. A restarted container can come back on a different host port, so callers must
 * ask again for every request rather than caching.
 */
public interface Endpoints {
  int size();

  Endpoint endpoint(int node);
}
```

`ChaosHttp.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.test.support.ContainersTestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Minimal HTTP client that tells "never sent" apart from "sent, answer unknown": only a failure of
 * {@link HttpURLConnection#connect()} is reported as {@link NotSentException}. A failure on a reused keep-alive
 * connection surfaces later, on write or read, and is therefore classified UNKNOWN, which is the conservative side.
 * (A {@code Connection: close} request header would not help: HttpURLConnection silently drops it as restricted.)
 */
public final class ChaosHttp {
  private static final String AUTHORIZATION = "Basic " + Base64.getEncoder()
      .encodeToString(("root:" + ContainersTestTemplate.PASSWORD).getBytes(StandardCharsets.UTF_8));

  public record Response(int status, String body) {
  }

  /**
   * The TCP connection could not be established, so the request was never sent.
   */
  public static final class NotSentException extends IOException {
    public NotSentException(final IOException cause) {
      super(cause.getMessage(), cause);
    }
  }

  private ChaosHttp() {
  }

  public static Response post(final String host, final int port, final String path, final String json,
      final int connectTimeoutMs, final int readTimeoutMs) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) URI.create("http://" + host + ":" + port + path).toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", AUTHORIZATION);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setConnectTimeout(connectTimeoutMs);
    connection.setReadTimeout(readTimeoutMs);
    connection.setDoOutput(true);
    try {
      try {
        connection.connect();
      } catch (final IOException e) {
        throw new NotSentException(e);
      }
      try (final OutputStream out = connection.getOutputStream()) {
        out.write(json.getBytes(StandardCharsets.UTF_8));
      }
      final int status = connection.getResponseCode();
      final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }
}
```

`OpOutcome.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.io.IOException;

/**
 * Maps an HTTP answer to a ledger outcome, conservatively: only answers that prove the write never reached the Raft
 * log are FAILED. Any 4xx/5xx other than authentication, and any error after the request was sent, is UNKNOWN,
 * because the server may have appended the entry before failing.
 */
public final class OpOutcome {
  private OpOutcome() {
  }

  public static byte fromStatus(final int status) {
    if (status >= 200 && status < 300)
      return Ledger.ACKED;
    if (status == 401 || status == 403)
      return Ledger.FAILED;
    return Ledger.UNKNOWN;
  }

  public static byte fromException(final IOException exception) {
    return exception instanceof ChaosHttp.NotSentException ? Ledger.FAILED : Ledger.UNKNOWN;
  }
}
```

`LoadGenerator.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Continuous write load recorded in the ledger. {@link #quiesce()} and {@link #resume()} must be called from the same
 * thread; {@link #close()} is idempotent.
 */
public interface LoadGenerator extends AutoCloseable {
  void start();

  /** Blocks until every writer is idle at an operation boundary; no new operation starts until {@link #resume()}. */
  void quiesce() throws InterruptedException;

  void resume();

  /** @return operations acknowledged so far, including late commits found at checkpoints */
  long acked();

  @Override
  void close();
}
```

`Workload.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * K writer threads, each a closed loop: pick a node, send one single-vertex insert (80%) or one vertex-plus-edge
 * transaction (20%), record the outcome, think briefly. Each operation is attempted exactly once. Writers pick any node
 * whatever its state: writes to a stopped node fail fast, writes to a paused one time out as UNKNOWN, writes to a
 * follower exercise leader forwarding. Each writer has its own Random derived from the seed.
 */
public final class Workload implements LoadGenerator {
  private static final int  DEFAULT_CONNECT_TIMEOUT_MS = 2_000;
  private static final int  DEFAULT_READ_TIMEOUT_MS    = 15_000;
  private static final int  DEFAULT_THINK_MS           = 5;
  private static final int  PAIR_PERCENT               = 20;
  private static final long WRITER_SEED_MIX            = 0x9E3779B97F4A7C15L;

  private final ChaosConfig            config;
  private final Ledger                 ledger;
  private final Endpoints              endpoints;
  private final String                 commandPath;
  private final int                    connectTimeoutMs;
  private final int                    readTimeoutMs;
  private final int                    thinkMs;
  private final ReentrantReadWriteLock gate    = new ReentrantReadWriteLock(true);
  private final AtomicBoolean          running = new AtomicBoolean();
  private final List<Thread>           threads = new ArrayList<>();

  public Workload(final ChaosConfig config, final Ledger ledger, final Endpoints endpoints, final String database) {
    this(config, ledger, endpoints, database, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_THINK_MS);
  }

  public Workload(final ChaosConfig config, final Ledger ledger, final Endpoints endpoints, final String database,
      final int connectTimeoutMs, final int readTimeoutMs, final int thinkMs) {
    this.config = config;
    this.ledger = ledger;
    this.endpoints = endpoints;
    this.commandPath = "/api/v1/command/" + database;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.thinkMs = thinkMs;
  }

  @Override
  public void start() {
    running.set(true);
    for (int w = 0; w < config.writers(); w++) {
      final int writer = w;
      final Random random = new Random(config.seed() ^ (WRITER_SEED_MIX * (writer + 1)));
      final Thread thread = new Thread(() -> loop(writer, random), "chaos-writer-" + writer);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  private void loop(final int writer, final Random random) {
    while (running.get()) {
      gate.readLock().lock();
      try {
        if (!running.get())
          return;
        operation(writer, random);
      } finally {
        gate.readLock().unlock();
      }
      try {
        Thread.sleep(thinkMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void operation(final int writer, final Random random) {
    final int node = random.nextInt(endpoints.size());
    final long target = random.nextInt(100) < PAIR_PERCENT ? ledger.randomAckedKey(writer, random) : -1;
    final boolean pair = target >= 0;
    final long key = ledger.reserve(writer, pair);
    final JSONObject payload = new JSONObject()
        .put("language", pair ? "sqlscript" : "sql")
        .put("command", pair ? ChaosSchema.INSERT_PAIR : ChaosSchema.INSERT_SINGLE)
        .put("params", new JSONObject(pair ? ChaosSchema.pairParams(key, target) : ChaosSchema.singleParams(key)));
    byte outcome;
    try {
      final Endpoint endpoint = endpoints.endpoint(node);
      final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), commandPath, payload.toString(),
          connectTimeoutMs, readTimeoutMs);
      outcome = OpOutcome.fromStatus(response.status());
    } catch (final IOException e) {
      outcome = OpOutcome.fromException(e);
    } catch (final RuntimeException e) {
      outcome = Ledger.UNKNOWN;
    }
    ledger.record(key, outcome);
  }

  @Override
  public void quiesce() throws InterruptedException {
    gate.writeLock().lockInterruptibly();
  }

  @Override
  public void resume() {
    if (gate.isWriteLockedByCurrentThread())
      gate.writeLock().unlock();
  }

  @Override
  public long acked() {
    return ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE);
  }

  @Override
  public void close() {
    running.set(false);
    resume();
    for (final Thread thread : threads)
      try {
        thread.join(readTimeoutMs + 5_000L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    threads.clear();
  }
}
```

- [ ] **Step 6.4: Run it to verify it passes**

Run: `RUN WorkloadIT` - Expected: `Tests run: 8, Failures: 0, Errors: 0`.

- [ ] **Step 6.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{Endpoint,Endpoints,ChaosHttp,OpOutcome,LoadGenerator,Workload,WorkloadIT}.java
```

---

### Task 7: Checkpoint (`NodeReader`, `HttpNodeReader`, `Checkpoint`)

**Files:**
- Create: `.../chaos/NodeReader.java`, `HttpNodeReader.java`, `Checkpoint.java`
- Test: `.../chaos/HttpNodeReaderIT.java`, `.../chaos/CheckpointIT.java`

**Interfaces:**
- Consumes: `Ledger`, `NodeSnapshot`, `InvariantChecker`, `Violation`, `ResultKind` (Tasks 2-3); `ChaosSchema` (Task 4);
  `Endpoints`, `ChaosHttp` (Task 6).
- Produces:
  - `interface NodeReader { long[] counts(int node) throws IOException; void scan(int node, NodeSnapshot sink) throws IOException; }`
    (`counts` returns `{ops, edges}` read locally on that node).
  - `HttpNodeReader(Endpoints, String database)`; package-private
    `static void scanPages(PageFetcher fetcher, NodeSnapshot sink, int pageSize) throws IOException` with
    `interface PageFetcher { JSONArray fetch(long last) throws IOException; }`.
  - `Checkpoint(NodeReader, Ledger, InvariantChecker, int nodes, Duration convergenceTimeout, Duration pollInterval)`;
    `Result run() throws InterruptedException`;
    `record Result(List<Violation> violations, long[] counts, long convergenceMillis, long durationMillis)`.
    Invariant ids added: `CONVERGENCE` (SAFETY), `DIVERGENCE` (SAFETY), `SCAN` (AVAILABILITY).

- [ ] **Step 7.1: Write the failing tests**

`HttpNodeReaderIT.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class HttpNodeReaderIT {

  private static JSONArray rows(final long... keys) {
    final JSONArray rows = new JSONArray();
    for (final long key : keys)
      rows.put(new JSONObject().put("id", key).put("e", 0));
    return rows;
  }

  @Test
  void exactMultipleOfPageSizeTerminates() throws Exception {
    final Ledger ledger = new Ledger(1);
    for (int i = 0; i < 3; i++)
      ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    final List<Long> requested = new ArrayList<>();
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> {
      requested.add(last);
      return last < 0 ? rows(Ledger.key(0, 0), Ledger.key(0, 1), Ledger.key(0, 2)) : rows();
    }, snapshot, 3);
    assertThat(requested).containsExactly(-1L, Ledger.key(0, 2));
    assertThat(snapshot.rows()).isEqualTo(3);
    assertThat(snapshot.duplicates()).isEmpty();
  }

  @Test
  void partialPageStopsAfterOneRequest() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    final List<Long> requested = new ArrayList<>();
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> {
      requested.add(last);
      return rows(Ledger.key(0, 0));
    }, snapshot, 3);
    assertThat(requested).containsExactly(-1L);
    assertThat(snapshot.present(0, 0)).isTrue();
  }

  @Test
  void edgeCountIsPassedThrough() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, true), Ledger.ACKED);
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    HttpNodeReader.scanPages(last -> new JSONArray().put(new JSONObject().put("id", Ledger.key(0, 0)).put("e", 1)), snapshot, 3);
    assertThat(snapshot.hasEdge(0, 0)).isTrue();
  }
}
```

`CheckpointIT.java`:

```java
package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class CheckpointIT {
  private final Ledger ledger = new Ledger(1);

  private long acked() {
    final long key = ledger.reserve(0, false);
    ledger.record(key, Ledger.ACKED);
    return key;
  }

  /** Node contents as {key, edges} rows; a node can lag (report stale counts) or be unreadable. */
  private static final class ScriptedNodeReader implements NodeReader {
    final Map<Integer, List<long[]>> rows       = new HashMap<>();
    final Map<Integer, Integer>      lagPolls   = new HashMap<>();
    final Set<Integer>               unreadable = new HashSet<>();

    @Override
    public long[] counts(final int node) throws IOException {
      if (unreadable.contains(node))
        throw new IOException("node " + node + " down");
      final int lag = lagPolls.getOrDefault(node, 0);
      if (lag > 0) {
        lagPolls.put(node, lag - 1);
        return new long[] { -1, -1 };
      }
      final List<long[]> nodeRows = rows.get(node);
      return new long[] { nodeRows.size(), nodeRows.stream().mapToLong(row -> row[1]).sum() };
    }

    @Override
    public void scan(final int node, final NodeSnapshot sink) {
      for (final long[] row : rows.get(node))
        sink.add(row[0], (int) row[1]);
    }
  }

  private Checkpoint checkpoint(final NodeReader reader, final Duration timeout) {
    return new Checkpoint(reader, ledger, new InvariantChecker(ledger), 3, timeout, Duration.ofMillis(20));
  }

  @Test
  void identicalNodesPass() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final Checkpoint.Result result = checkpoint(reader, Duration.ofSeconds(5)).run();
    assertThat(result.violations()).isEmpty();
    assertThat(result.counts()).containsExactly(2, 0, 2, 0, 2, 0);
  }

  @Test
  void laggingNodeConverges() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.lagPolls.put(2, 3);
    assertThat(checkpoint(reader, Duration.ofSeconds(5)).run().violations()).isEmpty();
  }

  @Test
  void neverConvergingIsASafetyViolation() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    reader.rows.put(0, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    reader.rows.put(1, List.of(new long[] { a, 0 }));
    reader.rows.put(2, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofMillis(300)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("CONVERGENCE");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(violations.getFirst().message()).contains("[2, 0, 1, 0, 2, 0]");
  }

  @Test
  void unreadableNodeTimesOutWithTheReadError() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.unreadable.add(1);
    final List<Violation> violations = checkpoint(reader, Duration.ofMillis(300)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("CONVERGENCE");
    assertThat(violations.getFirst().message()).contains("node 1 down");
  }

  @Test
  void sameCountsDifferentKeysIsDivergence() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final long c = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    reader.rows.put(0, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    reader.rows.put(1, List.of(new long[] { a, 0 }, new long[] { c, 0 }));
    reader.rows.put(2, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).contains("DIVERGENCE");
    final Violation divergence = violations.stream().filter(v -> v.invariant().equals("DIVERGENCE")).findFirst().orElseThrow();
    assertThat(divergence.keys()).containsExactlyInAnyOrder(b, c);
  }

  @Test
  void invariantViolationsSurface() throws InterruptedException {
    final long a = acked();
    final long lost = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("I1");
    assertThat(violations.getFirst().keys()).containsExactly(lost);
  }
}
```

- [ ] **Step 7.2: Run them to verify they fail**

Run: `RUN 'HttpNodeReaderIT,CheckpointIT'` - Expected: compilation failure (`NodeReader`, `HttpNodeReader`, `Checkpoint` missing).

- [ ] **Step 7.3: Implement**

`NodeReader.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.io.IOException;

/**
 * Reads what one node holds, from that node's local state (follower reads default to eventual consistency, so a
 * follower answers from what it has applied).
 */
public interface NodeReader {
  /** @return {@code {ChaosOp rows, NEXT edges}} as seen by the node */
  long[] counts(int node) throws IOException;

  /** Streams every ChaosOp row of the node into the snapshot, in key order. */
  void scan(int node, NodeSnapshot sink) throws IOException;
}
```

`HttpNodeReader.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;

/**
 * {@link NodeReader} over each node's own {@code /api/v1/query} endpoint, paging by key so a soak with millions of
 * rows never loads more than one page per request.
 */
public final class HttpNodeReader implements NodeReader {
  private static final int CONNECT_TIMEOUT_MS = 5_000;
  private static final int READ_TIMEOUT_MS    = 120_000;

  interface PageFetcher {
    JSONArray fetch(long last) throws IOException;
  }

  private final Endpoints endpoints;
  private final String    queryPath;

  public HttpNodeReader(final Endpoints endpoints, final String database) {
    this.endpoints = endpoints;
    this.queryPath = "/api/v1/query/" + database;
  }

  @Override
  public long[] counts(final int node) throws IOException {
    return new long[] { count(node, ChaosSchema.COUNT_OPS), count(node, ChaosSchema.COUNT_EDGES) };
  }

  @Override
  public void scan(final int node, final NodeSnapshot sink) throws IOException {
    final String sql = ChaosSchema.page(ChaosSchema.PAGE_SIZE);
    scanPages(last -> query(node, sql, new JSONObject().put("last", last)), sink, ChaosSchema.PAGE_SIZE);
  }

  static void scanPages(final PageFetcher fetcher, final NodeSnapshot sink, final int pageSize) throws IOException {
    long last = -1;
    while (true) {
      final JSONArray rows = fetcher.fetch(last);
      for (int i = 0; i < rows.length(); i++) {
        final JSONObject row = rows.getJSONObject(i);
        last = row.getLong("id");
        sink.add(last, row.getInt("e", 0));
      }
      if (rows.length() < pageSize)
        return;
    }
  }

  private long count(final int node, final String sql) throws IOException {
    final JSONArray rows = query(node, sql, new JSONObject());
    return rows.length() == 0 ? 0 : rows.getJSONObject(0).getLong("c", 0);
  }

  private JSONArray query(final int node, final String sql, final JSONObject params) throws IOException {
    final Endpoint endpoint = endpoints.endpoint(node);
    final String payload = new JSONObject().put("language", "sql").put("command", sql).put("params", params).toString();
    final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), queryPath, payload,
        CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
    if (response.status() != 200)
      throw new IOException("node " + node + " query failed with HTTP " + response.status() + ": " + response.body());
    return new JSONObject(response.body()).getJSONArray("result");
  }
}
```

`Checkpoint.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs with the workload quiesced: waits until every node reports the same row and edge counts twice in a row, scans
 * every node, reports keys that differ between nodes, then checks node 0 against the ledger.
 */
public final class Checkpoint {
  public record Result(List<Violation> violations, long[] counts, long convergenceMillis, long durationMillis) {
  }

  private final NodeReader       reader;
  private final Ledger           ledger;
  private final InvariantChecker checker;
  private final int              nodes;
  private final Duration         convergenceTimeout;
  private final Duration         pollInterval;

  public Checkpoint(final NodeReader reader, final Ledger ledger, final InvariantChecker checker, final int nodes,
      final Duration convergenceTimeout, final Duration pollInterval) {
    this.reader = reader;
    this.ledger = ledger;
    this.checker = checker;
    this.nodes = nodes;
    this.convergenceTimeout = convergenceTimeout;
    this.pollInterval = pollInterval;
  }

  public Result run() throws InterruptedException {
    final long start = System.nanoTime();
    final long deadline = start + convergenceTimeout.toNanos();
    long[] previous = null;
    long[] current;
    String lastError = null;
    while (true) {
      current = new long[nodes * 2];
      boolean readable = true;
      for (int i = 0; i < nodes; i++)
        try {
          final long[] counts = reader.counts(i);
          current[i * 2] = counts[0];
          current[i * 2 + 1] = counts[1];
        } catch (final IOException e) {
          readable = false;
          lastError = e.getMessage();
        }
      if (readable && converged(current) && Arrays.equals(previous, current))
        break;
      previous = readable ? current : null;
      if (System.nanoTime() > deadline) {
        final String message = "Nodes did not converge within " + convergenceTimeout + ": [ops, edges] per node = "
            + Arrays.toString(current) + (lastError == null ? "" : ", last read error: " + lastError);
        return new Result(List.of(new Violation(ResultKind.SAFETY, "CONVERGENCE", message, new long[0])), current,
            millisSince(start), millisSince(start));
      }
      Thread.sleep(pollInterval.toMillis());
    }
    final long convergenceMillis = millisSince(start);

    final NodeSnapshot[] snapshots = new NodeSnapshot[nodes];
    for (int i = 0; i < nodes; i++) {
      snapshots[i] = new NodeSnapshot(ledger);
      try {
        reader.scan(i, snapshots[i]);
      } catch (final IOException e) {
        return new Result(List.of(new Violation(ResultKind.AVAILABILITY, "SCAN",
            "node " + i + " could not be scanned after convergence: " + e.getMessage(), new long[0])), current,
            convergenceMillis, millisSince(start));
      }
    }

    final List<Violation> violations = new ArrayList<>();
    for (int i = 1; i < nodes; i++) {
      final long[] diff = snapshots[0].diff(snapshots[i], InvariantChecker.MAX_KEYS);
      if (diff.length > 0)
        violations.add(new Violation(ResultKind.SAFETY, "DIVERGENCE",
            "node " + i + " differs from node 0 in " + diff.length + (diff.length == InvariantChecker.MAX_KEYS ? "+" : "")
                + " keys despite equal counts", diff));
    }
    violations.addAll(checker.check(snapshots[0]));
    return new Result(violations, current, convergenceMillis, millisSince(start));
  }

  private static boolean converged(final long[] counts) {
    for (int i = 2; i < counts.length; i += 2)
      if (counts[i] != counts[0] || counts[i + 1] != counts[1])
        return false;
    return true;
  }

  private static long millisSince(final long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }
}
```

- [ ] **Step 7.4: Run them to verify they pass**

Run: `RUN 'HttpNodeReaderIT,CheckpointIT'` - Expected: `Tests run: 9, Failures: 0, Errors: 0`.

- [ ] **Step 7.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{NodeReader,HttpNodeReader,Checkpoint,HttpNodeReaderIT,CheckpointIT}.java
```

---

### Task 8: Report (`ChaosResult`, `StepRecord`, `TrendRow`, `ChaosReport`)

**Files:**
- Create: `.../chaos/ChaosResult.java`, `StepRecord.java`, `TrendRow.java`, `ChaosReport.java`
- Test: `.../chaos/ChaosReportIT.java`

**Interfaces:**
- Consumes: `ChaosConfig`, `Ledger`, `Violation`, `ResultKind`.
- Produces:
  - `record ChaosResult(ResultKind kind, String message, int steps, List<Violation> violations)`.
  - `record StepRecord(int step, String fault, String targets, int leaderBefore, int leaderAfter, long timeToLeaderMs, long convergenceMs, long ackedDuringHold, long acked, long unknown, long failed)`
    with `String toLine()`.
  - `record TrendRow(int step, long[] memoryBytes, long[] databaseBytes, long[] replicationBytes, double acksPerSecond, long checkpointMs)`
    with `static final String CSV_HEADER` and `List<String> toCsvLines()` (one line per node).
  - `ChaosReport(Path dir, ChaosConfig config) throws IOException`, `Path dir()`, `void step(StepRecord)`,
    `void trend(TrendRow)`, `void ledgerDiff(List<Violation>)`, `void summary(ChaosResult, Ledger, long lateCommits)`
    (all `throws IOException`), `close()`. Files: `summary.json`, `steps.log`, `trends.csv`, `ledger-diff.txt`.

- [ ] **Step 8.1: Write the failing test**

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class ChaosReportIT {
  @TempDir
  Path dir;

  private final ChaosConfig config = ChaosConfig.fromProperties(ChaosConfigIT.props("chaos.seed", "42"));

  @Test
  void stepsAndTrendsAreFlushedAsTheyAreWritten() throws Exception {
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.step(new StepRecord(1, "pause", "LEADER [0]", 0, 1, 1200, 300, 55, 1000, 3, 1));
      report.step(new StepRecord(2, "kill", "FOLLOWER [2]", 1, 1, 900, 250, 80, 1100, 3, 2));
      report.trend(new TrendRow(2, new long[] { 1, 2, 3 }, new long[] { 4, 5, 6 }, new long[] { 7, 8, 9 }, 12.5, 400));

      final List<String> steps = Files.readAllLines(dir.resolve("steps.log"));
      assertThat(steps).hasSize(2);
      assertThat(steps.get(1)).contains("step=2").contains("fault=kill").contains("targets=FOLLOWER [2]");

      final List<String> trends = Files.readAllLines(dir.resolve("trends.csv"));
      assertThat(trends).hasSize(4);
      assertThat(trends.getFirst()).isEqualTo(TrendRow.CSV_HEADER);
      assertThat(trends.get(3)).isEqualTo("2,2,3,6,9,12.50,400");
    }
  }

  @Test
  void summaryCarriesResultCountsAndReplay() throws Exception {
    final Ledger ledger = new Ledger(1);
    ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.summary(new ChaosResult(ResultKind.SAFETY, "I1 (SAFETY): 1 acknowledged writes are missing", 3, List.of()),
          ledger, 2);
    }
    final JSONObject summary = new JSONObject(Files.readString(dir.resolve("summary.json")));
    assertThat(summary.getString("result")).isEqualTo("SAFETY");
    assertThat(summary.getLong("seed")).isEqualTo(42L);
    assertThat(summary.getInt("steps")).isEqualTo(3);
    assertThat(summary.getJSONObject("ledger").getLong("acked")).isEqualTo(1L);
    assertThat(summary.getLong("lateCommits")).isEqualTo(2L);
    assertThat(summary.getString("replay")).contains("-Dchaos.seed=42");
  }

  @Test
  void ledgerDiffListsKeysPerViolation() throws Exception {
    try (final ChaosReport report = new ChaosReport(dir, config)) {
      report.ledgerDiff(List.of(new Violation(ResultKind.SAFETY, "I1", "1 acknowledged writes are missing",
          new long[] { Ledger.key(0, 3) })));
    }
    final String diff = Files.readString(dir.resolve("ledger-diff.txt"));
    assertThat(diff).contains("== I1 (SAFETY): 1 acknowledged writes are missing").contains("w0-3");
  }
}
```

- [ ] **Step 8.2: Run it to verify it fails**

Run: `RUN ChaosReportIT` - Expected: compilation failure (`ChaosReport`, `StepRecord`, `TrendRow`, `ChaosResult` missing).

- [ ] **Step 8.3: Implement**

`ChaosResult.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.List;

public record ChaosResult(ResultKind kind, String message, int steps, List<Violation> violations) {
}
```

`StepRecord.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * One line of {@code steps.log}. {@code ackedDuringHold} is -1 for faults that do not expect writes to stay available.
 */
public record StepRecord(int step, String fault, String targets, int leaderBefore, int leaderAfter, long timeToLeaderMs,
                         long convergenceMs, long ackedDuringHold, long acked, long unknown, long failed) {
  public String toLine() {
    return "step=" + step + " fault=" + fault + " targets=" + targets + " leader=" + leaderBefore + "->" + leaderAfter
        + " timeToLeaderMs=" + timeToLeaderMs + " convergenceMs=" + convergenceMs + " ackedDuringHold=" + ackedDuringHold
        + " acked=" + acked + " unknown=" + unknown + " failed=" + failed;
  }
}
```

`TrendRow.java`:

```java
package com.arcadedb.containers.ha.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Resource sample taken after a checkpoint, one CSV line per node. -1 means the value could not be read.
 */
public record TrendRow(int step, long[] memoryBytes, long[] databaseBytes, long[] replicationBytes, double acksPerSecond,
                       long checkpointMs) {
  public static final String CSV_HEADER = "step,node,memoryBytes,databaseBytes,replicationBytes,acksPerSecond,checkpointMs";

  public List<String> toCsvLines() {
    final List<String> lines = new ArrayList<>(memoryBytes.length);
    for (int node = 0; node < memoryBytes.length; node++)
      lines.add(String.format(Locale.ROOT, "%d,%d,%d,%d,%d,%.2f,%d", step, node, memoryBytes[node], databaseBytes[node],
          replicationBytes[node], acksPerSecond, checkpointMs));
    return lines;
  }
}
```

`ChaosReport.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes the run's report under {@code target/chaos/<seed>/}. Step and trend lines are flushed as they are written,
 * so a run killed by the CI timeout still leaves everything up to the step it hung in.
 */
public final class ChaosReport implements AutoCloseable {
  private final Path           dir;
  private final ChaosConfig    config;
  private final BufferedWriter steps;
  private final BufferedWriter trends;

  public ChaosReport(final Path dir, final ChaosConfig config) throws IOException {
    Files.createDirectories(dir);
    this.dir = dir;
    this.config = config;
    this.steps = Files.newBufferedWriter(dir.resolve("steps.log"), StandardCharsets.UTF_8);
    this.trends = Files.newBufferedWriter(dir.resolve("trends.csv"), StandardCharsets.UTF_8);
    trends.write(TrendRow.CSV_HEADER);
    trends.newLine();
    trends.flush();
  }

  public Path dir() {
    return dir;
  }

  public synchronized void step(final StepRecord record) throws IOException {
    steps.write(record.toLine());
    steps.newLine();
    steps.flush();
  }

  public synchronized void trend(final TrendRow row) throws IOException {
    for (final String line : row.toCsvLines()) {
      trends.write(line);
      trends.newLine();
    }
    trends.flush();
  }

  public void ledgerDiff(final List<Violation> violations) throws IOException {
    final StringBuilder text = new StringBuilder();
    for (final Violation violation : violations) {
      text.append("== ").append(violation.describe()).append('\n');
      for (final long key : violation.keys())
        text.append(Ledger.format(key)).append('\n');
    }
    Files.writeString(dir.resolve("ledger-diff.txt"), text.toString(), StandardCharsets.UTF_8);
  }

  public void summary(final ChaosResult result, final Ledger ledger, final long lateCommits) throws IOException {
    final JSONObject counts = new JSONObject()
        .put("acked", ledger.count(Ledger.ACKED))
        .put("ackedLate", ledger.count(Ledger.ACKED_LATE))
        .put("failed", ledger.count(Ledger.FAILED))
        .put("unknown", ledger.count(Ledger.UNKNOWN))
        .put("lostUnknown", ledger.count(Ledger.LOST_UNKNOWN))
        .put("inFlight", ledger.count(Ledger.IN_FLIGHT));
    final JSONObject summary = new JSONObject()
        .put("seed", config.seed())
        .put("nodes", config.nodes())
        .put("duration", config.duration().toString())
        .put("writers", config.writers())
        .put("faults", config.faultsSpec())
        .put("result", result.kind().name())
        .put("message", result.message())
        .put("steps", result.steps())
        .put("ledger", counts)
        .put("lateCommits", lateCommits)
        .put("replay", config.replayCommand());
    Files.writeString(dir.resolve("summary.json"), summary.toString(2), StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    steps.close();
    trends.close();
  }
}
```

- [ ] **Step 8.4: Run it to verify it passes**

Run: `RUN ChaosReportIT` - Expected: `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 8.5: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{ChaosResult,StepRecord,TrendRow,ChaosReport,ChaosReportIT}.java
```

---

### Task 9: `ChaosRunner`

**Files:**
- Create: `.../chaos/ChaosRunner.java`
- Create (test support): `.../chaos/FakeLoad.java`, `.../chaos/LedgerNodeReader.java`
- Test: `.../chaos/ChaosRunnerIT.java`

**Interfaces:**
- Consumes: everything from Tasks 1-8.
- Produces: `ChaosRunner(ChaosConfig, ClusterState, NodeControl, FaultPicker, LoadGenerator, Ledger, Checkpoint, InvariantChecker, ChaosReport, TrendSource trends /* nullable */, Sleeper)`;
  `ChaosResult run()`; nested `@FunctionalInterface interface Sleeper { void sleep(Duration) throws InterruptedException; }`
  and `@FunctionalInterface interface TrendSource { TrendRow sample(int step, double acksPerSecond, long checkpointMs); }`;
  constants `WARMUP_ACKS = 20`, `MIN_AVAILABILITY_WINDOW = 25 s`.

- [ ] **Step 9.1: Write the fakes and the failing test**

`FakeLoad.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * Load generator that, while {@code progress} is set, acknowledges one new write each time {@link #acked()} is read,
 * which is how the runner observes progress.
 */
final class FakeLoad implements LoadGenerator {
  private final Ledger  ledger;
  boolean progress = true;
  int     quiesced;
  int     resumed;
  boolean closed;

  FakeLoad(final Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public void start() {
  }

  @Override
  public void quiesce() {
    ++quiesced;
  }

  @Override
  public void resume() {
    ++resumed;
  }

  @Override
  public long acked() {
    if (progress && !closed)
      ledger.record(ledger.reserve(0, false), Ledger.ACKED);
    return ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE);
  }

  @Override
  public void close() {
    closed = true;
  }
}
```

`LedgerNodeReader.java`:

```java
package com.arcadedb.containers.ha.chaos;

/**
 * A perfect cluster: every node holds exactly the acknowledged writes, except {@code dropKey}, which every node lost.
 */
final class LedgerNodeReader implements NodeReader {
  private final Ledger ledger;
  long dropKey = -1;

  LedgerNodeReader(final Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public long[] counts(final int node) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    scan(node, snapshot);
    long edges = 0;
    for (int w = 0; w < ledger.writers(); w++)
      for (int s = 0; s < ledger.size(w); s++)
        if (snapshot.hasEdge(w, s))
          ++edges;
    return new long[] { snapshot.rows(), edges };
  }

  @Override
  public void scan(final int node, final NodeSnapshot sink) {
    for (int w = 0; w < ledger.writers(); w++)
      for (int s = 0; s < ledger.size(w); s++) {
        final long key = Ledger.key(w, s);
        final byte outcome = ledger.outcome(key);
        if ((outcome == Ledger.ACKED || outcome == Ledger.ACKED_LATE) && key != dropKey)
          sink.add(key, ledger.isPair(key) ? 1 : 0);
      }
  }
}
```

`ChaosRunnerIT.java`:

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class ChaosRunnerIT {
  @TempDir
  Path dir;

  private final List<Duration> sleeps = new ArrayList<>();

  private record Harness(ChaosRunner runner, FakeNodeControl control, FakeLoad load, LedgerNodeReader reader,
                         Ledger ledger) {
  }

  private static ChaosConfig config(final String... overrides) {
    final List<String> keyValues = new ArrayList<>(List.of("chaos.seed", "7", "chaos.maxSteps", "5", "chaos.duration",
        "PT1H", "chaos.writers", "1"));
    keyValues.addAll(Arrays.asList(overrides));
    return ChaosConfig.fromProperties(ChaosConfigIT.props(keyValues.toArray(new String[0])));
  }

  private Harness harness(final ChaosConfig config) throws IOException {
    final Ledger ledger = new Ledger(config.writers());
    final FakeNodeControl control = new FakeNodeControl();
    final FakeLoad load = new FakeLoad(ledger);
    final LedgerNodeReader reader = new LedgerNodeReader(ledger);
    final InvariantChecker checker = new InvariantChecker(ledger);
    final Checkpoint checkpoint = new Checkpoint(reader, ledger, checker, config.nodes(), Duration.ofSeconds(1),
        Duration.ofMillis(1));
    final ChaosReport report = new ChaosReport(dir.resolve(Long.toString(config.seed())), config);
    final ChaosRunner runner = new ChaosRunner(config, new ClusterState(config.nodes()), control,
        new FaultPicker(config.faultWeights(), config.electionTimeout()), load, ledger, checkpoint, checker, report, null,
        sleeps::add);
    return new Harness(runner, control, load, reader, ledger);
  }

  @Test
  void healthyClusterPassesAfterMaxSteps() throws IOException {
    final Harness harness = harness(config());
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.PASS);
    assertThat(result.steps()).isEqualTo(5);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).hasSize(5);
    assertThat(new JSONObject(Files.readString(dir.resolve("7/summary.json"))).getString("result")).isEqualTo("PASS");
    assertThat(harness.load().quiesced).isEqualTo(5);
    assertThat(harness.load().resumed).isEqualTo(5);
    assertThat(harness.load().closed).isTrue();
  }

  @Test
  void sameSeedReplaysTheSameFaultSequence() throws IOException {
    final Harness first = harness(config());
    first.runner().run();
    final Harness second = harness(config());
    second.runner().run();
    assertThat(first.control().calls).isEqualTo(second.control().calls);

    final Harness other = harness(config("chaos.seed", "8"));
    other.runner().run();
    assertThat(other.control().calls).isNotEqualTo(first.control().calls);
  }

  @Test
  void noAcksDuringHoldIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    for (int i = 0; i < ChaosRunner.WARMUP_ACKS; i++)
      harness.ledger().record(harness.ledger().reserve(0, false), Ledger.ACKED);
    harness.load().progress = false;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("kill");
    assertThat(result.steps()).isEqualTo(1);
  }

  @Test
  void missingLeaderAfterHealIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    harness.control().leaderAvailable = false;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("No leader");
  }

  @Test
  void lostAcknowledgedWriteStopsTheRunAtTheFirstCheckpoint() throws IOException {
    final Harness harness = harness(config());
    harness.reader().dropKey = Ledger.key(0, 0);
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.steps()).isEqualTo(1);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("I1");
    assertThat(Files.readString(dir.resolve("7/ledger-diff.txt"))).contains("w0-0");
  }

  @Test
  void harnessErrorIsNotReportedAsAClusterBug() throws IOException {
    final Harness harness = harness(config());
    harness.control().failure = new IllegalStateException("docker daemon gone");
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.HARNESS);
    assertThat(result.message()).contains("docker daemon gone");
  }

  @Test
  void availabilityWindowIsNeverShorterThanAWriterTimeout() throws IOException {
    final Harness harness = harness(config("chaos.faults", "pause", "chaos.maxSteps", "1", "chaos.holdMin", "PT10S",
        "chaos.holdMax", "PT10S", "chaos.availabilityGrace", "PT5S"));
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    assertThat(sleeps).containsSubsequence(Duration.ofSeconds(5), ChaosRunner.MIN_AVAILABILITY_WINDOW);
  }
}
```

- [ ] **Step 9.2: Run it to verify it fails**

Run: `RUN ChaosRunnerIT` - Expected: compilation failure, `cannot find symbol ... ChaosRunner`.

- [ ] **Step 9.3: Implement `ChaosRunner`**

```java
package com.arcadedb.containers.ha.chaos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Random;

/**
 * The step loop: pick a fault, inject, hold under load (checking that acknowledged writes keep flowing when a majority
 * stays connected), heal, wait for a leader, then checkpoint with the load quiesced. The first violation ends the run.
 * Every random decision comes from one {@code Random(seed)}, so a seed replays the same fault sequence.
 */
public final class ChaosRunner {
  static final long     WARMUP_ACKS             = 20;
  /** A writer stuck on a paused node blocks for its 15 s read timeout; the window must outlast it. */
  static final Duration MIN_AVAILABILITY_WINDOW = Duration.ofSeconds(25);

  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  @FunctionalInterface
  public interface TrendSource {
    TrendRow sample(int step, double acksPerSecond, long checkpointMs);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ChaosRunner.class);

  private final ChaosConfig      config;
  private final ClusterState     state;
  private final NodeControl      control;
  private final FaultPicker      picker;
  private final LoadGenerator    load;
  private final Ledger           ledger;
  private final Checkpoint       checkpoint;
  private final InvariantChecker checker;
  private final ChaosReport      report;
  private final TrendSource      trends;
  private final Sleeper          sleeper;
  private       long             lastSampleAcked;
  private       long             lastSampleNanos;

  public ChaosRunner(final ChaosConfig config, final ClusterState state, final NodeControl control, final FaultPicker picker,
      final LoadGenerator load, final Ledger ledger, final Checkpoint checkpoint, final InvariantChecker checker,
      final ChaosReport report, final TrendSource trends, final Sleeper sleeper) {
    this.config = config;
    this.state = state;
    this.control = control;
    this.picker = picker;
    this.load = load;
    this.ledger = ledger;
    this.checkpoint = checkpoint;
    this.checker = checker;
    this.report = report;
    this.trends = trends;
    this.sleeper = sleeper;
  }

  public ChaosResult run() {
    final Random random = new Random(config.seed());
    final long deadline = System.nanoTime() + config.duration().toNanos();
    int step = 0;
    try {
      load.start();
      warmup();
      lastSampleAcked = load.acked();
      lastSampleNanos = System.nanoTime();
      while (System.nanoTime() < deadline && (config.maxSteps() == 0 || step < config.maxSteps())) {
        ++step;
        final ChaosResult failed = step(step, random);
        if (failed != null)
          return finish(failed);
        sleeper.sleep(between(random, config.calmMin(), config.calmMax()));
      }
      load.close();
      final Checkpoint.Result last = checkpoint.run();
      if (!last.violations().isEmpty())
        return finish(fromViolations(last.violations(), step));
      return finish(new ChaosResult(ResultKind.PASS, "Completed " + step + " steps", step, List.of()));
    } catch (final ChaosFailure e) {
      return finish(single(e.kind(), e.getMessage(), step));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return finish(single(ResultKind.HARNESS, "Interrupted at step " + step, step));
    } catch (final Exception e) {
      LOGGER.error("Chaos harness error at step {}", step, e);
      return finish(single(ResultKind.HARNESS, e.getClass().getSimpleName() + ": " + e.getMessage(), step));
    } finally {
      load.close();
    }
  }

  private void warmup() throws InterruptedException {
    final long until = System.nanoTime() + config.electionTimeout().toNanos();
    while (load.acked() < WARMUP_ACKS) {
      if (System.nanoTime() > until)
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "Warmup: fewer than " + WARMUP_ACKS + " writes acknowledged within " + config.electionTimeout());
      sleeper.sleep(Duration.ofMillis(500));
    }
  }

  private ChaosResult step(final int step, final Random random) throws Exception {
    final Fault fault = picker.pick(state, random);
    if (fault == null)
      throw new ChaosFailure(ResultKind.HARNESS, "No applicable fault for cluster state " + state);
    final int leaderBefore = control.findLeader();
    LOGGER.info("CHAOS step {} fault={} leader={} state={}", step, fault.name(), leaderBefore, state);

    final String targets = fault.inject(state, control, random);
    final Duration hold = between(random, config.holdMin(), config.holdMax());
    long ackedDuringHold = -1;
    if (fault.expectsWritesAvailable()) {
      sleeper.sleep(config.availabilityGrace());
      final long ackedAtGrace = load.acked();
      final Duration rest = hold.minus(config.availabilityGrace());
      sleeper.sleep(rest.compareTo(MIN_AVAILABILITY_WINDOW) < 0 ? MIN_AVAILABILITY_WINDOW : rest);
      ackedDuringHold = load.acked() - ackedAtGrace;
      if (ackedDuringHold <= 0)
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "No write acknowledged during fault '" + fault.name() + "' " + targets + " after the " + config.availabilityGrace()
                + " election grace (step " + step + ")");
    } else
      sleeper.sleep(hold);

    fault.heal(state, control);
    final long healedAt = System.nanoTime();
    if (!control.awaitLeader(config.electionTimeout()))
      throw new ChaosFailure(ResultKind.AVAILABILITY,
          "No leader within " + config.electionTimeout() + " after healing fault '" + fault.name() + "' (step " + step + ")");
    final long timeToLeaderMs = (System.nanoTime() - healedAt) / 1_000_000;
    final int leaderAfter = control.findLeader();

    load.quiesce();
    final Checkpoint.Result result;
    try {
      result = checkpoint.run();
    } finally {
      load.resume();
    }

    report.step(new StepRecord(step, fault.name(), targets, leaderBefore, leaderAfter, timeToLeaderMs,
        result.convergenceMillis(), ackedDuringHold, ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE),
        ledger.count(Ledger.UNKNOWN), ledger.count(Ledger.FAILED)));
    if (trends != null)
      report.trend(trends.sample(step, acksPerSecond(), result.durationMillis()));
    return result.violations().isEmpty() ? null : fromViolations(result.violations(), step);
  }

  private double acksPerSecond() {
    final long acked = load.acked();
    final long now = System.nanoTime();
    final double seconds = Math.max((now - lastSampleNanos) / 1e9, 1e-3);
    final double rate = (acked - lastSampleAcked) / seconds;
    lastSampleAcked = acked;
    lastSampleNanos = now;
    return rate;
  }

  private static Duration between(final Random random, final Duration min, final Duration max) {
    final long span = max.toMillis() - min.toMillis();
    return min.plusMillis(span == 0 ? 0 : (long) (random.nextDouble() * span));
  }

  private static ChaosResult single(final ResultKind kind, final String message, final int step) {
    return new ChaosResult(kind, message, step, List.of(new Violation(kind, "RUNNER", message, new long[0])));
  }

  private static ChaosResult fromViolations(final List<Violation> violations, final int step) {
    final Violation first = violations.stream().filter(v -> v.kind() == ResultKind.SAFETY).findFirst()
        .orElse(violations.getFirst());
    final String message = first.describe() + (violations.size() > 1 ? " (+" + (violations.size() - 1) + " more)" : "");
    return new ChaosResult(first.kind(), message, step, violations);
  }

  private ChaosResult finish(final ChaosResult result) {
    try {
      if (!result.violations().isEmpty())
        report.ledgerDiff(result.violations());
      report.summary(result, ledger, checker.lateCommits());
    } catch (final IOException e) {
      LOGGER.error("Could not write the chaos report to {}", report.dir(), e);
    }
    LOGGER.info("CHAOS result {} after {} steps: {} | replay: {}", result.kind(), result.steps(), result.message(),
        config.replayCommand());
    return result;
  }
}
```

- [ ] **Step 9.4: Run it to verify it passes**

Run: `RUN ChaosRunnerIT` - Expected: `Tests run: 7, Failures: 0, Errors: 0`.

- [ ] **Step 9.5: Run every harness test together**

Run: `./mvnw verify -Pintegration -pl e2e-ha -Dfailsafe.excludedGroups= -Dit.test='ChaosConfigIT,LedgerIT,InvariantCheckerIT,ChaosSchemaIT,FaultsIT,WorkloadIT,HttpNodeReaderIT,CheckpointIT,ChaosReportIT,ChaosRunnerIT'`
Expected: `Tests run: 68, Failures: 0, Errors: 0`.

- [ ] **Step 9.6: Stage**

```bash
git add e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/{ChaosRunner,FakeLoad,LedgerNodeReader,ChaosRunnerIT}.java
```

---

### Task 10: Container wiring (`ContainersTestTemplate` overload, `HaChaosIT`) and smoke runs

**Files:**
- Modify: `load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java` (`createPersistentArcadeContainer`, around line 554)
- Create: `e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/HaChaosIT.java`

**Interfaces:**
- Consumes: everything from Tasks 1-9; `ContainersTestTemplate` helpers `startCluster`, `waitForRaftLeader`,
  `waitForAllNodesKnowLeader`, `findLeaderIndex`, `disconnectFromNetwork`, `reconnectToNetwork`, `dumpContainerLogs`,
  `stopContainers`, `compareAllDatabases(String)`, fields `network`, `toxiproxyClient`, `logger`, `PASSWORD`.
- Produces: `createPersistentArcadeContainer(String name, String serverList, String quorum, Network network, String memoryOpts, long containerMemoryBytes)`;
  test `HaChaosIT#chaos`.

- [ ] **Step 10.1: Add the memory-aware overload**

In `ContainersTestTemplate.java`, replace the existing 4-argument `createPersistentArcadeContainer` with a delegate
plus a 6-argument version holding the body. Only two lines of the body change (the memory env and the host-config
modifier):

```java
  protected GenericContainer<?> createPersistentArcadeContainer(
      final String name,
      final String serverList,
      final String quorum,
      final Network network) {
    return createPersistentArcadeContainer(name, serverList, quorum, network, "-Xms2G -Xmx2G", 3L * 1024 * 1024 * 1024);
  }

  /**
   * Same as {@link #createPersistentArcadeContainer(String, String, String, Network)} with an explicit heap and
   * container memory limit, so larger clusters fit on a CI runner.
   */
  protected GenericContainer<?> createPersistentArcadeContainer(
      final String name,
      final String serverList,
      final String quorum,
      final Network network,
      final String memoryOpts,
      final long containerMemoryBytes) {
    // ... unchanged body of the former 4-argument method, except:
    //   .withEnv("ARCADEDB_OPTS_MEMORY", memoryOpts)
    //   .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(containerMemoryBytes))
  }
```

(The comment marks the two edited lines; move the rest of the existing body verbatim.)

- [ ] **Step 10.2: Reinstall the support jar and compile**

Run: `./mvnw install -DskipTests -pl load-tests -q && ./mvnw test-compile -pl e2e-ha -q`
Expected: BUILD SUCCESS. Then re-run Step 9.5 to confirm nothing moved: 68 tests, 0 failures.

- [ ] **Step 10.3: Write `HaChaosIT`**

```java
package com.arcadedb.containers.ha.chaos;

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Statistics;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Long-running chaos test: a seeded sequence of process, network and freeze faults against one Raft cluster under
 * continuous write load, with every acknowledged write checked after every step. Configured by {@code chaos.*}
 * system properties (see {@link ChaosConfig}); excluded from the nightly HA job and run by ha-chaos-tests.yml.
 * Every node's Raft and forwarding traffic goes through Toxiproxy so any node can be degraded at any step.
 */
@Tag("chaos")
class HaChaosIT extends ContainersTestTemplate {
  private static final int    RAFT_PROXY_BASE = 8660;
  private static final int    HTTP_PROXY_BASE = 8670;
  private static final String NODE_HEAP       = "-Xms1G -Xmx1G";
  private static final long   NODE_MEMORY     = 1536L * 1024 * 1024;

  @Override
  protected boolean useToxiproxy() {
    return true;
  }

  @Test
  @DisplayName("Seeded chaos run: randomized faults under write load, ledger-checked after every step")
  void chaos() throws Exception {
    final ChaosConfig config = ChaosConfig.fromProperties(System.getProperties());
    logger.info("CHAOS seed={} nodes={} duration={} faults={} | replay: {}", config.seed(), config.nodes(),
        config.duration(), config.faultsSpec(), config.replayCommand());

    final List<Proxy> raftProxies = new ArrayList<>();
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < config.nodes(); i++) {
      raftProxies.add(toxiproxyClient.createProxy("raftProxy" + i, "0.0.0.0:" + (RAFT_PROXY_BASE + i), "arcadedb-" + i + ":2434"));
      toxiproxyClient.createProxy("httpProxy" + i, "0.0.0.0:" + (HTTP_PROXY_BASE + i), "arcadedb-" + i + ":2480");
      if (i > 0)
        serverList.append(',');
      serverList.append("proxy:").append(RAFT_PROXY_BASE + i).append(':').append(HTTP_PROXY_BASE + i);
    }
    final List<GenericContainer<?>> nodes = new ArrayList<>();
    for (int i = 0; i < config.nodes(); i++)
      nodes.add(createPersistentArcadeContainer("arcadedb-" + i, serverList.toString(), "majority", network, NODE_HEAP,
          NODE_MEMORY));

    final List<ServerWrapper> servers = startCluster();
    final int leader = waitForRaftLeader(servers, 120);
    assertThat(leader).as("initial leader election").isGreaterThanOrEqualTo(0);
    waitForAllNodesKnowLeader(servers, 60);

    final DockerNodeControl control = new DockerNodeControl(nodes, raftProxies);
    createDatabaseAndSchema(servers.get(leader), control);

    final Ledger ledger = new Ledger(config.writers());
    final InvariantChecker checker = new InvariantChecker(ledger);
    final Path reportDir = Path.of("target", "chaos", Long.toString(config.seed()));
    ChaosResult result;
    try (final ChaosReport report = new ChaosReport(reportDir, config);
        final Workload workload = new Workload(config, ledger, control, ChaosSchema.DATABASE)) {
      final Checkpoint checkpoint = new Checkpoint(new HttpNodeReader(control, ChaosSchema.DATABASE), ledger, checker,
          config.nodes(), config.convergenceTimeout(), Duration.ofSeconds(1));
      final ChaosRunner runner = new ChaosRunner(config, new ClusterState(config.nodes()), control,
          new FaultPicker(config.faultWeights(), config.electionTimeout()), workload, ledger, checkpoint, checker, report,
          control, duration -> Thread.sleep(duration.toMillis()));
      result = runner.run();

      dumpContainerLogs("chaos-" + config.seed());
      if (result.kind() == ResultKind.PASS) {
        stopContainers();
        try {
          compareAllDatabases(ChaosSchema.DATABASE);
        } catch (final Throwable t) {
          final Violation violation = new Violation(ResultKind.SAFETY, "PAGE_COMPARE", String.valueOf(t.getMessage()),
              new long[0]);
          result = new ChaosResult(ResultKind.SAFETY, violation.describe(), result.steps(), List.of(violation));
          report.summary(result, ledger, checker.lateCommits());
        }
      }
    }
    assertThat(result.kind())
        .as(result.message() + " | report: " + reportDir.toAbsolutePath() + " | replay: " + config.replayCommand())
        .isEqualTo(ResultKind.PASS);
  }

  private void createDatabaseAndSchema(final ServerWrapper leader, final DockerNodeControl control) {
    final RemoteServer server = new RemoteServer(leader.host(), leader.httpPort(), "root", PASSWORD);
    server.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    Awaitility.await("database creation").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2))
        .ignoreExceptions().until(() -> {
          if (!server.exists(ChaosSchema.DATABASE))
            server.create(ChaosSchema.DATABASE);
          return true;
        });

    final RemoteDatabase database = new RemoteDatabase(leader.host(), leader.httpPort(), ChaosSchema.DATABASE, "root",
        PASSWORD);
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    try {
      for (final String ddl : ChaosSchema.DDL)
        database.command("sql", ddl);
    } finally {
      database.close();
    }

    final HttpNodeReader reader = new HttpNodeReader(control, ChaosSchema.DATABASE);
    for (int i = 0; i < control.size(); i++) {
      final int node = i;
      Awaitility.await("schema on node " + node).atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
          .ignoreExceptions().until(() -> reader.counts(node)[0] == 0);
    }
  }

  static long directorySize(final Path dir) {
    if (!Files.isDirectory(dir))
      return -1;
    try (final Stream<Path> files = Files.walk(dir)) {
      return files.filter(Files::isRegularFile).mapToLong(file -> {
        try {
          return Files.size(file);
        } catch (final IOException e) {
          return 0;
        }
      }).sum();
    } catch (final IOException | UncheckedIOException e) {
      return -1;
    }
  }

  /**
   * Docker and Toxiproxy backed {@link NodeControl}. Containers are killed, stopped, paused and restarted in place
   * through the Docker API, so their bind-mounted data survives; the host port of a restarted container is re-read
   * because Docker may assign a new one.
   */
  private final class DockerNodeControl implements NodeControl, Endpoints, ChaosRunner.TrendSource {
    private static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(120);

    private final List<GenericContainer<?>> nodes;
    private final List<Proxy>               raftProxies;
    private final Endpoint[]                endpoints;

    DockerNodeControl(final List<GenericContainer<?>> nodes, final List<Proxy> raftProxies) {
      this.nodes = nodes;
      this.raftProxies = raftProxies;
      this.endpoints = new Endpoint[nodes.size()];
      for (int i = 0; i < nodes.size(); i++)
        refreshEndpoint(i);
    }

    private DockerClient docker() {
      return DockerClientFactory.instance().client();
    }

    private String id(final int node) {
      return nodes.get(node).getContainerId();
    }

    @Override
    public int size() {
      return nodes.size();
    }

    @Override
    public synchronized Endpoint endpoint(final int node) {
      return endpoints[node];
    }

    private synchronized void refreshEndpoint(final int node) {
      final Ports.Binding[] bindings = docker().inspectContainerCmd(id(node)).exec().getNetworkSettings().getPorts()
          .getBindings().get(ExposedPort.tcp(2480));
      if (bindings == null || bindings.length == 0)
        throw new ChaosFailure(ResultKind.HARNESS, "Node " + node + " has no host binding for port 2480");
      endpoints[node] = new Endpoint(nodes.get(node).getHost(), Integer.parseInt(bindings[0].getHostPortSpec()));
    }

    @Override
    public void kill(final int node) {
      docker().killContainerCmd(id(node)).exec();
    }

    @Override
    public void stopGracefully(final int node) {
      docker().stopContainerCmd(id(node)).withTimeout(30).exec();
    }

    @Override
    public void start(final int node) throws InterruptedException {
      docker().startContainerCmd(id(node)).exec();
      refreshEndpoint(node);
      awaitHealthy(node);
    }

    private void awaitHealthy(final int node) throws InterruptedException {
      final long deadline = System.nanoTime() + HEALTH_TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        final Endpoint endpoint = endpoint(node);
        try {
          final HttpURLConnection connection = (HttpURLConnection) URI.create(
              "http://" + endpoint.host() + ":" + endpoint.port() + "/api/v1/health").toURL().openConnection();
          connection.setConnectTimeout(2_000);
          connection.setReadTimeout(2_000);
          try {
            if (connection.getResponseCode() == 204)
              return;
          } finally {
            connection.disconnect();
          }
        } catch (final IOException e) {
          // still starting
        }
        Thread.sleep(1_000);
      }
      throw new ChaosFailure(ResultKind.AVAILABILITY, "Node " + node + " not healthy within " + HEALTH_TIMEOUT + " after restart");
    }

    @Override
    public void pause(final int node) {
      docker().pauseContainerCmd(id(node)).exec();
    }

    @Override
    public void unpause(final int node) {
      docker().unpauseContainerCmd(id(node)).exec();
    }

    @Override
    public void disconnect(final int node) {
      disconnectFromNetwork(nodes.get(node));
    }

    @Override
    public void reconnect(final int node) {
      reconnectToNetwork(nodes.get(node));
    }

    @Override
    public void addLatency(final int node, final int latencyMs, final int jitterMs) throws IOException {
      raftProxies.get(node).toxics().latency("chaos-latency", ToxicDirection.DOWNSTREAM, latencyMs).setJitter(jitterMs);
    }

    @Override
    public void addLoss(final int node, final float toxicity) throws IOException {
      raftProxies.get(node).toxics().limitData("chaos-loss", ToxicDirection.DOWNSTREAM, 0).setToxicity(toxicity);
    }

    @Override
    public void clearToxics(final int node) throws IOException {
      for (final Toxic toxic : raftProxies.get(node).toxics().getAll())
        toxic.remove();
    }

    @Override
    public int findLeader() {
      return findLeaderIndex(servers());
    }

    @Override
    public boolean awaitLeader(final Duration timeout) {
      final List<ServerWrapper> servers = servers();
      if (waitForRaftLeader(servers, (int) timeout.toSeconds()) < 0)
        return false;
      waitForAllNodesKnowLeader(servers, (int) timeout.toSeconds());
      return true;
    }

    private List<ServerWrapper> servers() {
      final List<ServerWrapper> servers = new ArrayList<>(size());
      for (int i = 0; i < size(); i++) {
        final Endpoint endpoint = endpoint(i);
        servers.add(new ServerWrapper(endpoint.host(), endpoint.port(), 0));
      }
      return servers;
    }

    @Override
    public TrendRow sample(final int step, final double acksPerSecond, final long checkpointMs) {
      final int n = size();
      final long[] memory = new long[n];
      final long[] databases = new long[n];
      final long[] replication = new long[n];
      for (int i = 0; i < n; i++) {
        memory[i] = memoryUsage(i);
        databases[i] = directorySize(Path.of("target", "databases", "arcadedb-" + i));
        replication[i] = directorySize(Path.of("target", "replication", "arcadedb-" + i));
      }
      return new TrendRow(step, memory, databases, replication, acksPerSecond, checkpointMs);
    }

    private long memoryUsage(final int node) {
      final AtomicLong usage = new AtomicLong(-1);
      try (final ResultCallback.Adapter<Statistics> callback = new ResultCallback.Adapter<>() {
        @Override
        public void onNext(final Statistics statistics) {
          if (statistics.getMemoryStats() != null && statistics.getMemoryStats().getUsage() != null)
            usage.set(statistics.getMemoryStats().getUsage());
        }
      }) {
        docker().statsCmd(id(node)).withNoStream(true).exec(callback).awaitCompletion(10, TimeUnit.SECONDS);
      } catch (final Exception e) {
        logger.warn("Could not read the memory usage of node {}: {}", node, e.getMessage());
      }
      return usage.get();
    }
  }
}
```

- [ ] **Step 10.4: Compile**

Run: `./mvnw test-compile -pl e2e-ha -q` - Expected: BUILD SUCCESS. If a docker-java or toxiproxy method name differs
from the version on the classpath, fix the call to match the library (the behavior stays as described in the Javadoc).

- [ ] **Step 10.5: Build the image under test**

Run: `./mvnw clean install -Pdocker -DskipTests -q` (tags `arcadedata/arcadedb:latest` locally, about 10 minutes).
Check: `docker images arcadedata/arcadedb:latest --format '{{.CreatedSince}}'` shows a fresh image.
Before any container run, check nothing holds the ports: `lsof -nP -iTCP:2480 -sTCP:LISTEN` prints nothing.

- [ ] **Step 10.6: 3-node smoke with process faults (covers Review Focus 2)**

Run:
`./mvnw verify -Pintegration -pl e2e-ha -Dfailsafe.excludedGroups= -Dit.test=HaChaosIT -Dchaos.seed=42 -Dchaos.maxSteps=3 -Dchaos.faults=kill,stop -Dchaos.holdMin=PT10S -Dchaos.holdMax=PT15S`
Expected: `Tests run: 1, Failures: 0`; `e2e-ha/target/chaos/42/summary.json` has `"result": "PASS"`, `ledger.acked > 0`;
`steps.log` has 3 lines. A PASS with `acked > 0` also proves the HTTP scan parses real server rows: had it returned
nothing, every acknowledged key would have been reported as I1.

- [ ] **Step 10.7: 3-node smoke with freeze and toxic faults**

Run the Step 10.6 command with `-Dchaos.seed=43 -Dchaos.faults=pause,latency,loss`.
Expected: PASS, 3 steps, `trends.csv` has 1 header + 9 rows with `memoryBytes > 0`.

- [ ] **Step 10.8: 5-node smoke with network faults**

Run the Step 10.6 command with `-Dchaos.seed=44 -Dchaos.nodes=5 -Dchaos.faults=isolate,split,rolling`.
Expected: PASS, 3 steps; `steps.log` shows `split` targets of two nodes when picked.

If a smoke run fails, read `summary.json`, `steps.log` and `target/container-logs/chaos-<seed>-*.log` before touching
code: the failure may be a real cluster bug (report it, do not mask it) or a harness bug (fix it and add a unit test
in the owning task's test class that reproduces it first).

- [ ] **Step 10.9: Stage**

```bash
git add load-tests/src/test/java/com/arcadedb/test/support/ContainersTestTemplate.java \
        e2e-ha/src/test/java/com/arcadedb/containers/ha/chaos/HaChaosIT.java
```

---

### Task 11: CI workflow

**Files:**
- Create: `.github/workflows/ha-chaos-tests.yml`
- Modify: `docs/superpowers/specs/2026-09-23-ha-chaos-test-design.md` section 8 (two lines, see Step 11.3)

**Interfaces:**
- Consumes: `HaChaosIT` and the `chaos` tag; `target/chaos/<seed>/summary.json` fields `result`, `seed`, `steps`,
  `message`, `replay`.

- [ ] **Step 11.1: Write the workflow**

Action SHAs are copied from `.github/workflows/ha-resilience-tests.yml` (the repo enforces SHA-pinned actions).

```yaml
name: HA Chaos Tests

on:
  workflow_dispatch:
    inputs:
      nodes:
        description: "Cluster size"
        required: false
        default: "3"
        type: choice
        options: ["3", "5"]
      duration:
        description: "ISO-8601 run duration (e.g. PT30M, PT4H)"
        required: false
        default: "PT60M"
      seed:
        description: "Seed to replay; empty picks a random one"
        required: false
        default: ""
      faults:
        description: "Comma list with optional weights (e.g. kill:3,pause); empty enables all"
        required: false
        default: ""
      writers:
        description: "Concurrent writer threads"
        required: false
        default: "4"
      timeout_minutes:
        description: "Job timeout (hang detector); keep it at duration + 45"
        required: false
        default: "105"
  schedule:
    - cron: "0 2 * * 0" # Sundays 02:00 UTC: 3 nodes, 60 minutes, random seed

jobs:
  chaos:
    runs-on: ubuntu-latest
    timeout-minutes: ${{ fromJSON(inputs.timeout_minutes || '105') }}
    permissions:
      contents: read

    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1

      - name: Set up JDK 21
        uses: actions/setup-java@de7274f081f381c8f8158605e0321c36c376e2e6 # v6.0.1
        with:
          distribution: "temurin"
          java-version: 21

      - name: Cache local Maven repository
        uses: actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: ~/.m2/repository
          key: ${{ runner.os }}-maven-${{ hashFiles('**/pom.xml') }}
          restore-keys: |
            ${{ runner.os }}-maven-

      - name: Set up QEMU
        uses: docker/setup-qemu-action@99012661954931238ded8c8b007157a8430204e1 # v4.4.0

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@f87e5991a6d7451dcb8d9637bfbc97413f497069 # v4.4.1

      - name: Build and package with Maven Docker profile
        run: ./mvnw clean install -Pdocker -DskipTests --batch-mode --errors --show-version

      - name: Run chaos test
        env:
          NODES: ${{ inputs.nodes || '3' }}
          DURATION: ${{ inputs.duration || 'PT60M' }}
          SEED: ${{ inputs.seed }}
          FAULTS: ${{ inputs.faults }}
          WRITERS: ${{ inputs.writers || '4' }}
        run: |
          args=(-Dchaos.nodes="${NODES}" -Dchaos.duration="${DURATION}" -Dchaos.writers="${WRITERS}")
          if [ -n "${SEED}" ]; then args+=(-Dchaos.seed="${SEED}"); fi
          if [ -n "${FAULTS}" ]; then args+=(-Dchaos.faults="${FAULTS}"); fi
          ./mvnw verify -Pintegration --batch-mode --errors --show-version -pl e2e-ha \
            -Dgroups=chaos -Dfailsafe.excludedGroups= "${args[@]}"

      - name: Write run summary
        if: success() || failure()
        run: |
          found=0
          for f in e2e-ha/target/chaos/*/summary.json; do
            [ -f "$f" ] || continue
            found=1
            jq -r '"### HA chaos: \(.result)\n\n- seed: `\(.seed)`\n- nodes: \(.nodes), duration: \(.duration), steps: \(.steps)\n- message: \(.message)\n- replay: `\(.replay)`\n"' "$f" >> "$GITHUB_STEP_SUMMARY"
          done
          if [ "$found" = 0 ]; then echo "### HA chaos: no summary written (build or setup failed)" >> "$GITHUB_STEP_SUMMARY"; fi

      - name: Upload chaos report
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        if: success() || failure()
        with:
          name: ha-chaos-report
          path: |
            e2e-ha/target/chaos/
            e2e-ha/target/container-logs/
            e2e-ha/target/logs/
            e2e-ha/target/failsafe-reports/
          retention-days: 14
          if-no-files-found: warn
```

`-Dgroups=chaos` runs the harness unit tests (seconds) together with `HaChaosIT`, so a broken harness is reported
next to the run it would have invalidated.

- [ ] **Step 11.2: Validate the workflow file**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ha-chaos-tests.yml'))" && grep -c "@[0-9a-f]\{40\}" .github/workflows/ha-chaos-tests.yml`
Expected: no exception; count `7` (every `uses:` is SHA-pinned). If `actionlint` is installed, run
`actionlint .github/workflows/ha-chaos-tests.yml` and expect no output.

- [ ] **Step 11.3: Align the spec with two decisions taken while planning**

In `docs/superpowers/specs/2026-09-23-ha-chaos-test-design.md` section 8, replace:
- `Runs -Dit.test=HaChaosIT -Dfailsafe.excludedGroups= plus the chaos.* properties.` with
  `Runs -Dgroups=chaos -Dfailsafe.excludedGroups= plus the chaos.* properties, so the harness unit tests run with it.`
- `timeout-minutes = duration + 45, as hang detector.` with
  `timeout-minutes comes from a timeout_minutes input (default 105 = the 60-minute scheduled run + 45); GitHub
  expressions cannot parse an ISO-8601 duration.`

- [ ] **Step 11.4: Stage**

```bash
git add .github/workflows/ha-chaos-tests.yml docs/superpowers/specs/2026-09-23-ha-chaos-test-design.md \
        docs/superpowers/plans/2026-09-23-ha-chaos-test.md
```

---

## Done criteria

- Step 9.5: 68 harness tests green with `-Dfailsafe.excludedGroups=`; Step 1.6: zero chaos tests run without it.
- Steps 10.6-10.8: three smoke runs PASS (3 nodes process faults, 3 nodes freeze/toxic, 5 nodes network faults).
- `git status` shows only the staged files listed in the tasks; nothing committed.
