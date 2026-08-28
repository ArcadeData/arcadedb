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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6798: the inactivity rebuild timer repeated the null-graph conflation that issue
 * #6772 fixed on the search path.
 * <p>
 * {@code graphIndex} is null in three distinct situations, and only two of them mean "a rebuild here is cheap":
 * no graph has ever been built, or the graph is genuinely small. The third is a large graph that is fully
 * persisted and that this session simply has not loaded, because loading is lazy on the first search - and a
 * session that ingests and then goes idle never searches, so it never loads. Both the "is a rebuild worth it"
 * gate ({@code inactivityRebuildIsWorthIt()}) and the sync/async arm choice inside the timer task read that third
 * null as the first two, so the mutation threshold was bypassed entirely and a single insert into a reopened
 * 10M-vector index rebuilt every vector, synchronously, on the timer thread.
 * <p>
 * Issue #6772's fix does not reach this path: it works by loading the persisted graph inside
 * {@code ensureGraphAvailable()}, which only a search calls.
 * <p>
 * Three behaviours are pinned, because fixing only the first is what broke three existing tests the last time
 * this gate was touched (PR #6510 for issue #6496):
 * <ul>
 *   <li>a single insert into a large-but-unloaded index must not rebuild anything at all;</li>
 *   <li>once pending mutations reach the threshold-derived floor the timer must still rebuild, so nothing is
 *       deferred forever;</li>
 *   <li>a genuinely small persisted index must keep flushing on any pending mutation, which is the case the
 *       threshold must never be applied to.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// Builds and persists a graph over a few thousand vectors and then waits on the inactivity timer, so it spends
// most of its time waiting on work LSMVectorIndex.REBUILD_SEMAPHORE serializes JVM-wide - see CLAUDE.md's
// @Tag("vector") note.
@Tag("vector")
class Issue6798InactivityTimerUnloadedGraphTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6798InactivityTimerUnloadedGraphTest";
  private static final int    DIMENSIONS  = 32;
  /** Must exceed {@code LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000), or none of this applies. */
  private static final int    LARGE_COUNT = 1_500;
  /** Must stay well under it, so the timer's cheap-rebuild arm is the one under test. */
  private static final int    SMALL_COUNT = 50;

  private static final int THRESHOLD  = 100;
  /** {@code inactivityRebuildIsWorthIt()}'s {@code Math.max(threshold / 10, 1)}. */
  private static final int FLOOR      = THRESHOLD / 10;
  private static final int TIMEOUT_MS = 500;

  private static final Duration REBUILD_SETTLE_TIMEOUT =
      Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

  private String dbPath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(dbPath));
  }

  /**
   * The scenario the issue reports: build a large index, close, reopen, write one vector, never search, go idle.
   * <p>
   * The rebuild that used to run here is the whole point - it covered every vector in the index for a single
   * pending mutation against a threshold of {@value #THRESHOLD}, which is precisely what issue #6496 established
   * must not happen on a large index. Nothing about that changes because the graph happens to be on disk rather
   * than in this session's heap.
   * <p>
   * The idle window is measured with {@link StallAwareStopwatch} rather than slept through, and is followed by a
   * positive control: the same timer on the same index must still rebuild once the pending mutations reach the
   * floor. A negative assertion after a wait is otherwise vacuous - it would hold just as well on a build where
   * the timer never ran at all.
   */
  @Test
  void oneInsertIntoALargeUnloadedIndexMustNotRebuildAnything() throws Exception {
    buildAndPersistFixture(LARGE_COUNT);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        configure(db);
        final LSMVectorIndex index = vectorIndex(db);

        assertThat(index.getStats().get("graphState"))
            .as("precondition: nothing has loaded the graph in this session yet")
            .isEqualTo(0L); // GraphState.LOADING
        assertThat(index.getStats().get("graphNodeCount"))
            .as("precondition: and no graph node is resident, though a complete graph is on disk")
            .isZero();
        assertThat(index.getStats().get("persistedGraphNodeCount"))
            .as("precondition: which the index can tell apart from having no graph at all - this is the "
                + "distinction the whole fix rests on")
            .isEqualTo((long) LARGE_COUNT);

        insert(db, LARGE_COUNT, LARGE_COUNT + 1);

        assertThat(index.getStats().get("mutationsSinceRebuild"))
            .as("precondition: exactly one pending mutation, far below the floor of %d", FLOOR)
            .isEqualTo(1L);
        assertThat(index.getStats().get("graphNodeCount"))
            .as("precondition: the write must NOT have loaded the graph - if it had, this test would be "
                + "exercising the already-fixed resident-graph path instead")
            .isZero();

        // Idle, comfortably past the inactivity timeout. Measured with StallAwareStopwatch rather than slept
        // through: a plain sleep returns on wall clock, and a stop-the-world pause covering most of it would
        // leave the timer no CPU to fire on - the assertions below would then hold because nothing ran at all,
        // not because the gate declined. effectiveMs() discounts the JVM-wide stall, so the window really did
        // contain that much running time.
        final StallAwareStopwatch idle = StallAwareStopwatch.start();
        while (idle.effectiveMs() < TIMEOUT_MS * 6L)
          Thread.sleep(50);

        assertThat(index.getStats().get("graphRebuildCount"))
            .as("a single pending mutation must not rebuild a large index just because this session has not "
                + "loaded its graph (issue #6798)")
            .isZero();
        assertThat(index.getStats().get("mutationsSinceRebuild"))
            .as("and the mutation must still be pending, waiting for the threshold like any other")
            .isEqualTo(1L);
        assertThat(index.getStats().get("asyncRebuildInProgress"))
            .as("no rebuild of either kind - the async arm must not have been taken either")
            .isZero();

        // The positive control, and the real answer to "did anything actually run in that window": top the
        // pending mutations up to the floor and the very same timer, on the very same index, must now rebuild.
        // Without this the three assertions above would still hold on a build where the inactivity timer were
        // broken outright, which is not what this test is about.
        insert(db, LARGE_COUNT + 1, LARGE_COUNT + FLOOR);
        Awaitility.await("the same timer still fires once the pending mutations reach the floor")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());

        Awaitility.await("the rebuild settles before the drop")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The other half of the same gate: reaching the floor on a large-but-unloaded index must still rebuild, or the
   * fix would trade a wasteful rebuild for a delta buffer that never drains.
   * <p>
   * Written as one transaction per vector rather than one transaction for all of them, deliberately: that drives
   * the mutation through {@code put()} - and so through {@code scheduleInactivityRebuild()}, and so through the
   * gate - once per vector instead of once per batch. It is the shape a loader process actually has, and the one
   * that makes what the gate costs per insert matter (PR #6857 review).
   */
  @Test
  void reachingTheFloorOnALargeUnloadedIndexStillRebuilds() {
    buildAndPersistFixture(LARGE_COUNT);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        configure(db);
        final LSMVectorIndex index = vectorIndex(db);

        for (int i = LARGE_COUNT; i < LARGE_COUNT + FLOOR; i++)
          insert(db, i, i + 1);

        assertThat(index.getStats().get("mutationsSinceRebuild")).isEqualTo((long) FLOOR);

        Awaitility.await("the inactivity timer rebuilds once pending mutations reach the floor")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(index.getStats().get("mutationsSinceRebuild")).isZero());

        assertThat(index.getStats().get("graphNodeCount"))
            .as("and the rebuilt graph covers every live vector")
            .isEqualTo((long) (LARGE_COUNT + FLOOR));

        Awaitility.await("the rebuild settles before the drop")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The over-correction guard. A genuinely small persisted index is still cheap to rebuild, so the timer must
   * keep flushing it on any pending mutation - applying the threshold there would only leave a handful of
   * vectors in the linear-scan delta buffer for no benefit, which is the regression PR #6510 introduced and
   * issue #6496's fix was reshaped to avoid.
   */
  @Test
  void oneInsertIntoASmallUnloadedIndexStillFlushesPromptly() {
    buildAndPersistFixture(SMALL_COUNT);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        configure(db);
        final LSMVectorIndex index = vectorIndex(db);

        assertThat(index.getStats().get("persistedGraphNodeCount"))
            .as("precondition: a small graph is on disk and unloaded, the same shape as the large case")
            .isEqualTo((long) SMALL_COUNT);

        insert(db, SMALL_COUNT, SMALL_COUNT + 1);
        assertThat(index.getStats().get("mutationsSinceRebuild")).isEqualTo(1L);

        Awaitility.await("the inactivity timer flushes a small index on any pending mutation")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> assertThat(index.getStats().get("mutationsSinceRebuild")).isZero());

        assertThat(index.getStats().get("graphNodeCount"))
            .as("and the flushed graph covers every live vector")
            .isEqualTo((long) (SMALL_COUNT + 1));
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Session 1: write {@code count} vectors, build and persist the graph, close cleanly. The reopening session
   * then finds a complete, valid persisted graph that nothing in it has loaded.
   */
  private void buildAndPersistFixture(final int count) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        insert(db, 0, count);
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("graphState"))
            .as("precondition: the graph must be built and IMMUTABLE before the close that persists it")
            .isEqualTo(1L); // GraphState.IMMUTABLE
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  /**
   * Pinned per-database rather than globally: the absolute threshold with graph-size scaling disabled, so the
   * floor is a known {@value #FLOOR}, and an inactivity window short enough to wait out.
   */
  private static void configure(final Database db) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, THRESHOLD);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, TIMEOUT_MS);
  }

  private static void insert(final Database db, final int fromInclusive, final int toExclusive) {
    db.transaction(() -> {
      if (db.getSchema().existsType("Doc"))
        return;
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });

    db.begin();
    for (int i = fromInclusive; i < toExclusive; i++) {
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      if ((i - fromInclusive) % 500 == 499) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x6798L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
