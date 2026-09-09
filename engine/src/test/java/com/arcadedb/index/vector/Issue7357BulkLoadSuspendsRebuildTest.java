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
import com.arcadedb.graph.GraphBatch;
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
 * Regression test for issue #7357: an inactivity graph rebuild fired in the middle of a bulk load.
 * <p>
 * The trigger is the index going quiet for {@code inactivityRebuildTimeoutMs}, which is meant to read as "the
 * writer is done". During a bulk load it does not: a loader stalls for an LSM index compaction, a page-flush burst
 * or a long GC, and any of those outlast the window. The rebuild that starts then covers the corpus loaded SO FAR,
 * takes minutes on a large one, and is superseded by the rest of the load - so the next stall starts another over a
 * larger set. The reporter's 4.2M-record load paid {@code build(1M) + build(1.6M) + build(2.6M) + build(4.2M)} and
 * had not finished after six and a half hours; the same load with the vector index removed took twenty-six minutes.
 * <p>
 * {@link GraphBatch} already tells the engine it is a bulk load - it relaxes WAL and async durability for its
 * duration and puts them back on close. Speculative index maintenance belongs in exactly that scope, so this pins
 * that it is suspended while the batch is open and that the one rebuild the load is worth happens when it closes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// Waits out several inactivity windows and then a graph build, so most of its time goes on work
// LSMVectorIndex.REBUILD_SEMAPHORE serializes JVM-wide - see CLAUDE.md's @Tag("vector") note.
@Tag("vector")
class Issue7357BulkLoadSuspendsRebuildTest {
  private static final String DB_ROOT    = "target/test-databases/Issue7357BulkLoadSuspendsRebuildTest";
  private static final int    DIMENSIONS = 16;
  private static final int    RECORDS    = 200;
  private static final int    TIMEOUT_MS = 300;

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
   * The load stalls for several inactivity windows in the middle of the batch, which is what a compaction or a GC
   * pause looks like from inside the index. Nothing may rebuild until the batch closes.
   */
  @Test
  void aStallInsideABulkLoadMustNotTriggerARebuild() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db);
        final LSMVectorIndex index = vectorIndex(db);

        try (final GraphBatch batch = GraphBatch.builder(db).build()) {
          insert(db, 0, RECORDS);

          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("precondition: the load left work a rebuild would pick up")
              .isEqualTo((long) RECORDS);

          idleFor(TIMEOUT_MS * 6L);

          assertThat(index.getStats().get("graphRebuildCount"))
              .as("a stall inside a bulk load is not the end of the load, and must not start a graph rebuild "
                  + "over the part of the corpus loaded so far (issue #7357)")
              .isZero();
          assertThat(index.getStats().get("asyncRebuildInProgress"))
              .as("neither arm of the timer may have been taken")
              .isZero();
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("and the writes are still pending, served by the delta scan as they are between any two rebuilds")
              .isEqualTo((long) RECORDS);

          // Note the batch is still open here: close() is what lifts the suspension, and the await below is the
          // positive control that says the timer was alive all along and simply declined.
          batch.close();
        }

        Awaitility.await("closing the batch releases the one rebuild the load is worth")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());

        Awaitility.await("the rebuild settles before the drop")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());

        assertThat(index.getStats().get("graphNodeCount"))
            .as("and it covers every vector the load wrote, in one build rather than one per stall")
            .isEqualTo((long) RECORDS);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("exactly one build, not one per stall")
            .isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The control. The identical write-then-stall sequence with no batch open must rebuild during the stall, or the
   * test above would hold on a build where the inactivity timer never fired at all.
   */
  @Test
  void theSameStallOutsideABulkLoadStillRebuilds() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db);
        final LSMVectorIndex index = vectorIndex(db);

        insert(db, 0, RECORDS);

        Awaitility.await("with no bulk load open the inactivity timer rebuilds as it always did")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());

        Awaitility.await("the rebuild settles before the drop")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Suspensions are reference-counted, so a second one taken while the first is open keeps the index suspended
   * until BOTH lift. A batch that resumed on the first close would hand another loader's suspension away.
   */
  @Test
  void overlappingSuspensionsCompose() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db);
        final LSMVectorIndex index = vectorIndex(db);

        index.suspendBackgroundMaintenance();
        index.suspendBackgroundMaintenance();

        insert(db, 0, RECORDS);
        idleFor(TIMEOUT_MS * 6L);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("two suspensions are open")
            .isZero();

        index.resumeBackgroundMaintenance();
        idleFor(TIMEOUT_MS * 6L);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("one is still open")
            .isZero();

        index.resumeBackgroundMaintenance();
        Awaitility.await("the last one lifts and the rebuild runs")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());

        Awaitility.await("the rebuild settles before the drop")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The race the whole fix turns on, driven directly: the inactivity timer fires WHILE the last suspension is
   * being lifted.
   * <p>
   * {@code runInactivityRebuild()} reads the suspension count and {@code resumeBackgroundMaintenance()} decrements
   * it to zero, on two different threads, with no ordering between them. Both outcomes must be correct: the timer
   * reading a non-zero count re-arms and the resume arms too (only one task is actually scheduled, because
   * {@code armInactivityRebuild()} declines while one is armed), and the timer reading zero proceeds to rebuild
   * while the resume finds nothing left to arm. What must never happen is the pair settling into "neither armed
   * anything", which would leave the load's writes in the delta buffer with no rebuild ever coming.
   * <p>
   * Driven by hammering the boundary rather than by pinning one interleaving: with a {@value #TIMEOUT_MS} ms
   * window and a resume landing at an arbitrary point inside it, repeating the open/write/close cycle lands on
   * both sides of the read over the run. Each iteration asserts the invariant that matters - the index does not
   * end up quiet with work pending - which is the property, not one particular schedule.
   */
  @Test
  void closingABatchWhileTheTimerFiresStillLeavesARebuildComing() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db);
        final LSMVectorIndex index = vectorIndex(db);

        for (int round = 0; round < 8; round++) {
          final int from = round * 20;
          try (final GraphBatch batch = GraphBatch.builder(db).build()) {
            insert(db, from, from + 20);
            // Land the close somewhere inside the inactivity window, so across the rounds the resume falls both
            // before and after the timer thread reads the suspension count.
            idleFor(TIMEOUT_MS - 50L + round * 15L);
            batch.close();
          }

          final int written = from + 20;
          Awaitility.await("round " + round + ": the write that outlived the batch still reaches the graph")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .pollInterval(Duration.ofMillis(25))
              .untilAsserted(() -> assertThat(index.getStats().get("mutationsSinceRebuild")).isZero());

          Awaitility.await("round " + round + ": the rebuild settles before the next round")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .pollInterval(Duration.ofMillis(25))
              .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());

          assertThat(index.getStats().get("graphNodeCount"))
              .as("round %d: and it covers every vector written so far", round)
              .isEqualTo((long) written);
        }

        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("nothing is left stranded in the buffer after eight rounds of racing the boundary")
            .isZero();
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Idles for {@code effectiveMs} of RUNNING time.
   * <p>
   * Measured with {@link StallAwareStopwatch} rather than slept through, because the assertions that follow are
   * negative ones: a plain sleep returns on wall clock, and a stop-the-world pause covering most of it would leave
   * the timer no CPU to fire on, so "nothing rebuilt" would hold because nothing ran rather than because the
   * suspension declined.
   */
  private static void idleFor(final long effectiveMs) {
    final StallAwareStopwatch idle = StallAwareStopwatch.start();
    while (idle.effectiveMs() < effectiveMs) {
      try {
        Thread.sleep(25);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static void createSchema(final Database db) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, TIMEOUT_MS);
    // The mutation threshold must stay out of reach so the inactivity timer is the only trigger under test.
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 10);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

    db.transaction(() -> {
      final var type = db.getSchema().createVertexType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });
  }

  private static void insert(final Database db, final int fromInclusive, final int toExclusive) {
    db.begin();
    for (int i = fromInclusive; i < toExclusive; i++)
      db.newVertex("Doc").set("id", i).set("vector", embedding(i)).save();
    db.commit();
  }

  /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x7357L * 17 + id);
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
