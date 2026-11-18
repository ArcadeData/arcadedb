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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle of the background graph rebuild. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorRebuildLifecycleTest {
  /**
   * Regression for #6067: deferred close rebuild.
   * <p>
   * Regression test for issue #6067: {@code flush()}'s rebuild-before-close condition (triggered whenever
   * {@code graphState == MUTABLE}, i.e. any write since the last graph build) always calls
   * {@code buildGraphFromScratch()} SYNCHRONOUSLY on the closing thread, with no size-based deferral - unlike the
   * search path's {@code rebuildGraphBeforeSearch()}, which already runs an async rebuild above 1000 vectors
   * ({@code ASYNC_REBUILD_MIN_GRAPH_SIZE}).
   * <p>
   * Measured on the issue (comment from 2026-08-23): a single write to a 200,000-vector index made {@code close()}
   * block for ~43 seconds - the same cost whether one vector or a thousand were written, because the rebuild is
   * always a full one. The fix defers the close-time rebuild to the next search on the index once the index is at
   * or above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}, reusing {@code ensureGraphAvailable()}'s existing
   * stale-persisted-graph detection (already exercised on every ordinary reopen) rather than paying the full cost
   * synchronously inside {@code close()}.
   * <p>
   * Two things are pinned here, because either alone would pass against a half-fix: {@code close()} itself must
   * return promptly (not merely "eventually", which a half-fixed async-but-still-blocking-close variant could also
   * satisfy), and no data may be lost - a later reopen and search must still find every vector, including the one
   * written after the last full build, once the deferred rebuild actually runs.
   * <p>
   * Same per-vector build parameters as {@code LSMVectorIndexCloseCancelsInFlightBuildTest} (128 dimensions,
   * {@code maxConnections=64}, {@code beamWidth=400}), scaled up to 20,000 vectors so a full rebuild reliably takes
   * multiple real seconds - long enough that the tripwire bound below cannot pass by accident.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class DeferredCloseRebuild {
    private static final String DB_ROOT         = "target/test-databases/Issue6067DeferredCloseRebuildTest";
    private static final int    DIMENSIONS      = 128;
    private static final int    NUM_VECTORS     = 20_000;
    private static final int    MAX_CONNECTIONS = 64;
    private static final int    BEAM_WIDTH      = 400;

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

    @Test
    void closeDefersTheRebuildForALargeMutatedIndexInsteadOfBlockingOnAFullRebuild() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // Force one complete, persisted build so the index starts IMMUTABLE rather than LOADING - isolates the
          // case under test (a single write after an already-built graph) from the separate first-ever-build case.
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("graphState")).as("precondition: graph must be built and IMMUTABLE first")
              .isEqualTo(1L); // GraphState.IMMUTABLE

          // One more write - mirrors the issue's own measurement that cost does not scale with what was written.
          db.transaction(
              () -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", extraVector()).save());

          assertThat(index.getStats().get("graphState"))
              .as("precondition: MUTABLE, otherwise flush()'s rebuild-before-close condition is never reached")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: above the async-rebuild threshold, otherwise close() takes the pre-existing "
                  + "small-graph synchronous path unaffected by this fix")
              .isGreaterThanOrEqualTo(1000L);

          final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
          db.close();
          // Measured on this fixture: ~50ms deferred vs. ~2.4s for a full synchronous rebuild - the bound below
          // is a tripwire between the two (see StallAwareStopwatch), with wide margin on both sides.
          stopwatch.assertGaveUpWithin(1_000,
              "a close() that defers the graph rebuild from one that runs a full synchronous rebuild to completion");
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Reopen and verify nothing was lost: the deferred rebuild must still make every vector - including the one
      // written after the last full build - reachable by search once it actually runs, proving the fix trades
      // promptness for a lazily-paid rebuild, not for correctness.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo((long) (NUM_VECTORS + 1));
          }

          final LSMVectorIndex index = vectorIndex(db);
          final var results = index.findNeighborsFromVector(extraVector(), 5, 64);
          assertThat(results)
              .as("the deferred rebuild must still make the vector written right before close() reachable by "
                  + "search after reopen")
              .hasSize(5);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Sibling of the test above, covering the OTHER arm of the same {@code needsBuild} condition:
     * {@code graphState == LOADING && !graphAlreadyOnDisk} (a graph never built at all, as opposed to one rebuilt
     * after an existing build). Both arms reach the identical deferral branch in {@code flush()} and the identical
     * fallback in {@code ensureGraphAvailable()}, but only the rebuilt-index arm was covered above.
     * <p>
     * {@code graphState} starts as {@code LOADING} on every constructed instance (both "new index" and "load
     * existing index"), but a fresh insert into a never-built index flips it straight to {@code MUTABLE} - so a
     * single populate-then-close session actually exercises the MUTABLE arm, same as the test above, not this one.
     * Reaching {@code LOADING} at {@code flush()} time needs a session that touches the index NEITHER by writing
     * NOR by searching: reopen a large index that a previous session already left un-built (deferred by the fix
     * under test), and close it again immediately - {@code graphState} then stays exactly what the constructor set
     * it to.
     * <p>
     * Unlike the test above, this one is NOT a fails-before/passes-after timing pin: the pre-fix code always builds
     * and persists a graph on every close that has one to do, so the very state this test needs (a large index,
     * closed twice, with no persisted graph either time) is only reachable once the fix exists - pre-fix, the
     * first close already does the build, and the second close then finds nothing left to do and is trivially
     * fast for an unrelated reason. So instead of timing, this pins {@code graphRebuildCount} directly: zero
     * rebuilds across both closes, exactly one once the deferred data is finally searched.
     */
    @Test
    void closeDefersTheFirstEverBuildForALargeIndexToo() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          // Closes with graphState == MUTABLE (see javadoc above) - the fix under test defers this build too, so
          // no graph is persisted yet. That is the starting condition this test actually needs, not its subject.
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);

          assertThat(index.getStats().get("graphState"))
              .as("precondition: LOADING - reopening alone (with no write or search in this session) must not "
                  + "advance graphState, otherwise this does not exercise the "
                  + "graphState == LOADING && !graphAlreadyOnDisk arm")
              .isEqualTo(0L); // GraphState.LOADING
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: above the async-rebuild threshold, otherwise close() takes the pre-existing "
                  + "small-graph synchronous path unaffected by this fix")
              .isGreaterThanOrEqualTo(1000L);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("precondition: the previous session's close must not have built anything either, or this test "
                  + "is not exercising the LOADING && !graphAlreadyOnDisk arm at all")
              .isZero();

          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("neither close() above must have built anything: this pins the actual behaviour under test, "
                  + "since a timing bound alone cannot distinguish 'deferred' from 'nothing was ever built', see "
                  + "the class javadoc")
              .isZero();

          try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo((long) NUM_VECTORS);
          }

          final var results = index.findNeighborsFromVector(randomVector(new Random(7)), 5, 64);
          assertThat(results)
              .as("the deferred first-ever build must still make every vector reachable by search after reopen")
              .hasSize(5);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the search above must be what finally triggers the deferred build - it was owed since the "
                  + "first close two sessions ago")
              .isEqualTo(1L);
        } finally {
          db.drop();
        }
      }
    }

    private static float[] extraVector() {
      return randomVector(new Random(5));
    }

    private static void populate(final Database db) {
      final Random rnd = new Random(7);
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
            + ", \"beamWidth\": " + BEAM_WIDTH + " }");
      });

      db.begin();
      for (int i = 0; i < NUM_VECTORS; i++) {
        db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
        if (i % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #6496: inactivity rebuild threshold.
   * <p>
   * Issue #6496: the inactivity rebuild timer used to arm and fire on {@code pending > 0} alone, with no regard for
   * the mutation-driven rebuild threshold that {@link LSMVectorIndex#rebuildGraphBeforeSearch()} already honours. A
   * single insert into a large, settled {@code LSM_VECTOR} index therefore cost a full O(N) graph rebuild on the
   * next quiet period - repeatedly, for every trickle insert - even though the very same single mutation against
   * the same threshold would not have triggered anything on the search path.
   * <p>
   * The fix ({@code inactivityRebuildIsWorthIt()}) must apply that gate only where a rebuild is actually expensive:
   * once the graph has grown past {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000 vectors). Below that size a rebuild is
   * cheap and {@code rebuildGraphBeforeSearch()} itself performs it unconditionally, so gating the timer there too
   * would only delay a rebuild that was never a performance problem, leaving pending vectors sitting in the
   * linear-scan delta buffer for no benefit - this is covered separately by
   * {@code LSMVectorIndexRebuildTest.deltaBufferShouldFlushAfterInactivityTimeout} and friends, which pin the
   * small-graph "any pending mutation flushes promptly" behaviour and must keep passing unmodified.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class InactivityRebuildThreshold extends TestHelper {

    private static final int EMBEDDING_DIM = 32;
    // Must be >= LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so the timer treats this as a "large" graph.
    private static final int LARGE_INDEX_VECTORS = 1100;

    private static final Duration REBUILD_SETTLE_TIMEOUT =
        Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

    /**
     * The scenario measured in issue #6496: a settled, large index takes a single insert, then goes quiet. The
     * inactivity timer must NOT rebuild the whole graph for one mutation against a threshold of 100 - it must wait
     * until enough mutations (the threshold-derived floor) have accumulated.
     */
    @Test
    void singleInsertIntoLargeSettledIndexMustNotTriggerFullRebuildOnInactivity() throws Exception {
      final int threshold = 100;
      final int timeoutMs = 500;
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
      // Pin the absolute threshold: disable graph-size scaling so the floor is deterministic (threshold/10 = 10).
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

      database.transaction(() -> {
        database.getSchema().createVertexType("Embedding");
        database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", """
            CREATE INDEX ON Embedding (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN"
            }""".formatted(EMBEDDING_DIM));
      });

      final Random random = new Random(42);

      // Settle the index: insert enough vectors to cross ASYNC_REBUILD_MIN_GRAPH_SIZE and build the graph.
      database.transaction(() -> {
        for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      final float[] queryVector = generateRandomVector(random);
      final List<Pair<RID, Float>> initialResults = lsmIndex.findNeighborsFromVector(queryVector, 10);
      assertThat(initialResults).isNotEmpty();

      Awaitility.await("initial synchronous build settles the mutation counter")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));

      // The bug: one single insert into the now-settled, large graph.
      database.transaction(() ->
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random)));

      assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
          .as("one pending mutation, well below the threshold-derived floor of %d", threshold / 10)
          .isEqualTo(1L);

      final long snapshotGenBefore = lsmIndex.getStats().get("rebuildSnapshotGeneration");

      // Wait comfortably past the inactivity timeout: the timer must fire and decline to rebuild.
      Thread.sleep(timeoutMs * 4L);

      assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
          .as("a single pending mutation against a threshold of %d must not trigger a full graph rebuild "
              + "on inactivity (issue #6496)", threshold)
          .isEqualTo(1L);
      assertThat(lsmIndex.getStats().get("rebuildSnapshotGeneration"))
          .as("no rebuild - of any kind - should have run")
          .isEqualTo(snapshotGenBefore);
    }

    /**
     * Once enough mutations accumulate to reach the threshold-derived floor, the inactivity timer must still do its
     * job and eventually flush the delta buffer - the fix must not defer rebuilds forever, only avoid the wasteful
     * single-mutation case.
     */
    @Test
    void pendingMutationsAtOrAboveTheFloorAreEventuallyRebuiltOnInactivity() throws Exception {
      final int threshold = 100;
      final int floor = threshold / 10; // 10, per inactivityRebuildIsWorthIt()'s Math.max(threshold / 10, 1)
      final int timeoutMs = 500;
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

      database.transaction(() -> {
        database.getSchema().createVertexType("Embedding");
        database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", """
            CREATE INDEX ON Embedding (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN"
            }""".formatted(EMBEDDING_DIM));
      });

      final Random random = new Random(42);

      database.transaction(() -> {
        for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      final float[] queryVector = generateRandomVector(random);
      assertThat(lsmIndex.findNeighborsFromVector(queryVector, 10)).isNotEmpty();

      Awaitility.await("initial synchronous build settles the mutation counter")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));

      // Insert exactly the floor's worth of pending mutations, then go quiet.
      database.transaction(() -> {
        for (int i = 0; i < floor; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });

      assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo((long) floor);

      Awaitility.await("the inactivity timer rebuilds once pending mutations reach the floor")
          .atMost(REBUILD_SETTLE_TIMEOUT)
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));
    }

    private float[] generateRandomVector(final Random random) {
      final float[] vector = new float[EMBEDDING_DIM];
      float norm = 0;
      for (int i = 0; i < EMBEDDING_DIM; i++) {
        vector[i] = random.nextFloat() * 2 - 1;
        norm += vector[i] * vector[i];
      }
      norm = (float) Math.sqrt(norm);
      if (norm > 0)
        for (int i = 0; i < EMBEDDING_DIM; i++)
          vector[i] /= norm;
      return vector;
    }
  }

  /**
   * Regression for #6518: close does not leak build pool.
   * <p>
   * Regression test for issue #6518: {@code LSMVectorIndex.close()} released its background resources BEFORE
   * flushing, the opposite of the order {@code LocalDatabase.closeDurableParts()} uses and documents ("an index
   * whose graceful shutdown is a graph build needs its build pool alive for it").
   * <p>
   * The inversion was not cosmetic. {@code releaseBackgroundResources()} shuts the graph-build pool down, and
   * {@code getOrCreateGraphBuildPool()} treats a shut-down pool as one to REPLACE - so the {@code flush()} that
   * followed built a brand new pool, ran the rebuild on it, and left it running, because the releasing step had
   * already happened and nothing shuts a pool down twice. Measured before the fix on this fixture: the graph was
   * persisted correctly and 17 worker threads were still alive on a closed, invalidated index. Nothing failed and
   * nothing was logged, which is what makes it the same failure issue #5418 was filed for, reached another way.
   * <p>
   * Two things are pinned here, because either alone would pass against a half-fix: the close still PERSISTS the
   * graph (so the reorder did not simply trade a leak for a lost flush), and it leaves NO live build pool.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CloseDoesNotLeakBuildPool {
    private static final String DB_ROOT     = "target/test-databases/Issue6518CloseDoesNotLeakBuildPoolTest";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 200;

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

    @Test
    void closingADirtyIndexPersistsTheGraphAndLeavesNoRunningBuildPool() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // A search makes the graph resident, then one more insert leaves pending work - so close() has a real
          // rebuild to do. Without both, flush() short-circuits and the leak never gets a chance to happen.
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(5)))
              .save());

          assertThat(index.getStats().get("graphState"))
              .as("precondition: the index must be MUTABLE, otherwise close() skips the build under test")
              .isEqualTo(2L); // GraphState.MUTABLE
          final long rebuildsBefore = index.getStats().get("graphRebuildCount");

          index.close();

          // The close still does its job: the pending vectors were absorbed into a persisted graph.
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("close() must still flush the pending vectors into the graph - the fix must not trade a leaked "
                  + "pool for a lost flush")
              .isGreaterThan(rebuildsBefore);

          // ...and it leaves nothing running. Before the fix this pool was a fresh one, built by the flush AFTER
          // the release had already shut its predecessor down, with 17 live workers and nobody left to stop them.
          final ForkJoinPool pool = graphBuildPool(index);
          if (pool != null) {
            assertThat(pool.isShutdown())
                .as("close() must not leave a running graph-build pool behind: %d worker threads outliving a "
                    + "closed index is what issue #5418 was filed for", pool.getPoolSize())
                .isTrue();
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Defence in depth for the same trap arriving from a future caller: if {@code flush()} is reached after the
     * background resources are gone, it must decline the build rather than quietly construct a pool that nothing
     * will ever shut down. Skipping costs a rebuild on the next open; building would cost the pool for the life of
     * the process.
     */
    @Test
    void flushAfterReleaseDeclinesTheBuildInsteadOfLeakingAPool() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(5)))
              .save());
          assertThat(index.getStats().get("graphState")).isEqualTo(2L); // MUTABLE

          // The inverted order, driven explicitly - this is what close() used to do.
          index.releaseBackgroundResources();
          final long rebuildsBefore = index.getStats().get("graphRebuildCount");

          index.flush();

          assertThat(index.getStats().get("graphRebuildCount"))
              .as("a flush that arrives after the release must decline the build, not run one on a pool it had to "
                  + "resurrect")
              .isEqualTo(rebuildsBefore);

          final ForkJoinPool pool = graphBuildPool(index);
          if (pool != null) {
            assertThat(pool.isShutdown())
                .as("the pool the release shut down must stay shut down: a declined build must not replace it")
                .isTrue();
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The sibling retention, from the same root cause: nulling {@code graphIndex} frees nothing while pooled
     * searchers still hold the graph they were pooled under. {@code borrow()} normally drains the pool the next
     * time it notices the identity moved, but the emptied-index path leaves no graph to search and
     * {@code findNeighborsFromVector()} returns on {@code graphIndex == null} BEFORE it borrows - so on that path
     * nothing ever drains it, and the last full graph would stay resident for the life of the index object.
     */
    @Test
    void emptyingTheIndexReleasesThePooledSearchersHoldingTheOldGraph() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // Searching populates the pool; releasing the searcher is what leaves one queued holding the graph.
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          assertThat(index.getStats().get("pooledGraphSearchers"))
              .as("precondition: a search must leave a searcher pooled, or this test proves nothing")
              .isGreaterThan(0L);

          // Delete every vector, then rebuild: the build finds nothing live and takes the graphIndex = null path.
          db.transaction(() -> db.command("sql", "DELETE FROM Doc").close());
          index.buildVectorGraphNow(null);

          assertThat(index.getGraphIndex()).as("precondition: the emptied index must have dropped its graph")
              .isNull();
          assertThat(index.getStats().get("pooledGraphSearchers"))
              .as("an index emptied of vectors must not keep its last graph alive through the searcher pool: no "
                  + "later search can drain it, because the search path returns before it borrows")
              .isZero();
        } finally {
          db.drop();
        }
      }
    }

    private static ForkJoinPool graphBuildPool(final LSMVectorIndex index) throws Exception {
      final Field field = LSMVectorIndex.class.getDeclaredField("graphBuildPool");
      field.setAccessible(true);
      return (ForkJoinPool) field.get(index);
    }

    private static void populate(final Database db) {
      final Random rnd = new Random(7);

      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\" }");
      });

      db.begin();
      for (int i = 0; i < NUM_VECTORS; i++)
        db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
      db.commit();
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }

    private static float[] queryVector() {
      return randomVector(new Random(99));
    }
  }

  /**
   * Regression for #11129: explicit build does not stall search.
   * <p>
   * Regression for issue #11129: an explicit {@link LSMVectorIndex#buildVectorGraphNow()} on an index that already had a
   * graph in memory forced the graph state back to LOADING, which {@code ensureGraphAvailable()} reads as "no graph in
   * memory yet". Every search that arrived while nothing wrote to the index then queued on {@code graphBuildLock}, which
   * the build holds from start to finish, and waited out the whole rebuild instead of answering from the resident graph.
   * <p>
   * The build is parked inside its progress callback, on the builder thread and with {@code graphBuildLock} held, while
   * a search runs on another thread. Before the fix that search could not finish until the callback returned, so the
   * bound on it is a hang detector rather than a latency bound: a search that answers from the resident graph takes
   * milliseconds, a search that waits for the build never finishes while the build is parked.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExplicitBuildDoesNotStallSearch {
    private static final String DB_PATH    = "target/test-databases/Issue11129ExplicitBuildDoesNotStallSearchTest";
    private static final int    DIMENSIONS = 16;
    // Above ASYNC_REBUILD_MIN_GRAPH_SIZE, so a write during the build does not route a search to the synchronous
    // small-graph rebuild, which waits for the build lock by design
    private static final int    COUNT      = 2_000;
    private static final int    K          = 10;
    private static final long   HANG_SECS  = 60;

    private DatabaseFactory factory;
    private Database        db;
    private LSMVectorIndex  index;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      factory = new DatabaseFactory(DB_PATH);
      db = factory.create();
      db.command("sql", "CREATE DOCUMENT TYPE V BUCKETS 1");
      db.command("sql", "CREATE PROPERTY V.vec ARRAY_OF_FLOATS");
      final Random random = new Random(7);
      db.transaction(() -> {
        for (int i = 0; i < COUNT; i++)
          db.newDocument("V").set("vec", randomVector(random)).save();
      });
      db.command("sql",
          "CREATE INDEX ON V (vec) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");
      index = (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("V[vec]")).getIndexesOnBuckets()[0];
      executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
      if (executor != null)
        executor.shutdownNow();
      if (db != null && db.isOpen())
        db.drop();
      if (factory != null)
        factory.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aSearchDuringAnExplicitRebuildAnswersFromTheResidentGraph() throws Exception {
      index.buildVectorGraphNow();
      assertThat(search(new Random(11))).as("precondition: the resident graph answers").isEqualTo(K);
      assertThat(stat("graphState")).as("precondition: a settled graph").isEqualTo(LSMVectorIndex.GraphState.IMMUTABLE.ordinal());
      final long rebuildsBefore = stat("graphRebuildCount");

      final AtomicReference<Object> outcome = new AtomicReference<>();
      index.buildVectorGraphNow(parkOnce(() -> {
        // Nothing writes during the build: the shape the issue reports as a certain stall
        outcome.set(runOffTheBuilder(() -> search(new Random(13))));
      }));

      assertThat(outcome.get()).as("a search run while the explicit build holds the build lock").isEqualTo(K);
      assertThat(stat("graphRebuildCount")).as("the explicit build still rebuilt").isEqualTo(rebuildsBefore + 1);
      assertThat(stat("graphState")).as("nothing pending after a build no write overlapped")
          .isEqualTo(LSMVectorIndex.GraphState.IMMUTABLE.ordinal());
      assertThat(search(new Random(17))).isEqualTo(K);
    }

    @Test
    void aSearchAfterAWriteDuringAnExplicitRebuildAnswersAndTheWriteStaysPending() throws Exception {
      index.buildVectorGraphNow();
      final long rebuildsBefore = stat("graphRebuildCount");

      final AtomicReference<Object> written = new AtomicReference<>();
      final AtomicReference<Object> searched = new AtomicReference<>();
      index.buildVectorGraphNow(parkOnce(() -> {
        written.set(runOffTheBuilder(() -> {
          db.transaction(() -> db.newDocument("V").set("vec", randomVector(new Random(19))).save());
          return 1;
        }));
        searched.set(runOffTheBuilder(() -> search(new Random(23))));
      }));

      assertThat(written.get()).as("a write run while the explicit build holds the build lock").isEqualTo(1);
      assertThat(searched.get()).as("a search run after that write, while the build still holds the lock").isEqualTo(K);
      assertThat(stat("graphRebuildCount")).isEqualTo(rebuildsBefore + 1);
      // The vector written during the build is past the build's snapshot: still pending, and still searchable
      assertThat(stat("graphState")).isEqualTo(LSMVectorIndex.GraphState.MUTABLE.ordinal());
      try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM V")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(COUNT + 1L);
      }
      assertThat(search(new Random(29))).isEqualTo(K);
    }

    @Test
    void anExplicitBuildOnAnIndexWithNoGraphInMemoryStillBuildsIt() {
      // A cold index (nothing resident) keeps the LOADING behaviour: searches have nothing to answer from but the build
      db.close();
      db = factory.open();
      index = (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("V[vec]")).getIndexesOnBuckets()[0];
      assertThat(index.isWarm()).as("precondition: nothing resident after a reopen").isFalse();

      index.buildVectorGraphNow();

      assertThat(index.isWarm()).isTrue();
      assertThat(stat("graphState")).isEqualTo(LSMVectorIndex.GraphState.IMMUTABLE.ordinal());
      assertThat(search(new Random(31))).isEqualTo(K);
    }

    @Test
    void aFailedExplicitBuildKeepsThePendingMutationsCounted() {
      index.buildVectorGraphNow();
      final int writes = 5;
      db.transaction(() -> {
        final Random random = new Random(41);
        for (int i = 0; i < writes; i++)
          db.newDocument("V").set("vec", randomVector(random)).save();
      });
      final long pending = stat("mutationsSinceRebuild");
      assertThat(pending).as("precondition: the writes are pending").isGreaterThanOrEqualTo(writes);
      final long rebuildsBefore = stat("graphRebuildCount");

      final AtomicReference<Throwable> failure = new AtomicReference<>();
      try {
        index.buildVectorGraphNow((phase, processed, total, accesses) -> {
          if ("validating".equals(phase))
            throw new IllegalStateException("build aborted by the test");
        });
      } catch (final RuntimeException e) {
        failure.set(e);
      }

      assertThat(failure.get()).as("the build must have failed").isNotNull();
      assertThat(stat("graphRebuildCount")).as("nothing published").isEqualTo(rebuildsBefore);
      assertThat(stat("mutationsSinceRebuild")).as("the mutations the failed build did not absorb still count")
          .isEqualTo(pending);
      assertThat(search(new Random(43))).isEqualTo(K);
    }

    @Test
    void anAbortedReplayDoesNotPutLoadingBackOverAGraphPublishedSinceIt() {
      // The replay flipped LOADING to MUTABLE before any graph was resident, a graph was published before the abort, and
      // the abort refunded every mutation: putting LOADING back would make every later search wait on graphBuildLock
      index.buildVectorGraphNow();
      db.transaction(() -> db.newDocument("V").set("vec", randomVector(new Random(37))).save());
      assertThat(stat("graphState")).as("precondition: a write promoted the state")
          .isEqualTo(LSMVectorIndex.GraphState.MUTABLE.ordinal());

      final VectorIndexReplayUndo undo = new VectorIndexReplayUndo(index, null);
      undo.recordGraphFlip(LSMVectorIndex.GraphState.LOADING);
      undo.mutationsCharged = Integer.MAX_VALUE; // floored at zero by the refund: nothing left pending
      index.undoReplay(undo);

      assertThat(index.isWarm()).isTrue();
      assertThat(stat("graphState")).isEqualTo(LSMVectorIndex.GraphState.IMMUTABLE.ordinal());
    }

    /**
     * Runs {@code whileParked} once, on the builder thread, at the build's final validation report: the build holds
     * {@code graphBuildLock} there and has not published anything yet.
     */
    private static LSMVectorIndex.GraphBuildCallback parkOnce(final Runnable whileParked) {
      final AtomicInteger calls = new AtomicInteger();
      return (phase, processed, total, accesses) -> {
        if ("validating".equals(phase) && processed == total && calls.getAndIncrement() == 0)
          whileParked.run();
      };
    }

    /**
     * @return what the task returned, or the exception it failed with ({@link TimeoutException} if it did not finish while
     * the build was parked)
     */
    private Object runOffTheBuilder(final Callable<Object> task) {
      final Future<Object> future = executor.submit(task);
      try {
        return future.get(HANG_SECS, TimeUnit.SECONDS);
      } catch (final Exception e) {
        return e;
      }
    }

    private int search(final Random random) {
      int rows = 0;
      try (final ResultSet rs = db.query("sql", "SELECT FROM (SELECT expand(vectorNeighbors('V[vec]', ?, " + K + ")))",
          (Object) randomVector(random))) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      return rows;
    }

    private long stat(final String name) {
      return ((Number) index.getStats().get(name)).longValue();
    }

    private static float[] randomVector(final Random random) {
      final float[] v = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        v[d] = random.nextFloat() * 2 - 1;
      return v;
    }
  }

  /**
   * Regression for #6657: recheck after a cancelled build.
   * <p>
   * Regression test for the gap review found in issue #6657's own fix: {@code markCloseTimeRebuildDeferred()} skips
   * the manifest write when {@code graphBuildLock.tryLock()} fails, reasoning that a build holding the lock is
   * "about to run its own completion write, which clears this same flag on its own". That is only true if the build
   * actually COMPLETES. When {@code close()} instead CANCELS an in-flight async rebuild -
   * {@code releaseBackgroundResources()} does exactly that, and runs right after {@code flush()} - the cancelled
   * build's {@code CancellationException} path never touches the manifest, so without a backstop the flag would be
   * left wherever it was before this close (typically {@code false}), even though a rebuild is now genuinely owed:
   * a false negative in the exact "why is my first query after reopen slow" moment the stat exists to answer.
   * <p>
   * Deterministic rather than racing a real JVector build's timing (which {@code LSMVectorIndexCloseCancelsInFlightBuildTest}
   * already shows is a legitimate but non-trivial thing to pin reliably): a helper thread holds
   * {@code graphBuildLock} directly, standing in for "a build is genuinely in progress", and releases it only after
   * {@code flush()} has already observed it contended - simulating the point at which a real cancellation would have
   * freed the lock without ever writing the manifest. The test then checks that the SECOND attempt, inside
   * {@code releaseBackgroundResources()}, catches what the first one had to skip.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class CancelledBuildRecheck {
    private static final String DB_ROOT     = "target/test-databases/CancelledBuildRecheck";
    private static final int    DIMENSIONS  = 8;
    private static final int    NUM_VECTORS = 1_005; // at/above ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)

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

    @Test
    void releaseBackgroundResourcesRecordsTheDeferralThatFlushHadToSkipBecauseABuildHeldTheLock() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          final Random rnd = new Random(13);
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++) {
            db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
            if (i % 500 == 499) {
              db.commit();
              db.begin();
            }
          }
          db.commit();

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphState"))
              .as("precondition: MUTABLE, otherwise flush() never reaches the deferral branch at all")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: at/above the async-rebuild threshold, the exact branch the reviewed race is about")
              .isGreaterThanOrEqualTo(1000L);

          final ReentrantLock graphBuildLock = graphBuildLock(index);
          final CountDownLatch lockHeld = new CountDownLatch(1);
          final CountDownLatch releaseLock = new CountDownLatch(1);
          final Thread lockHolder = new Thread(() -> {
            graphBuildLock.lock();
            try {
              lockHeld.countDown();
              releaseLock.await();
            } catch (final InterruptedException ignored) {
              // test teardown path only
            } finally {
              graphBuildLock.unlock();
            }
          }, "test-graph-build-lock-holder");
          lockHolder.setDaemon(true);
          lockHolder.start();

          assertThat(lockHeld.await(5, TimeUnit.SECONDS))
              .as("the helper thread must genuinely hold graphBuildLock before flush() runs, otherwise this test "
                  + "does not exercise the contended tryLock() at all")
              .isTrue();

          // flush()'s ASYNC_REBUILD_MIN_GRAPH_SIZE branch: needsBuild is true, but markCloseTimeRebuildDeferred()'s
          // tryLock() must fail while the helper thread holds the lock - standing in for a real async rebuild still
          // running at this exact point in close().
          index.flush();
          assertThat(index.getStats().get("closeTimeRebuildPending"))
              .as("flush() must have skipped the write while the lock was held - this is the state a cancelled "
                  + "build would otherwise leave behind uncorrected")
              .isEqualTo(0L);

          // Stand-in for cancellation freeing the lock without ever writing the manifest (a real CancellationException
          // path does not call writeGraphManifest()/markGraphManifestUnusable() either).
          releaseLock.countDown();
          lockHolder.join(5_000);
          assertThat(lockHolder.isAlive()).as("the helper thread must have released the lock before proceeding").isFalse();

          // This is the call under test: its tail recheck must catch what flush() had to skip, now that the lock is
          // free and graphState still says a build is genuinely owed.
          index.releaseBackgroundResources();

          assertThat(index.getStats().get("closeTimeRebuildPending"))
              .as("releaseBackgroundResources()'s recheck must record the deferral flush() could not - this is the "
                  + "false-negative the review comment identified")
              .isEqualTo(1L);
        } finally {
          if (db.isOpen())
            db.drop();
        }
      }
    }

    private static ReentrantLock graphBuildLock(final LSMVectorIndex index) throws ReflectiveOperationException {
      final Field field = LSMVectorIndex.class.getDeclaredField("graphBuildLock");
      field.setAccessible(true);
      return (ReentrantLock) field.get(index);
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #6657: getStats() reports a rebuild deferred at close.
   * <p>
   * Regression test for issue #6657: {@code getStats()} did not say whether the last {@code close()} deferred a
   * graph rebuild (issue #6067/#6653) - an operator debugging "why is my first query after reopen slow" had to go
   * looking for the {@code FINE} log line {@code LSMVectorIndex.flush()} emits, rather than checking a stat
   * alongside the existing {@code graphState}/{@code graphRebuildCount}/{@code mutationsSinceRebuild} entries.
   * <p>
   * Three things are pinned, because a half-fix could satisfy any two on its own: the {@code closeTimeRebuildPending}
   * stat must turn on exactly when {@code close()} defers a rebuild, it must still read {@code 1} on a <b>freshly
   * reopened</b> index - i.e. a brand-new {@link LSMVectorIndex} instance, not the one that set it - before that
   * index has been searched (the whole point: answering the question BEFORE the next search silently pays for it),
   * and it must clear back to {@code 0} once the deferred rebuild that search triggers actually completes.
   * <p>
   * A normal close that never deferred anything (below the async-rebuild threshold) is also pinned at {@code 0}, to
   * catch a stat that reads "true" unconditionally.
   * <p>
   * The search that pays for the deferral does not itself run the rebuild synchronously: the fixture's reopened
   * state - a persisted graph short by exactly one vector, both above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} - is
   * exactly what issue #6655's stale-prefix-reuse fix ({@link LSMVectorIndex#ensureGraphAvailable()}) now reuses
   * as a verified prefix instead of rebuilding, so the search answers immediately from the reused graph plus the
   * queued gap vector and only kicks the rebuild asynchronously. The stat therefore has to be awaited, not read
   * the instant the search returns.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class CloseTimeRebuildPendingStat {
    private static final String DB_ROOT         = "target/test-databases/CloseTimeRebuildPendingStat";
    private static final int    DIMENSIONS      = 16;
    private static final int    NUM_VECTORS     = 1_005; // just above ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)
    private static final int    MAX_CONNECTIONS = 8;
    private static final int    BEAM_WIDTH      = 32;

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

    @Test
    void closeTimeRebuildPendingTurnsOnAtDeferralSurvivesReopenAndClearsOnceTheDeferredRebuildRuns() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // Force one complete, persisted build so the index starts IMMUTABLE - isolates the deferral this test is
          // about (a write after an already-built graph) from the separate first-ever-build arm.
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("closeTimeRebuildPending"))
              .as("a graph that has just been built cleanly must not report a pending close-time deferral")
              .isEqualTo(0L);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // One more write on top of the already-built graph - the exact shape flush()'s ASYNC_REBUILD_MIN_GRAPH_SIZE
      // branch defers instead of rebuilding synchronously.
      final LSMVectorIndex indexAtDeferralTime;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          indexAtDeferralTime = vectorIndex(db);
          db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", extraVector()).save());

          assertThat(indexAtDeferralTime.getStats().get("graphState"))
              .as("precondition: MUTABLE, otherwise flush()'s deferral branch is never reached")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(indexAtDeferralTime.getStats().get("totalVectors"))
              .as("precondition: at or above the async-rebuild threshold, otherwise close() rebuilds synchronously "
                  + "and never sets the stat at all")
              .isGreaterThanOrEqualTo(1000L);
          assertThat(indexAtDeferralTime.getStats().get("closeTimeRebuildPending"))
              .as("precondition: not yet deferred - nothing has closed yet this session")
              .isEqualTo(0L);

          db.close();

          // Same object reference the deferring close() ran on: confirms flush() itself set the stat, before even
          // checking whether it is observable on a later, different instance (asserted below).
          assertThat(indexAtDeferralTime.getStats().get("closeTimeRebuildPending"))
              .as("close() deferred a rebuild above the threshold - the stat must say so immediately")
              .isEqualTo(1L);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Reopen: a brand-new LSMVectorIndex instance is constructed here (LSMVectorIndex.open() factory), with none
      // of indexAtDeferralTime's in-memory state. If the stat were a plain instance field it would read 0 here no
      // matter what the previous session did - this is the assertion that rules that design out.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex reopenedIndex = vectorIndex(db);
          assertThat(reopenedIndex).as("reopen must hand back a different Java instance").isNotSameAs(indexAtDeferralTime);

          // This test's gap is ONE vector, and since issue #7183 the search that reuses the persisted graph as a
          // prefix hands that gap to a rebuild only when it crosses the ordinary rebuild threshold - rebuilding all
          // 1,006 nodes for one queued vector is precisely what the threshold exists to refuse, and doing it anyway
          // meant close() then waited on the rebuild it had just started, cancelled it, and the next session found the
          // same vector queued and repeated the whole thing. So the debt this stat records is paid by the next search
          // only when a rebuild is actually warranted, which is what a threshold of one mutation says here.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          assertThat(reopenedIndex.getStats().get("graphState"))
              .as("precondition: LOADING - nothing in this session has searched or written yet")
              .isEqualTo(0L); // GraphState.LOADING
          assertThat(reopenedIndex.getStats().get("closeTimeRebuildPending"))
              .as("the deferral from the previous session's close() must still read 1 on this fresh instance, "
                  + "before this session's first search - this is the actual "
                  + "\"why is my first query after reopen slow\" moment the issue is about")
              .isEqualTo(1L);
          assertThat(reopenedIndex.getStats().get("graphRebuildCount"))
              .as("precondition: nothing has rebuilt yet in this fresh instance")
              .isZero();

          // The search that finally pays for the deferral: ensureGraphAvailable() finds the persisted graph short
          // by exactly the one vector written above - a stale-prefix-reuse candidate under issue #6655's fix - so
          // it answers immediately from the reused graph plus the queued gap vector and only kicks the rebuild
          // asynchronously (reuseStalePrefixGraph() -> startAsyncGraphRebuild()), rather than running it inline.
          final RID gapDocRid = ridOf(db, NUM_VECTORS);
          final var results = reopenedIndex.findNeighborsFromVector(extraVector(), 5, 64);
          assertThat(results)
              .as("the queued gap vector must already be searchable via the reused prefix graph's delta buffer, "
                  + "before the deferred rebuild it kicked off has even finished")
              .hasSize(5);
          assertThat(results.stream().map(Pair::getFirst))
              .as("the gap record must actually be found by its own embedding - a record is always its own "
                  + "nearest neighbour - not merely that the search returned five OTHER results")
              .contains(gapDocRid);

          // The async rebuild kicked off by the search above eventually folds the gap into the graph proper - the
          // same deferred rebuild the deferring close() owed, now actually paid. Both stats are awaited TOGETHER,
          // not graphRebuildCount first and closeTimeRebuildPending as a separate synchronous check right after:
          // incrementGraphRebuildCount() runs in-memory under lock.writeLock() the moment the rebuilt graph is
          // swapped in, but closeTimeRebuildPending is derived from the manifest, which writeGraphManifest() does
          // not write until AFTER the graph is persisted to disk and its transaction committed - a later point in
          // time, not an atomic one. Reading closeTimeRebuildPending synchronously right after graphRebuildCount
          // turns green would race that gap.
          Awaitility.await("the async rebuild kicked off by the deferred-rebuild search completes")
              .atMost(Duration.ofSeconds(60))
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> {
                assertThat(reopenedIndex.getStats().get("graphRebuildCount"))
                    .as("the search above must be what finally runs the deferred build")
                    .isEqualTo(1L);
                assertThat(reopenedIndex.getStats().get("closeTimeRebuildPending"))
                    .as("the deferred rebuild caught up - the stat must clear")
                    .isEqualTo(0L);
              });
        } finally {
          db.drop();
        }
      }
    }

    private static float[] extraVector() {
      return randomVector(new Random(5));
    }

    private static void populate(final Database db) {
      final Random rnd = new Random(7);
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", com.arcadedb.schema.Type.INTEGER);
        type.createProperty("vector", com.arcadedb.schema.Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
            + ", \"beamWidth\": " + BEAM_WIDTH + " }");
      });

      db.begin();
      for (int i = 0; i < NUM_VECTORS; i++) {
        db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
        if (i % 500 == 499) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }

    private static RID ridOf(final Database db, final int id) {
      try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
        assertThat(rs.hasNext()).as("the gap record must exist").isTrue();
        return rs.next().getProperty("@rid");
      }
    }
  }

  /**
   * Regression for #6657: rebuild-pending stat on the invalid-graph close path.
   * <p>
   * Companion to {@link CloseTimeRebuildPendingStat}, covering the OTHER call site of
   * {@code markCloseTimeRebuildDeferred()} inside {@code flush()}: the {@code needsBuild && !valid} branch, taken
   * when {@code flush()} runs after {@code releaseBackgroundResources()} instead of before it - its own WARNING log
   * says this "should not be reachable" through the ordinary {@code close()} path, since both close paths flush
   * BEFORE releasing. Reaching it here means calling {@code flush()}/{@code releaseBackgroundResources()} directly,
   * out of {@code close()}'s order, the same inversion the WARNING log guards against.
   * <p>
   * Small and fast on purpose - unlike the size-threshold branch, this one does not need
   * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} vectors to reach, only {@code graphState == MUTABLE}.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class InvalidPathRebuildPendingStat {
    private static final String DB_ROOT    = "target/test-databases/InvalidPathRebuildPendingStat";
    private static final int    DIMENSIONS = 8;

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

    @Test
    void flushAfterBackgroundResourcesAreReleasedStillRecordsTheDeferral() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          final Random rnd = new Random(11);
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.transaction(() -> db.newDocument("Doc").set("id", 0).set("vector", randomVector(rnd)).save());

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphState"))
              .as("precondition: MUTABLE, otherwise needsBuild is false and flush() takes neither deferral branch")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(index.getStats().get("closeTimeRebuildPending")).isEqualTo(0L);

          // The inversion the WARNING log in flush() describes: release BEFORE flush, not after. Exercised directly
          // rather than through db.close(), which always does these two in the safe order.
          index.releaseBackgroundResources();
          index.flush();

          assertThat(index.getStats().get("closeTimeRebuildPending"))
              .as("the !valid branch of flush() must record the deferral exactly like the size-threshold branch does")
              .isEqualTo(1L);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #6657: the post-cancellation recheck does not mislabel an unattempted flush.
   * <p>
   * Regression test for the mislabeling gap review round 4 found in {@code releaseBackgroundResources()}'s
   * post-cancellation recheck (added addressing round 2): the recheck used to key off {@code needsGraphBuild()}
   * alone, which is {@code true} whenever {@code graphState == MUTABLE} for ANY reason - not only "flush() skipped
   * an owed rebuild". {@code flush()}'s OTHER branch, the one that actually ATTEMPTS a synchronous build (below
   * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}), can also leave {@code graphState} at {@code MUTABLE} - a failed persist
   * ({@code markGraphManifestUnusable()} correctly writes {@code closeDeferredRebuild = false}) or new mutations
   * arriving mid-build both do. An ungated recheck would overwrite that correct {@code false} with a misleading
   * {@code true}, reporting "close chose to skip the rebuild" when what actually happened is unrelated to a skip
   * at all.
   * <p>
   * Pinned here at the level of the actual fix - the {@code flushDeferredRebuild} gate - rather than by
   * reproducing a real failed persist (already covered from a different angle by
   * {@code CompactIndexGraphPersistFailureTest}/{@code VectorBuildMemoryTest.GraphPersistOutOfMemory}): the recheck must do
   * nothing at all when {@code flush()} never ran, regardless of what {@code needsGraphBuild()} says, because
   * {@code flush()} not having run means it cannot possibly have chosen to skip anything.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class RecheckDoesNotMislabelUnattemptedFlush {
    private static final String DB_ROOT    = "target/test-databases/RecheckDoesNotMislabelUnattemptedFlush";
    private static final int    DIMENSIONS = 8;

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

    @Test
    void releaseBackgroundResourcesDoesNotMarkDeferredWhenFlushNeverRan() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          final Random rnd = new Random(17);
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.transaction(() -> db.newDocument("Doc").set("id", 0).set("vector", randomVector(rnd)).save());

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphState"))
              .as("precondition: MUTABLE - needsGraphBuild() must be true here, or this test does not exercise the "
                  + "case the old, ungated recheck would have mislabeled")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(index.getStats().get("closeTimeRebuildPending")).isEqualTo(0L);

          // flush() is deliberately never called - releaseBackgroundResources() alone, standing in for a build
          // attempt that failed (or any other reason graphState stayed MUTABLE without flush() choosing to skip).
          index.releaseBackgroundResources();

          assertThat(index.getStats().get("closeTimeRebuildPending"))
              .as("the recheck must not fire when flush() never ran - it cannot have skipped a build it never "
                  + "considered, so needsGraphBuild() alone must not be enough to mark a deferral")
              .isEqualTo(0L);
        } finally {
          if (db.isOpen())
            db.drop();
        }
      }
    }

    private static float[] randomVector(final Random rnd) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #6713: persisted-graph reuse validates vectors outside the build lock.
   * <p>
   * Regression test for issue #6713, a follow-up from #6712's code review.
   * <p>
   * {@code ensureGraphAvailable()}'s persisted-graph reuse path rebuilds {@code ordinalToVectorId} by re-validating
   * every LIVE vector - for a non-quantized (or PRODUCT-quantized) index this is one document lookup plus one
   * property read per vector - to make sure the graph on disk still describes exactly the live set (issues #3722,
   * #6106). That validation is {@code O(live vector count)} and used to run entirely under {@code lock.writeLock()},
   * the same global lock every insert and every search also needs. On an index large enough that the validation takes
   * real time, the first search after a reopen that finds a persisted graph therefore stalled every OTHER reader and
   * writer of the index for the whole scan - exactly the stall issue #5391 already moved
   * {@code buildGraphFromScratchExclusively}'s own (much larger) full-rebuild validation off of. This test targets the
   * up-to-date-reuse branch specifically: the persisted graph matches the live set exactly, so no rebuild is needed at
   * all, only the validation - the one path #5391 never touched because it predates #5391 and sits in a different
   * method.
   * <p>
   * Reproduction: a large index (600,000 vectors of 128 dimensions - vector count, not dimension, is what drives the
   * validation loop long enough to matter here, since each vector still costs one document lookup regardless of its
   * width; a higher dimension was tried first and reproduced the same locking bug, but its ~7x larger heap footprint
   * (600,000 x 1536 floats vs x 128) turned out to make the fixed-size validation workload itself GC-pressure-bound on
   * a memory-constrained CI runner - confirmed locally by reproducing the same order-of-magnitude slowdown, and
   * outright OutOfMemoryError, under a matching -Xmx2g. 128 dimensions keeps the same reproduction memory-safe) is
   * built, persisted and closed cleanly, so the persisted graph and its manifest describe exactly the live set. On
   * reopen, a background thread runs the first search, which drives {@code ensureGraphAvailable()} into the
   * validation loop. While that is in flight, the main thread inserts one more vector - an operation that only ever
   * needs {@code lock.writeLock()} briefly - and times it. Before the fix this insert would queue behind the whole
   * validation loop; after the fix it returns promptly regardless of how long the validation takes, because the loop
   * no longer holds that lock. The search thread is joined with a generous (not tight) timeout: how long the search
   * itself takes to finish is a function of hardware speed, not of what this fix changed, so only insert latency -
   * measured with {@link StallAwareStopwatch} - is asserted against a tight bound.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class EnsureGraphAvailableUnlockedValidation {
    private static final String DB_ROOT         = "target/test-databases/EnsureGraphAvailableUnlockedValidation";
    private static final int    DIMENSIONS      = 128;
    private static final int    NUM_VECTORS     = 600_000;
    private static final int    MAX_CONNECTIONS = 8;
    private static final int    BEAM_WIDTH      = 16;

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

    @Test
    void concurrentInsertIsNotBlockedByTheUnlockedPersistedGraphValidation() throws Exception {
      // Session 1: build and persist a graph over exactly NUM_VECTORS, then close cleanly - the persisted graph and
      // its manifest end up describing precisely the live set, so the next open's validation finds nothing stale.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db, 0, NUM_VECTORS);
          vectorIndex(db).buildVectorGraphNow();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Session 2: reopen. The graph is lazy-loaded on first search (GraphState.LOADING), which is what routes the
      // first search into ensureGraphAvailable()'s persisted-graph validation - the code path this issue targets.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphState")).as("precondition: reopen leaves the graph unloaded")
              .isEqualTo(0L); // GraphState.LOADING
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: large enough that the validation loop takes real time")
              .isEqualTo((long) NUM_VECTORS);

          final CountDownLatch searchStarted = new CountDownLatch(1);
          final AtomicReference<Throwable> searchFailure = new AtomicReference<>();
          final Thread searchThread = new Thread(() -> {
            searchStarted.countDown();
            try {
              index.findNeighborsFromVector(embedding(0), 5);
            } catch (final Throwable t) {
              searchFailure.set(t);
            }
          }, "issue-6713-first-search");
          // Measures the search thread's own total duration, not a fixed millisecond guess: how long an
          // O(NUM_VECTORS) validation loop takes is a function of hardware speed (a >100x gap was measured between a
          // fast workstation and a memory-constrained CI runner for the identical fixture), so the bound the
          // concurrent insert below is measured against is a FRACTION of this run's own search duration rather than
          // an absolute number - self-calibrating to whatever this machine turns out to be.
          final StallAwareStopwatch searchStopwatch = StallAwareStopwatch.start();
          searchThread.start();
          assertThat(searchStarted.await(10, TimeUnit.SECONDS)).as("the search thread must have started").isTrue();
          // A head start into ensureGraphAvailable()'s validation loop: negligible next to the loop's own total
          // duration on any hardware where this fixture is even worth running.
          Thread.sleep(50);

          // A concurrent insert only ever needs lock.writeLock() briefly. Before the fix, ensureGraphAvailable() held
          // that same lock for its ENTIRE persisted-graph validation, so this insert would queue behind the search
          // thread for as long as the validation loop over NUM_VECTORS took - effectively all of searchMs below, not
          // a small fraction of it.
          final StallAwareStopwatch insertStopwatch = StallAwareStopwatch.start();
          db.transaction(() -> db.command("sql", "INSERT INTO Doc SET id = ?, vector = ?", NUM_VECTORS,
              embedding(NUM_VECTORS)));
          final long insertMs = insertStopwatch.effectiveMs();

          // Generous, not tight: how long the search itself takes to finish is a function of hardware speed for an
          // O(NUM_VECTORS) validation loop, not of what this fix changed (the fix is about the LOCK, not the loop's
          // own duration) - a fixed tight bound here previously flaked on a slower CI runner even though nothing was
          // stuck.
          searchThread.join(TimeUnit.MINUTES.toMillis(5));
          assertThat(searchThread.isAlive()).as("the search must complete on its own, not hang").isFalse();
          assertThat(searchFailure.get()).as("the search must not have failed").isNull();
          final long searchMs = searchStopwatch.effectiveMs();

          assertThat(searchMs)
              .as("precondition: NUM_VECTORS must be large enough that the validation loop takes real time on this "
                  + "hardware, or the comparison below has no discriminating power")
              .isGreaterThanOrEqualTo(100L);

          // The self-calibrating tripwire: an insert that only ever needs the write lock briefly must take a small
          // FRACTION of however long this run's own validation search took - not scale with it the way a lock held
          // for the whole validation would. A fixed millisecond bound cannot separate the two cases across hardware
          // that differs by two orders of magnitude on the identical fixture; this ratio does, because both
          // measurements come from the same run on the same machine (PR #6720 review: the original fixed 400ms/30s
          // bounds flaked on a CI runner where the whole fixture ran ~100x slower than on a fast workstation).
          assertThat(insertMs)
              .as("an insert that only ever needs the write lock briefly, out of a search that took %,d ms on this "
                  + "hardware - not one queued behind the whole persisted-graph validation loop", searchMs)
              .isLessThan(searchMs / 3);

          // Not asserted here: graphRebuildCount. The concurrent insert above is itself a mutation, which - once
          // published - is exactly what schedules the ordinary async rebuild any mutation on a MUTABLE large graph
          // does; whether that background rebuild has completed by the time this line runs is a race against this
          // test's own write, not a signal about which branch ensureGraphAvailable() took. That branch (reuse over
          // rebuild, for a persisted graph that still matches the live set) is covered without a concurrent writer by
          // VectorGraphStalenessTest.StaleVectorGraph.anUntouchedGraphIsStillReusedAcrossARestart().
          assertThat(index.getStats().get("graphState"))
              .as("the graph must have been published (either straight to IMMUTABLE, or to MUTABLE if the concurrent "
                  + "insert above raced ahead of the publish) - never left LOADING")
              .isNotEqualTo(0L);
        } finally {
          db.drop();
        }
      }
    }

    private static void populate(final Database db, final int fromInclusive, final int toExclusive) {
      db.transaction(() -> {
        if (db.getSchema().existsType("Doc"))
          return;
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
            + ", \"beamWidth\": " + BEAM_WIDTH + " }");
      });

      db.begin();
      for (int i = fromInclusive; i < toExclusive; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if ((i - fromInclusive) % 2000 == 1999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    /** Deterministic per-id embedding, so any record's own vector can be reconstructed independently at query time. */
    private static float[] embedding(final int id) {
      final Random random = new Random(0x6713L * 31 + id);
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

  /**
   * Regression for #6798: the inactivity timer and a large but unloaded graph.
   * <p>
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
  @Nested
  @Tag("vector")
  class InactivityTimerUnloadedGraph {
    private static final String DB_ROOT     = "target/test-databases/InactivityTimerUnloadedGraph";
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

  /**
   * Regression for #6859: async rebuild reuses the persisted graph prefix.
   * <p>
   * Regression for issue #6859: a follow-up to #6798 (see {@link InactivityTimerUnloadedGraph}).
   * #6798 fixed the DECISION - the inactivity timer no longer reads a large-but-unloaded graph as small - but
   * once the threshold-derived floor is legitimately reached on such an index, the async rebuild the timer
   * starts still ran {@code buildGraphFromScratch()} over the whole live set instead of reusing the persisted
   * graph already on disk as a prefix (issues #6655, #6772) the way a SEARCH-triggered rebuild would.
   * <p>
   * Reproduces the issue's own scenario: build and persist a large graph, reopen, write past the persisted
   * graph without ever searching, and let the inactivity timer's async rebuild fire. Before the fix,
   * {@code startAsyncGraphRebuild()}'s daemon thread called {@code buildGraphFromScratch()} directly and
   * {@code stalePrefixGraphReuses} never moved - after it, the same daemon thread finds the on-disk prefix
   * eligible, publishes it, and queues only the gap before folding it in with the same
   * {@code buildGraphFromScratch()} call.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class AsyncRebuildPrefixReuse {
    private static final String DB_ROOT     = "target/test-databases/AsyncRebuildPrefixReuse";
    private static final int    DIMENSIONS  = 32;
    /** Must exceed {@code LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000), or the timer's cheap arm applies. */
    private static final int    LARGE_COUNT = 1_500;

    private static final int THRESHOLD  = 100;
    /** {@code inactivityRebuildIsWorthIt()}'s {@code Math.max(threshold / 10, 1)}. */
    private static final int FLOOR      = THRESHOLD / 10;
    // Wide enough that a stop-the-world pause between the last insert and the preconditions below cannot let the
    // timer fire first and drain the counters those assertions read (a full-suite run shares one JVM).
    private static final int TIMEOUT_MS = 5_000;

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

    @Test
    void inactivityTimerReusesThePersistedPrefixInsteadOfRebuildingFromScratch() {
      buildAndPersistFixture(LARGE_COUNT);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          configure(db);
          final LSMVectorIndex index = vectorIndex(db);

          assertThat(index.getStats().get("graphNodeCount"))
              .as("precondition: nothing has loaded the graph in this session yet")
              .isZero();
          assertThat(index.getStats().get("persistedGraphNodeCount"))
              .as("precondition: a complete graph over %d vectors is on disk", LARGE_COUNT)
              .isEqualTo((long) LARGE_COUNT);
          assertThat(index.getStats().get("stalePrefixGraphReuses"))
              .as("precondition: no reuse has happened yet")
              .isZero();

          // Write past the persisted graph, and never search - the exact shape of a loader process that goes
          // idle, and the one #6798's fix already guarantees will not skip the mutation threshold.
          for (int i = LARGE_COUNT; i < LARGE_COUNT + FLOOR; i++)
            insert(db, i, i + 1);

          assertThat(index.getStats().get("mutationsSinceRebuild")).isEqualTo((long) FLOOR);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("no search happened, so nothing but the inactivity timer can have triggered anything yet")
              .isZero();

          // The inactivity timer fires, decides a rebuild is worth it, and its async rebuild reuses the
          // persisted prefix instead of building from scratch (issue #6859) - the same reuse a search-triggered
          // rebuild would perform via ensureGraphAvailable()/reuseStalePrefixGraph().
          Awaitility.await("the inactivity timer's async rebuild reuses the persisted prefix")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("stalePrefixGraphReuses")).isPositive());

          // The reuse alone must not silently under-report the graph: the gap it queued into the delta buffer
          // has to be folded in by the same rebuild invocation for the final graph to cover every live vector.
          Awaitility.await("the queued gap is folded into a complete graph by the same rebuild")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .pollInterval(Duration.ofMillis(100))
              .untilAsserted(() -> assertThat(index.getStats().get("mutationsSinceRebuild")).isZero());

          assertThat(index.getStats().get("graphNodeCount"))
              .as("the rebuilt graph covers every live vector, prefix and gap alike")
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
      final Random random = new Random(0x6859L * 31 + id);
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

  /**
   * Regression for #7183: a mutation below the rebuild threshold does not start a full async rebuild.
   * <p>
   * The first search after a mutation below the rebuild threshold must not start a full async graph rebuild
   * (issue #7183).
   * <p>
   * Scenario, from the report: a persisted graph over N vectors; a session inserts ONE vector and closes without
   * searching; the next session searches once. {@code ensureGraphAvailable()} correctly reuses the persisted graph as
   * a stale prefix (issue #6655) and queues that one vector into the delta buffer, and then
   * {@code reuseStalePrefixGraph()} used to call {@code startAsyncGraphRebuild()} unconditionally - which logged
   * "accumulated 1 mutations, threshold: 100" and rebuilt every node in the graph anyway. {@code close()} then joined
   * that thread for up to five seconds, and because the rebuild was cancelled before it could persist, the next
   * session found the same one vector queued and did it all again. So a process that opens, searches once and exits
   * paid a full rebuild plus a five-second close, every time, for one mutation. The inactivity timer had the same
   * shape and learned to respect the threshold in #6857; this pins the search-path door.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class StalePrefixReuseRebuildThreshold {
    private static final String   DB_ROOT    = "target/test-databases/StalePrefixReuseRebuildThreshold";
    private static final int      DIMENSIONS = 32;
    private static final int      COUNT      = 1_500;
    private static final int      THRESHOLD  = 100;
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

    @Test
    void firstSearchAfterOneInsertMustNotRebuildTheWholeGraph() {
      buildAndPersistFixture();
      insertWithoutSearching(1);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          search(db);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("stalePrefixGraphReuses"))
              .as("precondition: the search reused the persisted graph as a stale prefix")
              .isEqualTo(1L);
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("precondition: exactly one mutation is pending, well under the threshold of " + THRESHOLD)
              .isEqualTo(1L);

          // The defect. One queued vector is served from the delta scan; nothing about it justifies rebuilding all
          // COUNT nodes, and the mutation threshold says so.
          assertThat(index.getStats().get("asyncRebuildInProgress"))
              .as("one mutation below the threshold must not start an async rebuild on the first search")
              .isZero();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("and must not have completed one either")
              .isZero();

          // The consequence a user sees: close() joins the rebuild thread for up to 5 s. A tripwire between a close
          // that waits on a rebuild and one that has nothing to wait on, not a latency target - so it is measured
          // with StallAwareStopwatch, which discounts a JVM-wide stop-the-world pause inside the window rather than
          // letting one turn "did close() join a rebuild thread" into a coin flip on the collector's mood (#6260).
          final StallAwareStopwatch closing = StallAwareStopwatch.start();
          db.close();
          closing.assertGaveUpWithin(4_000,
              "a close() that has no rebuild to wait on from one that joins the rebuild thread for its full 5s");
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    /**
     * The vector queued by the reuse stays searchable throughout: the point of the fix is that the delta scan serves
     * it, not that it is dropped. Without this, "no rebuild" could be passing for the wrong reason.
     */
    @Test
    void theOneQueuedVectorIsStillFoundWithoutARebuild() {
      buildAndPersistFixture();
      insertWithoutSearching(1);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          assertThat(idsNear(db, embedding(COUNT), 5))
              .as("the vector the write-only session added must come back from the delta scan")
              .contains(COUNT);
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(PendingDeltaVectors.of(index))
              .as("...which is where it is: queued, not folded into the graph")
              .isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    /**
     * Positive control: with pending mutations AT the threshold the first search is entitled to rebuild, and does.
     * Without this the assertions above would hold just as well on a build where the async path never ran at all.
     */
    @Test
    void firstSearchAtTheThresholdStillRebuilds() {
      buildAndPersistFixture();
      insertWithoutSearching(THRESHOLD);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          search(db);
          final LSMVectorIndex index = vectorIndex(db);
          Awaitility.await("a search with the threshold reached rebuilds the graph")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());
          Awaitility.await("the rebuild settles before the close")
              .atMost(REBUILD_SETTLE_TIMEOUT)
              .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private void buildAndPersistFixture() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        configure(db);
        try {
          insert(db, 0, COUNT);
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

    /** A session that writes and leaves: the shape of a batch job, or of a client that inserts and exits. */
    private void insertWithoutSearching(final int howMany) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          insert(db, COUNT, COUNT + howMany);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static void search(final Database db) {
      assertThat(idsNear(db, halfway(), 5)).as("the search returns neighbours").isNotEmpty();
    }

    private static List<Integer> idsNear(final Database db, final float[] query, final int k) {
      final StringBuilder vector = new StringBuilder();
      for (int d = 0; d < DIMENSIONS; d++)
        vector.append(d == 0 ? "" : ", ").append(query[d]);

      final List<Integer> ids = new ArrayList<>(k);
      try (final ResultSet rs = db.query("sql",
          "SELECT id FROM (SELECT expand(vectorNeighbors('Doc[vector]', [" + vector + "], " + k + ")))")) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
    }

    private static float[] halfway() {
      final float[] query = new float[DIMENSIONS];
      Arrays.fill(query, 0.5f);
      return query;
    }

    private static void configure(final Database db) {
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, THRESHOLD);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
      // Keep the inactivity timer out of the picture: this test is about the search path.
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 600_000);
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

    private static float[] embedding(final int id) {
      final Random random = new Random(0x7150L * 31 + id);
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

  /**
   * Regression for #7357: a bulk load suspends the background rebuild.
   * <p>
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
  @Nested
  @Tag("vector")
  class BulkLoadSuspendsRebuild {
    private static final String DB_ROOT    = "target/test-databases/BulkLoadSuspendsRebuild";
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

          // A node the last build could not link is re-queued into the buffer on purpose (issue #7190): it is already
          // IN the graph, so it is not pending work and no rebuild owes anything for it. Random vectors at this corpus
          // size orphan one about once in thirty runs, which read as a stranded write with a flat zero here (issue
          // #8115). What this separates is a write the race left behind from a node the build could not wire in, so
          // only the first is counted.
          final Map<String, Long> stats = index.getStats();
          assertThat(PendingDeltaVectors.of(stats))
              .as("nothing is left stranded in the buffer after eight rounds of racing the boundary "
                  + "(deltaVectorsCount=%d, unreachableGraphNodes=%d)", stats.get("deltaVectorsCount"),
                  stats.get("unreachableGraphNodes"))
              .isZero();
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The invariant a stranded suspension would break, asserted on every way out of a batch (PR #7360 review).
     * <p>
     * This is the one failure in the whole mechanism that is completely silent: an index whose suspension is never
     * lifted simply stops rebuilding - no exception, no log line, no gauge moving - until the database is reopened.
     * Pinned on all three exits a caller can actually reach: a clean close, a close whose {@code flush()} throws (a
     * duplicate on a unique edge index, the issue #4113 shape), and an {@link GraphBatch#abandon()} followed by a
     * {@code close()} that must not double-lift.
     * <p>
     * The fourth exit - an {@code Error} out of {@code flush()}, which used to escape the {@code catch
     * (RuntimeException)} and skip the restore entirely - is closed by construction rather than by this test:
     * {@code flush()} now sits inside the try whose {@code finally} restores. Injecting an {@code Error} there would
     * mean a production test hook for a path a code shape already forecloses, which is a worse trade than saying so
     * here.
     */
    @Test
    void everyWayOutOfABatchLiftsTheSuspension() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db);
          final LSMVectorIndex index = vectorIndex(db);

          assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
              .as("precondition: nothing is suspended before the first batch")
              .isZero();

          // 1. The ordinary exit.
          try (final GraphBatch batch = GraphBatch.builder(db).build()) {
            insert(db, 0, 10);
            assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
                .as("an open batch holds exactly one suspension")
                .isEqualTo(1L);
            batch.close();
          }
          assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
              .as("a clean close lifts it").isZero();

          // 2. The exit that throws. flush() fails on the duplicate, and the restore has to run anyway.
          final RID[] rids = new RID[2];
          db.transaction(() -> {
            rids[0] = db.newVertex("Doc").set("id", 900).set("vector", embedding(900)).save().getIdentity();
            rids[1] = db.newVertex("Doc").set("id", 901).set("vector", embedding(901)).save().getIdentity();
          });

          assertThatThrownBy(() -> {
            try (final GraphBatch batch = GraphBatch.builder(db).withLightEdges(false).build()) {
              batch.newEdge(rids[0], "Link", rids[1], "from_id", "a", "to_id", "b");
              batch.newEdge(rids[0], "Link", rids[1], "from_id", "a", "to_id", "b");
            }
          }).isInstanceOf(DuplicatedKeyException.class);

          assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
              .as("a batch that fails on the way out still lifts it - otherwise this index stops rebuilding "
                  + "silently until the database is reopened")
              .isZero();

          // 3. abandon() lifts it, and the close() that follows must not lift it a second time and hand away a
          // suspension this batch no longer holds.
          final GraphBatch abandoned = GraphBatch.builder(db).build();
          assertThat(index.getStats().get("backgroundMaintenanceSuspensions")).isEqualTo(1L);
          abandoned.abandon();
          assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
              .as("abandon() lifts it").isZero();
          abandoned.close();
          assertThat(index.getStats().get("backgroundMaintenanceSuspensions"))
              .as("and the close() after it is idempotent, not a second decrement")
              .isZero();

          // A suspension count of zero is only meaningful if the index still rebuilds, so end on that.
          Awaitility.await("the index is genuinely unsuspended, not merely reading zero")
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

        // An edge type carrying a unique index, so a batch can be made to fail on its way out (issue #4113 shape).
        final var edge = db.getSchema().createEdgeType("Link");
        edge.createProperty("from_id", Type.STRING);
        edge.createProperty("to_id", Type.STRING);
        db.getSchema().buildTypeIndex("Link", new String[] { "from_id", "to_id" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
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

  /**
   * Regression for #8862: search recall while the graph rebuilds in the background.
   * <p>
   * Regression for issue #8862: a search that runs while the index rebuilds its graph in the background returned poor
   * neighbors (recall@10 0.14-0.31 against 0.99 before and after). The rebuild published the NEW ordinal-to-vector-id
   * map long before the new graph, so a search walking the still-published OLD graph resolved its ordinals through a
   * map that was compacted for the new one, and scored every node against some other record's vector.
   * <p>
   * The test deletes 20% of the vectors (which crosses the rebuild threshold), samples recall@10 against an exact
   * cosine ground truth for as long as the rebuild runs, and requires every sample to stay as good as before it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class SearchDuringAsyncRebuildRecall {
    private static final String DB_PATH    = "target/test-databases/SearchDuringAsyncRebuildRecall";
    private static final int    DIMENSIONS = 32;
    private static final int    COUNT      = 20_000;
    private static final int    DELETES    = 4_000;
    private static final int    K          = 10;
    private static final int    EF_SEARCH  = 100;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void recallHoldsWhileTheAsyncRebuildRuns() {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          final Random random = new Random(5);
          final Map<RID, float[]> live = new HashMap<>();
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          });
          final List<RID> rids = new ArrayList<>(COUNT);
          db.begin();
          for (int i = 0; i < COUNT; i++) {
            final float[] v = randomVector(random);
            final RID rid = db.newDocument("Doc").set("id", i).set("vector", v).save().getIdentity();
            rids.add(rid);
            live.put(rid, v);
            if (i % 5000 == 4999) {
              db.commit();
              db.begin();
            }
          }
          db.commit();
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
          final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
              .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

          final double before = recall(index, live, live.keySet().toArray(new RID[0]), new Random(11), 20);
          assertThat(before).as("precondition: recall before the deletes").isGreaterThan(0.9);

          // 20% deleted is the default rebuild threshold, so the commit starts an async rebuild
          final long rebuildsBefore = stat(index, "graphRebuildCount");
          final List<RID> shuffled = new ArrayList<>(rids);
          Collections.shuffle(shuffled, new Random(13));
          db.begin();
          for (final RID rid : shuffled.subList(0, DELETES)) {
            db.deleteRecord(db.lookupByRID(rid, false));
            live.remove(rid);
          }
          db.commit();

          // Sample unconditionally until the rebuild has finished: every sample, before or during the rebuild, must hold
          final Random sampler = new Random(31);
          final RID[] order = live.keySet().toArray(new RID[0]);
          double worst = 1.0;
          int samples = 0;
          int samplesDuringRebuild = 0;
          final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
          boolean done = false;
          while (!done && System.nanoTime() < deadline) {
            final boolean rebuildingBefore = stat(index, "asyncRebuildInProgress") != 0L;
            worst = Math.min(worst, recall(index, live, order, sampler, 3));
            samples++;
            if (rebuildingBefore && stat(index, "asyncRebuildInProgress") != 0L)
              samplesDuringRebuild++;
            done = stat(index, "graphRebuildCount") > rebuildsBefore && stat(index, "asyncRebuildInProgress") == 0L;
          }

          assertThat(done).as("the async rebuild must have been triggered by the deletes and have completed").isTrue();
          assertThat(samplesDuringRebuild).as("samples that ran entirely inside the rebuild window (of %d)", samples)
              .isPositive();
          assertThat(worst).as("worst recall@10 sampled around the async rebuild (before: %s)", before)
              .isGreaterThan(0.7); // a 3-query sample of an approximate search varies; the bug measured 0.04-0.31
        } finally {
          if (db.isOpen())
            db.drop();
        }
      }
    }

    private static double recall(final LSMVectorIndex index, final Map<RID, float[]> live, final RID[] order, final Random random,
        final int queries) {
      double sum = 0;
      for (int q = 0; q < queries; q++) {
        final float[] query = randomVector(random);
        final List<Pair<RID, Float>> got = index.findNeighborsFromVector(query, K, EF_SEARCH);
        final double[] sims = new double[order.length];
        final Integer[] idx = new Integer[order.length];
        for (int i = 0; i < order.length; i++) {
          sims[i] = cosine(query, live.get(order[i]));
          idx[i] = i;
        }
        Arrays.sort(idx, (a, b) -> Double.compare(sims[b], sims[a]));
        final Set<RID> truth = new HashSet<>();
        for (int i = 0; i < K; i++)
          truth.add(order[idx[i]]);
        int hit = 0;
        for (final Pair<RID, Float> p : got)
          if (truth.contains(p.getFirst()))
            hit++;
        sum += hit / (double) K;
      }
      return sum / queries;
    }

    private static long stat(final LSMVectorIndex index, final String name) {
      return ((Number) index.getStats().get(name)).longValue();
    }

    private static float[] randomVector(final Random random) {
      final float[] v = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        v[d] = random.nextFloat() * 2 - 1;
      return v;
    }

    private static double cosine(final float[] a, final float[] b) {
      double dot = 0, na = 0, nb = 0;
      for (int i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
      }
      return dot / Math.sqrt(na * nb);
    }
  }
}
