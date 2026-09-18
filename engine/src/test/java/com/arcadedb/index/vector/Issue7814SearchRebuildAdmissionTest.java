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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7814: the synchronous graph build a SEARCH falls through to must honour the JVM-wide
 * rebuild permit, the same way an async rebuild has since issue #3868.
 * <p>
 * The reported incident is what happens when it does not: a reopened database whose vector indexes have no usable
 * graph on disk runs one full, unbounded build per index, each on the request thread that first queried it, all at
 * once. Eight of them pinned a 39 GB heap at its ceiling and had the node restarted under it. Nothing in the class
 * prevented that - {@code REBUILD_SEMAPHORE} was acquired only by the background rebuild paths.
 * <p>
 * The fixture reaches that build the shortest honest way: write the vectors, then crash before anything persists a
 * graph next to them. The reopened index then has a full corpus and no graph, which is exactly the state every
 * index in that incident was in after the pods restarted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7814SearchRebuildAdmissionTest {
  private static final String DB_ROOT     = "target/test-databases/Issue7814SearchRebuildAdmissionTest";
  private static final int    DIMENSIONS  = 16;
  /** Above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}, which is the threshold below which a build takes no permit. */
  private static final int    NUM_VECTORS = 1_200;
  /** Below it, so the exemption for a build too small to threaten the heap is exercised too. */
  private static final int    FEW_VECTORS = 200;

  /**
   * How long a parked search is given to reach the permit it is waiting for. A convergence wait, not a latency
   * bound: all it has to do first is materialise the location index and find no graph beside it. Generous because
   * a wider value cannot turn a passing run red - only a test that has already failed waits this long.
   */
  private static final Duration REACHES_THE_PERMIT = Duration.ofSeconds(30);

  /**
   * And how long the build itself is given once the permit is handed over. Sized for the worst case of the
   * {@code @Tag("vector")} convoy described in CLAUDE.md rather than for this corpus, which builds in well under a
   * second.
   */
  private static final Duration COMPLETES_THE_BUILD = Duration.ofMinutes(2);

  private String dbPath;
  private long   originalPermitTimeoutMs;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
    // A build that gives up waiting proceeds without a permit, by design - a query cannot be answered without a
    // graph. At the ten-minute default that turns a regression here into a ten-minute hang instead of a failure,
    // so the bound is shortened for the run and restored after it. Still far longer than the window these tests
    // observe, so the give-up path is never the one being measured.
    originalPermitTimeoutMs = GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(60_000);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(originalPermitTimeoutMs);
    FileUtils.deleteRecursively(new File(dbPath));
  }

  /**
   * The bug itself: with every rebuild permit held, a search that has to build the graph from scratch used to
   * build it anyway, immediately, however many other indexes were doing the same thing.
   */
  @Test
  void aSearchThatMustBuildTheGraphWaitsForTheJvmWideRebuildPermit() throws Exception {
    populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("persistedGraphNodeCount"))
            .as("precondition: the crash must have left no graph on disk, otherwise the search reuses one and "
                + "never reaches a build at all")
            .isZero();
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: the corpus must be above the threshold below which a build takes no permit")
            .isGreaterThanOrEqualTo(1_000L);

        final CountDownLatch searchReturned = new CountDownLatch(1);
        final AtomicReference<List<Pair<RID, Float>>> results = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        final Thread searcher = new Thread(() -> {
          try {
            results.set(index.findNeighborsFromVector(embedding(7), 5, 64));
          } catch (final Throwable t) {
            failure.set(t);
          } finally {
            searchReturned.countDown();
          }
        }, "Issue7814-search");
        searcher.setDaemon(true);

        boolean permitsHeld = true;
        try {
          searcher.start();

          Awaitility.await("the search reaches the rebuild permit and queues for it")
              .atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                  .as("a search that has to build the graph must go through the same JVM-wide permit an async "
                      + "rebuild takes (issue #7814)")
                  .isEqualTo(1L));

          assertThat(searchReturned.getCount())
              .as("and it must still be waiting: with no permit free, the build it needs has not run")
              .isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("no graph may have been built while every permit was held - this is the count that was 8 "
                  + "concurrent builds in the reported incident")
              .isZero();
          assertThat(index.getStats().get("searchRebuildsWithoutPermit"))
              .as("and it must be waiting for the permit, not have given up on it").isZero();

          // The ordering invariant the wait depends on. If the search held graphBuildLock while queueing, the
          // async rebuild path - which takes the permit first and this lock second - would deadlock against it
          // until the permit timeout expired.
          assertThat(index.graphBuildLockHeldForTest())
              .as("a search waiting for a rebuild permit must not be holding the per-index build lock that the "
                  + "permit's holder acquires next")
              .isFalse();

          LSMVectorIndex.releaseAllRebuildPermitsForTest();
          permitsHeld = false;

          assertThat(searchReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
              .as("once a permit is free the build runs and the search is answered - waiting for the permit must "
                  + "delay the answer, never withhold it")
              .isTrue();
        } finally {
          if (permitsHeld)
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          searcher.interrupt();
          searcher.join(COMPLETES_THE_BUILD.toMillis());
        }

        assertThat(failure.get()).as("the search must not have failed").isNull();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("the build the search was queueing for must have run once the permit freed").isEqualTo(1L);
        assertThat(results.get())
            .as("and it must return the neighbours of a corpus that was fully rebuilt, not an empty result set")
            .isNotEmpty();
        assertThat(results.get().stream().map(Pair::getFirst))
            .as("the query vector's own record is its nearest neighbour")
            .contains(ridOf(db, 7));
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Two searches arriving on the same index while it has no graph must produce ONE build between them.
   * <p>
   * This is the window the permit wait opens: the deciding thread has to release the per-index build lock before it
   * queues, so without something holding the decision the second search would repeat the whole validation walk,
   * reach the same answer, and queue for a second full build of the same corpus - the cost this gate exists to
   * bound, paid twice.
   */
  @Test
  void twoSearchesRacingTheSameMissingGraphProduceExactlyOneBuild() throws Exception {
    populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final CountDownLatch bothReturned = new CountDownLatch(2);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        final Thread[] searchers = new Thread[2];
        for (int i = 0; i < searchers.length; i++) {
          final int id = i;
          searchers[i] = new Thread(() -> {
            try {
              index.findNeighborsFromVector(embedding(id), 5, 64);
            } catch (final Throwable t) {
              failure.compareAndSet(null, t);
            } finally {
              bothReturned.countDown();
            }
          }, "Issue7814-search-" + i);
          searchers[i].setDaemon(true);
        }

        boolean permitsHeld = true;
        try {
          for (final Thread searcher : searchers)
            searcher.start();

          Awaitility.await("one of the two searches queues for the permit")
              .atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                  .isEqualTo(1L));

          // Whichever search lost the decision must be waiting on the winner rather than queueing for a permit of
          // its own: one build is owed here, not two.
          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("only the search that owns the build may queue for a permit; the other waits for its result")
              .isEqualTo(1L);

          LSMVectorIndex.releaseAllRebuildPermitsForTest();
          permitsHeld = false;

          assertThat(bothReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
              .as("both searches must be answered, neither wedged behind the other")
              .isTrue();
        } finally {
          if (permitsHeld)
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          for (final Thread searcher : searchers) {
            searcher.interrupt();
            searcher.join(COMPLETES_THE_BUILD.toMillis());
          }
        }

        assertThat(failure.get()).as("neither search may fail").isNull();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("the two searches must share one build of the corpus, not run one each")
            .isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * A search interrupted while waiting for another thread's build must fail loudly rather than take the build over.
   * <p>
   * Falling through would let it decide to build the same corpus a second time, and at
   * {@code maxConcurrentRebuilds} above 1 there is a second permit for it to take - so both would build at once,
   * which is the doubled cost the pending flag exists to prevent. The alternatives to failing are that duplicate
   * build or the empty result set a search with no graph returns, so the exception is the only honest answer.
   */
  @Test
  void aSearchInterruptedWhileWaitingForAnothersBuildFailsRatherThanStartingItsOwn() throws Exception {
    populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final CountDownLatch ownerReturned = new CountDownLatch(1);
        final CountDownLatch waiterReturned = new CountDownLatch(1);
        final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
        final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        final Thread owner = new Thread(() -> {
          try {
            index.findNeighborsFromVector(embedding(1), 5, 64);
          } catch (final Throwable t) {
            ownerFailure.set(t);
          } finally {
            ownerReturned.countDown();
          }
        }, "Issue7814-owner");
        owner.setDaemon(true);

        final Thread waiter = new Thread(() -> {
          try {
            index.findNeighborsFromVector(embedding(2), 5, 64);
          } catch (final Throwable t) {
            waiterFailure.set(t);
          } finally {
            waiterReturned.countDown();
          }
        }, "Issue7814-waiter");
        waiter.setDaemon(true);

        boolean permitsHeld = true;
        try {
          owner.start();
          // Only once the owner has taken the decision and is queueing for the permit can the second search park
          // on it rather than race it to the decision.
          Awaitility.await("the owning search queues for the permit")
              .atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                  .isEqualTo(1L));

          waiter.start();
          Awaitility.await("the second search parks on the owner's build")
              .atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(waiter.getState()).isEqualTo(Thread.State.WAITING));

          waiter.interrupt();
          assertThat(waiterReturned.await(REACHES_THE_PERMIT.toMillis(), TimeUnit.MILLISECONDS))
              .as("the interrupted search must return rather than keep waiting").isTrue();
          assertThat(waiterFailure.get())
              .as("and it must say why, instead of silently returning the empty result set a search with no graph "
                  + "produces")
              .isInstanceOf(IndexException.class);
          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("the interrupted search must not have taken the build over: only the owner ever queued")
              .isEqualTo(1L);

          LSMVectorIndex.releaseAllRebuildPermitsForTest();
          permitsHeld = false;

          assertThat(ownerReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
              .as("the owning search must still be answered").isTrue();
        } finally {
          if (permitsHeld)
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          owner.interrupt();
          owner.join(COMPLETES_THE_BUILD.toMillis());
          waiter.join(COMPLETES_THE_BUILD.toMillis());
        }

        assertThat(ownerFailure.get()).as("the owning search must not have failed").isNull();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("exactly one build of the corpus, not one per search thread").isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The exemption: an index too small to threaten the heap must not queue behind another index's rebuild.
   * <p>
   * Serializing every build regardless of size would trade milliseconds of work for however long the permit holder
   * takes, which is the wrong bargain for a corpus that cannot cause the failure the permit exists to prevent.
   */
  @Test
  void aBuildTooSmallToThreatenTheHeapDoesNotQueueForAPermit() throws Exception {
    populateAndCrashWithoutPersistingAGraph(FEW_VECTORS);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: the corpus must be below the threshold this exemption is keyed on")
            .isLessThan(1_000L);

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(3), 5, 64);

          assertThat(results)
              .as("a small index must answer while every rebuild permit is held elsewhere")
              .isNotEmpty();
          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("and must not have queued for one").isZero();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("it builds inline, as it always has").isEqualTo(1L);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Writes {@code count} vectors and kills the database rather than closing it, so no graph is ever persisted
   * beside them - the state every index of the reported cluster was in when its pods came back up.
   */
  private void populateAndCrashWithoutPersistingAGraph(final int count) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });

        db.begin();
        for (int i = 0; i < count; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();

        // kill() leaves the graph unpersisted, which is the point; close() after it is what takes the instance
        // out of the factory's active registry so the next session can open the same path.
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x7814L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final var rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
