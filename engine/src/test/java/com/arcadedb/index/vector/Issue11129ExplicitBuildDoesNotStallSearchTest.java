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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue11129ExplicitBuildDoesNotStallSearchTest {
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
