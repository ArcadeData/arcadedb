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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9896: a vector search must stop when its statement is terminated or runs past its deadline, also while it waits
 * for a graph build - its own, which queues for the JVM-wide rebuild permit, or another search's.
 * <p>
 * Each test holds every rebuild permit, so the graph the search needs cannot be built: before the fix the search waited
 * for the permit timeout (60 s here, ten minutes by default), deaf to the bound it was given. And the build a cancelled
 * search started must still run once a permit frees, exactly once: abandoning it would leave an index whose build takes
 * longer than the command timeout without a graph forever, each query starting the build again only to give up on it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue9896VectorSearchCancellationTest {
  private static final String DB_ROOT     = "target/test-databases/Issue9896VectorSearchCancellationTest";
  private static final int    DIMENSIONS  = 16;
  /** Above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}, below which a build takes no permit and so never waits. */
  private static final int    NUM_VECTORS = 1_200;
  /** The bound the cancelled searches are given. */
  private static final long   TIMEOUT_MS  = 500;

  /** A tripwire between a search that honours its bound and one that waits out the permit timeout (60 s). */
  private static final long     GIVES_UP_WITHIN     = 20_000;
  /** Convergence waits, not latency bounds: generous because only a run that has already failed waits this long. */
  private static final Duration REACHES_THE_PERMIT  = Duration.ofSeconds(30);
  private static final Duration COMPLETES_THE_BUILD = Duration.ofMinutes(2);

  private final RunningQueryRegistry registry = new RunningQueryRegistry();
  private       String               dbPath;
  private       long                 originalPermitTimeoutMs;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
    originalPermitTimeoutMs = GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(60_000);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(originalPermitTimeoutMs);
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @Test
  void sqlTimeoutEndsASearchQueuedForTheRebuildPermitAndTheBuildStillRuns() {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        boolean permitsHeld = true;
        try {
          final StallAwareStopwatch watch = StallAwareStopwatch.start();
          assertThatThrownBy(() -> drain(db.query("sql",
              "SELECT expand(vectorNeighbors('Doc[vector]', ?, 5)) TIMEOUT " + TIMEOUT_MS, (Object) embedding(7))))
              .satisfies(e -> assertThat(causeOfType(e, TimeoutException.class)).as("got: %s", e).isNotNull());
          watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a search honouring its TIMEOUT against one waiting out the rebuild permit");

          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("the build the search needed must have queued for the permit").isEqualTo(1L);

          LSMVectorIndex.releaseAllRebuildPermitsForTest();
          permitsHeld = false;
        } finally {
          if (permitsHeld)
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }

        Awaitility.await("the build the cancelled search started runs once a permit is free")
            .atMost(COMPLETES_THE_BUILD).pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L));

        assertThat(searchRids(db)).contains(ridOf(db, 7));
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("the next search must use the graph that build published, not build its own").isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void commandTimeoutEndsASearchWaitingForAnotherSearchesBuild() throws Exception {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final AtomicReference<List<Pair<RID, Float>>> ownerResults = new AtomicReference<>();
        final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
        final CountDownLatch ownerReturned = new CountDownLatch(1);

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        boolean permitsHeld = true;
        // An embedded search with no bound of its own owns the build
        final Thread owner = new Thread(() -> {
          try {
            ownerResults.set(index.findNeighborsFromVector(embedding(7), 5, 64));
          } catch (final Throwable t) {
            ownerFailure.set(t);
          } finally {
            ownerReturned.countDown();
          }
        }, "Issue9896-owner");
        owner.setDaemon(true);
        try {
          owner.start();
          Awaitility.await("the owning search queues for the permit").atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit")).isEqualTo(1L));

          db.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, TIMEOUT_MS);
          try {
            final StallAwareStopwatch watch = StallAwareStopwatch.start();
            assertThatThrownBy(() -> drain(db.query("sql", "SELECT expand(vector.neighbors('Doc[vector]', ?, 5))",
                (Object) embedding(3))))
                .satisfies(e -> assertThat(causeOfType(e, TimeoutException.class)).as("got: %s", e).isNotNull());
            watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a search honouring arcadedb.command.timeout against one waiting out another's build");
          } finally {
            db.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
          }

          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("the search that gave up waited for the owner's build, it did not queue a second one").isEqualTo(1L);

          LSMVectorIndex.releaseAllRebuildPermitsForTest();
          permitsHeld = false;
          assertThat(ownerReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
        } finally {
          if (permitsHeld)
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          owner.join(COMPLETES_THE_BUILD.toMillis());
        }

        assertThat(ownerFailure.get()).as("the owner, which has no bound, must not have failed").isNull();
        assertThat(ownerResults.get().stream().map(Pair::getFirst)).contains(ridOf(db, 7));
        assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void terminatedSearchStopsWaitingForTheRebuildPermit() throws Exception {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final AtomicReference<RunningQuery> entry = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final String query = "SELECT expand(vectorNeighbors('Doc[vector]', ?, 5))";

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        final Thread runner = new Thread(() -> {
          try (final RunningQuery q = registry.register(db.getName(), "admin", "test", null, "tag-9896", RunningQuery.Client.NONE)) {
            q.setStatement("sql", query);
            entry.set(q);
            drain(db.query("sql", query, (Object) embedding(7)));
          } catch (final Throwable e) {
            failure.set(e);
          }
        }, "Issue9896-runner");
        runner.setDaemon(true);
        try {
          runner.start();
          Awaitility.await("the search queues for the permit").atMost(REACHES_THE_PERMIT)
              .pollInterval(Duration.ofMillis(50))
              .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit")).isEqualTo(1L));

          final StallAwareStopwatch watch = StallAwareStopwatch.start();
          assertThat(entry.get().terminate("root")).isTrue();
          assertThat(entry.get().awaitEnd(60_000)).as("the statement must end once terminated").isTrue();
          watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a terminated search stopping against one waiting out the rebuild permit");
          runner.join(10_000);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }

        assertThat(causeOfType(failure.get(), QueryTerminatedException.class)).as("got: %s", failure.get()).isNotNull();
        assertThat(entry.get().getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

        Awaitility.await("the build still runs once a permit is free").atMost(COMPLETES_THE_BUILD)
            .pollInterval(Duration.ofMillis(50))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void cypherQueryNodesHonoursTheCommandTimeout() {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        db.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, TIMEOUT_MS);
        try {
          final StallAwareStopwatch watch = StallAwareStopwatch.start();
          assertThatThrownBy(() -> drain(db.query("opencypher",
              "CALL db.index.vector.queryNodes('Doc[vector]', 5, $vec) YIELD node, score RETURN node, score",
              Map.of("vec", embedding(7)))))
              .satisfies(e -> assertThat(causeOfType(e, TimeoutException.class)).as("got: %s", e).isNotNull());
          watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a Cypher vector search honouring its timeout against one waiting out the rebuild permit");
        } finally {
          db.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void selectApiTimeoutEndsAVectorSearchQueuedForTheRebuildPermit() {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          StallAwareStopwatch watch = StallAwareStopwatch.start();
          assertThatThrownBy(() -> db.select().fromType("Doc").timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS, true)
              .nearestTo("vector", embedding(7), 5).documents())
              .isInstanceOf(TimeoutException.class);
          watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a Select vector search honouring its timeout against one waiting out the rebuild permit");

          // Without the exception the expired search answers with what it produced: nothing
          watch = StallAwareStopwatch.start();
          assertThat(db.select().fromType("Doc").timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS, false)
              .nearestTo("vector", embedding(7), 5).documents()).isEmpty();
          watch.assertGaveUpWithin(GIVES_UP_WITHIN, "a Select vector search honouring its timeout against one waiting out the rebuild permit");
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The build a search hands off runs on a thread with none of the search's context. A search inside an open
   * transaction, which has a database context and a transaction bound to its own thread, still gets the graph built
   * and its answer.
   */
  @Test
  void searchInsideATransactionGetsTheGraphTheHandedOffBuildPublishes() {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final AtomicReference<List<RID>> rids = new AtomicReference<>();
        db.transaction(() -> {
          db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", embedding(NUM_VECTORS)).save();
          rids.set(searchRids(db));
        });
        assertThat(rids.get()).contains(ridOf(db, 7));
        assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  /** The guard reaches the search itself, not only the waits in front of it. */
  @Test
  void searchGivenATerminatedStatementsGuardFailsAtOnce() {
    populateAndCrashWithoutPersistingAGraph();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        // The graph is built first, so nothing below waits: what stops the search is the guard
        assertThat(index.findNeighborsFromVector(embedding(7), 5)).isNotEmpty();

        try (final RunningQuery q = registry.register(db.getName(), "admin", "test", null, "tag-9896", RunningQuery.Client.NONE)) {
          q.terminate("root");
          final WorkGuard guard = WorkGuard.forCommandDeadline(null);
          assertThatThrownBy(() -> index.findNeighborsFromVector(embedding(7), 5, -1, null, guard))
              .isInstanceOf(QueryTerminatedException.class);
        }
      } finally {
        db.drop();
      }
    }
  }

  private List<RID> searchRids(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT expand(vectorNeighbors('Doc[vector]', ?, 5))", (Object) embedding(7))) {
      return rs.stream().map(r -> r.<RID>getProperty("@rid")).toList();
    }
  }

  private void populateAndCrashWithoutPersistingAGraph() {
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
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        // kill() leaves the graph unpersisted, so the reopened index has to build it on its first search
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static void drain(final ResultSet resultSet) {
    try (resultSet) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x9896L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static <T extends Throwable> T causeOfType(final Throwable e, final Class<T> type) {
    for (Throwable t = e; t != null; t = t.getCause())
      if (type.isInstance(t))
        return type.cast(t);
    return null;
  }
}
