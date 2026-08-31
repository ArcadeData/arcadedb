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
 * Regression for issue #6859: a follow-up to #6798 (see {@link Issue6798InactivityTimerUnloadedGraphTest}).
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
@Tag("vector")
class Issue6859AsyncRebuildPrefixReuseTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6859AsyncRebuildPrefixReuseTest";
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
