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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.awaitility.Awaitility;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7842: deleting a vector must not condemn the persisted graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7842DeleteKeepsPersistedGraphTest {
  private static final String DB_ROOT     = "target/test-databases/Issue7842DeleteKeepsPersistedGraphTest";
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 2_000;
  private static final int    DELETED     = 5;
  private static final int    GAP_VECTORS = 200;

  // A convergence wait, not a latency bound: the async rebuild has to wait out whatever else in this JVM holds the
  // sole LSMVectorIndex.REBUILD_SEMAPHORE permit before it can even start (see the @Tag("vector") note in
  // CLAUDE.md), so this is sized for the worst case. A generous value cannot turn a passing run red.
  private static final Duration ASYNC_REBUILD_TIMEOUT = Duration.ofMinutes(4);

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
  void reopenAfterDeletesReusesThePersistedGraphInsteadOfRebuildingItFromScratch() throws Exception {
    // Session 1: build and persist a graph over exactly NUM_VECTORS records, then close cleanly.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("graphState")).as("precondition: graph built and IMMUTABLE").isEqualTo(1L);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 2: delete a handful of records, then crash rather than close, so nothing rebuilds the graph and the
    // persisted pages stay exactly as session 1 left them - with tombstones now recorded against some of them.
    final List<Integer> deletedIds = new ArrayList<>();
    for (int i = 0; i < DELETED; i++)
      deletedIds.add(i * 37);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.begin();
        for (final int id : deletedIds)
          db.command("sql", "DELETE FROM Doc WHERE id = ?", id);
        db.commit();

        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 3: reopen. The persisted graph still holds NUM_VECTORS nodes, DELETED of which now answer for
    // tombstoned vectors. Before this fix that alone made the whole graph UNUSABLE and the first search paid a
    // full synchronous rebuild.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("persistedGraphNodeCount"))
            .as("precondition: the persisted graph must still describe every record session 1 built it over")
            .isEqualTo((long) NUM_VECTORS);
        // Read BEFORE the first search: a rebuild republishes the location index and drops the tombstone set with
        // it, so on the pre-fix code this same read after the search answers 0.
        assertThat(index.getStats().get("deletedVectors"))
            .as("precondition: the tombstones must have survived the crash, otherwise nothing is being tested")
            .isEqualTo((long) DELETED);

        final int liveId = 1; // not in deletedIds
        final RID liveRid = ridOf(db, liveId);

        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(liveId), 5, 64);

        assertThat(index.getStats().get("graphRebuildCount"))
            .as("no full graph rebuild may run just because vectors were deleted")
            .isZero();
        assertThat(index.getStats().get("graphNodeCount"))
            .as("the persisted graph must be reused whole, tombstoned nodes included")
            .isEqualTo((long) NUM_VECTORS);
        assertThat(results.stream().map(Pair::getFirst))
            .as("a live record must still be found by its own embedding").contains(liveRid);

        // And the deleted ones must not come back.
        for (final int id : deletedIds) {
          final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(id), 5, 64);
          try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc WHERE id = ?", id)) {
            assertThat(rs.next().<Long>getProperty("cnt")).as("record %d must be gone", id).isZero();
          }
          assertThat(hits).as("a deleted vector must never be returned").allSatisfy(
              hit -> assertThat(db.lookupByRID(hit.getFirst(), true).asDocument().<Integer>get("id")).isNotEqualTo(id));
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The same reopen with vectors ALSO added after the persisted build: deletions and insertions together, which the
   * recompute path could not express at all - the stale-prefix reuse of issue #6655 was gated on there being no
   * deletions, so any tombstone sent the whole index back to a full rebuild.
   */
  @Test
  void reopenAfterDeletesAndInsertsReusesThePersistedGraphAsAPrefix() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    final int gapId = NUM_VECTORS + 1;
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.begin();
        db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
        for (int i = NUM_VECTORS; i < NUM_VECTORS + GAP_VECTORS; i++)
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        db.commit();

        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        // Leave only the absolute floor of 100 in play, so GAP_VECTORS genuinely crosses the threshold and the
        // async fold-in this test waits for is triggered BY the policy rather than in spite of it (issue #7183).
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

        final LSMVectorIndex index = vectorIndex(db);
        final RID gapRid = ridOf(db, gapId);

        // The async rebuild the reuse dispatches resets the very gauges asserted below, so park the sole JVM-wide
        // permit for the observation window - the same reason Issue6655StaleGraphPrefixReuseTest does.
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(gapId), 5, 64);

          assertThat(index.getStats().get("graphRebuildCount"))
              .as("no synchronous full rebuild may run for a delete plus some inserts").isZero();
          assertThat(index.getStats().get("stalePrefixGraphReuses"))
              .as("the persisted graph must have been reused as a prefix").isEqualTo(1L);
          assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
              .as("and that reuse must be recorded as one carrying tombstoned nodes").isEqualTo(1L);
          assertThat(results.stream().map(Pair::getFirst))
              .as("a vector written after the persisted build must be searchable from the first query")
              .contains(gapRid);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }

        Awaitility.await("the async rebuild the prefix reuse dispatched completes")
            .atMost(ASYNC_REBUILD_TIMEOUT)
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L));

        assertThat(index.getStats().get("graphNodeCount"))
            .as("the rebuild folds the gap in and the tombstone out")
            .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS - 1));
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The tombstones are not free, they are DEFERRED: the graph's dead nodes are charged to the ordinary mutation
   * counter, so the existing threshold, its heap admission check and the JVM-wide rebuild permit decide when they
   * are folded out - instead of the first search after a reopen paying for all of them synchronously.
   */
  @Test
  void tombstonedNodesAreChargedToTheOrdinaryRebuildSchedule() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    final int manyDeletes = 150; // above the absolute mutation floor of 100 the configuration below leaves in play
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.begin();
        db.command("sql", "DELETE FROM Doc WHERE id < ?", manyDeletes);
        db.commit();

        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("mutationsSinceRebuild"))
            .as("precondition: a freshly reopened session has counted nothing yet").isZero();

        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          index.findNeighborsFromVector(embedding(NUM_VECTORS - 1), 5, 64);

          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the search must be answered off the persisted graph, not off a rebuild of it").isZero();
          assertThat(index.getStats().get("graphReusesWithTombstonedNodes")).isEqualTo(1L);
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("the dead nodes are what the graph owes, so they must be counted as pending work")
              .isGreaterThanOrEqualTo((long) manyDeletes);
          assertThat(index.getStats().get("graphState"))
              .as("a graph that owes a compaction is MUTABLE, which is what the rebuild policies gate on")
              .isEqualTo(2L);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }

        Awaitility.await("the deferred compaction of the tombstoned nodes completes")
            .atMost(ASYNC_REBUILD_TIMEOUT)
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount"))
                .as("graphRebuildCount, with asyncRebuildInProgress=%s",
                    index.getStats().get("asyncRebuildInProgress"))
                .isEqualTo(1L));

        assertThat(index.getStats().get("graphNodeCount"))
            .as("once it runs, the compaction leaves only the live vectors in the graph")
            .isEqualTo((long) (NUM_VECTORS - manyDeletes));
      } finally {
        db.drop();
      }
    }
  }

  /**
   * PRODUCT quantization is the one case this fix deliberately does NOT cover: the PQ codes are addressed by the
   * same ordinal and are produced wholesale by the rebuild, so reusing a graph whose ordinal space has holes would
   * pair it with a codebook built over a dense one. That guard is a single {@code quantizationType == PRODUCT} test
   * at the top of the reuse path, nothing fails loudly if a refactor drops it, and what it would produce is a
   * silently wrong ranking rather than a crash - so it is pinned here.
   */
  @Test
  void aProductQuantizedIndexStillRebuildsAfterDeletes() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db, "PRODUCT");
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.begin();
        db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
        db.commit();

        ((DatabaseInternal) db).kill();
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
        assertThat(index.getMetadata().quantizationType)
            .as("precondition: the guard under test only applies to PRODUCT").isEqualTo(VectorQuantizationType.PRODUCT);
        assertThat(index.getStats().get("deletedVectors"))
            .as("precondition: read before the first search, which is what discards the tombstone set").isEqualTo(1L);

        index.findNeighborsFromVector(embedding(1), 5, 64);

        assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
            .as("a PRODUCT-quantized index must never take the tombstone-tolerant reuse path").isZero();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("it must pay the full rebuild instead, which is what keeps its codebook and its graph on the same "
                + "dense ordinal space")
            .isEqualTo(1L);
      } finally {
        db.drop();
      }
    }
  }

  private static void populate(final Database db) {
    populate(db, null);
  }

  private static void populate(final Database db, final String quantization) {
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\""
          + (quantization != null ? ", \"quantization\": \"" + quantization + "\"" : "") + " }");
    });

    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      if (i % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x7842L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
