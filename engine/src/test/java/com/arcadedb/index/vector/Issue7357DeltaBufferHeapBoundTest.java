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
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7357: the delta buffer held a full copy of every vector written since the last graph
 * rebuild, on the heap, with nothing bounding it.
 * <p>
 * Every write appends an entry to that buffer so the vector is searchable before it reaches the HNSW graph, and the
 * entry carried the whole vector. Only a rebuild drains the buffer, and a rebuild is triggered by the index going
 * quiet - so an ingest that never goes quiet grows a second complete copy of the corpus in RAM. At the reporter's
 * scale, 4.2M records of 768-dimension embeddings, that copy is 12.9 GB and it was two thirds of what killed a
 * {@code -Xmx16g} load. It is the same second copy issue #3144 removed from {@link GrowableVectorValues}, on the
 * one path that still had it.
 * <p>
 * The vector is persisted before the entry is queued, so the payload in the entry is a cache and nothing more. Past
 * the budget the entry keeps only its id and RID, and the delta scan reads the vector back from the pages.
 * <p>
 * Three things are pinned:
 * <ul>
 *   <li>the number of payloads held on the heap stops at the budget while the buffer itself keeps growing;</li>
 *   <li>searches still find the buffered rows, and rank them the same way, once the payloads are gone - which is
 *       the whole correctness question, because a bound that quietly dropped rows from results would satisfy the
 *       first assertion perfectly;</li>
 *   <li>{@code deltaCacheSize = -1} still keeps every payload, so the bound is opt-out rather than imposed.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7357DeltaBufferHeapBoundTest {
  private static final String DB_ROOT    = "target/test-databases/Issue7357DeltaBufferHeapBoundTest";
  private static final int    DIMENSIONS = 16;
  /** Kept under {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} so no rebuild drains the buffer behind the assertions. */
  private static final int    RECORDS    = 400;
  private static final int    BUDGET     = 50;

  /**
   * Fixture for the re-queue path: above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} so the persisted graph is treated as
   * a large one and the stale-prefix reuse is reachable at all.
   */
  private static final int PERSISTED_VECTORS = 1_200;
  /** Records written after the persisted graph, i.e. the gap the reuse re-queues. Must exceed {@link #BUDGET}. */
  private static final int GAP_VECTORS       = 200;

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
  void theBufferStopsHoldingPayloadsAtTheBudgetAndStillAnswersSearches() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db, BUDGET);
        insert(db, 0, RECORDS);

        final LSMVectorIndex index = vectorIndex(db);

        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("precondition: nothing has drained the buffer, so it holds every written vector")
            .isEqualTo((long) RECORDS);
        assertThat(index.getStats().get("deltaResidentVectorsCapacity"))
            .as("precondition: the explicit budget is the one in force")
            .isEqualTo((long) BUDGET);

        assertThat(index.getStats().get("deltaResidentVectors"))
            .as("the buffer's heap footprint stops at the budget even though it keeps accepting entries "
                + "(issue #7357)")
            .isEqualTo((long) BUDGET);

        // The assertion the bound is actually for. Every one of these rows is served ONLY by the delta scan -
        // there is no graph yet - and all but the first BUDGET of them now carry no vector at all, so a scan that
        // could not read them back would return the wrong rows rather than fail.
        for (final int probe : new int[] { 3, BUDGET, BUDGET + 1, RECORDS / 2, RECORDS - 1 }) {
          final List<Pair<RID, Float>> hits = search(db, embedding(probe), 1);
          assertThat(hits)
              .as("a search for the exact vector of record %d must find it", probe)
              .isNotEmpty();
          assertThat(idOf(db, hits.getFirst().getFirst()))
              .as("record %d is the nearest neighbour of its own vector, payload on the heap or not", probe)
              .isEqualTo(probe);
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The opt-out. {@code deltaCacheSize = -1} is the behaviour before this fix, kept reachable for an index whose
   * ingest is bounded by something else and that would rather not pay a page read per scored entry.
   */
  @Test
  void aNegativeBudgetKeepsEveryPayloadOnTheHeap() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db, -1);
        insert(db, 0, RECORDS);

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("deltaVectorsCount")).isEqualTo((long) RECORDS);
        assertThat(index.getStats().get("deltaResidentVectors"))
            .as("an explicitly negative budget declines nothing")
            .isEqualTo((long) RECORDS);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Deleting the records the buffer holds must give their heap budget back, or an index that churns would decline
   * payloads for room it has.
   */
  @Test
  void deletingBufferedRecordsGivesTheirBudgetBack() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db, BUDGET);
        insert(db, 0, RECORDS);

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("deltaResidentVectors")).isEqualTo((long) BUDGET);

        // Every row, not a prefix of them: which entries ended up holding a payload follows the order the
        // transaction applies its index writes in, which is not the order the documents were created in.
        db.transaction(() -> db.command("sql", "DELETE FROM Doc"));

        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("precondition: the deletions really did empty the buffer")
            .isZero();
        assertThat(index.getStats().get("deltaResidentVectors"))
            .as("the payloads of the deleted rows are no longer charged to the buffer")
            .isZero();
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The re-queue path, which is where the bound was first walked around (PR #7360 review).
   * <p>
   * {@code readDeltaEntriesFor()} builds delta entries carrying full payloads for the vectors a stale persisted
   * prefix does not cover, and hands them to the buffer. Those entries never write anything - they are read back
   * off the pages - so they did not pass through the write path's gate, and the budget did not apply to them. The
   * quantity involved is the worst possible one to exempt: the gap is "everything written since the persisted
   * graph was built", which for a session reopened after an interrupted load is the entire load. That is precisely
   * run 1 of the report, the one that OOM'd and left the database fenced for recovery.
   * <p>
   * The fix moves the budget to the door every entry goes through, so this pins the outcome rather than the
   * mechanism: after a reuse re-queues a gap four times the budget, the buffer holds all of it and the heap holds
   * only what the budget allows.
   */
  @Test
  @Tag("vector")
  void aReQueuedGapHonoursTheSameBudgetAsAWrite() {
    buildStalePersistedGraphFixture();

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, BUDGET);
        // The reuse only folds its gap into a rebuild once the gap crosses the ratio-scaled threshold; disabling
        // the scaling leaves the absolute floor, which is what this fixture is sized against (issue #7183).
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("precondition: this session has buffered nothing of its own")
            .isZero();

        // The rebuild the reuse dispatches would drain the buffer out from under the assertions, so the sole
        // JVM-wide rebuild permit is held for the observation window. No search path takes a permit, so the query
        // below still runs.
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          // The search is what triggers the reuse, and with it the gap re-queue this test is about.
          index.findNeighborsFromVector(embedding(0), 5);

          assertThat(index.getStats().get("stalePrefixGraphReuses"))
              .as("precondition: the reuse must actually have run, or nothing re-queued a gap and this test "
                  + "asserts nothing")
              .isEqualTo(1L);
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: the whole gap is buffered - the bound drops payloads, never entries")
              .isEqualTo((long) GAP_VECTORS);

          assertThat(index.getStats().get("deltaResidentVectors"))
              .as("a re-queued gap is charged the same heap budget as a write: %d entries, at most %d payloads",
                  GAP_VECTORS, BUDGET)
              .isLessThanOrEqualTo((long) BUDGET);
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * Session 1 builds and persists a graph over {@value #PERSISTED_VECTORS} records and closes cleanly; session 2
   * writes {@value #GAP_VECTORS} more and is killed rather than closed, so the persisted graph and its manifest
   * still describe only the first batch while the live vector set has moved on. The same fixture shape
   * {@code Issue6772WriteBeforeSearchPrefixReuseTest} uses, at a size that keeps this test cheap.
   */
  private void buildStalePersistedGraphFixture() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db, -1);
        insert(db, 0, PERSISTED_VECTORS);
        vectorIndex(db).buildVectorGraphNow();
        assertThat(vectorIndex(db).getStats().get("graphState"))
            .as("precondition: the graph must be built and IMMUTABLE before the close that persists it")
            .isEqualTo(1L); // GraphState.IMMUTABLE
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        insert(db, PERSISTED_VECTORS, PERSISTED_VECTORS + GAP_VECTORS);
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  /**
   * The auto-sized branch, which is what every installation that never touches either new setting gets, and the
   * only one of the three whose answer a test cannot otherwise predict (PR #7360 review).
   * <p>
   * The heap figures are pinned rather than read off the live JVM - the same seam
   * {@code VectorHeapBudget.buildCacheBudgetBytes(percent, maxHeap, availableHeap)} already provides for the
   * graph-build cache - because a budget derived from whatever a CI runner happens to have free is not an
   * assertion about anything. Three readings, one per arm of the arithmetic:
   * <ul>
   *   <li>a roomy heap, where the ceiling percentage is what binds;</li>
   *   <li>a heap an online rebuild has nearly filled, where the 90%-of-available cap binds instead and the buffer
   *       is told to stop growing - which is exactly when it must be (issue #6503's lesson applied here);</li>
   *   <li>the reporter's own shape, where the answer has to be far below the corpus or the bound does nothing.</li>
   * </ul>
   */
  @Test
  void theAutoSizedBudgetIsAShareOfTheCeilingCappedByWhatIsFree() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        createSchema(db, 0); // 0 = auto-size, the default
        final LSMVectorIndex index = vectorIndex(db);

        final long bytesPerVector = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
        assertThat(bytesPerVector)
            .as("precondition: the per-entry cost the budget divides by")
            .isEqualTo((long) DIMENSIONS * Float.BYTES + 64);

        final long maxHeap = 16L * 1024 * 1024 * 1024;

        // 1. Roomy heap: 10% of the 16 GB ceiling, since 90% of 12 GB free is far more than that.
        final long roomy = index.computeDeltaPayloadCapacity(maxHeap, 12L * 1024 * 1024 * 1024);
        assertThat(roomy)
            .as("the ceiling share binds while there is headroom")
            .isEqualTo((int) (maxHeap / 100 * 10 / bytesPerVector));

        // 2. A rebuild holding the old graph has left 512 MB. The available cap has to bind now, or the buffer
        // would keep growing into heap the rebuild is about to need.
        final long tight = index.computeDeltaPayloadCapacity(maxHeap, 512L * 1024 * 1024);
        assertThat(tight)
            .as("90%% of what is actually free binds when the heap is tight, not the ceiling share")
            .isEqualTo((int) (512L * 1024 * 1024 / 100 * 90 / bytesPerVector));
        assertThat(tight)
            .as("and it is a real reduction, not a formality")
            .isLessThan(roomy);

        // 3. The reporter's shape. At 768 dimensions the whole 4.2M-record buffer is 12.9 GB, so an auto-sized
        // budget on a 16 GB heap has to come out far below that or the bound accomplishes nothing.
        final long reporterBytesPerVector = VectorHeapBudget.bytesPerCachedVector(768);
        final long reporterCapacity = maxHeap / 100 * 10 / reporterBytesPerVector;
        assertThat(reporterCapacity * reporterBytesPerVector)
            .as("the default budget on the reported heap is a fraction of the 12.9 GB the buffer used to hold")
            .isLessThan(2L * 1024 * 1024 * 1024);
        assertThat(reporterCapacity)
            .as("and it still keeps a useful number of payloads resident")
            .isGreaterThan(100_000L);
      } finally {
        db.drop();
      }
    }
  }

  private static void createSchema(final Database db, final int deltaCacheSize) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, deltaCacheSize);
    // No rebuild may drain the buffer while the assertions look at it: the inactivity timer is off and the
    // mutation threshold is out of reach.
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, Integer.MAX_VALUE);

    db.transaction(() -> {
      if (db.getSchema().existsType("Doc"))
        return;
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });
  }

  private static void insert(final Database db, final int fromInclusive, final int toExclusive) {
    db.begin();
    for (int i = fromInclusive; i < toExclusive; i++)
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
    db.commit();
  }

  private static List<Pair<RID, Float>> search(final Database db, final float[] query, final int k) {
    final List<Pair<RID, Float>> hits = new ArrayList<>();
    db.transaction(() -> hits.addAll(vectorIndex(db).findNeighborsFromVector(query, k)));
    return hits;
  }

  private static int idOf(final Database db, final RID rid) {
    return db.lookupByRID(rid, true).asDocument().getInteger("id");
  }

  /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x7357L * 31 + id);
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
