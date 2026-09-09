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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

  private static void createSchema(final Database db, final int deltaCacheSize) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, deltaCacheSize);
    // No rebuild may drain the buffer while the assertions look at it: the inactivity timer is off and the
    // mutation threshold is out of reach.
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, Integer.MAX_VALUE);

    db.transaction(() -> {
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
