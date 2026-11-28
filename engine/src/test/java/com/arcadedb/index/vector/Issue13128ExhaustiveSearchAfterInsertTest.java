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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13128: an exhaustive {@code vector.neighbors} (k at or above the number of live vectors) answers every record on a freshly
 * built graph, because the walk comes up short and the issue #3722 brute-force completion fills the answer. After a write that no
 * graph build has absorbed, the vector lives only in the delta buffer and has no graph ordinal: the rows found counted it, the
 * budget the completion compares them with did not (it was capped by the number of graph ordinals), so a single delta-only row
 * hid one graph node the walk had not reached, and the completion never ran. The answer silently came back one record short.
 * <p>
 * The budget now counts the delta-only rows the search can return. It is judged only once the answer is short of the live count,
 * so a healthy search pays nothing for it, and it stays bounded by the live count, so a graph carrying tombstones next to a delta
 * buffer does not pay a completion scan (and a WARNING) on every search.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13128ExhaustiveSearchAfterInsertTest extends TestHelper {
  private static final int               DIMENSIONS = 8;
  private static final VectorTypeSupport VTS        = VectorizationProvider.getInstance().getVectorTypeSupport();

  @BeforeEach
  void keepTheGraphAsBuilt() {
    // The writes of the tests stay in the delta buffer: no rebuild may absorb them behind the test's back
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void restoreTheRebuildPolicy() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  /**
   * The reporter's repro: 1,300 clustered 8-d COSINE vectors on a hierarchical graph. The walk of such a graph can leave a node
   * out (which seeds do depends on the float arithmetic of the platform), and the exhaustive answer of the built graph is then
   * completed by the scan. One insert later the answer was one record short.
   */
  @ParameterizedTest
  @MethodSource("seeds")
  void anExhaustiveSearchAnswersEveryRecordAfterOneInsert(final int seed) {
    final int n = 1300;
    final Random random = new Random(seed);
    createType("COSINE", "NONE", 32, 8, true);
    final float[][] centers = new float[6][DIMENSIONS];
    for (final float[] center : centers)
      for (int j = 0; j < DIMENSIONS; j++)
        center[j] = (float) (random.nextGaussian() * 3);

    database.transaction(() -> {
      for (int i = 0; i < n; i++) {
        final float[] vector = new float[DIMENSIONS];
        final float[] center = centers[random.nextInt(centers.length)];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = center[j] + (float) (random.nextGaussian() * 0.3);
        database.newDocument("Doc").set("v", vector).save();
      }
    });
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    final float[] query = gaussian(random);
    assertThat(index.findNeighborsFromVector(query, n + 50)).as("the built graph answers every record").hasSize(n);

    insertOne(random);

    assertThat(index.findNeighborsFromVector(query, n + 50)).as("and still does after one insert").hasSize(n + 1);
  }

  /**
   * A BINARY index with a hierarchical graph leaves about 5% of its nodes out of every walk, so the completion is its normal
   * path, and the answer was short by up to the number of delta-only rows after W inserts.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3 })
  void anExhaustiveSearchOfABinaryHierarchicalIndexAnswersEveryRecordAfterManyInserts(final int seed) {
    final int n = 1100;
    final int writes = 20;
    final Random random = new Random(seed);
    createType("COSINE", "BINARY", 100, 16, true);
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    final float[] query = gaussian(random);
    assertThat(index.findNeighborsFromVector(query, n + 50, 2000, null)).as("the built graph answers every record").hasSize(n);

    database.transaction(() -> {
      for (int i = 0; i < writes; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });

    assertThat(index.findNeighborsFromVector(query, n + writes + 50, 2000, null)).as("and still does after %d inserts", writes)
        .hasSize(n + writes);
  }

  /** The shape of the report: the SQL function, asked for more neighbours than there are records, after writes no build absorbed. */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3 })
  void theSqlFunctionAnswersEveryRecordAfterInserts(final int seed) {
    final int n = 1100;
    final int writes = 20;
    final Random random = new Random(seed);
    createType("COSINE", "BINARY", 100, 16, true);
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });
    vectorIndex().buildVectorGraphNow();
    final float[] query = gaussian(random);
    database.transaction(() -> {
      for (int i = 0; i < writes; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });

    try (final ResultSet resultSet = database.query("sql",
        "SELECT count(*) AS rows FROM (SELECT expand(vector.neighbors('Doc[v]', :q, :k, {efSearch: 2000})))",
        Map.of("q", query, "k", n + writes + 50))) {
      assertThat(resultSet.next().<Long>getProperty("rows")).isEqualTo(n + writes);
    }
  }

  /**
   * The filtered form: an allow-list that is the whole index (every live vector, the new ones included) asked with
   * {@code k = allowList.size()}. The allow-list size capped the budget only when it was the smaller of the two, and with the
   * whole index it was not, so the delta-only rows hid the nodes the walk had not reached. A BINARY index with a hierarchical
   * graph leaves some out of every walk.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3 })
  void anAllowListOfTheWholeIndexAnswersEveryRecordAfterInserts(final int seed) {
    final int n = 1100;
    final Random random = new Random(seed);
    createType("COSINE", "BINARY", 100, 16, true);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        rids.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());
    });
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();
    final float[] query = gaussian(random);

    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        rids.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());
    });
    final Set<RID> allowList = new HashSet<>(rids);

    assertThat(index.findNeighborsFromVector(query, allowList.size(), 2000, allowList))
        .as("every member of the allow-list is answered").hasSize(allowList.size());
  }

  /**
   * A graph that carries tombstones next to a delta buffer is NOT a shortfall: the live count is what the answer is judged by,
   * and the tombstoned ordinals no query can return must not be counted next to the delta-only rows. Counting both would have
   * every exhaustive search pay a completion scan and log a WARNING about a healthy graph.
   */
  @Test
  void aTombstonedGraphNextToADeltaBufferDoesNotScanOnEverySearch() {
    final int n = 1200;
    final int deleted = 40;
    final int inserted = 10;
    final Random random = new Random(13128);
    createType("COSINE", "NONE", 100, 16, false);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        rids.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());
    });
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    database.transaction(() -> {
      for (int i = 0; i < deleted; i++)
        database.lookupByRID(rids.get(i), false).asDocument().delete();
    });
    insertMany(random, inserted);

    final int live = n - deleted + inserted;
    final float[] query = gaussian(random);
    final long scansBefore = bruteForceScans(index);
    for (int i = 0; i < 3; i++)
      assertThat(index.findNeighborsFromVector(query, live + 50)).as("every live record is answered").hasSize(live);
    assertThat(bruteForceScans(index) - scansBefore).as("and the answer is complete without a completion scan").isZero();
  }

  /**
   * The delta-only rows the budget counts are the ones a search can return: a row with a graph ordinal is a node of the graph, a
   * tombstoned one is gone, and an unscorable one (the all-zero vector under COSINE) is live in the index and queued in the buffer
   * but no search returns it, so the answer is not short of it and counting it would scan on every exhaustive search. Asked of the
   * helper directly: the search itself cannot be driven with such a row, because JVector asserts on scoring it under {@code -ea}.
   */
  @Test
  void theDeltaOnlyRowsAreTheOnesASearchCanReturn() {
    final Random random = new Random(13128);
    createType("COSINE", "NONE", 100, 16, false);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        rids.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());
    });
    final LSMVectorIndex index = vectorIndex();
    final int[] ordinalMap = { 1000, 1001, 1002, 1003 };
    final RID rid = rids.get(0);
    final int deletedId = index.residentLocationsForTest().getVectorIdsForRid(rids.get(5))[0];
    database.transaction(() -> database.lookupByRID(rids.get(5), false).asDocument().delete());
    final List<LSMVectorIndex.DeltaVectorEntry> delta = new ArrayList<>();
    // a node of the graph queued again: not delta-only
    delta.add(new LSMVectorIndex.DeltaVectorEntry(1002, rid, VTS.createFloatVector(gaussian(random))));
    // delta-only, with its payload, without it, and the pending row of a transaction
    delta.add(new LSMVectorIndex.DeltaVectorEntry(100, rid, VTS.createFloatVector(gaussian(random))));
    delta.add(new LSMVectorIndex.DeltaVectorEntry(101, rid, null));
    delta.add(new LSMVectorIndex.DeltaVectorEntry(TransactionVectorOverlay.PENDING_VECTOR_ID, rid, VTS.createFloatVector(gaussian(random))));
    // tombstoned since it was queued: gone
    delta.add(new LSMVectorIndex.DeltaVectorEntry(deletedId, rids.get(5), VTS.createFloatVector(gaussian(random))));
    // unscorable under COSINE: never returned
    delta.add(new LSMVectorIndex.DeltaVectorEntry(102, rid, VTS.createFloatVector(new float[DIMENSIONS])));
    delta.add(new LSMVectorIndex.DeltaVectorEntry(TransactionVectorOverlay.PENDING_VECTOR_ID, rid, VTS.createFloatVector(new float[DIMENSIONS])));

    assertThat(index.countDeltaOnlyRows(delta, ordinalMap, null)).isEqualTo(3);
    assertThat(index.countDeltaOnlyRows(List.of(), ordinalMap, null)).isZero();
  }

  /**
   * The rows a transaction wrote and has not committed are rows of its own search, so they are part of the budget the same way
   * the committed delta-only rows are: a search inside the transaction is as exhaustive as one after the commit.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3 })
  void anExhaustiveSearchInsideATransactionAnswersItsPendingRowsToo(final int seed) {
    final int n = 1100;
    final int writes = 20;
    final Random random = new Random(seed);
    createType("COSINE", "BINARY", 100, 16, true);
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    final float[] query = gaussian(random);
    database.begin();
    try {
      for (int i = 0; i < writes; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();

      assertThat(index.findNeighborsFromVector(query, n + writes + 50, 2000, null))
          .as("the committed rows and the %d pending ones", writes).hasSize(n + writes);
    } finally {
      database.rollback();
    }
  }

  static IntStream seeds() {
    return IntStream.rangeClosed(1, 12);
  }

  private void createType(final String similarity, final String quantization, final int beamWidth, final int maxConnections,
      final boolean hierarchy) {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create().createProperty("v", Type.ARRAY_OF_FLOATS);
    database.command("sql",
        "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: '" + similarity + "', quantization: '"
            + quantization + "', addHierarchy: " + hierarchy + ", maxConnections: " + maxConnections + ", beamWidth: " + beamWidth
            + ", inactivityRebuildTimeoutMs: 3600000}").close();
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("v")).getIndexesOnBuckets()[0];
  }

  private RID insertOne(final Random random) {
    final List<RID> inserted = new ArrayList<>(1);
    database.transaction(() -> inserted.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity()));
    return inserted.get(0);
  }

  private void insertMany(final Random random, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Doc").set("v", gaussian(random)).save();
    });
  }

  private static float[] gaussian(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) random.nextGaussian();
    return vector;
  }

  private static long bruteForceScans(final LSMVectorIndex index) {
    return index.getStats().get("bruteForceScans");
  }
}
