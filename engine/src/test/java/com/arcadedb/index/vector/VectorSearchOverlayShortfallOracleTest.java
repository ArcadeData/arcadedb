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
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle for the shortfall budget of {@code searchNeighbors} when the calling transaction has written to the index: an exhaustive
 * search (k above the live count) inside a transaction that rewrote, deleted and inserted records answers exactly the live
 * records, each once, whatever the graph walk left out.
 * <p>
 * A BINARY index with a hierarchical graph leaves a few percent of its nodes out of every walk, so the completion scan is its
 * normal path. The budget that decides whether the scan runs is {@code live + pending - superseded}: the committed count, plus
 * the rows the transaction wrote, minus the committed rows it replaced or removed (they are still counted by the committed index
 * and no search of the transaction may return them). A budget below the true live count lets the answer stay short of the nodes the
 * walk missed; the oracle here is the set of records the model says are live.
 */
class VectorSearchOverlayShortfallOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int RECORDS    = 1100;

  @BeforeEach
  void keepTheGraphAsBuilt() {
    // the writes of the tests stay in the delta buffer and the overlay: no rebuild may absorb them behind the test's back
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

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3, 4 })
  void anExhaustiveSearchInsideATransactionAnswersExactlyTheLiveRecords(final int seed) {
    final Random random = new Random(seed);
    final List<RID> rids = createAndFill(random);
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();
    final float[] query = gaussian(random);

    final int rewritten = 15;
    final int deleted = 10;
    final int inserted = 10;
    database.begin();
    try {
      final Set<RID> live = new HashSet<>(rids);
      for (int i = 0; i < rewritten; i++)
        database.lookupByRID(rids.get(i * 7), false).asDocument().modify().set("v", gaussian(random)).save();
      for (int i = 0; i < deleted; i++) {
        final RID gone = rids.get(500 + i);
        database.lookupByRID(gone, false).asDocument().delete();
        live.remove(gone);
      }
      for (int i = 0; i < inserted; i++)
        live.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());

      final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(query, RECORDS + 100, 2000, null);
      final Set<RID> answered = new HashSet<>();
      for (final Pair<RID, Float> row : answer)
        assertThat(answered.add(row.getFirst())).as("%s is answered once", row.getFirst()).isTrue();

      assertThat(answered).as("the live records of the transaction, rewritten and inserted ones included, deleted ones not")
          .isEqualTo(live);
    } finally {
      database.rollback();
    }
  }

  /**
   * The other side of the budget: when the walk and the delta merge already answer every live record, nothing is short, and the
   * netted budget must not ask for rows that cannot exist. Counting the rows the transaction replaced or removed as live (a sign
   * slip in the budget) would have every exhaustive search of such a transaction pay the completion scan and log a WARNING about a
   * healthy graph.
   */
  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 3 })
  void aCompleteAnswerInsideATransactionThatRewroteRowsDoesNotScan(final int seed) {
    final Random random = new Random(seed);
    final List<RID> rids = createAndFill(random, "NONE", false);
    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();
    final float[] query = gaussian(random);

    database.begin();
    try {
      final Set<RID> live = new HashSet<>(rids);
      for (int i = 0; i < 15; i++)
        database.lookupByRID(rids.get(i * 7), false).asDocument().modify().set("v", gaussian(random)).save();
      for (int i = 0; i < 10; i++) {
        final RID gone = rids.get(500 + i);
        database.lookupByRID(gone, false).asDocument().delete();
        live.remove(gone);
      }
      for (int i = 0; i < 10; i++)
        live.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());

      final long scansBefore = index.getStats().get("bruteForceScans");
      final Set<RID> answered = new HashSet<>();
      for (final Pair<RID, Float> row : index.findNeighborsFromVector(query, RECORDS + 100, 2000, null))
        answered.add(row.getFirst());

      assertThat(answered).as("the live records of the transaction").isEqualTo(live);
      assertThat(index.getStats().get("bruteForceScans") - scansBefore).as("answered by the walk and the delta merge, no completion scan")
          .isZero();
    } finally {
      database.rollback();
    }
  }

  private List<RID> createAndFill(final Random random) {
    return createAndFill(random, "BINARY", true);
  }

  private List<RID> createAndFill(final Random random, final String quantization, final boolean hierarchy) {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create().createProperty("v", Type.ARRAY_OF_FLOATS);
    database.command("sql",
        "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'COSINE', quantization: '"
            + quantization + "', addHierarchy: " + hierarchy + ", maxConnections: 16, beamWidth: 100, inactivityRebuildTimeoutMs: 3600000}")
        .close();
    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument("Doc").set("v", gaussian(random)).save().getIdentity());
    });
    return rids;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("v")).getIndexesOnBuckets()[0];
  }

  private static float[] gaussian(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) random.nextGaussian();
    return vector;
  }
}
