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
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13262: a search clamped its {@code k} to the vectors the index addresses before it took its read locks. A commit that
 * landed between the clamp and the locks raised the live count above that {@code k}, so the search answered the nearest {@code k}
 * rows and an exhaustive one (k at or above the live count) came back short, with the farthest live records missing. The search now
 * takes its {@code k} again under the read lock, where no writer can move the counts.
 * <p>
 * The tests make the race deterministic: a hook runs after the early clamp and before the locks, and commits the rows there.
 */
class Issue13262ExhaustiveSearchWithConcurrentCommitTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int RECORDS    = 1100;
  private static final int WRITES     = 30;

  private final Random random = new Random(13262);

  @BeforeEach
  void keepTheGraphAsBuilt() {
    // The rows the hook commits stay in the delta buffer: no rebuild may absorb them behind the test's back
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
  @ValueSource(strings = { "NONE", "INT8" })
  void anExhaustiveSearchAnswersTheRowsCommittedBeforeItsLocks(final String quantization) {
    final LSMVectorIndex index = indexedVectors(quantization);
    final float[] query = gaussian();
    assertThat(index.findNeighborsFromVector(query, RECORDS + WRITES + 100)).as("the built graph answers every record").hasSize(RECORDS);

    commitOnNextSearch(index);

    final List<Pair<RID, Float>> rows = index.findNeighborsFromVector(query, RECORDS + WRITES + 100);
    assertThat(rows).as("the %d records and the %d committed while the search waited for its locks", RECORDS, WRITES)
        .hasSize(RECORDS + WRITES);
    assertThat(distinctRids(rows)).hasSize(RECORDS + WRITES);
  }

  /** The statement modelhub runs: the SQL function over the index, asked for far more neighbours than the index holds. */
  @Test
  void theSqlFunctionAnswersTheRowsCommittedBeforeItsLocks() {
    final LSMVectorIndex index = indexedVectors("INT8");
    final float[] query = gaussian();
    commitOnNextSearch(index);

    try (final ResultSet resultSet = database.query("sql",
        "SELECT count(*) AS rows FROM (SELECT expand(vector.neighbors('Doc[v]', :q, :k)))", Map.of("q", query, "k", 100_000))) {
      assertThat(resultSet.next().<Long>getProperty("rows")).isEqualTo(RECORDS + WRITES);
    }
  }

  /**
   * The approximate search is not guaranteed to answer every record of the index (with the count taken under the lock this very
   * search answered 1,128 and 1,129 of 1,130 in some runs), so the assertion is the part of the answer the stale count decided: it
   * is not cut at the {@link #RECORDS} the index held when the search read its counts.
   */
  @Test
  void anApproximateSearchIsNotCutAtTheCountReadBeforeItsLocks() {
    final LSMVectorIndex index = indexedVectors("PRODUCT");
    assertThat(index.isPQSearchAvailable()).isTrue();
    final float[] query = gaussian();
    commitOnNextSearch(index);

    final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorApproximate(query, RECORDS + WRITES + 100);
    assertThat(distinctRids(rows)).as("more than the %d records the index held when the search read its counts", RECORDS)
        .hasSizeGreaterThan(RECORDS);
  }

  /** Control: commits that are all in before the search starts were never a problem, and the hook is one-shot. */
  @Test
  void aSearchAfterTheCommitAnswersEveryRecord() {
    final LSMVectorIndex index = indexedVectors("INT8");
    final float[] query = gaussian();
    insert(WRITES);

    assertThat(index.findNeighborsFromVector(query, RECORDS + WRITES + 100)).hasSize(RECORDS + WRITES);
  }

  /** Commits {@link #WRITES} rows from another thread when the next search of the index is about to take its locks. */
  private void commitOnNextSearch(final LSMVectorIndex index) {
    index.setBeforeSearchLocksForTest(() -> {
      index.setBeforeSearchLocksForTest(null);
      try {
        CompletableFuture.runAsync(() -> insert(WRITES)).get(60, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private void insert(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newVertex("Doc").set("v", gaussian()).save();
    });
  }

  /** {@link #RECORDS} vectors of a single-bucket type and their index, its graph built, the delta buffer empty. */
  private LSMVectorIndex indexedVectors(final String quantization) {
    database.getSchema().buildVertexType().withName("Doc").withTotalBuckets(1).create().createProperty("v", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("Doc").set("v", gaussian()).save();
    });
    database.command("sql", "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'COSINE', quantization: '"
        + quantization + "', maxConnections: 16, beamWidth: 100, inactivityRebuildTimeoutMs: 3600000"
        + ("PRODUCT".equals(quantization) ? ", pqClusters: 16" : "") + "}").close();
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getType("Doc").getPolymorphicIndexByProperties("v"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    return index;
  }

  private static Set<RID> distinctRids(final List<Pair<RID, Float>> rows) {
    final Set<RID> rids = new HashSet<>();
    for (final Pair<RID, Float> row : rows)
      rids.add(row.getFirst());
    return rids;
  }

  private float[] gaussian() {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) random.nextGaussian();
    return vector;
  }
}
