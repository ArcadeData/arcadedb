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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The issue #7378 overlay on the zero-disk-I/O PQ path, which has its own graph walk, its own post-filter and its
 * own delta merge ({@code mergeWithDeltaScanApproximate}) and so cannot inherit the exact path's coverage. The
 * fixture is deliberately the one {@code Issue6559ApproximateDeltaScoringScaleTest} uses - enough vectors, and
 * {@code quantization: PRODUCT} - because below that the method falls back to the exact search and the test would
 * pass without ever reaching the code it names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7378ApproximateSearchReadsOwnWritesTest extends TestHelper {

  private static final int DIMENSIONS  = 32;
  private static final int VECTORS     = 400;
  private static final int PQ_CLUSTERS = 16;
  private static final int TOP_K       = 10;

  @BeforeEach
  void freezeTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  /** The PQ graph walk plus {@code mergeWithDeltaScanApproximate}. */
  @Test
  void theApproximateSearchScoresARowInsertedInTheOpenTransaction() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();
    assertThat(index.isPQSearchAvailable()).as("the fixture is only meaningful while the PQ path is really taken")
        .isTrue();

    // A copy of an indexed vector: the pending row is then at the same point in space as its committed twin and
    // cannot fail to be inside a top-10 that already contains the twin.
    final float[] query = vector(17);

    database.begin();
    try {
      database.newDocument("Doc").set("id", "created-in-tx").set("embedding", query).save();

      assertThat(idsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null)))
          .as("the PQ path must resolve the caller's own uncommitted row too")
          .contains("created-in-tx");
    } finally {
      database.rollback();
    }
  }

  /** The issue #6514 pre-filter plan, the approximate path's second way of answering a query. */
  @Test
  void theApproximatePreFilterPlanScoresARowInsertedInTheOpenTransaction() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();

    final float[] query = vector(17);

    database.begin();
    try {
      final RID pending = database.newDocument("Doc").set("id", "created-in-tx").set("embedding", query).save()
          .getIdentity();

      final Set<RID> allowed = new HashSet<>();
      allowed.add(pending);
      allowed.add(ridOf("doc17"));

      final long preFilterBefore = index.getStats().get("preFilterSearches");
      final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, allowed);
      assertThat(index.getStats().get("preFilterSearches")).as("the fixture has to reach the pre-filter plan")
          .isGreaterThan(preFilterBefore);

      assertThat(idsOf(results)).as("an allow-list of rows the caller just wrote must still resolve them")
          .contains("created-in-tx");
    } finally {
      database.rollback();
    }
  }

  /** The symmetric half on this path: the committed vector of a row the transaction deleted must not come back. */
  @Test
  void theApproximateSearchDropsARowDeletedInTheOpenTransaction() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();

    final float[] query = vector(17);
    final RID doc17 = ridOf("doc17");
    assertThat(ridsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null))).as("precondition")
        .contains(doc17);

    database.begin();
    try {
      database.command("sql", "DELETE FROM Doc WHERE id = 'doc17'");

      assertThat(ridsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null)))
          .as("the PQ path must not return the committed vector of a row this transaction deleted")
          .doesNotContain(doc17);
    } finally {
      database.rollback();
    }
  }

  /**
   * The overlay superseding a row that is still in the delta buffer rather than in the graph - the one combination
   * of the two structures {@code TransactionVectorOverlay.augment()} handles that nothing else here reaches.
   * <p>
   * Every other test in this PR supersedes a graph-resident row, so {@code augment()}'s filter over the committed
   * buffer - the branch that drops a buffered entry whose RID this transaction has rewritten - was reachable but
   * never actually matched anything.
   * <p>
   * It lives on the approximate path on purpose. {@code findNeighborsFromVectorApproximate} is the one search that
   * never calls {@code rebuildGraphBeforeSearch()}, and on a graph below {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} that
   * method rebuilds synchronously and drains the buffer - so on the exact path the row is in the graph by the time
   * the assertion runs and the test would pass without ever reaching the branch it names. The delta count is
   * asserted on both sides of the search for exactly that reason.
   */
  @Test
  void aBufferedRowTheTransactionRewritesIsNotAlsoReturnedFromTheBuffer() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();

    final float[] query = vector(17);

    // Committed after the graph was built, and the graph is frozen, so this row lives in the buffer and nowhere
    // else.
    //
    // Its committed vector is deliberately NEAR the query, not far from it: if it were far, a build that failed to
    // drop it from the merged buffer would simply rank it outside TOP_K and the "only once" assertion below would
    // hold for the wrong reason. Near enough to be inside TOP_K, and distinct enough from the pending vector to be
    // a second row rather than the same one, is what makes this test able to fail.
    database.transaction(() -> database.newDocument("Doc").set("id", "buffered").set("embedding", nearly(query))
        .save());
    assertThat(index.getStats().get("deltaVectorsCount"))
        .as("the fixture is only a regression test while the row is in the buffer rather than the graph")
        .isEqualTo(1L);

    database.begin();
    try {
      database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = 'buffered'", (Object) query);

      final List<String> ids = idsOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null));

      assertThat(index.getStats().get("deltaVectorsCount"))
          .as("the search must not have drained the buffer, or this test proves nothing about augment()")
          .isEqualTo(1L);
      assertThat(ids).as("the rewritten row is scored on the vector this transaction wrote")
          .contains("buffered");
      assertThat(ids).as("and the buffer's own copy of it must not come back alongside the pending one")
          .containsOnlyOnce("buffered");
    } finally {
      database.rollback();
    }
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchemaAndData() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      for (int i = 0; i < VECTORS; i++)
        database.newDocument("Doc").set("id", "doc" + i).set("embedding", vector(i)).save();
    });

    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
          "dimensions": %d,
          "similarity": "COSINE",
          "quantization": "PRODUCT",
          "pqClusters": %d
        }""".formatted(DIMENSIONS, PQ_CLUSTERS));
  }

  /** A vector a short step from {@code v}: close enough to rank alongside it, far enough to be a distinct row. */
  private static float[] nearly(final float[] v) {
    final float[] near = new float[v.length];
    for (int i = 0; i < v.length; i++)
      near[i] = v[i] + 0.02f;
    return near;
  }

  private static float[] vector(final int seed) {
    final Random rnd = new Random(seed * 1_000_003L);
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = rnd.nextFloat() * 2 - 1;
    return v;
  }

  private static List<RID> ridsOf(final List<Pair<RID, Float>> results) {
    final List<RID> rids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      rids.add(r.getFirst());
    return rids;
  }

  private List<String> idsOf(final List<Pair<RID, Float>> results) {
    final List<String> ids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      ids.add(((Document) database.lookupByRID(r.getFirst(), true)).getString("id"));
    return ids;
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
