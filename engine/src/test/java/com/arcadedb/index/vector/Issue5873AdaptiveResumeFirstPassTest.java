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
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5873: the adaptive-{@code efSearch} branch of {@code findNeighborsFromVector} answered a short first pass by
 * replacing it with {@code searcher.resume(...)} rather than adding to it, and the resume could only ever return
 * nothing.
 * <p>
 * The branch is gated on {@code firstPass.getNodes().length < k}. JVector's {@code searchOneLayer} breaks early only
 * when its result queue has reached {@code rerankK} candidates, and {@code rerankK >= k} here, so a first pass that
 * came back with fewer than {@code k} cannot have stopped early: it ran its candidate queue dry, having expanded
 * every node reachable from the entry point. {@code resume} pushes the (empty) evicted pile back onto that same empty
 * queue and returns immediately with zero nodes - and for the same reason a wider beam would not have helped either,
 * because width is not what ran out. So the branch threw away a correct partial answer, and the brute-force fallback
 * then walked every ordinal in the index to reconstruct what the graph had already found.
 * <p>
 * The fixture is a graph whose live vectors are far fewer than {@code k}: 1500 vectors with all but four deleted, and
 * rebuilds frozen so the tombstones stay in the graph. That is the shape the branch needs - plenty of graph, almost
 * nothing admissible - and it is the shape a tombstone-heavy index has between rebuilds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5873AdaptiveResumeFirstPassTest extends TestHelper {

  private static final int DIMENSIONS = 8;
  private static final int VERTICES   = 1500;
  private static final int SURVIVORS  = 4;
  private static final int K          = 10;

  @BeforeEach
  void freezeTheGraph() {
    // Keep the deleted vectors in the graph: a rebuild would drop them, the graph would shrink below k and the
    // branch under test would never be reached. This is what an index looks like between rebuilds.
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

  /**
   * What the graph search found has to reach the caller, and the brute-force scan must not be asked to find it again.
   * The scan walks every ordinal in the index - the most expensive thing this index does - so firing it to recover
   * rows the search had already produced is pure waste, and it also makes {@code bruteForceScans} report a degraded
   * graph when the graph did its job.
   */
  @Test
  void aShortFirstPassIsKeptInsteadOfBeingScannedForAgain() {
    createSchema();
    insertVertices();
    final List<RID> survivors = deleteAllButTheSurvivors();

    // Preconditions: without these the test would pass without ever reaching the branch it is about.
    assertThat(vectorIndex().getStats().get("graphNodeCount"))
        .as("the graph has to hold at least k nodes, or the branch's own gate is not met").isGreaterThanOrEqualTo(K);
    assertThat(vectorIndex().getStats().get("activeVectors"))
        .as("and fewer than k of them may be live, or the first pass would not come back short")
        .isEqualTo(SURVIVORS).isLessThan(K);

    final long scansBefore = vectorIndex().getStats().get("bruteForceScans");
    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(0), K);

    assertThat(ridsOf(neighbors)).as("every live vector is reachable and has to come back, got %s", neighbors)
        .containsExactlyInAnyOrderElementsOf(survivors);
    assertThat(vectorIndex().getStats().get("bruteForceScans"))
        .as("the graph search already had the whole answer, so nothing may walk the index to find it again")
        .isEqualTo(scansBefore);
  }

  /** Distances still come back ascending, and no row is duplicated by the path that keeps the first pass. */
  @Test
  void theKeptFirstPassIsStillARankedAnswer() {
    createSchema();
    insertVertices();
    deleteAllButTheSurvivors();

    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(0), K);

    assertThat(ridsOf(neighbors)).as("a row kept from the first pass must not also be added by anything downstream")
        .doesNotHaveDuplicates();
    for (int i = 1; i < neighbors.size(); i++)
      assertThat(neighbors.get(i).getSecond()).as("row %d is nearer than row %d, so the answer is not sorted", i, i - 1)
          .isGreaterThanOrEqualTo(neighbors.get(i - 1).getSecond());
  }

  /**
   * The healthy case has to be untouched: with every vector live, a query for k gets k, and still no scan. This is
   * what pins the change to the short-answer branch rather than to the search as a whole.
   */
  @Test
  void aFullyLiveIndexIsUnaffected() {
    createSchema();
    insertVertices();

    final long scansBefore = vectorIndex().getStats().get("bruteForceScans");
    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(0), K);

    assertThat(neighbors).as("a healthy graph answers a k-NN query in full").hasSize(K);
    assertThat(vectorIndex().getStats().get("bruteForceScans")).as("and does not fall back").isEqualTo(scansBefore);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private List<RID> ridsOf(final List<Pair<RID, Float>> neighbors) {
    final List<RID> rids = new ArrayList<>(neighbors.size());
    for (final Pair<RID, Float> neighbor : neighbors)
      rids.add(neighbor.getFirst().getIdentity());
    return rids;
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");
      database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType()
          .withDimensions(DIMENSIONS).withSimilarity("COSINE").create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });
    // The graph is built on the first search, and only above ASYNC_REBUILD_MIN_GRAPH_SIZE - below it every query is
    // answered by the delta scan and the graph search this test is about never runs at all.
    vectorIndex().findNeighborsFromVector(embedding(0), 1);
  }

  private List<RID> deleteAllButTheSurvivors() {
    final List<RID> survivors = new ArrayList<>(SURVIVORS);
    database.transaction(() -> {
      for (int i = 0; i < SURVIVORS; i++)
        survivors.add(ridOf("doc" + i));
      for (int i = SURVIVORS; i < VERTICES; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });
    return survivors;
  }

  private RID ridOf(final String id) {
    try (final var rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  /** Points on an arc, so "nearest" is decidable and the survivors are the four closest to the query at vertex 0. */
  private float[] embedding(final int vertex) {
    final float[] v = new float[DIMENSIONS];
    final double theta = vertex * 0.001;
    for (int m = 1; m <= DIMENSIONS / 2; m++) {
      v[(m - 1) * 2] = (float) (Math.cos(m * theta) / m);
      v[(m - 1) * 2 + 1] = (float) (Math.sin(m * theta) / m);
    }
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
