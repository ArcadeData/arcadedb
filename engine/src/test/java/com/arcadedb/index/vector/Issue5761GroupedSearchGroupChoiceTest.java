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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5761: the {@code groupBy} cap used to be pushed into the HNSW traversal through a group-aware {@code Bits}
 * filter, which is score-blind - JVector calls it on each popped candidate, before the score function runs. The first
 * candidates popped are the entry-point descent, and the descent starts far from the query, so the {@code limit}
 * distinct-group slots were handed out to whatever the beam met on its way in. By the time it arrived at the query the
 * budget was gone and the nearest group was locked out of its own answer: on this fixture a query aimed at the centre
 * of cluster 1 came back with clusters 3, 4 and 8, while the ungrouped search over the same index on the same query
 * answered cluster 1 five times out of five.
 * <p>
 * Nothing here is deleted and nothing is stale. The fixture is the arc from {@link Issue5558DeletedRegionSearchTest}
 * with the clusters an order of magnitude further apart than they are wide, so "which cluster is nearest" is decidable
 * on paper and is not an artefact of quantization noise or of a wobble in HNSW recall.
 */
class Issue5761GroupedSearchGroupChoiceTest extends TestHelper {

  private static final int    DIMENSIONS  = 16;
  private static final int    CLUSTERS    = 12;
  private static final int    PER_CLUSTER = 100;
  private static final int    VERTICES    = CLUSTERS * PER_CLUSTER;
  private static final int    LIMIT       = 3;
  private static final int    GROUP_SIZE  = 2;
  // Clusters are 0.12 rad apart and a cluster is 0.01 rad wide, so "which cluster" is never a close call.
  private static final double CLUSTER_GAP = 0.12;
  private static final double JITTER      = 0.0001;

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

  /**
   * The reported defect. Cluster 1 is what the query is: every one of its hundred members is nearer to it than
   * anything else in the index, so it cannot be missing from the answer whatever the group budget does, and whatever
   * else comes back has to be one of its neighbours on the arc.
   */
  @Test
  void theNearestGroupIsInTheAnswer() {
    createSchema();
    insertVertices();

    final List<Integer> clusters = clustersOf(grouped(centerOf(1), LIMIT, GROUP_SIZE, -1));

    assertThat(clusters).as("the group the query sits in cannot be absent from its own answer, got %s", clusters)
        .contains(1);
    assertThat(clusters).as("and no group further out than the arc neighbours may take a slot, got %s", clusters)
        .isSubsetOf(0, 1, 2);
    assertThat(clusters).as("the per-group cap still holds").filteredOn(c -> c == 1).hasSizeLessThanOrEqualTo(
        GROUP_SIZE);
  }

  /**
   * The same query through {@code vector.neighbors}, which is the layer an application calls. The SQL function
   * re-applies its own group admission on top of what the index returned, so it can only ever narrow that answer: if
   * the index locked cluster 1 out, no amount of post-filtering puts it back.
   */
  @Test
  void theSqlFunctionAnswersWithTheNearestGroup() {
    createSchema();
    insertVertices();

    final List<Integer> clusters = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.neighbors`('Doc[embedding]', ?, " + LIMIT + ", { groupBy: 'cluster', groupSize: "
              + GROUP_SIZE + " }))", (Object) embedding(centerOf(1)))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          clusters.add(((Number) row.getProperty("cluster")).intValue());
        }
      }
    });

    assertThat(clusters).as("vector.neighbors must not answer a query at cluster 1 without cluster 1, got %s", clusters)
        .contains(1);
    assertThat(clusters).as("got %s", clusters).isSubsetOf(0, 1, 2);
  }

  /**
   * The cost of moving the cap out of the traversal, and the lever that pays it. Cluster 1 has a hundred members and
   * every one of them outranks the nearest member of any other cluster, so a candidate pool narrower than that holds
   * one group and one group only - the answer is right, but it is one group where three were asked for. Widening the
   * beam widens the pool, and the three nearest clusters come back.
   * <p>
   * This is the assertion that pins the pool to the beam. With the pool fixed at {@code limit * groupSize * 2} the
   * search returns cluster 1 twice at every {@code efSearch}, because {@code efSearch} only widens the beam and the
   * beam is not what is being read.
   */
  @Test
  void aWiderBeamWidensTheGroupCoverage() {
    createSchema();
    insertVertices();

    final List<Integer> narrow = clustersOf(grouped(centerOf(1), LIMIT, GROUP_SIZE, -1));
    assertThat(narrow).as("the default pool is a hundred candidates and cluster 1 fills it on its own, got %s", narrow)
        .containsOnly(1);

    final List<Integer> wide = clustersOf(grouped(centerOf(1), LIMIT, GROUP_SIZE, 400));
    assertThat(wide).as("a pool of 400 reaches past cluster 1 into both its neighbours, got %s", wide)
        .containsExactlyInAnyOrder(0, 0, 1, 1, 2, 2);
  }

  /** Group choice has to be by score at every query point on the arc, not only at the one the defect was found on. */
  @Test
  void everyClusterIsItsOwnNearestGroup() {
    createSchema();
    insertVertices();

    for (int cluster = 0; cluster < CLUSTERS; cluster++) {
      final List<Integer> clusters = clustersOf(grouped(centerOf(cluster), LIMIT, GROUP_SIZE, -1));
      assertThat(clusters).as("a query at the centre of cluster %d must answer cluster %d, got %s", cluster, cluster,
          clusters).containsOnly(cluster);
    }
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize, final int efSearch) {
    // The resolver receives the raw (unbound) RID the location map holds, exactly as the SQL function's does, so it
    // has to go through the database to read the record.
    return vectorIndex().findNeighborsFromVectorGrouped(embedding(vertex), limit, groupSize, efSearch, null,
        rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster"));
  }

  private List<Integer> clustersOf(final List<Pair<RID, Float>> neighbors) {
    final List<Integer> clusters = new ArrayList<>(neighbors.size());
    for (final Pair<RID, Float> neighbor : neighbors)
      clusters.add(neighbor.getFirst().asVertex().getInteger("cluster"));
    return clusters;
  }

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.cluster INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.INT8).withSimilarity("COSINE")
          .create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, cluster = ?, embedding = ?", "doc" + i, i / PER_CLUSTER,
            embedding(i));
    });
    // The graph is built on the first search, and it has to come out above ASYNC_REBUILD_MIN_GRAPH_SIZE or every
    // later search would rebuild it synchronously.
    vectorIndex().findNeighborsFromVector(embedding(centerOf(0)), 1);
  }

  private static int centerOf(final int cluster) {
    return cluster * PER_CLUSTER + PER_CLUSTER / 2;
  }

  /**
   * Points on an arc, embedded with damped harmonics so that cosine similarity is a strictly decreasing function of
   * the angular gap: {@code sum(cos(m*delta)/m^2)} has derivative {@code -(pi-delta)/2 < 0} on {@code (0, pi)}. The
   * nearest cluster to a query is therefore decidable on paper, and with the clusters an order of magnitude further
   * apart than they are wide, INT8 quantization noise cannot move the answer to another cluster.
   */
  private float[] embedding(final int vertex) {
    final float[] v = new float[DIMENSIONS];
    final double theta = (vertex / PER_CLUSTER) * CLUSTER_GAP + (vertex % PER_CLUSTER - PER_CLUSTER / 2.0) * JITTER;
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
