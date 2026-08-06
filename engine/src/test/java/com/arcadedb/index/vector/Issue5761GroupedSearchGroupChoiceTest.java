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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    assertThat(clusters).as("and it must answer with the three nearest groups the arc has, got %s", clusters)
        .containsExactlyInAnyOrder(0, 0, 1, 1, 2, 2);
  }

  /**
   * The other half of the contract, and the half a one-shot beam cannot keep. {@code limit} is a count of
   * <em>distinct groups</em>, so a query at cluster 1 with {@code limit=3} has to come back with three of them - the
   * arc puts clusters 0 and 2 next in line. Cluster 1 has a hundred members and every one of them outranks the
   * nearest member of any other cluster, so a candidate pool the width of the default beam holds cluster 1 and
   * nothing else: reading a fixed pool answers one group where three were asked for. The search has to keep walking
   * until the groups are filled, not stop at the first beam.
   */
  @Test
  void theAnswerCarriesAsManyGroupsAsWereAskedFor() {
    createSchema();
    insertVertices();

    final List<Integer> clusters = clustersOf(grouped(centerOf(1), LIMIT, GROUP_SIZE, -1));

    assertThat(clusters).as("limit counts distinct groups, and the arc has three to give here, got %s", clusters)
        .containsExactlyInAnyOrder(0, 0, 1, 1, 2, 2);
  }

  /**
   * {@code efSearch} stays a quality lever rather than a correctness requirement: a wider beam reaches the same
   * answer, it just gets there in one pass instead of several.
   */
  @Test
  void aWiderBeamReachesTheSameGroups() {
    createSchema();
    insertVertices();

    final List<Integer> wide = clustersOf(grouped(centerOf(1), LIMIT, GROUP_SIZE, 400));
    assertThat(wide).as("a pool of 400 reaches past cluster 1 into both its neighbours in one pass, got %s", wide)
        .containsExactlyInAnyOrder(0, 0, 1, 1, 2, 2);
  }

  /** Group choice has to be by score at every query point on the arc, not only at the one the defect was found on. */
  @Test
  void everyClusterIsItsOwnNearestGroup() {
    createSchema();
    insertVertices();

    for (int cluster = 0; cluster < CLUSTERS; cluster++) {
      final List<Integer> clusters = clustersOf(grouped(centerOf(cluster), LIMIT, GROUP_SIZE, -1));
      // The arc is ordered and the clusters are evenly spaced, so the LIMIT nearest groups to a query at the centre
      // of one of them are that cluster and its neighbours - clamped at the two ends of the arc, where the window
      // slides inwards because there is nothing further out to reach for.
      final int firstOfWindow = Math.min(Math.max(cluster - 1, 0), CLUSTERS - LIMIT);
      final List<Integer> expected = new ArrayList<>(LIMIT * GROUP_SIZE);
      for (int i = 0; i < LIMIT; i++)
        for (int j = 0; j < GROUP_SIZE; j++)
          expected.add(firstOfWindow + i);

      assertThat(clusters).as("a query at the centre of cluster %d must answer the %d groups nearest to it, got %s",
          cluster, LIMIT, clusters).containsExactlyInAnyOrderElementsOf(expected);
      assertThat(clusters.getFirst()).as("and the nearest row must belong to cluster %d itself", cluster)
          .isEqualTo(cluster);
    }
  }

  /**
   * The walk is resumed, not restarted: {@code GraphSearcher.resume} keeps the visited set, so a node the first pass
   * already returned can never come back in a later one. If it could, the per-group counters would be spent twice on
   * the same row and the answer would carry duplicates.
   */
  @Test
  void resumingTheWalkNeverReturnsTheSameRowTwice() {
    createSchema();
    insertVertices();

    // Deliberately more groups than the arc has clusters, so the loop runs until the graph is exhausted rather than
    // stopping early on a filled budget - the longest walk this fixture can produce.
    final List<Pair<RID, Float>> neighbors = grouped(centerOf(6), CLUSTERS + 4, GROUP_SIZE, -1);

    final List<RID> rids = new ArrayList<>(neighbors.size());
    for (final Pair<RID, Float> neighbor : neighbors)
      rids.add(neighbor.getFirst());

    assertThat(rids).as("a resumed pass must not re-offer a row the previous pass already admitted")
        .doesNotHaveDuplicates();
    // The declared contract is "ascending by distance", and it has to survive the pass boundary: a resumed pass can
    // expand a node the previous one left on the frontier and turn up a neighbour that outranks a row already
    // admitted, so the answer is sorted before it is returned.
    for (int i = 1; i < neighbors.size(); i++)
      assertThat(neighbors.get(i).getSecond()).as("row %d is nearer than row %d, so the answer is not sorted", i,
          i - 1).isGreaterThanOrEqualTo(neighbors.get(i - 1).getSecond());
  }

  /**
   * When the data cannot supply {@code limit} groups, the walk has to stop rather than resume forever. Every vertex
   * here carries the same group key, so no pass will ever open a second group; the search must come back with the one
   * group it can fill, and say so through {@code groupedSearchesShortOfLimit}.
   */
  @Test
  void aGroupKeyWithOneValueStopsInsteadOfWalkingTheWholeGraph() {
    createSchema();
    insertVertices();

    final long shortBefore = vectorIndex().getStats().get("groupedSearchesShortOfLimit");

    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorGrouped(embedding(centerOf(1)), LIMIT,
        GROUP_SIZE, -1, null, rid -> "one-and-only");

    assertThat(neighbors).as("the one group it can fill still has to come back, filled to groupSize")
        .hasSize(GROUP_SIZE);
    assertThat(vectorIndex().getStats().get("groupedSearchesShortOfLimit"))
        .as("a search that could not open limit groups has to be visible to the operator")
        .isEqualTo(shortBefore + 1);
  }

  /** A whitelist has to keep holding across the pass boundary, not just on the first beam. */
  @Test
  void theRidWhitelistSurvivesAResumedWalk() {
    createSchema();
    insertVertices();

    // Two members of cluster 4 and two of cluster 7, both far enough from the query that reaching them takes more
    // than the first beam.
    final Set<RID> allowed = new HashSet<>();
    for (final int vertex : new int[] { centerOf(4), centerOf(4) + 1, centerOf(7), centerOf(7) + 1 })
      allowed.add(ridOf("doc" + vertex));

    final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), LIMIT, GROUP_SIZE, -1, allowed);

    assertThat(neighbors).as("the whitelisted rows are reachable, so the walk has to find them").isNotEmpty();
    for (final Pair<RID, Float> neighbor : neighbors)
      assertThat(allowed).as("a row outside the whitelist leaked out of a resumed pass").contains(
          neighbor.getFirst().asVertex().getIdentity());
    assertThat(clustersOf(neighbors)).as("and the group cap still applies to what survives")
        .containsExactlyInAnyOrder(4, 4, 7, 7);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize, final int efSearch) {
    return grouped(vertex, limit, groupSize, efSearch, null);
  }

  private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize, final int efSearch,
      final Set<RID> allowedRIDs) {
    // The resolver receives the raw (unbound) RID the location map holds, exactly as the SQL function's does, so it
    // has to go through the database to read the record.
    return vectorIndex().findNeighborsFromVectorGrouped(embedding(vertex), limit, groupSize, efSearch, allowedRIDs,
        rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster"));
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
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
