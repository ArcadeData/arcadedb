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
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5558: a delete does not remove the vector from the HNSW graph, it only tombstones its location - the node
 * stays in the graph until the next rebuild, which for anything but a small index is thousands of mutations away. Two
 * things then went wrong on every query that walked into a tombstoned neighbourhood:
 * <ul>
 *   <li>JVector was told (via {@code Bits.ALL}) that every node was an acceptable result, so the beam filled with
 *       tombstones, saw a full result heap and stopped. The post-filter then dropped them one by one and the caller
 *       got back fewer results than it asked for - down to nothing at all when the live set was small.</li>
 *   <li>A tombstone was scored through a placeholder vector of {@code Float.MIN_NORMAL}s. Under COSINE its squared
 *       magnitude underflows to zero in float, so the similarity came back {@code Infinity}: every tombstone
 *       outranked every real vector in the beam, and JVector's own {@code 0 <= score <= 1} assertion tripped when
 *       assertions were on.</li>
 * </ul>
 * The fixture uses 12 well-separated clusters on an arc so the correct answer to a query aimed at a deleted cluster
 * is not "something", it is a specific surviving cluster, and it holds the rebuild threshold above the workload so
 * the graph keeps its tombstones for the whole test - which is the steady state of any index whose delete volume sits
 * under the threshold.
 * <p>
 * <b>Which test pins which half.</b> Worth knowing before touching either, because on this fixture <i>each half alone
 * is enough</i> - the end-to-end tests below stay green if you remove one of them, and they were verified by mutation
 * rather than assumed:
 * <ul>
 *   <li>The <b>accept-filter</b> is pinned by {@link #theApproximatePqSearchAnswersADeletedRegionQuery}, and only by
 *       it. The PQ path scores tombstones from their own PQ codes, so it is the one path where a floor score cannot
 *       stand in for the filter; restore {@code Bits.ALL} there and it returns the reported empty list.</li>
 *   <li>The <b>floor score</b> is pinned by {@link #anUnreadableNodeScoresTheFloorOfItsMetric}, and only by it. No
 *       end-to-end assertion can see it, because the filter removes a tombstone from the answer whatever it
 *       scored.</li>
 *   <li>Everything else pins that a search over a tombstoned graph returns the right vectors, without saying which
 *       half got it there.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5558DeletedRegionSearchTest extends TestHelper {

  private static final int   DIMENSIONS       = 16;
  private static final int   CLUSTERS         = 12;
  private static final int   PER_CLUSTER      = 100;
  private static final int   VERTICES         = CLUSTERS * PER_CLUSTER;
  private static final int   DELETED_CLUSTERS = 4;
  private static final int   DELETED          = DELETED_CLUSTERS * PER_CLUSTER;
  private static final int   K                = 5;
  // Clusters are 0.12 rad apart and a cluster is 0.01 rad wide, so "which cluster" is never a close call.
  private static final double CLUSTER_GAP     = 0.12;
  private static final double JITTER          = 0.0001;

  /** DOT_PRODUCT is only defined on unit-length vectors, so that test scales the fixture down to the unit sphere. */
  private boolean normalizeEmbeddings = false;

  @BeforeEach
  void freezeTheGraph() {
    // Keep the deleted vectors in the graph for the duration of the test: a rebuild would drop them and there would
    // be no hole left to search into. This is the configuration an index sees between rebuilds, not a special case.
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
   * The reported shape: the query lands where a block of vectors used to be. The right answer is the nearest cluster
   * that is still there - cluster 4, the first survivor - and never an empty list.
   */
  @Test
  void aQueryInsideADeletedRegionReturnsTheNearestSurvivingCluster() {
    createSchema();
    insertVertices();
    deleteFirstClusters();

    assertThat(vectorIndex().countEntries()).as("only the survivors are indexed").isEqualTo(VERTICES - DELETED);
    assertThat(vectorIndex().getGraphIndex().size())
        .as("and the graph still carries the tombstones, which is the state this test is about")
        .isEqualTo(VERTICES);

    // Counted across the query below rather than asserted as an absolute, so the assertion says "this query did not
    // fall back" and not "nothing in the fixture ever did" - the graph-building warm-up in insertVertices() is a
    // search too, and coupling to whether it fell back would make this depend on fixture ordering.
    final long scansBefore = (Long) vectorIndex().getStats().get("bruteForceScans");

    assertNeighborClusterIs(1, DELETED_CLUSTERS);

    // Stronger than "the answer is right": the answer came from the graph walk. The brute-force fallback would have
    // produced the same list by scanning all 1200 ordinals, which is what happens on main once the beam stops on
    // tombstones - so without this the test cannot tell a working search from a rescued one.
    //
    // This is the one assertion here that rides on HNSW recall rather than on a decidable property: it fails if the
    // beam under-fills k by even one, which a JVector or JVM change could do without anything being broken. If it
    // ever goes red on its own, check the assertion above first - while the returned cluster is still 4, this is
    // recall drift and not a regression, and the fix to make is the fixture (a wider cluster gap or a larger k), not
    // the search.
    assertThat((Long) vectorIndex().getStats().get("bruteForceScans") - scansBefore)
        .as("the beam must walk out of the deleted region on its own, not be rescued by a full scan").isEqualTo(0L);
  }

  /** The same query far from the hole. If this one ever fails, the fixture is broken rather than the search. */
  @Test
  void aQueryAwayFromTheDeletedRegionIsUnaffected() {
    createSchema();
    insertVertices();
    deleteFirstClusters();

    assertNeighborClusterIs(8, 8);
  }

  /**
   * The pathological end of the same defect: one vector survives. The beam fills with tombstones and stops, and the
   * brute-force safety net did not fire either - its "did we get less than 80% of what is available" guard evaluates
   * to {@code 0 < 0} for a single live vector. The answer was an empty list with the vector sitting right there.
   */
  @Test
  void aQueryAgainstASingleSurvivorFindsIt() {
    createSchema();
    insertVertices();

    final int survivor = VERTICES - 1;
    database.transaction(() -> {
      for (int i = 0; i < survivor; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });
    assertThat(vectorIndex().countEntries()).isEqualTo(1);

    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(0), K);
    assertThat(neighbors).as("one vector survives, so one is the answer, not zero").hasSize(1);
    assertThat(idOf(neighbors.getFirst().getFirst())).isEqualTo("doc" + survivor);
  }

  /** Same hole, through SQL, so the defect is pinned at the layer an application actually calls. */
  @Test
  void theSqlFunctionAnswersADeletedRegionQuery() {
    createSchema();
    insertVertices();
    deleteFirstClusters();

    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, " + K + ") AS neighbors FROM Doc LIMIT 1",
          (Object) embedding(centerOf(1)));
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).as("vector.neighbors must not answer an empty list while 800 vectors are live")
          .hasSize(K);
      for (final Map<String, Object> neighbor : neighbors)
        assertThat(clusterOf((String) neighbor.get("id"))).isEqualTo(DELETED_CLUSTERS);
    });
  }

  /**
   * The grouped path shares the traversal, so the tombstone scores reached it too - with assertions on it did not
   * return a wrong answer, it threw JVector's {@code 0 <= score <= 1} AssertionError out of the search. It has no
   * brute-force fallback to hide behind either, so this is the raw traversal.
   * <p>
   * The assertion is deliberately about liveness and not about which cluster wins: the grouped path spends its
   * distinct-group budget in traversal order rather than score order, so the groups it picks are the ones the beam
   * met on its way in. That is out of scope here (see the {@code findNeighborsFromVectorGrouped} contract, which
   * documents the admission as approximate) and is tracked separately.
   */
  @Test
  void theGroupedSearchAnswersADeletedRegionQuery() {
    createSchema();
    insertVertices();
    deleteFirstClusters();

    // The resolver receives the raw (unbound) RID the location map holds, exactly as the SQL function's does, so it
    // has to go through the database to read the record.
    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorGrouped(embedding(centerOf(1)), 3, 2,
        -1, null, rid -> clusterOf(((Document) database.lookupByRID(rid, true)).getString("id")));
    assertThat(neighbors).as("the grouped path has no brute-force fallback: an empty answer here is the raw defect")
        .isNotEmpty();
    for (final Pair<RID, Float> neighbor : neighbors)
      assertThat(clusterOf(idOf(neighbor.getFirst()))).as("no deleted vector may come back").isGreaterThanOrEqualTo(
          DELETED_CLUSTERS);
  }

  /**
   * The zero-disk-I/O PQ path, which {@code select().vectorNeighbors()} uses. It scores from PQ codes, so a tombstone
   * never produced a non-finite number there - the beam simply filled with tombstones, stopped, and the post-filter
   * emptied the list. With no delta scan and no brute-force fallback on this path, that empty list is what the caller
   * got: the reported symptom in its purest form.
   */
  @Test
  void theApproximatePqSearchAnswersADeletedRegionQuery() {
    createSchema(VectorQuantizationType.PRODUCT);
    insertVertices();
    vectorIndex().buildVectorGraphNow();
    assertThat(vectorIndex().isPQSearchAvailable()).as("the fixture has to actually train PQ").isTrue();

    deleteFirstClusters();

    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorApproximate(embedding(centerOf(1)), K,
        null);
    assertThat(neighbors).as("the PQ path has nothing to fall back on: an empty answer here is the reported defect")
        .hasSize(K);
    for (final Pair<RID, Float> neighbor : neighbors)
      assertThat(clusterOf(idOf(neighbor.getFirst()))).as("no deleted vector may come back")
          .isGreaterThanOrEqualTo(DELETED_CLUSTERS);
  }

  /**
   * The score a tombstone gets, asserted directly. It cannot be asserted through search results - the
   * {@link LiveVectorBitsFilter} keeps a tombstone out of the answer whatever it scored, which is exactly why the two
   * halves of the fix need separate tests: set {@code UNREADABLE_NODE_SCORE} to {@code 1.0f} and every end-to-end test
   * in this class still passes.
   * <p>
   * Two claims are pinned per metric. That an unreadable node scores the floor, {@code 0} - {@code Infinity} was the
   * defect and any finite-but-high value is the same defect more quietly, because it puts the tombstone ahead of real
   * candidates in the beam and wastes the search budget on it. And that the floor really is below every real score,
   * which is the property that lets one constant serve all three metrics.
   */
  @Test
  void anUnreadableNodeScoresTheFloorOfItsMetric() {
    for (final String similarity : List.of("COSINE", "EUCLIDEAN", "DOT_PRODUCT")) {
      normalizeEmbeddings = "DOT_PRODUCT".equals(similarity);
      dropSchema();
      createSchema(VectorQuantizationType.INT8, similarity);
      insertVertices();
      deleteFirstClusters();

      final LSMVectorIndex index = vectorIndex();
      final int[] ordinalMap = index.getOrdinalToVectorIdForTest();
      final float[] query = embedding(centerOf(1));

      int tombstoned = -1;
      int live = -1;
      for (int ordinal = 0; ordinal < ordinalMap.length && (tombstoned < 0 || live < 0); ordinal++) {
        final VectorLocationIndex.VectorLocation loc = index.getVectorIndex().getLocation(ordinalMap[ordinal]);
        if (loc == null || loc.deleted)
          tombstoned = tombstoned < 0 ? ordinal : tombstoned;
        else
          live = live < 0 ? ordinal : live;
      }

      // Without both of these the assertions below would hold vacuously.
      assertThat(tombstoned).as("%s: the graph has to still carry a tombstoned ordinal", similarity).isNotNegative();
      assertThat(live).as("%s: and a live one to compare it against", similarity).isNotNegative();

      final float tombstoneScore = index.scoreOrdinalForTest(query, tombstoned);
      final float liveScore = index.scoreOrdinalForTest(query, live);

      assertThat(tombstoneScore).as("%s: a node whose vector cannot be read scores the floor", similarity)
          .isEqualTo(0.0f);
      assertThat(liveScore).as("%s: a real vector scores a finite number", similarity).isFinite();
      assertThat(liveScore).as("%s: and strictly above the floor, so the tombstone cannot outrank it", similarity)
          .isGreaterThan(tombstoneScore);
    }
  }

  /**
   * The same deleted region under the two metrics the {@code Infinity} defect never reached - it came out of cosine
   * cancelling the magnitude, so these two pass on {@code main} as well. They are here to hold the end-to-end
   * behaviour of a non-cosine index, not to reproduce the bug; what pins the floor score itself is
   * {@link #anUnreadableNodeScoresTheFloorOfItsMetric}.
   * <p>
   * The arc embedding has the same magnitude at every vertex, so both metrics order the clusters exactly as cosine
   * does and the expected answer is unchanged. The DOT_PRODUCT fixture is scaled onto the unit sphere, which is
   * JVector's documented precondition for that metric and the condition under which its floor really is 0.
   */
  @Test
  void aDeletedRegionQueryUnderEuclidean() {
    createSchema(VectorQuantizationType.INT8, "EUCLIDEAN");
    insertVertices();
    deleteFirstClusters();

    assertNeighborClusterIs(1, DELETED_CLUSTERS);
  }

  @Test
  void aDeletedRegionQueryUnderDotProduct() {
    normalizeEmbeddings = true;
    createSchema(VectorQuantizationType.INT8, "DOT_PRODUCT");
    insertVertices();
    deleteFirstClusters();

    assertNeighborClusterIs(1, DELETED_CLUSTERS);
  }

  /**
   * A RID allow-list narrower than {@code k} caps what any answer could contain, so the search must return the whole
   * allow-list and not one entry more - including when the allow-list points into the deleted region, where the only
   * right answer is the part of it that survived.
   */
  @Test
  void anAllowListNarrowerThanKIsAnsweredInFull() {
    createSchema();
    insertVertices();

    // Three survivors from the cluster next to the hole, plus two that the delete is about to take - captured now,
    // because a deleted vertex has no RID left to look up.
    final Set<RID> allowed = new HashSet<>();
    for (final int vertex : new int[] { centerOf(DELETED_CLUSTERS), centerOf(DELETED_CLUSTERS) + 1,
        centerOf(DELETED_CLUSTERS) + 2, centerOf(0), centerOf(1) })
      allowed.add(ridOf("doc" + vertex));

    deleteFirstClusters();

    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(centerOf(1)), K, -1,
        allowed);

    assertThat(neighbors).as("three of the five allowed vectors are still alive, so three is the answer").hasSize(3);
    for (final Pair<RID, Float> neighbor : neighbors) {
      assertThat(allowed).as("nothing outside the allow-list may come back").contains(neighbor.getFirst());
      assertThat(clusterOf(idOf(neighbor.getFirst()))).as("and nothing deleted").isEqualTo(DELETED_CLUSTERS);
    }
  }

  /** A reopen reloads the graph from disk, so the hole has to stay searchable across it. */
  @Test
  void theDeletedRegionStaysSearchableAfterAReopen() {
    createSchema();
    insertVertices();
    deleteFirstClusters();

    reopenDatabase();

    assertThat(vectorIndex().countEntries()).isEqualTo(VERTICES - DELETED);
    assertNeighborClusterIs(1, DELETED_CLUSTERS);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void assertNeighborClusterIs(final int queryCluster, final int expectedCluster) {
    final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVector(embedding(centerOf(queryCluster)), K);
    assertThat(neighbors).as("a query at cluster %d must find the surviving vectors", queryCluster).hasSize(K);

    final List<String> ids = new ArrayList<>(K);
    for (final Pair<RID, Float> neighbor : neighbors)
      ids.add(idOf(neighbor.getFirst()));
    for (final String id : ids)
      assertThat(clusterOf(id)).as("cluster %d is the nearest surviving one, got %s", expectedCluster, ids)
          .isEqualTo(expectedCluster);
  }

  private void dropSchema() {
    if (database.getSchema().existsType("Doc"))
      database.transaction(() -> database.getSchema().dropType("Doc"));
  }

  private void createSchema() {
    createSchema(VectorQuantizationType.INT8, "COSINE");
  }

  private void createSchema(final VectorQuantizationType quantization) {
    createSchema(quantization, "COSINE");
  }

  private void createSchema(final VectorQuantizationType quantization, final String similarity) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(quantization).withSimilarity(similarity).create();
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });
    // The graph is built on the first search, and it has to come out above ASYNC_REBUILD_MIN_GRAPH_SIZE or every
    // later search would rebuild it synchronously and there would be no stale graph left to test.
    vectorIndex().findNeighborsFromVector(embedding(centerOf(0)), 1);
  }

  private void deleteFirstClusters() {
    database.transaction(() -> {
      for (int i = 0; i < DELETED; i++)
        database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + i);
    });
  }

  private RID ridOf(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
      return rs.next().getIdentity().orElseThrow();
    }
  }

  private String idOf(final RID rid) {
    return rid.asVertex().getString("id");
  }

  private static int centerOf(final int cluster) {
    return cluster * PER_CLUSTER + PER_CLUSTER / 2;
  }

  private static int clusterOf(final String id) {
    return Integer.parseInt(id.substring("doc".length())) / PER_CLUSTER;
  }

  /**
   * Points on an arc, embedded with damped harmonics so that cosine similarity is a strictly decreasing function of
   * the angular gap: {@code sum(cos(m*delta)/m^2)} has derivative {@code -(pi-delta)/2 < 0} on {@code (0, pi)}. The
   * nearest surviving vector to a query is therefore decidable on paper, and with the clusters an order of magnitude
   * further apart than they are wide, INT8 quantization noise cannot move the answer to another cluster.
   */
  private float[] embedding(final int vertex) {
    final float[] v = new float[DIMENSIONS];
    final double theta = (vertex / PER_CLUSTER) * CLUSTER_GAP + (vertex % PER_CLUSTER - PER_CLUSTER / 2.0) * JITTER;
    double magnitude = 0;
    for (int m = 1; m <= DIMENSIONS / 2; m++)
      magnitude += 2.0 / (m * (double) m);
    final double scale = normalizeEmbeddings ? 1 / Math.sqrt(magnitude) : 1;
    for (int m = 1; m <= DIMENSIONS / 2; m++) {
      v[(m - 1) * 2] = (float) (scale * Math.cos(m * theta) / m);
      v[(m - 1) * 2 + 1] = (float) (scale * Math.sin(m * theta) / m);
    }
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }
}
