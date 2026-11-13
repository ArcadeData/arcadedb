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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Correctness of search results after deletes, deltas and scalar queries. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorSearchCorrectnessTest {
  /**
   * Regression for #5558: deleted region search.
   * <p>
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
  @Nested
  class DeletedRegionSearch extends TestHelper {

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
     * It also says which cluster wins, which it could not do until #5761 was fixed: the grouped path used to spend its
     * distinct-group budget in traversal order rather than score order, so the groups it picked were whichever ones the
     * beam met on its way in, and one of the three was always a cluster that has no business being in the answer.
     * Group admission now runs on the score-ordered output of a walk that resumes until the groups are filled, so the
     * three groups are the three nearest surviving clusters, in that order, and nothing that was deleted comes back.
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

      final List<Integer> clusters = new ArrayList<>(neighbors.size());
      for (final Pair<RID, Float> neighbor : neighbors)
        clusters.add(clusterOf(idOf(neighbor.getFirst())));
      assertThat(clusters).as("the three nearest surviving clusters are the groups, and nothing deleted may come back, "
          + "got %s", clusters)
          .containsExactlyInAnyOrder(DELETED_CLUSTERS, DELETED_CLUSTERS, DELETED_CLUSTERS + 1, DELETED_CLUSTERS + 1,
              DELETED_CLUSTERS + 2, DELETED_CLUSTERS + 2);
      assertThat(clusters).as("three groups of two, which is what limit=3 groupSize=2 asks for").hasSize(6);
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

  /**
   * Regression for #5873: adaptive resume first pass.
   * <p>
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
  @Nested
  class AdaptiveResumeFirstPass extends TestHelper {

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

  /**
   * Regression for #10938: array of floats scalar.
   * <p>
   * Issue #10938: an {@code ARRAY_OF_FLOATS} property accepted a scalar (and any other value that is not an array of numbers),
   * stored it as it came, and a later {@code CREATE INDEX ... LSM_VECTOR} skipped that row with a SEVERE log line and reported
   * success with an index that silently lacked it. A write to a declared {@code ARRAY_OF_*} property now refuses a value that is
   * not an array of numbers, whether or not an index exists yet. A vector index build still leaves out the rows it cannot index,
   * as Neo4j does (#10863), but says so: one warning naming them, the count in the CREATE INDEX / REBUILD INDEX result and in
   * the index statistics.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ArrayOfFloatsScalar extends TestHelper {

    @Test
    void aScalarIsRefusedForAnArrayOfFloatsPropertyWithoutAnyIndex() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.embedding ARRAY_OF_FLOATS");

      database.transaction(() -> {
        assertThatThrownBy(() -> database.command("sql", "INSERT INTO D SET embedding = ?", 0.5d))//
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
        assertThatThrownBy(() -> database.newDocument("D").set("embedding", 0.5f).save())//
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
        assertThatThrownBy(() -> database.newDocument("D").set("embedding", "[0.1, 0.2]").save())//
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
        assertThatThrownBy(() -> database.newDocument("D").set("embedding", Map.of("a", 1)).save())//
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
      });

      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM D")) {
        assertThat(rs.next().<Long>getProperty("c")).isZero();
      }
    }

    @Test
    void everyArrayTypeRefusesAScalarAndTakesEveryArrayOfNumbers() {
      final DocumentType type = database.getSchema().createDocumentType("A");
      final Map<String, Type> properties = new HashMap<>();
      properties.put("shorts", Type.ARRAY_OF_SHORTS);
      properties.put("ints", Type.ARRAY_OF_INTEGERS);
      properties.put("longs", Type.ARRAY_OF_LONGS);
      properties.put("floats", Type.ARRAY_OF_FLOATS);
      properties.put("doubles", Type.ARRAY_OF_DOUBLES);
      for (final Map.Entry<String, Type> entry : properties.entrySet())
        type.createProperty(entry.getKey(), entry.getValue());

      final Object[] arraysOfNumbers = { new short[] { 1, 2 }, new int[] { 1, 2 }, new long[] { 1, 2 }, new float[] { 1, 2 },
          new double[] { 1, 2 }, new Short[] { 1, 2 }, new Integer[] { 1, 2 }, new Long[] { 1L, 2L }, new Float[] { 1f, 2f },
          new Double[] { 1d, 2d }, new Number[] { 1, 2L }, new Object[] { 1, 2d }, List.of(1, 2) };

      database.transaction(() -> {
        for (final Map.Entry<String, Type> entry : properties.entrySet()) {
          final String name = entry.getKey();
          final Class<?> javaClass = entry.getValue().getDefaultJavaType();

          for (final Object scalar : new Object[] { 1, 1L, 0.5f, 0.5d, (short) 1, "1", true })
            assertThatThrownBy(() -> database.newDocument("A").set(name, scalar).save())//
                .as("%s must refuse the scalar %s", name, scalar).isInstanceOf(IllegalArgumentException.class);

          for (final Object array : arraysOfNumbers) {
            final MutableDocument doc = database.newDocument("A").set(name, array).save();
            final Object stored = doc.get(name);
            assertThat(stored).as("%s from %s", name, array.getClass().getSimpleName()).isInstanceOf(javaClass);
            assertThat(Array.getLength(stored)).isEqualTo(2);
            assertThat(((Number) Array.get(stored, 1)).intValue()).isEqualTo(2);
          }

          assertThatThrownBy(() -> database.newDocument("A").set(name, new Object[] { 1, "x" }).save())//
              .as("%s must refuse an array with a non-numeric item", name).isInstanceOf(IllegalArgumentException.class);
          assertThatThrownBy(() -> database.newDocument("A").set(name, new Object[] { 1, null }).save())//
              .as("%s must refuse an array with a null item", name).isInstanceOf(IllegalArgumentException.class);
          assertThatThrownBy(() -> database.newDocument("A").set(name, List.of("1", "2")).save())//
              .as("%s must refuse a list of strings, even numeric ones", name).isInstanceOf(IllegalArgumentException.class);
        }

        // an int[] or a short[] goes through long[] and the range-checked narrowing: a value the target cannot hold is refused,
        // never wrapped
        assertThatThrownBy(() -> database.newDocument("A").set("shorts", new int[] { 1, 40_000 }).save())//
            .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("40000");
        assertThatThrownBy(() -> database.newDocument("A").set("shorts", new Integer[] { 1, -40_000 }).save())//
            .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("-40000");
        assertThat((short[]) database.newDocument("A").set("shorts", new int[] { Short.MIN_VALUE, Short.MAX_VALUE }).save()
            .get("shorts")).containsExactly(Short.MIN_VALUE, Short.MAX_VALUE);
        assertThat((long[]) database.newDocument("A").set("longs", new int[] { Integer.MIN_VALUE, Integer.MAX_VALUE }).save()
            .get("longs")).containsExactly(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertThat((int[]) database.newDocument("A").set("ints", new short[] { Short.MIN_VALUE, Short.MAX_VALUE }).save()
            .get("ints")).containsExactly(Short.MIN_VALUE, Short.MAX_VALUE);

        // a finite value that overflows FLOAT is refused, as for a FLOAT property, instead of being stored as an infinity
        assertThatThrownBy(() -> database.newDocument("A").set("floats", new double[] { 1, Double.MAX_VALUE }).save())//
            .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("out of range for type FLOAT");
        assertThatThrownBy(() -> database.newDocument("A").set("floats", List.of(1, -Double.MAX_VALUE)).save())//
            .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("out of range for type FLOAT");
        assertThatThrownBy(() -> database.newDocument("A").set("doubles", List.of(1, new BigDecimal("1e400"))).save())//
            .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("out of range for type DOUBLE");
        assertThat((float[]) database.newDocument("A").set("floats", new double[] { 0.5, Float.MAX_VALUE }).save().get("floats"))//
            .containsExactly(0.5f, Float.MAX_VALUE);
      });
    }

    @Test
    void aVectorIndexBuildReportsTheRowsItCannotIndex() {
      // A row an older version let through: written while the property was not declared yet, so nothing converted it
      database.command("sql", "CREATE DOCUMENT TYPE B");
      database.command("sql", "CREATE PROPERTY B.id INTEGER");

      final List<RID> bad = new ArrayList<>();
      database.transaction(() -> {
        database.newDocument("B").set("id", 1).set("embedding", new float[] { 0.9f, 0.1f, 0f }).save();
        bad.add(database.newDocument("B").set("id", 2).set("embedding", 0.5d).save().getIdentity());
        database.newDocument("B").set("id", 3).save(); // no vector at all: not indexed, and no error
        bad.add(database.newDocument("B").set("id", 4).set("embedding", new float[] { 0.9f, 0.1f }).save().getIdentity());
        bad.add(database.newDocument("B").set("id", 5).set("embedding", new float[] { Float.NaN, 0.1f, 0f }).save().getIdentity());
      });

      database.command("sql", "CREATE PROPERTY B.embedding ARRAY_OF_FLOATS");

      // a record written before the declaration is read back as it is, and an update of another property does not refuse it
      database.transaction(() -> {
        assertThat(bad.get(0).asDocument().get("embedding")).isEqualTo(0.5d);
        bad.get(0).asDocument().modify().set("note", "touched").save();
      });

      // the build leaves the 3 records out, as Neo4j leaves out a node holding no vector of the index's dimension (#10863), and
      // says so: in the result of the statement and in the index statistics. The record with no value is not counted
      try (final ResultSet rs = database.command("sql",
          "CREATE INDEX ON B (embedding) LSM_VECTOR METADATA {\"dimensions\": 3, \"similarity\": \"COSINE\"}")) {
        assertThat(rs.next().<Long>getProperty("skipped")).isEqualTo(3L);
      }
      assertThat(skippedInStats()).isEqualTo(3L);
      assertThat(neighbours()).containsExactly(1);

      // once the rows are fixed they are indexed by the write, and a rebuild has nothing left to skip
      database.transaction(() -> {
        for (final RID rid : bad)
          rid.asDocument().modify().set("embedding", new float[] { 0.8f, 0.2f, 0f }).save();
      });
      assertThat(neighbours()).containsExactlyInAnyOrder(1, 2, 4, 5);

      try (final ResultSet rs = database.command("sql", "REBUILD INDEX `B[embedding]`")) {
        assertThat(rs.next().<Long>getProperty("skipped")).isZero();
      }
      assertThat(skippedInStats()).isZero();
      assertThat(neighbours()).containsExactlyInAnyOrder(1, 2, 4, 5);
    }

    @Test
    void aRebuildReportsTheRecordsItLeftOutAndOtherIndexesKeepTheirResult() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.transaction(() -> {
        database.newDocument("R").set("id", 1).set("embedding", new float[] { 0.9f, 0.1f, 0f }).save();
        database.newDocument("R").set("id", 2).set("embedding", "not a vector").save();
      });
      database.command("sql", "CREATE PROPERTY R.id INTEGER");
      database.command("sql", "CREATE PROPERTY R.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY R.name STRING");
      database.command("sql", "CREATE INDEX ON R (id) UNIQUE");
      database.command("sql", "CREATE INDEX ON R (embedding) LSM_VECTOR METADATA {\"dimensions\": 3, \"similarity\": \"COSINE\"}");

      try (final ResultSet rs = database.command("sql", "REBUILD INDEX `R[embedding]`")) {
        assertThat(rs.next().<Long>getProperty("skipped")).isEqualTo(1L);
      }
      // the sweep re-creates every bucket index under a new name: the count is read from the index it put in place
      try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
        assertThat(rs.next().<Long>getProperty("skipped")).isEqualTo(1L);
      }
      try (final ResultSet rs = database.command("sql", "REBUILD INDEX `R[id]`")) {
        assertThat(rs.next().hasProperty("skipped")).as("only a vector index rebuild reports it").isFalse();
      }
      try (final ResultSet rs = database.command("sql", "CREATE INDEX ON R (name) NOTUNIQUE")) {
        assertThat(rs.next().hasProperty("skipped")).isFalse();
      }
    }

    @Test
    void aTypeOverSeveralBucketsCountsTheRecordsLeftOutOfEveryBucket() {
      database.command("sql", "CREATE DOCUMENT TYPE M BUCKETS 3");
      database.transaction(() -> {
        database.newDocument("M").set("embedding", new float[] { 0.9f, 0.1f, 0f }).save();
        for (int i = 0; i < 6; i++)
          database.newDocument("M").set("embedding", i).save();
      });
      database.command("sql", "CREATE PROPERTY M.embedding ARRAY_OF_FLOATS");

      try (final ResultSet rs = database.command("sql",
          "CREATE INDEX ON M (embedding) LSM_VECTOR METADATA {\"dimensions\": 3, \"similarity\": \"COSINE\"}")) {
        assertThat(rs.next().<Long>getProperty("skipped")).isEqualTo(6L);
      }

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("M[embedding]");
      int bucketsWithASkip = 0;
      for (final Index index : typeIndex.getIndexesOnBuckets())
        if (((LSMVectorIndex) index).getLastBuildSkippedRecords() > 0)
          ++bucketsWithASkip;
      assertThat(bucketsWithASkip).as("the records are spread over the buckets").isGreaterThan(1);
      assertThat(typeIndex.getStats().get(LSMVectorIndex.LAST_BUILD_SKIPPED_RECORDS_STAT)).as("summed, not the last bucket's")
          .isEqualTo(6L);
    }

    private long skippedInStats() {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("B[embedding]");
      long skipped = 0;
      for (final Index index : typeIndex.getIndexesOnBuckets())
        skipped += ((LSMVectorIndex) index).getStats().get("lastBuildSkippedRecords");
      return skipped;
    }

    private List<Integer> neighbours() {
      final List<Integer> found = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT expand(vector.neighbors('B[embedding]', [0.9, 0.1, 0.0], 5))")) {
        while (rs.hasNext())
          found.add(rs.next().getProperty("id"));
      }
      return found;
    }
  }

  /**
   * Regression for #6559: approximate delta scoring scale.
   * <p>
   * Issue #6559 item 2: {@link LSMVectorIndex#findNeighborsFromVectorApproximate} - the zero-disk-I/O PQ path - scored
   * every graph candidate approximately from the in-memory PQ codes, but merged in delta-buffer rows scored
   * <em>exactly</em>, then sorted the two against each other as one list.
   * <p>
   * The fixture puts the defect in its purest form: a row is inserted into the delta buffer carrying a vector that is
   * <em>bit-identical</em> to one already in the graph. The two are the same point in space, at the same true distance
   * from any query, so whatever number the search hands back for one it has to hand back for the other. Before the fix
   * the graph row came back with its PQ distance (carrying quantization error) and the delta row with an exact 0, so
   * the delta twin outranked its own graph copy every time - not by being nearer, but by being the only one of the two
   * measured without error.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ApproximateDeltaScoringScale extends TestHelper {

    private static final int DIMENSIONS = 32;
    private static final int VECTORS    = 400;
    private static final int PQ_CLUSTERS = 16;
    // The graph vector the delta row is made a twin of. Any indexed vector does; this one is simply not at an edge.
    private static final int TWINNED    = 17;
    // Wide enough that the twinned vector and its delta copy are both comfortably inside the answer.
    private static final int TOP_K      = 25;

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
     * Two rows, one vector, one query: the graph copy and its delta twin must come back with the same distance. The
     * guard below is what keeps this from passing vacuously - it pins that PQ really is lossy on this fixture, so a
     * build that scored the delta side exactly would produce two visibly different numbers and fail here.
     */
    @Test
    void aDeltaRowIsScoredOnTheSamePqScaleAsItsGraphTwin() {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      assertThat(index.isPQSearchAvailable()).as("the fixture is only meaningful while the PQ path is really taken")
          .isTrue();

      final float[] query = vector(TWINNED);

      // The graph row's own PQ distance to a query that IS its vector. Non-zero exactly to the extent PQ is lossy.
      final List<Pair<RID, Float>> beforeInsert = index.findNeighborsFromVectorApproximate(query, TOP_K, null);
      final float graphDistance = distanceOf(beforeInsert, "doc" + TWINNED);
      assertThat(graphDistance)
          .as("guard against a vacuous test: PQ has to be lossy here, or scoring the delta side exactly would agree "
              + "with the graph side by accident and this test could never fail")
          .isGreaterThan(1e-6f);

      // The twin: same vector, new record, and it stays in the delta buffer because the graph is frozen.
      database.transaction(() -> database.newDocument("Doc").set("id", "twin").set("embedding", vector(TWINNED)).save());
      assertThat(PendingDeltaVectors.of(index))
          .as("the fixture is only a regression test while the twin is still in the delta buffer").isEqualTo(1L);

      final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, null);
      final float twinDistance = distanceOf(results, "twin");

      assertThat(twinDistance)
          .as("a delta row and its bit-identical graph twin are the same point, so the PQ path has to score them on "
              + "the same scale - before the fix the twin came back exact (0) against the graph row's PQ distance")
          .isCloseTo(graphDistance, org.assertj.core.data.Offset.offset(1e-5f));
    }

    /**
     * The other surface with the same merge, and the same defect: the issue #6514 pre-filter plan scores its
     * allow-listed ordinals from the PQ codes too, so the delta rows merged alongside them need the same treatment.
     */
    @Test
    void thePreFilterPlanAlsoScoresDeltaRowsOnThePqScale() {
      createSchemaAndData();

      final LSMVectorIndex index = vectorIndex();
      final float[] query = vector(TWINNED);

      final float graphDistance = distanceOf(index.findNeighborsFromVectorApproximate(query, TOP_K, null), "doc" + TWINNED);
      assertThat(graphDistance).as("guard against a vacuous test: PQ has to be lossy here").isGreaterThan(1e-6f);

      database.transaction(() -> database.newDocument("Doc").set("id", "twin").set("embedding", vector(TWINNED)).save());

      // Narrow enough to take the pre-filter plan rather than the graph walk.
      final java.util.Set<RID> allowed = new java.util.HashSet<>();
      allowed.add(ridOf("doc" + TWINNED));
      allowed.add(ridOf("twin"));

      final long preFilterBefore = index.getStats().get("preFilterSearches");
      final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(query, TOP_K, allowed);
      assertThat(index.getStats().get("preFilterSearches")).as("the fixture has to actually reach the pre-filter plan")
          .isGreaterThan(preFilterBefore);

      assertThat(distanceOf(results, "twin"))
          .as("the pre-filter plan ranks its ordinals on the PQ scale, so its merged delta rows must be there too")
          .isCloseTo(distanceOf(results, "doc" + TWINNED), org.assertj.core.data.Offset.offset(1e-5f));
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private float distanceOf(final List<Pair<RID, Float>> results, final String id) {
      for (final Pair<RID, Float> r : results)
        if (id.equals(((Document) database.lookupByRID(r.getFirst(), true)).getString("id")))
          return r.getSecond();
      throw new AssertionError("row '" + id + "' is missing from the answer: " + results);
    }

    private RID ridOf(final String id) {
      try (final com.arcadedb.query.sql.executor.ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?",
          id)) {
        return rs.next().getIdentity().orElseThrow();
      }
    }

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

    /**
     * Deterministic, and spread over the whole space rather than clustered around one direction: with a constant
     * offset added to every component the vectors are all nearly parallel, cosine similarity is ~1 for every pair, and
     * every distance in the answer ties - which hides exactly the difference this test is here to measure.
     */
    private static float[] vector(final int seed) {
      final Random rnd = new Random(seed * 1_000_003L);
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = rnd.nextFloat() * 2 - 1;
      return v;
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #8962: all-zero and non-finite vectors.
   * <p>
   * Issue #8962: an all-zero vector was stored and then dropped by every read path even under EUCLIDEAN, where the
   * origin is a regular point (and the nearest one to [0.01, 0.01]); a vector with a NaN or an Infinity component was
   * accepted, indexed and returned with a NaN or infinite distance, under DOT_PRODUCT ahead of every finite record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ZeroAndNonFiniteVector extends TestHelper {

    @Test
    void euclideanReturnsTheOriginAsNearest() {
      final String type = create("EUCLIDEAN");
      database.transaction(() -> {
        database.newDocument(type).set("name", "origin", "vector", new float[] { 0f, 0f }).save();
        database.newDocument(type).set("name", "near", "vector", new float[] { 0.1f, 0.1f }).save();
        database.newDocument(type).set("name", "far", "vector", new float[] { 3f, 4f }).save();
      });

      assertThat(neighbors(type)).containsExactly("origin", "near", "far");
    }

    @Test
    void nonFiniteComponentsAreRejected() {
      for (final String similarity : List.of("EUCLIDEAN", "DOT_PRODUCT", "COSINE")) {
        final String type = create(similarity);
        for (final float bad : new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY })
          assertThatThrownBy(() -> database.transaction(() -> database.newDocument(type).set("name", "bad", "vector", new float[] { bad, 1f }).save()))
              .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("finite");

        database.transaction(() -> database.newDocument(type).set("name", "ok", "vector", new float[] { 0.6f, 0.8f }).save());
        assertThat(neighbors(type)).containsExactly("ok");
      }
    }

    @Test
    void nonFiniteQueryVectorIsRejected() {
      final String type = create("EUCLIDEAN");
      database.transaction(() -> database.newDocument(type).set("name", "a", "vector", new float[] { 1f, 1f }).save());

      assertThatThrownBy(() -> database.query("sql", "SELECT FROM (SELECT expand(vectorNeighbors('" + type + "[vector]', ?, 3)))",
          (Object) new float[] { Float.NaN, 1f }).close()).hasStackTraceContaining("finite");
    }

    @Test
    void cosineKeepsAcceptingZeroPlaceholderVectors() {
      final String type = create("COSINE");
      database.transaction(() -> {
        database.newDocument(type).set("name", "placeholder", "vector", new float[] { 0f, 0f }).save();
        database.newDocument(type).set("name", "real", "vector", new float[] { 0.6f, 0.8f }).save();
      });

      assertThat(neighbors(type)).containsExactly("real");
    }

    private String create(final String similarity) {
      final String type = "P" + similarity;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"" + similarity + "\" }");
      return type;
    }

    private List<String> neighbors(final String type) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name, distance FROM (SELECT expand(vectorNeighbors('" + type + "[vector]', ?, 10)))",
          (Object) new float[] { 0.01f, 0.01f })) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          names.add(r.getProperty("name"));
        }
      }
      return names;
    }
  }
}
