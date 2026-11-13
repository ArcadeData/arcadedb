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
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Grouped vector search. Each nested class keeps the fixture of the regression test it was folded from.
 */
class GroupedVectorSearchTest {
  /**
   * Regression for #10002: unresolved group key.
   * <p>
   * Issue #10002: a group-key resolver answers {@link GroupAdmissionState#UNRESOLVED} for a candidate whose record it
   * cannot read right now, and every grouped search skips such a candidate instead of filing it under a group. Filed under
   * the {@code null} group, as the SQL functions used to do, the nearest unreadable records took one of the search's
   * group slots and kept a real group out of the answer.
   * <p>
   * Records {@code 2g} and {@code 2g + 1} form group {@code g}, nearest first. The resolver reports the three nearest
   * records as unreadable, so {@code limit = 3, groupSize = 2} must answer record 3 (group 1, whose other member is
   * unreadable) and the full groups 2 and 3 - never a group made of the unreadable records.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnresolvedGroupKey extends TestHelper {
    private static final int COUNT       = 40;
    private static final int UNREADABLE  = 3;
    private static final int LIMIT       = 3;
    private static final int GROUP_SIZE  = 2;
    private static final List<Integer> EXPECTED_PIDS = List.of(3, 4, 5, 6, 7);

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Dense BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Dense.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Dense.grp INTEGER");
      database.command("sql", "CREATE PROPERTY Dense.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Dense (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");

      database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Sparse.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Sparse.grp INTEGER");
      database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16, rescoreOversample: 0 }");

      // The same corpus with exact rescoring on (the default): the group-restricted search then re-resolves and caps the
      // rescored rows itself, which is a second place an unreadable record has to be skipped
      database.command("sql", "CREATE DOCUMENT TYPE Rescored BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Rescored.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Rescored.grp INTEGER");
      database.command("sql", "CREATE PROPERTY Rescored.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Rescored.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Rescored (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");

      database.transaction(() -> {
        for (int i = 0; i < COUNT; i++) {
          database.newDocument("Rescored").set("pid", i, "grp", i / 2, "tokens", new int[] { 0 }, "weights",
              new float[] { 1.0f - i * 0.01f }).save();
          database.newDocument("Dense").set("pid", i, "grp", i / 2, "vector", new float[] { i, 0f }).save();
          database.newDocument("Sparse").set("pid", i, "grp", i / 2, "tokens", new int[] { 0 }, "weights",
              new float[] { 1.0f - i * 0.01f }).save();
        }
      });
    }

    @Test
    void denseGroupedSearchSkipsUnresolvedCandidates() {
      final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Dense[vector]"))
          .getIndexesOnBuckets()[0];
      final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorGrouped(new float[] { 0f, 0f }, LIMIT, GROUP_SIZE, -1,
          null, resolver());
      assertThat(pids(rows.stream().map(Pair::getFirst).toList())).containsExactlyElementsOf(EXPECTED_PIDS);
    }

    @Test
    void denseGroupRestrictedSearchSkipsUnresolvedCandidates() {
      final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Dense[vector]"))
          .getIndexesOnBuckets()[0];
      final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorForGroups(new float[] { 0f, 0f }, Set.of(0, 1, 2),
          GROUP_SIZE, -1, null, resolver(), Float.POSITIVE_INFINITY);
      // group 0 is wholly unreadable, group 1 keeps its readable member
      assertThat(pids(rows.stream().map(Pair::getFirst).toList())).containsExactly(3, 4, 5);
    }

    @Test
    void sparseGroupedSearchSkipsUnresolvedCandidates() {
      final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("Sparse[tokens,weights]")).getIndexesOnBuckets()[0];
      final List<RidScore> rows = index.topKGrouped(new int[] { 0 }, new float[] { 1.0f }, LIMIT, GROUP_SIZE, null,
          resolver());
      assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyElementsOf(EXPECTED_PIDS);
    }

    @Test
    void sparseGroupRestrictedSearchSkipsUnresolvedCandidates() {
      final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("Sparse[tokens,weights]")).getIndexesOnBuckets()[0];
      final List<RidScore> rows = index.topKForGroups(new int[] { 0 }, new float[] { 1.0f }, Set.of(0, 1, 2), GROUP_SIZE,
          Float.NEGATIVE_INFINITY, null, resolver(), null);
      assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyInAnyOrder(3, 4, 5);
    }

    @Test
    void rescoredGroupRestrictedSearchSkipsUnresolvedCandidates() {
      final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
          .getIndexByName("Rescored[tokens,weights]")).getIndexesOnBuckets()[0];
      final List<RidScore> rows = index.topKForGroups(new int[] { 0 }, new float[] { 1.0f }, Set.of(0, 1, 2), GROUP_SIZE,
          Float.NEGATIVE_INFINITY, null, resolver(), null);
      assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyInAnyOrder(3, 4, 5);
    }

    @Test
    void admissionNeverOpensAGroupForAnUnresolvedKey() {
      final GroupAdmissionState groups = new GroupAdmissionState(1, 1);
      assertThat(groups.admit(GroupAdmissionState.UNRESOLVED)).isFalse();
      assertThat(groups.distinctGroups()).isZero();
      // the one slot is still free for a real group, including the ordinary null group of a readable record
      assertThat(groups.admit(null)).isTrue();
      assertThat(groups.isFull()).isTrue();
    }

    @Test
    void plannerIgnoresUnresolvedRows() {
      // Source 0 holds an unresolved row that would otherwise be the best peak overall and win a group of its own,
      // pushing group "b" out of the winners and asking source 1 for a group no record belongs to.
      final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
      planner.addSource(List.of(new RID(1, 0), new RID(1, 1), new RID(1, 2)), new float[] { 0.99f, 0.9f, 0.8f },
          listOf(GroupAdmissionState.UNRESOLVED, "b", "b"), true);
      planner.addSource(List.of(new RID(2, 0), new RID(2, 1)), new float[] { 0.5f, 0.4f }, listOf("c", "c"), true);
      // "b" wins with two members at or above source 1's cut-off, so nothing needs a second phase
      assertThat(planner.plan()).isEmpty();
    }

    /** Reads the record's group, but reports the {@link #UNREADABLE} nearest records as unreadable. */
    private Function<RID, Object> resolver() {
      final Set<RID> unreadable = new HashSet<>();
      for (final String type : new String[] { "Dense", "Sparse", "Rescored" })
        database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE pid < " + UNREADABLE)
            .forEachRemaining(r -> unreadable.add(r.getProperty("rid")));
      assertThat(unreadable).hasSize(UNREADABLE * 3);
      return rid -> unreadable.contains(rid) ?
          GroupAdmissionState.UNRESOLVED :
          ((Document) database.lookupByRID(rid, true)).get("grp");
    }

    private List<Integer> pids(final List<RID> rids) {
      final Map<RID, Integer> cache = new HashMap<>();
      final List<Integer> pids = new ArrayList<>(rids.size());
      for (final RID rid : rids)
        pids.add(cache.computeIfAbsent(rid, r -> ((Document) database.lookupByRID(r, true)).getInteger("pid")));
      return pids;
    }

    private static List<Object> listOf(final Object... keys) {
      final List<Object> list = new ArrayList<>(keys.length);
      for (final Object key : keys)
        list.add(key);
      return list;
    }
  }

  /**
   * Regression for #5761: grouped search group choice.
   * <p>
   * Issue #5761: the {@code groupBy} cap used to be pushed into the HNSW traversal through a group-aware {@code Bits}
   * filter, which is score-blind - JVector calls it on each popped candidate, before the score function runs. The first
   * candidates popped are the entry-point descent, and the descent starts far from the query, so the {@code limit}
   * distinct-group slots were handed out to whatever the beam met on its way in. By the time it arrived at the query the
   * budget was gone and the nearest group was locked out of its own answer: on this fixture a query aimed at the centre
   * of cluster 1 came back with clusters 3, 4 and 8, while the ungrouped search over the same index on the same query
   * answered cluster 1 five times out of five.
   * <p>
   * Nothing here is deleted and nothing is stale. The fixture is the arc from {@link VectorSearchCorrectnessTest.DeletedRegionSearch}
   * with the clusters an order of magnitude further apart than they are wide, so "which cluster is nearest" is decidable
   * on paper and is not an artefact of quantization noise or of a wobble in HNSW recall.
   */
  @Nested
  class GroupedSearchGroupChoice extends TestHelper {

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
     * group it can fill, and say so through {@code groupedSearchesGroupsUnavailable} - NOT
     * {@code groupedSearchesShortOfLimit}, which issue #6559 narrowed to the case where the candidate budget cut the
     * walk short. A corpus with only one distinct group exhausts the reachable graph on its own instead: raising
     * efSearch could never manufacture a second group that does not exist, so this is the non-actionable counter.
     */
    @Test
    void aGroupKeyWithOneValueStopsInsteadOfWalkingTheWholeGraph() {
      createSchema();
      insertVertices();

      final long unavailableBefore = vectorIndex().getStats().get("groupedSearchesGroupsUnavailable");
      final long shortBefore = vectorIndex().getStats().get("groupedSearchesShortOfLimit");

      final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorGrouped(embedding(centerOf(1)), LIMIT,
          GROUP_SIZE, -1, null, rid -> "one-and-only");

      assertThat(neighbors).as("the one group it can fill still has to come back, filled to groupSize")
          .hasSize(GROUP_SIZE);
      assertThat(vectorIndex().getStats().get("groupedSearchesGroupsUnavailable"))
          .as("a search that could not open limit groups because the graph ran out on its own has to be visible to the operator")
          .isEqualTo(unavailableBefore + 1);
      assertThat(vectorIndex().getStats().get("groupedSearchesShortOfLimit"))
          .as("this shortfall is not a candidate-budget problem, so it must not pin the efSearch-actionable counter")
          .isEqualTo(shortBefore);
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

  /**
   * Regression for #6066: grouped search row budget.
   * <p>
   * Issue #6066: {@code findNeighborsFromVectorGrouped} derived its row budget with a plain {@code limit * groupSize}
   * int multiplication and then used it as an eager {@code ArrayList} capacity and as the floor on the beam width.
   * Neither factor is bounded anywhere on the way in - the method is public API and the SQL layer's own cap only
   * applies to the {@code vector.neighbors} entry point - so two individually reasonable values could produce a budget
   * that either wrapped through {@link Integer#MAX_VALUE} or was simply larger than any allocation can be:
   * <ul>
   *   <li>a product that overflows to a negative int failed the whole search with an
   *       {@code IllegalArgumentException: Illegal Capacity} out of {@code new ArrayList<>(maxRows)}, wrapped in an
   *       {@code IndexException} whose message says nothing about the real cause;</li>
   *   <li>a product that stays positive but is huge asked for a multi-GB backing array and died with an
   *       {@code OutOfMemoryError} before a single candidate was scored.</li>
   * </ul>
   * The budget is now computed in {@code long} and clamped to the number of candidates the index can actually address,
   * which is the same treatment {@code findNeighborsFromVector}'s {@code k} received for issue #5924. The clamp cannot
   * lose a row: no grouped search can return more rows than the index holds vectors, and a beam wider than the graph
   * has nothing extra to look at.
   */
  @Nested
  class GroupedSearchRowBudget extends TestHelper {

    private static final int    DIMENSIONS  = 16;
    private static final int    CLUSTERS    = 6;
    private static final int    PER_CLUSTER = 10;
    private static final int    VERTICES    = CLUSTERS * PER_CLUSTER;
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
     * {@code 50000 * 50000} is 2.5e9, which two's-complement-wraps to a negative int. The search used to die on the
     * result buffer's capacity; it now has to answer, and answer with exactly what the same request answers when the
     * budget is expressed in a width that holds it.
     */
    @Test
    void aRowBudgetThatOverflowsIntStillAnswers() {
      createSchema();
      insertVertices();

      assertThat((long) 50_000 * 50_000).as("the fixture has to actually overflow an int to be a regression test")
          .isGreaterThan(Integer.MAX_VALUE);
      assertThat(50_000 * 50_000).as("and the unguarded product is the negative capacity that used to throw")
          .isNegative();

      final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 50_000, 50_000);

      assertThat(neighbors).as("a budget larger than the index is not a reason to fail the search").hasSize(VERTICES);
      assertRankOrdered(neighbors);
    }

    /**
     * The same defect without the wrap: {@code 200000000 * 10} is 2e9, a perfectly valid positive int that is
     * nonetheless an 8 GB {@code Object[]} the result buffer used to ask for before a single candidate was scored - an
     * {@code OutOfMemoryError} on any ordinary heap, and several seconds of zeroing on a heap large enough to serve it.
     * After the clamp the buffer is sized by the index, so the request costs nothing whatever the heap is.
     */
    @Test
    void aRowBudgetTooLargeToAllocateStillAnswers() {
      createSchema();
      insertVertices();

      assertThat(200_000_000 * 10).as("this product does not wrap - it is simply bigger than any allocation")
          .isPositive();

      final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 200_000_000, 10);

      assertThat(neighbors).as("a budget the heap cannot hold is still only as many rows as the index has")
          .hasSize(VERTICES);
      assertRankOrdered(neighbors);
    }

    /**
     * The silent half of the defect, and the one the issue calls out as the more dangerous: a product that wraps to a
     * small positive throws nothing at all, it just under-drives the beam. {@code Integer.MAX_VALUE} squared wraps to
     * exactly {@code 1}, and {@code effectiveEfSearch} is {@code max(maxRows, efSearch)} - so with the caller's own
     * {@code efSearch} at 1 the whole search used to run on a one-candidate beam and come back with a fraction of the
     * rows it was asked for, in silence. {@code Integer.MAX_VALUE} is also precisely what a saturating narrowing at a
     * query entry point produces, which is how a request this shape arrives in practice.
     */
    @Test
    void aRowBudgetThatWrapsToOneDoesNotStarveTheBeam() {
      createSchema();
      insertVertices();

      assertThat(Integer.MAX_VALUE * Integer.MAX_VALUE).as("the unguarded product of two saturated factors is 1")
          .isEqualTo(1);

      final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), Integer.MAX_VALUE, Integer.MAX_VALUE, 1);

      assertThat(neighbors).as("the beam floor is the row budget, so a wrapped budget must not shrink the answer")
          .hasSize(VERTICES);
      assertRankOrdered(neighbors);
    }

    /**
     * The clamp must not touch a budget that fits: a request for fewer rows than the index holds still gets exactly the
     * group cap it asked for, so the fix is invisible to every realistic query.
     */
    @Test
    void aBudgetThatFitsIsUnchanged() {
      createSchema();
      insertVertices();

      final List<Pair<RID, Float>> neighbors = grouped(centerOf(1), 3, 2);

      assertThat(neighbors).as("three groups of two is six rows, and the clamp has no business shrinking it")
          .hasSize(6);
      assertThat(clustersOf(neighbors)).as("the group the query sits in cannot be absent from its own answer")
          .contains(1);
      assertRankOrdered(neighbors);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void assertRankOrdered(final List<Pair<RID, Float>> neighbors) {
      final List<RID> rids = new ArrayList<>(neighbors.size());
      for (final Pair<RID, Float> neighbor : neighbors)
        rids.add(neighbor.getFirst());
      assertThat(rids).as("the same row must not be admitted twice").doesNotHaveDuplicates();

      for (int i = 1; i < neighbors.size(); i++)
        assertThat(neighbors.get(i).getSecond()).as("row %d is nearer than row %d, so the answer is not sorted", i, i - 1)
            .isGreaterThanOrEqualTo(neighbors.get(i - 1).getSecond());
    }

    private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize) {
      return grouped(vertex, limit, groupSize, -1);
    }

    private List<Pair<RID, Float>> grouped(final int vertex, final int limit, final int groupSize, final int efSearch) {
      // The resolver receives the raw (unbound) RID the location map holds, exactly as the SQL function's does.
      return vectorIndex().findNeighborsFromVectorGrouped(embedding(vertex), limit, groupSize, efSearch, null,
          rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster"));
    }

    private List<Integer> clustersOf(final List<Pair<RID, Float>> neighbors) {
      final List<Integer> clusters = new ArrayList<>(neighbors.size());
      for (final Pair<RID, Float> neighbor : neighbors)
        clusters.add(neighbor.getFirst().asVertex().getInteger("cluster"));
      return clusters;
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
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
      // Force the first graph build outside the assertions, so a search under test measures the search only.
      vectorIndex().findNeighborsFromVector(embedding(centerOf(0)), 1);
    }

    private static int centerOf(final int cluster) {
      return cluster * PER_CLUSTER + PER_CLUSTER / 2;
    }

    /**
     * Points on an arc, embedded with damped harmonics so cosine similarity decreases strictly with the angular gap -
     * the same construction as {@code GroupedVectorSearchTest.GroupedSearchGroupChoice}, so which cluster is nearest is decidable
     * on paper rather than an artefact of quantization noise.
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
  }

  /**
   * Regression for #6501: grouped delta merge.
   * <p>
   * Issue #6501: a {@code groupBy} vector search could not see anything ingested since the last graph rebuild.
   * <p>
   * {@code findNeighborsFromVectorGrouped} answered purely from the HNSW graph, and the delta buffer - which holds
   * every vector written since that graph was built - was documented as deliberately out of scope. The result was a
   * full, plausible, exception-free answer computed over a stale corpus: the same index, the same query vector and the
   * same instant returned the newly written rows without {@code groupBy} and hid them with it. On stock knobs the
   * invisible window is up to {@code min(rebuildGraphRatio * graphSize, 50000)} rows wide and stays open for as long
   * as writes keep arriving, so this is the steady state under ingestion rather than a corner case.
   * <p>
   * The delta candidates are now merged into the candidate stream <em>before</em> the group cap sees it, in ascending
   * distance order, which is the only place they can go: {@link GroupAdmissionState} is first-come-first-served, so
   * appending them after the graph walk would let a far delta row take a group slot from a nearer graph row. These
   * tests pin both halves - the rows are visible, and the cap they arrive through is still the one the caller asked
   * for.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GroupedDeltaMerge extends TestHelper {

    private static final int    DIMENSIONS  = 16;
    private static final int    CLUSTERS    = 6;
    // 200 per cluster, so the graph clears ASYNC_REBUILD_MIN_GRAPH_SIZE. Below 1,000 nodes rebuildGraphBeforeSearch()
    // rebuilds synchronously on EVERY query with no threshold gate, which drains the delta buffer before the search
    // under test can look at it - a small fixture would be green whatever the grouped path does, for the wrong reason.
    private static final int    PER_CLUSTER = 200;
    private static final int    VERTICES    = CLUSTERS * PER_CLUSTER;
    private static final double CLUSTER_GAP = 0.12;
    private static final double JITTER      = 0.0001;

    /**
     * Angle of the query: a quarter of a cluster gap past cluster 1's centre. Far enough off the base grid that the
     * nearest base vertex sits at a distance floats can tell from zero, which is what lets a delta row a thousandth of
     * a radian away be provably nearer rather than a tie the index breaks however it likes.
     */
    private static final double QUERY_THETA = theta(centerOf(1)) + CLUSTER_GAP / 4;

    /** Angular step between consecutive delta rows: far inside the query's gap to the base grid, and above float noise. */
    private static final double DELTA_STEP  = 0.001;

    @BeforeEach
    void freezeTheGraph() {
      // The whole defect only exists while the delta buffer is non-empty, so the fixture has to keep it that way:
      // no mutation-count rebuild, no ratio rebuild, no inactivity-timer rebuild.
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
     * The issue as filed. Three rows written after the graph was built are the three nearest vectors in the index by a
     * wide margin; the ungrouped query returns exactly those three, and the grouped query used to return the best the
     * <em>stale</em> graph had instead - ten rows, no exception, no warning, and a nearest distance three orders of
     * magnitude worse.
     */
    @Test
    void aGroupedSearchSeesVectorsWrittenSinceTheLastGraphRebuild() {
      createSchema();
      insertBaseVertices();

      final long baseline = deltaCount();
      final List<RID> fresh = insertDeltaVertices(3);
      assertThat(deltaCount()).as("the fixture is only a regression test while the rows are still in the delta buffer")
          .isEqualTo(baseline + 3);

      final List<Pair<RID, Float>> ungrouped = vectorIndex().findNeighborsFromVector(query(), 3);
      assertThat(ridsOf(ungrouped)).as("control: without groupBy the delta rows are the answer")
          .containsExactlyInAnyOrderElementsOf(fresh);

      final long mergesBefore = stat("groupedSearchesMergingDelta");

      final List<Pair<RID, Float>> grouped = grouped(3, 1);
      assertThat(ridsOf(grouped)).as("the same index, vector and instant must not answer a groupBy query from a stale corpus")
          .containsExactlyInAnyOrderElementsOf(fresh);
      assertThat(grouped.getFirst().getSecond()).as("and the nearest row must be the nearest row")
          .isEqualTo(ungrouped.getFirst().getSecond());
      assertWellFormed(grouped);

      assertThat(stat("groupedSearchesMergingDelta"))
          .as("a grouped query paying for a delta scan has to be visible in the stats, which is what issue #6501 asked "
              + "for before it asked for the merge - groupedSearchesShortOfLimit means something else entirely")
          .isGreaterThan(mergesBefore);
    }

    /**
     * The surface the issue was actually filed against. {@code vector.neighbors} routes to the grouped index method on
     * the presence of {@code groupBy} alone, so a user bucketing an existing query by a field was opting into a stale
     * corpus without a syntax, a doc page or a log line saying so. Same query, same instant, with and without the
     * option.
     */
    @Test
    void theSqlSurfaceSeesTheDeltaBufferWithAndWithoutGroupBy() {
      createSchema();
      insertBaseVertices();

      final List<RID> fresh = insertDeltaVertices(3);

      final List<RID> plain = ridsFromSql("SELECT expand(`vector.neighbors`(?, ?, ?))");
      assertThat(plain).as("control: the ungrouped SQL query returns the rows written since the rebuild")
          .containsExactlyInAnyOrderElementsOf(fresh);

      final List<RID> byCluster = ridsFromSql(
          "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'cluster', groupSize: 1 }))");
      assertThat(byCluster).as("adding groupBy buckets the rows - it does not change which rows exist")
          .containsExactlyInAnyOrderElementsOf(fresh);
    }

    /**
     * Merging is not appending: {@link GroupAdmissionState} hands out slots first-come-first-served, so a delta row
     * offered after the graph walk would take a group its distance never earned. Here the delta rows sit in one brand
     * new group and are nearer than everything else, so a correct merge spends exactly {@code groupSize} of them and
     * fills the remaining groups from the graph.
     */
    @Test
    void theGroupCapStillHoldsOverTheMergedDeltaRows() {
      createSchema();
      insertBaseVertices();

      final long baseline = deltaCount();
      insertDeltaVerticesInOneGroup(5, 99);
      assertThat(deltaCount()).as("the rows have to still be in the delta buffer for this to test anything")
          .isEqualTo(baseline + 5);

      final List<Pair<RID, Float>> grouped = grouped(3, 2);

      assertThat(grouped).as("three groups of two rows each").hasSize(6);
      assertThat(clustersOf(grouped)).as("the delta rows are nearest, but the cap is still two per group")
          .filteredOn(cluster -> cluster == 99).hasSize(2);
      assertThat(new HashSet<>(clustersOf(grouped))).as("and still three distinct groups").hasSize(3);
      assertWellFormed(grouped);
    }

    /**
     * A delta row must be ranked, not just appended: with one row per group the answer is the {@code limit} nearest
     * distinct groups, and the merged stream has to interleave delta and graph rows by distance to produce it. The
     * delta rows here straddle the base clusters - two nearer than anything indexed, one deliberately further away
     * than the nearest base cluster - so appending in either direction gives a different (wrong) answer.
     */
    @Test
    void deltaRowsAreRankedAgainstGraphRowsRatherThanAppended() {
      createSchema();
      insertBaseVertices();

      final RID nearest = insertDeltaVertex(QUERY_THETA + DELTA_STEP, 90);
      final RID second = insertDeltaVertex(QUERY_THETA + 2 * DELTA_STEP, 91);
      final RID far = insertDeltaVertex(theta(centerOf(4)), 92);

      final List<Pair<RID, Float>> grouped = grouped(4, 1);

      assertThat(grouped).hasSize(4);
      assertThat(grouped.get(0).getFirst()).as("the nearest delta row leads the answer").isEqualTo(nearest);
      assertThat(grouped.get(1).getFirst()).as("followed by the second delta row").isEqualTo(second);
      assertThat(ridsOf(grouped)).as("a delta row further away than four base clusters has not earned a slot")
          .doesNotContain(far);
      assertWellFormed(grouped);
    }

    /**
     * The narrow-allow-list plan (issue #6514) is a second entry point into the same grouped contract, and it skipped
     * the delta buffer for the same reason. An allow-list made only of rows that are still in the delta buffer is the
     * sharpest form of the bug: every candidate the caller asked about was invisible, so the answer was empty.
     */
    @Test
    void theGroupedPreFilterPlanSeesTheDeltaBufferToo() {
      createSchema();
      insertBaseVertices();

      final List<RID> fresh = insertDeltaVertices(3);
      final Set<RID> allowed = new LinkedHashSet<>(fresh);

      final List<Pair<RID, Float>> grouped = vectorIndex().findNeighborsFromVectorGrouped(query(), 3, 1, -1, allowed,
          groupKeyResolver());

      assertThat(ridsOf(grouped)).as("an allow-list of delta-only rows must not answer empty")
          .containsExactlyInAnyOrderElementsOf(fresh);
      assertWellFormed(grouped);
    }

    /**
     * An allow-list that mixes graph rows and delta rows has to be answered from both, and in rank order across the
     * two: the delta rows are nearer, so they lead.
     */
    @Test
    void theGroupedPreFilterPlanRanksDeltaAndGraphRowsTogether() {
      createSchema();
      final List<RID> base = insertBaseVertices();

      final List<RID> fresh = insertDeltaVertices(2);
      final Set<RID> allowed = new LinkedHashSet<>(fresh);
      allowed.add(base.get(centerOf(3)));
      allowed.add(base.get(centerOf(5)));

      final List<Pair<RID, Float>> grouped = vectorIndex().findNeighborsFromVectorGrouped(query(), 4, 1, -1, allowed,
          groupKeyResolver());

      assertThat(grouped).hasSize(4);
      assertThat(ridsOf(grouped).subList(0, 2)).as("the delta rows are the nearest two of the allow-list")
          .containsExactlyInAnyOrderElementsOf(fresh);
      assertWellFormed(grouped);
    }

    /**
     * The one row that can reach the cap from both sides of the merge. A graph build occasionally leaves a vector with
     * a full out-degree and no in-edges and re-queues it into the delta buffer while its node stays in the graph, so
     * the walk and the cursor both offer the same RID. Admitting it twice would put one record in the answer twice and
     * charge its group two of its {@code groupSize} slots, evicting a record that earned one.
     * <p>
     * Driven through {@code requeueIntoDeltaBufferForTest} rather than by producing a real orphan: whether a build
     * orphans a node is decided by the data and JVector's diversity heuristic, so a fixture cannot ask for one. The
     * row re-queued is the nearest record to the query, so it is certain to reach the cap from the graph side too -
     * re-queueing an arbitrary one of 1,200 proves nothing, since the answer holds twelve. {@code groupSize} is 2 on
     * purpose: at 1 the cap would reject the second copy on its own and the test would pass with the dedup deleted.
     */
    @Test
    void aRowOfferedByBothTheGraphAndTheDeltaBufferIsAdmittedOnce() {
      createSchema();
      insertBaseVertices();

      final RID shared = vectorIndex().findNeighborsFromVector(query(), 1).getFirst().getFirst();
      assertThat(vectorIndex().requeueIntoDeltaBufferForTest(shared))
          .as("the nearest record has to be in the graph for this to be an overlap at all").isNotNegative();
      assertThat(deltaCount()).as("and in the delta buffer as well, which is the whole point").isPositive();

      final int sharedCluster = ((Document) database.lookupByRID(shared, true)).getInteger("cluster");
      final List<Pair<RID, Float>> grouped = grouped(CLUSTERS, 2);

      assertThat(ridsOf(grouped)).as("the row is in the graph and in the delta buffer; it belongs to the answer once")
          .filteredOn(shared::equals).hasSize(1);
      assertThat(clustersOf(grouped)).as("and its group must not have spent two slots on one record")
          .filteredOn(cluster -> cluster == sharedCluster).hasSizeLessThanOrEqualTo(2);
      assertWellFormed(grouped);
    }

    /**
     * Nothing about the merge may change a query that has nothing to merge: with no rows written since the graph was
     * built, the grouped answer has to be exactly what it always was. The buffer is not necessarily empty even here -
     * a rebuild re-queues any vector it left unreachable, so the same row can sit in the graph and the buffer at once -
     * which is precisely the case the merge has to admit once rather than twice.
     */
    @Test
    void aQueryWithNothingNewToSeeIsUnchanged() {
      createSchema();
      insertBaseVertices();

      final List<Pair<RID, Float>> grouped = grouped(3, 2);

      assertThat(grouped).hasSize(6);
      assertThat(new HashSet<>(clustersOf(grouped))).hasSize(3);
      assertThat(clustersOf(grouped)).as("the group the query sits in cannot be absent from its own answer").contains(1);
      assertWellFormed(grouped);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void assertWellFormed(final List<Pair<RID, Float>> neighbors) {
      assertThat(ridsOf(neighbors)).as("the same row must not be admitted twice, from either side of the merge")
          .doesNotHaveDuplicates();
      for (int i = 1; i < neighbors.size(); i++)
        assertThat(neighbors.get(i).getSecond()).as("row %d is nearer than row %d, so the answer is not sorted", i, i - 1)
            .isGreaterThanOrEqualTo(neighbors.get(i - 1).getSecond());
    }

    private List<RID> ridsFromSql(final String query) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query, "Doc[embedding]", query(), 3)) {
        while (rs.hasNext())
          rids.add(rs.next().getIdentity().get());
      }
      return rids;
    }

    private List<RID> ridsOf(final List<Pair<RID, Float>> neighbors) {
      final List<RID> rids = new ArrayList<>(neighbors.size());
      for (final Pair<RID, Float> neighbor : neighbors)
        rids.add(neighbor.getFirst());
      return rids;
    }

    private List<Integer> clustersOf(final List<Pair<RID, Float>> neighbors) {
      final List<Integer> clusters = new ArrayList<>(neighbors.size());
      for (final Pair<RID, Float> neighbor : neighbors)
        clusters.add(((Document) database.lookupByRID(neighbor.getFirst(), true)).getInteger("cluster"));
      return clusters;
    }

    private Function<RID, Object> groupKeyResolver() {
      return rid -> ((Document) database.lookupByRID(rid, true)).getInteger("cluster");
    }

    private List<Pair<RID, Float>> grouped(final int limit, final int groupSize) {
      return vectorIndex().findNeighborsFromVectorGrouped(query(), limit, groupSize, -1, null, groupKeyResolver());
    }

    private long deltaCount() {
      return stat("deltaVectorsCount");
    }

    private long stat(final String name) {
      return vectorIndex().getStats().getOrDefault(name, 0L);
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
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
        builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("COSINE")
            .create();
      });
    }

    private List<RID> insertBaseVertices() {
      final List<RID> rids = new ArrayList<>(VERTICES);
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          rids.add(insert("doc" + i, i / PER_CLUSTER, embeddingAt(theta(i))));
      });
      // Force the first graph build outside the assertions: everything inserted above is now in the graph, and the
      // delta buffer is empty again.
      vectorIndex().findNeighborsFromVector(query(), 1);
      return rids;
    }

    /** {@code count} rows nearer to the query than every base vertex, each in a group of its own. */
    private List<RID> insertDeltaVertices(final int count) {
      final List<RID> rids = new ArrayList<>(count);
      for (int i = 0; i < count; i++)
        rids.add(insertDeltaVertex(QUERY_THETA + (i + 1) * DELTA_STEP, 90 + i));
      return rids;
    }

    /** {@code count} rows nearer to the query than every base vertex, all sharing one group. */
    private void insertDeltaVerticesInOneGroup(final int count, final int cluster) {
      for (int i = 0; i < count; i++)
        insertDeltaVertex(QUERY_THETA + (i + 1) * DELTA_STEP, cluster);
    }

    private RID insertDeltaVertex(final double atTheta, final int cluster) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = insert("delta-" + atTheta + "-" + cluster, cluster, embeddingAt(atTheta)));
      return rid[0];
    }

    private RID insert(final String id, final int cluster, final float[] embedding) {
      return ((Document) database.command("sql", "INSERT INTO Doc SET id = ?, cluster = ?, embedding = ?", id, cluster,
          embedding).next().getRecord().get()).getIdentity();
    }

    private static int centerOf(final int cluster) {
      return cluster * PER_CLUSTER + PER_CLUSTER / 2;
    }

    private static double theta(final int vertex) {
      return (vertex / PER_CLUSTER) * CLUSTER_GAP + (vertex % PER_CLUSTER - PER_CLUSTER / 2.0) * JITTER;
    }

    private float[] query() {
      return embeddingAt(QUERY_THETA);
    }

    /**
     * Points on an arc, embedded with damped harmonics so cosine similarity decreases strictly with the angular gap -
     * the same construction as {@code GroupedVectorSearchTest.GroupedSearchGroupChoice}, so which row is nearest is decidable on
     * paper rather than an artefact of the index.
     */
    private static float[] embeddingAt(final double angle) {
      final float[] v = new float[DIMENSIONS];
      for (int m = 1; m <= DIMENSIONS / 2; m++) {
        v[(m - 1) * 2] = (float) (Math.cos(m * angle) / m);
        v[(m - 1) * 2 + 1] = (float) (Math.sin(m * angle) / m);
      }
      return v;
    }
  }

  /**
   * Regression for #6559: the grouped-search shortfall signal fires only when groups were really lost.
   * <p>
   * Issue #6559 item 1 (and item 4): {@code groupedSearchesShortOfLimit} used to fire on every grouped search that came
   * back with fewer than {@code limit} distinct groups, whatever the reason. A corpus that genuinely holds fewer
   * distinct {@code groupBy} values than {@code limit} - not a corner case, just ordinary low-cardinality data - hit
   * that on <em>every single query, forever</em>, and the counter's own contract ("raise efSearch") cannot be honoured:
   * there is no wider beam that manufactures a group that does not exist.
   * <p>
   * The fix splits the counter in two: {@code groupedSearchesShortOfLimit} now fires only when the candidate budget cut
   * the walk short while the graph still had more to give (the case efSearch actually helps), and
   * {@code groupedSearchesGroupsUnavailable} fires when the reachable graph (or, on the pre-filter plan, the
   * allow-list) ran out on its own - which is also the signal item 4 asked for: a grouped query that comes up short for
   * a reason other than "the cap is doing its job" now leaves a trace on this counter, where before there was none.
   */
  @Nested
  class GroupedSearchShortfallSignal extends TestHelper {

    private static final int DIMENSIONS  = 16;
    private static final int VERTICES    = 300;
    private static final int LIMIT       = 5;
    private static final int GROUP_SIZE  = 3;
    private static final int QUERY_COUNT = 5;

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
     * Every vertex shares the same {@code groupBy} key, so no query - however many times it is repeated - can ever open
     * a second group. Before the fix this pinned {@code groupedSearchesShortOfLimit} up on every single one of them,
     * a counter nobody could act on because raising efSearch changes nothing about how many distinct group keys exist.
     */
    @Test
    void aLowCardinalityGroupByNeverCriesWolfOnTheActionableCounter() {
      createSchema();
      insertVertices();

      final long shortBefore = vectorIndex().getStats().get("groupedSearchesShortOfLimit");
      final long unavailableBefore = vectorIndex().getStats().get("groupedSearchesGroupsUnavailable");

      for (int i = 0; i < QUERY_COUNT; i++) {
        final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorGrouped(embedding(i), LIMIT,
            GROUP_SIZE, -1, null, rid -> "one-and-only");
        assertThat(neighbors).as("the one group that exists still has to come back, filled to groupSize")
            .hasSize(GROUP_SIZE);
      }

      assertThat(vectorIndex().getStats().get("groupedSearchesShortOfLimit"))
          .as("low groupBy cardinality is not a candidate-budget problem, so the efSearch-actionable counter must stay "
              + "untouched across every repeat of the same query")
          .isEqualTo(shortBefore);
      assertThat(vectorIndex().getStats().get("groupedSearchesGroupsUnavailable"))
          .as("but the shortfall still has to be visible somewhere, once per query")
          .isEqualTo(unavailableBefore + QUERY_COUNT);
    }

    /**
     * A narrow allow-list on the pre-filter plan (issue #6514) is the other path with no candidate-budget concept at
     * all: every allow-listed candidate is scored up front, so a shortfall here can never be the "raise efSearch" case
     * either.
     */
    @Test
    void aNarrowAllowListOnThePreFilterPlanAlsoUsesTheNonActionableCounter() {
      createSchema();
      insertVertices();

      final Set<RID> allowed = new HashSet<>();
      for (int i = 0; i < 10; i++)
        allowed.add(ridOf("doc" + i));

      final long shortBefore = vectorIndex().getStats().get("groupedSearchesShortOfLimit");
      final long unavailableBefore = vectorIndex().getStats().get("groupedSearchesGroupsUnavailable");

      final List<Pair<RID, Float>> neighbors = vectorIndex().findNeighborsFromVectorGrouped(embedding(0), LIMIT,
          GROUP_SIZE, -1, allowed, rid -> "one-and-only");

      assertThat(neighbors).as("the allow-listed rows are still answered").isNotEmpty();
      assertThat(vectorIndex().getStats().get("groupedSearchesShortOfLimit"))
          .as("the pre-filter plan cannot hit a candidate-budget shortfall")
          .isEqualTo(shortBefore);
      assertThat(vectorIndex().getStats().get("groupedSearchesGroupsUnavailable"))
          .as("an allow-list confined to one group cannot open %d groups", LIMIT)
          .isEqualTo(unavailableBefore + 1);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private RID ridOf(final String id) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = ?", id)) {
        return rs.next().getIdentity().orElseThrow();
      }
    }

    private void createSchema() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
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
          database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
      });
      // The graph is built on the first search, and it has to come out above ASYNC_REBUILD_MIN_GRAPH_SIZE or every
      // later search would rebuild it synchronously.
      vectorIndex().findNeighborsFromVector(embedding(0), 1);
    }

    private float[] embedding(final int vertex) {
      final float[] v = new float[DIMENSIONS];
      final double theta = vertex * 0.01;
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
   * Regression for #8057: the shortfall fallback nets the deletes of the calling transaction.
   * <p>
   * Regression test for issue #8057: the issue #3722 shortfall fallback compared the answer against
   * {@code vectorIndex().size()}, the COMMITTED live count, which still holds the rows the calling transaction removed -
   * their tombstones are written only when the commit replays the queued REMOVEs. Since #7378 the search excludes those
   * rows, so a search inside such a transaction asking for (nearly) the whole corpus always came up "short": it logged a
   * WARNING that the graph may need rebuilding and ran a brute-force scan over every ordinal that found nothing new.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ShortfallNetsOwnDeletes extends TestHelper {
    private static final int DIMENSIONS = 8;
    private static final int SEEDED     = 30;

    @BeforeEach
    void freezeTheGraph() {
      // THE REMOVED ROW MUST STAY IN THE COMMITTED GRAPH: A REBUILD WOULD NOT CHANGE WHAT IS PINNED, BUT IT WOULD BUMP
      // THE METRIC THE TEST READS
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

    @Test
    void aSearchForTheWholeCorpusAfterAnOwnDeleteDoesNotFallBackToBruteForce() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        database.lookupByRID(ridOf("doc3"), true).delete();

        final long scansBefore = bruteForceScans(index);
        for (int i = 0; i < 3; i++) {
          final List<String> ids = idsOf(index.findNeighborsFromVector(seedVector(0), SEEDED));
          assertThat(ids).hasSize(SEEDED - 1).doesNotContain("doc3");
        }
        assertThat(bruteForceScans(index))
            .as("every row the transaction can see was returned: there is no shortfall to fall back on")
            .isEqualTo(scansBefore);
      } finally {
        database.rollback();
      }
    }

    /** Several deletes, and a k above the live count the transaction sees but below the committed one. */
    @Test
    void severalOwnDeletesAreAllNettedOut() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        for (final String id : new String[] { "doc1", "doc7", "doc20" })
          database.lookupByRID(ridOf(id), true).delete();

        final long scansBefore = bruteForceScans(index);
        assertThat(idsOf(index.findNeighborsFromVector(seedVector(5), SEEDED - 1)))
            .hasSize(SEEDED - 3)
            .doesNotContain("doc1", "doc7", "doc20");
        assertThat(bruteForceScans(index)).isEqualTo(scansBefore);
      } finally {
        database.rollback();
      }
    }

    /** A rewritten row is superseded on the committed side and contributed again on the pending side: still no shortfall. */
    @Test
    void aReEmbeddedRowIsNotAShortfallEither() {
      seedAndBuildGraph();
      final LSMVectorIndex index = vectorIndex();

      database.begin();
      try {
        final float[] moved = seedVector(29);
        moved[5] = 0.5f;
        database.lookupByRID(ridOf("doc4"), true).asDocument().modify().set("embedding", moved).save();

        final long scansBefore = bruteForceScans(index);
        assertThat(idsOf(index.findNeighborsFromVector(seedVector(0), SEEDED))).hasSize(SEEDED).containsOnlyOnce("doc4");
        assertThat(bruteForceScans(index)).isEqualTo(scansBefore);
      } finally {
        database.rollback();
      }
    }

    private static long bruteForceScans(final LSMVectorIndex index) {
      return ((Number) index.getStats().get("bruteForceScans")).longValue();
    }

    private void seedAndBuildGraph() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        for (int i = 0; i < SEEDED; i++)
          database.newDocument("Doc").set("id", "doc" + i).set("embedding", seedVector(i)).save();
      });

      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {
            "dimensions": %d,
            "similarity": "COSINE"
          }""".formatted(DIMENSIONS));

      // ONE SEARCH OUTSIDE ANY TRANSACTION MOVES THE ROWS INTO THE GRAPH: THE SHORTFALL CHECK IS ON THE GRAPH PATH
      final LSMVectorIndex index = vectorIndex();
      index.findNeighborsFromVector(seedVector(0), 1);
      assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) SEEDED);
    }

    private static float[] seedVector(final int i) {
      final float[] v = new float[DIMENSIONS];
      v[0] = 1.0f;
      v[1] = 0.02f * (i + 1);
      v[2] = 0.01f * ((i * 7) % 13);
      v[3] = 0.005f * ((i * 3) % 11);
      v[4] = 0.001f * (i + 1);
      return v;
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

  /**
   * Regression for #9633: the grouped search reranks on the stored vectors under BINARY quantization.
   * <p>
   * Issue #9633: under BINARY quantization the grouped search ({@code groupBy} on {@code vector.neighbors}) skipped the
   * rerank on the stored vectors that the plain search applies (#8960). It returned the graph's distances, computed on a
   * one-bit reconstruction of each vector, so recall halved, a record queried by its own vector reported a non-zero
   * distance, and a {@code maxDistance} bound that kept the exact match without {@code groupBy} dropped it with it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryGroupedRerank extends TestHelper {
    private static final int DIM     = 32;
    private static final int RECORDS = 1000;

    private final Map<RID, float[]> vectors = new HashMap<>();
    private final Map<RID, Integer> pids    = new HashMap<>();

    @Test
    void groupedSearchReturnsTheExactSelfMatchThroughSql() {
      createCorpus(500, 1);
      final float[] query = vectorOfPid(42);

      final List<Result> plain = query("SELECT expand(`vector.neighbors`('P[embedding]', ?, 3))", query);
      final List<Result> grouped = query(
          "SELECT expand(`vector.neighbors`('P[embedding]', ?, 3, { groupBy: 'grp', groupSize: 1 }))", query);

      assertThat(plain.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
      assertThat(grouped.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
      assertThat(grouped.getFirst().<Float>getProperty("distance")).isCloseTo(0f, within(1e-5f));
      for (int i = 0; i < plain.size(); i++) {
        assertThat(grouped.get(i).<Integer>getProperty("pid")).isEqualTo(plain.get(i).<Integer>getProperty("pid"));
        assertThat(grouped.get(i).<Float>getProperty("distance")).isCloseTo(plain.get(i).<Float>getProperty("distance"),
            within(1e-5f));
      }

      final List<Result> bounded = query(
          "SELECT expand(`vector.neighbors`('P[embedding]', ?, 3, { groupBy: 'grp', groupSize: 1, maxDistance: 0.01 }))",
          query);
      assertThat(bounded).hasSize(1);
      assertThat(bounded.getFirst().<Integer>getProperty("pid")).isEqualTo(42);
    }

    /**
     * One group per record and {@code groupSize = 1}, so the grouped answer must be the plain top-k: grouping by itself
     * removes nothing, and the two paths must reach the same recall and report the same, exact, distances.
     */
    @Test
    void groupedRecallMatchesThePlainSearch() {
      createCorpus(RECORDS, 1);
      final LSMVectorIndex index = bucketIndex();
      final Function<RID, Object> groupOf = rid -> rid.asDocument().get("grp");

      final Random random = new Random(9633);
      final int k = 10;
      double plainRecall = 0;
      double groupedRecall = 0;
      final int queries = 50;
      for (int q = 0; q < queries; q++) {
        final float[] query = unit(random);
        final Set<RID> truth = bruteForce(query, k);

        final List<Pair<RID, Float>> plain = index.findNeighborsFromVector(query, k, 128, null);
        final List<Pair<RID, Float>> grouped = index.findNeighborsFromVectorGrouped(query, k, 1, 128, null, groupOf);

        plainRecall += overlap(plain, truth);
        groupedRecall += overlap(grouped, truth);

        assertThat(grouped).hasSize(k);
        float previous = Float.NEGATIVE_INFINITY;
        for (final Pair<RID, Float> row : grouped) {
          assertThat(row.getSecond()).as("the distance must be the exact one").isCloseTo(exact(query, row.getFirst()), within(1e-3f));
          assertThat(row.getSecond()).isGreaterThanOrEqualTo(previous);
          previous = row.getSecond();
        }
      }
      plainRecall /= queries * k;
      groupedRecall /= queries * k;

      assertThat(groupedRecall).as("grouped recall %s against plain recall %s", groupedRecall, plainRecall)
          .isGreaterThanOrEqualTo(plainRecall - 0.02);
    }

    /**
     * Groups with several members: every row carries its exact distance, no group exceeds its cap, and the members of each
     * group are its nearest ones among the rows reranked.
     */
    @Test
    void groupedSearchWithSharedGroupsAppliesTheCapOnExactDistances() {
      createCorpus(RECORDS, 50);
      final LSMVectorIndex index = bucketIndex();
      final Function<RID, Object> groupOf = rid -> rid.asDocument().get("grp");

      final Random random = new Random(42);
      for (int q = 0; q < 20; q++) {
        final float[] query = unit(random);
        final List<Pair<RID, Float>> grouped = index.findNeighborsFromVectorGrouped(query, 5, 3, 128, null, groupOf);

        assertThat(grouped).hasSize(15);
        final Map<Object, Integer> perGroup = new HashMap<>();
        float previous = Float.NEGATIVE_INFINITY;
        for (final Pair<RID, Float> row : grouped) {
          assertThat(row.getSecond()).isCloseTo(exact(query, row.getFirst()), within(1e-3f));
          assertThat(row.getSecond()).isGreaterThanOrEqualTo(previous);
          previous = row.getSecond();
          perGroup.merge(groupOf.apply(row.getFirst()), 1, Integer::sum);
        }
        assertThat(perGroup).hasSize(5);
        assertThat(perGroup.values()).allMatch(count -> count == 3);
      }

      // The restricted search of a multi-bucket merge (issue #8002) is reranked too: its bound is an exact distance
      final float[] self = vectorOfPid(7);
      final Object group = groupOf.apply(ridOfPid(7));
      final List<Pair<RID, Float>> restricted = index.findNeighborsFromVectorForGroups(self, Set.of(group), 2, 128, null, groupOf,
          0.01f);
      assertThat(restricted).hasSize(1);
      assertThat(restricted.getFirst().getFirst()).isEqualTo(ridOfPid(7));
      assertThat(restricted.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
    }

    /**
     * The members that hold a group's slots are its nearest on the stored vectors, not on the graph's approximation. Every
     * member of the near group has the sign pattern of the query, so the one-bit codes cannot tell them apart and the graph
     * hands them over in an order that has nothing to do with their real distance. They are inserted farthest first, so a
     * cap applied in that order keeps the wrong two.
     */
    @Test
    void theNearestStoredVectorsWinTheGroupCap() {
      database.command("sql", "CREATE DOCUMENT TYPE Q BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Q.grp STRING");
      database.command("sql", "CREATE PROPERTY Q.rank INTEGER");
      database.command("sql", "CREATE PROPERTY Q.embedding ARRAY_OF_FLOATS");

      final Random random = new Random(9633);
      final float[] query = new float[DIM];
      for (int i = 0; i < DIM; i++)
        query[i] = 1f + random.nextFloat();

      final int members = 8;
      database.transaction(() -> {
        // The near group: the query plus noise that grows with the rank and never flips a sign. Farthest first
        for (int rank = members; rank >= 1; rank--) {
          final float[] v = new float[DIM];
          for (int i = 0; i < DIM; i++)
            v[i] = query[i] + (random.nextFloat() - 0.5f) * 0.1f * rank;
          database.newDocument("Q").set("grp", "near", "rank", rank, "embedding", v).save();
        }
        // Far groups: mixed signs, so they never compete with the near group
        for (int g = 0; g < 30; g++)
          for (int m = 0; m < 3; m++)
            database.newDocument("Q").set("grp", "far" + g, "rank", 0, "embedding", unit(random)).save();
      });
      database.command("sql", "CREATE INDEX ON Q (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
          + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");

      final List<Integer> ranks = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT rank FROM (SELECT expand(`vector.neighbors`('Q[embedding]', ?, 1, { groupBy: 'grp', groupSize: 2 })))", query)) {
        while (rs.hasNext())
          ranks.add(rs.next().getProperty("rank"));
      }
      assertThat(ranks).containsExactly(1, 2);
    }

    private void createCorpus(final int records, final int groupModulo) {
      database.command("sql", "CREATE DOCUMENT TYPE P BUCKETS 1");
      database.command("sql", "CREATE PROPERTY P.pid INTEGER");
      database.command("sql", "CREATE PROPERTY P.grp INTEGER");
      database.command("sql", "CREATE PROPERTY P.embedding ARRAY_OF_FLOATS");

      final Random random = new Random(records);
      database.transaction(() -> {
        for (int i = 0; i < records; i++) {
          final float[] v = unit(random);
          final Document doc = database.newDocument("P").set("pid", i, "grp", groupModulo == 1 ? i : i % groupModulo, "embedding", v)
              .save();
          vectors.put(doc.getIdentity(), v);
          pids.put(doc.getIdentity(), i);
        }
      });
      database.command("sql", "CREATE INDEX ON P (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
          + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");
    }

    private LSMVectorIndex bucketIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("P[embedding]")).getIndexesOnBuckets()[0];
    }

    private List<Result> query(final String sql, final float[] query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }

    private RID ridOfPid(final int pid) {
      for (final Map.Entry<RID, Integer> entry : pids.entrySet())
        if (entry.getValue() == pid)
          return entry.getKey();
      throw new IllegalArgumentException("No record with pid " + pid);
    }

    private float[] vectorOfPid(final int pid) {
      return vectors.get(ridOfPid(pid));
    }

    private Set<RID> bruteForce(final float[] query, final int k) {
      final List<Map.Entry<RID, float[]>> all = new ArrayList<>(vectors.entrySet());
      all.sort((a, b) -> Double.compare(l2(query, a.getValue()), l2(query, b.getValue())));
      final Set<RID> top = new HashSet<>();
      for (int i = 0; i < k; i++)
        top.add(all.get(i).getKey());
      return top;
    }

    private static int overlap(final List<Pair<RID, Float>> answer, final Set<RID> truth) {
      int hits = 0;
      for (final Pair<RID, Float> row : answer)
        if (truth.contains(row.getFirst()))
          ++hits;
      return hits;
    }

    /** The distance the index reports under EUCLIDEAN, computed on the stored vector: the squared L2 distance. */
    private float exact(final float[] query, final RID rid) {
      return (float) l2(query, vectors.get(rid));
    }

    private static double l2(final float[] a, final float[] b) {
      double sum = 0;
      for (int i = 0; i < a.length; i++) {
        final double d = a[i] - b[i];
        sum += d * d;
      }
      return sum;
    }

    private static float[] unit(final Random r) {
      final float[] v = new float[DIM];
      double s = 0;
      for (int i = 0; i < DIM; i++) {
        v[i] = (float) r.nextGaussian();
        s += v[i] * v[i];
      }
      s = Math.sqrt(s);
      for (int i = 0; i < DIM; i++)
        v[i] /= (float) s;
      return v;
    }
  }
}
