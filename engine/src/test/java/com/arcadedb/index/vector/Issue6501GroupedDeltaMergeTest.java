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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue6501GroupedDeltaMergeTest extends TestHelper {

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
    assertThat(grouped.get(0).getSecond()).as("and the nearest row must be the nearest row")
        .isEqualTo(ungrouped.get(0).getSecond());
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

    final RID shared = vectorIndex().findNeighborsFromVector(query(), 1).get(0).getFirst();
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
   * the same construction as {@code Issue5761GroupedSearchGroupChoiceTest}, so which row is nearest is decidable on
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
