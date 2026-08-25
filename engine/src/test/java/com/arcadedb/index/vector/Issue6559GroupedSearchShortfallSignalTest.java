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
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue6559GroupedSearchShortfallSignalTest extends TestHelper {

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
