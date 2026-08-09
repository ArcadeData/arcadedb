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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.executor.steps.CSRCountUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5757, the follow-up to #5715.
 * <p>
 * Two things, both of them about a label that is not there:
 * <ol>
 *   <li><b>An unlabelled anchor.</b> Every count-push-down operator walks out from one position of the pattern.
 *   {@code MATCH ()-[:TYPE]->() RETURN count(*)} labels none of them, which used to leave the operators with no set
 *   to enumerate; #5715 fixed the wrong answer that produced by declining to build the operator at all, so the
 *   cheapest question there is to ask a graph was the one shape the fast path could not serve. The two chain
 *   operators now read the absent label for what it means - <b>every vertex</b> - on both the CSR and the OLTP
 *   path.</li>
 *   <li><b>The overload that caused it.</b> {@code CSRCountUtils.buildValidBuckets} returned {@code null} both for
 *   "no label was given" and for "the label matches nothing", and its callers disagreed about which one they held.
 *   The two answers are now distinguishable: {@code null} is no filter, an empty set is a filter that keeps
 *   nothing.</li>
 * </ol>
 * Every count here is cross-checked against the row count of the same pattern projected without an aggregate, which
 * is the ordinary materialization pipeline answering the same question. The chains deliberately use a distinct edge
 * type per hop so that relationship isomorphism - which the push-downs do not model and the pipeline does - cannot
 * make the two disagree for an unrelated reason.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountPushDownUnlabelledAnchorIssue5757Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub");
      database.getSchema().createVertexType("Leaf");
      database.getSchema().createVertexType("Idle");
      database.getSchema().createEdgeType("LINKS");
      database.getSchema().createEdgeType("BOND");
      database.getSchema().createEdgeType("WIRE");

      // Two labels carry edges, so an unlabelled anchor has to reach beyond any single type.
      final MutableVertex h1 = database.newVertex("Hub").set("k", "h1").save();
      final MutableVertex h2 = database.newVertex("Hub").set("k", "h2").save();
      final MutableVertex l1 = database.newVertex("Leaf").set("k", "l1").save();
      final MutableVertex l2 = database.newVertex("Leaf").set("k", "l2").save();
      final MutableVertex l3 = database.newVertex("Leaf").set("k", "l3").save();

      // Vertices with no edge at all: seeding "every vertex" must not turn them into rows.
      for (int i = 1; i <= 4; i++)
        database.newVertex("Idle").set("k", "i" + i).save();

      h1.newEdge("LINKS", l1).save();
      h1.newEdge("LINKS", l2).save();
      h2.newEdge("LINKS", l1).save();
      l1.newEdge("LINKS", h2).save();
      l3.newEdge("LINKS", h1).save();

      l1.newEdge("BOND", h1).save();
      l2.newEdge("BOND", h2).save();
      h2.newEdge("BOND", l3).save();

      h1.newEdge("WIRE", h2).save();
      l3.newEdge("WIRE", l1).save();
      h2.newEdge("WIRE", h1).save();
    });
  }

  // ===================================================================================================
  // 1. an unlabelled anchor is served by the push-down, and answers what the pipeline answers
  // ===================================================================================================

  /**
   * {@code MATCH ()-[:TYPE]->() RETURN count(*)} - "how many edges of this type are there" - is exactly the question
   * the CSR operators exist for, and #5715 left it as the one shape they declined.
   */
  @Test
  void anUnlabelledAnchorIsClaimedByTheChainPushDown() {
    assertThat(explainOf("MATCH (a)-[:LINKS]->(b) RETURN count(*) AS c")).contains("COUNT CHAIN");
    assertThat(explainOf("MATCH (a)-[:LINKS]->(b:Leaf) RETURN count(*) AS c")).contains("COUNT CHAIN");
    assertThat(explainOf("MATCH ()-[:LINKS]->() RETURN count(*) AS c")).contains("COUNT CHAIN");
  }

  /** One hop, in every direction, with the label on neither end, on one end, or on both. */
  @Test
  void aSingleHopWithAnUnlabelledAnchorCountsEveryEdge() {
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)", 5);
    assertPushedDownCountMatchesThePipeline("MATCH (a)<-[:LINKS]-(b)", 5);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]-(b)", 10);

    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:Leaf)", 3);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:Hub)", 2);

    // The labelled shapes the operator always served, unchanged.
    assertPushedDownCountMatchesThePipeline("MATCH (a:Hub)-[:LINKS]->(b)", 3);
    assertPushedDownCountMatchesThePipeline("MATCH (a:Hub)-[:LINKS]->(b:Leaf)", 3);
    assertPushedDownCountMatchesThePipeline("MATCH (a:Leaf)-[:LINKS]->(b:Hub)", 2);
  }

  /** A multi-hop chain: the anchor seeding is one step, every hop after it has to propagate from all of them. */
  @Test
  void aMultiHopChainWithAnUnlabelledAnchorCountsEveryPath() {
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)", 4);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)-[:WIRE]->(d)", 4);
    // A label further along the chain still filters, and the unlabelled anchor still starts everywhere.
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:Leaf)-[:BOND]->(c)", 3);
  }

  /**
   * With an inequality the operator subtracts the paths whose two ends are the same vertex, and that subtraction
   * walks out from the inequality's earlier position - the unlabelled anchor again. Three of the four 3-hop paths
   * here close on themselves, so a subtraction that enumerated nothing would answer 4 instead of 1.
   */
  @Test
  void anInequalityOverAnUnlabelledAnchorSubtractsTheClosedPaths() {
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE a <> c", 3);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)-[:WIRE]->(d) WHERE a <> d", 1);
  }

  /** The anti-join operator anchors the same way, and had the same {@code anchorLabel == null} reading. */
  @Test
  void anAntiJoinOverAnUnlabelledAnchorCountsEveryPath() {
    assertPushedDownCountMatchesThePipeline(
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE NOT (a)-[:WIRE]->(c)", 2);
    assertPushedDownCountMatchesThePipeline(
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE NOT (a)-[:WIRE]->(c) AND a <> c", 1);
  }

  /**
   * The same answers off the CSR arrays. A view registered for the database moves every one of these from the OLTP
   * branch of the operator to the dense-propagation one, which is where the "seed every node" reading of an absent
   * anchor label lives.
   */
  @Test
  void theCsrPathAgreesWithTheOltpPathOnEveryUnlabelledShape() {
    final List<String> patterns = List.of(
        "MATCH (a)-[:LINKS]->(b)",
        "MATCH (a)<-[:LINKS]-(b)",
        "MATCH (a)-[:LINKS]-(b)",
        "MATCH (a)-[:LINKS]->(b:Leaf)",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)-[:WIRE]->(d)",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE a <> c",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c)-[:WIRE]->(d) WHERE a <> d",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE NOT (a)-[:WIRE]->(c)",
        "MATCH (a)-[:LINKS]->(b)-[:BOND]->(c) WHERE NOT (a)-[:WIRE]->(c) AND a <> c");

    final List<Long> withoutView = new ArrayList<>();
    for (final String pattern : patterns)
      withoutView.add(scalarOf(pattern + " RETURN count(*) AS c"));

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("everything")
        .withVertexTypes("Hub", "Leaf", "Idle")
        .withEdgeTypes("LINKS", "BOND", "WIRE")
        .build();
    try {
      // Without a ready provider covering the edge types, every assertion below would re-run the OLTP path and
      // compare it against itself.
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "LINKS", "BOND", "WIRE")).isNotNull();

      for (int i = 0; i < patterns.size(); i++)
        assertThat(scalarOf(patterns.get(i) + " RETURN count(*) AS c")).as(patterns.get(i)).isEqualTo(withoutView.get(i));
    } finally {
      view.drop();
    }
  }

  /**
   * "Every vertex" is a claim about the graph, not about a view. A view built over one vertex type accelerates a
   * walk anchored on a label it holds, but it cannot enumerate the anchors of an unlabelled one: its node domain is
   * a subset of them. The operator falls back to the OLTP path rather than counting the subset.
   */
  @Test
  void aViewThatHoldsSomeOfTheVerticesDoesNotAnswerAnUnlabelledAnchor() {
    final GraphAnalyticalView partial = GraphAnalyticalView.builder(database)
        .withName("hubs-only")
        .withVertexTypes("Hub")
        .withEdgeTypes("LINKS")
        .build();
    try {
      assertThat(GraphTraversalProviderRegistry.awaitAll(database, 30, TimeUnit.SECONDS)).isTrue();
      // The view IS offered to the operator - it covers the edge type - so the count below is saved by the
      // coverage test rather than by there being no provider at all.
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "LINKS")).isNotNull();

      // No LINKS edge joins two Hubs, so a view holding only Hubs would count 0 for a pattern with five matches.
      assertThat(scalarOf("MATCH (a)-[:LINKS]->(b) RETURN count(*) AS c")).isEqualTo(5L);
    } finally {
      partial.drop();
    }
  }

  /** Idle vertices carry no edge of any type: seeding every vertex must not turn them into rows. */
  @Test
  void verticesWithNoEdgeContributeNothing() {
    assertThat(rowCountOf("MATCH (i:Idle) RETURN i.k AS c")).isEqualTo(4);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b)", 5);
  }

  // ===================================================================================================
  // 2. the helper no longer says "no filter" and "matches nothing" with the same null
  // ===================================================================================================

  /**
   * The contract every operator here now reads. The two cases are separated at the source rather than at each of the
   * dozen sites that consume it, which is what stops the class of bug rather than the two instances of it #5715
   * found.
   */
  @Test
  void buildValidBucketsSeparatesNoFilterFromMatchesNothing() {
    // No label given: nothing to filter on.
    assertThat(CSRCountUtils.buildValidBuckets(database, null)).isNull();

    // A label no type declares: a filter that keeps nothing. Not null - that would read as "no filter".
    final IntHashSet undeclared = CSRCountUtils.buildValidBuckets(database, "NoSuchLabel");
    assertThat(undeclared).isNotNull();
    assertThat(undeclared.isEmpty()).isTrue();

    // A declared label: its own buckets.
    final IntHashSet hub = CSRCountUtils.buildValidBuckets(database, "Hub");
    assertThat(hub).isNotNull();
    assertThat(hub.isEmpty()).isFalse();
    for (final int bucketId : database.getSchema().getType("Hub").getBucketIds(true))
      assertThat(hub.contains(bucketId)).isTrue();
    assertThat(hub.contains(database.getSchema().getType("Leaf").getBucketIds(true).get(0))).isFalse();
  }

  /**
   * And the consumer side of it: an empty set zeroes every count instead of being waved through as "no filter",
   * which is the shape of the second wrong answer #5715 found.
   */
  @Test
  void filterByBucketsKeepsNothingForAnEmptySet() {
    final int[] bucketIds = { 3, 4, 5 };

    final long[] noFilter = { 7L, 8L, 9L };
    CSRCountUtils.filterByBuckets(bucketIds, noFilter, null);
    assertThat(noFilter).containsExactly(7L, 8L, 9L);

    final long[] matchesNothing = { 7L, 8L, 9L };
    CSRCountUtils.filterByBuckets(bucketIds, matchesNothing, new IntHashSet());
    assertThat(matchesNothing).containsExactly(0L, 0L, 0L);

    final IntHashSet onlyFour = new IntHashSet();
    onlyFour.add(4);
    final long[] filtered = { 7L, 8L, 9L };
    CSRCountUtils.filterByBuckets(bucketIds, filtered, onlyFour);
    assertThat(filtered).containsExactly(0L, 8L, 0L);
  }

  /** A label the schema does not declare keeps answering 0, whichever end of the pattern carries it. */
  @Test
  void anUndeclaredLabelStillMatchesNothing() {
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:NoSuchLabel)", 0);
    assertPushedDownCountMatchesThePipeline("MATCH (a:NoSuchLabel)-[:LINKS]->(b)", 0);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:NoSuchLabel)-[:BOND]->(c)", 0);
    assertPushedDownCountMatchesThePipeline("MATCH (a)-[:LINKS]->(b:Idle)", 0);
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  /**
   * Asserts the pushed-down count of the pattern, the row count the ordinary pipeline produces for the same pattern,
   * and the expected value all agree. The pipeline reference is what makes the expected number more than a
   * transcription of what the operator currently does.
   */
  private void assertPushedDownCountMatchesThePipeline(final String matchClause, final long expected) {
    assertThat(scalarOf(matchClause + " RETURN count(*) AS c")).as(matchClause).isEqualTo(expected);
    assertThat(rowCountOf(matchClause + " RETURN a")).as(matchClause + " (pipeline)").isEqualTo((int) expected);
  }

  /** The single {@code c} value of a one-row query. */
  private long scalarOf(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    assertThat(values).as(query).hasSize(1);
    return values.get(0);
  }

  /** The rendered execution plan of {@code EXPLAIN <query>}. */
  private String explainOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** How many rows the query produced, whatever they hold. */
  private int rowCountOf(final String query) {
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }
}
