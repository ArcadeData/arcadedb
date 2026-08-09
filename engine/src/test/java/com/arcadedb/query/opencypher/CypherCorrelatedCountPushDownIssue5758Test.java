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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5758: a <b>correlated</b> {@code COUNT { }} / {@code COLLECT { }} body lost both count
 * push-downs and materialized one row per edge per outer row, where the bound anchor makes the same count an
 * O(degree) read of the adjacency arrays.
 * <p>
 * #5686 restored the push-downs for a body that reads none of the seeded names, and #5737 extended that to
 * {@code COUNT { }} bodies. Neither helped the ordinary shape - "each person and how many people they know" - because
 * a body reading the seeded name was refused wholesale. What that guard is really protecting against is a body that
 * reads a seeded name at a position the operator cannot start from: seeding the chain from the bound RID instead of
 * from a label's bucket set answers exactly the same question, and answers it from one anchor rather than from every
 * vertex carrying the label.
 * <p>
 * The plan choice is measured rather than timed, the way {@code CypherCountPushDownPreconditionsIssue5715Test} and
 * {@code CypherUncorrelatedSubqueryCountPushDownIssue5686Test} measure it: a {@code readRecord} delta from
 * {@code database.getStats()}. The ordinary pipeline reads one record per matched row, a seeded push-down reads the
 * anchor's adjacency and nothing else, so the delta separates the two plans without a wall clock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCorrelatedCountPushDownIssue5758Test extends TestHelper {
  /** Wide enough that "one read per matched row" and "one read per anchor" cannot be confused. */
  private static final int FANOUT = 200;

  @Override
  protected void beginTest() {
    // One hub with FANOUT outgoing edges, and one sink with FANOUT incoming ones: the two ends of a chain.
    database.command("opencypher", "CREATE (:Hub {k: 1})");
    database.command("opencypher", "MATCH (h:Hub) UNWIND range(1, " + FANOUT + ") AS i CREATE (h)-[:LINKS]->(:Leaf {k: i})");
    database.command("opencypher", "CREATE (:Sink {k: 1})");
    database.command("opencypher", "MATCH (s:Sink) UNWIND range(1, " + FANOUT + ") AS i CREATE (:Src {k: i})-[:PTS]->(s)");
    // A second hop of a different type, so a two-hop chain can be pushed down: the chain detector refuses a repeated
    // edge type without an inequality, so `-[:LINKS]->()-[:LINKS]->()` would never reach the seeded walk.
    database.command("opencypher", "MATCH (l:Leaf), (s:Sink) WHERE l.k <= 3 CREATE (l)-[:TAGS]->(s)");

    // A tiny chain for the correctness cases, kept apart from the wide fixture so the counts are readable.
    database.command("opencypher", "CREATE (q1:Q {k: 1})-[:LINKS]->(q2:Q {k: 2})-[:LINKS]->(q3:Q {k: 3})");
    // A vertex of another label, to check that a label written on the seeded anchor still filters.
    database.command("opencypher", "CREATE (:Other {k: 1})");
  }

  // ===================================================================================================
  // 1. the gap this closes: a correlated body keeps the push-down, seeded from the bound anchor
  // ===================================================================================================

  /**
   * The issue's headline query. {@code COUNT { }} over a body whose only correlation is the anchor of the chain now
   * costs the anchor's adjacency instead of one record per edge.
   */
  @Test
  void aCountBodyAnchoredOnTheSeededVariableIsPushedDown() {
    final String pushedDown = "MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Leaf) } AS c";
    // The same count over the same edges, made unpushable by a predicate the operators cannot honor. It is the
    // control for what the ordinary pipeline costs on this fixture.
    final String ordinary = "MATCH (h:Hub) RETURN COUNT { MATCH (h)-[:LINKS]->(x:Leaf) WHERE x.k > 0 } AS c";

    assertThat(countsOf(pushedDown)).containsExactly((long) FANOUT);
    assertThat(countsOf(ordinary)).containsExactly((long) FANOUT);

    final long ordinaryReads = recordsReadBy(ordinary);
    assertThat(ordinaryReads).as("the control has to actually materialize the rows").isGreaterThanOrEqualTo(FANOUT);
    assertThat(recordsReadBy(pushedDown)).as(pushedDown).isLessThan(ordinaryReads / 10);
  }

  /** The same gap through {@code COLLECT { }}, where the body spells the count out itself. */
  @Test
  void aCollectBodyAnchoredOnTheSeededVariableIsPushedDown() {
    final String pushedDown = "MATCH (h:Hub) RETURN COLLECT { MATCH (h)-[:LINKS]->(x:Leaf) RETURN count(*) } AS c";
    final String ordinary = "MATCH (h:Hub) RETURN COLLECT { MATCH (h)-[:LINKS]->(x:Leaf) WHERE x.k > 0 RETURN count(*) } AS c";

    assertThat(collectOfLongs(pushedDown)).containsExactly((long) FANOUT);
    assertThat(collectOfLongs(ordinary)).containsExactly((long) FANOUT);

    final long ordinaryReads = recordsReadBy(ordinary);
    assertThat(ordinaryReads).as("the control has to actually materialize the rows").isGreaterThanOrEqualTo(FANOUT);
    assertThat(recordsReadBy(pushedDown)).as(pushedDown).isLessThan(ordinaryReads / 10);
  }

  /** And through a scoped {@code CALL (v) { }}, the third door onto the same plan. */
  @Test
  void aScopedCallBodyAnchoredOnTheImportedVariableIsPushedDown() {
    final String pushedDown = "MATCH (h:Hub) CALL (h) { MATCH (h)-[:LINKS]->(x:Leaf) RETURN count(*) AS c } RETURN c";
    final String ordinary =
        "MATCH (h:Hub) CALL (h) { MATCH (h)-[:LINKS]->(x:Leaf) WHERE x.k > 0 RETURN count(*) AS c } RETURN c";

    assertThat(countsOf(pushedDown)).containsExactly((long) FANOUT);
    assertThat(countsOf(ordinary)).containsExactly((long) FANOUT);

    final long ordinaryReads = recordsReadBy(ordinary);
    assertThat(ordinaryReads).as("the control has to actually materialize the rows").isGreaterThanOrEqualTo(FANOUT);
    assertThat(recordsReadBy(pushedDown)).as(pushedDown).isLessThan(ordinaryReads / 10);
  }

  /**
   * The anchor does not have to be written first. {@code COUNT { (:Src)-[:PTS]->(s) }} - "how many point at me" - is
   * the same chain walked the other way, and the seeded end is the one the walk starts from.
   */
  @Test
  void aBodyWhoseSeededVariableIsTheChainsFarEndIsPushedDown() {
    final String pushedDown = "MATCH (s:Sink) RETURN COUNT { (:Src)-[:PTS]->(s) } AS c";
    final String ordinary = "MATCH (s:Sink) RETURN COUNT { MATCH (x:Src)-[:PTS]->(s) WHERE x.k > 0 } AS c";

    assertThat(countsOf(pushedDown)).containsExactly((long) FANOUT);
    assertThat(countsOf(ordinary)).containsExactly((long) FANOUT);

    final long ordinaryReads = recordsReadBy(ordinary);
    assertThat(ordinaryReads).as("the control has to actually materialize the rows").isGreaterThanOrEqualTo(FANOUT);
    assertThat(recordsReadBy(pushedDown)).as(pushedDown).isLessThan(ordinaryReads / 10);
  }

  // ===================================================================================================
  // 2. the answers, which are the whole point of not answering from the global count
  // ===================================================================================================

  /** A seeded anchor counts that anchor's paths, never the graph's. Both spellings, one and two hops. */
  @Test
  void aSeededChainCountsOnlyThePathsOutOfTheBoundAnchor() {
    assertThat(collectOfLongs("MATCH (q:Q {k: 1}) RETURN COLLECT { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) } AS c"))
        .containsExactly(1L);
    assertThat(collectOfLongs("MATCH (q:Q {k: 2}) RETURN COLLECT { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) } AS c"))
        .containsExactly(1L);
    assertThat(collectOfLongs("MATCH (q:Q {k: 3}) RETURN COLLECT { MATCH (q)-[:LINKS]->(x:Q) RETURN count(*) } AS c"))
        .containsExactly(0L);

    // Every Q at once, so a plan answering from the global count would give 2 to each of the three rows.
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]->(:Q) } AS c ORDER BY c"))
        .containsExactly(0L, 1L, 1L);
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]->(:Q)-[:LINKS]->(:Q) } AS c ORDER BY c"))
        .containsExactly(0L, 0L, 1L);
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (:Q)-[:LINKS]->(q) } AS c ORDER BY c"))
        .containsExactly(0L, 1L, 1L);
  }

  /**
   * A label written on the seeded anchor is a filter on the bound vertex, not a set to enumerate: the same body under
   * an anchor of another label has to answer 0 rather than the count over the label it names.
   */
  @Test
  void aLabelOnTheSeededAnchorFiltersTheBoundVertex() {
    assertThat(countsOf("MATCH (n:Other) RETURN COUNT { (n:Q)-[:LINKS]->(:Q) } AS c")).containsExactly(0L);
    assertThat(countsOf("MATCH (n:Q {k: 1}) RETURN COUNT { (n:Q)-[:LINKS]->(:Q) } AS c")).containsExactly(1L);
    assertThat(countsOf("MATCH (n:Q {k: 1}) RETURN COUNT { (n:NoSuchLabel)-[:LINKS]->(:Q) } AS c")).containsExactly(0L);
  }

  /**
   * Two hops from a seeded anchor, walked forwards and backwards. Only the last hop of a seeded walk is counted
   * rather than expanded, so a two-hop chain is the shortest one that exercises the frontier in between.
   * <p>
   * The saving asserted here is only "no worse than the pipeline it replaced". Without a CSR provider the walk
   * expands its intermediate frontier by reading records, exactly as the pipeline would; what it saves there is the
   * materialization, not the reads. The read gap of the one-hop cases above reappears at two hops on the CSR arrays,
   * where the frontier costs no record read at all - which is what the {@code GraphAnalyticalView} section covers.
   */
  @Test
  void aTwoHopSeededChainIsCountedFromBothEnds() {
    final String forwards = "MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Leaf)-[:TAGS]->(:Sink) } AS c";
    final String backwards = "MATCH (s:Sink) RETURN COUNT { (:Hub)-[:LINKS]->(:Leaf)-[:TAGS]->(s) } AS c";
    final String ordinary = "MATCH (h:Hub) RETURN COUNT { MATCH (h)-[:LINKS]->(x:Leaf)-[:TAGS]->(:Sink) "
        + "WHERE x.k > 0 } AS c";

    assertThat(countsOf(forwards)).containsExactly(3L);
    assertThat(countsOf(backwards)).containsExactly(3L);
    assertThat(countsOf(ordinary)).containsExactly(3L);

    // A middle hop that matches nothing still collapses the whole chain.
    assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Q)-[:TAGS]->(:Sink) } AS c")).containsExactly(0L);

    final long ordinaryReads = recordsReadBy(ordinary);
    assertThat(ordinaryReads).as("the control has to actually materialize the rows").isGreaterThanOrEqualTo(FANOUT);
    assertThat(recordsReadBy(forwards)).as(forwards).isLessThanOrEqualTo(ordinaryReads);
  }

  /** A label on the far end still filters, and an absent one still matches nothing. */
  @Test
  void aLabelOnTheFarEndOfASeededChainStillFilters() {
    assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Leaf) } AS c")).containsExactly((long) FANOUT);
    assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Q) } AS c")).containsExactly(0L);
    assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:NoSuchLabel) } AS c")).containsExactly(0L);
    assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:NOSUCHTYPE]->(:Leaf) } AS c")).containsExactly(0L);
  }

  /** An undirected hop out of a seeded anchor counts both ways, as the ordinary pipeline does. */
  @Test
  void anUndirectedSeededHopCountsBothDirections() {
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]-(:Q) } AS c ORDER BY c"))
        .containsExactly(1L, 1L, 2L);
  }

  // ===================================================================================================
  // 3. what stays on the ordinary pipeline, because the anchor set is not one the operator can seed
  // ===================================================================================================

  /**
   * The seeded name has to be at one end of the chain and at one position only. A body binding it at both ends is a
   * self-loop constraint the propagation does not enforce, and one binding it in the middle has two anchor sets.
   */
  @Test
  void aSeededNameThatIsNotASingleEndOfTheChainIsNotPushedDown() {
    // Both ends: only the self-loop paths match, and there are none.
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]->(q) } AS c ORDER BY c"))
        .containsExactly(0L, 0L, 0L);
    // The middle of a two-hop chain.
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (:Q)-[:LINKS]->(q)-[:LINKS]->(:Q) } AS c ORDER BY c"))
        .containsExactly(0L, 0L, 1L);
  }

  /** Two seeded names read by one body leave two anchors to start from, so the body keeps the ordinary pipeline. */
  @Test
  void aBodyReadingTwoSeededNamesIsNotPushedDown() {
    assertThat(countsOf("MATCH (a:Q {k: 1}), (b:Q {k: 2}) RETURN COUNT { (a)-[:LINKS]->(b) } AS c"))
        .containsExactly(1L);
    assertThat(countsOf("MATCH (a:Q {k: 1}), (b:Q {k: 3}) RETURN COUNT { (a)-[:LINKS]->(b) } AS c"))
        .containsExactly(0L);
  }

  /** A seeded name read only outside the pattern names no anchor position, so nothing is seeded from it. */
  @Test
  void aSeededNameReadOnlyOutsideThePatternIsNotPushedDown() {
    assertThat(collectOfLongs("MATCH (q:Q {k: 1}) RETURN COLLECT { MATCH (x:Q)-[:LINKS]->(y:Q) WHERE x.k = q.k "
        + "RETURN count(*) } AS c")).containsExactly(1L);
  }

  /**
   * A seeded push-down reads the value the <b>outer</b> row bound, so a body able to bind names for itself would
   * break it. None can reach it: the detectors are only asked about a body made of nothing but {@code MATCH} and
   * {@code RETURN}, which leaves out the two clauses that bind - {@code UNWIND} and {@code WITH}.
   * <p>
   * The {@code UNWIND} below is what makes this an assertion rather than a restatement: it multiplies the body's rows
   * by two, so a plan that answered from the chain alone would say 1 where the pipeline says 2.
   */
  @Test
  void aBodyThatCouldBindNamesOfItsOwnNeverReachesTheSeededPushDown() {
    assertThat(countsOf("MATCH (q:Q {k: 1}) RETURN COUNT { UNWIND [1, 2] AS z MATCH (q)-[:LINKS]->(x:Q) RETURN x } AS c"))
        .containsExactly(2L);
    assertThat(countsOf("MATCH (q:Q {k: 1}) RETURN COUNT { MATCH (q)-[:LINKS]->(x:Q) WITH x AS y RETURN y } AS c"))
        .containsExactly(1L);
  }

  /** A bound value that is not a vertex matches no node pattern, whichever plan answers it. */
  @Test
  void aSeededNameBoundToANonVertexMatchesNothing() {
    assertThat(countsOf("UNWIND [1, 2] AS p RETURN COUNT { (p)-[:LINKS]->(:Q) } AS c"))
        .containsExactly(0L, 0L);
  }

  /**
   * The guard #5674 put there in the first place, restated for the shape that still needs it: an uncorrelated body
   * under a correlated query is answered by the global count, a correlated one never is.
   */
  @Test
  void anUncorrelatedBodyIsStillAnsweredByTheGlobalCount() {
    assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (:Q)-[:LINKS]->(:Q) } AS c"))
        .containsExactly(2L, 2L, 2L);
    assertThat(collectOfLongs("MATCH (q:Q) RETURN COLLECT { MATCH (a:Q)-[:LINKS]->(b:Q) RETURN count(*) } AS c"))
        .containsExactly(2L, 2L, 2L);
  }

  /** The type-count push-down is not seedable, and a body correlated through it still answers per anchor. */
  @Test
  void aSeededSingleNodeBodyIsNotAnsweredByTheTypeCounter() {
    assertThat(collectOfLongs("MATCH (q:Q {k: 1}) RETURN COLLECT { MATCH (q:Q) RETURN count(q) } AS c"))
        .containsExactly(1L);
    assertThat(countsOf("MATCH (q:Q {k: 1}) RETURN COUNT { MATCH (q:Q) } AS c")).containsExactly(1L);
    assertThat(countsOf("MATCH (n:Other) RETURN COUNT { MATCH (n:Q) } AS c")).containsExactly(0L);
  }

  // ===================================================================================================
  // 4. the same answers off the CSR arrays, which is the branch the fast path is for
  // ===================================================================================================

  /**
   * {@code CSRCountStep} picks {@code execute} over {@code executeOLTP} on whether a provider covers the edge types,
   * and a plain unit-test database has none - so every case above runs the OLTP walk and the seeded CSR walk would go
   * untested. A {@link GraphAnalyticalView} over the fixture supplies one, and the answers have to be the same ones.
   */
  @Test
  void theSeededChainAnswersTheSameOffTheCsrArrays() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("issue5758-view")
        .withVertexTypes("Hub", "Leaf", "Sink", "Src", "Q", "Other")
        .withEdgeTypes("LINKS", "PTS", "TAGS")
        .build();
    try {
      // The condition CSRCountStep itself branches on: without this the assertions below would re-test the OLTP walk.
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "LINKS"))
          .as("the view has to be the provider the count step finds").isNotNull();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, "PTS")).isNotNull();

      assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Leaf) } AS c")).containsExactly((long) FANOUT);
      // No label on the far end: the hop is counted by degree rather than filtered neighbour by neighbour.
      assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->() } AS c")).containsExactly((long) FANOUT);
      assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Q) } AS c")).containsExactly(0L);
      assertThat(countsOf("MATCH (s:Sink) RETURN COUNT { (:Src)-[:PTS]->(s) } AS c")).containsExactly((long) FANOUT);

      assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]->(:Q) } AS c ORDER BY c"))
          .containsExactly(0L, 1L, 1L);
      assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]->(:Q)-[:LINKS]->(:Q) } AS c ORDER BY c"))
          .containsExactly(0L, 0L, 1L);
      assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (:Q)-[:LINKS]->(q) } AS c ORDER BY c"))
          .containsExactly(0L, 1L, 1L);
      assertThat(countsOf("MATCH (q:Q) RETURN COUNT { (q)-[:LINKS]-(:Q) } AS c ORDER BY c"))
          .containsExactly(1L, 1L, 2L);

      assertThat(countsOf("MATCH (n:Other) RETURN COUNT { (n:Q)-[:LINKS]->(:Q) } AS c")).containsExactly(0L);
      assertThat(collectOfLongs("MATCH (h:Hub) RETURN COLLECT { MATCH (h)-[:LINKS]->(x:Leaf) RETURN count(*) } AS c"))
          .containsExactly((long) FANOUT);

      // Two hops, which is where the seeded walk expands a frontier rather than counting a degree.
      assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Leaf)-[:TAGS]->(:Sink) } AS c"))
          .containsExactly(3L);
      assertThat(countsOf("MATCH (s:Sink) RETURN COUNT { (:Hub)-[:LINKS]->(:Leaf)-[:TAGS]->(s) } AS c"))
          .containsExactly(3L);
      assertThat(countsOf("MATCH (h:Hub) RETURN COUNT { (h)-[:LINKS]->(:Q)-[:TAGS]->(:Sink) } AS c"))
          .containsExactly(0L);
    } finally {
      view.drop();
    }
  }

  // ===================================================================================================
  // helpers
  // ===================================================================================================

  /** The {@code c} column of every row, as longs. */
  private List<Long> countsOf(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    return values;
  }

  /** Every element of every {@code c} list, as longs. */
  private List<Long> collectOfLongs(final String query) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        for (final Object value : (List<Object>) row.getProperty("c"))
          values.add(((Number) value).longValue());
      }
    }
    return values;
  }

  /** The records the query materialized: the anchor's adjacency alone when a seeded push-down answered it. */
  private long recordsReadBy(final String query) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next().getPropertyNames();
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }
}
