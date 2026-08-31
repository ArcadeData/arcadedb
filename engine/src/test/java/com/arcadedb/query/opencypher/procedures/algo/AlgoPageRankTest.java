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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the algo.pagerank Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoPageRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-pagerank");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");

    // Create a small web graph:
    // A -> B, A -> C, B -> C, C -> A
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Page").set("name", "A").save();
      final MutableVertex b = database.newVertex("Page").set("name", "B").save();
      final MutableVertex c = database.newVertex("Page").set("name", "C").save();
      a.newEdge("LINKS", b, true, (Object[]) null).save();
      a.newEdge("LINKS", c, true, (Object[]) null).save();
      b.newEdge("LINKS", c, true, (Object[]) null).save();
      c.newEdge("LINKS", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void pageRankReturnsScoreForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final double score = ((Number) result.getProperty("score")).doubleValue();
      assertThat(score).isGreaterThan(0.0);
    }
  }

  @Test
  void pageRankScoresSumToApproximatelyOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN score");

    double sum = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      sum += ((Number) result.getProperty("score")).doubleValue();
    }
    // PageRank scores should sum to approximately 1.0
    assertThat(sum).isBetween(0.9, 1.1);
  }

  @Test
  void pageRankWithCustomDampingFactor() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.5}) YIELD node, score RETURN node, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void pageRankHigherScoreForHigherInDegree() {
    // Node C has 2 incoming edges (from A and B), so it should have higher rank
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
    // C has most incoming links (from A and B), should have higher score
    final String topNode = (String) results.getFirst().getProperty("name");
    assertThat(topNode).isEqualTo("C");
  }

  @Test
  void pageRankCSRAndOLTPProduceIdenticalResults() {
    // Step 1: Run PageRank without GAV → OLTP path
    final Map<String, Double> oltpScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0001}) \
        YIELD node, score RETURN node.name AS name, score""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        oltpScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(oltpScores).hasSize(3);

    // Step 2: Build a GAV so the CSR path is used
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-csr")
        .withVertexTypes("Page")
        .withEdgeTypes("LINKS")
        .build();
    gav.awaitReady(10, TimeUnit.SECONDS);

    // Step 3: Run PageRank with GAV → CSR path
    final Map<String, Double> csrScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0001}) \
        YIELD node, score RETURN node.name AS name, score""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        csrScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(csrScores).hasSize(3);

    // Step 4: Compare — CSR and OLTP paths accumulate floating-point differences
    // due to different iteration order and convergence behavior, so use 1e-4 tolerance
    for (final Map.Entry<String, Double> entry : oltpScores.entrySet()) {
      final String name = entry.getKey();
      assertThat(csrScores).containsKey(name);
      assertThat(csrScores.get(name)).isCloseTo(entry.getValue(), Offset.offset(1e-4));
    }

    gav.shutdown();
  }

  @Test
  void weightedPageRankHonoursInDirection() {
    // Weighted PageRank never takes the CSR path, so the OLTP fallback's weight arrays are only exercised here.
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE LINKS SET weight = 1")) {
        rs.close();
      }
      try (final ResultSet rs = database.command("sql",
          "UPDATE LINKS SET weight = 9 WHERE out.name = 'A' AND in.name = 'C'")) {
        rs.close();
      }
    });

    // A -> B, A -> C (weight 9), B -> C, C -> A. Following the stored direction concentrates rank on C.
    assertThat(topRankedName("OUT", "weight")).isEqualTo("C");
    // Reversing it concentrates rank on A instead: A is where both B's and 90% of C's rank now flows.
    assertThat(topRankedName("IN", "weight")).isEqualTo("A");
  }

  private String topRankedName(final String direction, final String weightProperty) {
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 40, tolerance: 0.0, direction: '" + direction
            + "', weightProperty: '" + weightProperty + "'}) YIELD node, score RETURN node.name AS name, score "
            + "ORDER BY score DESC")) {
      return (String) rs.next().getProperty("name");
    }
  }

  @Test
  void pageRankInDirectionCSRAndOLTPProduceIdenticalResults() {
    // Invoked through the procedure rather than the query engine so the call's CommandContext is observable:
    // awaitReady() returning true would not be enough on its own, because AlgoPageRank also drops to OLTP when
    // the view reports pending changes and when findProvider() does not reach it at all - and a parity test
    // served by OLTP twice pins the OLTP path against itself and can never fail. CSR_ACCELERATED_VAR is the
    // only signal that says which path actually ran. The 'IN' config string itself is covered end-to-end by
    // weightedPageRankHonoursInDirection.
    final Map<RID, Double> oltpScores = pageRankScores(newContext(), Vertex.DIRECTION.IN);
    assertThat(oltpScores).hasSize(3);

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-in-csr")
        .withVertexTypes("Page")
        .withEdgeTypes("LINKS")
        .build();
    try {
      assertThat(gav.awaitReady(10, TimeUnit.SECONDS)).isTrue();

      final BasicCommandContext csrContext = newContext();
      final Map<RID, Double> csrScores = pageRankScores(csrContext, Vertex.DIRECTION.IN);
      assertThat(csrContext.getVariable(CommandContext.CSR_ACCELERATED_VAR))
          .as("the view must actually back the call, or this comparison pins the OLTP path against itself")
          .isEqualTo(true);

      assertThat(csrScores.keySet()).isEqualTo(oltpScores.keySet());
      for (final Map.Entry<RID, Double> entry : oltpScores.entrySet())
        assertThat(csrScores.get(entry.getKey())).as("score for " + entry.getKey())
            .isCloseTo(entry.getValue(), Offset.offset(1e-4));

      // A -> B, A -> C, B -> C, C -> A. Reversed, A is where both B's and C's rank flows, so A must top both.
      final Map<String, Double> byName = new HashMap<>();
      csrScores.forEach((rid, score) -> byName.put(rid.asVertex().getString("name"), score));
      assertThat(byName.get("A")).isGreaterThan(byName.get("B"));
      assertThat(byName.get("A")).isGreaterThan(byName.get("C"));
    } finally {
      gav.shutdown();
    }
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private Map<RID, Double> pageRankScores(final CommandContext context, final Vertex.DIRECTION direction) {
    final Map<String, Object> config = new HashMap<>();
    config.put("dampingFactor", 0.85);
    config.put("maxIterations", 20);
    config.put("tolerance", 0.0);
    config.put("direction", direction.name());

    final Map<RID, Double> scores = new HashMap<>();
    for (final Iterator<Result> it = new AlgoPageRank().execute(new Object[] { config }, null, context).iterator();
        it.hasNext(); ) {
      final Result row = it.next();
      scores.put(row.getProperty("node"), ((Number) row.getProperty("score")).doubleValue());
    }
    return scores;
  }

  @Test
  void pageRankBothDirectionCSRAndOLTPProduceIdenticalResults() {
    // Step 1: Run PageRank with direction=BOTH without GAV → OLTP path
    final Map<String, Double> oltpScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0, direction: 'BOTH'}) \
        YIELD node, score RETURN node.name AS name, score""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        oltpScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(oltpScores).hasSize(3);

    // Step 2: Build a GAV so the CSR path is used
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-both-csr")
        .withVertexTypes("Page")
        .withEdgeTypes("LINKS")
        .build();
    // Without this the CSR half can silently fall back to OLTP and the comparison pins OLTP against itself.
    assertThat(gav.awaitReady(10, TimeUnit.SECONDS)).isTrue();

    // Step 3: Run PageRank with direction=BOTH with GAV → CSR path
    final Map<String, Double> csrScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        """
        CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0, direction: 'BOTH'}) \
        YIELD node, score RETURN node.name AS name, score""")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        csrScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(csrScores).hasSize(3);

    // Step 4: Compare — CSR and OLTP paths should match closely
    for (final Map.Entry<String, Double> entry : oltpScores.entrySet()) {
      final String name = entry.getKey();
      assertThat(csrScores).containsKey(name);
      assertThat(csrScores.get(name)).isCloseTo(entry.getValue(), Offset.offset(1e-4));
    }

    // Step 5: For undirected graph, scores should be more evenly distributed than directed
    double sum = 0;
    for (final double s : csrScores.values())
      sum += s;
    assertThat(sum).isBetween(0.9, 1.1);

    gav.shutdown();
  }

  /**
   * Regression for issue #6641 code review: {@code AbstractAlgoProcedure.findProvider()}'s whole-graph
   * fallback loop (used when {@code relTypes} is null, e.g. plain {@code algo.pagerank()}) used to call
   * {@code isReady()} before {@code coversEdgeType()} - the same ordering bug already fixed in
   * {@link com.arcadedb.graph.GraphTraversalProviderRegistry#findProvider}, just at a second call site.
   * Since {@code isReady()} now dispatches a {@link GraphAnalyticalView}'s deferred restore-from-disk as a
   * side effect when one is pending (see #6641), calling it before checking coverage meant a whole-graph
   * algorithm would eagerly resolve every registered view's deferred restore, not only the one it can
   * actually use. Verifies the fix with two persisted-CSR views reopened in the same database: one that
   * does not cover all edge types (registered first, so the loop has to skip past it) and one that does -
   * running {@code algo.pagerank()} must resolve only the latter, leaving the former's deferred restore
   * completely untouched.
   */
  @Test
  void pageRankFindProviderDoesNotTriggerUnrelatedViewsDeferredRestore() throws Exception {
    database.getSchema().createEdgeType("OTHER");
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("Page").set("name", "X").save();
      final MutableVertex y = database.newVertex("Page").set("name", "Y").save();
      x.newEdge("OTHER", y, true, (Object[]) null).save();
    });

    // Registered first, but does not cover every edge type in the schema (LINKS and OTHER) - not a valid
    // whole-graph candidate, so the fixed loop must skip it without ever calling isReady() on it.
    GraphAnalyticalView.builder(database)
        .withName("pagerank-order-other")
        .withVertexTypes("Page")
        .withEdgeTypes("OTHER")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
    // No edge-type filter - covers every edge type, the whole-graph candidate algo.pagerank() should use.
    GraphAnalyticalView.builder(database)
        .withName("pagerank-order-whole")
        .withVertexTypes("Page")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();

    final String dbPath = database.getDatabasePath();
    database.close();
    database = new DatabaseFactory(dbPath).open();

    final GraphAnalyticalView other = GraphAnalyticalViewRegistry.get(database, "pagerank-order-other");
    final GraphAnalyticalView whole = GraphAnalyticalViewRegistry.get(database, "pagerank-order-whole");
    assertThat(other).isNotNull();
    assertThat(whole).isNotNull();
    assertThat(other.getStatus())
        .as("both persisted CSRs must still be provisionally READY, unresolved, right after reopen")
        .isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(whole.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 5, tolerance: 0.0001}) YIELD node, score RETURN count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
    }

    assertThat(other.getStatus())
        .as("issue #6641: a whole-graph algorithm must not eagerly resolve an unrelated view's deferred "
            + "restore just because it was iterated over while searching for a usable provider")
        .isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(other.isBuilt()).isFalse();

    other.drop();
    whole.drop();
  }

  @Test
  void pageRankMaxIterationsAboveIntRangeThrows() {
    // Issue #5924: maxIterations used to narrow via .intValue(), so a Long above Integer.MAX_VALUE
    // wrapped into a small/negative int and silently ran a degenerate (or zero-iteration)
    // computation instead of failing. It must now be rejected loudly.
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher",
          "CALL algo.pagerank({maxIterations: $mi}) YIELD node, score RETURN node",
          Map.of("mi", 2147483648L));
      while (rs.hasNext())
        rs.next();
    }).hasStackTraceContaining("maxIterations");
  }

  @Test
  void pageRankEmptyGraph() {
    final DatabaseFactory emptyFactory = new DatabaseFactory("./target/databases/test-algo-pagerank-empty");
    if (emptyFactory.exists())
      emptyFactory.open().drop();
    final Database emptyDb = emptyFactory.create();
    try {
      emptyDb.getSchema().createVertexType("Node");
      final ResultSet rs = emptyDb.query("opencypher", "CALL algo.pagerank() YIELD node, score RETURN node, score");
      // Empty graph returns no results
      assertThat(rs.hasNext()).isFalse();
    } finally {
      emptyDb.drop();
    }
  }
}
