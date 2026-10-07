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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Issue #9453: a global aggregate over a column yielded by an {@code algo.*} procedure answered 0 / null while a Graph
 * Analytical View served the procedure, because the CALL step's count-only fast path replaced the procedure rows with
 * empty ones. The fast path also read the procedure's row-count hint from a query-wide variable, so a chained CALL
 * counted only the rows of the LAST input row, and a later CALL could count the rows of an EARLIER procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9453AlgoAggregatesUnderViewTest extends TestHelper {

  private static final String PAGERANK = "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 10, tolerance: 0.0, direction: 'BOTH'}) "
      + "YIELD node, score ";

  @Override
  protected void beginTest() {
    // The reporter's graph: 1->2->3 plus two isolated nodes, so three components
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE EDGE TYPE EDGE");
    // the weight algo.dijkstra.singleSource reads, declared so a view can materialize it
    database.command("sql", "CREATE PROPERTY EDGE.w DOUBLE");
    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql", "CREATE VERTEX Node SET id = " + i);
      database.command("sql", "CREATE EDGE EDGE FROM (SELECT FROM Node WHERE id = 1) TO (SELECT FROM Node WHERE id = 2) SET w = 1");
      database.command("sql", "CREATE EDGE EDGE FROM (SELECT FROM Node WHERE id = 2) TO (SELECT FROM Node WHERE id = 3) SET w = 2");
    });
  }

  private GraphAnalyticalView createView() throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW benchmark VERTEX TYPES (Node) EDGE TYPES (EDGE)");
    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW benchmark");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "benchmark");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private void dropView() {
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW benchmark");
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as(query).isFalse();
      return row;
    }
  }

  private long count(final String query) {
    return single(query).<Number>getProperty("n").longValue();
  }

  @Test
  void wccGlobalAggregatesOverAYieldedColumnUnderAView() throws Exception {
    final String q = "CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n, count(DISTINCT componentId) AS d, max(componentId) AS m";
    final String withQ = "CALL algo.wcc() YIELD node, componentId WITH node, componentId "
        + "RETURN count(*) AS n, count(DISTINCT componentId) AS d, max(componentId) AS m";

    final Result before = single(q);
    assertThat(before.<Number>getProperty("n").longValue()).isEqualTo(5);
    assertThat(before.<Number>getProperty("d").longValue()).isEqualTo(3);
    final long maxBefore = before.<Number>getProperty("m").longValue();

    createView();
    try {
      for (final String query : new String[] { q, withQ }) {
        final Result row = single(query);
        assertThat(row.<Number>getProperty("n").longValue()).as(query).isEqualTo(5);
        assertThat(row.<Number>getProperty("d").longValue()).as(query).isEqualTo(3);
        assertThat(row.<Number>getProperty("m")).as(query).isNotNull();
        assertThat(row.<Number>getProperty("m").longValue()).as(query).isEqualTo(maxBefore);
      }
      // count(*) alone keeps the fast path and must still be exact
      assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n")).isEqualTo(5);
      assertThat(count("CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n, count(*) AS n2")).isEqualTo(5);
    } finally {
      dropView();
    }
  }

  @Test
  void pagerankGlobalAggregatesOverAYieldedColumnUnderAView() throws Exception {
    final Result before = single(PAGERANK + "RETURN count(*) AS n, sum(score) AS s, max(score) AS m");
    final double sumBefore = before.<Number>getProperty("s").doubleValue();
    final double maxBefore = before.<Number>getProperty("m").doubleValue();
    assertThat(sumBefore).isCloseTo(1.0, offset(1e-9));

    createView();
    try {
      for (final String query : new String[] { PAGERANK + "RETURN count(*) AS n, sum(score) AS s, max(score) AS m",
          PAGERANK + "WITH node, score RETURN count(*) AS n, sum(score) AS s, max(score) AS m" }) {
        final Result row = single(query);
        assertThat(row.<Number>getProperty("n").longValue()).as(query).isEqualTo(5);
        assertThat(row.<Number>getProperty("s").doubleValue()).as(query).isCloseTo(sumBefore, offset(1e-9));
        assertThat(row.<Number>getProperty("m")).as(query).isNotNull();
        assertThat(row.<Number>getProperty("m").doubleValue()).as(query).isCloseTo(maxBefore, offset(1e-9));
      }
      assertThat(count(PAGERANK + "RETURN count(score) AS n")).isEqualTo(5);
      assertThat(single(PAGERANK + "RETURN avg(score) AS a").<Number>getProperty("a").doubleValue()).isCloseTo(sumBefore / 5, offset(1e-9));
    } finally {
      dropView();
    }
  }

  @Test
  void chainedCountOnlyCallSumsTheRowsOfEveryInputRow() throws Exception {
    final String[] queries = {
        // the procedure runs once per input row: every run contributes its own rows
        "UNWIND [1, 2, 3] AS x CALL algo.wcc() YIELD node RETURN count(*) AS n",
        "MATCH (s:Node) CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
        // a CALL that follows an algo.* CALL must not count the earlier procedure's rows
        "CALL algo.wcc() YIELD node WITH count(*) AS c CALL db.labels() YIELD label RETURN count(*) AS n",
        "MATCH (s:Node) WITH s LIMIT 1 CALL algo.wcc() YIELD node WITH count(*) AS c MATCH (s:Node {id: 4}) "
            + "CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
        // YIELD WHERE drops rows the procedure's size still counts
        "CALL algo.wcc() YIELD node, componentId WHERE node.id > 2 RETURN count(*) AS n",
        "UNWIND [1, 2] AS x CALL algo.wcc() YIELD node, componentId WHERE node.id > 2 RETURN count(*) AS n",
        // an empty OPTIONAL CALL still yields its one null row
        "MATCH (s:Node {id: 4}) OPTIONAL CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
        "MATCH (s:Node) WHERE s.id >= 3 OPTIONAL CALL algo.bfs(s) YIELD node RETURN count(*) AS n",
        // a procedure that is not an algorithm takes the same count-only path
        "CALL db.labels() YIELD label RETURN count(*) AS n",
        "UNWIND [1, 2, 3] AS x CALL db.labels() YIELD label RETURN count(*) AS n" };

    final long[] expected = new long[queries.length];
    for (int i = 0; i < queries.length; i++)
      expected[i] = count(queries[i]);
    assertThat(expected[0]).isEqualTo(15);
    // reachable over BOTH directions: 2 for each node of {1,2,3}, 0 for the isolated ones
    assertThat(expected[1]).isEqualTo(6);
    assertThat(expected[3]).isEqualTo(0);
    assertThat(expected[4]).isEqualTo(3);
    assertThat(expected[5]).isEqualTo(6);
    assertThat(expected[6]).isEqualTo(1);
    // node 3 reaches 2 nodes, nodes 4 and 5 none: one null row each
    assertThat(expected[7]).isEqualTo(4);
    assertThat(expected[8]).isEqualTo(1);
    assertThat(expected[9]).isEqualTo(3);

    createView();
    try {
      for (int i = 0; i < queries.length; i++)
        assertThat(count(queries[i])).as(queries[i]).isEqualTo(expected[i]);
    } finally {
      dropView();
    }
  }

  /**
   * The count-only fast path relies on each procedure's stream knowing its exact size. Nothing functional fails when a
   * stream loses it (a {@code filter()} added to one of them, say) - the count just goes back to building a row per
   * node - so this pins it, on both the OLTP and the view-backed path.
   */
  @Test
  void algoStreamsKnowTheirExactSize() throws Exception {
    assertAlgoStreamsAreSized(false);

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW sized VERTEX TYPES (Node) EDGE TYPES (EDGE) EDGE PROPERTIES (w)");
    assertThat(GraphAnalyticalViewRegistry.get(database, "sized").awaitReady(60, TimeUnit.SECONDS)).isTrue();
    try {
      assertAlgoStreamsAreSized(true);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW sized");
    }
  }

  private void assertAlgoStreamsAreSized(final boolean viaView) {
    final Vertex start;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Node WHERE id = 1")) {
      start = rs.next().getVertex().orElseThrow();
    }
    final Object[][] calls = { { "algo.wcc", new Object[0] }, { "algo.pagerank", new Object[0] },
        { "algo.labelpropagation", new Object[0] }, { "algo.localClusteringCoefficient", new Object[0] },
        { "algo.bfs", new Object[] { start } }, { "algo.dijkstra.singleSource", new Object[] { start, "EDGE", "w" } } };

    for (final Object[] call : calls) {
      final String name = (String) call[0];
      final CypherProcedure procedure = CypherProcedureRegistry.get(name);
      final long size;
      final BasicCommandContext context = newContext();
      try (final Stream<Result> rows = procedure.execute((Object[]) call[1], null, context)) {
        size = rows.spliterator().getExactSizeIfKnown();
      }
      assertThat(Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))).as(name).isEqualTo(viaView);
      long traversed = 0;
      try (final Stream<Result> rows = procedure.execute((Object[]) call[1], null, newContext())) {
        for (final Iterator<Result> it = rows.iterator(); it.hasNext(); it.next())
          ++traversed;
      }
      assertThat(traversed).as(name).isGreaterThan(0);
      assertThat(size).as(name).isEqualTo(traversed);
    }
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  /**
   * Found while fixing #9453: an OPTIONAL CALL whose procedure yielded no row for an input row dropped that row instead
   * of answering it with nulls, as OPTIONAL MATCH does and as Neo4j does for OPTIONAL CALL.
   */
  @Test
  void optionalCallWithNoRowsAnswersANullRow() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node) WHERE s.id >= 3 OPTIONAL CALL algo.bfs(s) YIELD node, depth RETURN s.id AS id, node.id AS reached, depth "
            + "ORDER BY id, reached")) {
      final StringBuilder rows = new StringBuilder();
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.append(row.<Object>getProperty("id")).append(':').append(row.<Object>getProperty("reached")).append(':')
            .append(row.<Object>getProperty("depth")).append(' ');
      }
      assertThat(rows.toString().trim()).isEqualTo("3:1:2 3:2:1 4:null:null 5:null:null");
    }

    // every row filtered out by YIELD WHERE is no row either
    final Result row = single("MATCH (s:Node {id: 1}) OPTIONAL CALL algo.bfs(s) YIELD node WHERE node.id > 100 RETURN s.id AS id, node");
    assertThat(row.<Integer>getProperty("id")).isEqualTo(1);
    assertThat(row.<Object>getProperty("node")).isNull();

    // a plain CALL still drops the input row
    try (final ResultSet rs = database.query("opencypher", "MATCH (s:Node {id: 4}) CALL algo.bfs(s) YIELD node RETURN s.id AS id, node")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
