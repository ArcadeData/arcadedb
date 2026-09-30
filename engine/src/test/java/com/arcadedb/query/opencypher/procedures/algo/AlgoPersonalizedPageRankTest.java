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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for the algo.personalizedPageRank Cypher procedure.
 */
class AlgoPersonalizedPageRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-ppr");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    // Linear chain: A -> B -> C -> D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      a.newEdge("FOLLOWS", b, true, (Object[]) null).save();
      b.newEdge("FOLLOWS", c, true, (Object[]) null).save();
      c.newEdge("FOLLOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void pprReturnsOneRowPerVertex() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}) \
        CALL algo.personalizedPageRank(a) YIELD nodeId, score \
        RETURN nodeId, score""");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void pprScoresAreNonNegative() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}) \
        CALL algo.personalizedPageRank(a) YIELD nodeId, score \
        RETURN nodeId, score""");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("score");
      assertThat(((Number) val).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }

  @Test
  void pprSourceHasHighestScore() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}) \
        CALL algo.personalizedPageRank(a) YIELD nodeId, score \
        RETURN nodeId, score ORDER BY score DESC""");

    assertThat(rs.hasNext()).isTrue();
    final Result topResult = rs.next();
    final Object val = topResult.getProperty("score");
    // Source node should have the highest (or near-highest) score
    assertThat(((Number) val).doubleValue()).isGreaterThan(0.0);
  }

  @Test
  void pprWithCustomDampingFactor() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}) \
        CALL algo.personalizedPageRank(a, 'FOLLOWS', 0.9) YIELD nodeId, score \
        RETURN nodeId, score""");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void pprScoresSumToApproximatelyOne() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name:'A'}) \
        CALL algo.personalizedPageRank(a) YIELD nodeId, score \
        RETURN nodeId, score""");

    double totalScore = 0.0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("score");
      totalScore += ((Number) val).doubleValue();
    }
    // PPR scores should sum to approximately 1.0
    assertThat(totalScore).isGreaterThan(0.0);
    assertThat(totalScore).isLessThanOrEqualTo(1.5);
  }

  private Map<String, Double> scores(final String query) {
    final Map<String, Double> scores = new HashMap<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext()) {
      final Result r = rs.next();
      final RID rid = r.getProperty("nodeId");
      scores.put(rid.asVertex().getString("name"), ((Number) r.getProperty("score")).doubleValue());
    }
    return scores;
  }

  private static final String YIELD_SUFFIX = " YIELD nodeId, score RETURN nodeId, score";

  @Test
  void pprSingleElementListEqualsSingleNode() {
    final Map<String, Double> single = scores("MATCH (a:Person {name:'A'}) CALL algo.personalizedPageRank(a)" + YIELD_SUFFIX);
    final Map<String, Double> list = scores("MATCH (a:Person {name:'A'}) CALL algo.personalizedPageRank([a])" + YIELD_SUFFIX);
    assertThat(list).hasSize(4);
    for (final String k : single.keySet())
      assertThat(list.get(k)).isCloseTo(single.get(k), within(1e-12));
  }

  /**
   * Dangling rank flows back to the personalization vector, which makes PPR non-linear in it. Turning the chain into a
   * cycle removes the only dangling node, so linearity can be asserted.
   */
  private void closeCycle() {
    database.transaction(() -> database.command("sql", "CREATE EDGE FOLLOWS FROM (SELECT FROM Person WHERE name = 'D') TO (SELECT FROM Person WHERE name = 'A')"));
  }

  @Test
  void pprMultipleSourcesAreUniform() {
    closeCycle();
    final Map<String, Double> s = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([a, c], 'FOLLOWS', 0.85, 100, 0.0000000001)" + YIELD_SUFFIX);
    final Map<String, Double> sa = scores(
        "MATCH (a:Person {name:'A'}) CALL algo.personalizedPageRank(a, 'FOLLOWS', 0.85, 100, 0.0000000001)" + YIELD_SUFFIX);
    final Map<String, Double> sc = scores(
        "MATCH (c:Person {name:'C'}) CALL algo.personalizedPageRank(c, 'FOLLOWS', 0.85, 100, 0.0000000001)" + YIELD_SUFFIX);
    // PPR is linear in the personalization vector when there are no dangling nodes (see closeCycle())
    for (final String k : s.keySet())
      assertThat(s.get(k)).isCloseTo(0.5 * sa.get(k) + 0.5 * sc.get(k), within(1e-9));
    assertThat(s.values().stream().mapToDouble(Double::doubleValue).sum()).isCloseTo(1.0, within(1e-9));
  }

  @Test
  void pprWeightedSourcesAreNormalized() {
    closeCycle();
    final String opts = "'FOLLOWS', 0.85, 100, 0.0000000001";
    final Map<String, Double> w = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([[a, 3.0], [c, 1.0]], " + opts + ")" + YIELD_SUFFIX);
    final Map<String, Double> sa = scores("MATCH (a:Person {name:'A'}) CALL algo.personalizedPageRank(a, " + opts + ")" + YIELD_SUFFIX);
    final Map<String, Double> sc = scores("MATCH (c:Person {name:'C'}) CALL algo.personalizedPageRank(c, " + opts + ")" + YIELD_SUFFIX);
    for (final String k : w.keySet())
      assertThat(w.get(k)).isCloseTo(0.75 * sa.get(k) + 0.25 * sc.get(k), within(1e-9));
    // scaling every weight does not change the result
    final Map<String, Double> w2 = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([[a, 30], [c, 10]], " + opts + ")" + YIELD_SUFFIX);
    for (final String k : w.keySet())
      assertThat(w2.get(k)).isCloseTo(w.get(k), within(1e-12));
  }

  @Test
  void pprDuplicateSourcesAccumulateWeight() {
    final Map<String, Double> dup = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([a, a, c])" + YIELD_SUFFIX);
    final Map<String, Double> weighted = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([[a, 2], [c, 1]])" + YIELD_SUFFIX);
    for (final String k : dup.keySet())
      assertThat(dup.get(k)).isCloseTo(weighted.get(k), within(1e-12));
  }

  @Test
  void pprRejectsInvalidSources() {
    final String[][] cases = { { "[]", "empty" }, { "[[a, -1]]", "non-negative" }, { "[[a, 0]]", "positive" }, { "[[a, 'x']]", "must be a number" },
        { "[[a, 1, 2]]", "[node, weight] pair" }, { "[1]", "must be a node" }, { "[null]", "cannot be null" }, { "[a, 3.0]", "[[node, weight]" } };
    for (final String[] c : cases)
      assertThatThrownBy(() -> database.query("opencypher",
          "MATCH (a:Person {name:'A'}) CALL algo.personalizedPageRank(" + c[0] + ") YIELD nodeId, score RETURN nodeId, score").stream().count())
          .as(c[0]).hasStackTraceContaining(c[1]);
  }

  @Test
  void pprMixesPlainNodesAndPairs() {
    final Map<String, Double> mixed = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([a, [c, 2]])" + YIELD_SUFFIX);
    final Map<String, Double> pairs = scores(
        "MATCH (a:Person {name:'A'}), (c:Person {name:'C'}) CALL algo.personalizedPageRank([[a, 1], [c, 2]])" + YIELD_SUFFIX);
    assertThat(mixed).hasSize(4);
    for (final String k : mixed.keySet())
      assertThat(mixed.get(k)).isCloseTo(pairs.get(k), within(1e-12));
  }

  @Test
  void pprFallsBackToOltpWhenSourceAbsentFromCsrView() {
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database).withName("ppr-unknown-csr").withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS").build();
    try {
      assertThat(gav.awaitReady(10, TimeUnit.SECONDS)).isTrue();
      final Object a = database.query("sql", "SELECT FROM Person WHERE name = 'A'").next().getElement().get();
      final Object[] noExtra = { List.of(a), "FOLLOWS", 0.85, 50, 0.0 };
      final BasicCommandContext csrContext = new BasicCommandContext();
      csrContext.setDatabase(database);
      final Map<RID, Double> csr = run(csrContext, noExtra);
      assertThat(csrContext.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);

      // A vertex created after the view was built is unknown to the CSR. Whichever path answers, a zero-weight unknown
      // source must not change the scores of the others
      final MutableVertex[] e = new MutableVertex[1];
      database.transaction(() -> e[0] = database.newVertex("Person").set("name", "E").save());
      final Map<RID, Double> withZero = run(newContext(), new Object[] { List.of(a, List.of(e[0], 0)), "FOLLOWS", 0.85, 50, 0.0 });
      for (final Map.Entry<RID, Double> entry : csr.entrySet())
        assertThat(withZero.get(entry.getKey())).isCloseTo(entry.getValue(), within(1e-9));
    } finally {
      gav.shutdown();
    }
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private Map<RID, Double> run(final CommandContext context, final Object[] args) {
    final Map<RID, Double> scores = new HashMap<>();
    new AlgoPersonalizedPageRank().execute(args, null, context).forEach(r -> scores.put(r.getProperty("nodeId"), ((Number) r.getProperty("score")).doubleValue()));
    return scores;
  }

  @Test
  void pprMultipleSourcesOltpPathMatchesCsr() {
    final Object a = database.query("sql", "SELECT FROM Person WHERE name = 'A'").next().getElement().get();
    final Object c = database.query("sql", "SELECT FROM Person WHERE name = 'C'").next().getElement().get();
    final Object[] call = { List.of(List.of(a, 2), List.of(c, 1)), "FOLLOWS", 0.85, 50, 0.0 };

    final BasicCommandContext oltpContext = newContext();
    final Map<RID, Double> oltp = run(oltpContext, call);
    assertThat(oltp).hasSize(4);
    assertThat(oltpContext.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isNull();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database).withName("ppr-multi-csr").withVertexTypes("Person")
        .withEdgeTypes("FOLLOWS").build();
    try {
      assertThat(gav.awaitReady(10, TimeUnit.SECONDS)).isTrue();
      final BasicCommandContext csrContext = newContext();
      final Map<RID, Double> csr = run(csrContext, call);
      assertThat(csrContext.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);
      assertThat(csr).hasSize(4);
      for (final Map.Entry<RID, Double> entry : oltp.entrySet())
        assertThat(csr.get(entry.getKey())).isCloseTo(entry.getValue(), within(1e-9));
    } finally {
      gav.shutdown();
    }
  }
}
