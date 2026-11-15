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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.executor.steps.PropagateChainOp;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11918: the chain count push-down ({@code COUNT CHAIN PATHS}) declined
 * {@code MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*)} because its two hops have one type, and the statement expanded
 * every path. Two adjacent hops walked the same way bind one relationship only when it is a self loop ({@code a = b = c}),
 * so the chain is now counted and the paths that use one self loop twice are subtracted. Every count is checked against
 * the row pipeline, reached through a {@code sum(1)} that the push-downs decline.
 */
class CypherCountSameTypeChainIssue11918Test extends TestHelper {
  private static final String PUSHED_DOWN = "COUNT CHAIN PATHS";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q EXTENDS P");
    database.command("sql", "CREATE VERTEX TYPE R");
    database.command("sql", "CREATE PROPERTY R.id INTEGER");
    database.command("sql", "CREATE INDEX ON R (id) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE KC EXTENDS K");
    // a second subtype: [:KC]->[:K]->[:KD] ties two pairs in a row whose first and third hops share no edge
    database.command("sql", "CREATE EDGE TYPE KD EXTENDS K");
    database.command("sql", "CREATE EDGE TYPE L");
    // a common subtype of two unrelated types: a KL relationship matches [:K] and [:L] alike
    database.command("sql", "CREATE EDGE TYPE KL EXTENDS K, L");
  }

  /**
   * Two hop types that are not subtypes of one another still overlap when an edge type extends both: a {@code KL}
   * relationship matches {@code [:K]} and {@code [:L]}. Such a pair is neither unique nor corrected, so the chain is declined.
   */
  @Test
  void hopsWhoseTypesShareOnlyACommonSubtypeAreDeclined() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1).save();
      final MutableVertex b = database.newVertex("P").set("id", 2).save();
      final MutableVertex c = database.newVertex("P").set("id", 3).save();
      a.newEdge("KL", b);
      b.newEdge("K", a);
      c.newEdge("KL", c);
    });
    // [:K]->[:K]->[:L]: (a)-[e:KL]->(b)-[:K]->(a)-[e]->(b) binds e twice, so no path is left (the adjacency count is 1).
    // [:K]->[:L]: (b)-[:K]->(a)-[e]->(b) is the one path; c's self loop for both hops is not (the adjacency count is 2)
    final String[] queries = { "MATCH (x)-[:K]->(y)-[:K]->(z)-[:L]->(w) RETURN count(*) AS n",
        "MATCH (x)-[:K]->(y)-[:L]->(z) RETURN count(*) AS n", "MATCH (x)<-[:L]-(y)<-[:K]-(z) RETURN count(*) AS n" };
    final long[] expected = { 0, 1, 1 };
    for (int i = 0; i < queries.length; i++) {
      assertThat(plan(queries[i])).as(queries[i]).doesNotContain(PUSHED_DOWN);
      assertThat(count(queries[i])).as(queries[i]).isEqualTo(enumerated(queries[i])).isEqualTo(expected[i]);
    }
  }

  /** The issue's statement on a graph with self loops: one, two parallel, and one of a subtype. */
  @Test
  void twoHopsOfOneTypeArePushedDownAndSubtractTheSelfLoopsUsedTwice() throws Exception {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1).save();
      final MutableVertex b = database.newVertex("P").set("id", 2).save();
      final MutableVertex c = database.newVertex("Q").set("id", 3).save();
      a.newEdge("K", b);
      b.newEdge("K", c);
      a.newEdge("K", a);
      b.newEdge("K", b);
      b.newEdge("K", b);
      c.newEdge("KC", c);
      c.newEdge("K", a);
    });
    final String query = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";
    assertThat(plan(query)).contains(PUSHED_DOWN);
    final long expected = enumerated(query);
    assertThat(count(query)).isEqualTo(expected);
    // K edges, KC included: out a {b, a}, b {c, b, b}, c {c, a}; in a {a, c}, b {a, b, b}, c {b, c}. The adjacency paths
    // are the sum over the middle vertex of in-degree times out-degree, 2 * 2 + 3 * 3 + 2 * 2 = 17, less the 4 that use one
    // self loop for both hops (a's, b's two, c's) = 13: checked by hand so the oracle is not the only witness
    assertThat(expected).isEqualTo(13L);

    assertThat(count("MATCH (a:P)<-[:K]-(b:P)<-[:K]-(c:P) RETURN count(*) AS n")).isEqualTo(13L);
    // the self loop vertex carries the label of every position the loop crosses: only c is a Q
    final String labelled = "MATCH (a:P)-[:K]->(b:Q)-[:K]->(c:P) RETURN count(*) AS n";
    assertThat(plan(labelled)).contains(PUSHED_DOWN);
    assertThat(count(labelled)).isEqualTo(enumerated(labelled));
    // the subtype hop: only c's KC self loop can be bound by both
    final String subtype = "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) RETURN count(*) AS n";
    assertThat(plan(subtype)).contains(PUSHED_DOWN);
    assertThat(count(subtype)).isEqualTo(enumerated(subtype));

    withView(() -> {
      assertThat(count(query)).isEqualTo(13L);
      assertThat(count(labelled)).isEqualTo(enumerated(labelled));
      assertThat(count(subtype)).isEqualTo(enumerated(subtype));
    });
  }

  /** Shapes whose overlapping hops share an ordinary edge, or are not adjacent, are still declined. */
  @Test
  void hopsThatShareAnOrdinaryEdgeAreStillDeclined() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("id", 1).save();
      final MutableVertex b = database.newVertex("P").set("id", 2).save();
      a.newEdge("K", b);
      a.newEdge("K", a);
      b.newEdge("K", a);
    });
    for (final String query : new String[] {
        "MATCH (a:P)-[:K]->(b:P)<-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)<-[:K]-(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]-(b:P)-[:K]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n",
        // the inequality rescues the K pair, and the L pair would need the self loop subtraction, which the operator does not
        // combine with an inequality
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:L]->(d:P)-[:L]->(e:P) WHERE a <> c RETURN count(*) AS n" }) {
      assertThat(plan(query)).as(query).doesNotContain(PUSHED_DOWN);
      assertThat(count(query)).as(query).isEqualTo(enumerated(query));
    }
    // an inequality that rescues the pair keeps the chain on the inequality subtraction: binding one self loop forces
    // a = b = c, so a <> c and a <> b both rule it out
    for (final String rescued : new String[] { "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) WHERE a <> c RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) WHERE a <> b RETURN count(*) AS n" }) {
      assertThat(plan(rescued)).as(rescued).contains(PUSHED_DOWN);
      assertThat(count(rescued)).as(rescued).isEqualTo(enumerated(rescued));
    }
  }

  /**
   * Random multigraphs with self loops, parallel edges, two vertex labels (one extending the other) and an edge subtype:
   * every random chain count is compared with the row pipeline, without and with a Graph Analytical View, and the walks
   * from one vertex (a correlated {@code COUNT {}} and a UNIQUE key) are compared with the pipeline's per-vertex count.
   */
  @Test
  void randomChainsMatchThePipeline() throws Exception {
    final Random random = new Random(11918);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < 36; i++) {
        final String type = i % 3 == 0 ? "P" : i % 3 == 1 ? "Q" : "R";
        vertices.add(database.newVertex(type).set("id", i).set("w", i % 2).save());
      }
      final String[] edgeTypes = { "K", "KC", "KD", "L", "KL" };
      for (final MutableVertex v : vertices) {
        for (final String type : edgeTypes) {
          // 0 to 2 self loops of each type, so parallel self loops are common
          for (int k = random.nextInt(4) - 1; k > 0; k--)
            v.newEdge(type, v);
          for (int k = random.nextInt(3); k > 0; k--) {
            final MutableVertex target = vertices.get(random.nextInt(vertices.size()));
            v.newEdge(type, target);
            if (random.nextInt(4) == 0)
              v.newEdge(type, target);
          }
        }
        rids.add(v.getIdentity());
      }
    });

    final List<String> queries = new ArrayList<>();
    for (int i = 0; i < 160; i++)
      queries.add(randomChain(random));
    // the issue's shapes and the ones a run of more than one pair takes
    queries.add("MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)");
    queries.add("MATCH (a)-[:K]->(b)-[:K]->(c)");
    queries.add("MATCH (a)-[:K]->(b)-[:KC]->(c)-[:K]->(d)");
    queries.add("MATCH (a)<-[:KC]-(b)<-[:K]-(c)<-[:KD]-(d)");
    queries.add("MATCH (a)-[:KC]->(b)-[:K]->(c:P)-[:KD]->(d)-[:L]->(e)");
    queries.add("MATCH (a:Q)-[:KC]->(b)-[:KC]->(c)-[:L]->(d)-[:L]->(e)");
    queries.add("MATCH (a)-[:L]->(b {w: 1})-[:L]->(c:Q)-[:L]->(d)");

    final long[] expected = new long[queries.size()];
    int pushedDown = 0;
    for (int i = 0; i < queries.size(); i++) {
      final String match = queries.get(i);
      expected[i] = enumerated(match + " RETURN count(*) AS n");
      assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected[i]);
      if (plan(match + " RETURN count(*) AS n").contains(PUSHED_DOWN))
        ++pushedDown;
    }
    // the random shapes are not vacuous: a good share of them takes the push-down
    assertThat(pushedDown).isGreaterThan(40);

    final String[] fromOneVertex = { "-[:K]->(b)-[:K]->(c)", "-[:K]->(b:P)-[:KC]->(c)-[:L]->(d)", "<-[:L]-(b)<-[:L]-(c:Q)",
        "-[:KC]->(b)-[:K]->(c)-[:KC]->(d)", "-[:L]->(b {w: 0})-[:L]->(c)-[:K]->(d)-[:K]->(e)" };
    for (final String tail : fromOneVertex)
      assertPerVertexCounts(tail);

    withView(() -> {
      for (int i = 0; i < queries.size(); i++)
        assertThat(count(queries.get(i) + " RETURN count(*) AS n")).as("with a view: %s", queries.get(i)).isEqualTo(expected[i]);
      for (final String tail : fromOneVertex)
        assertPerVertexCounts(tail);

      // the operator itself, from every vertex: the walk seeded on the view and on the edge lists, against the pipeline
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v11918");
      // chains the planner accepts (checked below): no two hops share an edge but adjacent ones walked the same way
      final String[][] chains = { { "K", "K" }, { "K", "KC" }, { "KC", "K", "KD" }, { "KC", "KC", "KD", "KD" },
          { "L", "KL" } };
      for (final String[] types : chains)
        for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
          final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[types.length];
          Arrays.fill(directions, direction);
          final StringBuilder hops = new StringBuilder();
          for (int h = 0; h < types.length; h++)
            hops.append(direction == Vertex.DIRECTION.OUT ? "-[:" + types[h] + "]->()" : "<-[:" + types[h] + "]-()");
          assertThat(plan("MATCH (x)" + hops + " RETURN count(*) AS n")).as(hops.toString()).contains(PUSHED_DOWN);
          final String pattern = "MATCH (x {id: $id})" + hops;
          for (final RID rid : rids) {
            final PropagateChainOp op = new PropagateChainOp(new String[types.length + 1], types, directions, -1, -1, rid, null);
            final long oracle = sumOne(pattern + " RETURN sum(1) AS n", (Integer) rid.asVertex().get("id"));
            assertThat(op.execute(view, database, WorkGuard.forCommandDeadline(null))).as("%s from %s", pattern, rid)
                .isEqualTo(oracle);
            assertThat(op.executeOLTP(database, WorkGuard.forCommandDeadline(null))).as("%s from %s", pattern, rid)
                .isEqualTo(oracle);
          }
        }
    });
  }

  /**
   * The correlated {@code COUNT {}} of a chain from each vertex, and the same chain from a vertex pinned by a UNIQUE key,
   * against the pipeline's per-vertex count.
   */
  private void assertPerVertexCounts(final String tail) {
    final TreeMap<Integer, Long> oracle = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (x)" + tail + " RETURN x.id AS id, sum(1) AS n")) {
      rs.stream().forEach(r -> oracle.put(r.getProperty("id"), ((Number) r.getProperty("n")).longValue()));
    }
    final TreeMap<Integer, Long> counted = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (x) RETURN x.id AS id, COUNT { (x)" + tail + " } AS n")) {
      rs.stream().forEach(r -> {
        final long n = ((Number) r.getProperty("n")).longValue();
        if (n != 0)
          counted.put(r.getProperty("id"), n);
      });
    }
    assertThat(counted).as(tail).isEqualTo(oracle);

    // R carries the UNIQUE key, and every third vertex is an R
    for (int id = 2; id < 36; id += 3) {
      final String keyed = "MATCH (x:R {id: " + id + "})" + tail + " RETURN count(*) AS n";
      assertThat(count(keyed)).as(keyed).isEqualTo(oracle.getOrDefault(id, 0L));
    }
  }

  /** A random chain of 2 to 4 hops, mostly walked one way, over overlapping types, with labels and an inline predicate. */
  private static String randomChain(final Random random) {
    final String[] types = { "K", "KC", "KD", "L", "KL" };
    final String[] labels = { "", "", ":P", ":Q", ":R" };
    final int hops = 2 + random.nextInt(3);
    final int mainDirection = random.nextInt(2);
    final StringBuilder b = new StringBuilder("MATCH ");
    for (int i = 0; i <= hops; i++) {
      b.append("(n").append(i).append(labels[random.nextInt(labels.length)]);
      if (random.nextInt(6) == 0)
        b.append(" {w: ").append(random.nextInt(2)).append('}');
      b.append(')');
      if (i == hops)
        break;
      final String type = types[random.nextInt(types.length)];
      final int direction = random.nextInt(8) == 0 ? 2 : random.nextInt(6) == 0 ? 1 - mainDirection : mainDirection;
      b.append(switch (direction) {
        case 0 -> "-[:" + type + "]->";
        case 1 -> "<-[:" + type + "]-";
        default -> "-[:" + type + "]-";
      });
    }
    if (random.nextInt(8) == 0)
      b.append(" WHERE n0 <> n2");
    return b.toString();
  }

  private void withView(final ThrowingRunnable check) throws Exception {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW v11918 VERTEX TYPES (P, Q, R) EDGE TYPES (K, KC, KD, L, KL) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v11918");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      check.run();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v11918");
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private long enumerated(final String query) {
    // sum(1) forces row-by-row matching, which has no push-down: this is the oracle
    return count(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n"));
  }

  private long sumOne(final String query, final int id) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("id", id))) {
      final Object n = rs.next().getProperty("n");
      return n == null ? 0L : ((Number) n).longValue();
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Object n = rs.next().getProperty("n");
      return n == null ? 0L : ((Number) n).longValue();
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
    }
  }
}
