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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #8750 and #9540: an undirected relationship pattern matches a self loop once (openCypher TCK "Matching a
 * self-loop with an undirected relationship pattern", Neo4j, and the ArcadeDB row pipeline), but the count push-downs
 * read the outgoing and the incoming adjacency of the vertex and a self loop sits in both, so they counted it twice:
 * the chain count ({@code COUNT CHAIN PATHS}), with and without a Graph Analytical View, the per-node edge count
 * ({@code COUNT EDGES RETURN}), the {@code OPTIONAL MATCH ... WITH n, count(m)} optimization and the {@code COUNT { }}
 * subquery.
 * <p>
 * The generic checks compare every push-down with the ordinary row pipeline, reached through a {@code WITH *} that
 * the push-downs decline, so a count is checked against the pipeline it stands in for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8750SelfLoopCountPushDownTest extends TestHelper {

  /** The queries of the #8750 report, with the rows Neo4j 2026.08.1 returns for them. */
  @Test
  void perNodeCountsMatchNeo4j() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}),
               (a)-[:R]->(a), (a)-[:R]->(b), (c)-[:R]->(c), (c)-[:S]->(b)"""));

    assertThat(rows("MATCH (n:N)-[:R]-(m) RETURN n.id AS id, m.id AS m ORDER BY id, m"))
        .containsExactly("1:1", "1:2", "2:1", "3:3");
    assertThat(rows("MATCH (n:N)-[:R]-(m) RETURN n.id AS id, count(*) AS c ORDER BY id"))
        .containsExactly("1:2", "2:1", "3:1");
    assertThat(rows("MATCH (n:N)-[r]-(m) RETURN n.id AS id, count(*) AS c ORDER BY id"))
        .containsExactly("1:2", "2:2", "3:2");
    assertThat(rows("MATCH (n:N) OPTIONAL MATCH (n)-[:R]-(m) WITH n, count(m) AS c RETURN n.id AS id, c ORDER BY id"))
        .containsExactly("1:2", "2:1", "3:1");
    assertThat(rows("MATCH (n:N) RETURN n.id AS id, COUNT { (n)-[:R]-() } AS c ORDER BY id"))
        .containsExactly("1:2", "2:1", "3:1");
    assertThat(rows("MATCH (n:N) WHERE COUNT { (n)-[:R]-() } = 2 RETURN n.id AS id ORDER BY id"))
        .containsExactly("1");
    assertThat(rows("MATCH (n:N)-[:R]-(m) WITH n, m RETURN n.id AS id, count(*) AS c ORDER BY id"))
        .containsExactly("1:2", "2:1", "3:1");
    assertThat(rows("MATCH (n:N) RETURN n.id AS id, size([(n)-[:R]-() | 1]) AS c ORDER BY id"))
        .containsExactly("1:2", "2:1", "3:1");

    // the same shapes over the untyped and the multi-type relationship, and with the node bound by a property
    assertThat(rows("MATCH (n:N) OPTIONAL MATCH (n)-[]-(m) WITH n, count(m) AS c RETURN n.id AS id, c ORDER BY id"))
        .containsExactly("1:2", "2:2", "3:2");
    assertThat(rows("MATCH (n:N) RETURN n.id AS id, COUNT { (n)-[:R|S]-() } AS c ORDER BY id"))
        .containsExactly("1:2", "2:2", "3:2");
    assertThat(rows("MATCH (n:N {id: 1})-[:R]-(m) RETURN count(*) AS c")).containsExactly("2");
    assertThat(rows("MATCH (n:N)-[:R]-(m) RETURN count(*) AS c")).containsExactly("4");
    // a directed pattern still matches the self loop once per direction it asks for
    assertThat(rows("MATCH (n:N)-[:R]->(m) RETURN n.id AS id, count(*) AS c ORDER BY id")).containsExactly("1:2", "3:1");
    assertThat(rows("MATCH (n:N)<-[:R]-(m) RETURN n.id AS id, count(*) AS c ORDER BY id"))
        .containsExactly("1:1", "2:1", "3:1");
  }

  /** The repro table of #9540, with and without a Graph Analytical View. */
  @Test
  void chainCountMatchesThePipeline() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE M");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE C");
    database.command("sql", "CREATE EDGE TYPE L");
    database.transaction(() -> {
      final MutableVertex p1 = database.newVertex("P").save();
      final MutableVertex p2 = database.newVertex("P").save();
      final MutableVertex m1 = database.newVertex("M").save();
      p1.newEdge("K", p1);
      p1.newEdge("K", p2);
      m1.newEdge("C", p1);
      p1.newEdge("L", m1);
      p2.newEdge("L", m1);
    });

    final String[] matches = {
        "MATCH (a:P)-[:K]-(b:P)",
        "MATCH (a:P)-[:K]-(b)",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:L]->(x:M)" };
    final long[] expected = { 3, 3, 2, 2 };
    for (int i = 0; i < matches.length; i++) {
      assertThat(countMatch(matches[i])).as(matches[i]).isEqualTo(expected[i]);
      assertThat(count(pipelineQuery(matches[i]))).as(matches[i]).isEqualTo(expected[i]);
    }

    withView("selfLoopChain", "(P, M)", "(K, C, L)", () -> {
      for (int i = 0; i < matches.length; i++)
        assertThat(countMatch(matches[i])).as("with a view: %s", matches[i]).isEqualTo(expected[i]);
    });
  }

  /**
   * The shape of the #8750 report: every vertex has a self loop plus a few ordinary edges, and the chain shapes walk the
   * undirected hop first, in the middle and last, with and without the inequality the chain count expands per source.
   */
  @Test
  void everyCountShapeMatchesThePipeline() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE VERTEX TYPE O");
    database.command("sql", "CREATE EDGE TYPE CR");
    database.command("sql", "CREATE EDGE TYPE RE");
    final Random random = new Random(8750);
    database.transaction(() -> {
      final List<MutableVertex> ps = new ArrayList<>();
      final List<MutableVertex> qs = new ArrayList<>();
      for (int i = 0; i < 30; i++)
        ps.add(database.newVertex("P").set("id", i).save());
      for (int i = 0; i < 10; i++)
        qs.add(database.newVertex("Q").set("id", i).save());
      for (int i = 0; i < ps.size(); i++) {
        final MutableVertex p = ps.get(i);
        p.newEdge("K", p);
        if (i % 3 == 0)
          p.newEdge("K", p); // two parallel self loops: each is matched once
        for (int k = random.nextInt(4); k > 0; k--)
          p.newEdge("K", ps.get(random.nextInt(ps.size())));
        for (int k = random.nextInt(3); k > 0; k--)
          p.newEdge("L", qs.get(random.nextInt(qs.size())));
        if (i % 5 == 0)
          p.newEdge("L", p);
      }
      // comments and posts by the P vertices, for the pair join: (a)<-[:CR]-(c:C)-[:RE]->(o:O)-[:CR]->(b)
      final List<MutableVertex> posts = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        final MutableVertex o = database.newVertex("O").save();
        o.newEdge("CR", ps.get(random.nextInt(ps.size())));
        posts.add(o);
      }
      for (int i = 0; i < 120; i++) {
        final MutableVertex c = database.newVertex("C").save();
        c.newEdge("CR", ps.get(random.nextInt(ps.size())));
        c.newEdge("RE", posts.get(random.nextInt(posts.size())));
      }
    });

    final String[] matches = {
        "MATCH (a:P)-[:K]-(b:P)",
        "MATCH (a:P)-[:K]-(b)",
        "MATCH (a)-[:K]-(b)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c)",
        "MATCH (a:P)-[:K]->(b:P)-[:K]-(c:P)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]->(c:P)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:P)",
        "MATCH (a:P)-[:K]-(b:P)-[:L]->(c:Q)",
        "MATCH (a:P)-[:K|L]-(b)",
        "MATCH (a:P)-[]-(b)",
        "MATCH (a:P)-[:K]-(b:P) WHERE a <> b",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:L]-(c) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P), (a)-[:K]-(c)",
        "MATCH (a:P)-[:K]-(b:P) WHERE NOT (a)-[:L]-()",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)-[:K]-(c)",
        // the chain count with the inequality between the two ends of three hops, and between the first and the third
        "MATCH (t1:Q)<-[:L]-(m:P)-[:K]-(c:P)-[:L]->(t2:Q) WHERE t1 <> t2",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(q:Q) WHERE a <> c",
        // the anti-join chain
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)-[:K]-(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(q:Q) WHERE NOT (a)-[:K]-(c) AND a <> c",
        "MATCH (t1:Q)<-[:L]-(m:P)<-[:K]-(c:P)-[:L]->(t2:Q) WHERE NOT (c)-[:L]->(t1) AND t1 <> t2",
        // the pair join, probing an undirected hop
        "MATCH (a:P)-[:K]-(b:P), (a)<-[:CR]-(c:C)-[:RE]->(o:O)-[:CR]->(b)" };

    final long[] expected = new long[matches.length];
    final StringBuilder plans = new StringBuilder();
    for (int i = 0; i < matches.length; i++) {
      expected[i] = count(pipelineQuery(matches[i]));
      assertThat(plan(pipelineQuery(matches[i]))).as("plan of the pipeline for %s", matches[i]).doesNotContain("COUNT ");
      plans.append(plan(matches[i] + " RETURN count(*) AS n"));
    }
    for (int i = 0; i < matches.length; i++)
      assertThat(countMatch(matches[i])).as(matches[i]).isEqualTo(expected[i]);

    // each per-node count next to the same count through the row pipeline: the rows are materialized behind a WITH *
    // before the count, so no edge-count step can claim it
    final String opt = "MATCH (a:P) OPTIONAL MATCH (a)";
    final String[][] perNode = {
        { "MATCH (a:P)-[:K]-(b) RETURN a.id AS id, count(*) AS n",
            "MATCH (a:P)-[:K]-(b) WITH * RETURN a.id AS id, count(*) AS n" },
        { "MATCH (a:P)-[:K]-(b) RETURN a, count(*) AS n", "MATCH (a:P)-[:K]-(b) WITH * RETURN a, count(*) AS n" },
        { "MATCH (a:P)-[r:K]-(b) RETURN a.id AS id, count(r) AS n",
            "MATCH (a:P)-[r:K]-(b) WITH * RETURN a.id AS id, count(r) AS n" },
        { "MATCH (a:P)-[]-(b) RETURN a.id AS id, count(*) AS n", "MATCH (a:P)-[]-(b) WITH * RETURN a.id AS id, count(*) AS n" },
        { opt + "-[:K]-(b) WITH a, count(b) AS n RETURN a.id AS id, n",
            opt + "-[:K]-(b) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { opt + "-[:K]-(b) WITH a, count(*) AS n RETURN a.id AS id, n",
            opt + "-[:K]-(b) WITH * WITH a, count(*) AS n RETURN a.id AS id, n" },
        { opt + "-[:K]-(b:P) WITH a, count(b) AS n RETURN a.id AS id, n",
            opt + "-[:K]-(b:P) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { opt + "-[:K|L]-(b) WITH a, count(b) AS n RETURN a.id AS id, n",
            opt + "-[:K|L]-(b) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { "MATCH (a:P) RETURN a.id AS id, COUNT { (a)-[:K]-() } AS n",
            opt + "-[:K]-(b) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { "MATCH (a:P) RETURN a.id AS id, COUNT { (a)-[:K]-(:P) } AS n",
            opt + "-[:K]-(b:P) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { "MATCH (a:P) RETURN a.id AS id, COUNT { (a)-[]-() } AS n",
            opt + "-[]-(b) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { "MATCH (a:P) RETURN a.id AS id, size([(a)-[:K]-() | 1]) AS n",
            opt + "-[:K]-(b) WITH * WITH a, count(b) AS n RETURN a.id AS id, n" },
        { "MATCH (a:P) WHERE COUNT { (a)-[:K]-() } > 2 RETURN a.id AS id, 1 AS n",
            opt + "-[:K]-(b) WITH * WITH a, count(b) AS c WHERE c > 2 RETURN a.id AS id, 1 AS n" },
        { opt + "-[:K]-(i:P) OPTIONAL MATCH (t)-[:K]-(i) WITH a, count(t) AS n RETURN a.id AS id, n",
            opt + "-[:K]-(i:P) OPTIONAL MATCH (t)-[:K]-(i) WITH * WITH a, count(t) AS n RETURN a.id AS id, n" } };
    for (final String[] pair : perNode) {
      assertThat(perNodeCounts(pair[0])).as(pair[0]).isEqualTo(perNodeCounts(pair[1]));
      plans.append(plan(pair[0]));
    }

    // every count push-down the issue is about is reached by some shape above, so none of them passes vacuously
    assertThat(plans.toString()).contains("COUNT CHAIN PATHS", "COUNT ANTI-JOIN CHAIN", "COUNT PAIR JOIN",
        "COUNT EDGES RETURN", "COUNT EDGES OPTIMIZATION", "COUNT CHAINED EDGES OPTIMIZATION");

    withView("selfLoops", "(P, Q)", "(K, L)", () -> {
      for (int i = 0; i < matches.length; i++)
        assertThat(countMatch(matches[i])).as("with a view: %s", matches[i]).isEqualTo(expected[i]);
      for (final String[] pair : perNode)
        assertThat(perNodeCounts(pair[0])).as("with a view: %s", pair[0]).isEqualTo(perNodeCounts(pair[1]));
    });
  }

  /**
   * Found while fixing #8750, with no self loop involved: the chain count subtracts the paths whose two inequality
   * positions are one vertex, and took the part of such a path before and after the inequality as products of that
   * vertex's own degrees. That ignored the labels written on those positions, was wrong for a prefix or a tail longer
   * than one hop, and on the edge-list path read the prefix hops from the wrong end.
   */
  @Test
  void inequalitySubtractionFollowsThePrefixAndTheTail() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE VERTEX TYPE Z");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE EDGE TYPE X");
    final Random random = new Random(87501);
    database.transaction(() -> {
      final List<MutableVertex> ps = new ArrayList<>();
      final List<MutableVertex> qs = new ArrayList<>();
      for (int i = 0; i < 25; i++)
        ps.add(database.newVertex("P").save());
      for (int i = 0; i < 8; i++)
        qs.add(database.newVertex("Q").save());
      for (final MutableVertex p : ps) {
        for (int k = 1 + random.nextInt(3); k > 0; k--) {
          final MutableVertex other = ps.get(random.nextInt(ps.size()));
          if (other != p)
            p.newEdge("K", other);
        }
        // L reaches both a Q and a P: only the first is a (:Q) of the pattern
        p.newEdge("L", qs.get(random.nextInt(qs.size())));
        p.newEdge("L", ps.get(random.nextInt(ps.size())));
      }
      for (final MutableVertex q : qs)
        for (int k = random.nextInt(3); k > 0; k--)
          q.newEdge("X", database.newVertex(random.nextBoolean() ? "Z" : "P").save());
    });

    final String[] matches = {
        // a labelled tail
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(q:Q) WHERE a <> c",
        // a tail of two hops
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(q:Q)-[:X]->(z:Z) WHERE a <> c",
        // a prefix of one hop and of two hops
        "MATCH (q:Q)<-[:L]-(a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        "MATCH (z:Z)<-[:X]-(q:Q)<-[:L]-(a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        // both, the inequality between the first and the third position of the sub-chain
        "MATCH (q:Q)<-[:L]-(a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(r:Q) WHERE a <> c" };
    final long[] expected = new long[matches.length];
    for (int i = 0; i < matches.length; i++) {
      expected[i] = count(pipelineQuery(matches[i]));
      assertThat(plan(matches[i] + " RETURN count(*) AS n")).as("plan of %s", matches[i]).contains("COUNT CHAIN PATHS");
      assertThat(countMatch(matches[i])).as(matches[i]).isEqualTo(expected[i]);
    }

    withView("inequalityChains", "(P, Q, Z)", "(K, L, X)", () -> {
      for (int i = 0; i < matches.length; i++)
        assertThat(countMatch(matches[i])).as("with a view: %s", matches[i]).isEqualTo(expected[i]);
    });
  }

  /**
   * The undirected count reads the edge-list heads of the instance the transaction holds: a handle taken before an
   * edge was appended in the same transaction still points at the previous heads and would hide the newest edges.
   */
  @Test
  void undirectedCountReadsTheTransactionInstance() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    final RID[] ids = new RID[2];
    database.transaction(() -> {
      ids[0] = database.newVertex("P").save().getIdentity();
      ids[1] = database.newVertex("P").save().getIdentity();
    });
    database.transaction(() -> {
      final VertexInternal stale = (VertexInternal) database.lookupByRID(ids[0], true).asVertex();
      final MutableVertex current = stale.modify();
      current.newEdge("K", current);
      current.newEdge("K", ids[1].asVertex());
      database.lookupByRID(ids[1], true).asVertex().modify().newEdge("K", current);

      // a self loop once, the edge to the other vertex and the one back from it
      assertThat(((DatabaseInternal) database).getGraphEngine().countUndirectedEdges(stale, "K")).isEqualTo(3L);
      assertThat(IncomingEdgeLookup.countPatternEdges(null, stale, Vertex.DIRECTION.BOTH, "K")).isEqualTo(3L);
      assertThat(stale.countEdges(Vertex.DIRECTION.BOTH, "K")).isEqualTo(4L);
    });
  }

  /** A self loop on a vertex whose type has no other edge, added and removed inside the transaction that reads it. */
  @Test
  void selfLoopInsideTheTransaction() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      final MutableVertex p = database.newVertex("P").save();
      p.newEdge("K", p);
      assertThat(countMatch("MATCH (a:P)-[:K]-(b)")).isEqualTo(1L);
      assertThat(rows("MATCH (a:P) RETURN COUNT { (a)-[:K]-() } AS c")).containsExactly("1");
    });
  }

  private void withView(final String name, final String vertexTypes, final String edgeTypes, final Runnable check)
      throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " VERTEX TYPES " + vertexTypes + " EDGE TYPES "
        + edgeTypes + " UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      check.run();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW " + name);
    }
  }

  /** The same count through the ordinary row pipeline: the push-down detectors only take a MATCH ... RETURN statement. */
  private static String pipelineQuery(final String match) {
    return match + " WITH * RETURN count(*) AS n";
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
    }
  }

  /** Rows of a per-node count as "id=n" strings, sorted, so two plans of the same query can be compared. */
  private List<String> perNodeCounts(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.stream().forEach(r -> {
        final Object key = r.hasProperty("id") ? r.getProperty("id") : r.getProperty("a").toString();
        out.add(key + "=" + r.getProperty("n"));
      });
    }
    out.sort(null);
    return out;
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      rs.stream().forEach(r -> {
        final StringBuilder b = new StringBuilder();
        for (final String name : r.getPropertyNames()) {
          if (!b.isEmpty())
            b.append(':');
          b.append((Object) r.getProperty(name));
        }
        out.add(b.toString());
      });
    }
    return out;
  }

  private long countMatch(final String match) {
    return count(match + " RETURN count(*) AS n");
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
