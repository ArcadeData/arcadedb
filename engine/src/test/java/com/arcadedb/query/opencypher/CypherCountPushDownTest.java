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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeBucketMask;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.SimpleCypherStatement;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The count push-down optimization: self loops, multiplicity, property predicates, pattern order, edge counts and cost-based decline.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherCountPushDownTest {
  /**
   * Issue #8426 follow-up: the count push-down declined every chain whose two overlapping hops were not adjacent, but the
   * hops of {@code (t1)<-[:HAS_TAG]-(m)<-[:REPLY_OF]-(c)-[:HAS_TAG]->(t2) WHERE t1 <> t2} (LSQB Q5) can bind one edge only
   * if {@code m = c} and {@code t1 = t2}, and the inequality forbids exactly that. The chain fell back to pattern matching
   * (0.2s to 3.5s on SF1). The rule is now "the inequality joins two nodes that binding one edge forces equal, under every
   * orientation of the hops", so unprotected shapes still decline.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8426NonAdjacentOverlapPushDown extends TestHelper {
    private static final int MESSAGES = 120;
    private static final int TAGS = 25;
    private static final String PUSHED_DOWN = "COUNT CHAIN PATHS";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE VERTEX TYPE Msg");
      database.command("sql", "CREATE VERTEX TYPE Cmt EXTENDS Msg");
      database.command("sql", "CREATE EDGE TYPE HAS_TAG");
      database.command("sql", "CREATE EDGE TYPE REPLY_OF");
      database.command("sql", "CREATE EDGE TYPE LIKES");
      database.command("sql", "CREATE EDGE TYPE SEEN");

      final Random random = new Random(8426);
      database.transaction(() -> {
        final List<Vertex> tags = new ArrayList<>();
        for (int i = 0; i < TAGS; i++)
          tags.add(database.newVertex("Tag").set("id", i).save());

        final List<Vertex> messages = new ArrayList<>();
        for (int i = 0; i < MESSAGES; i++) {
          final Vertex m = database.newVertex(i % 3 == 0 ? "Msg" : "Cmt").set("id", i).save();
          messages.add(m);
          // 0 to 3 tags per message, parallel edges to one tag included, so a wrong overlap rule changes the count
          final int tagCount = random.nextInt(4);
          for (int k = 0; k < tagCount; k++)
            m.modify().newEdge("HAS_TAG", tags.get(random.nextInt(TAGS)));
          if (random.nextInt(3) == 0)
            m.modify().newEdge("HAS_TAG", tags.get(0));
          if (random.nextInt(2) == 0)
            m.modify().newEdge("LIKES", messages.get(random.nextInt(messages.size())));
          // parallel SEEN edges to one message: two edges, two distinct paths
          final Vertex seen = messages.get(random.nextInt(messages.size()));
          for (int k = random.nextInt(3); k > 0; k--)
            m.modify().newEdge("SEEN", seen);
          if (random.nextInt(2) == 0)
            m.modify().newEdge("LIKES", seen);
          if (random.nextInt(2) == 0)
            m.modify().newEdge("LIKES", seen);
        }
        for (int i = 1; i < MESSAGES; i++)
          if (random.nextInt(4) != 0)
            messages.get(i).modify().newEdge("REPLY_OF", messages.get(random.nextInt(i)));
      });
    }

    /** LSQB Q5: the two HAS_TAG hops are two hops apart and {@code t1 <> t2} is the inequality that separates them. */
    @Test
    void theNonAdjacentOverlapProtectedByItsInequalityIsPushedDown() {
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg)<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS n";
      final long expected = enumerated(query);
      assertThat(expected).isPositive();

      createView();
      assertThat(count(query)).as("answer with the view").isEqualTo(expected);
      assertThat(plan(query)).as("the chain takes the count push-down").contains(PUSHED_DOWN);
    }

    /** The same shape written the other way round: hops 0 and 2 still overlap, the inequality is still on their far ends. */
    @Test
    void theMirroredShapeIsPushedDownToo() {
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(c:Cmt)-[:REPLY_OF]->(m:Msg)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS n";
      final long expected = enumerated(query);
      assertThat(expected).isPositive();

      createView();
      assertThat(count(query)).isEqualTo(expected);
      assertThat(plan(query)).contains(PUSHED_DOWN);
    }

    /** Same chain without the inequality: hops 0 and 2 can bind one edge (m = c, t1 = t2), so it must NOT be pushed down. */
    @Test
    void anUnprotectedOverlapIsStillDeclinedAndCountedRight() {
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg)<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) RETURN count(*) AS n";
      final long expected = enumerated(query);

      createView();
      assertThat(count(query)).isEqualTo(expected);
      assertThat(plan(query)).doesNotContain(PUSHED_DOWN);
    }

    /** Binding one edge twice forces {@code m = c} as well as {@code t1 = t2}, so {@code m <> c} protects the overlap too. */
    @Test
    void anInequalityOnTheOtherForcedEqualPairProtectsTheOverlapToo() {
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg)<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) WHERE m <> c RETURN count(*) AS n";
      final long expected = enumerated(query);

      createView();
      assertThat(count(query)).isEqualTo(expected);
      assertThat(plan(query)).contains(PUSHED_DOWN);
    }

    /** An inequality on nodes the shared edge does not force equal ({@code m} and {@code t2}) leaves the overlap open: declined, still correct. */
    @Test
    void anInequalityOnNodesThatAreNotForcedEqualDoesNotProtectTheOverlap() {
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg)<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) WHERE m <> t2 RETURN count(*) AS n";
      final long expected = enumerated(query);

      createView();
      assertThat(count(query)).isEqualTo(expected);
      assertThat(plan(query)).doesNotContain(PUSHED_DOWN);
    }

    /** Undirected overlapping hops can be walked either way, and one orientation is not covered by {@code t1 <> t2}. */
    @Test
    void undirectedOverlappingHopsAreCountedRight() {
      final String query = "MATCH (t1:Tag)-[:HAS_TAG]-(m:Msg)-[:LIKES]-(c:Msg)-[:HAS_TAG]-(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS n";
      final long expected = enumerated(query);

      createView();
      assertThat(count(query)).isEqualTo(expected);
    }

    /**
     * Two tags each held through parallel edges on both sides: tags(m) = {T1, T1, T2} and tags(c) = {T1, T2, T2} give 9 tag
     * pairs, 4 of them on the same tag (2 x 1 for T1, 1 x 2 for T2), so 5 pairs satisfy {@code t1 <> t2}. Subtracting the
     * minimum of the two multiplicities instead of their product would subtract 2 and answer 7.
     */
    @Test
    void parallelEdgesCloseOnePathPerPairOfEdgesOnTheNonAdjacentChain() {
      database.transaction(() -> {
        final Vertex t1 = database.newVertex("Tag").set("id", 9001).save();
        final Vertex t2 = database.newVertex("Tag").set("id", 9002).save();
        final Vertex m = database.newVertex("Msg").set("id", 9003).save();
        final Vertex c = database.newVertex("Cmt").set("id", 9004).save();
        m.modify().newEdge("HAS_TAG", t1);
        m.modify().newEdge("HAS_TAG", t1);
        m.modify().newEdge("HAS_TAG", t2);
        c.modify().newEdge("HAS_TAG", t1);
        c.modify().newEdge("HAS_TAG", t2);
        c.modify().newEdge("HAS_TAG", t2);
        c.modify().newEdge("REPLY_OF", m);
      });
      final String query = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg {id: 9003})<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS n";
      assertThat(enumerated(query)).isEqualTo(5);

      final String whole = "MATCH (t1:Tag)<-[:HAS_TAG]-(m:Msg)<-[:REPLY_OF]-(c:Cmt)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 RETURN count(*) AS n";
      final long expected = enumerated(whole);
      createView();
      assertThat(count(whole)).as("the pushed-down count over the whole graph, crafted pair included").isEqualTo(expected);
      assertThat(plan(whole)).contains(PUSHED_DOWN);
    }

    /**
     * The same multiplicities on a chain whose edge types are disjoint ({@code LIKES}, {@code REPLY_OF}, {@code SEEN}), where
     * the inequality closes the chain's two ends: the pushed-down count must not depend on how the parallel edges are paired.
     */
    @Test
    void parallelEdgesCloseOnePathPerPairOfEdgesOnADisjointTypeChain() {
      database.transaction(() -> {
        final Vertex p = database.newVertex("Msg").set("id", 9101).save();
        final Vertex q = database.newVertex("Msg").set("id", 9102).save();
        final Vertex b = database.newVertex("Msg").set("id", 9103).save();
        final Vertex c = database.newVertex("Msg").set("id", 9104).save();
        // a in {p, p, q} reaches b by LIKES, d in {p, q, q} is reached from c by SEEN
        p.modify().newEdge("LIKES", b);
        p.modify().newEdge("LIKES", b);
        q.modify().newEdge("LIKES", b);
        b.modify().newEdge("REPLY_OF", c);
        c.modify().newEdge("SEEN", p);
        c.modify().newEdge("SEEN", q);
        c.modify().newEdge("SEEN", q);
      });
      final String query = "MATCH (a:Msg)-[:LIKES]->(b:Msg {id: 9103})-[:REPLY_OF]->(c:Msg)-[:SEEN]->(d:Msg) WHERE a <> d RETURN count(*) AS n";
      assertThat(enumerated(query)).isEqualTo(5);

      final String whole = "MATCH (a:Msg)-[:LIKES]->(b:Msg)-[:REPLY_OF]->(c:Msg)-[:SEEN]->(d:Msg) WHERE a <> d RETURN count(*) AS n";
      final long expected = enumerated(whole);
      createView();
      assertThat(count(whole)).isEqualTo(expected);
      assertThat(plan(whole)).contains(PUSHED_DOWN);
    }

    /** The adjacent case the original rule accepted keeps working. */
    @Test
    void theAdjacentCaseStillWorks() {
      final String query = "MATCH (a:Msg)-[:LIKES]->(b:Msg)<-[:LIKES]-(c:Msg) WHERE a <> c RETURN count(*) AS n";
      final long expected = enumerated(query);
      assertThat(expected).isPositive();

      createView();
      assertThat(count(query)).isEqualTo(expected);
    }

    private long enumerated(final String query) {
      // sum(1) forces row-by-row matching, which has no push-down: this is the oracle
      return count(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n"));
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private void createView() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8426q5 VERTEX TYPES (Tag, Msg, Cmt) EDGE TYPES (HAS_TAG, REPLY_OF, LIKES, SEEN)");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426q5");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.onSpinWait();
      assertThat(view.isReady()).isTrue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Issues #8750 and #9540: an undirected relationship pattern matches a self loop once (openCypher TCK "Matching a
   * self-loop with an undirected relationship pattern", Neo4j, and the ArcadeDB row pipeline), but the count push-downs
   * read the outgoing and the incoming adjacency of the vertex and a self loop sits in both, so they counted it twice:
   * the chain count ({@code COUNT CHAIN PATHS}), with and without a Graph Analytical View, the per-node edge count
   * ({@code COUNT EDGES RETURN}), the {@code OPTIONAL MATCH ... WITH n, count(m)} optimization and the {@code COUNT { }}
   * subquery.
   * <p>
   * The generic checks compare every push-down with the ordinary row pipeline, reached through a {@code sum(1)} that
   * the push-downs decline, so a count is checked against the pipeline it stands in for.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8750SelfLoopCountPushDown extends TestHelper {

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

    /**
     * The same count through the ordinary row pipeline: no count push-down answers a {@code sum()}. A {@code WITH *} used to
     * be the barrier, until issue #9597 made a pass-through {@code WITH} transparent to the push-downs.
     */
    private static String pipelineQuery(final String match) {
      return match + " RETURN sum(1) AS n";
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

  /**
   * Issues #9283 and #9540: an undirected relationship pattern matches a self loop once (the openCypher TCK scenario
   * "Matching a self-loop with an undirected relationship pattern", Neo4j, and the ArcadeDB row pipeline all agree), but
   * a self loop sits in both adjacency lists of its vertex, so a count push-down that adds the OUT and the IN degree, or
   * walks the merged undirected neighbor list, counted it twice.
   * <p>
   * Every count is checked against the ordinary row pipeline, reached through a {@code WITH *} that the push-down
   * detectors decline, on a graph without a view and on the same graph with a view covering the whole pattern, and the
   * chain counts once more against a brute-force enumeration of the relationships. Two defects found on the way are pinned
   * too: the chain inequality's prefix and tail were counted off the degree of a single vertex, and the star count over a
   * view counted vertices of another label than the central one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9283SelfLoopCountPushDown extends TestHelper {

    /** Shapes reaching every count push-down that can read an undirected hop. */
    private static final String[] SHAPES = {
        // single hop
        "MATCH (a:P)-[:K]-(b:P)",
        "MATCH (a:P)-[:K]-(b)",
        "MATCH (a)-[:K]-(b:P)",
        "MATCH (a)-[:K]-(b)",
        "MATCH ()-[:K]-()",
        "MATCH (a:P)-[:K]-(a)",
        // chains
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)",
        "MATCH (a:P)-[:K]->(b:P)-[:K]-(c:P)",
        "MATCH (a:P)-[:K]-(b:P)<-[:K]-(c:P)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(t:T)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(t:T) WHERE a <> c",
        "MATCH (t:T)<-[:I]-(a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(u:T)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:P)",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:L]->(x:M)",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(x:M)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]-(x:M)",
        // an inequality that does not start the chain: the prefix before it and the tail after it
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(t:T) WHERE a <> c",
        "MATCH (n:M)-[:L]->(m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:P) WHERE b <> d",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:P) WHERE a <> d",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:P)-[:I]->(t:T) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d:M) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(d) WHERE a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:L]->(x:M) WHERE a <> b",
        "MATCH (a:P)-[:K]-(b:P)-[:K]->(x:M) WHERE a <> b",
        "MATCH (a)-[:K]-(b)-[:K]-(c) WHERE a <> c",
        // anti-joins (LSQB q9 shape)
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)-[:K]-(c)",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)-[:K]-(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(t:T) WHERE NOT (a)-[:K]-(c) AND a <> c",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) WHERE NOT (a)-[:K]->(c) AND a <> c",
        // a directed negated pattern lets a walk over a self loop survive, and an undirected tail can reach one
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)-[:K]->(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE NOT (a)<-[:K]-(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]-(t) WHERE NOT (a)-[:K]-(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]-(t) WHERE NOT (a)-[:K]->(c) AND a <> c",
        "MATCH (a)-[:K]-(b)-[:K]-(c)-[:I]-(t) WHERE NOT (a)-[:K]->(c) AND a <> c",
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]-(t)",
        "MATCH (a:P)-[:I]-(t), (a)-[:K]-(b:P)",
        "MATCH (t:T)<-[:I]-(a:P)-[:K]-(b:P)-[:I]->(t)",
        "MATCH (t:T)<-[:I]-(a:P)-[:K]-(b:P) WHERE NOT (b)-[:I]->(t)",
        // cycles and pair joins
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(a)",
        "MATCH (a:P)-[:K]-(b:P), (a)-[:I]->(t:T)<-[:I]-(b)",
        "MATCH (a:P)<-[:C]-(m:M)-[:L]->(n:M)-[:C]->(b:P), (a)-[:K]-(b)",
        "MATCH (a:P)<-[:C]-(m:M)<-[:L]-(b:P), (a)-[:K]-(b)",
        "MATCH (a:P)<-[:C]-(m:M)<-[:L]-(b:P), (a)-[:K]->(b)",
        "MATCH (a:P)<-[:C]-(m:M)<-[:L]-(b:P), (b)-[:I]-(a)",
        // stars
        "MATCH (a:P)-[:K]-(b:P), (a)-[:I]->(t:T)",
        "MATCH (a:P)-[:K]-(b:P) OPTIONAL MATCH (a)-[:I]->(t:T)",
    };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE M");
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE C");
      database.command("sql", "CREATE EDGE TYPE L");
      database.command("sql", "CREATE EDGE TYPE I");
    }

    /** The repro of #9540, with the numbers worked out by hand. */
    @Test
    void issue9540ReproCountsTheSelfLoopOnce() throws InterruptedException {
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
      final String[] matches = { "MATCH (a:P)-[:K]-(b:P)", "MATCH (a:P)-[:K]-(b)", "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)",
          "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:L]->(x:M)" };
      final long[] expected = { 3, 3, 2, 2 };
      for (int i = 0; i < matches.length; i++)
        assertThat(countMatch(matches[i])).as(matches[i]).isEqualTo(expected[i]);
      assertEveryShapeMatchesThePipeline("no view");

      withView(() -> {
        for (int i = 0; i < matches.length; i++)
          assertThat(countMatch(matches[i])).as("with a view: " + matches[i]).isEqualTo(expected[i]);
        assertEveryShapeMatchesThePipeline("with a view");
      });
    }

    /** The random multigraph of #9283: parallel edges, record and light edges, single and parallel self loops. */
    @Test
    void randomMultigraphWithSelfLoopsMatchesThePipeline() throws InterruptedException {
      buildMultigraph(new Random(9283));
      assertEveryShapeMatchesThePipeline("no view");
      withView(() -> assertEveryShapeMatchesThePipeline("with a view"));
    }

    /**
     * The chain counts against the openCypher semantics worked out without the query engine: every edge is a relationship
     * of its own, an undirected hop reaches a self loop once, and the relationships of one path are distinct. An oracle that
     * shares no code with the row pipeline, which the other tests compare against.
     */
    @Test
    void chainCountsMatchABruteForceEnumeration() throws InterruptedException {
      buildMultigraph(new Random(9540));
      final Vertex.DIRECTION out = Vertex.DIRECTION.OUT;
      final Vertex.DIRECTION both = Vertex.DIRECTION.BOTH;
      final Object[][] chains = {
          { "MATCH (a:P)-[:K]-(b:P)", new String[] { "P", "P" }, new String[] { "K" }, new Vertex.DIRECTION[] { both }, -1, -1 },
          { "MATCH (a)-[:K]-(b)", new String[] { null, null }, new String[] { "K" }, new Vertex.DIRECTION[] { both }, -1, -1 },
          { "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)", new String[] { "M", "P", "P" }, new String[] { "C", "K" },
              new Vertex.DIRECTION[] { out, both }, -1, -1 },
          { "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c", new String[] { "P", "P", "P" }, new String[] { "K", "K" },
              new Vertex.DIRECTION[] { both, both }, 0, 2 },
          { "MATCH (a)-[:K]-(b)-[:K]-(c) WHERE a <> c", new String[] { null, null, null }, new String[] { "K", "K" },
              new Vertex.DIRECTION[] { both, both }, 0, 2 },
          { "MATCH (m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P)-[:I]->(t:T) WHERE a <> c", new String[] { "M", "P", "P", "P", "T" },
              new String[] { "C", "K", "K", "I" }, new Vertex.DIRECTION[] { out, both, both, out }, 1, 3 },
          // a two-hop labelled tail after the inequality, and a two-hop prefix before it
          { "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:L]->(x:M)-[:C]->(y:P) WHERE a <> c", new String[] { "P", "P", "P", "M", "P" },
              new String[] { "K", "K", "L", "C" }, new Vertex.DIRECTION[] { both, both, out, out }, 0, 2 },
          { "MATCH (n:M)-[:L]->(m:M)-[:C]->(a:P)-[:K]-(b:P)-[:K]-(c:P) WHERE a <> c", new String[] { "M", "M", "P", "P", "P" },
              new String[] { "L", "C", "K", "K" }, new Vertex.DIRECTION[] { out, out, both, both }, 2, 4 } };

      final long[] expected = new long[chains.length];
      for (int i = 0; i < chains.length; i++) {
        expected[i] = bruteForceChain((String[]) chains[i][1], (String[]) chains[i][2], (Vertex.DIRECTION[]) chains[i][3],
            (Integer) chains[i][4], (Integer) chains[i][5]);
        assertThat(expected[i]).as("the graph exercises %s", chains[i][0]).isPositive();
        // one hop between two unconstrained nodes is the edge count of its type (issue #9600)
        assertThat(plan(chains[i][0] + " RETURN count(*) AS n")).as("plan of %s", chains[i][0])
            .contains(((String[]) chains[i][1]).length == 2 && ((String[]) chains[i][1])[0] == null ? "COUNT EDGES" : "COUNT CHAIN PATHS");
        assertThat(countMatch((String) chains[i][0])).as("no view: %s", chains[i][0]).isEqualTo(expected[i]);
      }
      withView(() -> {
        for (int i = 0; i < chains.length; i++)
          assertThat(countMatch((String) chains[i][0])).as("with a view: %s", chains[i][0]).isEqualTo(expected[i]);
      });
    }

    /**
     * A unidirectional edge type keeps no incoming entry, and the query reads its incoming side from a scan of the edges
     * (issue #8625): the self loop is in the outgoing list and in the scan, and still matches once.
     */
    @Test
    void aUnidirectionalSelfLoopCountsOnce() {
      database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
      database.transaction(() -> {
        final MutableVertex p1 = database.newVertex("P").set("uid", 1).save();
        final MutableVertex p2 = database.newVertex("P").set("uid", 2).save();
        final MutableVertex m1 = database.newVertex("M").set("uid", 3).save();
        p1.newEdge("U", p1);
        p1.newEdge("U", p2);
        p2.newEdge("U", m1);
      });
      assertThat(countMatch("MATCH (a:P)-[:U]-(b:P)")).isEqualTo(3L);
      assertThat(countMatch("MATCH (a:P)-[:U]-(b)")).isEqualTo(4L);
      assertThat(groups("MATCH (a:P)-[:U]-(b) RETURN a.uid AS k, count(*) AS n")).containsExactly("1=2", "2=2");
      assertThat(groups("MATCH (a:P)-[:U]-(b:P) RETURN a.uid AS k, count(*) AS n")).containsExactly("1=2", "2=1");
      assertThat(groups("MATCH (a:P) OPTIONAL MATCH (a)-[:U]-(b) RETURN a.uid AS k, count(b) AS n")).containsExactly("1=2", "2=2");
      for (final String query : new String[] { "MATCH (a:P)-[:U]-(b) RETURN a.uid AS k, count(*) AS n",
          "MATCH (a:P)-[:U]-(b:P) RETURN a.uid AS k, count(*) AS n",
          "MATCH (a:P) OPTIONAL MATCH (a)-[:U]-(b) RETURN a.uid AS k, count(b) AS n",
          "MATCH (a:P) OPTIONAL MATCH (a)-[:U]->(m) OPTIONAL MATCH (x)-[:U]-(m) WITH a, count(x) AS n RETURN a.uid AS k, n" })
        assertGroupedMatchesPipeline(query, "unidirectional");
    }

    /**
     * The star count over a view visits every node of the view, and a node of another label that has edges of a mandatory
     * arm's type is not filtered out by that arm: under an optional arm it added a row of its own.
     */
    @Test
    void theStarCountKeepsToTheCentralLabel() throws InterruptedException {
      database.transaction(() -> {
        final MutableVertex p1 = database.newVertex("P").save();
        final MutableVertex p2 = database.newVertex("P").save();
        final MutableVertex m1 = database.newVertex("M").save();
        final MutableVertex t1 = database.newVertex("T").save();
        p1.newEdge("K", p2);
        m1.newEdge("K", p1);
        p1.newEdge("I", t1);
        m1.newEdge("I", t1);
      });
      // p1 knows p2 and has one interest, p2 knows p1 and has none: m1 knows p1 and has an interest too, but is not a P
      final String optional = "MATCH (a:P)-[:K]-(b:P) OPTIONAL MATCH (a)-[:I]->(t:T)";
      final String mandatory = "MATCH (a:P)-[:K]-(b:P), (a)-[:I]->(t:T)";
      assertThat(countMatch(optional)).isEqualTo(2L);
      assertThat(countMatch(mandatory)).isEqualTo(1L);
      withView(() -> {
        assertThat(plan(optional + " RETURN count(*) AS n")).contains("COUNT STAR JOIN");
        assertThat(plan(mandatory + " RETURN count(*) AS n")).contains("COUNT STAR JOIN");
        assertThat(countMatch(optional)).as("with a view").isEqualTo(2L);
        assertThat(countMatch(mandatory)).as("with a view").isEqualTo(1L);
      });
    }

    /** The undirected view keeps one entry per self loop, parallel loops included, and is built once per view. */
    @Test
    void theUndirectedViewListsEachSelfLoopOnce() {
      // node 0: two parallel self loops (four entries) and an edge to 1; node 1: the edge back; node 2: none
      final NeighborView merged = new NeighborView(3, new int[] { 0, 5, 6, 6 }, new int[] { 0, 0, 0, 0, 1, 0 });
      final NeighborView once = merged.withSelfLoopsOnce();
      assertThat(once.degree(0)).isEqualTo(3);
      assertThat(once.degree(1)).isEqualTo(1);
      assertThat(once.degree(2)).isZero();
      assertThat(Arrays.copyOfRange(once.neighbors(), once.offset(0), once.offsetEnd(0))).containsExactly(0, 0, 1);
      assertThat(once.neighbor(1, 0)).isZero();
      assertThat(merged.withSelfLoopsOnce()).isSameAs(once);
      assertThat(once.withSelfLoopsOnce()).isSameAs(once);

      final NeighborView noLoops = new NeighborView(2, new int[] { 0, 1, 2 }, new int[] { 1, 0 });
      assertThat(noLoops.withSelfLoopsOnce()).isSameAs(noLoops);

      // one entry of itself on each of two nodes is no pair to drop, though the two add up to one
      final NeighborView oddOnTwoNodes = new NeighborView(2, new int[] { 0, 1, 2 }, new int[] { 0, 1 });
      assertThat(oddOnTwoNodes.withSelfLoopsOnce()).isSameAs(oddOnTwoNodes);
      final NeighborView oddAndPair = new NeighborView(3, new int[] { 0, 1, 2, 4 }, new int[] { 0, 1, 2, 2 });
      final NeighborView oddAndPairOnce = oddAndPair.withSelfLoopsOnce();
      assertThat(oddAndPairOnce.degree(0)).isEqualTo(1);
      assertThat(oddAndPairOnce.degree(1)).isEqualTo(1);
      assertThat(oddAndPairOnce.degree(2)).isEqualTo(1);
      assertThat(oddAndPairOnce.edgeCount()).isEqualTo(3);

      // a zero-copy view over a larger buffer: the copy holds the ranges, not the buffer's tail
      final NeighborView overBuffer = new NeighborView(1, new int[] { 0, 2 }, new int[] { 0, 0, 7, 7, 7 });
      assertThat(overBuffer.withSelfLoopsOnce().edgeCount()).isEqualTo(1);
    }

    /** Self loops added after the view was built are served from its overlay, not from its CSR. */
    @Test
    void selfLoopsInTheViewOverlayMatchThePipeline() throws InterruptedException {
      buildMultigraph(new Random(42));
      withView(() -> {
        database.transaction(() -> {
          final List<Vertex> persons = new ArrayList<>();
          database.query("opencypher", "MATCH (a:P) RETURN a LIMIT 5").forEachRemaining(r -> persons.add(r.getProperty("a")));
          for (final Vertex p : persons) {
            final MutableVertex v = p.modify();
            v.newEdge("K", v);
            v.newEdge("K", v);
          }
          // self loops on a vertex the view has never seen, joined to the rest. Record edges only: a light edge created
          // after the build does not reach the overlay at all, which is a defect of its own
          final MutableVertex added = database.newVertex("P").set("uid", 99).save();
          added.newEdge("K", added);
          added.newEdge("K", added);
          added.newEdge("K", persons.get(0));
          persons.get(1).modify().newEdge("K", added);
          added.newEdge("I", database.newVertex("T").set("uid", 2099).save());
        });
        assertEveryShapeMatchesThePipeline("with a view and an overlay");
      });
    }

    /** The grouped form: one count per anchor vertex. */
    @Test
    void groupedCountsMatchThePipeline() throws InterruptedException {
      buildMultigraph(new Random(7));
      final String[] grouped = { "MATCH (a:P)-[:K]-(b) RETURN a.uid AS k, count(*) AS n",
          "MATCH (a:P)-[:K]-(b:P) RETURN a.uid AS k, count(b) AS n",
          "MATCH (a:P)-[:K]-(b)-[:K]-(c) RETURN a.uid AS k, count(*) AS n",
          "MATCH (a:P) RETURN a.uid AS k, COUNT { (a)-[:K]-() } AS n",
          "MATCH (a:P) RETURN a.uid AS k, size([(a)-[:K]-(x) | x]) AS n",
          "MATCH (a:P) OPTIONAL MATCH (a)-[:K]-(b) RETURN a.uid AS k, count(b) AS n",
          "MATCH (a:P) OPTIONAL MATCH (a)-[:K]->(m) OPTIONAL MATCH (x)-[:K]-(m) WITH a, count(x) AS n RETURN a.uid AS k, n",
          "MATCH (a:P) OPTIONAL MATCH (a)-[:K]-(m) OPTIONAL MATCH (x)-[:K]->(m) WITH a, count(x) AS n RETURN a.uid AS k, n" };
      for (final String query : grouped)
        assertGroupedMatchesPipeline(query, "no view");
      withView(() -> {
        for (final String query : grouped)
          assertGroupedMatchesPipeline(query, "with a view");
      });
    }

    private void buildMultigraph(final Random random) {
      database.transaction(() -> {
        final List<MutableVertex> persons = new ArrayList<>();
        final List<MutableVertex> messages = new ArrayList<>();
        final List<MutableVertex> tags = new ArrayList<>();
        for (int i = 0; i < 40; i++)
          persons.add(database.newVertex("P").set("uid", i).save());
        for (int i = 0; i < 30; i++)
          messages.add(database.newVertex("M").set("uid", 1000 + i).save());
        for (int i = 0; i < 8; i++)
          tags.add(database.newVertex("T").set("uid", 2000 + i).save());

        // parallel light edges are distinct relationships to the row pipeline too since #9573
        for (int i = 0; i < 160; i++) {
          final MutableVertex from = persons.get(random.nextInt(persons.size()));
          final MutableVertex to = random.nextInt(8) == 0 ? from : persons.get(random.nextInt(persons.size()));
          if (random.nextBoolean())
            from.newLightEdge("K", to);
          else
            from.newEdge("K", to);
        }
        // a few K edges leave the persons, so an unlabelled end reaches a vertex that is not a P
        for (int i = 0; i < 6; i++)
          persons.get(random.nextInt(persons.size())).newEdge("K", messages.get(random.nextInt(messages.size())));
        // parallel self loops on one vertex, one record and one light
        persons.get(0).newEdge("K", persons.get(0));
        persons.get(0).newLightEdge("K", persons.get(0));

        for (final MutableVertex p : persons) {
          for (int k = random.nextInt(3); k > 0; k--)
            p.newEdge("I", tags.get(random.nextInt(tags.size())));
          // a self loop of another type than the chain's, for an undirected tail hop to reach
          if (random.nextInt(5) == 0)
            p.newEdge("I", p);
        }
        for (final MutableVertex m : messages) {
          m.newEdge("C", persons.get(random.nextInt(persons.size())));
          for (int k = random.nextInt(3); k > 0; k--)
            persons.get(random.nextInt(persons.size())).newEdge("L", m);
          if (random.nextInt(4) == 0)
            m.newEdge("L", m);
        }
        for (int i = 0; i < 20; i++)
          messages.get(random.nextInt(messages.size())).newEdge("L", messages.get(random.nextInt(messages.size())));
      });
    }

    /**
     * The paths of a chain, enumerated over the edge records and light edges themselves: each edge is listed once, from its
     * source, and gets a number of its own, so two parallel light edges are two relationships.
     */
    private long bruteForceChain(final String[] labels, final String[] types, final Vertex.DIRECTION[] directions,
        final int inequalityA, final int inequalityB) {
      final List<RID[]> edges = new ArrayList<>();
      final List<String> edgeTypes = new ArrayList<>();
      final List<RID> vertices = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (v) RETURN v")) {
        while (rs.hasNext()) {
          final Vertex v = rs.next().getProperty("v");
          vertices.add(v.getIdentity());
          for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT)) {
            edges.add(new RID[] { e.getOut(), e.getIn() });
            edgeTypes.add(e.getTypeName());
          }
        }
      }
      final RID[] path = new RID[labels.length];
      final boolean[] used = new boolean[edges.size()];
      long total = 0;
      for (final RID start : vertices)
        if (hasLabel(start, labels[0])) {
          path[0] = start;
          total += extend(path, 0, used, edges, edgeTypes, labels, types, directions, inequalityA, inequalityB);
        }
      return total;
    }

    private long extend(final RID[] path, final int hop, final boolean[] used, final List<RID[]> edges, final List<String> edgeTypes,
        final String[] labels, final String[] types, final Vertex.DIRECTION[] directions, final int inequalityA, final int inequalityB) {
      if (hop == types.length)
        return inequalityA >= 0 && path[inequalityA].equals(path[inequalityB]) ? 0 : 1;
      final RID from = path[hop];
      long total = 0;
      for (int e = 0; e < edges.size(); e++) {
        if (used[e] || !edgeTypes.get(e).equals(types[hop]))
          continue;
        final RID source = edges.get(e)[0];
        final RID target = edges.get(e)[1];
        // an undirected hop walks the edge from either end, and a self loop has only one way to be walked
        for (int orientation = 0; orientation < 2; orientation++) {
          final RID near = orientation == 0 ? source : target;
          final RID far = orientation == 0 ? target : source;
          if (orientation == 1 && source.equals(target))
            continue;
          final boolean allowed = directions[hop] == Vertex.DIRECTION.BOTH
              || (directions[hop] == Vertex.DIRECTION.OUT) == (orientation == 0);
          if (!allowed || !near.equals(from) || !hasLabel(far, labels[hop + 1]))
            continue;
          used[e] = true;
          path[hop + 1] = far;
          total += extend(path, hop + 1, used, edges, edgeTypes, labels, types, directions, inequalityA, inequalityB);
          used[e] = false;
        }
      }
      return total;
    }

    private boolean hasLabel(final RID rid, final String label) {
      return label == null || database.getSchema().getTypeByBucketId(rid.getBucketId()).instanceOf(label);
    }

    private void assertEveryShapeMatchesThePipeline(final String variant) {
      final SoftAssertions softly = new SoftAssertions();
      for (final String match : SHAPES) {
        final String pushedDown = match + " RETURN count(*) AS n";
        final String pipeline = match + " WITH * RETURN count(*) AS n";
        softly.assertThat(count(pushedDown)).as("%s, %s\nplan: %s", variant, pushedDown, plan(pushedDown)).isEqualTo(count(pipeline));
      }
      softly.assertAll();
    }

    private void assertGroupedMatchesPipeline(final String query, final String variant) {
      // the aggregation reads its rows through a WITH * the count steps do not see through
      final int aggregation = query.contains(" WITH a, ") ? query.indexOf(" WITH a, ") : query.indexOf(" RETURN ");
      final String pipeline = query.substring(0, aggregation) + " WITH *" + query.substring(aggregation);
      assertThat(groups(query)).as("%s, %s\nplan: %s", variant, query, plan(query)).isEqualTo(groups(pipeline));
    }

    private List<String> groups(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(r.getProperty("k") + "=" + r.getProperty("n"));
        }
      }
      rows.sort(null);
      return rows;
    }

    private void withView(final ThrowingRunnable body) throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW selfLoops VERTEX TYPES (P, M, T) EDGE TYPES (K, C, L, I) UPDATE MODE SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "selfLoops");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        body.run();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW selfLoops");
      }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
      void run() throws InterruptedException;
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
      }
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

  /**
   * Issue #9298: the COUNT push-downs (PairHashJoinOp for LSQB Q2, PartitionedTriangleOp for Q3) counted a parallel or a
   * reciprocal KNOWS edge once, where the row pipeline matches one row per relationship. The oracle is the same pattern
   * behind a WITH (the row pipeline), which is the Cypher answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9298CountPushDownMultiplicity extends TestHelper {
    private static final String Q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
    private static final String Q3 = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) "
        + "MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) "
        + "MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)";
    private static final String Q3_VARS = "country, person1, person2, person3, city1, city2, city3";

    private RID[] p;

    @Override
    protected void beginTest() {
      for (final String t : new String[] { "Person", "Comment", "Post", "Country", "City" })
        database.command("sql", "CREATE VERTEX TYPE " + t);
      for (final String e : new String[] { "KNOWS", "HAS_CREATOR", "REPLY_OF", "IS_LOCATED_IN", "IS_PART_OF" })
        database.command("sql", "CREATE EDGE TYPE " + e);
      p = new RID[3];
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          p[i] = database.newVertex("Person").set("id", i).save().getIdentity();
        final RID country = database.newVertex("Country").set("id", 1).save().getIdentity();
        final RID city = database.newVertex("City").set("id", 1).save().getIdentity();
        city.asVertex().newEdge("IS_PART_OF", country);
        for (int i = 0; i < 3; i++)
          p[i].asVertex().newEdge("IS_LOCATED_IN", city);
        final RID comment = database.newVertex("Comment").set("id", 10).save().getIdentity();
        final RID post = database.newVertex("Post").set("id", 20).save().getIdentity();
        comment.asVertex().newEdge("HAS_CREATOR", p[0]);
        comment.asVertex().newEdge("REPLY_OF", post);
        post.asVertex().newEdge("HAS_CREATOR", p[1]);
      });
    }

    private void knows(final int from, final int to) {
      database.transaction(() -> p[from].asVertex().newEdge("KNOWS", p[to]));
    }

    @Test
    void q2ParallelAndReciprocalKnowsCountPerRelationship() throws InterruptedException {
      knows(0, 1);
      assertAllAgree(Q2, "person1, person2, comment, post", 1L);
      knows(0, 1);
      assertAllAgree(Q2, "person1, person2, comment, post", 2L);
      knows(1, 0);
      assertAllAgree(Q2, "person1, person2, comment, post", 3L);
    }

    @Test
    void q3ParallelKnowsCountPerRelationship() throws InterruptedException {
      knows(0, 1);
      knows(1, 2);
      knows(2, 0);
      assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 6L);
      knows(0, 1);
      assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 12L);
    }

    @Test
    void q3ReciprocalKnowsCountPerRelationship() throws InterruptedException {
      knows(0, 1);
      knows(1, 0);
      knows(1, 2);
      knows(2, 0);
      assertAllAgree(Q3, "country, person1, person2, person3, city1, city2, city3", 12L);
    }

    /**
     * A value that occurs more than once in BOTH of the two sorted neighbour ranges that are intersected: the matches are m * n,
     * not min(m, n) or max(m, n). Two parallel edges p0-p1, three p1-p2 and two p2-p0 close 2 * 3 * 2 relationship triples, in each of
     * the six orderings of the three persons.
     */
    @Test
    void q3ParallelEdgesOnEverySideMultiply() throws InterruptedException {
      knowsTimes(0, 1, 2);
      knowsTimes(1, 2, 3);
      knowsTimes(2, 0, 2);
      assertAllAgree(Q3, Q3_VARS, 6L * 2 * 3 * 2);
    }

    /**
     * A SYNCHRONOUS view keeps a commit in its delta overlay and hands out no neighbour view until the next compaction, so the
     * triangle count intersects the per-node neighbour arrays instead of the CSR ranges.
     */
    @Test
    void q3ParallelEdgesOnEverySideMultiplyAfterAnOverlayCommit() throws InterruptedException {
      knowsTimes(0, 1, 1);
      knowsTimes(1, 2, 3);
      knowsTimes(2, 0, 2);
      createView("overlay", "VERTEX TYPES (Person, Comment, Post, Country, City) EDGE TYPES (KNOWS, HAS_CREATOR, REPLY_OF, IS_LOCATED_IN, IS_PART_OF) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
      try {
        final String written = Q3 + " RETURN count(*) AS n";
        assertThat(count(written)).as("before the commit").isEqualTo(6L * 1 * 3 * 2);

        knows(0, 1);
        assertThat(explain(written)).as("the push-down is the plan").contains("COUNT TRIANGLES");
        assertThat(count(Q3 + " WITH " + Q3_VARS + " RETURN count(*) AS n")).as("row pipeline after the commit").isEqualTo(6L * 2 * 3 * 2);
        assertThat(count(written)).as("push-down after the commit").isEqualTo(6L * 2 * 3 * 2);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW overlay");
      }
    }

    private void knowsTimes(final int from, final int to, final int times) {
      database.transaction(() -> {
        for (int i = 0; i < times; i++)
          p[from].asVertex().newEdge("KNOWS", p[to]);
      });
    }

    private void assertAllAgree(final String match, final String withVars, final long expected) throws InterruptedException {
      assertThat(count(match + " WITH " + withVars + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
      final String written = match + " RETURN count(*) AS n";
      assertThat(count(written)).as("no view").isEqualTo(expected);

      createView("narrow", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) PROPERTIES (id) UPDATE MODE OFF");
      assertThat(count(written)).as("narrow view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

      createView("wide", "VERTEX TYPES (Person, Comment, Post, Country, City) EDGE TYPES (KNOWS, HAS_CREATOR, REPLY_OF, IS_LOCATED_IN, IS_PART_OF) PROPERTIES (id) UPDATE MODE OFF");
      assertThat(count(written)).as("wide view").isEqualTo(expected);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
    }

    private void createView(final String name, final String definition) throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
    }

    private String explain(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9539: since #9484 the out-of-view star count ({@code COUNT STAR JOIN}) reads every arm off the edge lists of
   * the central vertices, which is right for light edges but was 2.3x slower than the edge-record scan it replaced: each
   * arm walked the list again and looked up every neighbor to check the far-end label. The count now walks each edge
   * list once for all the arms leaving in its direction and checks the label on the bucket the entry carries.
   * <p>
   * Every count here is checked against the ordinary row pipeline, reached through a {@code sum(1)} that the count
   * push-downs decline, so the push-down is compared with the pipeline it stands in for, not with a number
   * worked out by hand.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9539StarCountEdgeList extends TestHelper {
    private static final String STAR_PLAN = "COUNT STAR JOIN";

    private static final String Q4 = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
        + "(message)<-[:LIKES]-(liker:Person), (message)<-[:REPLY_OF]-(comment:Message)";
    private static final String Q7 = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
        + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message)";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE VERTEX TYPE SubTag EXTENDS Tag");
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE VERTEX TYPE Forum");
      database.command("sql", "CREATE VERTEX TYPE Message");
      database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
      database.command("sql", "CREATE EDGE TYPE HAS_TAG");
      database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
      database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE REPLY_OF");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
    }

    @Test
    void lsqbShapesMatchThePipeline() {
      buildSocialGraph(400);
      assertStarMatchesPipeline(Q4);
      assertStarMatchesPipeline(Q7);
    }

    /**
     * The cost the issue is about, measured in record lookups rather than in time: the count reads the central vertices
     * and their edge lists, and nothing proportional to the number of edges. Before the fix every arm with a far-end label
     * looked up each neighbor, so the lookups grew with the edges (over 4,000 here) instead of with the vertices.
     */
    @Test
    void theCountLooksUpNoNeighbor() {
      final int messages = 200;
      final int edgesPerArm = 5;
      database.transaction(() -> {
        final List<Vertex> tags = new ArrayList<>();
        final List<Vertex> persons = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
          tags.add(database.newVertex("Tag").save());
          persons.add(database.newVertex("Person").save());
        }
        final List<MutableVertex> created = new ArrayList<>();
        for (int i = 0; i < messages; i++)
          created.add(database.newVertex("Message").save());
        for (int i = 0; i < messages; i++) {
          final MutableVertex m = created.get(i);
          for (int k = 0; k < edgesPerArm; k++) {
            m.newEdge("HAS_TAG", tags.get((i + k) % tags.size()));
            m.newEdge("HAS_CREATOR", persons.get((i + k) % persons.size()));
            persons.get((i * 3 + k) % persons.size()).asVertex().modify().newLightEdge("LIKES", m);
            created.get((i + k + 1) % messages).newEdge("REPLY_OF", m);
          }
        }
      });

      final long expected = (long) messages * edgesPerArm * edgesPerArm * edgesPerArm * edgesPerArm;
      assertThat(starPlan(Q4)).contains(STAR_PLAN);

      final long readsBefore = readRecordStat();
      assertThat(countMatch(Q4)).isEqualTo(expected);
      final long reads = readRecordStat() - readsBefore;

      // 4,000 edges hang off the central vertices: a count that looks any of them up cannot stay under this
      final long edges = (long) messages * 4 * edgesPerArm;
      assertThat(reads).as("records read by the star count, for %s central vertices and %s edges", messages, edges)
          .isLessThan(edges / 2);
      assertThat(count(pipelineQuery(Q4))).isEqualTo(expected);
    }

    @Test
    void farEndLabelsFilterOnTheEntryBucket() {
      // HAS_TAG reaches a Tag, a SubTag (a Tag too) and a Forum (not a Tag); LIKES comes from a Person and from a Forum
      database.transaction(() -> {
        for (int i = 0; i < 6; i++) {
          final MutableVertex m = database.newVertex(i % 2 == 0 ? "Message" : "Comment").save();
          m.newEdge("HAS_TAG", database.newVertex("Tag").save());
          m.newEdge("HAS_TAG", database.newVertex("SubTag").save());
          m.newEdge("HAS_TAG", database.newVertex("Forum").save());
          m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
          database.newVertex("Person").save().newLightEdge("LIKES", m);
          database.newVertex("Forum").save().newLightEdge("LIKES", m);
          database.newVertex("Message").save().newEdge("REPLY_OF", m);
        }
      });
      assertThat(countMatch(Q4)).isEqualTo(6 * 2L);
      assertStarMatchesPipeline(Q4);
      assertStarMatchesPipeline("MATCH (:SubTag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
          + "(message)<-[:LIKES]-(liker:Forum)");
      assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Comment)-[:HAS_CREATOR]->(creator:Person), "
          + "(message)<-[:LIKES]-(liker)");
      // a far-end label no vertex type carries reaches nothing
      assertThat(countMatch("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
          + "(message)<-[:LIKES]-(liker:Nobody)")).isZero();
    }

    @Test
    void undirectedAndMultiHopArmsMatchThePipeline() {
      buildSocialGraph(150);
      // a BOTH arm reads the OUT and the IN list of the central vertex
      assertStarMatchesPipeline("MATCH (other:Message)-[:REPLY_OF]-(message:Message)-[:HAS_CREATOR]->(creator:Person), "
          + "(message)-[:HAS_TAG]->(:Tag)");
      assertStarMatchesPipeline("MATCH (other:Message)-[:REPLY_OF]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person)");
      // a multi-hop arm: its intermediate label is checked before the vertex is looked up, its last hop on raw entries
      assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]->(friend:Person), "
          + "(message)<-[:LIKES]-(liker:Person)");
      assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Message)-[:HAS_CREATOR]->(p:Person), "
          + "(message)<-[:LIKES]-(liker:Person)");
      assertStarMatchesPipeline("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person)-[:KNOWS]-(friend:Person)");
    }

    /**
     * A self loop sits in both edge lists of its vertex, and an undirected relationship matches it once (as Neo4j does,
     * and as the row pipeline does): an undirected arm adding the OUT and the IN degree counted it twice. Checked with no
     * view and with a view covering the whole pattern, on a single-hop arm and on both kinds of hop of a multi-hop arm.
     */
    @Test
    void aSelfLoopOnAnUndirectedArmCountsOnce() throws InterruptedException {
      database.transaction(() -> {
        final MutableVertex m1 = database.newVertex("Message").save();
        final MutableVertex m2 = database.newVertex("Message").save();
        final MutableVertex p1 = database.newVertex("Person").save();
        final MutableVertex p2 = database.newVertex("Person").save();
        m1.newEdge("REPLY_OF", m1);
        m1.newEdge("REPLY_OF", m2);
        m1.newEdge("HAS_CREATOR", p1);
        m2.newEdge("HAS_CREATOR", p2);
        p1.newEdge("KNOWS", p1);
        p1.newEdge("KNOWS", p2);
        p2.newLightEdge("KNOWS", p2);
        p1.newLightEdge("LIKES", m1);
        p2.newLightEdge("LIKES", m1);
        p2.newLightEdge("LIKES", m2);
      });
      // m1 reaches itself (once) and m2, m2 reaches m1
      final String singleHop = "MATCH (other:Message)-[:REPLY_OF]-(message:Message), (message)-[:HAS_CREATOR]->(creator:Person)";
      // the friends of p1 are p1 (once) and p2, those of p2 are p1 and p2 (once): m1 2 x 2, m2 1 x 2
      final String lastHop = "MATCH (other:Message)-[:REPLY_OF]-(message:Message), "
          + "(message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]-(friend:Person)";
      // p1 likes m1 and p2 likes m1 and m2: m1 (1 + 2) x 2, m2 (1 + 2) x 1
      final String middleHop = "MATCH (message:Message)-[:HAS_CREATOR]->(creator:Person)-[:KNOWS]-(friend:Person)-[:LIKES]->(liked:Message), "
          + "(message)-[:REPLY_OF]-(other:Message)";

      assertThat(countMatch(singleHop)).isEqualTo(3L);
      assertThat(countMatch(lastHop)).isEqualTo(6L);
      assertThat(countMatch(middleHop)).isEqualTo(9L);
      for (final String match : new String[] { singleHop, lastHop, middleHop })
        assertStarMatchesPipeline(match);

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW selfLoops VERTEX TYPES (Message, Person) "
          + "EDGE TYPES (REPLY_OF, HAS_CREATOR, KNOWS) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "selfLoops");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        assertThat(countMatch(singleHop)).as("with a view").isEqualTo(3L);
        assertThat(countMatch(lastHop)).as("with a view").isEqualTo(6L);
        assertThat(countMatch(middleHop)).as("with a view").isEqualTo(9L);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW selfLoops");
      }
    }

    /**
     * The edge-list primitive itself, below the operator: several filters answered in one walk of a chain of many
     * segments, mixing record and light edges, two edge types, two far-end labels and self loops.
     */
    @Test
    void countIntoAnswersEveryFilterInOneWalkOfAMultiSegmentChain() {
      database.command("sql", "CREATE VERTEX TYPE X");
      database.command("sql", "CREATE VERTEX TYPE Y");
      database.command("sql", "CREATE EDGE TYPE A");
      database.command("sql", "CREATE EDGE TYPE B");

      final long[] expected = new long[5];
      final RID[] hubRid = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("X").save();
        hubRid[0] = hub.getIdentity();
        for (int i = 0; i < 300; i++) {
          final boolean toX = i % 3 == 0;
          final MutableVertex far = database.newVertex(toX ? "X" : "Y").save();
          final String type = i % 4 == 0 ? "B" : "A";
          if (i % 2 == 0)
            hub.newLightEdge(type, far);
          else
            hub.newEdge(type, far);
          if (type.equals("A")) {
            expected[0]++;
            if (toX)
              expected[1]++;
          } else
            expected[2]++;
        }
        for (int i = 0; i < 3; i++) {
          if (i == 0)
            hub.newLightEdge("A", hub);
          else
            hub.newEdge("A", hub);
          // a self loop of A reaches an X (the hub itself)
          expected[0]++;
          expected[1]++;
        }
        // the same filter as the first, leaving the self loops out
        expected[3] = expected[0] - 3;
        // both types at once
        expected[4] = expected[0] + expected[2];
      });

      database.transaction(() -> {
        final DatabaseInternal internal = (DatabaseInternal) database;
        final VertexInternal hub = (VertexInternal) hubRid[0].asVertex(true);
        final EdgeLinkedList out = internal.getGraphEngine().getEdgeHeadChunk(hub, Vertex.DIRECTION.OUT);
        assertThat(((EdgeSegment) database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).getPreviousRID())
            .as("the OUT list spans several segments").isNotNull();

        final EdgeBucketMask a = EdgeBucketMask.of(internal, new String[] { "A" });
        final EdgeBucketMask b = EdgeBucketMask.of(internal, new String[] { "B" });
        final EdgeBucketMask x = EdgeBucketMask.ofBucketIds(database.getSchema().getType("X").getBucketIds(true).stream()
            .mapToInt(Integer::intValue).toArray());
        final long[] counts = new long[5];
        out.countInto(new EdgeBucketMask[] { a, a, b, a, EdgeBucketMask.of(internal, new String[] { "A", "B" }) },
            new EdgeBucketMask[] { null, x, null, null, null }, new boolean[] { false, false, false, true, false }, counts);
        assertThat(counts).containsExactly(expected);

        // the single-filter count behind Vertex.countEdges agrees
        assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "A")).isEqualTo(expected[0]);
        assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "B")).isEqualTo(expected[2]);
        assertThat(hub.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(expected[4]);
        assertThat(hub.countEdges(Vertex.DIRECTION.IN, "A")).isEqualTo(3L);
      });
    }

    @Test
    void anOptionalArmOverAnUndeclaredTypeKeepsTheRow() {
      buildSocialGraph(50);
      final String query = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "OPTIONAL MATCH (message)<-[:NOT_A_TYPE]-(x:Person)";
      assertStarMatchesPipeline(query);
      assertThat(countMatch(query)).isEqualTo(countMatch("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person)"));
    }

    @Test
    void uncommittedEdgesAreCounted() {
      buildSocialGraph(80);
      database.transaction(() -> {
        final MutableVertex m = database.newVertex("Message").save();
        m.newEdge("HAS_TAG", database.newVertex("Tag").save());
        m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
        for (int i = 0; i < 3; i++)
          database.newVertex("Person").save().newLightEdge("LIKES", m);
        database.newVertex("Message").save().newEdge("REPLY_OF", m);
        assertStarMatchesPipeline(Q4);
        assertStarMatchesPipeline(Q7);
      });
    }

    @Test
    void aSuperNodeStripedEdgeListIsCountedOnEveryStripe() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      try {
        final RID[] hub = new RID[1];
        database.transaction(() -> {
          final MutableVertex m = database.newVertex("Message").save();
          m.newEdge("HAS_TAG", database.newVertex("Tag").save());
          m.newEdge("HAS_CREATOR", database.newVertex("Person").save());
          database.newVertex("Message").save().newEdge("REPLY_OF", m);
          hub[0] = m.getIdentity();
        });
        // ONE EDGE PER TRANSACTION, SO THE LIST GROWS CHUNK BY CHUNK AND IS PROMOTED ONCE IT CROSSES THE THRESHOLD
        for (int i = 0; i < 300; i++) {
          final String likerType = i % 3 == 0 ? "Forum" : "Person";
          database.transaction(() -> database.newVertex(likerType).save().newLightEdge("LIKES", hub[0].asVertex()));
        }

        database.transaction(() -> {
          final RID inHead = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
          assertThat(database.lookupByRID(inHead, true)).isInstanceOf(StripeDirectory.class);
        });
        assertThat(countMatch(Q4)).isEqualTo(200L);
        assertStarMatchesPipeline(Q4);

        // an undirected arm over the striped list: two parallel self loops of the hub sit in its classic OUT list and in
        // the stripes of its IN list, and each is matched once
        database.transaction(() -> {
          final MutableVertex hubVertex = hub[0].asVertex().modify();
          hubVertex.newLightEdge("LIKES", hubVertex);
          hubVertex.newEdge("LIKES", hubVertex);
        });
        final String undirected = "MATCH (other)-[:LIKES]-(message:Message), (message)-[:HAS_CREATOR]->(creator:Person)";
        assertThat(countMatch(undirected)).isEqualTo(300L + 2L);
        assertStarMatchesPipeline(undirected);
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    /**
     * Tags, persons and messages (a quarter of them comments, a subtype of Message) wired at random, with record and light
     * edges, edges reaching vertices of the wrong label and messages missing one arm or another.
     */
    private void buildSocialGraph(final int messages) {
      final Random random = new Random(9539);
      database.transaction(() -> {
        final List<Vertex> tags = new ArrayList<>();
        final List<Vertex> persons = new ArrayList<>();
        final List<Vertex> forums = new ArrayList<>();
        for (int i = 0; i < 30; i++)
          tags.add(database.newVertex(i % 5 == 0 ? "SubTag" : "Tag").save());
        for (int i = 0; i < 60; i++)
          persons.add(database.newVertex("Person").save());
        for (int i = 0; i < 10; i++)
          forums.add(database.newVertex("Forum").save());
        for (int i = 0; i < 100; i++)
          persons.get(random.nextInt(persons.size())).asVertex().modify().newEdge("KNOWS", persons.get(random.nextInt(persons.size())));

        final List<Vertex> created = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
          final MutableVertex m = database.newVertex(i % 4 == 0 ? "Comment" : "Message").save();
          created.add(m);
          for (int k = random.nextInt(4); k > 0; k--)
            if (random.nextInt(5) == 0)
              m.newEdge("HAS_TAG", forums.get(random.nextInt(forums.size())));
            else if (random.nextBoolean())
              m.newLightEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
            else
              m.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          if (random.nextInt(10) > 0)
            m.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
          for (int k = random.nextInt(6); k > 0; k--) {
            final List<Vertex> likers = random.nextInt(6) == 0 ? forums : persons;
            likers.get(random.nextInt(likers.size())).asVertex().modify().newLightEdge("LIKES", m);
          }
          if (i > 0 && random.nextBoolean())
            m.newEdge("REPLY_OF", created.get(random.nextInt(i)));
        }
      });
    }

    /** The push-down answers the query and agrees with the row pipeline. */
    private void assertStarMatchesPipeline(final String match) {
      final String pushedDown = match + " RETURN count(*) AS n";
      final String pipeline = pipelineQuery(match);
      assertThat(starPlan(match)).as("plan of %s", pushedDown).contains(STAR_PLAN);
      assertThat(plan(pipeline)).as("plan of %s", pipeline).doesNotContain(STAR_PLAN);
      assertThat(count(pushedDown)).as(pushedDown).isEqualTo(count(pipeline));
    }

    /**
     * The same count through the ordinary row pipeline: no count push-down answers a {@code sum()}. A {@code WITH *} used to
     * be the barrier, until issue #9597 made a pass-through {@code WITH} transparent to the push-downs.
     */
    private static String pipelineQuery(final String match) {
      return match + " RETURN sum(1) AS n";
    }

    private String starPlan(final String match) {
      return plan(match + " RETURN count(*) AS n");
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
      }
    }

    private long countMatch(final String match) {
      return count(match + " RETURN count(*) AS n");
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private long readRecordStat() {
      return (long) database.getStats().get("readRecord");
    }
  }

  /**
   * Issue #9595: a property predicate on a pattern node - {@code (:Message {kind: 'Post'})} or
   * {@code WHERE m.kind = 'Post'} - took a count-only chain off the count push-down, so LSQB Q1 written with the parent type
   * and a {@code kind} property ran 400x slower than with the sub-type labels. A predicate that reads one node only is a
   * per-vertex filter, and the chain and star count operators now apply it on top of the label.
   * <p>
   * Every count is checked against the row pipeline, reached through {@code RETURN sum(1)} which no count push-down
   * answers, on a graph where the {@code kind} property and the sub-type disagree on purpose: some {@code Comment} vertices
   * say {@code kind: 'Post'}, some plain {@code Message} vertices carry either kind and some carry none.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9595CountPushDownPropertyPredicate extends TestHelper {
    private static final String PUSH_DOWN = "Using Count Push-Down";

    private static final String Q1_PROPERTY =
        "MATCH (:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message {kind: 'Post'})"
            + "<-[:CONTAINER_OF]-(:Forum)-[:HAS_MEMBER]->(:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country)";

    @Override
    protected void beginTest() {
      for (final String type : new String[] { "TagClass", "Tag", "Forum", "Person", "City", "Country", "Message" })
        database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE VERTEX TYPE Post EXTENDS Message");
      database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
      for (final String type : new String[] { "HAS_TYPE", "HAS_TAG", "REPLY_OF", "CONTAINER_OF", "HAS_MEMBER", "IS_LOCATED_IN",
          "IS_PART_OF", "HAS_CREATOR", "LIKES" })
        database.command("sql", "CREATE EDGE TYPE " + type);
      buildGraph();
    }

    @Test
    void inlinePropertyOnAChainKeepsThePushDown() {
      assertPushedDownAndExact(Q1_PROPERTY);
      assertPushedDownAndExact("MATCH (:Message {kind: 'Post'})<-[:REPLY_OF]-(:Message {kind: 'Comment'})");
      assertPushedDownAndExact("MATCH (:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message {kind: 'Post'})");
      // the property and the label together, and two properties on one node
      assertPushedDownAndExact("MATCH (:Comment {kind: 'Post'})-[:REPLY_OF]->(m:Message {kind: 'Post', cid: 3})");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})");
    }

    @Test
    void wherePredicatesOnSingleNodesKeepThePushDown() {
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.kind = 'Post' AND c.kind = 'Comment'");
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind = 'Comment' AND p.cid > 20");
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind IS NULL");
      // a missing property makes <> null, which filters the row out like = does
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE c.kind <> 'Post'");
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE NOT (c.kind = 'Post') AND p.kind IS NOT NULL");
      assertPushedDownAndExact("MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.kind STARTS WITH 'Po' OR p.cid < 10");
      assertPushedDownAndExact(
          "MATCH (:TagClass)<-[:HAS_TYPE]-(:Tag)<-[:HAS_TAG]-(c:Message)-[:REPLY_OF]->(p:Message)<-[:CONTAINER_OF]-(:Forum) "
              + "WHERE p.kind = 'Post' AND c.kind = 'Comment'");
    }

    @Test
    void parametersAreResolvedPerExecution() {
      for (final String kind : new String[] { "Post", "Comment", "None" }) {
        final Map<String, Object> params = Map.of("kind", kind);
        final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: $kind})-[:REPLY_OF]->(:Message)";
        assertThat(plan(match + " RETURN count(*) AS n", params)).contains(PUSH_DOWN);
        assertThat(count(match + " RETURN count(*) AS n", params)).as("%s with %s", match, kind)
            .isEqualTo(count(match + " RETURN sum(1) AS n", params));

        final String where = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:REPLY_OF]->(:Message) WHERE m.kind = $kind";
        assertThat(count(where + " RETURN count(*) AS n", params)).as("%s with %s", where, kind)
            .isEqualTo(count(where + " RETURN sum(1) AS n", params));
      }
    }

    @Test
    void anInequalityAndAPropertyPredicateTogether() {
      assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)<-[:REPLY_OF]-(c:Message)-[:HAS_TAG]->(t2:Tag) "
          + "WHERE t1 <> t2 AND c.kind = 'Comment'");
      assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})<-[:REPLY_OF]-(c:Message)-[:HAS_TAG]->(t2:Tag) "
          + "WHERE t1 <> t2");
      assertPushedDownAndExact("MATCH (t1:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_TAG]->(t2:Tag) WHERE t1 <> t2 AND m.kind = 'Post'");
    }

    @Test
    void starArmsAndCentralNodeCarryPredicates() {
      // LSQB Q4 split into three MATCH clauses, with the comment written as a Message of kind 'Comment'
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "MATCH (message)<-[:LIKES]-(liker:Person) MATCH (message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message {kind: 'Post'})-[:HAS_CREATOR]->(creator:Person), "
          + "(message)<-[:LIKES]-(liker:Person), (message)<-[:REPLY_OF]-(comment:Message)");
      // Q7 with a filtered optional arm: a message whose replies all fail the filter still keeps its row
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:Person) OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message {kind: 'Comment'})");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(creator:Person) "
          + "MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE comment.kind = 'Comment' AND message.cid > 5");
    }

    @Test
    void anOptionalClauseMatchesAsAWhole() {
      // Found while extending the star count: an OPTIONAL clause yielding two arms was counted as two independent optional
      // arms, max(1, d1) * max(1, d2), where the clause contributes one row as soon as either arm reaches nothing
      for (final String match : new String[] {
          "MATCH (m:Message) OPTIONAL MATCH (c:Message)-[:REPLY_OF]->(m)-[:HAS_TAG]->(t:Tag)",
          "MATCH (m:Message) OPTIONAL MATCH (m)-[:HAS_TAG]->(t:Tag), (m)<-[:LIKES]-(p:Person)" }) {
        assertThat(count(match + " RETURN count(*) AS n", Map.of())).as(match)
            .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));
      }
    }

    @Test
    void anOptionalClauseFilterIsAConditionOfItsArm() {
      // a WHERE of an OPTIONAL clause on the reached node keeps the row of a message whose replies all fail it
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
          + "OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE comment.kind = 'Comment'");
      // on the central node it decides whether the arm matched, not whether the row exists: left to the pipeline
      final String central = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
          + "OPTIONAL MATCH (message)<-[:REPLY_OF]-(comment:Message) WHERE message.kind = 'Post'";
      assertThat(count(central + " RETURN count(*) AS n", Map.of())).isEqualTo(count(central + " RETURN sum(1) AS n", Map.of()));
      final String inline = "MATCH (:Tag)<-[:HAS_TAG]-(message:Message) "
          + "OPTIONAL MATCH (message {kind: 'Post'})<-[:REPLY_OF]-(comment:Message)";
      assertThat(count(inline + " RETURN count(*) AS n", Map.of())).isEqualTo(count(inline + " RETURN sum(1) AS n", Map.of()));
    }

    @Test
    void theSameAnswersOverAGraphAnalyticalView() throws InterruptedException {
      withView(() -> {
        inlinePropertyOnAChainKeepsThePushDown();
        wherePredicatesOnSingleNodesKeepThePushDown();
        parametersAreResolvedPerExecution();
        anInequalityAndAPropertyPredicateTogether();
        starArmsAndCentralNodeCarryPredicates();
        anOptionalClauseMatchesAsAWhole();
        anOptionalClauseFilterIsAConditionOfItsArm();
      });
    }

    @Test
    void aCorrelatedBodyFiltersTheReachedNodes() throws InterruptedException {
      final String query = "MATCH (p:Message) RETURN p.cid AS cid, COUNT { (p)<-[:REPLY_OF]-(:Message {kind: 'Comment'}) } AS n "
          + "ORDER BY cid";
      final String pipeline = "MATCH (p:Message) OPTIONAL MATCH (p)<-[:REPLY_OF]-(c:Message) WITH p, c "
          + "RETURN p.cid AS cid, sum(CASE WHEN c.kind = 'Comment' THEN 1 ELSE 0 END) AS n ORDER BY cid";
      assertThat(rows(query)).isEqualTo(rows(pipeline));
      withView(() -> assertThat(rows(query)).isEqualTo(rows(pipeline)));
    }

    @Test
    void predicatesReadingTwoNodesStayOnThePipeline() {
      final String match = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE p.cid < c.cid";
      assertThat(plan(match + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);
      assertThat(count(match + " RETURN count(*) AS n", Map.of())).isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));

      // an inline value read off another node of the pattern is not a constant
      final String inline = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message {kind: p.kind})";
      assertThat(plan(inline + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);
      assertThat(count(inline + " RETURN count(*) AS n", Map.of())).isEqualTo(count(inline + " RETURN sum(1) AS n", Map.of()));

      // a non-deterministic predicate is evaluated per row, not per vertex
      final String random = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE rand() < 2";
      assertThat(plan(random + " RETURN count(*) AS n", Map.of())).doesNotContain(PUSH_DOWN);

      // so is a function that may resolve to a DEFINE FUNCTION body, which can read anything on every call
      database.command("sql", "DEFINE FUNCTION kinds.twice \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
      final String custom = "MATCH (p:Message)<-[:REPLY_OF]-(c:Message) WHERE kinds.twice(p.cid) > 40";
      // run as commands: a call that may reach a DEFINE FUNCTION body is not a read-only query
      try (final ResultSet pushed = database.command("opencypher", "EXPLAIN " + custom + " RETURN count(*) AS n")) {
        assertThat(pushed.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("")).doesNotContain(PUSH_DOWN);
      }
      try (final ResultSet counted = database.command("opencypher", custom + " RETURN count(*) AS n");
          final ResultSet summed = database.command("opencypher", custom + " RETURN sum(1) AS n")) {
        assertThat(((Number) counted.next().getProperty("n")).longValue())
            .isEqualTo(((Number) summed.next().getProperty("n")).longValue());
      }
    }

    @Test
    void aSelfLoopOnAnUndirectedHopIsCountedOnceUnderAPredicate() throws InterruptedException {
      database.transaction(() -> {
        final MutableVertex loop = database.newVertex("Message").set("cid", 1000).set("kind", "Comment").save();
        loop.newEdge("REPLY_OF", loop);
      });
      final String[] matches = { "MATCH (a:Message {kind: 'Comment'})-[:REPLY_OF]-(b:Message)",
          "MATCH (a:Message)-[:REPLY_OF]-(b:Message) WHERE a.kind = 'Comment' AND b.cid >= 1000",
          "MATCH (:Tag)<-[:HAS_TAG]-(a:Message)-[:REPLY_OF]-(b:Message {kind: 'Comment'})" };
      for (final String match : matches)
        assertPushedDownAndExact(match);
      withView(() -> {
        for (final String match : matches)
          assertPushedDownAndExact(match);
      });
    }

    @Test
    void numbersCompareByValueAcrossTypes() {
      // cid is stored as an Integer: an inline Long, a Double and a parameter of another type all compare by value
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: 3})");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: 3.0})");
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message) WHERE m.cid = 3.0");
      for (final Object cid : new Object[] { 3, 3L, 3.0d, 3.0f, "3" }) {
        final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {cid: $cid})";
        assertThat(count(match + " RETURN count(*) AS n", Map.of("cid", cid))).as("cid %s (%s)", cid, cid.getClass().getSimpleName())
            .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of("cid", cid)));
      }
    }

    @Test
    void uncommittedChangesOfTheTransactionAreSeen() throws InterruptedException {
      // a transaction with changes of its own does not use the view, and the records it reads carry those changes
      final String match = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: 'Post'})-[:REPLY_OF]->(:Message)";
      final Runnable check = () -> {
        database.begin();
        try {
          final long before = count(match + " RETURN count(*) AS n", Map.of());
          database.command("opencypher", "MATCH (m:Message {kind: 'Post'}) SET m.kind = 'Moved'");
          assertThat(count(match + " RETURN count(*) AS n", Map.of())).isZero()
              .isEqualTo(count(match + " RETURN sum(1) AS n", Map.of()));
          database.command("opencypher", "MATCH (m:Message {kind: 'Moved'}) SET m.kind = 'Post'");
          assertThat(count(match + " RETURN count(*) AS n", Map.of())).isEqualTo(before).isPositive();
        } finally {
          database.rollback();
        }
      };
      check.run();
      withView(check);
    }

    @Test
    void anInlineNullMatchesNothing() {
      // {kind: null} means kind = null, which is never true, not "kind is missing"
      assertPushedDownAndExact("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: null})");
      assertThat(count("MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: null}) RETURN count(*) AS n", Map.of())).isZero();
      final String parameter = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message {kind: $kind}) RETURN count(*) AS n";
      final Map<String, Object> nullKind = new HashMap<>();
      nullKind.put("kind", null);
      assertThat(count(parameter, nullKind)).isZero();
    }

    @Test
    void thePredicateIsEvaluatedOncePerDistinctVertex() throws InterruptedException {
      // Every Comment-kind message is reached once per tag it carries; the filter reads it once
      final String match = "MATCH (:Tag)<-[:HAS_TAG]-(c:Message {kind: 'Comment'})-[:REPLY_OF]->(:Message)";
      final long messages = count("MATCH (m:Message) RETURN count(m) AS n", Map.of());
      withView(() -> {
        final long before = readRecordStat();
        count(match + " RETURN count(*) AS n", Map.of());
        assertThat(readRecordStat() - before).isLessThanOrEqualTo(messages);
      });
    }

    /**
     * A small social graph whose {@code kind} property does not follow the sub-types: the filter has to read the
     * property, not the type.
     */
    private void buildGraph() {
      final Random random = new Random(9595);
      database.transaction(() -> {
        final List<Vertex> tagClasses = create("TagClass", 4);
        final List<Vertex> tags = create("Tag", 25);
        final List<Vertex> forums = create("Forum", 8);
        final List<Vertex> persons = create("Person", 30);
        final List<Vertex> cities = create("City", 6);
        final List<Vertex> countries = create("Country", 3);
        for (final Vertex tag : tags)
          tag.modify().newEdge("HAS_TYPE", pick(random, tagClasses));
        for (final Vertex city : cities)
          city.modify().newEdge("IS_PART_OF", pick(random, countries));
        for (final Vertex person : persons)
          person.modify().newEdge("IS_LOCATED_IN", pick(random, cities));
        for (final Vertex forum : forums)
          for (int k = random.nextInt(6); k > 0; k--)
            forum.modify().newEdge("HAS_MEMBER", pick(random, persons));

        final List<Vertex> messages = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
          final int shape = random.nextInt(10);
          final String type = shape < 4 ? "Post" : shape < 8 ? "Comment" : "Message";
          final MutableVertex m = database.newVertex(type);
          m.set("cid", i);
          if (shape < 3)
            m.set("kind", "Post");
          else if (shape == 3)
            m.set("kind", "Comment"); // a Post saying it is a comment
          else if (shape < 7)
            m.set("kind", "Comment");
          else if (shape == 7)
            m.set("kind", "Post"); // a Comment saying it is a post
          else if (shape == 8)
            m.set("kind", random.nextBoolean() ? "Post" : "Comment");
          // shape 9: no kind at all
          m.save();
          messages.add(m);
          for (int k = random.nextInt(3); k > 0; k--)
            m.newEdge("HAS_TAG", pick(random, tags));
          if (random.nextInt(8) > 0)
            m.newEdge("HAS_CREATOR", pick(random, persons));
          for (int k = random.nextInt(4); k > 0; k--)
            pick(random, persons).modify().newEdge("LIKES", m);
          if (i > 0 && random.nextInt(4) > 0)
            m.newEdge("REPLY_OF", messages.get(random.nextInt(i)));
          if (random.nextInt(3) == 0)
            pick(random, forums).modify().newEdge("CONTAINER_OF", m);
        }
      });
    }

    private List<Vertex> create(final String type, final int count) {
      final List<Vertex> vertices = new ArrayList<>(count);
      for (int i = 0; i < count; i++)
        vertices.add(database.newVertex(type).set("cid", i).save());
      return vertices;
    }

    private static Vertex pick(final Random random, final List<Vertex> vertices) {
      return vertices.get(random.nextInt(vertices.size()));
    }

    private void assertPushedDownAndExact(final String match) {
      final String pushedDown = match + " RETURN count(*) AS n";
      assertThat(plan(pushedDown, Map.of())).as("plan of %s", pushedDown).contains(PUSH_DOWN);
      final long expected = count(match + " RETURN sum(1) AS n", Map.of());
      assertThat(count(pushedDown, Map.of())).as(pushedDown).isEqualTo(expected);
    }

    private void withView(final Runnable check) throws InterruptedException {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW social VERTEX TYPES (TagClass, Tag, Forum, Person, City, Country, "
          + "Message, Post, Comment) EDGE TYPES (HAS_TYPE, HAS_TAG, REPLY_OF, CONTAINER_OF, HAS_MEMBER, IS_LOCATED_IN, "
          + "IS_PART_OF, HAS_CREATOR, LIKES) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "social");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        check.run();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW social");
      }
    }

    private String plan(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    private long count(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        final Object value = rs.next().getProperty("n");
        return value == null ? 0L : ((Number) value).longValue();
      }
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final var row = rs.next();
          rows.add(row.getProperty("cid") + ":" + ((Number) row.getProperty("n")).longValue());
        }
      }
      return rows;
    }

    private long readRecordStat() {
      return (long) database.getStats().get("readRecord");
    }
  }

  /**
   * Issue #9599: the same MATCH written in another order took another plan. LSQB Q2 starts from the KNOWS pair and the count
   * push-down answered it with a pair join; written from the post, as four one-hop patterns, no count push-down recognized it,
   * and it ran 17x slower. The detectors now read the MATCH as the pattern graph it describes: a cycle is split into a probe
   * hop and the chain closing it by the statistics, a chain cut into parts is joined again, and a variable's labels hold at
   * every position it is written at. So every spelling gets the same operator.
   * <p>
   * Every spelling is checked against the row pipeline, reached through a {@code sum(1)} that no push-down takes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9599PatternOrderCountPushDown extends TestHelper {
    /** The four relationships of LSQB Q2: from, type, to, undirected. */
    private static final String[][] Q2_PARTS = {
        { "person1", "KNOWS", "person2", "-" },
        { "comment", "HAS_CREATOR", "person1", ">" },
        { "comment", "REPLY_OF", "post", ">" },
        { "post", "HAS_CREATOR", "person2", ">" } };
    private static final String[][] Q2_LABELS = { { "person1", "Person" }, { "person2", "Person" }, { "comment", "Comment" },
        { "post", "Post" } };

    /** The three relationships of LSQB Q8's chain. */
    private static final String[][] Q8_PARTS = {
        { "message", "HAS_TAG", "tag1", ">" },
        { "comment", "REPLY_OF", "message", ">" },
        { "comment", "HAS_TAG", "tag2", ">" } };
    private static final String[][] Q8_LABELS = { { "tag1", "Tag" }, { "tag2", "Tag" }, { "comment", "Comment" },
        { "message", "Message" } };

    private static final String Q2 = "MATCH (person1:Person)-[:KNOWS]-(person2:Person), "
        + "(person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(person2)";
    private static final String Q2_REORDERED = "MATCH (post:Post)-[:HAS_CREATOR]->(person2:Person), (comment:Comment)-[:REPLY_OF]->(post), "
        + "(comment)-[:HAS_CREATOR]->(person1:Person), (person2)-[:KNOWS]-(person1)";

    @Override
    protected void beginTest() {
      for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE VERTEX TYPE Message",
          "CREATE VERTEX TYPE Post EXTENDS Message", "CREATE VERTEX TYPE Comment EXTENDS Message", "CREATE EDGE TYPE KNOWS",
          "CREATE EDGE TYPE HAS_CREATOR", "CREATE EDGE TYPE REPLY_OF", "CREATE EDGE TYPE HAS_TAG" })
        database.command("sql", ddl);
    }

    @Test
    void theReportedReorderingTakesTheSamePairJoin() {
      populate(1);
      final String plan = pushDown(Q2 + " RETURN count(*) AS n");
      assertThat(plan).contains("COUNT PAIR JOIN").contains("probe: KNOWS");
      assertThat(pushDown(Q2_REORDERED + " RETURN count(*) AS n")).isEqualTo(plan);
      assertThat(count(Q2_REORDERED + " RETURN count(*) AS n")).isEqualTo(count(Q2 + " RETURN sum(1) AS n"));
    }

    /** Every order of the four one-hop parts, each part read from either end and the labels at any of their positions. */
    @Test
    void everySpellingOfTheCycleGetsTheSameOperatorAndCount() {
      populate(2);
      final long expected = count(Q2 + " RETURN sum(1) AS n");
      final String plan = pushDown(Q2 + " RETURN count(*) AS n");
      final Random random = new Random(2);
      for (final List<String[]> order : permutations(Q2_PARTS)) {
        final String match = spell(order, Q2_LABELS, random);
        assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
      }
    }

    @Test
    void aViewAnswersEverySpellingAlike() {
      populate(3);
      final long expected = count(Q2 + " RETURN sum(1) AS n");
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("q2").build();
      try {
        final String plan = pushDown(Q2 + " RETURN count(*) AS n");
        final Random random = new Random(3);
        for (final List<String[]> order : permutations(Q2_PARTS)) {
          final String match = spell(order, Q2_LABELS, random);
          assertThat(pushDown(match + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
          assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
        }
      } finally {
        view.drop();
      }
    }

    /** A chain cut into parts is the chain: Q8 written as three patterns, in any order, takes the anti-join. */
    @Test
    void aChainCutIntoPartsIsJoinedAgain() {
      populate(4);
      final String where = " WHERE NOT (comment)-[:HAS_TAG]->(tag1) AND tag1 <> tag2";
      final String q8 = "MATCH (tag1:Tag)<-[:HAS_TAG]-(message:Message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(tag2:Tag)";
      final long expected = count(q8 + where + " RETURN sum(1) AS n");
      final String plan = pushDown(q8 + where + " RETURN count(*) AS n");
      assertThat(plan).contains("COUNT ANTI-JOIN CHAIN");

      final Random random = new Random(4);
      for (final List<String[]> order : permutations(Q8_PARTS)) {
        final String match = spell(order, Q8_LABELS, random);
        assertThat(pushDown(match + where + " RETURN count(*) AS n")).as(match).isEqualTo(plan);
        assertThat(count(match + where + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
        // without the anti-join it is a plain chain
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
      }
    }

    /**
     * Over a unidirectional type only the outgoing side is stored: a chain written as outgoing parts is joined, but never read
     * from its other end, which would walk the incoming side and count nothing.
     */
    @Test
    void aChainOverAUnidirectionalTypeIsReadOnlyAlongItsStoredSide() {
      database.command("sql", "CREATE EDGE TYPE FOLLOWS UNIDIRECTIONAL");
      final Random random = new Random(6);
      database.transaction(() -> {
        final List<MutableVertex> persons = new ArrayList<>();
        for (int i = 0; i < 40; i++)
          persons.add(database.newVertex("Person").save());
        for (final MutableVertex person : persons)
          for (int k = random.nextInt(4); k > 0; k--)
            person.newEdge("FOLLOWS", persons.get(random.nextInt(persons.size())));
      });
      for (final String match : new String[] {
          "MATCH (a:Person)-[:FOLLOWS]->(b:Person), (b)-[:FOLLOWS]->(c:Person)",
          "MATCH (b)-[:FOLLOWS]->(c:Person), (a:Person)-[:FOLLOWS]->(b:Person)",
          "MATCH (b:Person)-[:FOLLOWS]->(c:Person), (c)-[:FOLLOWS]->(d:Person), (a:Person)-[:FOLLOWS]->(b)" }) {
        final long expected = count(match + " RETURN sum(1) AS n");
        assertThat(expected).isPositive();
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(expected);
      }
    }

    /**
     * Shapes the graph reading does not apply to, or applies to and must still decline, keep the pipeline's count: a variable
     * written with two different labels, a cycle whose two hops can bind the same relationship, a cycle with no named node to
     * probe between.
     */
    @Test
    void shapesThatAreNotOneCycleOrOneChainStayExact() {
      populate(5);
      for (final String match : new String[] {
          // comment is a Comment in one part and a Message in another
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (comment:Comment)-[:HAS_CREATOR]->(person1), "
              + "(comment:Message)-[:REPLY_OF]->(post:Post), (post)-[:HAS_CREATOR]->(person2)",
          // a KNOWS triangle written as three parts: two hops can bind the same KNOWS edge
          "MATCH (a:Person)-[:KNOWS]-(b:Person), (b)-[:KNOWS]-(c:Person), (c)-[:KNOWS]-(a)",
          // a square of KNOWS, the pair join's shape, where relationship uniqueness is what the count is about
          "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(c:Person), (c)-[:KNOWS]->(d:Person), (d)-[:KNOWS]->(a)",
          // a cycle and a hop hanging off it
          "MATCH (person1:Person)-[:KNOWS]-(person2:Person), (person1)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post), "
              + "(post)-[:HAS_CREATOR]->(person2), (post)-[:HAS_TAG]->(t:Tag)" })
        assertThat(count(match + " RETURN count(*) AS n")).as(match).isEqualTo(count(match + " RETURN sum(1) AS n"));
    }

    /**
     * People who know each other, posts and comments each with a creator, comments replying to posts or comments, tags on the
     * messages. KNOWS is the smallest relation, as in LSQB, so it is the cheapest probe.
     */
    private void populate(final long seed) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        final List<MutableVertex> persons = new ArrayList<>();
        final List<MutableVertex> tags = new ArrayList<>();
        final List<MutableVertex> messages = new ArrayList<>();
        for (int i = 0; i < 30; i++)
          persons.add(database.newVertex("Person").save());
        for (int i = 0; i < 8; i++)
          tags.add(database.newVertex("Tag").save());
        for (final MutableVertex person : persons)
          for (int k = random.nextInt(3); k > 0; k--)
            person.newEdge("KNOWS", persons.get(random.nextInt(persons.size())));
        for (int i = 0; i < 300; i++) {
          final boolean post = i < 80;
          final MutableVertex message = database.newVertex(post ? "Post" : "Comment").save();
          message.newEdge("HAS_CREATOR", persons.get(random.nextInt(persons.size())));
          for (int t = random.nextInt(3); t > 0; t--)
            message.newEdge("HAS_TAG", tags.get(random.nextInt(tags.size())));
          if (!post)
            message.newEdge("REPLY_OF", messages.get(random.nextInt(messages.size())));
          messages.add(message);
        }
      });
    }

    /**
     * The parts as one MATCH, each read from a random end, every variable labelled at one random position of the ones it is
     * written at.
     */
    private static String spell(final List<String[]> parts, final String[][] labels, final Random random) {
      final List<String[]> oriented = new ArrayList<>(parts.size());
      for (final String[] part : parts)
        oriented.add(random.nextBoolean() ? part : new String[] { part[2], part[1], part[0], part[3].equals(">") ? "<" : part[3] });

      final List<int[]> positions = new ArrayList<>();
      final String[][] nodeLabels = new String[oriented.size()][2];
      for (final String[] variable : labels) {
        positions.clear();
        for (int p = 0; p < oriented.size(); p++)
          for (int side = 0; side < 2; side++)
            if (oriented.get(p)[side * 2].equals(variable[0]))
              positions.add(new int[] { p, side });
        final int[] at = positions.get(random.nextInt(positions.size()));
        nodeLabels[at[0]][at[1]] = variable[1];
      }

      final StringBuilder match = new StringBuilder("MATCH ");
      for (int p = 0; p < oriented.size(); p++) {
        final String[] part = oriented.get(p);
        if (p > 0)
          match.append(", ");
        match.append('(').append(part[0]).append(nodeLabels[p][0] != null ? ":" + nodeLabels[p][0] : "").append(')');
        match.append(switch (part[3]) {
          case ">" -> "-[:" + part[1] + "]->";
          case "<" -> "<-[:" + part[1] + "]-";
          default -> "-[:" + part[1] + "]-";
        });
        match.append('(').append(part[2]).append(nodeLabels[p][1] != null ? ":" + nodeLabels[p][1] : "").append(')');
      }
      return match.toString();
    }

    private static List<List<String[]>> permutations(final String[][] parts) {
      final List<List<String[]>> result = new ArrayList<>();
      permute(new ArrayList<>(List.of(parts)), 0, result);
      return result;
    }

    private static void permute(final List<String[]> parts, final int from, final List<List<String[]>> result) {
      if (from == parts.size()) {
        result.add(new ArrayList<>(parts));
        return;
      }
      for (int i = from; i < parts.size(); i++) {
        Collections.swap(parts, from, i);
        permute(parts, from + 1, result);
        Collections.swap(parts, from, i);
      }
    }

    /** The count push-down part of the plan, which has to be there. */
    private String pushDown(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        final String plan = rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
        assertThat(plan).as("plan of %s", query).contains("Using Count Push-Down");
        final int start = plan.indexOf("+ COUNT");
        final int end = plan.indexOf("\nUsing ", start);
        return (end > 0 ? plan.substring(start, end) : plan.substring(start)).trim();
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9600: {@code MATCH ()-[e:KNOWS]->() RETURN count(e)} and {@code MATCH ()-[e]->() RETURN count(e)} walked every edge
   * of the graph, and {@code MATCH (n) RETURN count(n)} loaded every vertex. A relationship counted by its own variable is
   * {@code count(*)} when a non-optional MATCH binds it, one hop between two unconstrained nodes is the number of edges of its
   * types (read off a view's totals or one walk of the edge lists, never off the edge type's record counter, which a light edge
   * is not in), and an unlabelled node is every vertex (the sum of the vertex types' counters).
   * <p>
   * Every count is checked against the row pipeline, reached through a {@code sum} that no push-down takes, and against a
   * count taken off the vertices' edge lists by hand.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9600EdgeCountPushDown extends TestHelper {
    private static final String EDGE_COUNT = "COUNT EDGES";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE VERTEX TYPE Student EXTENDS Person");
      database.command("sql", "CREATE VERTEX TYPE City");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
      database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS");
      database.command("sql", "CREATE EDGE TYPE LIVES_IN");
      database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT");
    }

    @Test
    void theReportedQueriesArePushedDownAndExact() {
      buildGraph(7, 300);
      assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      assertVertexCount();
    }

    @Test
    void everyDirectedAndUndirectedSpellingCountsLikeThePipeline() {
      buildGraph(11, 250);
      for (final String count : new String[] { "count(e)", "count(*)" }) {
        assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.OUT);
        assertEdgeCount("MATCH (a)<-[e:KNOWS]-(b) RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.OUT);
        assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.BOTH);
        assertEdgeCount("MATCH ()-[e]-() RETURN " + count + " AS n", null, Vertex.DIRECTION.BOTH);
        assertEdgeCount("MATCH ()-[e:KNOWS_WELL]->() RETURN " + count + " AS n", "KNOWS_WELL", Vertex.DIRECTION.OUT);
        // a light edge has no record: the type's counter would miss every one of them
        assertEdgeCount("MATCH ()-[e:LIKES]->() RETURN " + count + " AS n", "LIKES", Vertex.DIRECTION.OUT);
      }
    }

    /**
     * A unidirectional type stores its edges on the outgoing side only, which is where a directed count reads them; its
     * incoming side is not stored, so an undirected hop over it is left to the row pipeline, which answers that side.
     */
    @Test
    void aUnidirectionalTypeIsCountedFromItsSources() {
      database.command("sql", "CREATE EDGE TYPE FOLLOWS UNIDIRECTIONAL");
      buildGraph(31, 200);
      final Random random = new Random(31);
      database.transaction(() -> {
        final List<Vertex> people = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext(); )
          people.add(it.next().asVertex());
        for (final Vertex person : people)
          for (int k = random.nextInt(3); k > 0; k--)
            person.modify().newEdge("FOLLOWS", people.get(random.nextInt(people.size())));
      });
      assertEdgeCount("MATCH ()-[e:FOLLOWS]->() RETURN count(e) AS n", "FOLLOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      assertPipelineOnly("MATCH ()-[e:FOLLOWS]-() RETURN count(e) AS n", byHand("FOLLOWS", Vertex.DIRECTION.BOTH));
      assertPipelineOnly("MATCH ()-[e]-() RETURN count(e) AS n", byHand(null, Vertex.DIRECTION.BOTH));
    }

    @Test
    void alternativesCountEachEdgeOnce() {
      buildGraph(13, 200);
      final long knows = byHand("KNOWS", Vertex.DIRECTION.OUT);
      final long likes = byHand("LIKES", Vertex.DIRECTION.OUT);
      assertThat(pushedDown("MATCH ()-[e:KNOWS|LIKES]->() RETURN count(e) AS n")).isEqualTo(knows + likes);
      // KNOWS_WELL is a KNOWS: naming both is still the KNOWS family once
      assertThat(pushedDown("MATCH ()-[e:KNOWS|KNOWS_WELL]->() RETURN count(e) AS n")).isEqualTo(knows);
      assertThat(pushedDown("MATCH ()-[e:KNOWS_WELL|KNOWS|KNOWS]->() RETURN count(e) AS n")).isEqualTo(knows);
      // a name that is no edge type matches nothing
      assertThat(pushedDown("MATCH ()-[e:KNOWS|NOT_A_TYPE]->() RETURN count(e) AS n")).isEqualTo(knows);
    }

    @Test
    void shapesTheOperatorDoesNotCountStayExact() {
      buildGraph(17, 200);
      // a name that is no edge type, alone, and a vertex type written as a relationship type
      assertPipelineOnly("MATCH ()-[e:NOT_A_TYPE]->() RETURN count(e) AS n", 0L);
      assertPipelineOnly("MATCH ()-[e:Person]->() RETURN count(e) AS n", 0L);
      // a node named at both ends keeps the self loops only
      assertPipelineOnly("MATCH (a)-[e:KNOWS]->(a) RETURN count(e) AS n", null);
      // a labelled end is a filter
      assertPipelineOnly("MATCH (:Student)-[e:KNOWS]->() RETURN count(e) AS n", null);
      // a relationship counted DISTINCT, or bound by an OPTIONAL MATCH where it can be null, is not count(*)
      assertPipelineOnly("MATCH ()-[e:KNOWS]->() RETURN count(DISTINCT e) AS n", null);
      assertPipelineOnly("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS]->() RETURN count(e) AS n", null);
      assertPipelineOnly("OPTIONAL MATCH (n:Student {age: 99}) RETURN count(n) AS n", 0L);
      // a property filter on the relationship, as a map or as an inline WHERE that naming it allows
      assertPipelineOnly("MATCH ()-[e:KNOWS {since: 3}]->() RETURN count(e) AS n", null);
      assertPipelineOnly("MATCH ()-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n", null);
      assertThat(plan("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n")).doesNotContain("COUNT CHAIN");
      assertThat(count("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n"))
          .isEqualTo(count("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN sum(1) AS n"));
    }

    @Test
    void uncommittedEdgesAndVerticesAreCounted() {
      buildGraph(19, 120);
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Person").save();
        final MutableVertex b = database.newVertex("Student").save();
        a.newEdge("KNOWS", b);
        b.newEdge("KNOWS_WELL", a);
        a.newEdge("KNOWS", a);
        a.newLightEdge("KNOWS", b);
        a.newLightEdge("LIKES", b);
        // and edges deleted in the same transaction
        final List<Record> doomed = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType("KNOWS", true); it.hasNext() && doomed.size() < 4; )
          doomed.add(it.next());
        for (final Record edge : doomed)
          edge.asEdge().delete();
        assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
        assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
        assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
        assertVertexCount();
      });
    }

    @Test
    void aViewAnswersTheSameCountsBeforeAndAfterItTakesChanges() {
      buildGraph(23, 250);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("counts")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
      try {
        assertThat(view.isReady()).isTrue();
        assertThat(view.hasPendingChanges()).isFalse();
        assertAllEdgeCounts();

        // the totals themselves: an unknown type holds nothing, a sub-type listed with its super-type is counted once
        assertThat(view.countAllEdges("NOT_A_TYPE")).isZero();
        assertThat(view.countAllEdges("KNOWS", "KNOWS_WELL")).isEqualTo(view.countAllEdges("KNOWS"));
        assertThat(view.countAllEdges("KNOWS")).isEqualTo(byHand("KNOWS", Vertex.DIRECTION.OUT));
        assertThat(view.countAllEdges()).isEqualTo(byHand(null, Vertex.DIRECTION.OUT));

        // committed after the build: the totals of the slices are no longer the graph, the per-node counts are. Record edges
        // only: a light edge created after the build never reaches the overlay, whatever reads the view (issue #9572)
        database.transaction(() -> {
          final MutableVertex a = database.newVertex("Person").save();
          final MutableVertex b = database.newVertex("Person").save();
          a.newEdge("KNOWS", b);
          a.newEdge("KNOWS", a);
          b.newEdge("KNOWS_WELL", a);
          b.newEdge("LIVES_IN", database.newVertex("City").save());
        });
        assertThat(view.hasPendingChanges()).isTrue();
        assertAllEdgeCounts();

        // and edges deleted after the build: a regular one, a sub-type one and a self loop
        database.transaction(() -> {
          final List<Record> edges = new ArrayList<>();
          for (final Iterator<Record> it = database.iterateType("KNOWS", true); it.hasNext() && edges.size() < 6; )
            edges.add(it.next());
          for (final Record edge : edges)
            edge.asEdge().delete();
        });
        assertAllEdgeCounts();
      } finally {
        view.drop();
      }
    }

    /**
     * A view that leaves a vertex type out does not hold the edges that reach it: the count is not read off it, and the walk of
     * the edge lists answers instead.
     */
    @Test
    void aViewOverSomeVertexTypesIsNotUsedForTheCount() {
      buildGraph(37, 200);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("people").withVertexTypes("Person", "Student")
          .build();
      try {
        assertThat(view.isReady()).isTrue();
        assertThat(view.coversVertexType(null)).isFalse();
        // LIVES_IN reaches the cities, which the view does not map
        assertEdgeCount("MATCH ()-[e:LIVES_IN]->() RETURN count(e) AS n", "LIVES_IN", Vertex.DIRECTION.OUT);
        assertAllEdgeCounts();
      } finally {
        view.drop();
      }
    }

    /** An edge type with two super-types belongs to two families, and an edge of it is still one relationship. */
    @Test
    void anEdgeTypeWithTwoSuperTypesIsCountedOnce() {
      database.command("sql", "CREATE EDGE TYPE MENTORS");
      database.command("sql", "CREATE EDGE TYPE COACHES EXTENDS KNOWS, MENTORS");
      buildGraph(41, 120);
      database.transaction(() -> {
        final List<Vertex> people = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext(); )
          people.add(it.next().asVertex());
        for (int i = 0; i + 1 < people.size(); i += 3) {
          people.get(i).modify().newEdge("COACHES", people.get(i + 1));
          people.get(i + 1).modify().newEdge("MENTORS", people.get(i));
        }
      });
      final long knows = byHand("KNOWS", Vertex.DIRECTION.OUT);
      final long mentors = byHand("MENTORS", Vertex.DIRECTION.OUT);
      final long coaches = byHand("COACHES", Vertex.DIRECTION.OUT);
      assertThat(coaches).isPositive();
      assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]->() RETURN count(e) AS n")).isEqualTo(knows + mentors - coaches);
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e]-() RETURN count(e) AS n", null, Vertex.DIRECTION.BOTH);

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("coaching").build();
      try {
        assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]->() RETURN count(e) AS n")).isEqualTo(knows + mentors - coaches);
        assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]-() RETURN count(e) AS n"))
            .isEqualTo(count("MATCH ()-[e:KNOWS|MENTORS]-() RETURN sum(1) AS n"));
        assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      } finally {
        view.drop();
      }
    }

    /** A name a non-optional MATCH binds is never null, even when an OPTIONAL MATCH writes it again. */
    @Test
    void aNameBoundByTheMandatoryMatchCountsEveryRow() {
      buildGraph(43, 150);
      final String query = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) OPTIONAL MATCH (q)-[:KNOWS]->(p) RETURN count(p) AS n";
      assertThat(count(query)).isEqualTo(count(pipeline(query)));
      assertThat(count(query.replace("count(p)", "count(q)"))).isEqualTo(count(pipeline(query.replace("count(p)", "count(q)"))));

      // a WITH that binds the counted name to what may be null makes count(p) a count of the non-null values again
      final String rebound = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) WITH q AS p RETURN count(p) AS n";
      assertThat(count(rebound)).isEqualTo(count(pipeline(rebound)))
          .isEqualTo(count("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN sum(CASE WHEN q IS NULL THEN 0 ELSE 1 END) AS n"));
    }

    /** {@code MATCH (n)}: a subtype's vertices are a supertype's too, and are counted once. */
    @Test
    void anUnlabelledNodeCountsEveryVertexOnce() {
      buildGraph(29, 150);
      // documents and edges are records too, and no node
      database.command("sql", "CREATE DOCUMENT TYPE Note");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Note").save();
      });
      assertVertexCount();

      // created and deleted in the current transaction
      database.transaction(() -> {
        database.newVertex("Student").save();
        database.newVertex("City").save();
        final List<Record> doomed = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext() && doomed.size() < 3; )
          doomed.add(it.next());
        for (final Record vertex : doomed)
          vertex.asVertex().delete();
        assertVertexCount();
      });
      assertVertexCount();
      // with WHERE, a property map or a dynamic label it is a filter, not a count of the types
      assertThat(plan("MATCH (n) WHERE n.age > 3 RETURN count(n) AS n")).doesNotContain("TYPE COUNT");
      assertThat(plan("MATCH (n {age: 3}) RETURN count(n) AS n")).doesNotContain("TYPE COUNT");
      assertThat(plan("MATCH (n WHERE n.age > 3) RETURN count(n) AS n")).doesNotContain("TYPE COUNT");
      assertThat(count("MATCH (n WHERE n.age > 3) RETURN count(n) AS n")).isEqualTo(count("MATCH (n) WHERE n.age > 3 RETURN sum(1) AS n"));
      // a dynamic label names a type only when the query runs
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:$($label)) RETURN count(n) AS n", "label", "Student")) {
        assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(database.countType("Student", true));
      }
      assertThat(count("MATCH (n) WHERE n.age > 3 RETURN count(n) AS n")).isEqualTo(count("MATCH (n) WHERE n.age > 3 RETURN sum(1) AS n"));
    }

    private void assertAllEdgeCounts() {
      assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e:LIKES]->() RETURN count(e) AS n", "LIKES", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e]-() RETURN count(e) AS n", null, Vertex.DIRECTION.BOTH);
      // a sub-type alone, and listed with its super-type, which already counts it
      assertEdgeCount("MATCH ()-[e:KNOWS_WELL]-() RETURN count(e) AS n", "KNOWS_WELL", Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e:KNOWS|KNOWS_WELL]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e:KNOWS_WELL|KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
    }

    /**
     * People with random KNOWS (some of them KNOWS_WELL, some light, some self loops, some parallel), LIVES_IN to cities and
     * light LIKES.
     */
    private void buildGraph(final long seed, final int people) {
      final Random random = new Random(seed);
      database.transaction(() -> {
        final List<MutableVertex> persons = new ArrayList<>();
        final List<MutableVertex> cities = new ArrayList<>();
        for (int i = 0; i < people; i++)
          persons.add(database.newVertex(i % 4 == 0 ? "Student" : "Person").set("age", i % 7).save());
        for (int i = 0; i < people / 20 + 1; i++)
          cities.add(database.newVertex("City").save());

        for (final MutableVertex person : persons) {
          final int knows = random.nextInt(6);
          for (int k = 0; k < knows; k++) {
            final MutableVertex other = random.nextInt(15) == 0 ? person : persons.get(random.nextInt(persons.size()));
            switch (random.nextInt(5)) {
            case 0 -> person.newEdge("KNOWS_WELL", other);
            case 1 -> person.newLightEdge("KNOWS", other);
            default -> person.newEdge("KNOWS", other, "since", random.nextInt(5));
            }
          }
          if (random.nextBoolean())
            person.newEdge("LIVES_IN", cities.get(random.nextInt(cities.size())));
          for (int k = random.nextInt(4); k > 0; k--)
            person.newEdge("LIKES", persons.get(random.nextInt(persons.size())));
        }
      });
    }

    /** The query is answered by the edge count, and agrees with the row pipeline and with the edge lists. */
    private void assertEdgeCount(final String query, final String type, final Vertex.DIRECTION direction) {
      assertThat(plan(query)).as("plan of %s", query).contains(EDGE_COUNT);
      final long expected = byHand(type, direction);
      assertThat(count(query)).as(query).isEqualTo(expected);
      assertThat(count(pipeline(query))).as(pipeline(query)).isEqualTo(expected);
    }

    private long pushedDown(final String query) {
      assertThat(plan(query)).as("plan of %s", query).contains(EDGE_COUNT);
      final long value = count(query);
      assertThat(count(pipeline(query))).as(pipeline(query)).isEqualTo(value);
      return value;
    }

    /** The query is not answered by the edge count, and still agrees with the row pipeline (and with {@code expected}). */
    private void assertPipelineOnly(final String query, final Long expected) {
      assertThat(plan(query)).as("plan of %s", query).doesNotContain(EDGE_COUNT);
      final long value = count(query);
      assertThat(value).as(query).isEqualTo(count(pipeline(query)));
      if (expected != null)
        assertThat(value).as(query).isEqualTo(expected);
    }

    private void assertVertexCount() {
      final String query = "MATCH (n) RETURN count(n) AS n";
      assertThat(plan(query)).contains("TYPE COUNT OPTIMIZATION (all vertex types)");
      long expected = 0;
      for (final Iterator<Record> it = Labels.iterateMatchingVertices(database, null, false); it.hasNext(); it.next())
        ++expected;
      assertThat(count(query)).isEqualTo(expected);
      assertThat(count("MATCH (n) RETURN count(*) AS n")).isEqualTo(expected);
      assertThat(count("MATCH (n) RETURN sum(1) AS n")).isEqualTo(expected);
    }

    /** The relationships the hop matches, off the edge lists: undirected, each edge from both ends and a self loop once. */
    private long byHand(final String type, final Vertex.DIRECTION direction) {
      final String[] types = type == null ? new String[0] : new String[] { type };
      long total = 0;
      for (final Iterator<Record> it = Labels.iterateMatchingVertices(database, null, false); it.hasNext(); ) {
        final Vertex vertex = it.next().asVertex();
        for (final Vertex neighbor : vertex.getVertices(Vertex.DIRECTION.OUT, types)) {
          ++total;
          if (direction == Vertex.DIRECTION.BOTH && !neighbor.getIdentity().equals(vertex.getIdentity()))
            ++total;
        }
      }
      return total;
    }

    /**
     * The same count through the row pipeline: a sum is no count, so no push-down takes it, whatever the MATCH looks like. A
     * {@code count(x)} counts the rows where {@code x} is not null, {@code count(DISTINCT x)} the distinct values.
     */
    private static String pipeline(final String query) {
      final int returnAt = query.lastIndexOf(" RETURN count(");
      final String match = query.substring(0, returnAt);
      final String counted = query.substring(returnAt + " RETURN count(".length(), query.lastIndexOf(") AS n"));
      if (counted.equals("*"))
        return match + " RETURN sum(1) AS n";
      if (counted.startsWith("DISTINCT "))
        return match + " WITH DISTINCT " + counted.substring("DISTINCT ".length()) + " AS x RETURN sum(CASE WHEN x IS NULL THEN 0 ELSE 1 END) AS n";
      return match + " RETURN sum(CASE WHEN " + counted + " IS NULL THEN 0 ELSE 1 END) AS n";
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #9652: the Cypher queries answered from an index - a count with an equality predicate, {@code min}, {@code max},
   * {@code ORDER BY ... LIMIT 1} - cost 6 to 14 percent more per call after #9601, with the same answers. A plan is built for
   * every execution, and every execution asked every count push-down whether it applied: #9601 added a fold of the
   * statement's pass-through {@code WITH} clauses and a split into disconnected parts in front of them, and taught the
   * anti-join and star detectors to parse the {@code WHERE} before checking the pattern's shape. None of them takes a
   * single node, so all of that was paid only to be declined.
   * <p>
   * The fold is now derived once per statement and kept on it, a {@code MATCH} with no relationship skips the edge-walking
   * detectors, the split counts its units before allocating, and the shape checks come before the {@code WHERE} is read.
   * What is tested here is that the work is not redone and that every push-down the reordered checks guard is still taken,
   * with the answer the row pipeline gives.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9652CountPushDownDeclineCost extends TestHelper {
    private static final int VERTICES = 2_000;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.a LONG");
      database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE L");
      final Random random = new Random(9652);
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.newVertex("V").set("a", (long) random.nextInt(500)).save();
        final MutableVertex[] bs = new MutableVertex[7];
        for (int i = 0; i < bs.length; i++)
          bs[i] = database.newVertex("B").set("id", i).save();
        final MutableVertex[] as = new MutableVertex[11];
        for (int i = 0; i < as.length; i++) {
          as[i] = database.newVertex("A").set("id", i).save();
          for (int k = 0; k < i % 4; k++)
            as[i].newEdge("K", bs[(i + k) % bs.length]);
        }
        for (int i = 0; i < as.length; i++)
          for (int k = 1; k <= 2; k++)
            as[i].newEdge("L", as[(i * 3 + k) % as.length]);
      });
    }

    @Test
    void aStatementWithNoWithIsReadAsItselfAndNotFoldedAgain() {
      final String query = "MATCH (v:V) WHERE v.a = 77 RETURN count(*) AS c";
      final long expected = sql("SELECT count(@rid) AS c FROM V WHERE a = 77");
      assertThat(cypher(query)).isEqualTo(expected);
      assertThat(cypher(query)).isEqualTo(expected);

      final SimpleCypherStatement statement = cachedStatement(query);
      assertThat(statement.getCountPushDownForm()).isSameAs(statement);

      assertThat(cypher(query)).isEqualTo(expected);
      assertThat(cachedStatement(query)).isSameAs(statement);
      assertThat(statement.getCountPushDownForm()).isSameAs(statement);
    }

    @Test
    void aFoldedStatementIsDerivedOnceAndStillPushedDown() {
      final String query = "MATCH (v:V) WITH v RETURN count(v) AS c";
      assertThat(cypher(query)).isEqualTo(VERTICES);
      assertThat(cypher(query)).isEqualTo(VERTICES);

      final SimpleCypherStatement statement = cachedStatement(query);
      final CypherStatement folded = statement.getCountPushDownForm();
      assertThat(folded).isNotNull().isNotSameAs(statement);
      assertThat(folded.getWithClauses()).isEmpty();

      // a second execution reads the same fold rather than folding again
      assertThat(cypher(query)).isEqualTo(VERTICES);
      assertThat(statement.getCountPushDownForm()).isSameAs(folded);

      assertThat(profile(query)).contains("TYPE COUNT OPTIMIZATION (V)");
      assertThat(profile("MATCH (v:V) WITH v RETURN max(v.a) AS c")).contains("MAX FROM INDEX V[a]");
      assertThat(cypher("MATCH (v:V) WITH v RETURN max(v.a) AS c")).isEqualTo(sql("SELECT max(a) AS c FROM V"));
    }

    @Test
    void aRefusedFoldIsRememberedAsTheStatementItself() {
      final String query = "MATCH (v:V) WITH v WHERE v.a > 10 RETURN count(*) AS c";
      final long expected = sql("SELECT count(@rid) AS c FROM V WHERE a > 10");
      assertThat(cypher(query)).isEqualTo(expected);
      assertThat(cypher(query)).isEqualTo(expected);
      final SimpleCypherStatement statement = cachedStatement(query);
      assertThat(statement.getCountPushDownForm()).isSameAs(statement);

      // the remembered refusal sends the next execution straight to the pipeline, with the same answer
      assertThat(cypher(query)).isEqualTo(expected);
      assertThat(statement.getCountPushDownForm()).isSameAs(statement);
    }

    @Test
    void theIndexAnsweredQueriesOfTheIssueAnswerAsBefore() {
      assertThat(cypher("MATCH (v:V) WHERE v.a = 77 RETURN count(*) AS c")).isEqualTo(
          cypher("MATCH (v:V) WHERE v.a = 77 RETURN sum(1) AS c")).isEqualTo(sql("SELECT count(@rid) AS c FROM V WHERE a = 77"));
      assertThat(cypher("MATCH (v:V) RETURN max(v.a) AS c")).isEqualTo(sql("SELECT max(a) AS c FROM V"));
      assertThat(cypher("MATCH (v:V) WHERE v.a > 250 RETURN min(v.a) AS c")).isEqualTo(
          sql("SELECT min(a) AS c FROM V WHERE a > 250"));
      assertThat(cypher("MATCH (v:V) WHERE v.a > 250 RETURN v.a AS c ORDER BY v.a ASC LIMIT 1")).isEqualTo(
          sql("SELECT a AS c FROM V WHERE a > 250 ORDER BY a ASC LIMIT 1"));
      assertThat(profile("MATCH (v:V) RETURN max(v.a) AS c")).contains("MAX FROM INDEX V[a]");
      assertThat(profile("MATCH (v:V) WHERE v.a > 250 RETURN min(v.a) AS c")).contains("MIN FROM INDEX V[a]");
    }

    @Test
    void patternsWithNoRelationshipKeepTheirPushDownsAndAnswers() {
      // the product of two single nodes is asked before the relationship-free shortcut
      assertThat(profile("MATCH (a:A), (b:B) RETURN count(*) AS c")).contains("COUNT CARTESIAN PRODUCT");
      assertThat(cypher("MATCH (a:A), (b:B) RETURN count(*) AS c")).isEqualTo(11L * 7);
      assertThat(cypher("MATCH (a:A), (b:B) WHERE a.id > 4 AND b.id < 3 RETURN count(*) AS c")).isEqualTo(6L * 3);

      // one node written twice is one variable: no star arm, no product, the pipeline counts it
      assertThat(cypher("MATCH (a:A), (a) RETURN count(*) AS c")).isEqualTo(
          cypher("MATCH (a:A), (a) RETURN sum(1) AS c")).isEqualTo(11L);
      assertThat(cypher("MATCH (a:A) WHERE a.id >= 3 RETURN count(*) AS c")).isEqualTo(8L);
      assertThat(cypher("MATCH (a:A {id: 3}) RETURN count(a) AS c")).isEqualTo(1L);
    }

    @Test
    void patternsWithRelationshipsStillReachTheEdgeWalkingDetectors() {
      // a star around the anchor written alone as well as in its arm
      final String star = "MATCH (a:A), (a)-[:K]->(b:B) RETURN count(*) AS c";
      assertThat(profile(star)).contains("COUNT STAR JOIN");
      assertThat(cypher(star)).isEqualTo(cypher("MATCH (a:A), (a)-[:K]->(b:B) RETURN sum(1) AS c"));

      // the relationship sits in a later, optional clause: the check reads every clause, so the star still answers it
      final String optional = "MATCH (a:A) OPTIONAL MATCH (a)-[:K]->(b:B) RETURN count(*) AS c";
      assertThat(profile(optional)).contains("COUNT STAR JOIN");
      assertThat(cypher(optional)).isEqualTo(cypher("MATCH (a:A) OPTIONAL MATCH (a)-[:K]->(b:B) RETURN sum(1) AS c"));

      final String chain = "MATCH (a:A)-[:K]->(b:B) WHERE a.id > 2 RETURN count(*) AS c";
      assertThat(profile(chain)).contains("COUNT CHAIN PATHS");
      assertThat(cypher(chain)).isEqualTo(cypher("MATCH (a:A)-[:K]->(b:B) WHERE a.id > 2 RETURN sum(1) AS c"));

      // an anti-join chain still has its WHERE read once its shape qualifies
      final String antiJoin = "MATCH (a1:A)-[:L]-(a2:A)-[:L]-(a3:A)-[:K]->(b:B) WHERE NOT (a1)-[:L]-(a3) AND a1 <> a3 "
          + "RETURN count(*) AS c";
      assertThat(profile(antiJoin)).contains("COUNT ANTI-JOIN CHAIN");
      assertThat(cypher(antiJoin)).isEqualTo(cypher(antiJoin.replace("count(*)", "sum(1)")));
    }

    private long cypher(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private long sql(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private String profile(final String query) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    /**
     * The statement the engine executes for the query from now on: the cached one of its shape, its literals extracted
     * (issue #8307). The first text of a shape runs as written, so the query has to have run twice before it is the one used.
     */
    private SimpleCypherStatement cachedStatement(final String query) {
      return (SimpleCypherStatement) ((DatabaseInternal) database).getCypherStatementCache().getParameterized(query).statement()
          .statement();
    }
  }
}
