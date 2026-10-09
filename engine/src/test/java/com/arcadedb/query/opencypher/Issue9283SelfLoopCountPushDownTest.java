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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue9283SelfLoopCountPushDownTest extends TestHelper {

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
      assertThat(plan(chains[i][0] + " RETURN count(*) AS n")).as("plan of %s", chains[i][0]).contains("COUNT CHAIN PATHS");
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
