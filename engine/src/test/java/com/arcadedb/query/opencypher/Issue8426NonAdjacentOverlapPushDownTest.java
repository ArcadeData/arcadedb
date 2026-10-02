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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8426 follow-up: the count push-down declined every chain whose two overlapping hops were not adjacent, but the
 * hops of {@code (t1)<-[:HAS_TAG]-(m)<-[:REPLY_OF]-(c)-[:HAS_TAG]->(t2) WHERE t1 <> t2} (LSQB Q5) can bind one edge only
 * if {@code m = c} and {@code t1 = t2}, and the inequality forbids exactly that. The chain fell back to pattern matching
 * (0.2s to 3.5s on SF1). The rule is now "the inequality joins two nodes that binding one edge forces equal, under every
 * orientation of the hops", so unprotected shapes still decline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8426NonAdjacentOverlapPushDownTest extends TestHelper {
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
    // WITH * forces row-by-row matching, which has no push-down: this is the oracle
    return count(query.replace(" RETURN count(*) AS n", " WITH * RETURN count(*) AS n"));
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
