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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pattern comprehensions: scope, COUNT in WHERE, all() and undirected self loops.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherPatternComprehensionTest {
  /**
   * Regression tests for issue #5140: {@code COUNT { ... }} block subqueries inside a
   * pattern-comprehension {@code WHERE} clause must filter the comprehension results.
   * ArcadeDB previously ignored the predicate, returning all rows.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5140CountInPatternComprehensionWhere extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("CB");
      database.getSchema().createEdgeType("E");

      // a -> b (weight 1), a -> c (weight 2), b -> d (weight 3)
      // b has 1 outgoing edge; c has 0 outgoing edges.
      database.command("opencypher",
          """
          CREATE (a:CB {name: 'a'}), \
          (b:CB {name: 'b'}), \
          (c:CB {name: 'c'}), \
          (d:CB {name: 'd'}), \
          (a)-[:E {weight: 1}]->(b), \
          (a)-[:E {weight: 2}]->(c), \
          (b)-[:E {weight: 3}]->(d)""");
    }

    @Test
    void countZeroInPatternComprehensionWhere() {
      // Only c has zero outgoing edges.
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:CB {name: 'a'})
          RETURN [
            (a)-[:E]->(b:CB)
            WHERE COUNT { MATCH (b)-[:E]->(:CB) } = 0
            | b.name
          ] AS names""");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<Object> names = result.getProperty("names");
      assertThat(names).containsExactly("c");
    }

    @Test
    void countGreaterThanZeroInPatternComprehensionWhere() {
      // Only b has outgoing edges.
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:CB {name: 'a'})
          RETURN [
            (a)-[:E]->(b:CB)
            WHERE COUNT { MATCH (b)-[:E]->(:CB) } > 0
            | b.name
          ] AS names""");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<Object> names = result.getProperty("names");
      assertThat(names).containsExactly("b");
    }

    @Test
    void ordinaryPredicateInPatternComprehensionWhereStillWorks() {
      // Control: an ordinary predicate must keep filtering correctly.
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:CB {name: 'a'})
          RETURN [
            (a)-[:E]->(b:CB)
            WHERE b.name = 'c'
            | b.name
          ] AS names""");

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<Object> names = result.getProperty("names");
      assertThat(names).containsExactly("c");
    }
  }

  /**
   * Regression tests for issue #5145: {@code all()} evaluated over a list produced by a pattern
   * comprehension ({@code [(u)-[:FRIEND]->(f:User) | f.active]}) must return the same result as the
   * equivalent {@code MATCH ... collect()} formulation, on graphs of any size.
   * <p>
   * The reporter observed a wrong result ({@code cnt = 0} instead of {@code cnt = 2}) on a
   * deterministic 35-node graph. These tests exercise the reporter's exact query on the documented
   * 6-node baseline, on 35-node graphs shaped like the reporter's (20 active / 15 inactive, exactly
   * two active users whose friends are all inactive), and cross-check the pattern-comprehension list
   * against {@code collect()} and against the raw graph.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5145AllPatternComprehension extends TestHelper {
    private static final String FLAG_QUERY =
        "MATCH (u:User {active: true}) " +
        "WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives " +
        "WHERE size(actives) > 0 AND all(a IN actives WHERE a = false) " +
        "SET u.flagged = true";

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("FRIEND");
    }

    private long flaggedCount() {
      return ((Number) database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt").next().getProperty("cnt")).longValue();
    }

    /** Ground truth from the stored graph: active users with a non-empty, all-inactive friend list. */
    private int expectedFlagged() {
      int count = 0;
      final ResultSet users = database.query("sql", "SELECT FROM User WHERE active = true");
      while (users.hasNext()) {
        final Vertex u = users.next().getVertex().get();
        int friends = 0;
        boolean allInactive = true;
        for (final Vertex f : u.getVertices(Vertex.DIRECTION.OUT, "FRIEND")) {
          friends++;
          if (Boolean.TRUE.equals(f.get("active")))
            allInactive = false;
        }
        if (friends > 0 && allInactive)
          count++;
      }
      return count;
    }

    @Test
    void sixNodeBaseline() {
      database.command("opencypher",
          "CREATE (u1:User {id:1, active:true}), (u2:User {id:2, active:true}), (u3:User {id:3, active:true}), "
          + "(u4:User {id:4, active:false}), (u5:User {id:5, active:false}), (u6:User {id:6, active:false}), "
          + "(u1)-[:FRIEND]->(u4), (u1)-[:FRIEND]->(u5), (u2)-[:FRIEND]->(u4), (u2)-[:FRIEND]->(u1), "
          + "(u3)-[:FRIEND]->(u5), (u3)-[:FRIEND]->(u6)");

      database.command("opencypher", FLAG_QUERY);

      final ResultSet rs = database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN u.id AS id ORDER BY id");
      final List<Object> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
      assertThat(ids).containsExactly(1, 3);
    }

    /**
     * Builds a 35-node graph shaped like the reporter's (ids 0-19 active, 20-34 inactive, each user
     * befriending up to five random others) and asserts the pattern-comprehension query flags exactly
     * the active users whose friends are all inactive - matching the raw graph and {@code collect()}.
     */
    @Test
    void thirtyFiveNodeMatchesGroundTruthAndCollect() {
      final Random rnd = new Random(42);
      final StringBuilder create = new StringBuilder("CREATE ");
      final List<String> parts = new ArrayList<>();
      for (int i = 0; i < 35; i++)
        parts.add("(u" + i + ":User {id:" + i + ", active:" + (i < 20) + "})");
      for (int i = 0; i < 35; i++) {
        final int k = rnd.nextInt(6);
        final List<Integer> others = new ArrayList<>();
        for (int x = 0; x < 35; x++) if (x != i) others.add(x);
        java.util.Collections.shuffle(others, rnd);
        for (int t = 0; t < k; t++)
          parts.add("(u" + i + ")-[:FRIEND]->(u" + others.get(t) + ")");
      }
      database.command("opencypher", create.append(String.join(",", parts)).toString());

      final int groundTruth = expectedFlagged();

      final long collectCnt = ((Number) database.query("opencypher",
          "MATCH (u:User {active: true})-[:FRIEND]->(f:User) " +
          "WITH u, collect(f.active) AS actives " +
          "WHERE size(actives) > 0 AND all(a IN actives WHERE a = false) " +
          "RETURN count(DISTINCT u) AS cnt").next().getProperty("cnt")).longValue();

      database.command("opencypher", FLAG_QUERY);
      final long patternComprehension = flaggedCount();

      assertThat(collectCnt).as("collect() control").isEqualTo(groundTruth);
      assertThat(patternComprehension).as("pattern-comprehension all()").isEqualTo(groundTruth);
    }

    /**
     * The pattern-comprehension list must equal the {@code collect()} list per user, both in content
     * and element type (Boolean, not e.g. String), so {@code all(a IN actives WHERE a = false)} agrees.
     */
    @Test
    void patternComprehensionListEqualsCollectPerUser() {
      buildRandomGraph(35, 42);

      final ResultSet pc = database.query("opencypher",
          "MATCH (u:User {active: true}) RETURN u.id AS id, [(u)-[:FRIEND]->(f:User) | f.active] AS actives ORDER BY id");
      final ResultSet cl = database.query("opencypher",
          "MATCH (u:User {active: true}) OPTIONAL MATCH (u)-[:FRIEND]->(f:User) " +
          "WITH u.id AS id, collect(f.active) AS actives RETURN id, actives ORDER BY id");

      final java.util.Map<Object, List<Object>> pcMap = new java.util.TreeMap<>();
      while (pc.hasNext()) {
        final Result r = pc.next();
        pcMap.put(r.getProperty("id"), sorted(r.getProperty("actives")));
      }
      while (cl.hasNext()) {
        final Result r = cl.next();
        final Object id = r.getProperty("id");
        assertThat(pcMap.get(id)).as("actives for id=%s", id).isEqualTo(sorted(r.getProperty("actives")));
      }
    }

    /**
     * Follow-up to issue #5145: a <em>parenthesized</em> inner predicate inside {@code all()} used in a
     * {@code WITH ... WHERE} filter must behave identically to the unparenthesized form. The recursive
     * parenthesized-expression finder in the WHERE-clause boolean parser used to dive into the inner
     * {@code (a = false)} and discard the enclosing {@code all()}, evaluating the bare predicate against
     * an unbound iteration variable and filtering out every row (flagging nothing instead of 1 and 4).
     */
    @Test
    void parenthesizedInnerPredicateInWhereMatchesUnparenthesized() {
      database.command("opencypher",
          "CREATE (u1:User {id:1, active:true}), (u2:User {id:2, active:true}), (u3:User {id:3, active:false}), "
          + "(u4:User {id:4, active:true}), "
          + "(u1)-[:FRIEND]->(u3), (u2)-[:FRIEND]->(u3), (u2)-[:FRIEND]->(u4), (u4)-[:FRIEND]->(u3)");
      // u1 friends: [false] -> all inactive; u2 friends: [false, true] -> not all; u4 friends: [false] -> all inactive.

      database.command("opencypher",
          "MATCH (u:User {active: true}) WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives "
          + "WHERE size(actives) > 0 AND all(a IN actives WHERE (a = false)) SET u.flagged = true");

      final ResultSet rs = database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN u.id AS id ORDER BY id");
      final List<Object> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
      assertThat(ids).containsExactly(1, 4);
    }

    /**
     * Equivalence check for the parenthesized predicate directly in a read {@code WHERE}: the
     * parenthesized and unparenthesized forms must select the same users.
     */
    @Test
    void parenthesizedAndUnparenthesizedWhereAgree() {
      database.command("opencypher",
          "CREATE (u1:User {id:1, active:true}), (u2:User {id:2, active:true}), (u3:User {id:3, active:false}), "
          + "(u4:User {id:4, active:true}), "
          + "(u1)-[:FRIEND]->(u3), (u2)-[:FRIEND]->(u3), (u2)-[:FRIEND]->(u4), (u4)-[:FRIEND]->(u3)");

      assertThat(selectFlagged("all(a IN actives WHERE a = false)")).containsExactly(1, 4);
      assertThat(selectFlagged("all(a IN actives WHERE (a = false))")).containsExactly(1, 4);
      assertThat(selectFlagged("all(a IN actives WHERE ((a = false)))")).containsExactly(1, 4);
    }

    private List<Object> selectFlagged(final String predicate) {
      final ResultSet rs = database.query("opencypher",
          "MATCH (u:User {active: true}) WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives "
          + "WHERE size(actives) > 0 AND " + predicate + " RETURN u.id AS id ORDER BY id");
      final List<Object> ids = new ArrayList<>();
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
      return ids;
    }

    /** A relationship-type filter in a comprehension must not leak edges of other types into the list. */
    @Test
    void relationshipTypeFilterDoesNotLeak() {
      database.getSchema().createEdgeType("FOLLOWS");
      // u0 (active) -FRIEND-> u1 (inactive); u0 -FOLLOWS-> u2 (active). The comprehension over FRIEND
      // must yield [false] only, so the active FOLLOWS target does not turn all() false.
      database.command("opencypher",
          "CREATE (u0:User {id:0, active:true}), (u1:User {id:1, active:false}), (u2:User {id:2, active:true}), "
          + "(u0)-[:FRIEND]->(u1), (u0)-[:FOLLOWS]->(u2)");
      final Result r = database.query("opencypher",
          "MATCH (u:User {id:0}) WITH u, [(u)-[:FRIEND]->(f:User) | f.active] AS actives "
          + "RETURN actives, all(a IN actives WHERE a = false) AS allInactive").next();
      assertThat(sorted(r.getProperty("actives"))).containsExactly(false);
      assertThat(r.<Boolean>getProperty("allInactive")).isTrue();
    }

    private void buildRandomGraph(final int nodeCount, final long seed) {
      final Random rnd = new Random(seed);
      database.transaction(() -> {
        final List<MutableVertex> users = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++)
          users.add((MutableVertex) database.newVertex("User").set("id", i).set("active", i < nodeCount * 20 / 35).save());
        for (int i = 0; i < nodeCount; i++) {
          final int k = rnd.nextInt(6);
          final List<Integer> others = new ArrayList<>();
          for (int x = 0; x < nodeCount; x++) if (x != i) others.add(x);
          java.util.Collections.shuffle(others, rnd);
          for (int t = 0; t < k; t++)
            users.get(i).newEdge("FRIEND", users.get(others.get(t))).save();
        }
      });
    }

    @SuppressWarnings("unchecked")
    private static List<Object> sorted(final Object o) {
      final List<Object> l = new ArrayList<>();
      if (o instanceof Iterable)
        for (final Object x : (Iterable<Object>) o)
          l.add(x);
      l.sort((x, y) -> String.valueOf(x).compareTo(String.valueOf(y)));
      return l;
    }
  }

  /**
   * Regression tests for issue #5179: a variable introduced only inside a pattern comprehension
   * (or list comprehension) must not leak into the outer {@code any()/all()/none()/single()}
   * predicate. Referencing such a variable must raise an undefined-variable scope error, matching
   * Neo4j ({@code SyntaxError: Variable `n` not defined}) and Memgraph ({@code Unbound variable: n}),
   * instead of silently returning an empty result set.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5179PatternComprehensionScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (a:A {v:1}), (b:B {v:10}), (a)-[:R]->(b)");
    }

    /** The reporter's failing query: pattern-comprehension-local {@code n} referenced in any() WHERE. */
    @Test
    void patternComprehensionVariableLeakInAnyPredicateThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          "MATCH (a:A) WHERE any(p IN [(a)-[:R]->(n) | n.v] WHERE n.v > 5) RETURN a.v").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("n");
    }

    /** Optional control from the issue: same leak with an ordinary list comprehension in RETURN. */
    @Test
    void listComprehensionVariableLeakInAnyPredicateThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          "RETURN any(p IN [x IN [10] | x] WHERE x > 5) AS r").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("x");
    }

    /** Control 1: referencing the any() iteration variable {@code p} is valid and returns a.v = 1. */
    @Test
    void iterationVariableIsInScope() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:A) WHERE any(p IN [(a)-[:R]->(n) | n.v] WHERE p > 5) RETURN a.v AS v");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }

    /** Control 2: {@code n} used inside the pattern comprehension itself is valid and yields [10]. */
    @Test
    void patternComprehensionInternalUsageIsValid() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:A) RETURN [(a)-[:R]->(n) | n.v] AS vals");
      assertThat(rs.hasNext()).isTrue();
      final Object vals = rs.next().getProperty("vals");
      assertThat(vals).isInstanceOf(Iterable.class);
      @SuppressWarnings("unchecked")
      final Iterable<Object> list = (Iterable<Object>) vals;
      assertThat(list).containsExactly(10);
    }
  }

  /**
   * Regression tests for issue #5456: an undirected relationship pattern inside a pattern
   * comprehension emitted every self-loop twice, once through the outgoing adjacency list of the
   * vertex and once through the incoming one. Neo4j returns each relationship exactly once, and the
   * equivalent undirected {@code MATCH} already did after issue #5446 / PR #5447, so the pattern
   * comprehension evaluator was the last path missing the deduplication.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5456UndirectedPatternComprehensionSelfLoop extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (single:Person {id: 'single'}), (multi:Person {id: 'multi'}), "
          + "(head:Person {id: 'head'}), (tail:Person {id: 'tail'}), (far:Person {id: 'far'})");
      database.command("opencypher", "MATCH (a:Person {id: 'single'}) CREATE (a)-[:LOOP {rid: 1}]->(a)");
      database.command("opencypher", "MATCH (a:Person {id: 'multi'}) CREATE (a)-[:LOOP {rid: 1}]->(a), (a)-[:LOOP {rid: 2}]->(a)");
      // An ordinary relationship on a pair of loop-free nodes, so the plain undirected case stays covered.
      database.command("opencypher",
          "MATCH (h:Person {id: 'head'}), (t:Person {id: 'tail'}) CREATE (h)-[:LOOP {rid: 3}]->(t)");
      // A second relationship pointing at 'tail', so head..far is only reachable by changing direction.
      database.command("opencypher",
          "MATCH (f:Person {id: 'far'}), (t:Person {id: 'tail'}) CREATE (f)-[:LOOP {rid: 4}]->(t)");
    }

    /** The reporter's first witness: one self-loop must contribute exactly one list element. */
    @Test
    void undirectedPatternComprehensionEmitsSingleSelfLoopOnce() {
      assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[r:LOOP]-(b) | r.rid] AS ids")).containsExactly(1);
    }

    /** The reporter's second witness: two distinct self-loops must stay distinct and not be doubled. */
    @Test
    void undirectedPatternComprehensionEmitsEveryDistinctSelfLoopOnce() {
      assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[r:LOOP]-(b) | r.rid] AS ids"))
          .containsExactlyInAnyOrder(1, 2);
    }

    /** size() over the comprehension is the cardinality the reporter cares about downstream. */
    @Test
    void sizeOfUndirectedPatternComprehensionCountsEachSelfLoopOnce() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Person {id: 'multi'}) RETURN size([(a)-[:LOOP]-(b) | b.id]) AS n");
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(2);
      rs.close();
    }

    /** Control from the issue: the directed comprehension was always correct and must stay so. */
    @Test
    void directedPatternComprehensionIsUnaffected() {
      assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[r:LOOP]->(b) | r.rid] AS ids")).containsExactly(1);
      assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[r:LOOP]->(b) | r.rid] AS ids"))
          .containsExactlyInAnyOrder(1, 2);
    }

    /** An undirected hop over a non-loop relationship must still be reached from either endpoint. */
    @Test
    void undirectedPatternComprehensionStillWalksBothDirectionsForNonLoopEdges() {
      assertThat(ids("MATCH (a:Person {id: 'head'}) RETURN [(a)-[r:LOOP]-(b) | b.id] AS ids")).containsExactly("tail");
      assertThat(ids("MATCH (a:Person {id: 'tail'}) RETURN [(a)-[r:LOOP]-(b) | b.id] AS ids"))
          .containsExactlyInAnyOrder("head", "far");
    }

    /** The variable-length form walks the same adjacency lists and must deduplicate identically. */
    @Test
    void undirectedVariableLengthPatternComprehensionEmitsEachSelfLoopOnce() {
      assertThat(ids("MATCH (a:Person {id: 'single'}) RETURN [(a)-[:LOOP*1..1]-(b) | b.id] AS ids"))
          .containsExactly("single");
      assertThat(ids("MATCH (a:Person {id: 'multi'}) RETURN [(a)-[:LOOP*1..1]-(b) | b.id] AS ids"))
          .containsExactly("multi", "multi");
    }

    /**
     * A single undirected walk also lets a variable-length hop change direction between steps:
     * {@code head -> tail <- far} is a valid undirected 2-hop path, which the previous
     * one-direction-per-pass traversal could never produce.
     */
    @Test
    void undirectedVariableLengthPatternComprehensionChangesDirectionBetweenHops() {
      assertThat(ids("MATCH (a:Person {id: 'head'}) RETURN [(a)-[:LOOP*1..2]-(b) | b.id] AS ids"))
          .containsExactlyInAnyOrder("tail", "far");
    }

    /** The undirected {@code MATCH} control the issue used to prove the comprehension disagreed. */
    @Test
    void undirectedMatchControlKeepsReturningOneRowPerRelationship() {
      final List<Object> rids = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", "MATCH (a:Person {id: 'multi'})-[r:LOOP]-(b) RETURN r.rid AS rid");
      while (rs.hasNext())
        rids.add(rs.next().getProperty("rid"));
      rs.close();
      assertThat(rids).containsExactlyInAnyOrder(1, 2);
    }

    @SuppressWarnings("unchecked")
    private List<Object> ids(final String query) {
      final ResultSet rs = database.query("opencypher", query);
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object value = row.getProperty(row.getPropertyNames().iterator().next());
      rs.close();
      assertThat(value).isInstanceOf(List.class);
      return (List<Object>) value;
    }
  }
}
