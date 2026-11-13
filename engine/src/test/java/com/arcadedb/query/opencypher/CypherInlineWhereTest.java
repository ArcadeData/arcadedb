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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Inline property maps and inline WHERE on node, relationship and variable-length patterns.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherInlineWhereTest {
  /**
   * Regression test for GitHub issue #5093.
   * Tests that an inline property filter on a relationship pattern, e.g.
   * {@code MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode)}, is correctly applied.
   * Prior to the fix, the optimizer path ignored the inline relationship property map
   * and returned all LINK edges regardless of the {@code w} value.
   * <p>
   * The behavior is verified against the OpenCypher reference implementation (Neo4j),
   * which returns only the edges where {@code w = 1}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5093RelationshipInlinePropertyFilter {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5093-rel-inline-property").create();

      database.getSchema().createVertexType("BugNode");
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex n1 = database.newVertex("BugNode").set("id", 1).save();
        final MutableVertex n2 = database.newVertex("BugNode").set("id", 2).save();
        final MutableVertex n3 = database.newVertex("BugNode").set("id", 3).save();
        final MutableVertex n4 = database.newVertex("BugNode").set("id", 4).save();
        final MutableVertex n5 = database.newVertex("BugNode").set("id", 5).save();

        n1.newEdge("LINK", n2).set("w", 1).save();
        n1.newEdge("LINK", n3).set("w", 2).save();
        n2.newEdge("LINK", n4).set("w", 1).save();
        n4.newEdge("LINK", n5).set("w", 1).save();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void inlineRelationshipPropertyFilterIsApplied() {
      // Only the 3 edges with w=1 must be returned (1->2, 2->4, 4->5).
      final List<int[]> pairs = runPairs(
          "MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

      assertThat(pairs).hasSize(3);
      assertThat(pairs.get(0)).containsExactly(1, 2);
      assertThat(pairs.get(1)).containsExactly(2, 4);
      assertThat(pairs.get(2)).containsExactly(4, 5);
    }

    @Test
    void inlineRelationshipPropertyFilterMatchesWhereEquivalent() {
      // The inline filter must be semantically equivalent to the explicit WHERE form.
      final List<int[]> inline = runPairs(
          "MATCH (a:BugNode)-[r:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");
      final List<int[]> where = runPairs(
          "MATCH (a:BugNode)-[r:LINK]->(b:BugNode) WHERE r.w = 1 RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

      assertThat(inline).hasSameSizeAs(where);
      for (int i = 0; i < inline.size(); i++)
        assertThat(inline.get(i)).containsExactly(where.get(i)[0], where.get(i)[1]);
    }

    @Test
    void inlineRelationshipPropertyFilterNoEdgeVariable() {
      // Same filter but on an anonymous relationship (no 'r' variable).
      final List<int[]> pairs = runPairs(
          "MATCH (a:BugNode)-[:LINK {w: 1}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

      assertThat(pairs).hasSize(3);
      assertThat(pairs.get(0)).containsExactly(1, 2);
      assertThat(pairs.get(1)).containsExactly(2, 4);
      assertThat(pairs.get(2)).containsExactly(4, 5);
    }

    @Test
    void inlineRelationshipPropertyFilterOtherValue() {
      // Only the single edge with w=2 (1->3) must be returned.
      final List<int[]> pairs = runPairs(
          "MATCH (a:BugNode)-[r:LINK {w: 2}]->(b:BugNode) RETURN a.id AS a_id, b.id AS b_id ORDER BY a_id, b_id");

      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0)).containsExactly(1, 3);
    }

    private List<int[]> runPairs(final String cypher) {
      final List<int[]> pairs = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext()) {
        final Result r = rs.next();
        pairs.add(new int[] { ((Number) r.getProperty("a_id")).intValue(), ((Number) r.getProperty("b_id")).intValue() });
      }
      rs.close();
      return pairs;
    }
  }

  /**
   * Regression test for GitHub issues #5463 and #5462, two reports of the same defect with the same
   * reproducer.
   * <p>
   * A relationship inline {@code WHERE} predicate that reads an edge property must select the same
   * rows no matter what the projection does with the relationship variable afterwards. The reported
   * symptom was that {@code RETURN count(*)} answered 0 while {@code RETURN count(r)} answered 1 for
   * one and the same {@code MATCH}: the edge binding was elided from the plan whenever nothing
   * outside the pattern referenced {@code r}, so the inline predicate was evaluated against an
   * unbound variable and rejected every row. Referencing {@code r} anywhere downstream - through
   * {@code count(r)}, {@code r.tag} or even a {@code collect(r.tag)} sitting next to the
   * {@code count(*)} - kept the binding alive and masked the defect.
   * <p>
   * The behavior is pinned here for both pattern-parsing paths that accept a relationship inline
   * predicate: the {@code MATCH} path and the pattern-comprehension / {@code EXISTS} path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5463CountStarRelInlineWhere {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5463-count-star-rel-inline-where").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:2})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:2}) "
            + "CREATE (a)-[:E {tag: 'ok'}]->(b), (a)-[:E {tag: 'bad'}]->(b)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        final long value = rs.next().<Number>getProperty("c").longValue();
        assertThat(rs.hasNext()).as("a single aggregate row").isFalse();
        return value;
      }
    }

    private List<Object> column(final String query, final String name) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(name));
      }
      return values;
    }

    @Test
    void countStarAgreesWithCountRelationshipOnInlinePropertyPredicate() {
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c"))
          .as("countStar").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(r) AS c"))
          .as("countRelationship").isEqualTo(1);
      assertThat(column("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN r.tag AS tag", "tag"))
          .as("materializedRows").containsExactly("ok");
    }

    @Test
    void countStarHonoursTheInlinePredicateOnEveryTagValue() {
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'bad']->(b:A) RETURN count(*) AS c"))
          .as("bad").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'nope']->(b:A) RETURN count(*) AS c"))
          .as("noMatch").isZero();
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag IS NOT NULL]->(b:A) RETURN count(*) AS c"))
          .as("allTagged").isEqualTo(2);
    }

    @Test
    void aSecondProjectionItemDoesNotChangeTheCount() {
      // issue #5462: adding a non-filtering output expression that mentions r used to flip count(*)
      // from 0 to 1, because it was the only thing keeping the edge binding alive
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c, collect(r.tag) AS tags")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("c").longValue()).as("countStar").isEqualTo(1);
        assertThat(row.<List<Object>>getProperty("tags")).as("tags").containsExactly("ok");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void literalInlinePredicateAndClauseLevelWhereKeepWorking() {
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE 1=1]->(b:A) RETURN count(*) AS c"))
          .as("literalInline").isEqualTo(2);
      assertThat(count("MATCH (a:A {v:1})-[r:E]->(b:A) WHERE r.tag = 'ok' RETURN count(*) AS c"))
          .as("clauseLevel").isEqualTo(1);
    }

    @Test
    void countStarWithAnonymousEndpointsAndUnlabelledTarget() {
      assertThat(count("MATCH (:A {v:1})-[r:E WHERE r.tag = 'ok']->(b) RETURN count(*) AS c"))
          .as("anonymousSource").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->() RETURN count(*) AS c"))
          .as("anonymousTarget").isEqualTo(1);
      assertThat(count("MATCH ()-[r:E WHERE r.tag = 'ok']->() RETURN count(*) AS c"))
          .as("bothEndsAnonymous").isEqualTo(1);
    }

    @Test
    void directionVariantsHonourTheInlinePredicate() {
      assertThat(count("MATCH (b:A {v:2})<-[r:E WHERE r.tag = 'ok']-(a:A) RETURN count(*) AS c"))
          .as("rightToLeft").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']-(b:A) RETURN count(*) AS c"))
          .as("undirected").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r WHERE r.tag = 'ok']->(b:A) RETURN count(*) AS c"))
          .as("untypedRelationship").isEqualTo(1);
    }

    @Test
    void otherAggregatesSeeTheSameRows() {
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(b) AS c"))
          .as("countTargetNode").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(DISTINCT b) AS c"))
          .as("countDistinctTargetNode").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) WITH count(*) AS c RETURN c AS c"))
          .as("countStarThroughWith").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN sum(b.v) AS c"))
          .as("sumOverTargetProperty").isEqualTo(2);
    }

    @Test
    void countStarGroupedByTheSourceNode() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(b:A) RETURN a.v AS v, count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("v").intValue()).isEqualTo(1);
        assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(1);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void optionalMatchKeepsTheRowAndStillFilters() {
      assertThat(count("MATCH (a:A {v:1}) OPTIONAL MATCH (a)-[r:E WHERE r.tag = 'ok']->(b:A) RETURN count(b) AS c"))
          .as("optionalHit").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1}) OPTIONAL MATCH (a)-[r:E WHERE r.tag = 'zz']->(b:A) RETURN count(b) AS c"))
          .as("optionalMiss").isZero();
    }

    @Test
    void patternComprehensionAndExistsShareTheSamePredicateSemantics() {
      assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E WHERE r.tag = 'ok']->(b:A) | b.v]) AS c"))
          .as("patternComprehension").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E WHERE r.tag = 'nope']->(b:A) | b.v]) AS c"))
          .as("patternComprehensionNoMatch").isZero();
      assertThat(count("MATCH (a:A {v:1}) RETURN COUNT { (a)-[r:E WHERE r.tag = 'ok']->(:A) } AS c"))
          .as("countSubquery").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E WHERE r.tag = 'ok']->(:A) } RETURN count(*) AS c"))
          .as("existsSubquery").isEqualTo(1);
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E WHERE r.tag = 'nope']->(:A) } RETURN count(*) AS c"))
          .as("existsSubqueryNoMatch").isZero();
    }
  }

  /**
   * Regression test for GitHub issue #5464.
   * <p>
   * An {@code EXISTS { ... }} subquery whose pattern carries a relationship inline {@code WHERE}
   * predicate must evaluate the predicate like any other filter. Before the fix a trivially true
   * predicate such as {@code [r:E WHERE 1=1]} flipped the result from {@code true} to {@code false}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5464ExistsRelInlineWhere {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5464-exists-rel-inline-where").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:2})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:2}) "
            + "CREATE (a)-[:E {tag: 'ok'}]->(b), (a)-[:E {tag: 'bad'}]->(b)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private boolean existsBool(final String existsBody) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1}) RETURN EXISTS { " + existsBody + " } AS v")) {
        assertThat(rs.hasNext()).isTrue();
        return Boolean.TRUE.equals(rs.next().<Boolean>getProperty("v"));
      }
    }

    @Test
    void patternOnlyExistsWithTriviallyTrueInlineWhere() {
      assertThat(existsBool("(a)-[:E]->(:A)")).as("plainExists").isTrue();
      assertThat(existsBool("(a)-[r:E]->(:A)")).as("namedRelationshipExists").isTrue();
      assertThat(existsBool("(a)-[r:E WHERE 1=1]->(:A)")).as("inlineTrueExists").isTrue();
    }

    @Test
    void patternOnlyExistsWithRelationshipPropertyInlineWhere() {
      assertThat(existsBool("(a)-[r:E WHERE r.tag = 'ok']->(:A)")).as("matchingProperty").isTrue();
      assertThat(existsBool("(a)-[r:E WHERE r.tag = 'bad']->(:A)")).as("otherMatchingProperty").isTrue();
      assertThat(existsBool("(a)-[r:E WHERE r.tag = 'nope']->(:A)")).as("nonMatchingProperty").isFalse();
    }

    @Test
    void explicitMatchExistsWithInlineWhere() {
      assertThat(existsBool("MATCH (a)-[r:E WHERE 1=1]->(:A)")).as("explicitMatchInlineTrue").isTrue();
      assertThat(existsBool("MATCH (a)-[r:E WHERE r.tag = 'ok']->(:A)")).as("explicitMatchInlineProp").isTrue();
      assertThat(existsBool("MATCH (a)-[r:E WHERE r.tag = 'nope']->(:A)")).as("explicitMatchInlineNoMatch").isFalse();
    }

    @Test
    void explicitMatchExistsWithClauseLevelWhereStillWorks() {
      assertThat(existsBool("MATCH (a)-[r:E]->(:A) WHERE r.tag = 'ok'")).as("clauseWhereMatch").isTrue();
      assertThat(existsBool("MATCH (a)-[r:E]->(:A) WHERE r.tag = 'nope'")).as("clauseWhereNoMatch").isFalse();
    }

    @Test
    void nodeInlineWhereInsideExists() {
      assertThat(existsBool("(a)-[:E]->(b:A WHERE b.v = 2)")).as("nodeInlineTrue").isTrue();
      assertThat(existsBool("(a)-[:E]->(b:A WHERE b.v = 99)")).as("nodeInlineFalse").isFalse();
    }

    @Test
    void notExistsWithInlineWhere() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1}) WHERE NOT EXISTS { (a)-[r:E WHERE r.tag = 'nope']->(:A) } RETURN a.v AS v")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1}) WHERE NOT EXISTS { (a)-[r:E WHERE r.tag = 'ok']->(:A) } RETURN a.v AS v")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void countAndCollectSubqueriesShareTheSameRewrite() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1}) RETURN COUNT { (a)-[r:E WHERE r.tag = 'ok']->(:A) } AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1}) RETURN COLLECT { MATCH (a)-[r:E WHERE r.tag = 'ok']->(b:A) RETURN b.v } AS l")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<List<Object>>getProperty("l")).hasSize(1);
      }
    }

    @Test
    void clauseKeywordInsideAStringLiteralIsNotMistakenForAClause() {
      // 'RETURN' as a value must not be scanned as the subquery's RETURN clause
      assertThat(existsBool("(a)-[r:E WHERE r.tag = 'RETURN']->(:A)")).as("literalKeyword").isFalse();
      assertThat(existsBool("(a)-[r:E WHERE r.tag <> 'RETURN']->(:A)")).as("literalKeywordNegated").isTrue();
    }

    @Test
    void regularMatchWithInlineWhereIsUnaffected() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:A {v:1})-[r:E WHERE 1=1]->(b:A) RETURN count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2);
      }
    }
  }

  /**
   * Regression test for GitHub issue #5480.
   * <p>
   * A node inline {@code WHERE} predicate - the {@code WHERE n.v = 2} in {@code (n:A WHERE n.v = 2)} -
   * must filter candidate nodes in plain {@code MATCH}, in {@code EXISTS {}} / {@code COUNT {}}
   * subqueries and in pattern comprehensions. Before the fix the predicate was parsed but silently
   * discarded by the pattern-comprehension parser path, so every candidate node matched.
   * <p>
   * {@code shortestPath} is deliberately not covered here: that evaluator ignores inline predicates
   * altogether and is tracked by issue #5481.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5480NodeInlineWhere {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5480-node-inline-where").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:2}), (c:A {v:3})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:2}), (c:A {v:3}) "
            + "CREATE (a)-[:E]->(b), (a)-[:E]->(c)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private List<Integer> queryInts(final String query, final String field) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Number n = row.getProperty(field);
          values.add(n == null ? null : n.intValue());
        }
      }
      return values;
    }

    private int queryInt(final String query, final String field) {
      final List<Integer> values = queryInts(query, field);
      assertThat(values).hasSize(1);
      return values.get(0);
    }

    // ---------------------------------------------------------------- plain MATCH

    @Test
    void plainMatchAppliesNodeInlineWhere() {
      assertThat(queryInts("MATCH (n:A WHERE n.v = 2) RETURN n.v AS v ORDER BY v", "v")).containsExactly(2);
    }

    @Test
    void plainMatchAppliesNodeInlineWhereOnEveryNodeOfThePath() {
      assertThat(queryInts("MATCH (a:A WHERE a.v = 1)-[:E]->(b:A WHERE b.v = 3) RETURN b.v AS v", "v"))
          .containsExactly(3);
    }

    @Test
    void plainMatchCombinesNodeInlineWhereWithClauseWhere() {
      assertThat(queryInts("MATCH (n:A WHERE n.v > 1) WHERE n.v < 3 RETURN n.v AS v", "v")).containsExactly(2);
    }

    // ---------------------------------------------------------------- EXISTS {}

    @Test
    void existsSubqueryAppliesNodeInlineWhere() {
      assertThat(queryInts("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[:E]->(x:A WHERE x.v = 99) } RETURN a.v AS v", "v"))
          .isEmpty();
      assertThat(queryInts("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[:E]->(x:A WHERE x.v = 2) } RETURN a.v AS v", "v"))
          .containsExactly(1);
    }

    @Test
    void existsSubqueryWithExplicitMatchAppliesNodeInlineWhere() {
      assertThat(queryInts(
          "MATCH (a:A {v:1}) WHERE EXISTS { MATCH (a)-[:E]->(x:A WHERE x.v = 99) } RETURN a.v AS v", "v")).isEmpty();
      assertThat(queryInts(
          "MATCH (a:A {v:1}) WHERE EXISTS { MATCH (a)-[:E]->(x:A WHERE x.v = 3) } RETURN a.v AS v", "v"))
          .containsExactly(1);
    }

    @Test
    void countSubqueryAppliesNodeInlineWhere() {
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN COUNT { (a)-[:E]->(x:A WHERE x.v = 2) } AS c", "c")).isEqualTo(1);
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN COUNT { (a)-[:E]->(x:A) } AS c", "c")).isEqualTo(2);
    }

    // ---------------------------------------------------------------- pattern comprehension

    @Test
    void patternComprehensionAppliesNodeInlineWhere() {
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A WHERE x.v = 2) | x]) AS c", "c")).isEqualTo(1);
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(2);
    }

    @Test
    void patternComprehensionAppliesNodeInlineWhereOnTheAnchorNode() {
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a WHERE a.v = 99)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(0);
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a WHERE a.v = 1)-[:E]->(x:A) | x]) AS c", "c")).isEqualTo(2);
    }

    @Test
    void patternComprehensionAppliesNodeInlineWhereOnAVariableLengthHop() {
      // The evaluation row is reused across candidates of one expansion, so a predicate that matches
      // only some of them must still reject the others.
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E*1..2]->(x:A WHERE x.v = 3) | x]) AS c", "c"))
          .isEqualTo(1);
      assertThat(queryInt("MATCH (a:A {v:1}) RETURN size([(a)-[:E*1..2]->(x:A) | x]) AS c", "c")).isEqualTo(2);
    }

    @Test
    void patternComprehensionCombinesNodeInlineWhereWithTrailingWhere() {
      assertThat(queryInt(
          "MATCH (a:A {v:1}) RETURN size([(a)-[:E]->(x:A WHERE x.v > 1) WHERE x.v < 3 | x]) AS c", "c")).isEqualTo(1);
    }
  }

  /**
   * Regression test for GitHub issue #5489.
   * <p>
   * A node inline {@code WHERE} predicate may reference the relationship variable bound by the same
   * hop, as in {@code [(a)-[r:E]->(x:A WHERE x.v > r.w) | x]}. In a pattern comprehension the
   * predicate was evaluated against a row copied from the bindings visible <em>before</em> the hop,
   * which does not yet contain {@code r}, so the reference resolved to null and the predicate rejected
   * every candidate. The comprehension returned an empty list while the equivalent {@code MATCH}
   * spelling returned the correct rows.
   * <p>
   * The relationship variable must be visible to the end node's predicate, matching the {@code MATCH}
   * spelling, which hoists the inline predicate into the clause {@code WHERE} where {@code r} is bound.
   */
  @Nested
  class Issue5489NodeInlineWhereRelVariable {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5489-node-inline-where-rel-var").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}) "
            + "CREATE (a)-[:E {tag:'ok', w:5}]->(b), (a)-[:E {tag:'bad', w:99}]->(c), (b)-[:E {tag:'ok', w:1}]->(d)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    /** Returns the single list-valued projection of a one-row result, as ints. */
    private List<Integer> comprehension(final String query) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as("query returned no row: %s", query).isTrue();
        final Result row = rs.next();
        final List<Object> list = row.getProperty("vs");
        assertThat(list).as("projection 'vs' was not a list: %s", query).isNotNull();
        for (final Object o : list)
          values.add(o == null ? null : ((Number) o).intValue());
      }
      return values;
    }

    private List<Integer> queryInts(final String query, final String field) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Number n = row.getProperty(field);
          values.add(n == null ? null : n.intValue());
        }
      }
      return values;
    }

    // ------------------------------------------------- the defect: same-hop relationship variable

    @Test
    void comprehensionNodeInlineWhereSeesSameHopRelationshipVariable() {
      // b.v = 10 > w = 5 matches; c.v = 2 > w = 99 does not.
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
          .containsExactly(10);
    }

    @Test
    void comprehensionNodeInlineWhereReferencingOnlyTheRelationshipVariable() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE r.w = 5) | x.v] AS vs"))
          .containsExactly(10);
    }

    @Test
    void comprehensionNodeInlineWhereOnRelationshipVariableRejectingEveryCandidate() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE r.w = 12345) | x.v] AS vs"))
          .isEmpty();
    }

    @Test
    void comprehensionCombinesRelationshipInlineWhereWithNodeInlineWhereOnTheSameVariable() {
      assertThat(comprehension(
          "MATCH (a:A {v:1}) RETURN [(a)-[r:E WHERE r.tag = 'ok']->(x:A WHERE x.v > r.w) | x.v] AS vs"))
          .containsExactly(10);
    }

    // ------------------------------------------------- parity with the MATCH spelling

    @Test
    void matchSpellingAgreesWithTheComprehension() {
      final List<Integer> viaMatch = queryInts(
          "MATCH (a:A {v:1})-[r:E]->(x:A WHERE x.v > r.w) RETURN x.v AS v", "v");
      final List<Integer> viaComprehension = comprehension(
          "MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs");
      assertThat(viaMatch).containsExactly(10);
      assertThat(viaComprehension).containsExactlyElementsOf(viaMatch);
    }

    // ------------------------------------------------- controls that must keep working

    @Test
    void comprehensionNodeInlineWhereWithoutRelationshipReferenceStillWorks() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > 5) | x.v] AS vs"))
          .containsExactly(10);
    }

    @Test
    void comprehensionRelationshipInlineWhereStillWorks() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E WHERE r.w = 5]->(x:A) | x.v] AS vs"))
          .containsExactly(10);
    }

    @Test
    void comprehensionNodeInlineWhereSeesRelationshipVariableFromAnEarlierHop() {
      // a -[r1 w=5]-> b -[r2]-> d(v=7): 7 > 5 matches. The other branch (c) has no outgoing edge.
      assertThat(comprehension(
          "MATCH (a:A {v:1}) RETURN [(a)-[r1:E]->(m:A)-[r2:E]->(x:A WHERE x.v > r1.w) | x.v] AS vs"))
          .containsExactly(7);
    }

    @Test
    void comprehensionNodeInlineWhereOnAVariableLengthHopSeesTheRelationshipVariable() {
      // The relationship variable of a variable-length hop is the list walked, as in MATCH (issue #9828), so the
      // predicate reads its element: r.w is the TypeError the MATCH spelling raises too.
      assertThat(comprehension(
          "MATCH (a:A {v:1}) RETURN [(a)-[r:E*1..1]->(x:A WHERE x.v > r[0].w) | x.v] AS vs"))
          .containsExactly(10);
      assertThatThrownBy(() -> comprehension(
          "MATCH (a:A {v:1}) RETURN [(a)-[r:E*1..1]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
          .hasMessageContaining("TypeError");
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher",
            "MATCH (a:A {v:1})-[r:E*1..1]->(x:A WHERE x.v > r.w) RETURN x.v AS v")) {
          while (rs.hasNext())
            rs.next();
        }
      }).hasMessageContaining("TypeError");
    }

    /**
     * An anonymous relationship binds no variable, so a predicate that names one is referencing an
     * undefined variable. It resolves to null and the predicate rejects every candidate, which is the
     * engine's pre-existing behavior for any undefined variable rather than anything this hop changes.
     */
    @Test
    void anonymousRelationshipLeavesAPredicateVariableUndefined() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
          .isEmpty();
      // Naming the relationship is what makes the very same predicate resolve.
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A WHERE x.v > r.w) | x.v] AS vs"))
          .containsExactly(10);
    }

    @Test
    void comprehensionWithoutAnyInlineWhereIsUnaffected() {
      assertThat(comprehension("MATCH (a:A {v:1}) RETURN [(a)-[r:E]->(x:A) | x.v] AS vs"))
          .containsExactlyInAnyOrder(10, 2);
    }
  }

  /**
   * Regression test for GitHub issue #5490.
   * <p>
   * A relationship inline {@code WHERE} predicate on a variable-length pattern, the
   * {@code WHERE r.tag = 'ok'} in {@code -[r:E*1..2 WHERE r.tag = 'ok']->}, was discarded: neither the
   * {@code MATCH} executor nor the {@code EXISTS {}} evaluator handed it to the traverser, so
   * {@code WHERE false} still returned every row. The fixed-length spelling and the variable-length
   * property-map spelling were unaffected, as were pattern comprehensions.
   * <p>
   * Semantics: as with the inline property map, every relationship traversed by the path must satisfy
   * the predicate, which is the same rule as the clause-level {@code all(e IN r WHERE ...)} spelling.
   */
  @Nested
  class Issue5490VarLengthRelInlineWhere {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5490-varlen-rel-inline-where").create();
      database.transaction(() -> {
        database.command("opencypher", "CREATE (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}), (e:A {v:8})");
        database.command("opencypher", "MATCH (a:A {v:1}), (b:A {v:10}), (c:A {v:2}), (d:A {v:7}), (e:A {v:8}) "
            + "CREATE (a)-[:E {tag:'ok', w:5}]->(b), (a)-[:E {tag:'bad', w:99}]->(c), "
            + "(b)-[:E {tag:'ok', w:1}]->(d), (c)-[:E {tag:'ok', w:2}]->(e)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as("query returned no row: %s", query).isTrue();
        final Number n = rs.next().getProperty("c");
        return n == null ? 0L : n.longValue();
      }
    }

    private List<Integer> queryInts(final String query, final String field) {
      final List<Integer> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Number n = row.getProperty(field);
          values.add(n == null ? null : n.intValue());
        }
      }
      return values;
    }

    // ---------------------------------------------------------------- MATCH, variable length

    @Test
    void matchVariableLengthAppliesTheRelationshipInlineWhere() {
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) RETURN count(*) AS c")).isEqualTo(1);
    }

    @Test
    void matchVariableLengthHonorsAnAlwaysFalsePredicate() {
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE false]->(x:A) RETURN count(*) AS c")).isZero();
    }

    @Test
    void matchVariableLengthHonorsAnAlwaysTruePredicate() {
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE true]->(x:A) RETURN count(*) AS c")).isEqualTo(2);
    }

    /**
     * Every relationship of the path must satisfy the predicate. From a the all-'ok' paths are
     * a-&gt;b (one hop) and a-&gt;b-&gt;d (two hops). The a-&gt;c edge is 'bad', so neither a-&gt;c nor
     * a-&gt;c-&gt;e survives even though the second edge of the latter is 'ok'.
     */
    @Test
    void matchVariableLengthRequiresEveryRelationshipToSatisfyThePredicate() {
      assertThat(queryInts("MATCH (a:A {v:1})-[r:E*1..2 WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v"))
          .containsExactly(7, 10);
    }

    @Test
    void matchUnboundedVariableLengthAppliesThePredicate() {
      assertThat(queryInts("MATCH (a:A {v:1})-[r:E* WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v"))
          .containsExactly(7, 10);
    }

    @Test
    void matchVariableLengthPredicateSeesOuterBindings() {
      // r.w = 5 satisfies w < a.v + 10 = 11; the 'bad' edge's w = 99 does not.
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 WHERE r.w < a.v + 10]->(x:A) RETURN count(*) AS c")).isEqualTo(1);
    }

    @Test
    void matchVariableLengthAgreesWithTheClauseLevelAllSpelling() {
      final List<Integer> viaInline = queryInts(
          "MATCH (a:A {v:1})-[r:E*1..2 WHERE r.tag = 'ok']->(x:A) RETURN x.v AS v ORDER BY v", "v");
      final List<Integer> viaClause = queryInts(
          "MATCH (a:A {v:1})-[r:E*1..2]->(x:A) WHERE all(e IN r WHERE e.tag = 'ok') RETURN x.v AS v ORDER BY v", "v");
      assertThat(viaInline).containsExactlyElementsOf(viaClause);
    }

    // ---------------------------------------------------------------- EXISTS {}, variable length

    @Test
    void existsVariableLengthAppliesTheRelationshipInlineWhere() {
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 WHERE r.tag = 'nope']->(x:A) } "
          + "RETURN count(*) AS c")).isZero();
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) } "
          + "RETURN count(*) AS c")).isEqualTo(1);
    }

    @Test
    void existsVariableLengthAppliesTheRelationshipPropertyMap() {
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 {tag:'nope'}]->(x:A) } "
          + "RETURN count(*) AS c")).isZero();
      assertThat(count("MATCH (a:A {v:1}) WHERE EXISTS { (a)-[r:E*1..1 {tag:'ok'}]->(x:A) } "
          + "RETURN count(*) AS c")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- controls that must keep working

    @Test
    void fixedLengthRelationshipInlineWhereStillWorks() {
      assertThat(count("MATCH (a:A {v:1})-[r:E WHERE r.tag = 'ok']->(x:A) RETURN count(*) AS c")).isEqualTo(1);
    }

    @Test
    void variableLengthPropertyMapStillWorks() {
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1 {tag:'ok'}]->(x:A) RETURN count(*) AS c")).isEqualTo(1);
    }

    @Test
    void patternComprehensionVariableLengthStillWorks() {
      assertThat(count("MATCH (a:A {v:1}) RETURN size([(a)-[r:E*1..1 WHERE r.tag = 'ok']->(x:A) | x.v]) AS c"))
          .isEqualTo(1);
    }

    @Test
    void variableLengthWithoutAnyPredicateIsUnaffected() {
      assertThat(count("MATCH (a:A {v:1})-[r:E*1..1]->(x:A) RETURN count(*) AS c")).isEqualTo(2);
    }
  }
}
