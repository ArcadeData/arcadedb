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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relationship matching semantics: uniqueness across comma patterns, self loops and relationship sub-type labels.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherRelationshipMatchingTest {
  /**
   * Issue #5904 reported that SQL {@code expand(both('E'))} and Cypher {@code -[:E]-} disagree on how many
   * times a self-loop is visited: SQL returns the vertex twice, Cypher once. This is not a bug to reconcile -
   * it is two query languages each matching their own reference implementation's documented semantics:
   * <p>
   * - SQL's {@code both()} mirrors Apache TinkerPop/Gremlin's {@code both()} step, defined as the union of
   * {@code out()} and {@code in()}. A self-loop is a member of both sets, so the union legitimately contains
   * it twice - the same behavior any Gremlin-based graph database exhibits.
   * <p>
   * - Cypher's undirected {@code -[]-} hop was deliberately fixed to match Neo4j, which returns each
   * relationship once regardless of direction (issue #5446 / PR #5447 for the cost-based operators
   * {@code ExpandAll}/{@code GAVExpandAll}, issue #5456 for pattern comprehensions, the latter citing
   * verification against Neo4j 2026.05). Reverting that would regress two already Neo4j-verified fixes and
   * the openCypher TCK conformance they preserved.
   * <p>
   * Forcing the two engines to agree would require breaking whichever one currently matches its own reference
   * implementation. This test locks in the divergence as intentional so it is not "fixed" by accident.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5904SqlCypherSelfLoopBothDivergence extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        final MutableVertex d = database.newVertex("N").set("name", "d").save();
        d.newEdge("E", d, "name", "loop").save();
      });
    }

    @Test
    void sqlBothUnionsOutAndInSoASelfLoopIsVisitedTwice() {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(both('E')) FROM N WHERE name = 'd'")) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().<String>getProperty("name"));
        assertThat(names).containsExactly("d", "d");
      }
    }

    @Test
    void sqlOutAndInEachVisitTheSelfLoopOnce() {
      try (final ResultSet rs = database.query("sql", "SELECT expand(out('E')) FROM N WHERE name = 'd'")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT expand(in('E')) FROM N WHERE name = 'd'")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void cypherUndirectedMatchVisitsTheSelfLoopOnceMatchingNeo4j() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:N {name:'d'})-[:E]-(x) RETURN x.name AS name")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void cypherDirectedMatchesEachVisitTheSelfLoopOnce() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:N {name:'d'})-[:E]->(x) RETURN x.name AS name")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (x)-[:E]->(p:N {name:'d'}) RETURN x.name AS name")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("d");
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  /**
   * Issue #9124: {@code WHERE r:R} compared relationship type names for equality while the pattern {@code [r:R]} is
   * polymorphic, so a relationship of a type that extends {@code R} was returned by the pattern and not by the predicate.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9124RelationshipSubtypeLabelCheck extends TestHelper {

    @Override
    public void beginTest() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE N9124");
        database.command("sql", "CREATE EDGE TYPE R9124");
        database.command("sql", "CREATE EDGE TYPE R9124Sub EXTENDS R9124");
        database.command("sql", "CREATE EDGE TYPE Other9124");
        database.command("sql", "CREATE VERTEX N9124 SET id = 1");
        database.command("sql", "CREATE VERTEX N9124 SET id = 2");
        for (final String edge : new String[] { "R9124", "R9124Sub", "Other9124" })
          database.command("sql", "CREATE EDGE " + edge + " FROM (SELECT FROM N9124 WHERE id = 1) TO (SELECT FROM N9124 WHERE id = 2)");
      });
    }

    private List<String> types(final String query) {
      final List<String> types = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          types.add(rs.next().getProperty("t"));
      }
      return types.stream().sorted().toList();
    }

    @Test
    void patternAndPredicateAgreeOnSubtypes() {
      final List<String> pattern = types("MATCH ()-[r:R9124]->() RETURN type(r) AS t");
      assertThat(pattern).containsExactly("R9124", "R9124Sub");
      assertThat(types("MATCH ()-[r]->() WHERE r:R9124 RETURN type(r) AS t")).isEqualTo(pattern);
    }

    @Test
    void disjunctionAgreesWithThePatternToo() {
      assertThat(types("MATCH ()-[r:R9124|Other9124]->() RETURN type(r) AS t")).containsExactly("Other9124", "R9124", "R9124Sub");
      assertThat(types("MATCH ()-[r]->() WHERE r:R9124|Other9124 RETURN type(r) AS t")).containsExactly("Other9124", "R9124", "R9124Sub");
    }

    @Test
    void aTwoLevelSubtypeChainMatches() {
      database.transaction(() -> {
        database.command("sql", "CREATE EDGE TYPE R9124Leaf EXTENDS R9124Sub");
        database.command("sql", "CREATE EDGE R9124Leaf FROM (SELECT FROM N9124 WHERE id = 1) TO (SELECT FROM N9124 WHERE id = 2)");
      });
      assertThat(types("MATCH ()-[r:R9124]->() RETURN type(r) AS t")).containsExactly("R9124", "R9124Leaf", "R9124Sub");
      assertThat(types("MATCH ()-[r]->() WHERE r:R9124 RETURN type(r) AS t")).containsExactly("R9124", "R9124Leaf", "R9124Sub");
      assertThat(types("MATCH ()-[r]->() WHERE r:R9124Sub RETURN type(r) AS t")).containsExactly("R9124Leaf", "R9124Sub");
    }

    @Test
    void aSiblingTypeIsStillRefused() {
      assertThat(types("MATCH ()-[r]->() WHERE r:R9124Sub RETURN type(r) AS t")).containsExactly("R9124Sub");
    }
  }

  /**
   * Issue #9485: openCypher requires the relationships of one {@code MATCH} clause, comma separated parts included, to be
   * distinct. The pair-join count push-down counted the adjacency paths instead, so a self loop let two hops land on one
   * relationship and the count came out too high. Separate {@code MATCH} clauses have no such rule and keep counting the
   * repeated relationship.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9485CommaPatternRelationshipUniqueness extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE E");
    }

    @Test
    void graphWithoutSelfLoop() {
      build(false);
      assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(a) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (b)-[:E]->(c:P) MATCH (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P), (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(3L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P)<-[:E]-(a) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]-(b:P), (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]-(b:P) MATCH (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(6L);
    }

    @Test
    void graphWithSelfLoop() {
      build(true);
      // the triple a, b, c is the only one with three distinct relationships: a>b, b>c, a>c
      assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P)-[:E]->(c:P), (a)-[:E]->(c) RETURN count(a) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (a:P)-[r1:E]->(b:P)-[r2:E]->(c:P), (a)-[r3:E]->(c) WITH a, b, c, r1, r2, r3 RETURN count(*) AS n"))
          .isEqualTo(1L);
      // separate MATCH clauses have no uniqueness rule across them
      assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (b)-[:E]->(c:P) MATCH (a)-[:E]->(c) RETURN count(*) AS n")).isEqualTo(4L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P), (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P) MATCH (a)-[:E]->(b) RETURN count(*) AS n")).isEqualTo(4L);
      assertThat(count("MATCH (a:P)-[:E]->(b:P)<-[:E]-(a) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]-(b:P), (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[r:E]-(b:P), (a)-[s:E]-(b) WITH a, b, r, s RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (a:P)-[:E]-(b:P) MATCH (a)-[:E]-(b) RETURN count(*) AS n")).isEqualTo(7L);
    }

    @Test
    void commaStarOverOneEdgeTypeNeverUsesAnEdgeTwice() {
      // a star whose two arms are the same type: with the arms free to coincide, a vertex with 2 out edges counts 2 * 2 = 4
      // rows instead of the 2 ordered pairs of distinct relationships
      database.transaction(() -> {
        final MutableVertex s = database.newVertex("P").set("id", 1).save();
        s.newEdge("E", database.newVertex("P").set("id", 2).save());
        s.newEdge("E", database.newVertex("P").set("id", 3).save());
      });
      assertThat(count("MATCH (s:P)-[:E]->(t:P), (s)-[:E]->(u:P) RETURN count(*) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (s:P)-[:E]->(t:P), (s)-[:E]->(u:P) RETURN count(s) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (s:P)-[:E]->(t:P) MATCH (s)-[:E]->(u:P) RETURN count(*) AS n")).isEqualTo(4L);
    }

    @Test
    void disjointEdgeTypesKeepThePushDown() {
      database.command("sql", "CREATE VERTEX TYPE Comment");
      database.command("sql", "CREATE VERTEX TYPE Post");
      database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
      database.command("sql", "CREATE EDGE TYPE REPLY_OF");
      database.command("sql", "CREATE EDGE TYPE KNOWS");
      database.transaction(() -> {
        final MutableVertex p1 = database.newVertex("P").save();
        final MutableVertex p2 = database.newVertex("P").save();
        final MutableVertex post = database.newVertex("Post").save();
        final MutableVertex comment = database.newVertex("Comment").save();
        p1.newEdge("KNOWS", p2);
        comment.newEdge("HAS_CREATOR", p1);
        comment.newEdge("REPLY_OF", post);
        post.newEdge("HAS_CREATOR", p2);
      });
      final String query = "MATCH (p1:P)-[:KNOWS]-(p2:P), (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2) "
          + "RETURN count(*) AS n";
      assertThat(count(query)).isEqualTo(1L);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("COUNT PAIR JOIN");
      }
    }

    @Test
    void disjointEdgeTypesKeepTheStarPushDown() {
      database.command("sql", "CREATE VERTEX TYPE Message");
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE EDGE TYPE HAS_TAG");
      database.command("sql", "CREATE EDGE TYPE HAS_CREATOR");
      database.command("sql", "CREATE EDGE TYPE LIKES");
      database.transaction(() -> {
        final MutableVertex m = database.newVertex("Message").save();
        m.newEdge("HAS_TAG", database.newVertex("Tag").save());
        m.newEdge("HAS_CREATOR", database.newVertex("P").save());
        database.newVertex("P").save().newEdge("LIKES", m);
        database.newVertex("P").save().newEdge("LIKES", m);
      });
      final String query = "MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:P), (m)<-[:LIKES]-(:P) RETURN count(*) AS n";
      assertThat(count(query)).isEqualTo(2L);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("COUNT STAR JOIN");
      }
    }

    private void build(final boolean selfLoop) {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("id", 1).save();
        final MutableVertex b = database.newVertex("P").set("id", 2).save();
        final MutableVertex c = database.newVertex("P").set("id", 3).save();
        a.newEdge("E", b);
        b.newEdge("E", c);
        a.newEdge("E", c);
        if (selfLoop)
          b.newEdge("E", b);
      });
      reopenDatabase();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }
}
