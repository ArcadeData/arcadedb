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
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/13574: a {@code WHERE} predicate on the path variable of a
 * {@code shortestPath()} / {@code allShortestPaths()} pattern restricts the paths the search may answer (Neo4j answers the
 * shortest path that satisfies it), instead of filtering the one path the search found first.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherShortestPathWherePredicateIssue13574Test extends TestHelper {
  private static final String ISSUE_QUERY = """
      MATCH p = shortestPath((:Person {name:'K'})-[*]-(:Person {name:'A'}))
      WHERE any(n IN nodes(p) WHERE n.title = $m)
      RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Movie");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> database.command("opencypher", """
        CREATE (k:Person {name:'K'})-[:R]->(m1:Movie {title:'M1'})<-[:R]-(a:Person {name:'A'}),
               (k)-[:R]->(m2:Movie {title:'M2'})<-[:R]-(a),
               (k)-[:R]->(x:Movie {title:'X'})-[:R]->(m3:Movie {title:'M3'})-[:R]->(a)"""));
  }

  @Test
  void bothTiedShortestPathsAreFoundByTheirPredicate() {
    assertThat(namesOf(ISSUE_QUERY, Map.of("m", "M1"))).containsExactly(List.of("K", "M1", "A"));
    assertThat(namesOf(ISSUE_QUERY, Map.of("m", "M2"))).containsExactly(List.of("K", "M2", "A"));
  }

  @Test
  void sizeOfListComprehensionPredicateIsHonouredToo() {
    final String query = """
        MATCH p = shortestPath((p1:Person {name:'K'})-[*]-(p2:Person {name:'A'}))
        WHERE size([n IN nodes(p) WHERE n.title = $m]) > 0
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(query, Map.of("m", "M1"))).containsExactly(List.of("K", "M1", "A"));
    assertThat(namesOf(query, Map.of("m", "M2"))).containsExactly(List.of("K", "M2", "A"));
  }

  @Test
  void predicateForcesALongerShortestPath() {
    // No path of length 2 goes through M3: the shortest that does is K-X-M3-A, of length 3
    assertThat(namesOf(ISSUE_QUERY, Map.of("m", "M3"))).containsExactly(List.of("K", "X", "M3", "A"));
  }

  @Test
  void allShortestPathsAnswersEveryPassingPathOfTheFirstLengthThatHasOne() {
    final String query = """
        MATCH p = allShortestPaths((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        WHERE none(n IN nodes(p) WHERE coalesce(n.title, '') IN ['M1', 'M2'])
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(query, Map.of())).containsExactly(List.of("K", "X", "M3", "A"));

    final String twoOfThree = """
        MATCH p = allShortestPaths((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        WHERE length(p) >= 2 AND none(n IN nodes(p) WHERE coalesce(n.title, '') = 'X')
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(twoOfThree, Map.of())).containsExactlyInAnyOrder(List.of("K", "M1", "A"), List.of("K", "M2", "A"));
  }

  @Test
  void predicateThatRejectsEveryPathAnswersNoRow() {
    assertThat(namesOf(ISSUE_QUERY, Map.of("m", "nope"))).isEmpty();

    final String all = """
        MATCH p = allShortestPaths((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        WHERE any(n IN nodes(p) WHERE n.title = 'nope')
        RETURN p""";
    assertThat(namesOf(all.replace("RETURN p", "RETURN [n IN nodes(p) | n.name] AS names"), Map.of())).isEmpty();
  }

  @Test
  void hopUpperBoundStillBoundsThePredicateSearch() {
    final String query = """
        MATCH p = shortestPath((:Person {name:'K'})-[*..2]-(:Person {name:'A'}))
        WHERE any(n IN nodes(p) WHERE n.title = $m)
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(query, Map.of("m", "M3"))).isEmpty();
    assertThat(namesOf(query, Map.of("m", "M1"))).containsExactly(List.of("K", "M1", "A"));
  }

  @Test
  void directionAndPredicateOnBoundEndpoints() {
    // Directed K->A: K-M1-A and K-M2-A walk M1/M2->A against the arrows, so the only directed path is K-X-M3-A
    final String directed = """
        MATCH (k:Person {name:'K'}), (a:Person {name:'A'})
        MATCH p = shortestPath((k)-[:R*]->(a))
        WHERE any(n IN nodes(p) WHERE n.title = 'M3')
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(directed, Map.of())).containsExactly(List.of("K", "X", "M3", "A"));

    final String predicateOnPathAndEndpoint = """
        MATCH (k:Person {name:'K'}), (a:Person {name:'A'})
        MATCH p = shortestPath((k)-[:R*]-(a))
        WHERE a.name = 'A' AND any(r IN relationships(p) WHERE startNode(r).title = 'X')
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(predicateOnPathAndEndpoint, Map.of())).containsExactly(List.of("K", "X", "M3", "A"));
  }

  @Test
  void directedShortestPathWithoutPredicate() {
    final String directed = """
        MATCH (k:Person {name:'K'}), (a:Person {name:'A'})
        MATCH p = shortestPath((k)-[:R*]->(a))
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(directed, Map.of())).containsExactly(List.of("K", "X", "M3", "A"));
  }

  @Test
  void predicateCombinesWithInlineRelationshipConstraint() {
    database.transaction(() -> database.command("opencypher", "MATCH ()-[r:R]->(:Movie {title:'M1'}) SET r.blocked = true"));
    final String query = """
        MATCH p = shortestPath((:Person {name:'K'})-[r:R* WHERE r.blocked IS NULL]-(:Person {name:'A'}))
        WHERE any(n IN nodes(p) WHERE n.title IN [$m, 'M3'])
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    // Both M1 relationships are refused by the inline WHERE, so the predicate falls through to the longer M3 path
    assertThat(namesOf(query, Map.of("m", "M1"))).containsExactly(List.of("K", "X", "M3", "A"));
  }

  @Test
  void optionalMatchKeepsTheRowWithANullPathWhenNoPathPasses() {
    final String query = """
        MATCH (k:Person {name:'K'}), (a:Person {name:'A'})
        OPTIONAL MATCH p = shortestPath((k)-[*]-(a))
        WHERE any(n IN nodes(p) WHERE n.title = $m)
        RETURN k.name AS k, [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    try (final ResultSet rs = database.query("opencypher", query, Map.of("m", "M1"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<List<String>>getProperty("names")).containsExactly("K", "M1", "A");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("opencypher", query, Map.of("m", "nope"))) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("k")).isEqualTo("K");
      assertThat(row.<Object>getProperty("names")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void undirectedSelfLoopIsOneRelationshipNotTwoPaths() {
    database.transaction(() -> database.command("opencypher", "MATCH (m:Movie {title:'M1'}) CREATE (m)-[:R]->(m)"));
    final String query = """
        MATCH p = allShortestPaths((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        WHERE length(p) = 3 AND any(n IN nodes(p) WHERE n.title = 'M1')
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names""";
    assertThat(namesOf(query, Map.of())).containsExactly(List.of("K", "M1", "M1", "A"));
  }

  @Test
  void limitStopsTheLazySearch() {
    final String query = """
        MATCH p = allShortestPaths((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        WHERE length(p) = 2
        RETURN [n IN nodes(p) | coalesce(n.name, n.title)] AS names LIMIT 1""";
    assertThat(namesOf(query, Map.of())).hasSize(1);
  }

  @Test
  void stepEvaluatesThePredicateOnlyForThePathsItIsAskedFor() {
    // Two passing paths of length 2 (through M1 and through M2): pulling one row must evaluate the predicate on one candidate only
    final int[] evaluations = new int[1];
    final BooleanExpression countingPredicate = new BooleanExpression() {
      @Override
      public boolean evaluate(final Result result, final CommandContext context) {
        evaluations[0]++;
        return true;
      }

      @Override
      public String getText() {
        return "counting";
      }
    };

    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final Vertex k = database.query("opencypher", "MATCH (k:Person {name:'K'}) RETURN k").next().getProperty("k");
      final Vertex a = database.query("opencypher", "MATCH (a:Person {name:'A'}) RETURN a").next().getProperty("a");
      final AbstractExecutionStep input = new AbstractExecutionStep(context) {
        private boolean done;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
          final InternalResultSet rs = new InternalResultSet();
          if (!done) {
            done = true;
            rs.add(new ResultInternal().setProperty("k", k).setProperty("a", a));
          }
          return rs;
        }
      };

      final ShortestPathPattern pattern = new ShortestPathPattern(
          List.of(new NodePattern("k", null, null), new NodePattern("a", null, null)),
          List.of(new RelationshipPattern(null, List.of("R"), Direction.BOTH, null, 1, 2)), "p", true);
      final ShortestPathStep step = new ShortestPathStep("k", "a", "p", pattern, countingPredicate, context);
      step.setPrevious(input);

      final ResultSet rs = step.syncPull(context, 1);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("p")).isNotNull();
      assertThat(evaluations[0]).isEqualTo(1);
    });
  }

  @Test
  void withoutPredicateTheShortestPathIsUnchanged() {
    final String query = """
        MATCH p = shortestPath((:Person {name:'K'})-[*]-(:Person {name:'A'}))
        RETURN length(p) AS len""";
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("len").intValue()).isEqualTo(2);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private List<List<String>> namesOf(final String query, final Map<String, Object> params) {
    final List<List<String>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("names"));
      }
    }
    return rows;
  }
}
