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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * APOC's relationship pattern syntax ({@code 'KNOWS>|<LIKES'}) in the functions and procedures that take one.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherApocRelationshipPatternTest {

  /**
   * Regression tests for issue #12103: {@code node.degree}, {@code node.relationship.exists}, {@code node.relationship.types} and
   * the {@code algo.dijkstra} / {@code algo.aStar} / {@code algo.allSimplePaths} path finders read the whole pattern as one type
   * name: {@code apoc.node.degree(a, 'KNOWS>')} answered 0 and {@code apoc.algo.dijkstra(a, b, 'KNOWS>', 'w')} refused
   * {@code 'KNOWS>'} as a type that does not exist. The expected values are what Neo4j 5.26 with APOC 5.26.33 answers on the
   * same graph: {@code (a)-[:KNOWS {w: 1}]->(b)}, {@code (a)-[:LIKES {w: 5}]->(b)}, {@code (b)-[:KNOWS {w: 2}]->(a)}.
   */
  @Nested
  class Issue12103ApocRelationshipPattern extends TestHelper {

    @BeforeEach
    void createGraph() {
      database.transaction(() -> database.command("opencypher", """
          CREATE (a:P {uid: 1})-[:KNOWS {w: 1.0}]->(b:P {uid: 2}), (a)-[:LIKES {w: 5.0}]->(b), (b)-[:KNOWS {w: 2.0}]->(a)"""));
    }

    @Test
    void nodeDegreeReadsDirectionsAndAlternatives() {
      assertThat(longOf("RETURN apoc.node.degree(a) AS r")).isEqualTo(3L);
      assertThat(longOf("RETURN apoc.node.degree(a, 'KNOWS>') AS r")).isEqualTo(1L);
      assertThat(longOf("RETURN apoc.node.degree(a, '<KNOWS') AS r")).isEqualTo(1L);
      assertThat(longOf("RETURN apoc.node.degree(a, 'KNOWS|LIKES') AS r")).isEqualTo(3L);
      assertThat(longOf("RETURN apoc.node.degree(a, '>') AS r")).isEqualTo(2L);
      assertThat(longOf("RETURN apoc.node.degree(a, '') AS r")).isEqualTo(3L);
      assertThat(longOf("RETURN apoc.node.degree(a, 'KNOWS>|KNOWS') AS r")).isEqualTo(3L);
      assertThat(longOf("RETURN apoc.node.degree(a, 'KNOWS|KNOWS') AS r")).isEqualTo(4L);
      assertThat(longOf("RETURN apoc.node.degree(a, 'NOPE') AS r")).isZero();
      // a blank type list is every type for the one-direction degrees too
      assertThat(longOf("RETURN apoc.node.degree.in(a, '') AS r")).isEqualTo(1L);
      assertThat(longOf("RETURN apoc.node.degree.out(a, '') AS r")).isEqualTo(2L);
      // ArcadeDB's direction argument restricts the alternatives without a marker
      assertThat(longOf("RETURN node.degree(a, 'KNOWS', 'OUT') AS r")).isEqualTo(1L);
    }

    @Test
    void relationshipExistsReadsTheSamePattern() {
      assertThat(boolOf("RETURN apoc.node.relationship.exists(a, 'LIKES>') AS r")).isTrue();
      assertThat(boolOf("RETURN apoc.node.relationship.exists(a, '<LIKES') AS r")).isFalse();
      assertThat(boolOf("RETURN apoc.node.relationship.exists(a, 'NOPE|LIKES>') AS r")).isTrue();
      assertThat(boolOf("RETURN apoc.node.relationship.exists(a, '') AS r")).isTrue();
      assertThat(boolOf("MATCH (b:P {uid: 2}) WITH b RETURN apoc.node.relationship.exists(b, 'LIKES>') AS r")).isFalse();
    }

    @Test
    void relationshipTypesTakesAPatternNotADirection() {
      assertThat(listOf("RETURN apoc.node.relationship.types(a, 'KNOWS') AS r")).containsExactly("KNOWS");
      assertThat(listOf("RETURN apoc.node.relationship.types(a, 'OUT') AS r")).isEmpty();
      assertThat(listOf("RETURN apoc.node.relationship.types(a, 'LIKES|KNOWS') AS r")).containsExactly("LIKES", "KNOWS");
      assertThat(listOf("RETURN apoc.node.relationship.types(a, 'LIKES>') AS r")).containsExactly("LIKES");
      assertThat(listOf("RETURN apoc.node.relationship.types(a, '<LIKES') AS r")).isEmpty();
      assertThat(listOf("RETURN apoc.node.relationship.types(a, '') AS r")).containsExactlyInAnyOrder("KNOWS", "LIKES");
      // the direction moved to the third argument
      assertThat(listOf("RETURN node.relationship.types(a, null, 'IN') AS r")).containsExactly("KNOWS");
    }

    @Test
    void dijkstraReadsThePatternAndTheDefaultWeight() {
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, 'KNOWS>', 'w') YIELD weight RETURN weight AS r")).isEqualTo(1.0);
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, '<KNOWS', 'w') YIELD weight RETURN weight AS r")).isEqualTo(2.0);
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, 'LIKES', 'w') YIELD weight RETURN weight AS r")).isEqualTo(5.0);
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, 'KNOWS|LIKES', 'w') YIELD weight RETURN weight AS r")).isEqualTo(1.0);
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, '', 'w') YIELD weight RETURN weight AS r")).isEqualTo(1.0);
      // the alternatives mix directions: LIKES outgoing (w 5) or KNOWS incoming, the (b)-[:KNOWS {w: 2}]->(a) one: 2 is cheaper
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, 'LIKES>|<KNOWS', 'w') YIELD weight RETURN weight AS r")).isEqualTo(2.0);
      // the fifth argument is APOC's defaultWeight, the weight of a relationship without the weight property
      assertThat(weight("CALL apoc.algo.dijkstra(a, b, 'KNOWS', 'missing', 2.5) YIELD weight RETURN weight AS r")).isEqualTo(2.5);
      // a text there is still ArcadeDB's direction
      assertThat(weight("CALL algo.dijkstra(a, b, 'KNOWS', 'w', 'IN') YIELD weight RETURN weight AS r")).isEqualTo(2.0);
    }

    @Test
    void aStarAndAllSimplePathsReadThePattern() {
      assertThat(weight("CALL apoc.algo.aStar(a, b, 'KNOWS>', 'w', null, null) YIELD weight RETURN weight AS r")).isEqualTo(1.0);
      assertThat(weight("CALL apoc.algo.aStar(a, b, '<KNOWS', 'w', null, null) YIELD weight RETURN weight AS r")).isEqualTo(2.0);

      assertThat(paths("CALL apoc.algo.allSimplePaths(a, b, '>', 3) YIELD path RETURN [r IN relationships(path) | type(r)] AS t"))
          .containsExactlyInAnyOrder(List.of("KNOWS"), List.of("LIKES"));
      assertThat(paths("CALL apoc.algo.allSimplePaths(a, b, '', 3) YIELD path RETURN [r IN relationships(path) | type(r)] AS t"))
          .containsExactlyInAnyOrder(List.of("KNOWS"), List.of("LIKES"), List.of("KNOWS"));
      assertThat(paths("CALL apoc.algo.allSimplePaths(a, b, '<LIKES', 3) YIELD path RETURN [r IN relationships(path) | type(r)] AS t"))
          .isEmpty();
      assertThat(paths("CALL apoc.algo.allSimplePaths(a, b, 'KNOWS>', 3) YIELD path RETURN [r IN relationships(path) | type(r)] AS t"))
          .containsExactly(List.of("KNOWS"));
    }

    private long longOf(final String returnClause) {
      return ((Number) single("MATCH (a:P {uid: 1}) WITH a " + returnClause)).longValue();
    }

    private boolean boolOf(final String query) {
      return (Boolean) single(query.startsWith("MATCH") ? query : "MATCH (a:P {uid: 1}) WITH a " + query);
    }

    @SuppressWarnings("unchecked")
    private List<Object> listOf(final String returnClause) {
      return (List<Object>) single("MATCH (a:P {uid: 1}) WITH a " + returnClause);
    }

    private double weight(final String call) {
      return ((Number) single("MATCH (a:P {uid: 1}), (b:P {uid: 2}) " + call)).doubleValue();
    }

    private List<Object> paths(final String call) {
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:P {uid: 1}), (b:P {uid: 2}) " + call)) {
        return rs.stream().map(r -> r.getProperty("t")).toList();
      }
    }

    private Object single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final List<Result> rows = rs.stream().toList();
        assertThat(rows).as(query).hasSize(1);
        return rows.getFirst().getProperty("r");
      }
    }
  }
}
