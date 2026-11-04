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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10433: an openCypher {@code MERGE} of a whole path never matched the path already there when the edge type was declared
 * {@code LIGHTWEIGHT}. With no bound endpoint the match scanned the records of the edge type, and a lightweight edge has none,
 * so every {@code MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})} created both nodes and the edge again. The rows of the report's matrix
 * are pinned for the four ways to declare the type; plain and {@code UNIDIRECTIONAL} types are the control.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10433MergePathLightweightEdgeTest extends TestHelper {

  private static final String[] DECLARATIONS = { "", "LIGHTWEIGHT", "UNIDIRECTIONAL LIGHTWEIGHT", "UNIDIRECTIONAL" };

  private void schema(final String declaration) {
    database.command("sql", "CREATE VERTEX TYPE A").close();
    database.command("sql", "CREATE VERTEX TYPE B").close();
    database.command("sql", "CREATE EDGE TYPE R " + declaration).close();
    database.command("sql", "CREATE EDGE TYPE S " + declaration).close();
  }

  private void write(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void assertCounts(final String what, final long nodes, final long relationships) {
    assertThat(count("MATCH (n) RETURN count(n) AS c")).as(what + ": nodes").isEqualTo(nodes);
    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).as(what + ": relationships").isEqualTo(relationships);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void mergingAPathTwiceCreatesItOnce(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
    write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
    assertCounts(DECLARATIONS[declaration], 2, 1);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void mergingAnExistingCreatedPathFindsIt(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (:A {k: 0})-[:R]->(:B {k: 1})");
    write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
    assertCounts(DECLARATIONS[declaration], 2, 1);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void aRepeatedKeyInOneStatementCreatesItsPathOnce(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("UNWIND [1, 2, 1] AS x MERGE (a:A {k: x})-[:R]->(b:B {k: x})");
    assertCounts(DECLARATIONS[declaration], 4, 2);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void theMatchingEdgeIsFoundBesideOtherEdgesOfTheSameNode(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (n0:A {k: 0}), (n1:B {k: 1}), (n0)-[:R]->(n0), (n0)-[:R]->(n1)");
    write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
    assertCounts(DECLARATIONS[declaration], 2, 2);

    write("CREATE (n0:A {k: 5}), (n1:B {k: 6}), (n2:B {k: 7}), (n0)-[:R]->(n2), (n0)-[:R]->(n1)");
    write("MERGE (a:A {k: 5})-[:R]->(b:B {k: 6})");
    assertCounts(DECLARATIONS[declaration], 5, 4);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void aDifferentRelationshipTypeIsNotTheMergedOne(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (:A {k: 0})-[:S]->(:B {k: 1})");
    write("MERGE (a:A {k: 0})-[:R]->(b:B {k: 1})");
    // THE S EDGE DOES NOT MATCH [:R], SO THE PATH IS CREATED AGAIN WITH ITS OWN NODES
    assertCounts(DECLARATIONS[declaration], 4, 2);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void mergeOfTheRelationshipBetweenBoundEndpointsStillMatches(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (:A {k: 0}), (:B {k: 1})");
    write("MATCH (a:A {k: 0}), (b:B {k: 1}) MERGE (a)-[:R]->(b)");
    write("MATCH (a:A {k: 0}), (b:B {k: 1}) MERGE (a)-[:R]->(b)");
    assertCounts(DECLARATIONS[declaration], 2, 1);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void theMatchedPathIsBoundToItsVariables(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (:A {k: 0, name: 'first'})-[:R]->(:B {k: 1, name: 'second'})");
    try (final ResultSet rs = database.command("opencypher",
        "MERGE p = (a:A {k: 0})-[r:R]->(b:B {k: 1}) RETURN a.name AS a, type(r) AS t, b.name AS b, length(p) AS len")) {
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat((String) row.getProperty("a")).isEqualTo("first");
      assertThat((String) row.getProperty("t")).isEqualTo("R");
      assertThat((String) row.getProperty("b")).isEqualTo("second");
      assertThat(((Number) row.getProperty("len")).intValue()).isEqualTo(1);
      assertThat(rs.hasNext()).as("one matched path, one row").isFalse();
    }
    assertCounts(DECLARATIONS[declaration], 2, 1);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void anIncomingAndAnUndirectedPatternMatchToo(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("CREATE (:A {k: 0})-[:R]->(:B {k: 1})");
    write("MERGE (b:B {k: 1})<-[:R]-(a:A {k: 0})");
    assertCounts(DECLARATIONS[declaration] + " incoming", 2, 1);
    write("MERGE (a:A {k: 0})-[:R]-(b:B {k: 1})");
    assertCounts(DECLARATIONS[declaration] + " undirected", 2, 1);
  }

  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2, 3 })
  void aLongerPathIsMatched(final int declaration) {
    schema(DECLARATIONS[declaration]);
    write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})-[:R]->(c:B {k: 2})");
    write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})-[:R]->(c:B {k: 2})");
    assertCounts(DECLARATIONS[declaration], 3, 2);
  }

  @Test
  void aPathWithNoMatchingFirstNodeIsCreated() {
    schema("LIGHTWEIGHT");
    write("MERGE (a:A {k: 1})-[:R]->(b:B {k: 1})");
    write("MERGE (a:A {k: 2})-[:R]->(b:B {k: 1})");
    assertCounts("same target, another source", 4, 2);
  }
}
