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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10191: null values in Cypher write patterns. Each expected outcome was checked on Neo4j 2026.09:
 * <ul>
 * <li>a relationship endpoint bound to null, or to something that is not a node, fails CREATE and MERGE alike ("Expected
 * the value NULL to be of type NODE") - it used to create an anonymous node in its place (CREATE) or silently skip the row
 * (MERGE);</li>
 * <li>a null property in a MERGE pattern fails, whether it is written as a literal, comes through a variable or a
 * parameter, or reads a property that is missing, on a node or on a relationship - only the literal used to be refused,
 * and the others created a node without the property, a new one per row;</li>
 * <li>a null property in a CREATE pattern is no error: the property is simply not set.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10191NullWritePatternsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createVertexType("M");
      database.getSchema().createEdgeType("R");
      database.command("opencypher", "CREATE (:A {k: 0})").close();
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "OPTIONAL MATCH (a:A {k: 99}) CREATE (a)-[:R]->(b:B {k: 5})",
      "OPTIONAL MATCH (a:A {k: 99}) CREATE (b:B {k: 5})-[:R]->(a)",
      "OPTIONAL MATCH (a:A {k: 99}) CREATE (a)-[:R]->(a)",
      "OPTIONAL MATCH (a:A {k: 0}) OPTIONAL MATCH (z:A {k: 99}) CREATE (a)-[:R]->(m:M)-[:R]->(z)",
      "OPTIONAL MATCH (a:A {k: 99}) MERGE (a)-[:R]->(b:B {k: 5})",
      "OPTIONAL MATCH (a:A {k: 99}) MERGE (b:B {k: 5})<-[:R]-(a)",
      "OPTIONAL MATCH (a:A {k: 99}) WITH a MERGE (a)-[:R]->(b:B {k: 5})" })
  void aNullEndpointFailsTheStatement(final String cypher) {
    assertThatThrownBy(() -> runInTransaction(cypher)).hasMessageContaining("Expected the value NULL to be of type NODE");
    assertUnchanged();
  }

  /** The validator refuses a variable it knows to be a scalar; one that arrives through a parameter is refused at run time. */
  @ParameterizedTest
  @ValueSource(strings = { "WITH 1 AS a CREATE (a)-[:R]->(b:B)", "WITH 'x' AS a MERGE (a)-[:R]->(b:B)",
      "WITH $p AS a CREATE (a)-[:R]->(b:B)", "WITH $p AS a MERGE (a)-[:R]->(b:B)" })
  void anEndpointThatIsNotANodeFailsTheStatement(final String cypher) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", cypher, Map.of("p", 1)).close()))
        .hasMessageContaining("NODE");
    assertUnchanged();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "MERGE (n:A {k: null})",
      "UNWIND [null, null] AS x MERGE (n:A {k: x})",
      "MATCH (a:A) MERGE (n:B {k: a.missing})",
      "MATCH (a:A) MERGE (n:B {k: 1, j: a.missing})",
      "MATCH (a:A) MERGE (a)-[:R]->(b:B {w: null})",
      "MATCH (a:A) MERGE (a)-[:R {w: null}]->(b:B)",
      "MATCH (a:A) WITH a, null AS x MERGE (a)-[:R {w: x}]->(b:B)" })
  void aNullMergePropertyFailsTheStatement(final String cypher) {
    assertThatThrownBy(() -> runInTransaction(cypher)).hasMessageContaining("cannot be used with a graph element property value that is null");
    assertUnchanged();
  }

  @Test
  void aNullMergePropertyFromAParameterFailsTheStatement() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("p", null);
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MERGE (n:A {k: $p})", parameters).close()))
        .hasMessageContaining("cannot be used with a graph element property value that is null");
    assertUnchanged();
  }

  /** An endpoint bound to a bare RID is the vertex it names: the pattern connects it rather than a new node. */
  @Test
  void anEndpointBoundToARidIsTheVertexItNames() {
    final RID a;
    try (final ResultSet rs = database.query("sql", "SELECT FROM A WHERE k = 0")) {
      a = rs.next().getIdentity().get();
    }
    final Map<String, Object> parameters = Map.of("a", a);

    database.transaction(() -> database.command("opencypher", "WITH $a AS a MERGE (a)-[:R]->(b:B {k: 9})", parameters).close());
    database.transaction(() -> database.command("opencypher", "WITH $a AS a MERGE (a)-[:R]->(b:B {k: 9})", parameters).close());
    database.transaction(() -> database.command("opencypher", "WITH $a AS a CREATE (a)-[:R]->(:M {k: 1})", parameters).close());

    assertThat(graph()).containsExactly("[A]{k=0}", "[B]{k=9}", "[M]{k=1}");
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:A {k: 0})-[r:R]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
    }
  }

  @Test
  void aNullCreatePropertyIsJustNotSet() {
    final List<Result> rows = runInTransaction("MATCH (a:A) CREATE (n:B {k: a.missing, j: 1}) RETURN n");
    assertThat(rows).hasSize(1);
    assertThat(graph()).containsExactly("[A]{k=0}", "[B]{j=1}");
  }

  @Test
  void aNullSetAfterTheMergeIsNoError() {
    runInTransaction("MATCH (a:A) MERGE (a)-[:R]->(b:B) ON CREATE SET b.k = null RETURN b");
    assertThat(graph()).containsExactly("[A]{k=0}", "[B]{}");
  }

  @Test
  void aMergePropertyThatIsNotNullStillMerges() {
    runInTransaction("UNWIND [1, 1, 2] AS x MERGE (n:B {k: x})");
    assertThat(graph()).containsExactly("[A]{k=0}", "[B]{k=1}", "[B]{k=2}");
  }

  private void assertUnchanged() {
    assertThat(graph()).containsExactly("[A]{k=0}");
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  private List<Result> runInTransaction(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  private List<String> graph() {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n) RETURN labels(n) AS l, properties(n) AS p ORDER BY labels(n)[0], n.k")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("l") + "" + new TreeMap<>(r.<Map<String, Object>>getProperty("p")));
      }
    }
    return out;
  }
}
