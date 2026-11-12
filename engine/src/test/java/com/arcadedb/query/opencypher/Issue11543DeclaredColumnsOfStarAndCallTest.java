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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11543: a wire protocol announces the columns of a zero-row result from the statement, since there is no row to read
 * them from (issue #11386). Two shapes declare their columns somewhere other than in RETURN items: {@code RETURN *}, whose
 * columns are the variables in scope at the RETURN, and a standalone {@code CALL ... YIELD} (or a {@code CALL} with no YIELD,
 * or {@code YIELD *}), whose columns are the yielded fields. Both used to declare nothing, so a zero-row result of them
 * announced no column at all.
 * <p>
 * Every shape is checked against the keys of a row the same statement answers when it does match, so the columns a zero-row
 * result announces are the ones a non-empty result of it carries, in the same order.
 */
class Issue11543DeclaredColumnsOfStarAndCallTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createEdgeType("R");
      database.command("opencypher", "CREATE (:A {i: 1})-[:R]->(:B {j: 2})");
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "MATCH (n:A) RETURN *",
      "MATCH (z:A)-[r:R]->(b:B) RETURN *",
      "MATCH (z)-[r]->(b:B {j: 2}) RETURN *",
      "MATCH (z:A), (b:B) RETURN *",
      "MATCH (z:A) WITH z, 1 AS a RETURN *",
      "MATCH (z:A) WITH z.i AS i RETURN *",
      "MATCH p = (z:A)-->() RETURN *",
      "MATCH (z:A)-->() RETURN *",
      "UNWIND [1] AS y MATCH (z:A) RETURN *",
      "UNWIND [1, 2] AS x RETURN *",
      "MATCH (z:A) WITH * MATCH (b:B) RETURN *",
      "MATCH (z:A) CALL { WITH z RETURN 5 AS k } RETURN *",
      "MATCH (z:A) OPTIONAL MATCH (z)-[q:X]->(m) RETURN *",
      "MATCH (z:A) RETURN * ORDER BY z.i LIMIT 1",
      "CREATE (c:A {i: 3}) RETURN *",
      "MERGE (c:A {i: 1}) RETURN *",
      "MATCH (n:A) RETURN * UNION MATCH (n:B) RETURN *",
      "CALL db.labels() YIELD label AS l",
      "CALL db.labels() YIELD label",
      "CALL db.labels() YIELD label WHERE label = 'A'",
      "CALL db.labels() YIELD *",
      "CALL db.labels()",
      "CALL db.labels() YIELD label MATCH (n:A) RETURN *"
  })
  void theDeclaredColumnsAreTheKeysOfARowOfTheStatement(final String query) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(rs.hasNext()).as("the control statement answers a row: %s", query).isTrue();
        assertThat(declaredColumns(query)).as(query).isEqualTo(new ArrayList<>(rs.next().getPropertyNames()));
      }
    });
  }

  @Test
  void aZeroRowReturnStarDeclaresTheVariablesInScope() {
    assertThat(declaredColumns("MATCH (n:Missing) RETURN *")).containsExactly("n");
    assertThat(declaredColumns("MATCH (a:A)-[r:R]->(b:B) WHERE a.i > 100 RETURN *")).containsExactly("a", "r", "b");
    assertThat(declaredColumns("MATCH (a:A) WHERE a.i > 100 WITH a, a.i AS i RETURN *")).containsExactly("a", "i");
    assertThat(declaredColumns("MATCH (a:A) WHERE a.i > 100 WITH a.i AS i RETURN *")).containsExactly("i");
    assertThat(declaredColumns("EXPLAIN MATCH (n:Missing) RETURN *")).containsExactly("n");
    assertThat(declaredColumns("PROFILE MATCH (n:Missing) RETURN *")).containsExactly("n");
  }

  @Test
  void aZeroRowCallDeclaresItsYieldedFields() {
    assertThat(declaredColumns("CALL db.labels() YIELD label WHERE label = 'Missing'")).containsExactly("label");
    assertThat(declaredColumns("CALL db.labels() YIELD label AS l WHERE l = 'Missing'")).containsExactly("l");
    assertThat(declaredColumns("CALL algo.wcc() YIELD componentId")).containsExactly("componentId");
    assertThat(declaredColumns("CALL algo.wcc()")).containsExactly("node", "componentId");
    assertThat(declaredColumns("CALL algo.wcc() YIELD *")).containsExactly("node", "componentId");
  }

  /** A CALL whose procedure is not one the engine knows still declares what it explicitly yields, and nothing otherwise. */
  @Test
  void anUnknownProcedureDeclaresOnlyItsExplicitYield() {
    assertThat(declaredColumns("CALL my.unknown() YIELD a, b AS c")).containsExactly("a", "c");
    assertThat(declaredColumns("CALL my.unknown()")).isNull();
  }

  /** A statement with neither a RETURN nor a trailing CALL still declares nothing: it answers no rows. */
  @Test
  void aWriteWithoutReturnDeclaresNoColumns() {
    assertThat(declaredColumns("CREATE (:A {i: 99})")).isNull();
    assertThat(declaredColumns("MATCH (n:A) SET n.x = 1")).isNull();
  }

  private List<String> declaredColumns(final String query) {
    return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
  }
}
