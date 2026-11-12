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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11571, follow-up of #11543: a {@code RETURN *} whose scope runs through a {@code CALL { ... }} subquery that itself
 * returns {@code *} declared no columns, so a zero-row result of it announced {@code fields=[]} over Bolt. The scope walk now
 * follows the subquery's own {@code RETURN *} scope.
 * <p>
 * Every shape is checked against the keys of a row the same statement answers when it does match, so the columns a zero-row
 * result announces are the ones a non-empty result of it carries, in the same order.
 */
class Issue11571DeclaredColumnsOfSubqueryStarTest extends TestHelper {

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
      "CALL { MATCH (n:A) RETURN * } RETURN *",
      "CALL { MATCH (n:A)-[r:R]->(m:B) RETURN * } RETURN *",
      "MATCH (z:A) CALL { MATCH (b:B) RETURN * } RETURN *",
      "MATCH (z:A) CALL { MATCH (z:A) RETURN * } RETURN *",
      "MATCH (z:A) CALL (z) { MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
      "MATCH (z:A) CALL { WITH z MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
      "MATCH (z:A) CALL (*) { MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
      "MATCH (z:A) CALL { WITH * MATCH (z)-[r:R]->(b) RETURN * } RETURN *",
      "MATCH (z:A) CALL (z) { WITH z MATCH (b:B) WITH b RETURN * } RETURN *",
      "CALL { MATCH (n:A) RETURN *, 1 AS one } RETURN *",
      "CALL { CALL { MATCH (n:A) RETURN * } RETURN * } RETURN *",
      "CALL { MATCH (n:A) RETURN * UNION MATCH (n:B) RETURN * } RETURN *",
      "CALL { CALL db.labels() YIELD * RETURN * } RETURN *",
      "CALL { MATCH (n:A) RETURN * } WITH * MATCH (b:B) RETURN *",
      "CALL { MATCH (n:A) RETURN * } RETURN *, 2 AS two"
  })
  void theDeclaredColumnsAreTheKeysOfARowOfTheStatement(final String query) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(rs.hasNext()).as("the control statement answers a row: %s", query).isTrue();
        assertThat(declaredColumns(query)).as(query).isEqualTo(new ArrayList<>(rs.next().getPropertyNames()));
      }
    });
  }

  /** The reported shape: a zero-row {@code RETURN *} through a subquery returning {@code *}. */
  @Test
  void aZeroRowReturnStarThroughASubqueryStarDeclaresTheSubqueryVariables() {
    assertThat(declaredColumns("CALL { MATCH (n:Missing) RETURN * } RETURN *")).containsExactly("n");
    assertThat(declaredColumns("MATCH (z:A) WHERE z.i > 100 CALL { MATCH (b:B) RETURN * } RETURN *")).containsExactly("z", "b");
    assertThat(declaredColumns("MATCH (z:A) CALL (z) { MATCH (z)-[r:R]->(b:Missing) RETURN * } RETURN *"))
        .containsExactly("z", "r", "b");
    assertThat(declaredColumns("PROFILE CALL { MATCH (n:Missing) RETURN * } RETURN *")).containsExactly("n");
  }

  /**
   * An OPTIONAL CALL declares the columns its body returns, as Neo4j binds them to null when the body matches nothing. The null
   * row the executor produces leaves them out today (#11632), so this is pinned on the declared columns alone.
   */
  @Test
  void anOptionalSubqueryStarDeclaresTheBodyColumns() {
    assertThat(declaredColumns("MATCH (z:A) OPTIONAL CALL (z) { MATCH (z)-[r:X]->(b) RETURN * } RETURN *"))
        .containsExactly("z", "r", "b");
  }

  /**
   * A subquery returning {@code *} through a CALL of a procedure with no static signature still declares nothing: its columns
   * are not known before it runs.
   */
  @Test
  void aSubqueryStarOverAnUnknownProcedureStillDeclaresNothing() {
    assertThat(declaredColumns("CALL { MATCH (n:A) CALL my.unknown() YIELD * RETURN * } RETURN *")).isNull();
    assertThat(declaredColumns("CALL my.unknown() YIELD *")).isNull();
  }

  private List<String> declaredColumns(final String query) {
    return OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query);
  }
}
