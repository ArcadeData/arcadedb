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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6397: a label disjunction ({@code (n:A|B {id: $x})}) always executed as
 * {@code NodeByLabelDisjunctionScan}, a full scan of every alternative's type, even when every alternative has an
 * index that could resolve the equality predicate with a seek.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDisjunctionIndexSeekIssue6397Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var typeA = database.getSchema().createVertexType("Alpha6397");
      typeA.createProperty("id", String.class);
      typeA.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      final var typeB = database.getSchema().createVertexType("Bravo6397");
      typeB.createProperty("id", String.class);
      typeB.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      // No index at all: used to prove the all-or-nothing fallback still returns correct rows.
      final var typeC = database.getSchema().createVertexType("Charlie6397");
      typeC.createProperty("id", String.class);
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("opencypher", "CREATE (:Alpha6397 {id: $id})", Map.of("id", "a" + i));
      for (int i = 0; i < 50; i++)
        database.command("opencypher", "CREATE (:Bravo6397 {id: $id})", Map.of("id", "b" + i));
      for (int i = 0; i < 5; i++)
        database.command("opencypher", "CREATE (:Charlie6397 {id: $id})", Map.of("id", "c" + i));
    });
  }

  @Test
  void inlinePropertyEqualityOnEveryIndexedAlternativeUsesASeek() {
    final String plan = profilePlan("MATCH (n:Alpha6397|Bravo6397 {id: 'a1'}) RETURN n.id AS k");
    assertThat(plan).as("plan\n%s", plan)
        .contains("NodeByLabelDisjunctionIndexSeek")
        .doesNotContain("NodeByLabelDisjunctionScan");

    assertThat(ids("MATCH (n:Alpha6397|Bravo6397 {id: 'a1'}) RETURN n.id AS k")).containsExactly("a1");
  }

  @Test
  void whereClauseEqualityOnEveryIndexedAlternativeUsesASeek() {
    final String query = "MATCH (n:Alpha6397|Bravo6397) WHERE n.id = 'b7' RETURN n.id AS k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("NodeByLabelDisjunctionIndexSeek")
        .doesNotContain("NodeByLabelDisjunctionScan");

    assertThat(ids(query)).containsExactly("b7");
  }

  @Test
  void oneNonIndexedAlternativeFallsBackToTheFullScanButStaysCorrect() {
    final String plan = profilePlan("MATCH (n:Alpha6397|Charlie6397 {id: 'a2'}) RETURN n.id AS k");
    assertThat(plan).as("all-or-nothing: Charlie6397 has no index, so the whole disjunction stays a scan\n%s", plan)
        .contains("NodeByLabelDisjunctionScan")
        .doesNotContain("NodeByLabelDisjunctionIndexSeek");

    assertThat(ids("MATCH (n:Alpha6397|Charlie6397 {id: 'a2'}) RETURN n.id AS k")).containsExactly("a2");
    assertThat(ids("MATCH (n:Alpha6397|Charlie6397 {id: 'c3'}) RETURN n.id AS k")).containsExactly("c3");
  }

  @Test
  void aValueNoAlternativeHasReturnsNothing() {
    assertThat(ids("MATCH (n:Alpha6397|Bravo6397 {id: 'nope'}) RETURN n.id AS k")).isEmpty();
  }

  @Test
  void aTypeMultiplyInheritingFromTwoIndexedAlternativesIsReturnedOnceNotTwice() {
    database.command("sql", "CREATE VERTEX TYPE Delta6397 EXTENDS Alpha6397, Bravo6397");
    database.transaction(() -> database.command("sql", "INSERT INTO Delta6397 SET id = 'diamond'"));

    final String query = "MATCH (n:Alpha6397|Bravo6397 {id: 'diamond'}) RETURN n.id AS k";
    assertThat(profilePlan(query)).contains("NodeByLabelDisjunctionIndexSeek");
    assertThat(ids(query)).containsExactly("diamond");
  }

  private List<String> ids(final String cypher) {
    final List<String> result = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext())
        result.add(rs.next().getProperty("k"));
      rs.close();
    });
    return result;
  }

  private String profilePlan(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
