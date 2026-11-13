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
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11556: a RETURN that mixes the star with explicit items ({@code RETURN *, z.i AS i}) answered only the variables
 * in scope, and silently dropped every item written after the star. {@code WITH *, expr AS alias} already kept both.
 */
class Issue11556ReturnStarWithExplicitItemsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createEdgeType("R");
      database.command("opencypher", "CREATE (:A {i: 1})-[:R]->(:B {j: 2})");
      database.command("opencypher", "CREATE (:A {i: 2})");
      database.command("opencypher", "CREATE (:A {i: 2})");
    });
  }

  @Test
  void theItemAfterTheStarIsAColumnOfEveryRow() {
    final List<Result> rows = rows("MATCH (z:A) RETURN *, z.i AS i ORDER BY i");
    assertThat(rows).hasSize(3);
    for (final Result row : rows) {
      assertThat(row.getPropertyNames()).containsExactly("z", "i");
      assertThat(row.<Integer>getProperty("i")).isEqualTo(row.<Document>getProperty("z").getInteger("i"));
    }
    assertThat(rows.stream().map(r -> r.<Integer>getProperty("i")).toList()).containsExactly(1, 2, 2);
  }

  @Test
  void severalItemsAfterTheStarKeepTheirWrittenOrder() {
    final Result row = single("MATCH (z:A {i: 1})-[r:R]->(b:B) RETURN *, b.j AS j, z.i + b.j AS s");
    assertThat(row.getPropertyNames()).containsExactly("z", "r", "b", "j", "s");
    assertThat(row.<Integer>getProperty("j")).isEqualTo(2);
    assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(3L);
  }

  @Test
  void anUnaliasedItemAfterTheStarIsNamedByItsText() {
    final Result row = single("MATCH (z:A {i: 1}) RETURN *, z.i");
    assertThat(row.getPropertyNames()).containsExactly("z", "z.i");
    assertThat(row.<Integer>getProperty("z.i")).isEqualTo(1);
  }

  @Test
  void anAggregateAfterTheStarGroupsByTheVariablesInScope() {
    final List<Result> rows = rows("MATCH (z:A) WITH z.i AS k RETURN *, count(*) AS c ORDER BY k");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getPropertyNames()).containsExactly("k", "c");
    assertThat(rows.get(0).<Integer>getProperty("k")).isEqualTo(1);
    assertThat(rows.get(0).<Long>getProperty("c")).isEqualTo(1L);
    assertThat(rows.get(1).<Integer>getProperty("k")).isEqualTo(2);
    assertThat(rows.get(1).<Long>getProperty("c")).isEqualTo(2L);
  }

  @Test
  void distinctIsOverTheStarAndTheItems() {
    final List<Result> rows = rows("MATCH (z:A) WITH z.i AS k RETURN DISTINCT *, k * 10 AS t ORDER BY k");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getPropertyNames()).containsExactly("k", "t");
    assertThat(rows.get(0).<Number>getProperty("t").longValue()).isEqualTo(10L);
    assertThat(rows.get(1).<Number>getProperty("t").longValue()).isEqualTo(20L);
  }

  @Test
  void orderByAndLimitCanUseTheItemAfterTheStar() {
    final List<Result> rows = rows("UNWIND [3, 1, 2] AS x RETURN *, x * 10 AS y ORDER BY y DESC LIMIT 2");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getPropertyNames()).containsExactly("x", "y");
    assertThat(rows.get(0).<Number>getProperty("y").longValue()).isEqualTo(30L);
    assertThat(rows.get(1).<Number>getProperty("y").longValue()).isEqualTo(20L);
  }

  @Test
  void aLiteralAfterTheStarOfAWriteIsReturned() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "CREATE (c:B {j: 9}) RETURN *, c.j AS j, 'x' AS tag")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactly("c", "j", "tag");
        assertThat(row.<Integer>getProperty("j")).isEqualTo(9);
        assertThat(row.<String>getProperty("tag")).isEqualTo("x");
      }
    });
  }

  /**
   * A scope the parser cannot read without running (a {@code YIELD *} of a {@code DEFINE FUNCTION}, whose output no signature
   * declares) keeps the star, which the projection resolves against the row instead: the items after it are still columns.
   */
  @Test
  void aStarOverAScopeKnownOnlyAtRunTimeKeepsTheItems() {
    defineTwice();
    final List<Result> rows = commandRows("MATCH (z:A {i: 1}) CALL math.twice(z.i) YIELD * RETURN *, z.i AS i, z.i + 1 AS n");
    assertThat(rows).hasSize(1);
    final Result row = rows.getFirst();
    assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("z", "value", "i", "n");
    assertThat(row.<Number>getProperty("value").longValue()).isEqualTo(2L);
    assertThat(row.<Integer>getProperty("i")).isEqualTo(1);
    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(2L);
  }

  /** The same unknown scope under DISTINCT: the rows are told apart by the variables and the items. */
  @Test
  void aDistinctStarOverAScopeKnownOnlyAtRunTimeKeepsTheItems() {
    defineTwice();
    final List<Result> rows = commandRows("MATCH (z:A) CALL math.twice(z.i) YIELD * RETURN DISTINCT *, z.i AS i ORDER BY i");
    assertThat(rows).hasSize(3);
    assertThat(rows.getFirst().getPropertyNames()).containsExactlyInAnyOrder("z", "value", "i");
  }

  /** Grouping by keys only known at run time is refused, rather than answered without its grouping keys. */
  @Test
  void anAggregateOverAScopeKnownOnlyAtRunTimeIsRefused() {
    defineTwice();
    assertThatThrownBy(() -> rows("MATCH (z:A) CALL math.twice(z.i) YIELD * RETURN *, count(*) AS c"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("RETURN *");
  }

  /**
   * A CALL subquery that itself returns {@code *} has a scope known without running since #11571, so the star expands at
   * parse time and an aggregation groups by its variables.
   */
  @Test
  void anAggregateOverASubqueryReturningStarGroupsByItsScope() {
    final List<Result> rows = rows("MATCH (z:A) CALL { UNWIND [5, 5] AS k RETURN * } RETURN *, count(*) AS c");
    assertThat(rows).hasSize(3);
    for (final Result row : rows) {
      assertThat(row.getPropertyNames()).containsExactly("z", "k", "c");
      assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(2L);
    }
  }

  private void defineTwice() {
    database.command("sql", "DEFINE FUNCTION math.twice \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
  }

  /** A RETURN *, expr inside a CALL subquery answers its items to the enclosing query too. */
  @Test
  void theItemsAfterTheStarOfASubqueryReachTheEnclosingQuery() {
    final Result row = single("MATCH (z:A {i: 1}) CALL { UNWIND [5] AS k RETURN *, k * 2 AS d } RETURN z.i AS i, k, d");
    assertThat(row.<Number>getProperty("k").longValue()).isEqualTo(5L);
    assertThat(row.<Number>getProperty("d").longValue()).isEqualTo(10L);
  }

  /** Each branch of a UNION expands its own star. */
  @Test
  void eachBranchOfAUnionKeepsItsItems() {
    final List<Result> rows = rows("MATCH (z:A {i: 1}) RETURN *, z.i AS i UNION MATCH (z:B) RETURN *, z.j AS i");
    assertThat(rows).hasSize(2);
    for (final Result row : rows)
      assertThat(row.getPropertyNames()).containsExactly("z", "i");
    assertThat(rows.stream().map(r -> r.<Integer>getProperty("i")).toList()).containsExactlyInAnyOrder(1, 2);
  }

  /** An item named like a variable the star already returns defines that column twice, which Neo4j refuses too. */
  @Test
  void anItemNamedLikeAVariableOfTheStarIsAColumnConflict() {
    assertThatThrownBy(() -> rows("MATCH (z:A) RETURN *, z.i AS z")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("ColumnNameConflict").hasMessageContaining("'z'");
    assertThatThrownBy(() -> rows("MATCH (z:A) RETURN *, z")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("ColumnNameConflict");
  }

  /** What a zero-row result announces (#11543) is what a row of the same statement carries, item after the star included. */
  @ParameterizedTest
  @ValueSource(strings = {
      "MATCH (z:A {i: 1}) RETURN *, z.i AS i",
      "MATCH (z:A {i: 1})-[r:R]->(b:B) RETURN *, b.j AS j, z.i + b.j AS s",
      "MATCH (z:A {i: 1}) RETURN *, z.i",
      "UNWIND [1] AS x RETURN *, x * 10 AS y",
      "MATCH (z:A) WITH z.i AS k RETURN *, count(*) AS c",
      "MATCH (z:A) WITH z.i AS k RETURN DISTINCT *, k * 10 AS t"
  })
  void theDeclaredColumnsAreTheKeysOfARow(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).as("the control statement answers a row: %s", query).isNotEmpty();
    assertThat(OpenCypherQueryEngine.declaredColumns((DatabaseInternal) database, query)).as(query)
        .isEqualTo(new ArrayList<>(rows.getFirst().getPropertyNames()));
  }

  private Result single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).as(query).hasSize(1);
    return rows.getFirst();
  }

  /** A call that may reach a DEFINE FUNCTION body is not idempotent, so it runs as a command. */
  private List<Result> commandRows(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }

  private List<Result> rows(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }
}
