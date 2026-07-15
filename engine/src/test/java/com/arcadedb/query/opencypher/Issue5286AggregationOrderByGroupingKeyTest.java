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
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5286: ordering by a grouping-key expression that is projected under an
 * alias must sort, instead of being silently ignored.
 * <p>
 * <pre>
 *   MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age
 * </pre>
 * returned rows in storage order and raised no error. The semantic validator admits {@code n} into
 * the ORDER BY scope for an aggregating projection, but the aggregation step does not carry {@code n}
 * onto the output rows, so the sort key evaluated to null on every row, all rows compared equal and
 * the sort became a no-op. The aliased + aggregating combination was the only affected one: the
 * un-aliased form matched the output column by name and sorted correctly.
 * <p>
 * The grouping key has exactly one value per group, so this is well defined and Neo4j sorts it. The
 * ORDER BY expression is now resolved to the projected output column, as for DISTINCT (#5283).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5286AggregationOrderByGroupingKeyTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5286");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    // inserted out of age order, so an unsorted result is distinguishable from a sorted one
    database.transaction(() -> {
      database.command("cypher", "CREATE (:P {active: true, age: 30, name: 'c'})");
      database.command("cypher", "CREATE (:P {active: false, age: 20, name: 'b'})");
      database.command("cypher", "CREATE (:P {active: true, age: 10, name: 'a'})");
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private List<Object> column(final String cypher, final String columnName) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty(columnName));
      }
    }
    return values;
  }

  /**
   * The reported query: the grouping key is aliased, and ORDER BY names the original expression.
   */
  @Test
  public void orderByAliasedGroupingKey() {
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age", "a")) //
        .containsExactly(10, 20, 30);
  }

  @Test
  public void orderByAliasedGroupingKeyDescending() {
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age DESC", "a")) //
        .containsExactly(30, 20, 10);
  }

  /**
   * Ordering by the alias always worked and must keep working.
   */
  @Test
  public void orderByAliasStillWorks() {
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY a", "a")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * The un-aliased form resolves by column name and must keep working.
   */
  @Test
  public void orderByUnaliasedGroupingKeyStillWorks() {
    assertThat(column("MATCH (n:P) RETURN n.age, count(*) AS c ORDER BY n.age", "n.age")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * A composite grouping key must match regardless of whitespace.
   */
  @Test
  public void orderByAliasedCompositeGroupingKey() {
    assertThat(column("MATCH (n:P) RETURN n.age + 1 AS x, count(*) AS c ORDER BY n.age+1", "x")) //
        .containsExactly(11L, 21L, 31L);
  }

  /**
   * Ordering by a projected aggregate expression is well defined per group and must sort too.
   */
  @Test
  public void orderByProjectedAggregate() {
    // count() is 64-bit, hence Long
    assertThat(column("MATCH (n:P) RETURN n.active AS a, count(*) AS c ORDER BY count(*)", "c")) //
        .containsExactly(1L, 2L);
    assertThat(column("MATCH (n:P) RETURN n.active AS a, count(*) AS c ORDER BY count(*) DESC", "c")) //
        .containsExactly(2L, 1L);
  }

  /**
   * The same rule applies to an aggregating WITH.
   */
  @Test
  public void withAggregationOrderByAliasedGroupingKey() {
    assertThat(column("MATCH (n:P) WITH n.age AS a, count(*) AS c ORDER BY n.age RETURN a, c", "a")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * Grouping by one key and ordering by another projected key must sort on the requested one.
   */
  @Test
  public void orderByAliasedGroupingKeyAmongSeveral() {
    assertThat(column("MATCH (n:P) RETURN n.name AS nm, n.age AS a, count(*) AS c ORDER BY n.age", "nm")) //
        .containsExactly("a", "b", "c");
  }

  /**
   * An ORDER BY built out of a grouping key is well defined too: the grouping key is constant per
   * group, so the enclosing expression is. Neo4j sorts this; it is resolved by substituting the
   * projected part, i.e. sorting on {@code a + 1}.
   */
  @Test
  public void orderByExpressionContainingGroupingKey() {
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + 1", "a")) //
        .containsExactly(10, 20, 30);
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + 1 DESC", "a")) //
        .containsExactly(30, 20, 10);
  }

  /**
   * The substitution reaches inside function-call arguments.
   */
  @Test
  public void orderByFunctionOfGroupingKey() {
    assertThat(column("MATCH (n:P) RETURN n.name AS nm, count(*) AS c ORDER BY toUpper(n.name) DESC", "nm")) //
        .containsExactly("c", "b", "a");
  }

  /**
   * When the node itself is the grouping key, a property of it is well defined per group and must
   * sort: the read follows the variable to the column it was projected as.
   */
  @Test
  public void orderByPropertyOfProjectedNode() {
    assertThat(column("MATCH (n:P) RETURN n AS node, count(*) AS c ORDER BY n.age", "c")) //
        .containsExactly(1L, 1L, 1L);
    final List<Object> ages = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", "MATCH (n:P) RETURN n AS node, count(*) AS c ORDER BY n.age")) {
      while (rs.hasNext())
        ages.add(((Document) rs.next().getProperty("node")).get("age"));
    }
    assertThat(ages).containsExactly(10, 20, 30);
  }

  /**
   * A reference the projection dropped has no value to sort on. Reporting it is what Neo4j does, and
   * it is the point of the fix: the query used to return unsorted rows and say nothing.
   */
  @Test
  public void orderByNonGroupingKeyRejected() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.name").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
  }

  /**
   * An ORDER BY item that mixes an aggregation with a complex non-aggregated part is ambiguous by
   * specification, even when that part is projected (openCypher ReturnOrderBy6 [5]). Resolving the
   * part to its output column would make the query compile, so the substitution must leave items
   * containing an aggregation alone. Covered by the TCK; pinned here because it is the one shape the
   * rewrite must deliberately not touch.
   */
  @Test
  public void orderByComplexExpressionWithAggregationStaysAmbiguous() {
    assertThatThrownBy(() -> database
        .query("cypher", "MATCH (n:P) RETURN n.age + 1, count(*) AS c ORDER BY n.age + 1 + count(*)").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("AmbiguousAggregationExpression");
  }

  /**
   * By contrast a simple projected property read inside an aggregation-containing ORDER BY item is
   * legal and must keep compiling (openCypher ReturnOrderBy6 [3]).
   */
  @Test
  public void orderBySimplePropertyWithAggregationStillCompiles() {
    assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + count(*)", "a")) //
        .containsExactlyInAnyOrder(10, 20, 30);
  }
}
