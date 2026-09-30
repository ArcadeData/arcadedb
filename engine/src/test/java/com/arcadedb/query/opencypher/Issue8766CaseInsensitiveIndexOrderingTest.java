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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.executor.operators.NodeIndexRangeScan;
import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8766 (#8698, #8699, #8700): a {@code COLLATE ci} index stores its string keys lower-cased, so its order and its
 * key ranges are those of the folded keys, not of the values. SQL {@code min()} / {@code max()}, SQL and Cypher
 * {@code ORDER BY ... LIMIT} and Cypher range predicates must answer exactly what the same statement answers on a type
 * with no index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8766CaseInsensitiveIndexOrderingTest extends TestHelper {
  private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Ci");
    database.command("sql", "CREATE PROPERTY Ci.s STRING");
    database.command("sql", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Nx");
    database.command("sql", "CREATE PROPERTY Nx.s STRING");
    database.command("sql", "CREATE VERTEX TYPE Cc");
    database.command("sql", "CREATE PROPERTY Cc.k INTEGER");
    database.command("sql", "CREATE PROPERTY Cc.s STRING");
    database.command("sql", "CREATE INDEX ON Cc (k, s COLLATE ci) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Cx");
    database.command("sql", "CREATE PROPERTY Cx.k INTEGER");
    database.command("sql", "CREATE PROPERTY Cx.s STRING");
    database.transaction(() -> {
      int k = 0;
      for (final String value : VALUES) {
        database.newVertex("Ci").set("s", value).save();
        database.newVertex("Nx").set("s", value).save();
        database.newVertex("Cc").set("k", k % 2).set("s", value).save();
        database.newVertex("Cx").set("k", k++ % 2).set("s", value).save();
      }
      for (final String value : new String[] { "\u00c9clair", "\u00e9t\u00e9", "zebra" }) {
        database.newVertex("Ci").set("s", value).save();
        database.newVertex("Nx").set("s", value).save();
      }
      database.newVertex("Ci").save();
      database.newVertex("Nx").save();
    });
  }

  private List<Object> column(final String language, final String query) {
    final List<Object> result = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("s"));
    }
    return result;
  }

  private void sameAnswer(final String language, final String template, final boolean ordered) {
    final List<Object> expected = column(language, template.replace("%T", "Nx"));
    final List<Object> actual = column(language, template.replace("%T", "Ci"));
    if (ordered)
      assertThat(actual).as(template).containsExactlyElementsOf(expected);
    else
      assertThat(actual).as(template).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void sqlMaxAndMinReturnARecordValue() {
    sameAnswer("sql", "SELECT max(s) AS s FROM %T", true);
    sameAnswer("sql", "SELECT min(s) AS s FROM %T", true);
  }

  @Test
  void sqlOrderByFollowsTheValues() {
    sameAnswer("sql", "SELECT s FROM %T ORDER BY s", true);
    sameAnswer("sql", "SELECT s FROM %T ORDER BY s LIMIT 3", true);
    sameAnswer("sql", "SELECT s FROM %T ORDER BY s DESC LIMIT 3", true);
  }

  @Test
  void cypherOrderByFollowsTheValues() {
    sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s LIMIT 3", true);
    sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s", true);
    sameAnswer("opencypher", "MATCH (c:%T) WHERE c.s >= 'B' RETURN c.s AS s ORDER BY s LIMIT 4", true);
  }

  @Test
  void cypherRangePredicatesFollowTheValues() {
    for (final String predicate : new String[] { "c.s < 'a'", "c.s >= 'M'", "c.s > 'M'", "c.s <= 'Zoo'", "c.s >= 'B' AND c.s < 'b'" })
      sameAnswer("opencypher", "MATCH (c:%T) WHERE " + predicate + " RETURN c.s AS s", false);
  }

  @Test
  void cypherOrderByDescendingFollowsTheValues() {
    sameAnswer("opencypher", "MATCH (c:%T) RETURN c.s AS s ORDER BY s DESC LIMIT 3", true);
  }

  @Test
  void compositeIndexWithAFoldedColumnFollowsTheValues() {
    sameAnswer("sql", "SELECT s FROM %T WHERE k = 1 ORDER BY s", true);
    sameAnswer("sql", "SELECT s FROM %T WHERE k = 0 ORDER BY s LIMIT 3", true);
  }

  @Test
  void aPlanBuiltBeforeTheIndexWasFoldedStaysCorrect() {
    database.command("sql", "CREATE VERTEX TYPE Late");
    database.command("sql", "CREATE PROPERTY Late.s STRING");
    database.command("sql", "CREATE INDEX ON Late (s) NOTUNIQUE");
    database.transaction(() -> {
      for (final String value : VALUES)
        database.newVertex("Late").set("s", value).save();
    });
    final String query = "MATCH (c:Late) WHERE c.s < 'a' RETURN c.s AS s";
    final List<Object> before = column("opencypher", query);
    database.command("sql", "DROP INDEX `Late[s]`");
    database.command("sql", "CREATE INDEX ON Late (s COLLATE ci) NOTUNIQUE");
    assertThat(column("opencypher", query)).containsExactlyInAnyOrderElementsOf(before);
  }

  @Test
  void parameterizedRangeFollowsTheValues() {
    final List<Object> expected = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (c:Nx) WHERE c.s < $p RETURN c.s AS s", Map.of("p", "a"))) {
      while (rs.hasNext())
        expected.add(rs.next().getProperty("s"));
    }
    final List<Object> actual = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (c:Ci) WHERE c.s < $p RETURN c.s AS s", Map.of("p", "a"))) {
      while (rs.hasNext())
        actual.add(rs.next().getProperty("s"));
    }
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void aRetainedRangeScanRefusesAFoldedIndex() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final NodeIndexRangeScan scan = new NodeIndexRangeScan("c", "Ci", "s",
        List.of(new RangePredicate("s", ComparisonExpression.Operator.LESS_THAN, "a", false)), "Ci[s]", 1.0, 1L);
    assertThatThrownBy(() -> scan.execute(context, -1).hasNext()).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("re-plan");
  }
}
