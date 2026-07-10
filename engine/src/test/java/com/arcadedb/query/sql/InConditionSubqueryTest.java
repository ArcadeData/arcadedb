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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.query.sql.parser.WhereClause;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for IN (SELECT …) subquery evaluation.
 * <p>
 * Two code paths are covered:
 * <ul>
 *   <li>Standard SELECT path — subquery is extracted into a LET variable by
 *       SelectExecutionPlanner and materialised as List&lt;Result&gt;. The loop in
 *       InCondition.evaluateExpression handles the comparison via QueryOperatorEquals.</li>
 *   <li>Direct-evaluation path — no planner extractSubQueries is called, so evaluateRight
 *       calls executeQuery which returns Set&lt;Result&gt;. Before the fix, the Set fast-path
 *       in evaluateExpression called set.contains(scalar), which was always false because
 *       scalar.equals(Result{name:scalar}) is never true.</li>
 * </ul>
 */
class InConditionSubqueryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person4337");
      database.getSchema().createDocumentType("AllowedName4337");

      database.command("SQL", "INSERT INTO Person4337 SET name = 'Alice', age = 30");
      database.command("SQL", "INSERT INTO Person4337 SET name = 'Bob', age = 25");
      database.command("SQL", "INSERT INTO Person4337 SET name = 'Charlie', age = 40");

      database.command("SQL", "INSERT INTO AllowedName4337 SET name = 'Alice'");
      database.command("SQL", "INSERT INTO AllowedName4337 SET name = 'Charlie'");
    });
  }

  @Test
  void inWithScalarSubqueryReturnsMatchingRows() {
    final ResultSet rs = database.query("SQL",
        "SELECT name FROM Person4337 WHERE name IN (SELECT name FROM AllowedName4337) ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      names.add(r.getProperty("name"));
    }

    assertThat(names).containsExactly("Alice", "Charlie");
  }

  @Test
  void notInWithScalarSubqueryExcludesMatchingRows() {
    final ResultSet rs = database.query("SQL",
        "SELECT name FROM Person4337 WHERE name NOT IN (SELECT name FROM AllowedName4337) ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      names.add(r.getProperty("name"));
    }

    assertThat(names).containsExactly("Bob");
  }

  @Test
  void inWithIntegerSubqueryReturnsMatchingRows() {
    final ResultSet rs = database.query("SQL",
        "SELECT name FROM Person4337 WHERE age IN (SELECT age FROM Person4337 WHERE name = 'Alice' OR name = 'Bob') ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      names.add(r.getProperty("name"));
    }

    assertThat(names).containsExactly("Alice", "Bob");
  }

  @Test
  void inWithSubqueryCountMatchesExpected() {
    final ResultSet rs = database.query("SQL",
        "SELECT count(*) as cnt FROM Person4337 WHERE name IN (SELECT name FROM AllowedName4337)");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat(r.<Long>getProperty("cnt")).isEqualTo(2L);
  }

  /**
   * Direct-evaluation path: the InCondition is evaluated without going through
   * SelectExecutionPlanner.extractSubQueries, so evaluateRight calls executeQuery which
   * returns Set&lt;Result&gt;. Before the fix, set.contains(scalar) always returned false.
   * After the fix, executeQuery unwraps single-property Results to scalars, and
   * set.contains("Alice") returns true.
   */
  @Test
  void directEvalWithoutPlannerReturnsCorrectMatch() {
    database.transaction(() -> {
      final WhereClause where = new SQLAntlrParser(database)
          .parseCondition("name IN (SELECT name FROM AllowedName4337)");

      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);

      // Extract the InCondition from the parsed WHERE clause tree.
      final InCondition inCond = extractInCondition(where);
      assertThat(inCond).as("InCondition should be present in parsed WHERE clause").isNotNull();
      assertThat(inCond.rightStatement).as("rightStatement should be non-null before extractSubQueries").isNotNull();

      // Evaluate against a record that IS in the allowed set — must return true.
      final ResultInternal alice = new ResultInternal(Map.of("name", "Alice"));
      assertThat(inCond.evaluate(alice, ctx)).isTrue();

      // Evaluate against a record that is NOT in the allowed set — must return false.
      final ResultInternal bob = new ResultInternal(Map.of("name", "Bob"));
      assertThat(inCond.evaluate(bob, ctx)).isFalse();
    });
  }

  private InCondition extractInCondition(final WhereClause where) {
    final BooleanExpression base = where.getBaseExpression();
    if (base instanceof InCondition ic)
      return ic;
    if (base instanceof AndBlock ab) {
      for (final BooleanExpression sub : ab.getSubBlocks())
        if (sub instanceof InCondition ic)
          return ic;
    }
    if (base instanceof OrBlock ob) {
      for (final BooleanExpression ab : ob.getSubBlocks()) {
        if (ab instanceof InCondition ic)
          return ic;
        if (ab instanceof AndBlock andB) {
          for (final BooleanExpression sub : andB.getSubBlocks())
            if (sub instanceof InCondition ic)
              return ic;
        }
      }
    }
    return null;
  }
}
