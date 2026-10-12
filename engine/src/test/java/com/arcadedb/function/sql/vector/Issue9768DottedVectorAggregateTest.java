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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9768: {@code vector.sum}, {@code vector.avg}, {@code vector.min} and {@code vector.max} never aggregated when written
 * with the dotted name: the AST builder rewrote {@code <id>.<method>(...)} into a {@code FunctionCall} only for the namespaces
 * in a hand-maintained set, {@code vector} not among them, while the executor recognised the same shape at EXECUTION time by
 * asking the function registry. The planner decides what aggregates by walking the AST, so it saw an identifier carrying a
 * method call, built no aggregation step and the query answered one NULL per row.
 * <p>
 * The rule pinned here: a namespaced call is the SAME AST node whichever namespace it is in, because it is a registered function
 * and not because somebody listed its prefix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9768DottedVectorAggregateTest extends TestHelper {

  private void load() {
    final var type = database.getSchema().createDocumentType("D");
    type.createProperty("g", Type.INTEGER);
    type.createProperty("v", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> {
      database.newDocument("D").set("g", 0).set("v", new float[] { 1f, 2f }).save();
      database.newDocument("D").set("g", 1).set("v", new float[] { 2f, 1f }).save();
      database.newDocument("D").set("g", 0).set("v", new float[] { 1f, 2f }).save();
      database.newDocument("D").set("g", 1).set("v", new float[] { 2f, 3f }).save();
    });
  }

  private List<Result> run(final String sql) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private static float[] vector(final Result row, final String property) {
    final Object value = row.getProperty(property);
    assertThat(value).as(property).isNotNull();
    if (value instanceof float[] floats)
      return floats;
    final List<?> list = (List<?>) value;
    final float[] result = new float[list.size()];
    for (int i = 0; i < result.length; i++)
      result[i] = ((Number) list.get(i)).floatValue();
    return result;
  }

  @Test
  void dottedAggregatesAnswerOneRow() {
    load();
    assertThat(run("SELECT vector.sum(v) AS s FROM D")).hasSize(1);
    assertThat(run("SELECT vector.avg(v) AS s FROM D")).hasSize(1);
    assertThat(run("SELECT vector.min(v) AS s FROM D")).hasSize(1);
    assertThat(run("SELECT vector.max(v) AS s FROM D")).hasSize(1);
  }

  @Test
  void dottedAggregatesAnswerTheSameValuesAsTheirAliasAndTheBackTickedName() {
    load();
    assertThat(vector(run("SELECT vector.sum(v) AS s FROM D").getFirst(), "s")).containsExactly(6f, 8f);
    assertThat(vector(run("SELECT vector.avg(v) AS s FROM D").getFirst(), "s")).containsExactly(1.5f, 2f);
    assertThat(vector(run("SELECT vector.min(v) AS s FROM D").getFirst(), "s")).containsExactly(1f, 1f);
    assertThat(vector(run("SELECT vector.max(v) AS s FROM D").getFirst(), "s")).containsExactly(2f, 3f);

    for (final String name : new String[] { "sum", "avg", "min", "max" }) {
      final float[] dotted = vector(run("SELECT vector." + name + "(v) AS s FROM D").getFirst(), "s");
      final float[] alias = vector(run("SELECT vector" + Character.toUpperCase(name.charAt(0)) + name.substring(1) + "(v) AS s FROM D").getFirst(),
          "s");
      final float[] quoted = vector(run("SELECT `vector." + name + "`(v) AS s FROM D").getFirst(), "s");
      assertThat(dotted).as(name).containsExactly(alias);
      assertThat(quoted).as(name).containsExactly(alias);
    }
  }

  @Test
  void dottedAggregateNextToAnotherAggregateAndWithGroupBy() {
    load();

    final List<Result> together = run("SELECT vector.sum(v) AS s, count(*) AS c FROM D");
    assertThat(together).hasSize(1);
    assertThat(together.getFirst().<Long>getProperty("c")).isEqualTo(4L);
    assertThat(vector(together.getFirst(), "s")).containsExactly(6f, 8f);

    final List<Result> grouped = run("SELECT g, vector.sum(v) AS s FROM D GROUP BY g ORDER BY g");
    assertThat(grouped).hasSize(2);
    assertThat(vector(grouped.get(0), "s")).containsExactly(2f, 4f);
    assertThat(vector(grouped.get(1), "s")).containsExactly(4f, 4f);
  }

  @Test
  void explainShowsTheAggregationStep() {
    load();
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT vector.sum(v) AS s FROM D")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("CALCULATE AGGREGATE PROJECTIONS").contains("GUARANTEE SINGLE ROW FOR EMPTY AGGREGATION");
    }
  }

  @Test
  void aPropertyNamedLikeANamespaceKeepsItsOwnMethods() {
    // `vector` is the most natural name of an embedding column: a method call on it must stay a method call as long as
    // `vector.<method>` is not a registered function
    final var type = database.getSchema().createDocumentType("E");
    type.createProperty("vector", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> database.newDocument("E").set("vector", new float[] { 1f, 2f, 3f }).save());

    final List<Result> rows = run("SELECT vector.size() AS n FROM E");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Integer>getProperty("n")).isEqualTo(3);
  }

  /**
   * The test that would have caught this and will catch the next namespace somebody adds: for every registered function that
   * aggregates and has a dotted name, the dotted call must be an aggregate in the parsed statement, which is what the planner
   * decides on. The arguments are irrelevant to that decision, so none are executed.
   */
  @Test
  void everyRegisteredDottedAggregateIsAnAggregateWhenWrittenWithoutBackTicks() {
    load();
    final DatabaseInternal db = (DatabaseInternal) database;
    final CommandContext context = new BasicCommandContext();
    ((BasicCommandContext) context).setDatabase(db);

    final Set<String> names = new TreeSet<>(DefaultSQLFunctionFactory.getInstance().getFunctionNames());
    names.addAll(FunctionRegistry.getFunctionNames());

    final List<String> checked = new ArrayList<>();
    final List<String> failures = new ArrayList<>();
    for (final String name : names) {
      final int dot = name.indexOf('.');
      if (dot < 1 || dot != name.lastIndexOf('.') || dot == name.length() - 1)
        continue;
      final SQLFunction function = DefaultSQLFunctionFactory.getInstance().getFunctionInstance(name);
      if (function == null || !function.aggregateResults())
        continue;

      checked.add(name);
      try {
        final SelectStatement statement = (SelectStatement) db.getStatementCache().get("SELECT " + name + "(v) FROM D");
        if (!statement.projection.items.getFirst().isAggregate(context))
          failures.add(name);
      } catch (final RuntimeException e) {
        failures.add(name + " (" + e.getMessage() + ")");
      }
    }

    assertThat(checked).as("the dotted aggregates of the registry").contains("vector.sum", "vector.avg", "vector.min", "vector.max");
    assertThat(failures).as("dotted aggregates the planner would not see as aggregates").isEmpty();
  }

  @Test
  void aNameThatIsNotRegisteredStaysAMethodCall() {
    load();
    final DatabaseInternal db = (DatabaseInternal) database;
    final SelectStatement statement = (SelectStatement) db.getStatementCache().get("SELECT vector.thisDoesNotExist(v) FROM D");
    final CommandContext context = new BasicCommandContext();
    ((BasicCommandContext) context).setDatabase(db);
    assertThat(statement.projection.items.getFirst().isAggregate(context)).isFalse();
  }
}
