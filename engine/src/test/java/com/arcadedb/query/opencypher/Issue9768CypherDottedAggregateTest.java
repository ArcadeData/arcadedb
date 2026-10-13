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
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9768, the openCypher half: {@code vector.sum}, {@code vector.avg}, {@code vector.min} and {@code vector.max} were
 * called once per row and answered a NULL for each, because {@code FunctionCallExpression} knew which calls aggregate from a
 * list of names kept beside the planner, and the namespaced aggregates of the function registry were not on it. In Cypher these
 * are the ONLY spelling of them: the camelCase aliases of the SQL side are not Cypher names.
 * <p>
 * Per the openCypher reference an aggregate is a function that folds the rows of its group into one value, whatever its name;
 * the registry is what knows that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9768CypherDottedAggregateTest extends TestHelper {

  private void load() {
    final var type = database.getSchema().createVertexType("D");
    type.createProperty("g", Type.INTEGER);
    type.createProperty("n", Type.INTEGER);
    type.createProperty("v", Type.ARRAY_OF_FLOATS);
    database.transaction(() -> {
      database.newVertex("D").set("g", 0).set("n", 1).set("v", new float[] { 1f, 2f }).save();
      database.newVertex("D").set("g", 1).set("n", 2).set("v", new float[] { 2f, 1f }).save();
      database.newVertex("D").set("g", 0).set("n", 3).set("v", new float[] { 1f, 2f }).save();
      database.newVertex("D").set("g", 1).set("n", 4).set("v", new float[] { 2f, 3f }).save();
    });
  }

  private List<Result> run(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
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
  void dottedAggregatesAnswerOneRowWithTheFoldedValue() {
    load();
    final List<Result> sum = run("MATCH (d:D) RETURN vector.sum(d.v) AS s");
    assertThat(sum).hasSize(1);
    assertThat(vector(sum.getFirst(), "s")).containsExactly(6f, 8f);

    assertThat(vector(run("MATCH (d:D) RETURN vector.avg(d.v) AS s").getFirst(), "s")).containsExactly(1.5f, 2f);
    assertThat(vector(run("MATCH (d:D) RETURN vector.min(d.v) AS s").getFirst(), "s")).containsExactly(1f, 1f);
    assertThat(vector(run("MATCH (d:D) RETURN vector.max(d.v) AS s").getFirst(), "s")).containsExactly(2f, 3f);
  }

  @Test
  void dottedAggregateNextToAnotherAggregateAndGroupedByAKey() {
    load();
    final List<Result> together = run("MATCH (d:D) RETURN vector.sum(d.v) AS s, count(d) AS c");
    assertThat(together).hasSize(1);
    assertThat(together.getFirst().<Long>getProperty("c")).isEqualTo(4L);
    assertThat(vector(together.getFirst(), "s")).containsExactly(6f, 8f);

    final List<Result> grouped = run("MATCH (d:D) RETURN d.g AS g, vector.sum(d.v) AS s ORDER BY g");
    assertThat(grouped).hasSize(2);
    assertThat(vector(grouped.get(0), "s")).containsExactly(2f, 4f);
    assertThat(vector(grouped.get(1), "s")).containsExactly(4f, 4f);
  }

  @Test
  void theExplicitSqlPrefixAggregatesToo() {
    load();
    final List<Result> rows = run("MATCH (d:D) RETURN sql.vector.sum(d.v) AS s");
    assertThat(rows).hasSize(1);
    assertThat(vector(rows.getFirst(), "s")).containsExactly(6f, 8f);

    final List<Result> sum = run("MATCH (d:D) RETURN sql.sum(d.n) AS s");
    assertThat(sum).hasSize(1);
    assertThat(((Number) sum.getFirst().getProperty("s")).longValue()).isEqualTo(10L);
  }

  @Test
  void aNamespacedScalarStaysPerRow() {
    load();
    assertThat(run("MATCH (d:D) RETURN vector.magnitude(d.v) AS m")).hasSize(4);
  }

  @Test
  void aCypherScalarThatSharesANameWithASqlAggregateStaysPerRow() {
    load();
    // last() folds the rows in SQL; in Cypher it is the last element of a list
    final List<Result> rows = run("MATCH (d:D) RETURN last([1, 2, 3]) AS l");
    assertThat(rows).hasSize(4);
    assertThat(((Number) rows.getFirst().getProperty("l")).intValue()).isEqualTo(3);
  }

  /** A function that validates its arguments when it is configured, which the planner's probe configuration must survive. */
  public static class StrictConfig extends SQLFunctionAbstract {
    public StrictConfig() {
      super("acme9768.strict");
    }

    @Override
    public SQLFunction config(final Object[] configuredParameters) {
      throw new IllegalArgumentException("acme9768.strict wants a number");
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      return null;
    }

    @Override
    public String getSyntax() {
      return "acme9768.strict(<n>)";
    }
  }

  @Test
  void aFunctionThatRefusesTheProbeConfigurationIsNotAnAggregationAndDoesNotFailPlanning() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register("acme9768.strict", StrictConfig.class);
    try {
      assertThat(CypherFunctionFactory.isNamespacedAggregation("acme9768.strict", 1)).isFalse();
    } finally {
      factory.unregister("acme9768.strict");
    }
  }

  /**
   * The planner can only decide on what the probe configuration lets it see, so a registered function that refuses it would be
   * planned as a scalar again - one NULL per row, no error, which is the bug. This fails by name for such a function, and for any
   * registered dotted function the probe and the function itself disagree about.
   */
  @Test
  void everyRegisteredDottedSqlFunctionAcceptsTheProbeAndIsPlannedAsItsOwnFlagSays() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    final List<String> refused = new ArrayList<>();
    final List<String> aggregates = new ArrayList<>();
    for (final String name : new TreeSet<>(factory.getFunctionNames())) {
      if (name.indexOf('.') < 1 || CypherBuiltinFunctions.isBuiltin(name) || CypherFunctionRegistry.get(name) != null)
        continue;

      final int arguments = Math.max(1, factory.getFunctionInstance(name).getMinArgs());
      final Object[] placeholders = new Object[arguments];
      Arrays.fill(placeholders, "dummy");
      final boolean aggregatesByFlag;
      try {
        aggregatesByFlag = factory.getFunctionInstance(name).config(placeholders).aggregateResults();
      } catch (final RuntimeException e) {
        refused.add(name + " (" + e.getMessage() + ")");
        continue;
      }
      if (aggregatesByFlag)
        aggregates.add(name);
      assertThat(CypherFunctionFactory.isNamespacedAggregation(name, arguments)).as(name).isEqualTo(aggregatesByFlag);
    }

    assertThat(refused).as("registered dotted functions that refuse the planner's probe configuration").isEmpty();
    assertThat(aggregates).as("the dotted aggregates of the registry").contains("vector.sum", "vector.avg", "vector.min", "vector.max");
  }

  /** The functions only the Cypher registry holds say whether they aggregate themselves, with no configuration to refuse. */
  @Test
  void everyDottedFunctionOfTheCypherRegistryIsPlannedAsItsOwnFlagSays() {
    final List<String> aggregates = new ArrayList<>();
    for (final String name : new TreeSet<>(FunctionRegistry.getFunctionNames())) {
      final StatelessFunction function = CypherFunctionRegistry.get(name);
      if (name.indexOf('.') < 1 || function == null || CypherBuiltinFunctions.isBuiltin(name))
        continue;
      if (function.aggregateResults())
        aggregates.add(name);
      assertThat(CypherFunctionFactory.isNamespacedAggregation(name, 1)).as(name).isEqualTo(function.aggregateResults());
    }
    assertThat(aggregates).as("dotted aggregates that only the Cypher registry holds").doesNotContain("vector.sum");
  }

  @Test
  void aFunctionRegisteredAsAClassIsFreshAndOneRegisteredAsAnInstanceIsShared() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    assertThat(factory.isSharedInstance("vector.sum")).as("stateful aggregate").isFalse();
    assertThat(factory.getFunctionInstance("vector.sum")).isNotSameAs(factory.getFunctionInstance("vector.sum"));
    assertThat(factory.isSharedInstance("geo.point")).as("stateless function").isTrue();
    assertThat(factory.getFunctionInstance("geo.point")).isSameAs(factory.getFunctionInstance("geo.point"));
    assertThat(factory.isSharedInstance("no.such_function")).isFalse();
  }

  @Test
  void theRegistryAnswersWhichNamespacedCallsAggregate() {
    assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.sum", 1)).isTrue();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.avg", 1)).isTrue();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.min", 1)).isTrue();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.max", 1)).isTrue();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("sql.vector.sum", 1)).isTrue();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.magnitude", 1)).isFalse();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("text.indexof", 2)).isFalse();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("no.such_function", 1)).isFalse();
    assertThat(CypherFunctionFactory.isNamespacedAggregation("last", 1)).as("undotted names are never asked").isFalse();
  }
}
