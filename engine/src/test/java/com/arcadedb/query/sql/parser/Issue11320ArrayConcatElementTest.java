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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #11320 (children #11300 and #11306): an operand of {@code ||} is an
 * {@link ArrayConcatExpressionElement}, an {@link Expression} subclass that re-listed its superclass's fields by hand in
 * {@code copy()} and {@code splitForAggregation()} and fell behind it.
 * <ul>
 * <li>#11300: {@code copy()} left {@code whereCondition} and {@code arrayConcatExpression} behind, so a parenthesised
 * condition used as an operand of {@code ||} silently disappeared from the result (the planner executes the copy);</li>
 * <li>#11306: the aggregate split moved the operand's nested projection onto the aggregate item, where nothing applies
 * it, and {@code toString()} never printed it, so the projection was not applied and a materialized view persisted a
 * definition without it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11320ArrayConcatElementTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T");
      database.newDocument("T").set("a", 5).save();
    });
  }

  @Test
  void aParenthesisedConditionSurvivesTheCopyOfAConcatOperand() {
    for (final String sql : List.of("SELECT (1 > 0) || [1] AS v", "SELECT [1] || (2 > 1) AS v")) {
      final Statement parsed = parse(sql);
      final Statement copy = parsed.copy();
      assertThat(copy.toString()).as("copy of '%s'", sql).isEqualTo(parsed.toString());
      assertThat(copy).as("copy of '%s'", sql).isEqualTo(parsed);
    }
  }

  @Test
  void aParenthesisedConditionIsConcatenatedLikeAnyOtherOperand() {
    assertThat(single("SELECT (a > 1) || [1] AS v FROM T")).isEqualTo(List.of(true, 1));
    assertThat(single("SELECT [1] || (a > 1) AS v FROM T")).isEqualTo(List.of(1, true));
    assertThat(single("SELECT [1] || (a > 10) || [2] AS v FROM T")).isEqualTo(List.of(1, false, 2));
    // the controls of the report: a scalar and a boolean literal were always concatenated
    assertThat(single("SELECT true || [1] AS v FROM T")).isEqualTo(List.of(true, 1));
  }

  /**
   * The census: every field {@link Expression} declares (the authority {@code execute()} reads) must survive
   * {@link ArrayConcatExpressionElement#copy()}, so the next field added to {@link Expression} cannot be dropped by the
   * subclass again.
   */
  @Test
  void copyOfAConcatOperandCarriesEveryFieldExpressionDeclares() throws Exception {
    final List<String> checked = new ArrayList<>();
    for (final Field field : Expression.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()))
        continue;
      field.setAccessible(true);

      final ArrayConcatExpressionElement original = new ArrayConcatExpressionElement();
      final Object value = sampleValueOf(field.getType());
      field.set(original, value);

      final ArrayConcatExpressionElement copy = original.copy();
      final Object copied = field.get(copy);
      assertThat(copied).as("Expression.%s after ArrayConcatExpressionElement.copy()", field.getName()).isNotNull();
      if (value instanceof SimpleNode)
        assertThat(copied).as("Expression.%s must be deep-copied", field.getName()).isNotSameAs(value).isEqualTo(value);
      else
        assertThat(copied).as("Expression.%s after ArrayConcatExpressionElement.copy()", field.getName()).isEqualTo(value);
      checked.add(field.getName());
    }
    assertThat(checked).as("the census must still reach the fields of Expression that #11300 lost")
        .contains("whereCondition", "arrayConcatExpression", "mathExpression", "json", "rid", "booleanValue", "isNull");
  }

  @Test
  void aNestedProjectionOnAnAggregateOperandIsApplied() {
    assertThat(single("SELECT list({'x':1,'y':2}):{x} || [] AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
    assertThat(single("SELECT [] || list({'x':1,'y':2}):{x} AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
    assertThat(single("SELECT list({'x':1,'y':2}):{!x} || [3] AS v FROM T")).isEqualTo(List.of(Map.of("y", 2), 3));
    // the controls: without || the projection was always applied
    assertThat(single("SELECT list({'x':1,'y':2}):{x} AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
  }

  @Test
  void aNestedProjectionOnAConcatOperandIsRenderedAndKeptByTheCopy() {
    final String sql = "SELECT list({'x':1}):{x} || [] AS v";
    final Statement parsed = parse(sql);
    assertThat(parsed.toString()).contains(":{x}");
    assertThat(parsed.copy().toString()).isEqualTo(parsed.toString());
    // the rendering parses back to the same statement
    assertThat(parse(parsed.toString())).isEqualTo(parsed);

    // equals() tells two operands apart by their projection
    assertThat(parse("SELECT list({'x':1}):{x} || [] AS v")).isNotEqualTo(parse("SELECT list({'x':1}):{y} || [] AS v"));
  }

  @Test
  void aMaterializedViewKeepsTheNestedProjectionOfAConcatOperand() {
    database.command("sql", "CREATE MATERIALIZED VIEW mvn AS SELECT list({'x':1,'y':2}):{x} || [] AS v FROM T");
    try {
      assertThat(database.getSchema().getMaterializedView("mvn").getQuery()).contains(":{x}");
      assertThat(single("SELECT v FROM mvn")).isEqualTo(List.of(Map.of("x", 1)));
    } finally {
      database.command("sql", "DROP MATERIALIZED VIEW mvn");
    }
  }

  /** The {@code v} column of the only row, with every projected {@link Result} turned into its map. */
  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Result row = rs.next();
      assertThat(rs.hasNext()).as(sql).isFalse();
      return normalize(row.getProperty("v"));
    }
  }

  private static Object normalize(final Object value) {
    if (value instanceof Result result)
      return result.toMap();
    if (value instanceof List<?> list) {
      final List<Object> normalized = new ArrayList<>(list.size());
      for (final Object element : list)
        normalized.add(normalize(element));
      return normalized;
    }
    return value;
  }

  private Statement parse(final String sql) {
    return ((DatabaseInternal) database).getStatementCache().get(sql);
  }

  /** A real, non-default value of each kind of field {@link Expression} declares, parsed so its own copy() works. */
  private Object sampleValueOf(final Class<?> type) {
    if (type == boolean.class)
      return true;
    if (type == Boolean.class)
      return Boolean.TRUE;
    final Object value = switch (type.getSimpleName()) {
      case "Rid" -> projectionOf("SELECT #1:2 AS v").rid;
      case "MathExpression" -> projectionOf("SELECT 1 + 2 AS v").mathExpression;
      case "ArrayConcatExpression" -> projectionOf("SELECT [1] || [2] AS v").arrayConcatExpression;
      // a map literal is parsed as an Expression holding the Json, wrapped in a BaseExpression
      case "Json" -> ((BaseExpression) projectionOf("SELECT {'k': 1} AS v").mathExpression).expression.json;
      case "WhereClause" -> projectionOf("SELECT (1 > 0) AS v").whereCondition;
      default -> throw new AssertionError("no sample for the Expression field type " + type.getName()
          + ": add one, so the census keeps covering every field");
    };
    assertThat(value).as("the sample statement must populate a %s", type.getSimpleName()).isNotNull();
    return value;
  }

  private Expression projectionOf(final String sql) {
    return ((SelectStatement) parse(sql)).getProjection().getItems().getFirst().getExpression();
  }
}
