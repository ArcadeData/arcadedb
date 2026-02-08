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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * IN expression for WHERE clauses.
 * Example: n.name IN ['Alice', 'Bob', 'Charlie']
 */
public class InExpression implements BooleanExpression {
  private final Expression expression;
  private final List<Expression> list;
  private final boolean isNot;

  public InExpression(final Expression expression, final List<Expression> list, final boolean isNot) {
    this.expression = expression;
    this.list = list;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value;

    if (expression instanceof FunctionCallExpression)
      value = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, result, context);
    else
      value = expression.evaluate(result, context);

    // Build the list of values to check against
    final List<Object> valuesToCheck = new ArrayList<>();

    if (list.size() == 1) {
      // Single expression on RHS (e.g., x IN listVar, x IN func(), x IN [1,2,3])
      // Evaluate and unwrap the list value
      final Expression listItem = list.get(0);
      final Object listValue;
      if (listItem instanceof FunctionCallExpression)
        listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
      else
        listValue = listItem.evaluate(result, context);

      if (listValue == null)
        return null; // x IN null -> null
      if (listValue instanceof List)
        valuesToCheck.addAll((List<?>) listValue);
      else if (listValue instanceof Collection)
        valuesToCheck.addAll((Collection<?>) listValue);
      else
        throw new IllegalArgumentException("InvalidArgumentType: IN requires a list on the right side, got " + listValue.getClass().getSimpleName());
    } else {
      // Multiple expressions (parsed list literal items)
      for (final Expression listItem : list) {
        final Object listValue;
        if (listItem instanceof FunctionCallExpression)
          listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
        else
          listValue = listItem.evaluate(result, context);
        valuesToCheck.add(listValue);
      }
    }

    // 3VL: null IN [1,2,3] -> null, 5 IN [1,null,3] -> null (if not found otherwise)
    boolean foundNull = false;
    for (final Object checkValue : valuesToCheck) {
      final Boolean cmp = valuesCompare(value, checkValue);
      if (cmp == null)
        foundNull = true;
      else if (cmp)
        return isNot ? false : true;
    }

    if (foundNull)
      return null;

    return isNot ? true : false;
  }

  /**
   * Three-valued comparison for IN operator.
   * Returns Boolean.TRUE if definitely equal, Boolean.FALSE if definitely not equal,
   * null if uncertain (involves null comparisons where non-null elements match).
   */
  private Boolean valuesCompare(final Object a, final Object b) {
    if (a == null || b == null)
      return null; // null = anything is null in Cypher

    // List equality: must use 3VL element-wise comparison (not Java equals)
    // because Java's ArrayList.equals treats null==null as true, but Cypher requires null
    if (a instanceof List && b instanceof List) {
      final List<?> listA = (List<?>) a;
      final List<?> listB = (List<?>) b;
      if (listA.size() != listB.size())
        return false;
      boolean hasNull = false;
      for (int i = 0; i < listA.size(); i++) {
        final Boolean elemCmp = valuesCompare(listA.get(i), listB.get(i));
        if (elemCmp == null)
          hasNull = true;
        else if (!elemCmp)
          return false; // Definitely not equal
      }
      return hasNull ? null : true;
    }

    // Direct equality (handles booleans, strings, etc.)
    if (a.equals(b))
      return true;

    // Numeric comparison (handles int/long/double cross-type)
    if (a instanceof Number && b instanceof Number) {
      if ((a instanceof Long || a instanceof Integer) && (b instanceof Long || b instanceof Integer))
        return ((Number) a).longValue() == ((Number) b).longValue();
      return ((Number) a).doubleValue() == ((Number) b).doubleValue();
    }

    // Different types that aren't both numbers
    return false;
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    sb.append(expression.getText());
    sb.append(isNot ? " NOT IN [" : " IN [");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(list.get(i).getText());
    }
    sb.append("]");
    return sb.toString();
  }

  public Expression getExpression() {
    return expression;
  }

  public List<Expression> getList() {
    return list;
  }

  public boolean isNot() {
    return isNot;
  }
}
