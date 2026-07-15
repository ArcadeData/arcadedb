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
import com.arcadedb.query.sql.executor.MultiValue;
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

  // Membership is equality against each element, so the element check reuses the = operator's
  // comparator (issue #5293) rather than a second, drifting implementation. Held per AST node so no
  // wrapper is allocated per element per row.
  private final ComparisonExpression equalityComparator = ComparisonExpression.valueComparator(ComparisonExpression.Operator.EQUALS);

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

    // Iterable of values to compare against. For lists/Object[]/collections this is a zero-copy view;
    // primitive arrays (long[], double[], ...) are auto-boxed by getMultiValueAsList in a single pass.
    final Iterable<?> valuesToCheck;

    if (list.size() == 1) {
      // Single expression on RHS (e.g., x IN listVar, x IN func(), x IN [1,2,3]). Evaluate and unwrap.
      final Expression listItem = list.get(0);
      final Object listValue;
      if (listItem instanceof FunctionCallExpression)
        listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
      else
        listValue = listItem.evaluate(result, context);

      if (listValue == null)
        return null; // x IN null -> null
      if (listValue instanceof Collection<?> coll) {
        valuesToCheck = coll;
      } else if (listValue.getClass().isArray()) {
        // Includes primitive arrays (long[], int[], double[], ...) coming from JSON numeric-array
        // query parameters (issue #4284).
        valuesToCheck = MultiValue.getMultiValueAsList(listValue);
      } else
        throw new IllegalArgumentException(
            "InvalidArgumentType: IN requires a list on the right side, got " + listValue.getClass().getSimpleName());
    } else {
      // Multiple expressions (parsed list literal items)
      final List<Object> evaluated = new ArrayList<>(list.size());
      for (final Expression listItem : list) {
        final Object listValue;
        if (listItem instanceof FunctionCallExpression)
          listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
        else
          listValue = listItem.evaluate(result, context);
        evaluated.add(listValue);
      }
      valuesToCheck = evaluated;
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
   * Three-valued comparison of one list element against the left operand.
   * Returns Boolean.TRUE if definitely equal, Boolean.FALSE if definitely not equal,
   * null if uncertain (involves null comparisons where non-null elements match).
   * <p>
   * Membership is defined in terms of equality, so this delegates to the {@code =} operator's
   * comparator instead of re-implementing it (issue #5293). The previous hand-rolled version tried
   * {@code a.equals(b)} before dispatching on type, which imported Java's Double.equals contract
   * (NaN equals itself) and Java's Map.equals contract (null equals null) into Cypher, making
   * {@code NaN IN [NaN]} return true while {@code NaN = NaN} returns false. Delegating also gives IN
   * the temporal coercion, array coercion and map 3VL that {@code =} already implements.
   */
  private Boolean valuesCompare(final Object a, final Object b) {
    final Object cmp = equalityComparator.evaluateWithValues(a, b);
    return cmp == null ? null : (Boolean) cmp;
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
