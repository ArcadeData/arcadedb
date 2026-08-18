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

import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * IN expression for WHERE clauses.
 * Example: n.name IN ['Alice', 'Bob', 'Charlie']
 */
public class InExpression implements BooleanExpression {
  /** {@code 2^53}: the first magnitude at which a double stops representing every long exactly. */
  private static final double EXACT_DOUBLE_LIMIT = 9007199254740992d;

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

    // Answers the walk below would pay O(n) for. Its cost is the POSITION of the match, and a miss walks all of
    // it, so on the lazy range() of advisory GHSA-xmjm-8q85-g778 an element near the end costs seconds (#6323).
    if (value == null)
      // Every comparison against null is null, whatever the elements are, so only their number matters: a
      // non-empty list makes this uncertain, an empty one leaves nothing to be uncertain about.
      return valuesToCheck.iterator().hasNext() ? null : Boolean.valueOf(isNot);

    if (valuesToCheck instanceof LongRangeList range) {
      final Boolean found = rangeMembership(range, value);
      if (found != null)
        return isNot != found;
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
   * Membership in a lazy range, answered from its start, step and size, or null when it cannot be answered that
   * way and the walk has to run. Never returns the 3VL {@code null} answer: a range holds longs and no nulls, and
   * a null left operand is answered before this is called, so uncertainty is impossible here.
   * <p>
   * What the walk asks per element is {@link #valuesCompare}, i.e. the {@code =} operator against a {@code Long}.
   * The branches of {@code ComparisonExpression.compareValuesTernary} that a {@code Long} right operand can reach
   * are exactly the three below - the RID-string interop, the numeric comparison, and "different types are not
   * equal" for everything else (a Long is not {@code Identifiable}, not temporal, and
   * {@code MultiValue.getMultiValueAsList} does not turn it into a list, so those branches cannot fire). A new
   * coercion added there and not learned here would make this diverge, which is what
   * {@code CypherInRangeMembershipTest.answersExactlyAsTheWalkDoes} exists to catch.
   */
  private static Boolean rangeMembership(final LongRangeList range, final Object value) {
    if (range.isEmpty())
      return Boolean.FALSE;

    // Integral types, answered as longs. The = operator's own long-vs-long branch covers only a Long/Integer
    // pair, so a Short or a Byte is compared there through doubleValue() instead - which reaches the same answer:
    // a value of at most 15 bits converts to a double exactly, and so does any element that could equal it, since
    // an element large enough to lose precision is far larger than any Short or Byte.
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
      return range.containsLong(((Number) value).longValue());

    if (value instanceof Number number) {
      // Any other numeric pair is compared by = through doubleValue(), so this has to be too.
      final double asDouble = number.doubleValue();
      if (Double.isNaN(asDouble) || Double.isInfinite(asDouble))
        // NaN equals nothing, not even itself (issue #5293), and no long is infinite.
        return Boolean.FALSE;
      if (Math.abs(asDouble) >= EXACT_DOUBLE_LIMIT)
        // From 2^53 up a double no longer distinguishes adjacent longs, so which elements it equals stops being a
        // question of one value: leave it to the walk, which is the definition of the answer. The bound is
        // inclusive because 2^53 ITSELF is already ambiguous - 2^53+1 is not representable and rounds ties-to-even
        // down onto 2^53, so both longs convert to this same double, and picking one of them would answer FALSE
        // for a range that holds only the other.
        return null;
      if (asDouble != Math.rint(asDouble))
        return Boolean.FALSE;
      // Below 2^53 every element is converted to a double exactly, so equality of the doubles is equality of the
      // longs, and no element at or above 2^53 can equal a double below it.
      return range.containsLong((long) asDouble);
    }

    // The = operator reads a RID-shaped string against a number as the id it denotes (Neo4j-compatible id()
    // interop): the same coercion, not a second one.
    if (value instanceof String string)
      return RID.is(string) ? range.containsLong(IdFunction.encodeRidAsLong(new RID(string))) : Boolean.FALSE;

    // Anything else is of a different type than a Long, and for = that is simply not equal.
    return Boolean.FALSE;
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
