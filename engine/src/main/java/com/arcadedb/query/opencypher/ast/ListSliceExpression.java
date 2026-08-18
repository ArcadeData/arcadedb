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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.LongRangeList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Expression representing list slicing: list[from..to]
 * Both from and to are optional. Supports negative indices (Python-style).
 * Examples: list[0..3], list[1..], list[..5], list[-3..]
 */
public class ListSliceExpression implements Expression {
  private final Expression listExpression;
  private final Expression fromExpression; // null means start from beginning
  private final Expression toExpression;   // null means go to end

  public ListSliceExpression(final Expression listExpression, final Expression fromExpression, final Expression toExpression) {
    this.listExpression = listExpression;
    this.fromExpression = fromExpression;
    this.toExpression = toExpression;
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public Expression getFromExpression() {
    return fromExpression;
  }

  public Expression getToExpression() {
    return toExpression;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object listValue = listExpression.evaluate(result, context);
    if (listValue == null)
      return null;

    final Integer from = sliceBound(fromExpression, result, context);
    if (fromExpression != null && from == null)
      return null;
    final Integer to = sliceBound(toExpression, result, context);
    if (toExpression != null && to == null)
      return null;

    return slice(listValue, from, to);
  }

  private static Integer sliceBound(final Expression bound, final Result result, final CommandContext context) {
    return bound == null ? null : sliceBound(bound.evaluate(result, context));
  }

  /**
   * A slice bound from its already evaluated value: the index it denotes, or null when the value is null - which
   * makes the whole slice null, as any null operand does. Shared with {@code ExpressionEvaluator}, which evaluates
   * the operand through itself: without it that path cast straight to {@code Number} and answered a non-numeric
   * bound with a raw ClassCastException instead of saying what was wrong with the query (issue #6323).
   */
  public static Integer sliceBound(final Object value) {
    if (value == null)
      return null;
    if (value instanceof Number number)
      return number.intValue();
    throw new IllegalArgumentException("Slice index must be a number, got: " + value.getClass().getSimpleName());
  }

  /**
   * Applies a Cypher slice to an already evaluated list, array or string, with the bounds already evaluated and
   * {@code null} meaning "from the beginning" / "to the end". Shared with
   * {@code ExpressionEvaluator.evaluateListSlice}, which resolves the operands through itself so aggregation
   * overrides apply, but must then slice identically: the two used to carry a copy of this each, and only one of
   * them was fixed at a time (issue #6323).
   */
  public static Object slice(final Object listValue, final Integer fromIndex, final Integer toIndex) {
    // Treat Collections and Java arrays (incl. primitive arrays from numeric-array parameters,
    // issue #4284) uniformly as Cypher lists without copying upfront.
    final boolean isListLike = listValue instanceof Collection || listValue.getClass().isArray();
    final int size;
    if (isListLike)
      size = MultiValue.getSize(listValue);
    else if (listValue instanceof String string)
      size = string.length();
    else
      throw new IllegalArgumentException("Cannot slice type: " + listValue.getClass().getSimpleName());

    int from = fromIndex != null ? fromIndex : 0;
    int to = toIndex != null ? toIndex : size;

    // Handle negative indices
    if (from < 0)
      from = Math.max(0, size + from);
    if (to < 0)
      to = Math.max(0, size + to);

    // Clamp to valid range
    from = Math.min(from, size);
    to = Math.min(to, size);

    // If from >= to, return empty
    if (from >= to)
      return listValue instanceof String ? "" : new ArrayList<>();

    if (listValue instanceof LongRangeList range)
      // A slice of an arithmetic progression is one, and subList() returns it in constant space. Copying it
      // instead reinstated the heap exhaustion the lazy range removed (advisory GHSA-xmjm-8q85-g778): a slice can
      // be as large as the range, so range(0, 999999999)[0..1000000000] allocated a billion boxed longs (#6323).
      return range.subList(from, to);

    if (isListLike) {
      // Boxing into a List is unavoidable here - the Cypher slice result is itself a List - but only
      // copy the requested range, not the whole input.
      final List<Object> slice = new ArrayList<>(to - from);
      for (int i = from; i < to; i++)
        slice.add(MultiValue.getValue(listValue, i));
      return slice;
    }
    return ((String) listValue).substring(from, to);
  }

  @Override
  public boolean isAggregation() {
    return listExpression.isAggregation()
        || (fromExpression != null && fromExpression.isAggregation())
        || (toExpression != null && toExpression.isAggregation());
  }

  @Override
  public boolean containsAggregation() {
    return listExpression.containsAggregation()
        || (fromExpression != null && fromExpression.containsAggregation())
        || (toExpression != null && toExpression.containsAggregation());
  }

  @Override
  public String getText() {
    final String fromText = fromExpression != null ? fromExpression.getText() : "";
    final String toText = toExpression != null ? toExpression.getText() : "";
    return listExpression.getText() + "[" + fromText + ".." + toText + "]";
  }
}
