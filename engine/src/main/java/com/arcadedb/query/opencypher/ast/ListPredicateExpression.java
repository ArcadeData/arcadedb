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
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;

/**
 * Expression representing a list predicate: all(), any(), none(), single().
 * Syntax: predicateType(variable IN listExpression WHERE filterExpression)
 * Examples:
 * - all(x IN [1, 2, 3] WHERE x > 0)   -> true if ALL items satisfy the condition
 * - any(x IN [1, 2, 3] WHERE x = 2)    -> true if at least one item satisfies the condition
 * - none(x IN [1, 2, 3] WHERE x < 0)   -> true if NO items satisfy the condition
 * - single(x IN [1, 2, 3] WHERE x = 2) -> true if EXACTLY ONE item satisfies the condition
 */
public class ListPredicateExpression implements Expression {

  public enum PredicateType {
    ALL, ANY, NONE, SINGLE
  }

  private final PredicateType predicateType;
  private final String variable;
  private final Expression listExpression;
  private final Expression whereExpression;
  private final String text;

  public ListPredicateExpression(final PredicateType predicateType, final String variable,
      final Expression listExpression, final Expression whereExpression, final String text) {
    this.predicateType = predicateType;
    this.variable = variable;
    this.listExpression = listExpression;
    this.whereExpression = whereExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listExpression, result, context);

    if (listValue == null)
      return null;

    final Iterable<?> iterable;
    if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else if (listValue.getClass().isArray())
      iterable = arrayToList(listValue);
    else
      throw new IllegalArgumentException("List predicate requires an iterable, got: " + listValue.getClass().getSimpleName());

    switch (predicateType) {
    case ALL:
      return evaluateAll(iterable, result, context);
    case ANY:
      return evaluateAny(iterable, result, context);
    case NONE:
      return evaluateNone(iterable, result, context);
    case SINGLE:
      return evaluateSingle(iterable, result, context);
    default:
      throw new IllegalStateException("Unknown predicate type: " + predicateType);
    }
  }

  private boolean evaluateAll(final Iterable<?> iterable, final Result result, final CommandContext context) {
    for (final Object item : iterable) {
      if (!testItem(item, result, context))
        return false;
    }
    return true;
  }

  private boolean evaluateAny(final Iterable<?> iterable, final Result result, final CommandContext context) {
    for (final Object item : iterable) {
      if (testItem(item, result, context))
        return true;
    }
    return false;
  }

  private boolean evaluateNone(final Iterable<?> iterable, final Result result, final CommandContext context) {
    for (final Object item : iterable) {
      if (testItem(item, result, context))
        return false;
    }
    return true;
  }

  private boolean evaluateSingle(final Iterable<?> iterable, final Result result, final CommandContext context) {
    int count = 0;
    for (final Object item : iterable) {
      if (testItem(item, result, context)) {
        count++;
        if (count > 1)
          return false;
      }
    }
    return count == 1;
  }

  private boolean testItem(final Object item, final Result baseResult, final CommandContext context) {
    if (whereExpression == null)
      return item != null;

    final ResultInternal iterResult = new ResultInternal();
    if (baseResult != null)
      for (final String prop : baseResult.getPropertyNames())
        iterResult.setProperty(prop, baseResult.getProperty(prop));
    iterResult.setProperty(variable, item);

    final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, iterResult, context);
    return filterValue instanceof Boolean && (Boolean) filterValue;
  }

  private List<Object> arrayToList(final Object array) {
    final List<Object> list = new ArrayList<>();
    if (array instanceof Object[]) {
      for (final Object o : (Object[]) array)
        list.add(o);
    } else if (array instanceof int[]) {
      for (final int i : (int[]) array)
        list.add(i);
    } else if (array instanceof long[]) {
      for (final long l : (long[]) array)
        list.add(l);
    } else if (array instanceof double[]) {
      for (final double d : (double[]) array)
        list.add(d);
    }
    return list;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public boolean containsAggregation() {
    if (listExpression.containsAggregation())
      return true;
    if (whereExpression != null && whereExpression.containsAggregation())
      return true;
    return false;
  }

  @Override
  public String getText() {
    return text;
  }
}
