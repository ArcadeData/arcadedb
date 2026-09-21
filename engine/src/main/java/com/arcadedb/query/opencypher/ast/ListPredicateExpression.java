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
import com.arcadedb.query.sql.executor.ResultInternal;

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

  /**
   * Evaluates all() with three-valued logic.
   * Returns null if any condition is null and none is false.
   */
  private Object evaluateAll(final Iterable<?> iterable, final Result result, final CommandContext context) {
    boolean hasNull = false;
    for (final Object item : iterable) {
      final Boolean test = testItem(item, result, context);
      if (test == null)
        hasNull = true;
      else if (!test)
        return false;
    }
    return hasNull ? null : true;
  }

  /**
   * Evaluates any() with three-valued logic.
   * Returns null if any condition is null and none is true.
   */
  private Object evaluateAny(final Iterable<?> iterable, final Result result, final CommandContext context) {
    boolean hasNull = false;
    for (final Object item : iterable) {
      final Boolean test = testItem(item, result, context);
      if (test == null)
        hasNull = true;
      else if (test)
        return true;
    }
    return hasNull ? null : false;
  }

  /**
   * Evaluates none() with three-valued logic.
   * Returns null if any condition is null and none is true.
   */
  private Object evaluateNone(final Iterable<?> iterable, final Result result, final CommandContext context) {
    boolean hasNull = false;
    for (final Object item : iterable) {
      final Boolean test = testItem(item, result, context);
      if (test == null)
        hasNull = true;
      else if (test)
        return false;
    }
    return hasNull ? null : true;
  }

  /**
   * Evaluates single() with three-valued logic.
   * Returns null if uncertain due to nulls.
   */
  private Object evaluateSingle(final Iterable<?> iterable, final Result result, final CommandContext context) {
    int trueCount = 0;
    boolean hasNull = false;
    for (final Object item : iterable) {
      final Boolean test = testItem(item, result, context);
      if (test == null)
        hasNull = true;
      else if (test) {
        trueCount++;
        if (trueCount > 1)
          return false;
      }
    }
    if (trueCount == 1 && !hasNull)
      return true;
    if (trueCount == 0 && !hasNull)
      return false;
    // With nulls: uncertain
    return null;
  }

  /**
   * Tests a single item against the WHERE expression.
   * Returns Boolean.TRUE, Boolean.FALSE, or null (for three-valued logic).
   */
  private Boolean testItem(final Object item, final Result baseResult, final CommandContext context) {
    if (whereExpression == null)
      return item != null ? Boolean.TRUE : null;

    final ResultInternal iterResult = new ResultInternal();
    if (baseResult != null)
      for (final String prop : baseResult.getPropertyNames())
        iterResult.setProperty(prop, baseResult.getProperty(prop));
    iterResult.setProperty(variable, item);

    final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, iterResult, context);
    if (filterValue == null)
      return null;
    if (filterValue instanceof Boolean)
      return (Boolean) filterValue;
    return null;
  }

  /**
   * Boxes any array by its actual component type. The hand-rolled ladder this replaced covered only
   * {@code Object[]}, {@code int[]}, {@code long[]} and {@code double[]}, so an {@code ARRAY_OF_FLOATS} or
   * {@code ARRAY_OF_SHORTS} property - or any other component type - produced an empty element list, which
   * turns {@code all()} into vacuous truth and {@code any()} into false with no error (issue #8036).
   * <p>
   * A {@code byte[]} is boxed here one byte at a time, which is what {@code ListComprehensionExpression},
   * {@code ReduceExpression} and {@code AllReduceExpression} already do through the explicit {@code byte[]} arm
   * in each of their own ladders. It is deliberately NOT the rule {@code UnwindStep} takes, which excludes a
   * {@code byte[]} through {@code MultiValue.isSequenceArray()} so that a {@code BINARY} property stays one
   * opaque value, as it is in SQL. Issue #8098 is where that disagreement gets settled for every clause at
   * once; until then the behaviour here is pinned by
   * {@code CypherUnwindArrayComponentTypeIssue8036Test.listPredicatesOverABinaryPropertyEvaluateOncePerByteUntilIssue8098IsSettled}.
   */
  private List<Object> arrayToList(final Object array) {
    return MultiValue.getMultiValueAsList(array);
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

  public String getVariable() {
    return variable;
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public PredicateType getPredicateType() {
    return predicateType;
  }

  @Override
  public String getText() {
    return text;
  }
}
