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
import java.util.Arrays;
import java.util.List;

/**
 * Expression representing the Cypher allReduce() function (Cypher 25).
 * Syntax: allReduce(accumulator = initial, variable IN list | expression, predicate)
 * <p>
 * Returns true if, during the stepwise evaluation of a value across the elements in the list,
 * the accumulated result satisfies the predicate at every step. For an empty list, returns true.
 * <p>
 * Examples:
 * - allReduce(acc = 0, x IN [1, 2, 3] | acc + x, acc < 10) -> true (0<10, 1<10, 3<10, 6<10)
 * - allReduce(acc = 0, x IN [] | acc + x, acc < 10) -> true (vacuous truth)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AllReduceExpression implements Expression {
  private final String     accumulatorVariable;
  private final Expression initialValue;
  private final String     iteratorVariable;
  private final Expression listExpression;
  private final Expression reduceExpression;
  private final Expression predicateExpression;
  private final String     text;

  public AllReduceExpression(final String accumulatorVariable, final Expression initialValue,
                             final String iteratorVariable, final Expression listExpression,
                             final Expression reduceExpression, final Expression predicateExpression,
                             final String text) {
    this.accumulatorVariable = accumulatorVariable;
    this.initialValue = initialValue;
    this.iteratorVariable = iteratorVariable;
    this.listExpression = listExpression;
    this.reduceExpression = reduceExpression;
    this.predicateExpression = predicateExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Evaluate the initial value for the accumulator
    Object accumulator = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(initialValue, result, context);

    // Evaluate the list expression
    final Object listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listExpression, result, context);

    if (listValue == null)
      return true;

    // Convert to iterable
    final Iterable<?> iterable;
    if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else if (listValue.getClass().isArray())
      iterable = arrayToList(listValue);
    else
      throw new IllegalArgumentException("allReduce() requires a list, got: " + listValue.getClass().getSimpleName());

    // Check initial accumulator against predicate
    final ResultInternal initResult = createIterationResult(result, null, accumulator);
    final Object initCheck = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(predicateExpression, initResult, context);
    if (!Boolean.TRUE.equals(initCheck))
      return false;

    // Iterate over each element, updating the accumulator and checking the predicate
    for (final Object item : iterable) {
      final ResultInternal iterResult = createIterationResult(result, item, accumulator);

      // Evaluate the reduce expression to update accumulator
      accumulator = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(reduceExpression, iterResult, context);

      // Check predicate with new accumulator value
      final ResultInternal predicateResult = createIterationResult(result, item, accumulator);
      final Object predicateCheck = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(predicateExpression, predicateResult, context);

      if (!Boolean.TRUE.equals(predicateCheck))
        return false;
    }

    return true;
  }

  private ResultInternal createIterationResult(final Result baseResult, final Object item, final Object accumulator) {
    final ResultInternal iterResult = new ResultInternal();

    if (baseResult != null)
      for (final String prop : baseResult.getPropertyNames())
        iterResult.setProperty(prop, baseResult.getProperty(prop));

    if (item != null)
      iterResult.setProperty(iteratorVariable, item);
    iterResult.setProperty(accumulatorVariable, accumulator);
    return iterResult;
  }

  private List<Object> arrayToList(final Object array) {
    final List<Object> list = new ArrayList<>();
    if (array instanceof Object[]) {
      list.addAll(Arrays.asList((Object[]) array));
    } else if (array instanceof int[] a) {
      for (final int i : a)
        list.add(i);
    } else if (array instanceof long[] a) {
      for (final long l : a)
        list.add(l);
    } else if (array instanceof double[] a) {
      for (final double d : a)
        list.add(d);
    } else if (array instanceof float[] a) {
      for (final float f : a)
        list.add(f);
    } else if (array instanceof boolean[] a) {
      for (final boolean b : a)
        list.add(b);
    } else if (array instanceof byte[] a) {
      for (final byte b : a)
        list.add(b);
    } else if (array instanceof short[] a) {
      for (final short s : a)
        list.add(s);
    } else if (array instanceof char[] a) {
      for (final char c : a)
        list.add(c);
    }
    return list;
  }

  @Override
  public boolean isAggregation() {
    if (initialValue.isAggregation())
      return true;
    if (listExpression.isAggregation())
      return true;
    if (reduceExpression.isAggregation())
      return true;
    if (predicateExpression.isAggregation())
      return true;
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public String getAccumulatorVariable() {
    return accumulatorVariable;
  }

  public Expression getInitialValue() {
    return initialValue;
  }

  public String getIteratorVariable() {
    return iteratorVariable;
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public Expression getReduceExpression() {
    return reduceExpression;
  }

  public Expression getPredicateExpression() {
    return predicateExpression;
  }
}
