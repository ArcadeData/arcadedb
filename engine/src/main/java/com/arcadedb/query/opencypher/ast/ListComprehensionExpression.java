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
 * Expression representing a list comprehension.
 * Syntax: [variable IN listExpression WHERE filterExpression | mapExpression]
 * Examples:
 * - [x IN [1, 2, 3] | x * 2] -> [2, 4, 6]
 * - [x IN list WHERE x > 5 | x.name] -> filtered and mapped list
 * - [x IN list WHERE x.active] -> filtered list (no mapping)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ListComprehensionExpression implements Expression {
  private final String variable;
  private final Expression listExpression;
  private final Expression whereExpression;  // Optional filter
  private final Expression mapExpression;    // Optional mapping (after |)
  private final String text;

  public ListComprehensionExpression(final String variable, final Expression listExpression, final Expression whereExpression,
      final Expression mapExpression, final String text) {
    this.variable = variable;
    this.listExpression = listExpression;
    this.whereExpression = whereExpression;
    this.mapExpression = mapExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Use ExpressionEvaluator to properly handle function calls like range()
    final Object listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listExpression, result, context);

    if (listValue == null)
      return null;

    // Convert to iterable
    final Iterable<?> iterable;
    if (listValue instanceof Iterable)
      iterable = (Iterable<?>) listValue;
    else if (listValue.getClass().isArray())
      iterable = arrayToList(listValue);
    else
      throw new IllegalArgumentException("List comprehension requires an iterable, got: " + listValue.getClass().getSimpleName());

    final List<Object> resultList = new ArrayList<>();

    for (final Object item : iterable) {
      // Create a new result with the iteration variable bound
      final ResultInternal iterResult = createIterationResult(result, item);

      // Apply WHERE filter if present
      if (whereExpression != null) {
        final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, iterResult, context);
        if (filterValue == null || (filterValue instanceof Boolean && !((Boolean) filterValue)))
          continue; // Skip items that don't match the filter
      }

      // Apply mapping expression if present, otherwise use the item itself
      if (mapExpression != null)
        resultList.add(OpenCypherQueryEngine.getExpressionEvaluator().evaluate(mapExpression, iterResult, context));
      else
        resultList.add(item);
    }

    return resultList;
  }

  private ResultInternal createIterationResult(final Result baseResult, final Object item) {
    final ResultInternal iterResult = new ResultInternal();

    // Copy all properties from the base result
    if (baseResult != null)
      for (final String prop : baseResult.getPropertyNames())
        iterResult.setProperty(prop, baseResult.getProperty(prop));

    // Add the iteration variable
    iterResult.setProperty(variable, item);
    return iterResult;
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
    } else if (array instanceof float[]) {
      for (final float f : (float[]) array)
        list.add(f);
    } else if (array instanceof boolean[]) {
      for (final boolean b : (boolean[]) array)
        list.add(b);
    } else if (array instanceof byte[]) {
      for (final byte b : (byte[]) array)
        list.add(b);
    } else if (array instanceof short[]) {
      for (final short s : (short[]) array)
        list.add(s);
    } else if (array instanceof char[]) {
      for (final char c : (char[]) array)
        list.add(c);
    }
    return list;
  }

  @Override
  public boolean isAggregation() {
    if (listExpression.isAggregation())
      return true;
    if (whereExpression != null && whereExpression.isAggregation())
      return true;
    if (mapExpression != null && mapExpression.isAggregation())
      return true;
    return false;
  }

  @Override
  public boolean containsAggregation() {
    if (listExpression.containsAggregation())
      return true;
    if (whereExpression != null && whereExpression.containsAggregation())
      return true;
    if (mapExpression != null && mapExpression.containsAggregation())
      return true;
    return false;
  }

  @Override
  public String getText() {
    return text;
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

  public Expression getMapExpression() {
    return mapExpression;
  }
}
