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

import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;

import java.util.Collection;
import java.util.Map;

/**
 * Expression representing list/array indexing.
 * Example: list[0], collection[i], items[index]
 *
 * Supports negative indices (Python-style): list[-1] returns the last element.
 */
public class ListIndexExpression implements Expression {
  private final Expression listExpression;
  private final Expression indexExpression;

  public ListIndexExpression(final Expression listExpression, final Expression indexExpression) {
    this.listExpression = listExpression;
    this.indexExpression = indexExpression;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Route through the shared evaluator so that aggregation overrides applied
    // by AggregationStep / GroupByAggregationStep are honored when the list or
    // index sub-expression contains an inline aggregator (issue #4100).
    final Object listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listExpression, result, context);
    if (listValue == null) {
      return null;
    }

    final Object indexValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(indexExpression, result, context);
    if (indexValue == null) {
      return null;
    }

    // Treat Collections and Java arrays (incl. primitive arrays from numeric-array parameters,
    // issue #4284) uniformly as Cypher lists, without copying.
    final boolean isListLike = listValue instanceof Collection || listValue.getClass().isArray();

    // Map/Document property access via bracket notation: map['key']
    if (indexValue instanceof String key) {
      if (listValue instanceof Map<?, ?> map)
        return map.get(key);
      if (listValue instanceof Document document)
        return document.get(key);
      if (listValue instanceof Result result1)
        return result1.getProperty(key);
      // List with string index is a type error in Cypher
      if (isListLike)
        throw new IllegalArgumentException("List index must be a number, got: String");
      return null;
    }

    // Convert index to integer — validate type for list indexing
    final int index;
    if (indexValue instanceof Long || indexValue instanceof Integer)
      index = ((Number) indexValue).intValue();
    else if (indexValue instanceof Double || indexValue instanceof Float) {
      // Float indices are a type error for lists in Cypher
      if (isListLike)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got Float");
      index = ((Number) indexValue).intValue();
    } else if (indexValue instanceof Boolean) {
      if (isListLike)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got Boolean");
      return null;
    } else if (indexValue instanceof Number number)
      index = number.intValue();
    else {
      if (isListLike)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got " + indexValue.getClass().getSimpleName());
      return null;
    }

    if (isListLike) {
      // O(1) access on lists and arrays - no intermediate copy, so vector embeddings stay cheap.
      final int size = MultiValue.getSize(listValue);
      if (size == 0)
        return null;
      final int actualIndex = index < 0 ? size + index : index;
      if (actualIndex < 0 || actualIndex >= size)
        return null; // Out of bounds returns null (Cypher behavior)
      return MultiValue.getValue(listValue, actualIndex);
    }

    // In Cypher, numeric indexing is only valid for lists, not for strings or other types
    throw new IllegalArgumentException(
        "TypeError: InvalidArgumentType - Cannot index into type: " + listValue.getClass().getSimpleName());
  }

  @Override
  public boolean isAggregation() {
    return listExpression.isAggregation() || indexExpression.isAggregation();
  }

  @Override
  public boolean containsAggregation() {
    return listExpression.containsAggregation() || indexExpression.containsAggregation();
  }

  @Override
  public String getText() {
    return listExpression.getText() + "[" + indexExpression.getText() + "]";
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public Expression getIndexExpression() {
    return indexExpression;
  }
}
