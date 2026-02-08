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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
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
    final Object listValue = listExpression.evaluate(result, context);
    if (listValue == null) {
      return null;
    }

    final Object indexValue = indexExpression.evaluate(result, context);
    if (indexValue == null) {
      return null;
    }

    // Map/Document property access via bracket notation: map['key']
    if (indexValue instanceof String) {
      final String key = (String) indexValue;
      if (listValue instanceof Map)
        return ((Map<?, ?>) listValue).get(key);
      if (listValue instanceof Document)
        return ((Document) listValue).get(key);
      if (listValue instanceof Result)
        return ((Result) listValue).getProperty(key);
      // List with string index is a type error in Cypher
      if (listValue instanceof List)
        throw new IllegalArgumentException("List index must be a number, got: String");
      return null;
    }

    // Convert index to integer — validate type for list indexing
    final int index;
    if (indexValue instanceof Long || indexValue instanceof Integer)
      index = ((Number) indexValue).intValue();
    else if (indexValue instanceof Double || indexValue instanceof Float) {
      // Float indices are a type error for lists in Cypher
      if (listValue instanceof List)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got Float");
      index = ((Number) indexValue).intValue();
    } else if (indexValue instanceof Boolean) {
      if (listValue instanceof List)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got Boolean");
      return null;
    } else if (indexValue instanceof Number)
      index = ((Number) indexValue).intValue();
    else {
      if (listValue instanceof List)
        throw new IllegalArgumentException("TypeError: list index must be an integer, got " + indexValue.getClass().getSimpleName());
      return null;
    }

    // Handle list types
    if (listValue instanceof List) {
      final List<?> list = (List<?>) listValue;
      if (list.isEmpty()) {
        return null;
      }

      // Support negative indices (Python-style)
      final int actualIndex = index < 0 ? list.size() + index : index;

      if (actualIndex < 0 || actualIndex >= list.size()) {
        return null; // Out of bounds returns null (Cypher behavior)
      }

      return list.get(actualIndex);
    }

    // Handle array types
    if (listValue.getClass().isArray()) {
      final Object[] array = (Object[]) listValue;
      if (array.length == 0) {
        return null;
      }

      // Support negative indices
      final int actualIndex = index < 0 ? array.length + index : index;

      if (actualIndex < 0 || actualIndex >= array.length) {
        return null;
      }

      return array[actualIndex];
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
