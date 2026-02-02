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
import com.arcadedb.query.sql.executor.Result;

import java.util.List;

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

    // Convert index to integer
    final int index;
    if (indexValue instanceof Number) {
      index = ((Number) indexValue).intValue();
    } else {
      throw new IllegalArgumentException("List index must be a number, got: " + indexValue.getClass().getSimpleName());
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

    // Handle string indexing (get character at position)
    if (listValue instanceof String) {
      final String str = (String) listValue;
      if (str.isEmpty()) {
        return null;
      }

      // Support negative indices
      final int actualIndex = index < 0 ? str.length() + index : index;

      if (actualIndex < 0 || actualIndex >= str.length()) {
        return null;
      }

      return String.valueOf(str.charAt(actualIndex));
    }

    throw new IllegalArgumentException("Cannot index into type: " + listValue.getClass().getSimpleName());
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
