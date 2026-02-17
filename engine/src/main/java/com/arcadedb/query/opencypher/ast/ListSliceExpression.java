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

import java.util.ArrayList;
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

    final int size;
    if (listValue instanceof List)
      size = ((List<?>) listValue).size();
    else if (listValue instanceof String)
      size = ((String) listValue).length();
    else
      throw new IllegalArgumentException("Cannot slice type: " + listValue.getClass().getSimpleName());

    // Resolve from index (default: 0)
    int from = 0;
    if (fromExpression != null) {
      final Object fromValue = fromExpression.evaluate(result, context);
      if (fromValue == null)
        return null;
      if (fromValue instanceof Number)
        from = ((Number) fromValue).intValue();
      else
        throw new IllegalArgumentException("Slice index must be a number, got: " + fromValue.getClass().getSimpleName());
    }

    // Resolve to index (default: size)
    int to = size;
    if (toExpression != null) {
      final Object toValue = toExpression.evaluate(result, context);
      if (toValue == null)
        return null;
      if (toValue instanceof Number)
        to = ((Number) toValue).intValue();
      else
        throw new IllegalArgumentException("Slice index must be a number, got: " + toValue.getClass().getSimpleName());
    }

    // Handle negative indices
    if (from < 0)
      from = Math.max(0, size + from);
    if (to < 0)
      to = Math.max(0, size + to);

    // Clamp to valid range
    from = Math.min(from, size);
    to = Math.min(to, size);

    // If from >= to, return empty
    if (from >= to) {
      if (listValue instanceof String)
        return "";
      return new ArrayList<>();
    }

    if (listValue instanceof List)
      return new ArrayList<>(((List<?>) listValue).subList(from, to));
    else
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
