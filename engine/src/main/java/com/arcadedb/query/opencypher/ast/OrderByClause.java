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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents an ORDER BY clause in a Cypher query.
 * Specifies how results should be sorted.
 * <p>
 * Example: ORDER BY n.name ASC, n.age DESC
 */
public class OrderByClause {
  private final List<OrderByItem> items;

  /**
   * Creates an ORDER BY clause.
   *
   * @param items list of order by items
   */
  public OrderByClause(final List<OrderByItem> items) {
    this.items = items != null ? new ArrayList<>(items) : new ArrayList<>();
  }

  /**
   * Returns the list of order by items.
   *
   * @return list of order by items
   */
  public List<OrderByItem> getItems() {
    return Collections.unmodifiableList(items);
  }

  /**
   * Returns true if there are no order by items.
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return items.isEmpty();
  }

  /**
   * Represents a single item in an ORDER BY clause.
   */
  public static class OrderByItem {
    private final String expression;
    private final boolean ascending;
    private final Expression expressionAST;

    public OrderByItem(final String expression, final boolean ascending) {
      this(expression, ascending, null);
    }

    public OrderByItem(final String expression, final boolean ascending, final Expression expressionAST) {
      this.expression = expression;
      this.ascending = ascending;
      this.expressionAST = expressionAST;
    }

    public String getExpression() {
      return expression;
    }

    public boolean isAscending() {
      return ascending;
    }

    public boolean isDescending() {
      return !ascending;
    }

    public Expression getExpressionAST() {
      return expressionAST;
    }
  }
}
