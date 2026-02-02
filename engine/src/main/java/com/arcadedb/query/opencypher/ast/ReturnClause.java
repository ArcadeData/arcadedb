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
import java.util.List;

/**
 * Represents a RETURN clause in a Cypher query.
 * Specifies what to return from the query.
 */
public class ReturnClause {
  private final List<ReturnItem> items;

  /**
   * Create a ReturnClause from string items (backward compatibility).
   */
  public ReturnClause(final List<String> items) {
    this.items = new ArrayList<>();
    for (final String item : items) {
      // Parse simple items into expressions
      if (item.contains(".")) {
        final String[] parts = item.split("\\.", 2);
        this.items.add(new ReturnItem(new PropertyAccessExpression(parts[0], parts[1]), item));
      } else {
        this.items.add(new ReturnItem(new VariableExpression(item), item));
      }
    }
  }

  /**
   * Create a ReturnClause from expression items.
   */
  public ReturnClause(final List<ReturnItem> items, final boolean dummy) {
    this.items = items;
  }

  /**
   * Get items as strings (backward compatibility).
   */
  public List<String> getItems() {
    final List<String> result = new ArrayList<>();
    for (final ReturnItem item : items) {
      result.add(item.getAlias() != null ? item.getAlias() : item.getExpression().getText());
    }
    return result;
  }

  /**
   * Get return items as expressions.
   */
  public List<ReturnItem> getReturnItems() {
    return items;
  }

  /**
   * Check if any return item is or contains an aggregation function.
   * This includes both direct aggregations (e.g., COLLECT(x)) and
   * wrapped aggregations (e.g., HEAD(COLLECT(x))).
   */
  public boolean hasAggregations() {
    for (final ReturnItem item : items) {
      if (item.getExpression().containsAggregation()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if any return item is NOT an aggregation function.
   * Used to detect implicit GROUP BY (when RETURN has both aggregations and non-aggregations).
   * Note: This checks for items that don't contain any aggregation, including wrapped ones.
   */
  public boolean hasNonAggregations() {
    for (final ReturnItem item : items) {
      if (!item.getExpression().containsAggregation()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Represents a single item in the RETURN clause.
   * Can have an optional alias (AS alias).
   */
  public static class ReturnItem {
    private final Expression expression;
    private final String alias;

    public ReturnItem(final Expression expression, final String alias) {
      this.expression = expression;
      this.alias = alias;
    }

    public Expression getExpression() {
      return expression;
    }

    public String getAlias() {
      return alias;
    }

    public String getOutputName() {
      return alias != null ? alias : expression.getText();
    }
  }
}
