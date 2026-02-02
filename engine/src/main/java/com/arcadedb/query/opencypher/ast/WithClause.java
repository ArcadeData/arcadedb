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

import java.util.List;

/**
 * Represents a WITH clause in a Cypher query.
 * WITH allows query chaining by projecting and transforming results before passing them to the next part of the query.
 * <p>
 * Examples:
 * - MATCH (a:Person) WITH a.name AS name, a.age AS age WHERE age > 30 RETURN name
 * - MATCH (a:Person) WITH a, count(*) AS cnt WHERE cnt > 5 RETURN a
 * - MATCH (a:Person) WITH a ORDER BY a.name LIMIT 10 MATCH (a)-[:KNOWS]->(b) RETURN a, b
 * <p>
 * WITH supports:
 * - Projection (selecting and aliasing columns)
 * - DISTINCT (removing duplicates)
 * - WHERE clause (filtering after projection)
 * - ORDER BY (ordering results)
 * - SKIP/LIMIT (pagination)
 * - Aggregation (grouping and computing aggregates)
 */
public class WithClause {
  private final List<ReturnClause.ReturnItem> items;
  private final boolean distinct;
  private final WhereClause whereClause;
  private final OrderByClause orderByClause;
  private final Integer skip;
  private final Integer limit;

  public WithClause(final List<ReturnClause.ReturnItem> items,
                    final boolean distinct,
                    final WhereClause whereClause,
                    final OrderByClause orderByClause,
                    final Integer skip,
                    final Integer limit) {
    this.items = items;
    this.distinct = distinct;
    this.whereClause = whereClause;
    this.orderByClause = orderByClause;
    this.skip = skip;
    this.limit = limit;
  }

  public List<ReturnClause.ReturnItem> getItems() {
    return items;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public WhereClause getWhereClause() {
    return whereClause;
  }

  public OrderByClause getOrderByClause() {
    return orderByClause;
  }

  public Integer getSkip() {
    return skip;
  }

  public Integer getLimit() {
    return limit;
  }

  /**
   * Check if any WITH item is an aggregation function.
   * This includes wrapped aggregations like head(collect(...)).
   */
  public boolean hasAggregations() {
    for (final ReturnClause.ReturnItem item : items) {
      if (item.getExpression().containsAggregation()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if any WITH item is NOT an aggregation function.
   * Used to detect implicit GROUP BY (when WITH has both aggregations and non-aggregations).
   * An expression is considered non-aggregation if it doesn't contain any aggregation.
   */
  public boolean hasNonAggregations() {
    for (final ReturnClause.ReturnItem item : items) {
      if (!item.getExpression().containsAggregation()) {
        return true;
      }
    }
    return false;
  }
}
