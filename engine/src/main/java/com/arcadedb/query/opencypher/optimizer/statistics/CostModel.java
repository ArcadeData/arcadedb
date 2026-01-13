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
package com.arcadedb.query.opencypher.optimizer.statistics;

/**
 * Cost model for query optimization.
 * Provides cost estimation and selectivity estimation based on heuristics.
 *
 * Cost constants are tunable for different workloads.
 */
public class CostModel {
  // Cost constants (tunable)
  public static final double SCAN_COST_PER_ROW = 1.0;
  public static final double INDEX_SEEK_COST = 5.0;  // Fixed overhead for index lookup
  public static final double INDEX_LOOKUP_COST_PER_ROW = 0.1;
  public static final double EXPAND_COST_PER_ROW = 2.0;
  public static final double FILTER_COST_PER_ROW = 0.5;
  public static final double HASH_BUILD_COST_PER_ROW = 1.0;
  public static final double HASH_PROBE_COST_PER_ROW = 0.5;

  // Selectivity heuristics (0.0 = no rows, 1.0 = all rows)
  public static final double SELECTIVITY_EQUALITY = 0.1;      // 10% for equality (=)
  public static final double SELECTIVITY_RANGE = 0.3;         // 30% for range (<, >, <=, >=)
  public static final double SELECTIVITY_DEFAULT = 0.5;       // 50% default
  public static final double SELECTIVITY_UNIQUE_INDEX = 0.001; // 0.1% for unique index

  // Selectivity threshold for index usage decision
  public static final double INDEX_SELECTIVITY_THRESHOLD = 0.1; // 10%

  // Default average degree for graph traversals (when statistics unavailable)
  public static final double DEFAULT_AVG_DEGREE = 10.0;

  private final StatisticsProvider statistics;

  public CostModel(final StatisticsProvider statistics) {
    this.statistics = statistics;
  }

  /**
   * Estimates the cost of a full table scan.
   *
   * @param typeName the type to scan
   * @return estimated cost
   */
  public double estimateScanCost(final String typeName) {
    final long cardinality = statistics.getCardinality(typeName);
    return cardinality * SCAN_COST_PER_ROW;
  }

  /**
   * Estimates the cost of an index seek operation.
   *
   * @param typeName the type name
   * @param propertyName the property name
   * @param selectivity estimated selectivity of the predicate
   * @return estimated cost
   */
  public double estimateIndexSeekCost(final String typeName, final String propertyName,
                                      final double selectivity) {
    final IndexStatistics index = statistics.findIndexForProperty(typeName, propertyName);
    if (index == null) {
      return Double.MAX_VALUE; // No index available
    }

    final long typeCardinality = statistics.getCardinality(typeName);
    final long estimatedRows = (long) (typeCardinality * selectivity);

    return INDEX_SEEK_COST + (estimatedRows * INDEX_LOOKUP_COST_PER_ROW);
  }

  /**
   * Estimates the cost of filtering rows.
   *
   * @param inputCardinality number of input rows
   * @return estimated cost
   */
  public double estimateFilterCost(final long inputCardinality) {
    return inputCardinality * FILTER_COST_PER_ROW;
  }

  /**
   * Estimates the cost of expanding relationships (traversal).
   *
   * @param inputCardinality number of input vertices
   * @param avgDegree average number of edges per vertex
   * @return estimated cost
   */
  public double estimateExpandCost(final long inputCardinality, final double avgDegree) {
    return inputCardinality * avgDegree * EXPAND_COST_PER_ROW;
  }

  /**
   * Estimates the cost of ExpandInto (semi-join).
   * Much cheaper than ExpandAll since it just checks existence.
   *
   * @param inputCardinality number of input vertex pairs
   * @return estimated cost
   */
  public double estimateExpandIntoCost(final long inputCardinality) {
    // ExpandInto is O(m) RID scan, much cheaper than loading edges
    return inputCardinality * 1.0;
  }

  /**
   * Estimates the cost of a hash join.
   *
   * @param leftCardinality cardinality of left input
   * @param rightCardinality cardinality of right input
   * @return estimated cost
   */
  public double estimateHashJoinCost(final long leftCardinality, final long rightCardinality) {
    // Build hash table from smaller side
    final long buildCardinality = Math.min(leftCardinality, rightCardinality);
    final long probeCardinality = Math.max(leftCardinality, rightCardinality);

    return (buildCardinality * HASH_BUILD_COST_PER_ROW) +
           (probeCardinality * HASH_PROBE_COST_PER_ROW);
  }

  /**
   * Estimates the cardinality (number of output rows) after applying a filter.
   *
   * @param inputCardinality number of input rows
   * @param selectivity selectivity of the filter (0.0-1.0)
   * @return estimated output cardinality
   */
  public long estimateFilterCardinality(final long inputCardinality, final double selectivity) {
    return Math.max(1, (long) (inputCardinality * selectivity));
  }

  /**
   * Estimates selectivity of an equality predicate.
   *
   * @param typeName the type name
   * @param propertyName the property name
   * @param hasIndex true if an index exists on the property
   * @return estimated selectivity (0.0-1.0)
   */
  public double estimateEqualitySelectivity(final String typeName, final String propertyName,
                                            final boolean hasIndex) {
    if (hasIndex) {
      final IndexStatistics index = statistics.findIndexForProperty(typeName, propertyName);
      if (index != null && index.isUnique()) {
        // Unique index: 1/N selectivity
        final long cardinality = statistics.getCardinality(typeName);
        return cardinality > 0 ? (1.0 / cardinality) : SELECTIVITY_UNIQUE_INDEX;
      }
    }
    return SELECTIVITY_EQUALITY;
  }

  /**
   * Estimates selectivity of a range predicate (<, >, <=, >=).
   *
   * @return estimated selectivity (0.0-1.0)
   */
  public double estimateRangeSelectivity() {
    return SELECTIVITY_RANGE;
  }

  /**
   * Estimates selectivity of a generic predicate.
   *
   * @return estimated selectivity (0.0-1.0)
   */
  public double estimateDefaultSelectivity() {
    return SELECTIVITY_DEFAULT;
  }

  /**
   * Combines selectivities for AND predicates.
   * Assumes independence: P(A AND B) = P(A) * P(B)
   *
   * @param selectivities array of selectivities
   * @return combined selectivity
   */
  public double combineSelectivitiesAnd(final double... selectivities) {
    double result = 1.0;
    for (final double selectivity : selectivities) {
      result *= selectivity;
    }
    return result;
  }

  /**
   * Combines selectivities for OR predicates.
   * Uses union formula: P(A OR B) = P(A) + P(B) - P(A AND B)
   * Assumes independence: P(A AND B) = P(A) * P(B)
   *
   * @param selectivities array of selectivities
   * @return combined selectivity
   */
  public double combineSelectivitiesOr(final double... selectivities) {
    if (selectivities.length == 0) {
      return 1.0;
    }
    if (selectivities.length == 1) {
      return selectivities[0];
    }

    // For multiple OR predicates, use: 1 - product(1 - P(i))
    double result = 1.0;
    for (final double selectivity : selectivities) {
      result *= (1.0 - selectivity);
    }
    return 1.0 - result;
  }

  /**
   * Decides whether to use an index for a query based on cost comparison.
   *
   * @param typeName the type name
   * @param propertyName the property name
   * @param selectivity estimated selectivity
   * @return true if index should be used
   */
  public boolean shouldUseIndex(final String typeName, final String propertyName,
                                final double selectivity) {
    // Rule: Use index if selectivity < threshold AND cost is lower
    if (selectivity >= INDEX_SELECTIVITY_THRESHOLD) {
      return false; // Not selective enough
    }

    final double scanCost = estimateScanCost(typeName);
    final double indexCost = estimateIndexSeekCost(typeName, propertyName, selectivity);

    return indexCost < scanCost;
  }

  /**
   * Estimates the average degree (number of edges per vertex) for a relationship type.
   * Returns default heuristic when statistics unavailable.
   *
   * @param relationshipTypeName the relationship type name
   * @return estimated average degree
   */
  public double estimateAverageDegree(final String relationshipTypeName) {
    // TODO: Collect actual statistics for relationship types
    // For now, use default heuristic
    return DEFAULT_AVG_DEGREE;
  }

  @Override
  public String toString() {
    return "CostModel{statistics=" + statistics + '}';
  }
}
