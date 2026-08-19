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
package com.arcadedb.query.opencypher.optimizer.plan;

import com.arcadedb.query.opencypher.optimizer.RangePredicate;
import com.arcadedb.query.opencypher.optimizer.statistics.IndexStatistics;

import java.util.List;

/**
 * Result of anchor selection algorithm.
 * Identifies the best starting node for query execution.
 *
 * Supports three access patterns:
 * 1. Full scan (no index)
 * 2. Index seek (equality: property = value)
 * 3. Index range scan (range: property > value1 AND property < value2)
 */
public class AnchorSelection {
  private final String variable;
  private final LogicalNode node;
  private final boolean useIndex;
  private final boolean isRangeScan;
  private final IndexStatistics index;
  private final String propertyName;
  private final Object propertyValue;  // For equality index seek
  private final List<Object> keyValues;  // Equality values covering a leading prefix of the index key
  private final List<RangePredicate> rangePredicates;  // For range scan
  private final List<DisjunctionIndexSeek> disjunctionIndexSeeks;  // For a label-disjunction equality index seek
  private final double estimatedCost;
  private final long estimatedCardinality;

  /**
   * One root type's contribution to a label-disjunction index seek (issue #6397): the type to seek, the index on
   * it that resolves the shared equality predicate, and the equality values covering a leading prefix of that
   * index's own key (same meaning as {@link #getKeyValues()}, computed per root since a composite index's extra
   * columns - and therefore how much of its key a prefix seek can pin down - can differ from root to root).
   * {@code propertyName}/{@code propertyValue} on the enclosing {@link AnchorSelection} are shared across every
   * root - a disjunction anchor has exactly one predicate driving the seek, evaluated once against each root's own
   * index.
   */
  public record DisjunctionIndexSeek(String typeName, IndexStatistics index, List<Object> keyValues) {
  }

  // Constructor for full scan (no index)
  public AnchorSelection(final String variable, final LogicalNode node,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, false, null, null, null, null, null, null, estimatedCost, estimatedCardinality);
  }

  // Constructor for equality index seek
  public AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                        final IndexStatistics index, final String propertyName,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, useIndex, index, propertyName, null, null, null, null, estimatedCost, estimatedCardinality);
  }

  // Constructor for equality index seek with value
  public AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                        final IndexStatistics index, final String propertyName, final Object propertyValue,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, useIndex, index, propertyName, propertyValue, null, null, null, estimatedCost, estimatedCardinality);
  }

  // Constructor for equality index seek covering a prefix of a composite key (issue #5444)
  public AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                        final IndexStatistics index, final String propertyName, final Object propertyValue,
                        final List<Object> keyValues,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, useIndex, index, propertyName, propertyValue, keyValues, null, null, estimatedCost, estimatedCardinality);
  }

  // Constructor for range index scan
  public AnchorSelection(final String variable, final LogicalNode node,
                        final IndexStatistics index, final String propertyName,
                        final List<RangePredicate> rangePredicates,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, true, index, propertyName, null, null, rangePredicates, null, estimatedCost, estimatedCardinality);
  }

  // Constructor for a label-disjunction equality index seek: one seek per root type (issue #6397)
  public AnchorSelection(final String variable, final LogicalNode node, final String propertyName,
                        final Object propertyValue, final List<DisjunctionIndexSeek> disjunctionIndexSeeks,
                        final double estimatedCost, final long estimatedCardinality) {
    this(variable, node, true, null, propertyName, propertyValue, null, null, disjunctionIndexSeeks, estimatedCost,
        estimatedCardinality);
  }

  // Main constructor
  private AnchorSelection(final String variable, final LogicalNode node, final boolean useIndex,
                         final IndexStatistics index, final String propertyName, final Object propertyValue,
                         final List<Object> keyValues, final List<RangePredicate> rangePredicates,
                         final List<DisjunctionIndexSeek> disjunctionIndexSeeks,
                         final double estimatedCost, final long estimatedCardinality) {
    this.variable = variable;
    this.node = node;
    this.useIndex = useIndex;
    this.isRangeScan = rangePredicates != null && !rangePredicates.isEmpty();
    this.index = index;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
    this.keyValues = keyValues == null || keyValues.isEmpty() ? List.of() : List.copyOf(keyValues);
    this.rangePredicates = rangePredicates;
    this.disjunctionIndexSeeks = disjunctionIndexSeeks == null ? List.of() : List.copyOf(disjunctionIndexSeeks);
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  /**
   * Returns the variable name of the anchor node.
   */
  public String getVariable() {
    return variable;
  }

  /**
   * Returns the logical node selected as anchor.
   */
  public LogicalNode getNode() {
    return node;
  }

  /**
   * Returns true if an index should be used for this anchor.
   */
  public boolean useIndex() {
    return useIndex;
  }

  /**
   * Returns the index to use, if useIndex() is true.
   */
  public IndexStatistics getIndex() {
    return index;
  }

  /**
   * Returns the property name used for index seek.
   */
  public String getPropertyName() {
    return propertyName;
  }

  /**
   * Returns the property value for index seek (from WHERE clause or inline properties).
   */
  public Object getPropertyValue() {
    return propertyValue;
  }

  /**
   * Returns the equality values that cover a leading prefix of the index key, in key order. The first
   * element is always {@link #getPropertyValue()}. Empty when the anchor is not an equality index seek.
   * <p>
   * A list as long as the index key lets the seek resolve a single entry; a shorter one makes it scan
   * the matching prefix range, with the remaining predicates applied by the Filter above (issue #5444).
   */
  public List<Object> getKeyValues() {
    return keyValues.isEmpty() && propertyValue != null ? List.of(propertyValue) : keyValues;
  }

  /**
   * Returns true if this is a range scan (uses range predicates).
   */
  public boolean isRangeScan() {
    return isRangeScan;
  }

  /**
   * Returns the range predicates for range scan.
   */
  public List<RangePredicate> getRangePredicates() {
    return rangePredicates;
  }

  /**
   * Returns true when this anchor is a label-disjunction equality index seek (issue #6397): every root type the
   * disjunction resolves to has its own usable index on {@link #getPropertyName()}, so
   * {@link com.arcadedb.query.opencypher.optimizer.rules.IndexSelectionRule} builds one seek per root instead of
   * the full {@code NodeByLabelDisjunctionScan}.
   */
  public boolean isDisjunctionIndexSeek() {
    return !disjunctionIndexSeeks.isEmpty();
  }

  /**
   * Returns the per-root-type seeks for a label-disjunction index seek. Empty unless
   * {@link #isDisjunctionIndexSeek()} is true.
   */
  public List<DisjunctionIndexSeek> getDisjunctionIndexSeeks() {
    return disjunctionIndexSeeks;
  }

  /**
   * Returns the estimated cost of accessing this anchor.
   */
  public double getEstimatedCost() {
    return estimatedCost;
  }

  /**
   * Returns the estimated cardinality (rows) from this anchor.
   */
  public long getEstimatedCardinality() {
    return estimatedCardinality;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AnchorSelection{");
    sb.append("variable='").append(variable).append('\'');
    sb.append(", useIndex=").append(useIndex);
    if (useIndex) {
      // A disjunction index seek (issue #6397) has no single index - it has one per root - so it is the one
      // useIndex()==true shape with a null #index. PhysicalPlan.explain()/toString() append this object directly
      // (via appendStepChain -> AbstractExecutionStep.prettyPrint on the physical-operator wrapper step, reached
      // through EXPLAIN's per-branch UNION description), so index.getIndexName() below would NPE for this shape
      // if it were not branched around.
      if (isDisjunctionIndexSeek()) {
        sb.append(", disjunctionSeeks=").append(disjunctionIndexSeeks);
        sb.append(", property=").append(propertyName);
        sb.append(", value=").append(propertyValue);
      } else {
        sb.append(", index=").append(index.getIndexName());
        sb.append(", property=").append(propertyName);
        if (isRangeScan) {
          sb.append(", rangeScan=").append(rangePredicates);
        } else {
          sb.append(", value=").append(propertyValue);
        }
      }
    }
    sb.append(", cost=").append(String.format("%.2f", estimatedCost));
    sb.append(", cardinality=").append(estimatedCardinality);
    sb.append('}');
    return sb.toString();
  }
}
