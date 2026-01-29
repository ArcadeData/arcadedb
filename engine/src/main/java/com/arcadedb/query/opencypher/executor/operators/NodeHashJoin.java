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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Physical operator that performs a hash join between two input streams.
 * Used for Cartesian products with join conditions, or for disconnected graph patterns.
 *
 * Algorithm:
 * 1. Build Phase: Materialize left input into hash table (keyed by join variable)
 * 2. Probe Phase: For each right input row, look up matching left rows and produce output
 *
 * This is efficient when one side is significantly smaller than the other (build from smaller side).
 *
 * Cost: O(L + R) where L is left cardinality, R is right cardinality
 * Cardinality: Estimated based on join selectivity
 */
public class NodeHashJoin extends AbstractPhysicalOperator {
  private final PhysicalOperator leftChild;
  private final PhysicalOperator rightChild;
  private final String joinVariable; // Variable to join on (present in both inputs)

  public NodeHashJoin(final PhysicalOperator leftChild, final PhysicalOperator rightChild,
                     final String joinVariable, final double estimatedCost,
                     final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.joinVariable = joinVariable;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      private Map<Object, List<Result>> hashTable = null;
      private ResultSet rightResults = null;
      private Result currentRightResult = null;
      private List<Result> matchingLeftResults = null;
      private int matchingIndex = 0;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        fetchMore(nRecords > 0 ? nRecords : 100);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Build phase: materialize left input into hash table (only once)
        if (hashTable == null) {
          buildHashTable(context);
        }

        // Probe phase: join right input with hash table
        if (rightResults == null) {
          rightResults = rightChild.execute(context, -1);
        }

        while (buffer.size() < n) {
          // If we've exhausted matches for current right row, get next right row
          if (matchingLeftResults == null || matchingIndex >= matchingLeftResults.size()) {
            if (!rightResults.hasNext()) {
              finished = true;
              break;
            }

            currentRightResult = rightResults.next();
            final Object joinValue = currentRightResult.getProperty(joinVariable);

            // Look up matching left rows in hash table
            matchingLeftResults = hashTable.get(joinValue);
            matchingIndex = 0;

            if (matchingLeftResults == null || matchingLeftResults.isEmpty()) {
              continue; // No matches, try next right row
            }
          }

          // Produce join result
          final Result leftResult = matchingLeftResults.get(matchingIndex++);
          final ResultInternal joinedResult = new ResultInternal();

          // Add properties from left result
          for (final String prop : leftResult.getPropertyNames()) {
            joinedResult.setProperty(prop, leftResult.getProperty(prop));
          }

          // Add properties from right result (skip join variable if already present)
          for (final String prop : currentRightResult.getPropertyNames()) {
            if (!joinedResult.getPropertyNames().contains(prop)) {
              joinedResult.setProperty(prop, currentRightResult.getProperty(prop));
            }
          }

          buffer.add(joinedResult);
        }
      }

      private void buildHashTable(final CommandContext context) {
        hashTable = new HashMap<>();

        final ResultSet leftResults = leftChild.execute(context, -1);
        while (leftResults.hasNext()) {
          final Result leftResult = leftResults.next();
          final Object joinValue = leftResult.getProperty(joinVariable);

          // Add to hash table (allow null join values for Cartesian products)
          hashTable.computeIfAbsent(joinValue, k -> new ArrayList<>()).add(leftResult);
        }
        leftResults.close();
      }

      @Override
      public void close() {
        if (rightResults != null) {
          rightResults.close();
        }
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeHashJoin";
  }

  @Override
  public PhysicalOperator getChild() {
    // Hash join has two children, return left by convention
    return leftChild;
  }

  @Override
  public void setChild(final PhysicalOperator child) {
    throw new UnsupportedOperationException("NodeHashJoin has two children, use constructor");
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ NodeHashJoin");
    sb.append(" [on=").append(joinVariable);
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    sb.append(indent).append("  Left:\n");
    if (leftChild != null) {
      sb.append(leftChild.explain(depth + 2));
    }

    sb.append(indent).append("  Right:\n");
    if (rightChild != null) {
      sb.append(rightChild.explain(depth + 2));
    }

    return sb.toString();
  }

  public PhysicalOperator getLeftChild() {
    return leftChild;
  }

  public PhysicalOperator getRightChild() {
    return rightChild;
  }

  public String getJoinVariable() {
    return joinVariable;
  }
}
