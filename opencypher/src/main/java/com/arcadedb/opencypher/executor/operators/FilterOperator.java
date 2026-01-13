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
package com.arcadedb.opencypher.executor.operators;

import com.arcadedb.opencypher.ast.Expression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Physical operator that filters input rows based on a predicate expression.
 * Rows that satisfy the predicate are passed through, others are discarded.
 *
 * This operator is used for:
 * - WHERE clause predicates that cannot be pushed down to scans
 * - Post-join filtering
 * - Complex predicates that require multiple variables
 *
 * Cost: O(N) where N is input cardinality
 * Cardinality: input_cardinality * selectivity
 */
public class FilterOperator extends AbstractPhysicalOperator {
  private final Expression predicate;

  public FilterOperator(final PhysicalOperator child, final Expression predicate,
                       final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.predicate = predicate;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
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

        // Filter input rows
        while (buffer.size() < n) {
          if (!inputResults.hasNext()) {
            finished = true;
            break;
          }

          final Result inputResult = inputResults.next();

          // Evaluate predicate on input row
          final Object predicateValue = predicate.evaluate(inputResult, context);

          // Pass through if predicate evaluates to true
          if (isTrue(predicateValue)) {
            buffer.add(inputResult);
          }
        }
      }

      /**
       * Checks if a value is considered true for filtering.
       * Follows Cypher semantics: true is true, everything else is false.
       */
      private boolean isTrue(final Object value) {
        if (value == null) {
          return false;
        }
        if (value instanceof Boolean) {
          return (Boolean) value;
        }
        // Non-boolean values are considered false
        return false;
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "Filter";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ Filter");
    sb.append(" [predicate=").append(predicate.getText());
    sb.append(", cost=").append(String.format("%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null) {
      sb.append(child.explain(depth + 1));
    }

    return sb.toString();
  }

  public Expression getPredicate() {
    return predicate;
  }
}
