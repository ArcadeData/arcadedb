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
package com.arcadedb.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for OPTIONAL MATCH clauses.
 * Wraps a chain of match steps and ensures that if no matches are found,
 * the input rows are returned with NULL values for the pattern variables.
 * <p>
 * Example: OPTIONAL MATCH (n:Person)-[r:KNOWS]->(m:Person)
 * - If matches found: returns input rows expanded with matched vertices/edges
 * - If no matches: returns input rows with n, r, m set to NULL
 */
public class OptionalMatchStep extends AbstractExecutionStep {
  private final AbstractExecutionStep matchStep;
  private final Set<String> variableNames;

  /**
   * Creates an optional match step.
   *
   * @param matchStep     the chain of match steps to execute
   * @param variableNames names of variables that should be set to NULL if no match
   * @param context       command context
   */
  public OptionalMatchStep(final AbstractExecutionStep matchStep, final Set<String> variableNames,
      final CommandContext context) {
    super(context);
    this.matchStep = matchStep;
    this.variableNames = variableNames;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private ResultSet matchResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Result currentInputResult = null;
      private boolean currentInputHadMatch = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        fetchMore(nRecords);
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

        if (hasInput) {
          // With input: for each input row, try to match, or return with NULLs
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          while (buffer.size() < n) {
            // Try to get matches for current input
            if (matchResults != null && matchResults.hasNext()) {
              buffer.add(matchResults.next());
              currentInputHadMatch = true;
            } else {
              // No more matches for current input - check if we need to emit NULL row
              if (currentInputResult != null && !currentInputHadMatch) {
                // Emit input row with NULL values for pattern variables
                final ResultInternal nullResult = new ResultInternal();
                for (final String prop : currentInputResult.getPropertyNames()) {
                  nullResult.setProperty(prop, currentInputResult.getProperty(prop));
                }
                for (final String varName : variableNames) {
                  nullResult.setProperty(varName, null);
                }
                buffer.add(nullResult);
              }

              // Move to next input
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }

              currentInputResult = prevResults.next();
              currentInputHadMatch = false;

              // Execute match step with this input
              matchResults = matchStep.syncPull(context, nRecords);
            }
          }
        } else {
          // No input: standalone OPTIONAL MATCH (rare, but valid)
          if (matchResults == null) {
            matchResults = matchStep.syncPull(context, nRecords);
          }

          // If there are matches, return them
          while (buffer.size() < n && matchResults.hasNext()) {
            buffer.add(matchResults.next());
          }

          // If no matches at all, return a single row with NULL values
          if (buffer.isEmpty() && !matchResults.hasNext()) {
            final ResultInternal nullResult = new ResultInternal();
            for (final String varName : variableNames) {
              nullResult.setProperty(varName, null);
            }
            buffer.add(nullResult);
            finished = true;
          } else if (!matchResults.hasNext()) {
            finished = true;
          }
        }
      }

      @Override
      public void close() {
        if (matchResults != null) {
          matchResults.close();
        }
        OptionalMatchStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ OPTIONAL MATCH (variables: ").append(String.join(", ", variableNames)).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    builder.append(matchStep.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
