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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for OPTIONAL MATCH clauses.
 * Implements LEFT OUTER JOIN semantics: for each input row, try to match the pattern.
 * If matches found, return them. If no matches, return input row with NULL values.
 * <p>
 * Example: MATCH (a) OPTIONAL MATCH (a)-[r:KNOWS]->(b)
 * - For each 'a' from first MATCH, try to find (a)-[r:KNOWS]->(b)
 * - If found: return a, r, b
 * - If not found: return a, null, null
 */
public class OptionalMatchStep extends AbstractExecutionStep {
  private final AbstractExecutionStep matchChainStart;
  private final AbstractExecutionStep matchChainEnd;
  private final Set<String> variableNames;

  /**
   * Creates an optional match step.
   *
   * @param matchChainStart first step in the optional match chain (where input is injected)
   * @param matchChainEnd   last step in the optional match chain (where execution starts)
   * @param variableNames   names of variables that should be set to NULL if no match
   * @param context         command context
   */
  public OptionalMatchStep(final AbstractExecutionStep matchChainStart, final AbstractExecutionStep matchChainEnd,
      final Set<String> variableNames, final CommandContext context) {
    super(context);
    this.matchChainStart = matchChainStart;
    this.matchChainEnd = matchChainEnd;
    this.variableNames = variableNames;
  }

  /**
   * Legacy constructor for backward compatibility.
   * Uses matchChainStart as both start and end.
   */
  public OptionalMatchStep(final AbstractExecutionStep matchChainStart, final Set<String> variableNames,
      final CommandContext context) {
    this(matchChainStart, matchChainStart, variableNames, context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
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
          // With input: process each input row through the optional match chain
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          while (buffer.size() < n && prevResults.hasNext()) {
            final Result inputRow = prevResults.next();

            // Feed this single input row into the match chain
            // Create a single-row input provider for the match chain
            final SingleRowInputStep singleRowInput = new SingleRowInputStep(inputRow, context);
            matchChainStart.setPrevious(singleRowInput);

            // Execute the match chain with this input
            // Call syncPull on the END of the chain to execute the full chain
            final ResultSet matchResults = matchChainEnd.syncPull(context, 100);

            // Collect all matches for this input
            boolean foundMatch = false;
            while (matchResults.hasNext()) {
              buffer.add(matchResults.next());
              foundMatch = true;
            }
            matchResults.close();

            // If no matches found, emit input row with NULL values for match variables
            if (!foundMatch) {
              final ResultInternal nullResult = new ResultInternal();
              // Copy all properties from input row
              for (final String prop : inputRow.getPropertyNames()) {
                nullResult.setProperty(prop, inputRow.getProperty(prop));
              }
              // Add NULL values for optional match variables
              for (final String varName : variableNames) {
                nullResult.setProperty(varName, null);
              }
              buffer.add(nullResult);
            }
          }

          if (!prevResults.hasNext()) {
            finished = true;
          }
        } else {
          // No input: standalone OPTIONAL MATCH
          // Execute match chain without input
          matchChainStart.setPrevious(null);
          // Call syncPull on the END of the chain to execute the full chain
          final ResultSet matchResults = matchChainEnd.syncPull(context, nRecords);

          // Collect matches
          boolean foundAnyMatch = false;
          while (buffer.size() < n && matchResults.hasNext()) {
            buffer.add(matchResults.next());
            foundAnyMatch = true;
          }

          // If no matches at all, return a single row with NULL values
          if (!foundAnyMatch && buffer.isEmpty()) {
            final ResultInternal nullResult = new ResultInternal();
            for (final String varName : variableNames) {
              nullResult.setProperty(varName, null);
            }
            buffer.add(nullResult);
          }

          if (!matchResults.hasNext()) {
            finished = true;
          }
          matchResults.close();
        }
      }

      @Override
      public void close() {
        OptionalMatchStep.this.close();
      }
    };
  }

  /**
   * Helper step that provides a single row as input to the match chain.
   * This allows us to feed one input row at a time into the optional match chain.
   */
  private static class SingleRowInputStep extends AbstractExecutionStep {
    private final Result singleRow;
    private boolean consumed = false;

    public SingleRowInputStep(final Result row, final CommandContext context) {
      super(context);
      this.singleRow = row;
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
      if (consumed) {
        return new IteratorResultSet(List.<Result>of().iterator());
      }
      consumed = true;
      return new IteratorResultSet(List.of(singleRow).iterator());
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "  ".repeat(Math.max(0, depth * indent)) + "+ SINGLE ROW INPUT";
    }
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
    // Print the full chain starting from the end (which will recursively print previous steps)
    builder.append(matchChainEnd.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
