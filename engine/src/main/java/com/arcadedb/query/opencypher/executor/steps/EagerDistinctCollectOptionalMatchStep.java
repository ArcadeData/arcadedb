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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Optimized execution step for OPTIONAL MATCH when variables are only used in collect(DISTINCT ...).
 * <p>
 * This step avoids creating Cartesian products by directly collecting distinct values using HashSets.
 * Instead of creating N rows for N matches and then applying DISTINCT at the end, this step:
 * 1. For each input row, executes the OPTIONAL MATCH pattern
 * 2. Collects matched values into HashSets (one per variable) to maintain uniqueness
 * 3. Returns a single row per input row with the collected distinct values as lists
 * <p>
 * Example query that benefits:
 * <pre>
 * MATCH (chunk:CHUNK) WHERE ID(chunk) = "x"
 * OPTIONAL MATCH (chunk)<-[:in]-(ner:NER)
 * OPTIONAL MATCH (chunk)<-[:topic]-(theme:THEME)
 * RETURN collect(DISTINCT ner) as ners, collect(DISTINCT theme) as themes
 * </pre>
 * <p>
 * Without this optimization:
 * - 1000 NER × 10 THEME = 10,000 intermediate rows → then DISTINCT → 1010 unique values
 * <p>
 * With this optimization:
 * - Collect 1000 distinct NER into HashSet
 * - Collect 10 distinct THEME into HashSet
 * - Return 1 row with 2 collections
 * - Memory: ~100KB instead of ~6MB
 */
public class EagerDistinctCollectOptionalMatchStep extends AbstractExecutionStep {
  private final AbstractExecutionStep matchChainStart;
  private final Set<String> collectDistinctVariables;
  private final Set<String> allVariables;

  /**
   * Creates an eager distinct collect optional match step.
   *
   * @param matchChainStart          first step in the optional match chain
   * @param collectDistinctVariables variables that will be used in collect(DISTINCT ...)
   * @param allVariables             all variables introduced by this OPTIONAL MATCH
   * @param context                  command context
   */
  public EagerDistinctCollectOptionalMatchStep(final AbstractExecutionStep matchChainStart,
      final Set<String> collectDistinctVariables, final Set<String> allVariables, final CommandContext context) {
    super(context);
    this.matchChainStart = matchChainStart;
    this.collectDistinctVariables = collectDistinctVariables;
    this.allVariables = allVariables;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;
    final List<Result> results = new ArrayList<>();

    if (hasInput) {
      // Pull all input rows
      final ResultSet prevResults = prev.syncPull(context, Integer.MAX_VALUE);

      while (prevResults.hasNext()) {
        final Result inputRow = prevResults.next();

        // Create distinct collections for this input row
        final Map<String, Set<Object>> distinctSets = new HashMap<>();
        for (final String varName : collectDistinctVariables) {
          distinctSets.put(varName, new LinkedHashSet<>());
        }

        // Feed this single input row into the match chain
        final SingleRowInputStep singleRowInput = new SingleRowInputStep(inputRow, context);
        matchChainStart.setPrevious(singleRowInput);

        // Execute the match chain and collect distinct values
        final ResultSet matchResults = matchChainStart.syncPull(context, Integer.MAX_VALUE);
        while (matchResults.hasNext()) {
          final Result matchResult = matchResults.next();

          // Add each variable's value to its distinct set
          for (final String varName : collectDistinctVariables) {
            final Object value = matchResult.getProperty(varName);
            if (value != null) {
              if (value instanceof Identifiable) {
                // Use RID for identity to handle same record loaded multiple times
                distinctSets.get(varName).add(new IdentifiableWrapper((Identifiable) value));
              } else {
                distinctSets.get(varName).add(value);
              }
            }
          }
        }
        matchResults.close();

        // Build result row with collected distinct values
        final ResultInternal resultRow = new ResultInternal();

        // Copy input properties (not OPTIONAL MATCH variables)
        for (final String prop : inputRow.getPropertyNames()) {
          if (!allVariables.contains(prop)) {
            resultRow.setProperty(prop, inputRow.getProperty(prop));
          }
        }

        // Add collected distinct sets as lists
        for (final String varName : collectDistinctVariables) {
          final Set<Object> distinctSet = distinctSets.get(varName);
          final List<Object> list = new ArrayList<>(distinctSet.size());
          for (final Object value : distinctSet) {
            if (value instanceof IdentifiableWrapper) {
              list.add(((IdentifiableWrapper) value).getIdentifiable());
            } else {
              list.add(value);
            }
          }
          // Store with prefix to indicate it's a pre-collected list
          resultRow.setProperty("__collected_distinct__" + varName, list);
        }

        // For non-collect-distinct variables, set to null (they shouldn't be accessed directly)
        for (final String varName : allVariables) {
          if (!collectDistinctVariables.contains(varName)) {
            resultRow.setProperty(varName, null);
          }
        }

        results.add(resultRow);
      }
    } else {
      // No input: standalone OPTIONAL MATCH
      // Create distinct collections
      final Map<String, Set<Object>> distinctSets = new HashMap<>();
      for (final String varName : collectDistinctVariables) {
        distinctSets.put(varName, new LinkedHashSet<>());
      }

      // Execute match chain without input
      matchChainStart.setPrevious(null);
      final ResultSet matchResults = matchChainStart.syncPull(context, Integer.MAX_VALUE);

      while (matchResults.hasNext()) {
        final Result matchResult = matchResults.next();
        for (final String varName : collectDistinctVariables) {
          final Object value = matchResult.getProperty(varName);
          if (value != null) {
            if (value instanceof Identifiable) {
              distinctSets.get(varName).add(new IdentifiableWrapper((Identifiable) value));
            } else {
              distinctSets.get(varName).add(value);
            }
          }
        }
      }
      matchResults.close();

      // Build single result row
      final ResultInternal resultRow = new ResultInternal();
      for (final String varName : collectDistinctVariables) {
        final Set<Object> distinctSet = distinctSets.get(varName);
        final List<Object> list = new ArrayList<>(distinctSet.size());
        for (final Object value : distinctSet) {
          if (value instanceof IdentifiableWrapper) {
            list.add(((IdentifiableWrapper) value).getIdentifiable());
          } else {
            list.add(value);
          }
        }
        resultRow.setProperty("__collected_distinct__" + varName, list);
      }
      results.add(resultRow);
    }

    return new IteratorResultSet(results.iterator());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ EAGER DISTINCT COLLECT OPTIONAL MATCH (variables: ")
        .append(String.join(", ", collectDistinctVariables)).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    builder.append(matchChainStart.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  /**
   * Helper step that provides a single row as input to the match chain.
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

  /**
   * Wrapper for Identifiable objects that uses RID for equals/hashCode.
   */
  private static class IdentifiableWrapper {
    private final Identifiable identifiable;

    IdentifiableWrapper(final Identifiable identifiable) {
      this.identifiable = identifiable;
    }

    Identifiable getIdentifiable() {
      return identifiable;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (obj == null || getClass() != obj.getClass())
        return false;
      final IdentifiableWrapper other = (IdentifiableWrapper) obj;
      return identifiable.getIdentity().equals(other.identifiable.getIdentity());
    }

    @Override
    public int hashCode() {
      return identifiable.getIdentity().hashCode();
    }
  }
}
