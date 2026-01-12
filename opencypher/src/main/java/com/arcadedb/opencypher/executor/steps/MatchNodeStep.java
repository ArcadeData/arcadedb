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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.opencypher.ast.NodePattern;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for matching node patterns.
 * Fetches vertices from the database by type (label).
 * <p>
 * Example: MATCH (n:Person)
 * - Iterates all vertices of type "Person"
 * - Binds each vertex to variable 'n'
 */
public class MatchNodeStep extends AbstractExecutionStep {
  private final String variable;
  private final NodePattern pattern;

  /**
   * Creates a match node step.
   *
   * @param variable variable name to bind vertices to
   * @param pattern  node pattern to match
   * @param context  command context
   */
  public MatchNodeStep(final String variable, final NodePattern pattern, final CommandContext context) {
    super(context);
    this.variable = variable;
    this.pattern = pattern;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Iterator<Identifiable> iterator = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private Result currentInputResult = null;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
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
          // Chained mode: for each input result, add matched nodes
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          // Process input results and add our matched nodes
          while (buffer.size() < n) {
            // If we've exhausted nodes for current input, get next input
            if (iterator == null || !iterator.hasNext()) {
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }
              currentInputResult = prevResults.next();
              iterator = getVertexIterator();
            }

            // Match nodes and add to input result
            if (iterator.hasNext()) {
              final Identifiable identifiable = iterator.next();
              if (identifiable instanceof Vertex) {
                final Vertex vertex = (Vertex) identifiable;

                // Apply property filters if specified in pattern
                if (!matchesProperties(vertex)) {
                  continue;
                }

                // Copy input result and add our vertex
                final ResultInternal result = new ResultInternal();
                if (currentInputResult != null) {
                  for (final String prop : currentInputResult.getPropertyNames()) {
                    result.setProperty(prop, currentInputResult.getProperty(prop));
                  }
                }
                result.setProperty(variable, vertex);
                buffer.add(result);
              }
            }
          }
        } else {
          // Standalone mode: no input, create fresh results
          // Initialize iterator on first call
          if (iterator == null) {
            iterator = getVertexIterator();
          }

          // Fetch up to n vertices
          while (buffer.size() < n && iterator.hasNext()) {
            final Identifiable identifiable = iterator.next();

            if (identifiable instanceof Vertex) {
              final Vertex vertex = (Vertex) identifiable;

              // Apply property filters if specified in pattern
              if (!matchesProperties(vertex)) {
                continue;
              }

              // Create result with vertex bound to variable
              final ResultInternal result = new ResultInternal();
              result.setProperty(variable, vertex);
              buffer.add(result);
            }
          }

          if (!iterator.hasNext()) {
            finished = true;
          }
        }
      }

      @Override
      public void close() {
        MatchNodeStep.this.close();
      }
    };
  }

  /**
   * Gets an iterator for vertices matching the pattern.
   */
  private Iterator<Identifiable> getVertexIterator() {
    if (pattern.hasLabels()) {
      // Iterate vertices of specific type(s)
      final String label = pattern.getFirstLabel();
      @SuppressWarnings("unchecked")
      final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);
      return iter;
    } else {
      // No label specified - iterate ALL vertex types
      // Get all vertex types from schema and chain their iterators
      final java.util.List<Iterator<Identifiable>> iterators = new java.util.ArrayList<>();

      for (final com.arcadedb.schema.DocumentType type : context.getDatabase().getSchema().getTypes()) {
        // Only include vertex types (not edge types or document types)
        if (type instanceof com.arcadedb.schema.VertexType) {
          @SuppressWarnings("unchecked")
          final Iterator<Identifiable> iter = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(type.getName(), false);
          iterators.add(iter);
        }
      }

      // Chain all iterators together
      return new ChainedIterator(iterators);
    }
  }

  /**
   * Iterator that chains multiple iterators together.
   */
  private static class ChainedIterator implements Iterator<Identifiable> {
    private final java.util.List<Iterator<Identifiable>> iterators;
    private int currentIndex = 0;

    public ChainedIterator(final java.util.List<Iterator<Identifiable>> iterators) {
      this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
      while (currentIndex < iterators.size()) {
        if (iterators.get(currentIndex).hasNext()) {
          return true;
        }
        currentIndex++;
      }
      return false;
    }

    @Override
    public Identifiable next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      return iterators.get(currentIndex).next();
    }
  }

  /**
   * Checks if a vertex matches the property filters in the pattern.
   *
   * @param vertex vertex to check
   * @return true if matches or no properties specified
   */
  private boolean matchesProperties(final Vertex vertex) {
    if (!pattern.hasProperties()) {
      return true; // No property filters
    }

    // Check each property filter
    for (final java.util.Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final String key = entry.getKey();
      Object expectedValue = entry.getValue();

      // Handle string literals: remove quotes
      if (expectedValue instanceof String) {
        final String strValue = (String) expectedValue;
        if (strValue.startsWith("'") && strValue.endsWith("'")) {
          expectedValue = strValue.substring(1, strValue.length() - 1);
        } else if (strValue.startsWith("\"") && strValue.endsWith("\"")) {
          expectedValue = strValue.substring(1, strValue.length() - 1);
        }
      }

      final Object actualValue = vertex.get(key);

      // Compare values
      if (actualValue == null || !actualValue.equals(expectedValue)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MATCH NODE ");
    builder.append("(").append(variable);
    if (pattern.hasLabels()) {
      builder.append(":").append(String.join("|", pattern.getLabels()));
    }
    builder.append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
