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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
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
 * Execution step for variable-length path patterns.
 * Handles patterns like (a)-[*1..3]->(b).
 * <p>
 * Uses specialized traversers (BFS/DFS) to efficiently find paths
 * within the specified hop range.
 */
public class ExpandPathStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String pathVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;
  private final boolean useBFS;

  /**
   * Creates an expand path step.
   *
   * @param sourceVariable variable name for source vertex
   * @param pathVariable   variable name for the path (can be null)
   * @param targetVariable variable name for target vertex
   * @param pattern        relationship pattern with variable-length specification
   * @param useBFS         true for BFS (shortest paths), false for DFS (all paths)
   * @param context        command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String targetVariable,
      final RelationshipPattern pattern, final boolean useBFS, final CommandContext context) {
    super(context);

    if (!pattern.isVariableLength()) {
      throw new IllegalArgumentException("ExpandPathStep requires a variable-length relationship pattern");
    }

    this.sourceVariable = sourceVariable;
    this.pathVariable = pathVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
    this.useBFS = useBFS;
  }

  /**
   * Creates an expand path step with BFS (default).
   *
   * @param sourceVariable variable name for source vertex
   * @param pathVariable   variable name for the path (can be null)
   * @param targetVariable variable name for target vertex
   * @param pattern        relationship pattern with variable-length specification
   * @param context        command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String targetVariable,
      final RelationshipPattern pattern, final CommandContext context) {
    this(sourceVariable, pathVariable, targetVariable, pattern, true, context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ExpandPathStep requires a previous step");

    // Optimization: Use vertex iterator when path variable is not needed (avoids loading edges)
    final boolean needsPath = pathVariable != null && !pathVariable.isEmpty();

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result lastResult = null;
      private Iterator<TraversalPath> currentPaths = null;
      private Iterator<Vertex> currentVertices = null;
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

        while (buffer.size() < n) {
          // Optimization: Use vertex-only iterator when path is not needed
          if (needsPath) {
            // Path needed: use full path traversal (slower, loads edges)
            if (currentPaths != null && currentPaths.hasNext()) {
              final TraversalPath path = currentPaths.next();
              final Vertex targetVertex = path.getEndVertex();

              // Create result with path and target vertex
              final ResultInternal result = new ResultInternal();

              // Copy all properties from previous result
              for (final String prop : lastResult.getPropertyNames()) {
                result.setProperty(prop, lastResult.getProperty(prop));
              }

              // Add path binding
              result.setProperty(pathVariable, path);

              // Add target vertex binding
              result.setProperty(targetVariable, targetVertex);

              buffer.add(result);
            } else {
              // Get next source vertex from previous step
              if (prevResults == null) {
                prevResults = prev.syncPull(context, nRecords);
              }

              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }

              lastResult = prevResults.next();
              final Object sourceObj = lastResult.getProperty(sourceVariable);

              if (sourceObj instanceof Vertex) {
                final Vertex sourceVertex = (Vertex) sourceObj;
                currentPaths = createTraverser().traversePaths(sourceVertex);
              } else {
                // Source is not a vertex, skip
                currentPaths = null;
              }
            }
          } else {
            // Path not needed: use vertex-only traversal (faster, no edge loading)
            if (currentVertices != null && currentVertices.hasNext()) {
              final Vertex targetVertex = currentVertices.next();

              // Create result with target vertex only
              final ResultInternal result = new ResultInternal();

              // Copy all properties from previous result
              for (final String prop : lastResult.getPropertyNames()) {
                result.setProperty(prop, lastResult.getProperty(prop));
              }

              // Add target vertex binding
              result.setProperty(targetVariable, targetVertex);

              buffer.add(result);
            } else {
              // Get next source vertex from previous step
              if (prevResults == null) {
                prevResults = prev.syncPull(context, nRecords);
              }

              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }

              lastResult = prevResults.next();
              final Object sourceObj = lastResult.getProperty(sourceVariable);

              if (sourceObj instanceof Vertex) {
                final Vertex sourceVertex = (Vertex) sourceObj;
                currentVertices = createTraverser().traverse(sourceVertex);
              } else {
                // Source is not a vertex, skip
                currentVertices = null;
              }
            }
          }
        }
      }

      @Override
      public void close() {
        ExpandPathStep.this.close();
      }
    };
  }

  /**
   * Creates a traverser for this pattern.
   */
  private VariableLengthPathTraverser createTraverser() {
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    return new VariableLengthPathTraverser(
        pattern.getDirection(),
        types,
        pattern.getEffectiveMinHops(),
        pattern.getEffectiveMaxHops(),
        true, // always track paths
        useBFS
    );
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXPAND PATH ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append(pattern);
    builder.append("(").append(targetVariable).append(")");
    builder.append(" [").append(useBFS ? "BFS" : "DFS").append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
