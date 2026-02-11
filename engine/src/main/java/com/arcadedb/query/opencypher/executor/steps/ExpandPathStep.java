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
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import com.arcadedb.graph.Edge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private final String relationshipVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;
  private final NodePattern targetNodePattern;
  private final boolean useBFS;

  /**
   * Creates an expand path step.
   *
   * @param sourceVariable       variable name for source vertex
   * @param pathVariable         variable name for the path (can be null)
   * @param relationshipVariable variable name for the relationship list (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern with variable-length specification
   * @param useBFS               true for BFS (shortest paths), false for DFS (all paths)
   * @param context              command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final boolean useBFS,
      final NodePattern targetNodePattern, final CommandContext context) {
    super(context);

    if (!pattern.isVariableLength()) {
      throw new IllegalArgumentException("ExpandPathStep requires a variable-length relationship pattern");
    }

    this.sourceVariable = sourceVariable;
    this.pathVariable = pathVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
    this.targetNodePattern = targetNodePattern;
    this.useBFS = useBFS;
  }

  /**
   * Creates an expand path step with BFS (default).
   *
   * @param sourceVariable       variable name for source vertex
   * @param pathVariable         variable name for the path (can be null)
   * @param relationshipVariable variable name for the relationship list (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern with variable-length specification
   * @param context              command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final CommandContext context) {
    this(sourceVariable, pathVariable, relationshipVariable, targetVariable, pattern, true, null, context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ExpandPathStep requires a previous step");

    final boolean hasPathVar = pathVariable != null && !pathVariable.isEmpty();
    final boolean hasRelVar = relationshipVariable != null && !relationshipVariable.isEmpty();

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result lastResult = null;
      private Iterator<TraversalPath> currentPaths = null;
      private Vertex boundTarget = null;
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
          // Always use path traversal for correct Cypher edge-based relationship uniqueness
          if (currentPaths != null && currentPaths.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final TraversalPath path = currentPaths.next();
              final Vertex targetVertex = path.getEndVertex();

              // Filter by target node label if specified
              if (targetNodePattern != null && targetNodePattern.hasLabels()) {
                if (!matchesTargetLabel(targetVertex))
                  continue;
              }

              // Filter by target node properties if specified
              if (targetNodePattern != null && targetNodePattern.hasProperties()) {
                if (!matchesTargetProperties(targetVertex))
                  continue;
              }

              // If the target variable is already bound from a previous step,
              // only accept paths that end at the bound vertex
              if (boundTarget != null) {
                if (!targetVertex.getIdentity().equals(boundTarget.getIdentity()))
                  continue;
              }

              // Relationship uniqueness: check if any edge in this path is
              // already used by a relationship variable in the current result
              if (hasEdgeConflict(lastResult, path))
                continue;

              final ResultInternal result = new ResultInternal();

              // Copy all properties from previous result
              for (final String prop : lastResult.getPropertyNames()) {
                result.setProperty(prop, lastResult.getProperty(prop));
              }

              // Add path binding - extend existing path if present (multi-segment VLP)
              if (hasPathVar) {
                final Object existingPath = lastResult.getProperty(pathVariable);
                if (existingPath instanceof TraversalPath)
                  result.setProperty(pathVariable, new TraversalPath((TraversalPath) existingPath, path));
                else
                  result.setProperty(pathVariable, path);
              }

              // Add relationship variable as list of edges
              if (hasRelVar)
                result.setProperty(relationshipVariable, new ArrayList<>(path.getEdges()));

              // Add target vertex binding
              result.setProperty(targetVariable, targetVertex);

              buffer.add(result);
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
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

            // Check if target variable is already bound (e.g., from a previous MATCH)
            final Object targetObj = lastResult.getProperty(targetVariable);
            boundTarget = (targetObj instanceof Vertex) ? (Vertex) targetObj : null;

            if (sourceObj instanceof Vertex) {
              final Vertex sourceVertex = (Vertex) sourceObj;
              currentPaths = createTraverser().traversePaths(sourceVertex);
            } else {
              currentPaths = null;
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

    final Map<String, Object> props = pattern.hasProperties() ? pattern.getProperties() : null;

    return new VariableLengthPathTraverser(
        pattern.getDirection(),
        types,
        props,
        pattern.getEffectiveMinHops(),
        pattern.getEffectiveMaxHops(),
        true, // always track paths
        useBFS
    );
  }

  /**
   * Checks if any edge in the traversal path conflicts with edges already
   * bound in the result (relationship uniqueness within a MATCH clause).
   */
  @SuppressWarnings("unchecked")
  private boolean hasEdgeConflict(final Result result, final TraversalPath path) {
    if (path.getEdges().isEmpty())
      return false;
    for (final String prop : result.getPropertyNames()) {
      // Skip our own variables
      if (prop.equals(relationshipVariable) || prop.equals(pathVariable) || prop.equals(targetVariable))
        continue;
      final Object val = result.getProperty(prop);
      if (val instanceof Edge) {
        for (final Edge pathEdge : path.getEdges())
          if (pathEdge.getIdentity().equals(((Edge) val).getIdentity()))
            return true;
      }
      if (val instanceof List) {
        for (final Object item : (List<Object>) val)
          if (item instanceof Edge) {
            for (final Edge pathEdge : path.getEdges())
              if (pathEdge.getIdentity().equals(((Edge) item).getIdentity()))
                return true;
          }
      }
    }
    return false;
  }

  private boolean matchesTargetLabel(final Vertex vertex) {
    for (final String label : targetNodePattern.getLabels())
      if (!vertex.getType().instanceOf(label))
        return false;
    return true;
  }

  private boolean matchesTargetProperties(final Vertex vertex) {
    for (final Map.Entry<String, Object> entry : targetNodePattern.getProperties().entrySet()) {
      final Object actual = vertex.get(entry.getKey());
      Object expected = entry.getValue();
      // Handle string literals: remove quotes
      if (expected instanceof String) {
        final String s = (String) expected;
        if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\"")))
          expected = s.substring(1, s.length() - 1);
      }
      if (actual == null || !actual.equals(expected))
        return false;
    }
    return true;
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
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
