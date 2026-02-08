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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for matching relationship patterns.
 * Expands from source vertices to target vertices following relationship patterns.
 * <p>
 * Example: (a)-[r:KNOWS]->(b)
 * - Takes vertices bound to 'a' from previous step
 * - Follows KNOWS relationships in OUT direction
 * - Binds edges to 'r' and target vertices to 'b'
 */
public class MatchRelationshipStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String relationshipVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;
  private final String pathVariable;
  private final NodePattern targetNodePattern;
  private final Set<String> boundVariableNames;

  /**
   * Creates a match relationship step.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, null, context);
  }

  /**
   * Creates a match relationship step with path variable support.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param pathVariable         path variable name (e.g., p in p = (a)-[r]->(b)), can be null
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, pathVariable, null, null, context);
  }

  /**
   * Creates a match relationship step with target node filtering and bound variable awareness.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param pathVariable         path variable name (e.g., p in p = (a)-[r]->(b)), can be null
   * @param targetNodePattern    target node pattern for label filtering (can be null)
   * @param boundVariableNames   set of variable names already bound in previous steps (can be null)
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final NodePattern targetNodePattern,
      final Set<String> boundVariableNames, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
    this.pathVariable = pathVariable;
    this.targetNodePattern = targetNodePattern;
    this.boundVariableNames = boundVariableNames;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("MatchRelationshipStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result lastResult = null;
      private Iterator<Edge> currentEdges = null;
      private Set<RID> seenEdges = null;
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
          // Get edges from current vertex
          if (currentEdges != null && currentEdges.hasNext()) {
            final Edge edge = currentEdges.next();

            // For undirected patterns, deduplicate self-loop edges
            // (self-loops appear twice: once as OUT, once as IN)
            if (seenEdges != null && !seenEdges.add(edge.getIdentity()))
              continue;

            final Vertex targetVertex = getTargetVertex(edge, (Vertex) lastResult.getProperty(sourceVariable));

            // Filter by edge type if specified
            if (pattern.hasTypes() && !matchesEdgeType(edge))
              continue;

            // Filter by inline relationship properties if specified
            if (pattern.hasProperties() && !matchesEdgeProperties(edge))
              continue;

            // Relationship uniqueness: Cypher requires each relationship in a pattern
            // to be matched to a distinct edge (no edge traversed twice)
            if (isEdgeAlreadyUsed(lastResult, edge))
              continue;

            // Filter by target node label if specified in the pattern
            if (targetNodePattern != null && targetNodePattern.hasLabels()) {
              if (!matchesTargetLabel(targetVertex)) {
                continue;
              }
            }

            // If the target variable is already bound from a previous step,
            // verify the traversed vertex matches the bound value (identity check)
            if (boundVariableNames != null && boundVariableNames.contains(targetVariable)) {
              final Object boundValue = lastResult.getProperty(targetVariable);
              if (boundValue instanceof Vertex) {
                if (!((Vertex) boundValue).getIdentity().equals(targetVertex.getIdentity())) {
                  continue;
                }
              }
            }

            // Create result with edge and target vertex
            final ResultInternal result = new ResultInternal();

            // Copy all properties from previous result
            for (final String prop : lastResult.getPropertyNames()) {
              result.setProperty(prop, lastResult.getProperty(prop));
            }

            // Add relationship binding if variable is specified
            if (relationshipVariable != null && !relationshipVariable.isEmpty()) {
              result.setProperty(relationshipVariable, edge);
            }

            // Add target vertex binding
            result.setProperty(targetVariable, targetVertex);

            // Add path binding if path variable is specified (e.g., p = (a)-[r]->(b))
            if (pathVariable != null && !pathVariable.isEmpty()) {
              // Check if there's an existing path from a previous hop to extend
              final Object existingPath = lastResult.getProperty(pathVariable);
              final TraversalPath path;
              if (existingPath instanceof TraversalPath)
                // Extend existing path (multi-hop pattern)
                path = new TraversalPath((TraversalPath) existingPath, edge, targetVertex);
              else {
                // Create new path starting from source vertex
                path = new TraversalPath((Vertex) lastResult.getProperty(sourceVariable));
                path.addStep(edge, targetVertex);
              }
              result.setProperty(pathVariable, path);
            }

            buffer.add(result);
          } else {
            // Initialize prevResults on first call
            if (prevResults == null) {
              prevResults = prev.syncPull(context, nRecords);
            }

            // Get next source vertex from previous step
            if (!prevResults.hasNext()) {
              finished = true;
              break;
            }

            lastResult = prevResults.next();
            final Object sourceObj = lastResult.getProperty(sourceVariable);

            if (sourceObj instanceof Vertex) {
              final Vertex sourceVertex = (Vertex) sourceObj;
              currentEdges = getEdges(sourceVertex);
              // Track seen edges for BOTH direction to deduplicate self-loops
              seenEdges = pattern.getDirection() == Direction.BOTH ? new HashSet<>() : null;
            } else {
              // Source is not a vertex, skip
              currentEdges = null;
              seenEdges = null;
            }
          }
        }
      }

      @Override
      public void close() {
        MatchRelationshipStep.this.close();
      }
    };
  }

  /**
   * Gets edges from a vertex based on the relationship pattern.
   */
  private Iterator<Edge> getEdges(final Vertex vertex) {
    final Direction direction = pattern.getDirection();
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    if (types == null || types.length == 0) {
      return vertex.getEdges(direction.toArcadeDirection()).iterator();
    } else {
      return vertex.getEdges(direction.toArcadeDirection(), types).iterator();
    }
  }

  /**
   * Gets the target vertex from an edge based on direction.
   */
  private Vertex getTargetVertex(final Edge edge, final Vertex sourceVertex) {
    final Vertex out = edge.getOutVertex();
    final Vertex in = edge.getInVertex();

    // Determine which vertex is the target based on direction
    if (pattern.getDirection() == Direction.OUT) {
      return in;
    } else if (pattern.getDirection() == Direction.IN) {
      return out;
    } else {
      // BOTH direction - return the vertex that's not the source
      if (out.getIdentity().equals(sourceVertex.getIdentity())) {
        return in;
      } else {
        return out;
      }
    }
  }

  /**
   * Checks if a target vertex matches the label constraints from the target node pattern.
   */
  private boolean matchesTargetLabel(final Vertex vertex) {
    if (targetNodePattern == null || !targetNodePattern.hasLabels())
      return true;

    // Check that the vertex has ALL required labels using type hierarchy
    for (final String label : targetNodePattern.getLabels())
      if (!vertex.getType().instanceOf(label))
        return false;
    return true;
  }

  /**
   * Checks if an edge is already used in the result.
   * Enforces Cypher's relationship uniqueness constraint.
   * Checks both Edge-typed properties and edges inside TraversalPaths.
   */
  @SuppressWarnings("unchecked")
  private boolean isEdgeAlreadyUsed(final Result result, final Edge edge) {
    final RID edgeRid = edge.getIdentity();
    for (final String prop : result.getPropertyNames()) {
      final Object val = result.getProperty(prop);
      if (val instanceof Edge && ((Edge) val).getIdentity().equals(edgeRid))
        return true;
      if (val instanceof TraversalPath) {
        for (final Edge pathEdge : ((TraversalPath) val).getEdges())
          if (pathEdge.getIdentity().equals(edgeRid))
            return true;
      }
      // Check edge lists from VLP relationship variables
      if (val instanceof List) {
        for (final Object item : (List<Object>) val)
          if (item instanceof Edge && ((Edge) item).getIdentity().equals(edgeRid))
            return true;
      }
    }
    return false;
  }

  /**
   * Checks if an edge matches the type filter.
   */
  private boolean matchesEdgeType(final Edge edge) {
    if (!pattern.hasTypes())
      return true;

    final String edgeType = edge.getTypeName();
    for (final String type : pattern.getTypes())
      if (type.equals(edgeType))
        return true;
    return false;
  }

  /**
   * Checks if an edge matches the inline property filters.
   */
  private boolean matchesEdgeProperties(final Edge edge) {
    if (!pattern.hasProperties())
      return true;

    for (final Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final Object actual = edge.get(entry.getKey());
      final Object expected = entry.getValue();
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
    builder.append("+ MATCH RELATIONSHIP ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append(pattern);
    builder.append("(").append(targetVariable).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
