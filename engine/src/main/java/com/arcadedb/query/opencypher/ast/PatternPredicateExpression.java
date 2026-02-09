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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Iterator;
import java.util.List;

/**
 * Represents a pattern predicate expression in a WHERE clause.
 * Pattern predicates test whether a pattern exists in the graph.
 * <p>
 * Examples:
 * - WHERE (n)-[:KNOWS]->() - checks if n has any KNOWS relationship
 * - WHERE (a)-[:KNOWS]->(b) - checks if a KNOWS b
 * - WHERE (n)-[:KNOWS|LIKES]->() - checks if n has KNOWS or LIKES relationship
 * - WHERE NOT (n)-[:KNOWS]->() - checks if n has no KNOWS relationships
 */
public class PatternPredicateExpression implements BooleanExpression {
  private final PathPattern pathPattern;
  private final boolean isNegated;

  public PatternPredicateExpression(final PathPattern pathPattern, final boolean isNegated) {
    this.pathPattern = pathPattern;
    this.isNegated = isNegated;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    // Pattern predicates check if a pattern exists
    // For example: WHERE (n)-[:KNOWS]->() checks if n has any KNOWS relationship

    final boolean patternExists = evaluatePattern(result, context);
    return isNegated ? !patternExists : patternExists;
  }

  private boolean evaluatePattern(final Result result, final CommandContext context) {
    // For now, we support simple relationship existence checks
    // Pattern: (startNode)-[relationship]->(endNode)

    if (pathPattern == null || pathPattern.getRelationshipCount() == 0) {
      // No relationships in pattern - this shouldn't happen
      return false;
    }

    // Get the start node
    final NodePattern startNodePattern = pathPattern.getNode(0);
    final Vertex startVertex = getVertexFromPattern(startNodePattern, result);

    if (startVertex == null) {
      // Start node not found in result
      return false;
    }

    // Get the relationship pattern
    final RelationshipPattern relPattern = pathPattern.getRelationship(0);

    // Handle variable-length patterns (e.g., -[:REL*2]-, -[:REL*]-)
    if (relPattern.isVariableLength())
      return evaluateVLPPattern(startVertex, relPattern, result);

    final List<String> relationshipTypesList = relPattern.getTypes();
    final String[] relationshipTypes = relationshipTypesList != null && !relationshipTypesList.isEmpty()
        ? relationshipTypesList.toArray(new String[0])
        : null;
    final Direction direction = relPattern.getDirection();
    final boolean isOutgoing = direction == Direction.OUT || direction == Direction.BOTH;
    final boolean isIncoming = direction == Direction.IN || direction == Direction.BOTH;

    // Get the end node pattern (if specified)
    final NodePattern endNodePattern = pathPattern.getNode(1);
    final Vertex endVertex = getVertexFromPattern(endNodePattern, result);

    // Get target node labels (if specified) for filtering
    final List<String> targetLabels = endNodePattern != null ? endNodePattern.getLabels() : null;

    // Check if the pattern exists
    if (endVertex != null) {
      // We have a specific end node - check if relationship exists between them
      // Also verify target labels if specified (e.g., (a)-[:T]->(b:Label))
      if (targetLabels != null && !targetLabels.isEmpty() && !matchesTargetLabels(endVertex, targetLabels))
        return false;
      return checkRelationshipExists(startVertex, endVertex, relationshipTypes, isOutgoing, isIncoming);
    } else {
      // No specific end node - check if any relationship of the specified type exists
      // Also filter by target node labels if specified
      return checkAnyRelationshipExists(startVertex, relationshipTypes, targetLabels, isOutgoing, isIncoming);
    }
  }

  /**
   * Evaluates a variable-length pattern predicate using the VLP traverser.
   * Handles patterns like (n)-[:REL*2]-() or (n)-[:REL*]-(m).
   */
  private boolean evaluateVLPPattern(final Vertex startVertex, final RelationshipPattern relPattern, final Result result) {
    final List<String> typesList = relPattern.getTypes();
    final String[] types = typesList != null && !typesList.isEmpty() ? typesList.toArray(new String[0]) : null;

    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(
        relPattern.getDirection(), types, null,
        relPattern.getEffectiveMinHops(), relPattern.getEffectiveMaxHops(),
        true, true);

    // Get the end node (if bound)
    final NodePattern endNodePattern = pathPattern.getNode(1);
    final Vertex endVertex = getVertexFromPattern(endNodePattern, result);
    final List<String> targetLabels = endNodePattern != null ? endNodePattern.getLabels() : null;

    final Iterator<TraversalPath> paths = traverser.traversePaths(startVertex);
    while (paths.hasNext()) {
      final TraversalPath path = paths.next();
      // Skip zero-length paths (the start node itself) unless explicitly requested
      if (path.length() == 0)
        continue;
      final Vertex target = path.getEndVertex();
      if (endVertex != null) {
        if (target.getIdentity().equals(endVertex.getIdentity())
            && (targetLabels == null || targetLabels.isEmpty() || matchesTargetLabels(target, targetLabels)))
          return true;
      } else {
        if (targetLabels == null || targetLabels.isEmpty() || matchesTargetLabels(target, targetLabels))
          return true;
      }
    }
    return false;
  }

  /**
   * Get a vertex from the result based on the node pattern.
   */
  private Vertex getVertexFromPattern(final NodePattern nodePattern, final Result result) {
    if (nodePattern == null) {
      return null;
    }

    final String variable = nodePattern.getVariable();
    if (variable == null || variable.isEmpty()) {
      // Anonymous node
      return null;
    }

    final Object obj = result.getProperty(variable);
    if (obj instanceof Vertex) {
      return (Vertex) obj;
    }

    return null;
  }

  /**
   * Check if a relationship exists between two specific vertices.
   */
  private boolean checkRelationshipExists(
      final Vertex startVertex,
      final Vertex endVertex,
      final String[] relationshipTypes,
      final boolean isOutgoing,
      final boolean isIncoming
  ) {
    // Check outgoing edges: startVertex -> endVertex
    if (isOutgoing) {
      final Iterator<Edge> outEdges;
      if (relationshipTypes != null && relationshipTypes.length > 0) {
        outEdges = startVertex.getEdges(Vertex.DIRECTION.OUT, relationshipTypes).iterator();
      } else {
        outEdges = startVertex.getEdges(Vertex.DIRECTION.OUT).iterator();
      }

      while (outEdges.hasNext()) {
        final Edge edge = outEdges.next();
        if (edge.getIn().equals(endVertex)) {
          return true;
        }
      }
    }

    // Check incoming edges: startVertex <- endVertex
    if (isIncoming) {
      final Iterator<Edge> inEdges;
      if (relationshipTypes != null && relationshipTypes.length > 0) {
        inEdges = startVertex.getEdges(Vertex.DIRECTION.IN, relationshipTypes).iterator();
      } else {
        inEdges = startVertex.getEdges(Vertex.DIRECTION.IN).iterator();
      }

      while (inEdges.hasNext()) {
        final Edge edge = inEdges.next();
        if (edge.getOut().equals(endVertex)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check if any relationship of the specified type exists from the start vertex.
   * Also filters by target node labels if specified.
   */
  private boolean checkAnyRelationshipExists(
      final Vertex startVertex,
      final String[] relationshipTypes,
      final List<String> targetLabels,
      final boolean isOutgoing,
      final boolean isIncoming
  ) {
    // Check outgoing edges
    if (isOutgoing) {
      final Iterator<Edge> outEdges;
      if (relationshipTypes != null && relationshipTypes.length > 0) {
        outEdges = startVertex.getEdges(Vertex.DIRECTION.OUT, relationshipTypes).iterator();
      } else {
        outEdges = startVertex.getEdges(Vertex.DIRECTION.OUT).iterator();
      }

      while (outEdges.hasNext()) {
        final Edge edge = outEdges.next();
        if (matchesTargetLabels(edge.getInVertex(), targetLabels))
          return true;
      }
    }

    // Check incoming edges
    if (isIncoming) {
      final Iterator<Edge> inEdges;
      if (relationshipTypes != null && relationshipTypes.length > 0) {
        inEdges = startVertex.getEdges(Vertex.DIRECTION.IN, relationshipTypes).iterator();
      } else {
        inEdges = startVertex.getEdges(Vertex.DIRECTION.IN).iterator();
      }

      while (inEdges.hasNext()) {
        final Edge edge = inEdges.next();
        if (matchesTargetLabels(edge.getOutVertex(), targetLabels))
          return true;
      }
    }

    return false;
  }

  /**
   * Check if a vertex matches the target labels.
   * If no labels are specified, returns true (matches any vertex).
   */
  private boolean matchesTargetLabels(final Vertex vertex, final List<String> targetLabels) {
    if (targetLabels == null || targetLabels.isEmpty())
      return true;

    // Check if vertex has ALL the specified labels (AND semantics)
    for (final String label : targetLabels) {
      if (!Labels.hasLabel(vertex, label))
        return false;
    }
    return true;
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    if (isNegated) {
      sb.append("NOT ");
    }
    sb.append("(");
    sb.append(pathPattern.toString());
    sb.append(")");
    return sb.toString();
  }

  public PathPattern getPathPattern() {
    return pathPattern;
  }

  public boolean isNegated() {
    return isNegated;
  }
}
