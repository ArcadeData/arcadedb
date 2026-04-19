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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expression representing a pattern comprehension.
 * Syntax: [(variable =)? pattern WHERE filterExpression | mapExpression]
 * <p>
 * Examples:
 * - [(a)-->(friend) | friend.name] -> list of friend names
 * - [(a)-->(friend) WHERE friend.name <> 'B' | friend.name] -> filtered list
 * - [(a)-[:KNOWS]->(friend) | friend.name] -> list of KNOWS friend names
 */
public class PatternComprehensionExpression implements Expression {
  private final String pathVariable;      // Optional path variable name
  private final PathPattern pathPattern;
  private final Expression whereExpression;  // Optional filter
  private final Expression mapExpression;    // Required mapping (after |)
  private final String text;

  public PatternComprehensionExpression(final String pathVariable, final PathPattern pathPattern,
      final Expression whereExpression, final Expression mapExpression, final String text) {
    this.pathVariable = pathVariable;
    this.pathPattern = pathPattern;
    this.whereExpression = whereExpression;
    this.mapExpression = mapExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final List<Object> resultList = new ArrayList<>();
    final List<Object> pathElements = new ArrayList<>();
    traversePattern(result, context, 0, result, resultList, pathElements);
    return resultList;
  }

  /**
   * Recursively traverse pattern hops, collecting results at each complete match.
   */
  private void traversePattern(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements) {
    if (hopIndex >= pathPattern.getRelationshipCount()) {
      // All hops matched - apply WHERE filter and map expression
      if (whereExpression != null) {
        final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, currentResult, context);
        if (filterValue == null || (filterValue instanceof Boolean && !((Boolean) filterValue)))
          return;
      }
      // Bind path variable if present (e.g., [p = (n)-->() | p])
      Result evalResult = currentResult;
      if (pathVariable != null && !pathVariable.isEmpty()) {
        final ResultInternal pathResult = new ResultInternal();
        for (final String prop : currentResult.getPropertyNames())
          pathResult.setProperty(prop, currentResult.getProperty(prop));
        pathResult.setProperty(pathVariable, new ArrayList<>(pathElements));
        evalResult = pathResult;
      }
      resultList.add(OpenCypherQueryEngine.getExpressionEvaluator().evaluate(mapExpression, evalResult, context));
      return;
    }

    final NodePattern startNodePattern = pathPattern.getNode(hopIndex);
    final Vertex startVertex = resolveVertex(startNodePattern, currentResult);
    if (startVertex == null)
      return;

    // Add start vertex to path at first hop
    if (hopIndex == 0 && pathVariable != null)
      pathElements.add(startVertex);

    final RelationshipPattern relPattern = pathPattern.getRelationship(hopIndex);
    final NodePattern endNodePattern = pathPattern.getNode(hopIndex + 1);

    final List<String> relTypes = relPattern.getTypes();
    final String[] relTypeArray = relTypes != null && !relTypes.isEmpty() ? relTypes.toArray(new String[0]) : null;

    final Direction direction = relPattern.getDirection();
    final boolean checkOut = direction == Direction.OUT || direction == Direction.BOTH;
    final boolean checkIn = direction == Direction.IN || direction == Direction.BOTH;

    if (relPattern.isVariableLength()) {
      final int minHops = relPattern.getEffectiveMinHops();
      final int maxHops = relPattern.getEffectiveMaxHops();

      // Zero-length path: start and end are the same vertex (only valid if matches end pattern)
      if (minHops == 0 && matchesEndPattern(startVertex, endNodePattern)) {
        final ResultInternal hopResult = buildHopResult(currentResult, endNodePattern, startVertex, relPattern, null);
        traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements);
      }

      if (maxHops >= 1) {
        final Set<RID> visitedEdges = new HashSet<>();
        if (checkOut)
          traverseVariableLength(baseResult, context, hopIndex, currentResult, resultList, pathElements,
              startVertex, Vertex.DIRECTION.OUT, relTypeArray, endNodePattern, relPattern, 0, minHops, maxHops, visitedEdges);
        if (checkIn)
          traverseVariableLength(baseResult, context, hopIndex, currentResult, resultList, pathElements,
              startVertex, Vertex.DIRECTION.IN, relTypeArray, endNodePattern, relPattern, 0, minHops, maxHops, visitedEdges);
      }
    } else {
      if (checkOut)
        traverseEdges(baseResult, context, hopIndex, currentResult, resultList, pathElements,
            startVertex, Vertex.DIRECTION.OUT, relTypeArray, endNodePattern, relPattern);

      if (checkIn)
        traverseEdges(baseResult, context, hopIndex, currentResult, resultList, pathElements,
            startVertex, Vertex.DIRECTION.IN, relTypeArray, endNodePattern, relPattern);
    }

    // Remove start vertex from path when backtracking
    if (hopIndex == 0 && pathVariable != null && !pathElements.isEmpty())
      pathElements.remove(pathElements.size() - 1);
  }

  private void traverseVariableLength(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements,
      final Vertex currentVertex, final Vertex.DIRECTION edgeDirection,
      final String[] relTypeArray, final NodePattern endNodePattern,
      final RelationshipPattern relPattern,
      final int currentHop, final int minHops, final int maxHops,
      final Set<RID> visitedEdges) {
    if (currentHop >= maxHops)
      return;

    final Iterator<Edge> edges;
    if (relTypeArray != null)
      edges = currentVertex.getEdges(edgeDirection, relTypeArray).iterator();
    else
      edges = currentVertex.getEdges(edgeDirection).iterator();

    while (edges.hasNext()) {
      final Edge edge = edges.next();
      final RID edgeRid = edge.getIdentity();
      // Trail semantics: do not repeat the same edge in a single path
      if (!visitedEdges.add(edgeRid))
        continue;

      final Vertex nextVertex = edgeDirection == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
      final int nextHop = currentHop + 1;
      final boolean trackPath = pathVariable != null;
      if (trackPath) {
        pathElements.add(edge);
        pathElements.add(nextVertex);
      }

      if (nextHop >= minHops && matchesEndPattern(nextVertex, endNodePattern)) {
        final ResultInternal hopResult = buildHopResult(currentResult, endNodePattern, nextVertex, relPattern, edge);
        traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements);
      }

      if (nextHop < maxHops)
        traverseVariableLength(baseResult, context, hopIndex, currentResult, resultList, pathElements,
            nextVertex, edgeDirection, relTypeArray, endNodePattern, relPattern, nextHop, minHops, maxHops, visitedEdges);

      if (trackPath) {
        pathElements.removeLast();
        pathElements.removeLast();
      }
      visitedEdges.remove(edgeRid);
    }
  }

  private boolean matchesEndPattern(final Vertex vertex, final NodePattern endNodePattern) {
    if (endNodePattern.hasLabels()) {
      for (final String label : endNodePattern.getLabels()) {
        if (!Labels.hasLabel(vertex, label))
          return false;
      }
    }
    if (endNodePattern.hasProperties()) {
      for (final Map.Entry<String, Object> entry : endNodePattern.getProperties().entrySet()) {
        final Object actual = vertex.get(entry.getKey());
        if (!entry.getValue().equals(actual))
          return false;
      }
    }
    return true;
  }

  private ResultInternal buildHopResult(final Result currentResult, final NodePattern endNodePattern, final Vertex targetVertex,
      final RelationshipPattern relPattern, final Edge edge) {
    final ResultInternal hopResult = new ResultInternal();
    if (currentResult != null)
      for (final String prop : currentResult.getPropertyNames())
        hopResult.setProperty(prop, currentResult.getProperty(prop));
    if (endNodePattern.getVariable() != null)
      hopResult.setProperty(endNodePattern.getVariable(), targetVertex);
    if (relPattern.getVariable() != null && edge != null)
      hopResult.setProperty(relPattern.getVariable(), edge);
    return hopResult;
  }

  private void traverseEdges(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements,
      final Vertex startVertex, final Vertex.DIRECTION edgeDirection,
      final String[] relTypeArray, final NodePattern endNodePattern,
      final RelationshipPattern relPattern) {
    final Iterator<Edge> edges;
    if (relTypeArray != null)
      edges = startVertex.getEdges(edgeDirection, relTypeArray).iterator();
    else
      edges = startVertex.getEdges(edgeDirection).iterator();

    while (edges.hasNext()) {
      final Edge edge = edges.next();
      final Vertex targetVertex = edgeDirection == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();

      // Check target node labels
      if (endNodePattern.hasLabels()) {
        boolean labelsMatch = true;
        for (final String label : endNodePattern.getLabels()) {
          if (!Labels.hasLabel(targetVertex, label)) {
            labelsMatch = false;
            break;
          }
        }
        if (!labelsMatch)
          continue;
      }

      // Check target node property constraints
      if (endNodePattern.hasProperties()) {
        boolean propsMatch = true;
        for (final Map.Entry<String, Object> entry : endNodePattern.getProperties().entrySet()) {
          final Object actual = targetVertex.get(entry.getKey());
          if (!entry.getValue().equals(actual)) {
            propsMatch = false;
            break;
          }
        }
        if (!propsMatch)
          continue;
      }

      // Build result with matched variables
      final ResultInternal hopResult = new ResultInternal();
      // Copy base result properties
      if (currentResult != null)
        for (final String prop : currentResult.getPropertyNames())
          hopResult.setProperty(prop, currentResult.getProperty(prop));

      // Bind the target node variable
      if (endNodePattern.getVariable() != null)
        hopResult.setProperty(endNodePattern.getVariable(), targetVertex);

      // Bind the relationship variable
      if (relPattern.getVariable() != null)
        hopResult.setProperty(relPattern.getVariable(), edge);

      // Add edge and target to path elements for path variable
      if (pathVariable != null) {
        pathElements.add(edge);
        pathElements.add(targetVertex);
      }

      // Continue to next hop or collect result
      traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements);

      // Remove edge and target from path when backtracking
      if (pathVariable != null) {
        pathElements.remove(pathElements.size() - 1);
        pathElements.remove(pathElements.size() - 1);
      }
    }
  }

  private Vertex resolveVertex(final NodePattern nodePattern, final Result result) {
    if (nodePattern == null)
      return null;
    final String variable = nodePattern.getVariable();
    if (variable == null || variable.isEmpty())
      return null;
    final Object obj = result.getProperty(variable);
    if (obj instanceof Vertex)
      return (Vertex) obj;
    return null;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public boolean containsAggregation() {
    if (whereExpression != null && whereExpression.containsAggregation())
      return true;
    return mapExpression.containsAggregation();
  }

  @Override
  public String getText() {
    return text;
  }
}
