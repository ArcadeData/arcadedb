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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    traversePattern(result, context, 0, result, resultList);
    return resultList;
  }

  /**
   * Recursively traverse pattern hops, collecting results at each complete match.
   */
  private void traversePattern(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList) {
    if (hopIndex >= pathPattern.getRelationshipCount()) {
      // All hops matched - apply WHERE filter and map expression
      if (whereExpression != null) {
        final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, currentResult, context);
        if (filterValue == null || (filterValue instanceof Boolean && !((Boolean) filterValue)))
          return;
      }
      resultList.add(OpenCypherQueryEngine.getExpressionEvaluator().evaluate(mapExpression, currentResult, context));
      return;
    }

    final NodePattern startNodePattern = pathPattern.getNode(hopIndex);
    final Vertex startVertex = resolveVertex(startNodePattern, currentResult);
    if (startVertex == null)
      return;

    final RelationshipPattern relPattern = pathPattern.getRelationship(hopIndex);
    final NodePattern endNodePattern = pathPattern.getNode(hopIndex + 1);

    final List<String> relTypes = relPattern.getTypes();
    final String[] relTypeArray = relTypes != null && !relTypes.isEmpty() ? relTypes.toArray(new String[0]) : null;

    final Direction direction = relPattern.getDirection();
    final boolean checkOut = direction == Direction.OUT || direction == Direction.BOTH;
    final boolean checkIn = direction == Direction.IN || direction == Direction.BOTH;

    if (checkOut)
      traverseEdges(baseResult, context, hopIndex, currentResult, resultList,
          startVertex, Vertex.DIRECTION.OUT, relTypeArray, endNodePattern, relPattern);

    if (checkIn)
      traverseEdges(baseResult, context, hopIndex, currentResult, resultList,
          startVertex, Vertex.DIRECTION.IN, relTypeArray, endNodePattern, relPattern);
  }

  private void traverseEdges(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
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

      // Continue to next hop or collect result
      traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList);
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
