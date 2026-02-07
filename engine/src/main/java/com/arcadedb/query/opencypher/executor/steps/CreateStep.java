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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.sql.executor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Execution step for CREATE clause.
 * Creates new vertices and/or edges in the graph.
 * <p>
 * Examples:
 * - CREATE (n:Person {name: 'Alice', age: 30})
 * - CREATE (a)-[r:KNOWS]->(b)
 * - CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
 * <p>
 * Can work standalone or chained after MATCH for context-dependent creation.
 */
public class CreateStep extends AbstractExecutionStep {
  private final CreateClause createClause;
  private final ExpressionEvaluator evaluator;

  public CreateStep(final CreateClause createClause, final CommandContext context,
                    final CypherFunctionFactory functionFactory) {
    super(context);
    this.createClause = createClause;
    this.evaluator = functionFactory != null ? new ExpressionEvaluator(functionFactory) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final boolean hasInput = prev != null;

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      private boolean createdStandalone = false;

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
          // Chained CREATE: create for each input result
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          while (buffer.size() < n && prevResults.hasNext()) {
            final Result inputResult = prevResults.next();
            final Result createdResult = createPatterns(inputResult);
            buffer.add(createdResult);
          }

          if (!prevResults.hasNext()) {
            finished = true;
          }
        } else {
          // Standalone CREATE: create once
          if (!createdStandalone) {
            final Result createdResult = createPatterns(null);
            buffer.add(createdResult);
            createdStandalone = true;
          }
          finished = true;
        }
      }

      @Override
      public void close() {
        CreateStep.this.close();
      }
    };
  }

  /**
   * Creates vertices and edges according to the path patterns.
   * Uses database.transaction() to get automatic retry on MVCC conflicts.
   *
   * @param inputResult input result from previous step (may be null for standalone CREATE)
   * @return result containing all created elements
   */
  private Result createPatterns(final Result inputResult) {
    final Database database = context.getDatabase();
    final AtomicReference<ResultInternal> resultRef = new AtomicReference<>();

    // Use database.transaction() for automatic retry on NeedRetryException/ConcurrentModificationException
    // joinCurrentTx=true means it will join an existing transaction if one is active
    database.transaction(() -> {
      final ResultInternal result = new ResultInternal();

      // Copy input properties if present
      if (inputResult != null) {
        for (final String prop : inputResult.getPropertyNames()) {
          result.setProperty(prop, inputResult.getProperty(prop));
        }
      }

      // Create each path pattern
      for (final PathPattern pathPattern : createClause.getPathPatterns()) {
        createPath(pathPattern, result);
      }

      resultRef.set(result);
    }, true);

    return resultRef.get();
  }

  /**
   * Creates a complete path (vertices and edges).
   */
  private void createPath(final PathPattern pathPattern, final ResultInternal result) {
    if (pathPattern.isSingleNode()) {
      // Simple node creation: CREATE (n:Person {name: 'Alice'})
      final NodePattern nodePattern = pathPattern.getFirstNode();
      final Vertex vertex = createVertex(nodePattern, result);
      final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : "n";
      result.setProperty(variable, vertex);
    } else {
      // Path with relationships: CREATE (a)-[r:KNOWS]->(b)
      final List<Vertex> vertices = new ArrayList<>();

      // Create all vertices in the path
      for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
        final NodePattern nodePattern = pathPattern.getNode(i);
        Vertex vertex = null;

        // Check if vertex already exists in result (from MATCH)
        if (nodePattern.getVariable() != null) {
          final Object existing = result.getProperty(nodePattern.getVariable());
          if (existing instanceof Vertex) {
            vertex = (Vertex) existing;
          }
        }

        // Create vertex if not found
        if (vertex == null) {
          vertex = createVertex(nodePattern, result);
          if (nodePattern.getVariable() != null) {
            result.setProperty(nodePattern.getVariable(), vertex);
          }
        }

        vertices.add(vertex);
      }

      // Create relationships between vertices
      for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
        final RelationshipPattern relPattern = pathPattern.getRelationship(i);
        final Vertex fromVertex;
        final Vertex toVertex;

        if (relPattern.getDirection() == Direction.IN) {
          fromVertex = vertices.get(i + 1);
          toVertex = vertices.get(i);
        } else {
          fromVertex = vertices.get(i);
          toVertex = vertices.get(i + 1);
        }

        final Edge edge = createEdge(fromVertex, toVertex, relPattern, result);
        if (relPattern.getVariable() != null) {
          result.setProperty(relPattern.getVariable(), edge);
        }
      }
    }
  }

  /**
   * Creates a vertex from a node pattern.
   * <p>
   * Supports multi-label vertices. When multiple labels are specified
   * (e.g., CREATE (n:Person:Developer)), a composite type is automatically
   * created that extends all label types.
   */
  private Vertex createVertex(final NodePattern nodePattern, final Result currentResult) {
    final List<String> labels = nodePattern.hasLabels()
        ? nodePattern.getLabels()
        : List.of("Vertex");

    // Get or create the appropriate type (composite if multiple labels)
    final String typeName = Labels.ensureCompositeType(
        context.getDatabase().getSchema(),
        labels
    );

    final MutableVertex vertex = context.getDatabase().newVertex(typeName);

    // Set properties from pattern
    if (nodePattern.hasProperties()) {
      setProperties(vertex, nodePattern.getProperties(), currentResult);
    }

    vertex.save();
    return vertex;
  }

  /**
   * Creates an edge between two vertices.
   */
  private Edge createEdge(final Vertex fromVertex, final Vertex toVertex, final RelationshipPattern relPattern, final Result currentResult) {
    final String type = relPattern.hasTypes() ? relPattern.getFirstType() : "EDGE";

    // Ensure edge type exists (Cypher auto-creates types)
    context.getDatabase().getSchema().getOrCreateEdgeType(type);

    final MutableEdge edge = fromVertex.newEdge(type, toVertex);

    // Set properties from pattern
    if (relPattern.hasProperties()) {
      setPropertiesOnEdge(edge, relPattern.getProperties(), currentResult);
    }

    edge.save();
    return edge;
  }

  /**
   * Sets properties on a document from a property map.
   * Property values can be:
   * - Literal values (already evaluated)
   * - ParameterReference objects (to be resolved from context parameters)
   * - Expression objects (to be evaluated in the context of the current result)
   */
  private void setProperties(final MutableDocument document, final Map<String, Object> properties, final Result currentResult) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Resolve parameter references
      if (value instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) value).getName();
        value = context.getInputParameters().get(paramName);
      }
      // Evaluate Expression objects (e.g., property access, function calls like rand())
      else if (value instanceof Expression) {
        final Expression expr = (Expression) value;
        if (evaluator != null)
          value = evaluator.evaluate(expr, currentResult, context);
        else
          value = expr.evaluate(currentResult, context);
      }

      // In Cypher, null property values are not stored
      if (value != null)
        document.set(key, convertTemporalForStorage(value));
    }
  }

  /**
   * Convert CypherTemporalValue objects to java.time types for ArcadeDB storage.
   */
  private static Object convertTemporalForStorage(final Object value) {
    if (value instanceof CypherDate)
      return ((CypherDate) value).getValue();
    if (value instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) value).getValue();
    if (value instanceof CypherDateTime)
      return ((CypherDateTime) value).getValue().toLocalDateTime();
    if (value instanceof CypherLocalTime)
      return ((CypherLocalTime) value).getValue().toString();
    if (value instanceof CypherTime)
      return ((CypherTime) value).getValue().toString();
    if (value instanceof CypherDuration)
      return value.toString();
    return value;
  }

  /**
   * Sets properties on an edge from a property map.
   * Property values can be:
   * - Literal values (already evaluated)
   * - ParameterReference objects (to be resolved from context parameters)
   * - Expression objects (to be evaluated in the context of the current result)
   */
  private void setPropertiesOnEdge(final MutableEdge edge, final Map<String, Object> properties, final Result currentResult) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Resolve parameter references
      if (value instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) value).getName();
        value = context.getInputParameters().get(paramName);
      }
      // Evaluate Expression objects (e.g., property access, function calls like rand())
      else if (value instanceof Expression) {
        final Expression expr = (Expression) value;
        if (evaluator != null)
          value = evaluator.evaluate(expr, currentResult, context);
        else
          value = expr.evaluate(currentResult, context);
      }

      edge.set(key, convertTemporalForStorage(value));
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ CREATE");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
