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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.CreateClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.sql.executor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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

  public CreateStep(final CreateClause createClause, final CommandContext context) {
    super(context);
    this.createClause = createClause;
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
   *
   * @param inputResult input result from previous step (may be null for standalone CREATE)
   * @return result containing all created elements
   */
  private Result createPatterns(final Result inputResult) {
    // Check if we're already in a transaction
    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      // Begin transaction if not already active
      if (!wasInTransaction) {
        context.getDatabase().begin();
      }

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

      // Commit transaction if we started it
      if (!wasInTransaction) {
        context.getDatabase().commit();
      }

      return result;
    } catch (final Exception e) {
      // Rollback if we started the transaction
      if (!wasInTransaction && context.getDatabase().isTransactionActive()) {
        context.getDatabase().rollback();
      }
      throw e;
    }
  }

  /**
   * Creates a complete path (vertices and edges).
   */
  private void createPath(final PathPattern pathPattern, final ResultInternal result) {
    if (pathPattern.isSingleNode()) {
      // Simple node creation: CREATE (n:Person {name: 'Alice'})
      final NodePattern nodePattern = pathPattern.getFirstNode();
      final Vertex vertex = createVertex(nodePattern);
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
          vertex = createVertex(nodePattern);
          if (nodePattern.getVariable() != null) {
            result.setProperty(nodePattern.getVariable(), vertex);
          }
        }

        vertices.add(vertex);
      }

      // Create relationships between vertices
      for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
        final RelationshipPattern relPattern = pathPattern.getRelationship(i);
        final Vertex fromVertex = vertices.get(i);
        final Vertex toVertex = vertices.get(i + 1);

        final Edge edge = createEdge(fromVertex, toVertex, relPattern);
        if (relPattern.getVariable() != null) {
          result.setProperty(relPattern.getVariable(), edge);
        }
      }
    }
  }

  /**
   * Creates a vertex from a node pattern.
   */
  private Vertex createVertex(final NodePattern nodePattern) {
    final String label = nodePattern.hasLabels() ? nodePattern.getFirstLabel() : "Vertex";

    // Ensure vertex type exists (Cypher auto-creates types)
    context.getDatabase().getSchema().getOrCreateVertexType(label);

    final MutableVertex vertex = context.getDatabase().newVertex(label);

    // Set properties from pattern
    if (nodePattern.hasProperties()) {
      setProperties(vertex, nodePattern.getProperties());
    }

    vertex.save();
    return vertex;
  }

  /**
   * Creates an edge between two vertices.
   */
  private Edge createEdge(final Vertex fromVertex, final Vertex toVertex, final RelationshipPattern relPattern) {
    final String type = relPattern.hasTypes() ? relPattern.getFirstType() : "EDGE";

    // Ensure edge type exists (Cypher auto-creates types)
    context.getDatabase().getSchema().getOrCreateEdgeType(type);

    final MutableEdge edge = fromVertex.newEdge(type, toVertex);

    // Set properties from pattern
    if (relPattern.hasProperties()) {
      setPropertiesOnEdge(edge, relPattern.getProperties());
    }

    edge.save();
    return edge;
  }

  /**
   * Sets properties on a document from a property map.
   * Property values are already processed by CypherASTBuilder.evaluateExpression:
   * - String literals have quotes stripped and escape sequences decoded
   * - Parameters are represented as ParameterReference objects
   */
  private void setProperties(final MutableDocument document, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Resolve parameter references
      if (value instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) value).getName();
        value = context.getInputParameters().get(paramName);
      }

      document.set(key, value);
    }
  }

  /**
   * Sets properties on an edge from a property map.
   * Property values are already processed by CypherASTBuilder.evaluateExpression.
   */
  private void setPropertiesOnEdge(final MutableEdge edge, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Resolve parameter references
      if (value instanceof CypherASTBuilder.ParameterReference) {
        final String paramName = ((CypherASTBuilder.ParameterReference) value).getName();
        value = context.getInputParameters().get(paramName);
      }

      edge.set(key, value);
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
