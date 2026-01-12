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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.opencypher.ast.MergeClause;
import com.arcadedb.opencypher.ast.NodePattern;
import com.arcadedb.opencypher.ast.PathPattern;
import com.arcadedb.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Execution step for MERGE clause.
 * Ensures a pattern exists: matches if present, creates if not.
 * <p>
 * Examples:
 * - MERGE (n:Person {name: 'Alice'}) - finds or creates person
 * - MERGE (a)-[r:KNOWS]->(b) - finds or creates relationship
 * <p>
 * MERGE is an "upsert" operation (update or insert).
 * TODO: Support ON CREATE SET and ON MATCH SET sub-clauses
 */
public class MergeStep extends AbstractExecutionStep {
  private final MergeClause mergeClause;

  public MergeStep(final MergeClause mergeClause, final CommandContext context) {
    super(context);
    this.mergeClause = mergeClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // MERGE is a standalone operation (no previous step required)

    return new ResultSet() {
      private boolean executed = false;
      private Result mergedResult = null;

      @Override
      public boolean hasNext() {
        if (!executed) {
          // Execute MERGE operation
          mergedResult = executeMerge();
          executed = true;
        }
        return mergedResult != null;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Result result = mergedResult;
        mergedResult = null; // Consume the result
        return result;
      }

      @Override
      public void close() {
        MergeStep.this.close();
      }
    };
  }

  /**
   * Executes the MERGE operation: tries to match, creates if not found.
   *
   * @return result containing matched or created elements
   */
  private Result executeMerge() {
    final PathPattern pathPattern = mergeClause.getPathPattern();

    if (pathPattern.isSingleNode()) {
      // Simple node merge: MERGE (n:Person {name: 'Alice'})
      return mergeSingleNode(pathPattern.getFirstNode());
    } else {
      // Path merge with relationships: MERGE (a)-[r:KNOWS]->(b)
      return mergePath(pathPattern);
    }
  }

  /**
   * Merges a single node: finds or creates it.
   *
   * @param nodePattern node pattern to merge
   * @return result containing the merged node
   */
  private Result mergeSingleNode(final NodePattern nodePattern) {
    final ResultInternal result = new ResultInternal();
    final String variable = nodePattern.getVariable() != null ? nodePattern.getVariable() : "n";

    // Try to find existing node
    Vertex existing = findNode(nodePattern);

    if (existing != null) {
      // Node exists - return it
      result.setProperty(variable, existing);
    } else {
      // Node doesn't exist - create it
      final Vertex created = createVertex(nodePattern);
      result.setProperty(variable, created);
    }

    return result;
  }

  /**
   * Merges a path with relationships.
   * For now, this is a simplified implementation that creates if any part doesn't exist.
   *
   * @param pathPattern path pattern to merge
   * @return result containing merged elements
   */
  private Result mergePath(final PathPattern pathPattern) {
    final ResultInternal result = new ResultInternal();
    final List<Vertex> vertices = new ArrayList<>();

    // Merge all vertices in the path
    for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
      final NodePattern nodePattern = pathPattern.getNode(i);
      Vertex vertex = findNode(nodePattern);

      if (vertex == null) {
        // Create vertex if not found
        vertex = createVertex(nodePattern);
      }

      vertices.add(vertex);

      if (nodePattern.getVariable() != null) {
        result.setProperty(nodePattern.getVariable(), vertex);
      }
    }

    // Merge relationships between vertices
    for (int i = 0; i < pathPattern.getRelationshipCount(); i++) {
      final RelationshipPattern relPattern = pathPattern.getRelationship(i);
      final Vertex fromVertex = vertices.get(i);
      final Vertex toVertex = vertices.get(i + 1);

      // Try to find existing relationship
      Edge edge = findEdge(fromVertex, toVertex, relPattern);

      if (edge == null) {
        // Create relationship if not found
        edge = createEdge(fromVertex, toVertex, relPattern);
      }

      if (relPattern.getVariable() != null) {
        result.setProperty(relPattern.getVariable(), edge);
      }
    }

    return result;
  }

  /**
   * Finds a node matching the pattern.
   *
   * @param nodePattern node pattern to find
   * @return matching vertex or null
   */
  private Vertex findNode(final NodePattern nodePattern) {
    if (!nodePattern.hasLabels() || !nodePattern.hasProperties()) {
      // Can't match without label and properties
      return null;
    }

    final String label = nodePattern.getFirstLabel();
    @SuppressWarnings("unchecked")
    final Iterator<Identifiable> iterator = (Iterator<Identifiable>) (Object) context.getDatabase().iterateType(label, true);

    // Find first vertex matching all properties
    while (iterator.hasNext()) {
      final Identifiable identifiable = iterator.next();
      if (identifiable instanceof Vertex) {
        final Vertex vertex = (Vertex) identifiable;
        if (matchesProperties(vertex, nodePattern.getProperties())) {
          return vertex;
        }
      }
    }

    return null;
  }

  /**
   * Finds an edge matching the pattern between two vertices.
   *
   * @param from source vertex
   * @param to   target vertex
   * @param relPattern relationship pattern
   * @return matching edge or null
   */
  private Edge findEdge(final Vertex from, final Vertex to, final RelationshipPattern relPattern) {
    if (!relPattern.hasTypes()) {
      return null;
    }

    final String type = relPattern.getFirstType();
    final Iterator<Edge> edges = from.getEdges(Vertex.DIRECTION.OUT, type).iterator();

    while (edges.hasNext()) {
      final Edge edge = edges.next();
      if (edge.getIn().equals(to)) {
        // Found edge to target vertex
        if (relPattern.hasProperties()) {
          if (matchesProperties(edge, relPattern.getProperties())) {
            return edge;
          }
        } else {
          return edge;
        }
      }
    }

    return null;
  }

  /**
   * Checks if a vertex/edge matches all property filters.
   *
   * @param doc        document to check
   * @param properties expected properties
   * @return true if all properties match
   */
  private boolean matchesProperties(final com.arcadedb.database.Document doc, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
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

      final Object actualValue = doc.get(key);
      if (actualValue == null || !actualValue.equals(expectedValue)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates a vertex from a node pattern.
   */
  private Vertex createVertex(final NodePattern nodePattern) {
    final String label = nodePattern.hasLabels() ? nodePattern.getFirstLabel() : "Vertex";
    final MutableVertex vertex = context.getDatabase().newVertex(label);

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
    final MutableEdge edge = fromVertex.newEdge(type, toVertex);

    if (relPattern.hasProperties()) {
      setProperties(edge, relPattern.getProperties());
    }

    edge.save();
    return edge;
  }

  /**
   * Sets properties on a document from a property map.
   */
  private void setProperties(final MutableDocument document, final Map<String, Object> properties) {
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      Object value = entry.getValue();

      // Handle string literals: remove quotes
      if (value instanceof String) {
        final String strValue = (String) value;
        if (strValue.startsWith("'") && strValue.endsWith("'")) {
          value = strValue.substring(1, strValue.length() - 1);
        } else if (strValue.startsWith("\"") && strValue.endsWith("\"")) {
          value = strValue.substring(1, strValue.length() - 1);
        }
      }

      document.set(key, value);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MERGE");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
