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
package com.arcadedb.query.opencypher.procedures.merge;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.EdgeType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: merge.relationship(startNode, relType, matchProps, createProps, endNode)
 * <p>
 * Merges a relationship between two nodes. If a relationship with the specified type
 * and matching properties exists, it returns the existing relationship. Otherwise,
 * it creates a new relationship with both matchProps and createProps.
 * </p>
 * <p>
 * This is the key use case from issue #3256:
 * <pre>
 * UNWIND $batch AS row
 * MATCH (a), (b) WHERE elementId(a) = row.source_id AND elementId(b) = row.target_id
 * CALL merge.relationship(a, row.rel_type, {}, row.props, b)
 * YIELD rel
 * RETURN elementId(rel) as id
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MergeRelationship implements CypherProcedure {
  public static final String NAME = "merge.relationship";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 5;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Merges a relationship between two nodes. Creates the relationship if it doesn't exist.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("rel");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    // Extract arguments
    final Vertex startNode = extractVertex(args[0], "startNode");
    final String relType = extractString(args[1], "relType");
    final Map<String, Object> matchProps = extractMap(args[2], "matchProps");
    final Map<String, Object> createProps = extractMap(args[3], "createProps");
    final Vertex endNode = extractVertex(args[4], "endNode");

    final Database database = context.getDatabase();

    // Ensure edge type exists
    if (!database.getSchema().existsType(relType)) {
      database.getSchema().createEdgeType(relType);
    }

    // Try to find existing relationship matching the criteria
    Edge existingEdge = findMatchingEdge(startNode, endNode, relType, matchProps);

    if (existingEdge != null) {
      // Return existing relationship
      return createResultStream(existingEdge);
    }

    // Create new relationship with both matchProps and createProps
    // Note: using bidirectional=true so the edge can be traversed from both ends
    final MutableEdge newEdge = startNode.newEdge(relType, endNode, Boolean.TRUE, new Object[0]);

    // Apply matchProps
    if (matchProps != null) {
      for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
        newEdge.set(entry.getKey(), entry.getValue());
      }
    }

    // Apply createProps
    if (createProps != null) {
      for (final Map.Entry<String, Object> entry : createProps.entrySet()) {
        newEdge.set(entry.getKey(), entry.getValue());
      }
    }

    newEdge.save();

    return createResultStream(newEdge);
  }

  /**
   * Finds an existing edge between startNode and endNode with the given type
   * that matches all the specified properties.
   */
  private Edge findMatchingEdge(final Vertex startNode, final Vertex endNode,
                                 final String relType, final Map<String, Object> matchProps) {
    // Get outgoing edges of the specified type
    final Iterator<Edge> edges = startNode.getEdges(Vertex.DIRECTION.OUT, relType).iterator();

    while (edges.hasNext()) {
      final Edge edge = edges.next();

      // Check if edge connects to the endNode
      if (!edge.getIn().equals(endNode.getIdentity())) {
        continue;
      }

      // Check if all matchProps match
      if (matchProps == null || matchProps.isEmpty()) {
        return edge; // No props to match, found a match
      }

      boolean allMatch = true;
      for (final Map.Entry<String, Object> entry : matchProps.entrySet()) {
        final Object edgeValue = edge.get(entry.getKey());
        final Object matchValue = entry.getValue();

        if (matchValue == null) {
          if (edgeValue != null) {
            allMatch = false;
            break;
          }
        } else if (!matchValue.equals(edgeValue)) {
          allMatch = false;
          break;
        }
      }

      if (allMatch) {
        return edge;
      }
    }

    return null;
  }

  private Stream<Result> createResultStream(final Edge edge) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("rel", edge);
    return Stream.of(result);
  }

  private Vertex extractVertex(final Object arg, final String paramName) {
    if (arg == null) {
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    }
    if (!(arg instanceof Vertex)) {
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a node, got " + arg.getClass().getSimpleName());
    }
    return (Vertex) arg;
  }

  private String extractString(final Object arg, final String paramName) {
    if (arg == null) {
      throw new IllegalArgumentException(getName() + "(): " + paramName + " cannot be null");
    }
    return arg.toString();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMap(final Object arg, final String paramName) {
    if (arg == null) {
      return null;
    }
    if (!(arg instanceof Map)) {
      throw new IllegalArgumentException(
          getName() + "(): " + paramName + " must be a map, got " + arg.getClass().getSimpleName());
    }
    return (Map<String, Object>) arg;
  }
}
