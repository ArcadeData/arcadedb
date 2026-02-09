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

import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for DELETE clause.
 * Deletes vertices and/or edges from the graph.
 * <p>
 * Examples:
 * - MATCH (n:Person {name: 'Alice'}) DELETE n
 * - MATCH (a)-[r:KNOWS]->(b) DELETE r
 * - MATCH (n:Person) WHERE n.age < 18 DETACH DELETE n
 * <p>
 * DETACH DELETE removes all relationships connected to a vertex before deleting it.
 * Regular DELETE will fail if the vertex still has relationships.
 */
public class DeleteStep extends AbstractExecutionStep {
  private final DeleteClause deleteClause;

  public DeleteStep(final DeleteClause deleteClause, final CommandContext context) {
    super(context);
    this.deleteClause = deleteClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("DeleteStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
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

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
        }

        // Process each input result
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();

          // Capture type info for relationships before deletion
          final Map<String, String> relTypes = new java.util.HashMap<>();
          for (final String variable : deleteClause.getVariables()) {
            if (!variable.contains(".") && !variable.contains("[")) {
              final Object obj = inputResult.getProperty(variable);
              if (obj instanceof Edge)
                relTypes.put(variable, ((Edge) obj).getTypeName());
            }
          }

          // Apply DELETE operations to this result
          final Set<String> deletedVars = new HashSet<>();
          applyDeleteOperations(inputResult, deletedVars);

          // Mark deleted variables in the result so subsequent access throws DeletedEntityAccess
          if (!deletedVars.isEmpty() && inputResult instanceof ResultInternal mutableResult) {
            for (final String var : deletedVars) {
              final String relType = relTypes.get(var);
              mutableResult.setProperty(var, relType != null ? new DeletedEntityMarker(relType) : DeletedEntityMarker.INSTANCE);
            }
          }

          // Pass through the result (elements are now deleted)
          buffer.add(inputResult);
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        DeleteStep.this.close();
      }
    };
  }

  /**
   * Applies all DELETE operations to a result.
   *
   * @param result the result containing variables to delete
   */
  private void applyDeleteOperations(final Result result, final Set<String> deletedVars) {
    if (deleteClause == null || deleteClause.isEmpty())
      return;

    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      if (!wasInTransaction)
        context.getDatabase().begin();

      // Collect all delete targets first, then delete edges before vertices
      // to avoid "DeleteConnectedNode" errors when multiple targets share nodes
      final List<Object> allEdges = new ArrayList<>();
      final List<Object> allOther = new ArrayList<>();

      for (final String variable : deleteClause.getVariables()) {
        final Object obj = resolveDeleteTarget(variable, result);
        if (obj == null)
          continue;
        collectDeleteTargets(obj, allEdges, allOther);
        if (!variable.contains(".") && !variable.contains("["))
          deletedVars.add(variable);
      }

      // Delete all edges first, then all other objects (vertices, documents)
      final Set<Object> deleted = new HashSet<>();
      for (final Object edge : allEdges)
        deleteObject(edge, deleted);
      for (final Object other : allOther)
        deleteObject(other, deleted);

      if (!wasInTransaction)
        context.getDatabase().commit();
    } catch (final Exception e) {
      if (!wasInTransaction && context.getDatabase().isTransactionActive())
        context.getDatabase().rollback();
      throw e;
    }
  }

  private void collectDeleteTargets(final Object obj, final List<Object> edges, final List<Object> other) {
    if (obj == null)
      return;
    if (obj instanceof Edge)
      edges.add(obj);
    else if (obj instanceof TraversalPath path) {
      for (final Edge e : path.getEdges())
        edges.add(e);
      for (final Vertex v : path.getVertices())
        other.add(v);
    } else if (obj instanceof List) {
      for (final Object elem : (List<?>) obj)
        collectDeleteTargets(elem, edges, other);
    } else if (obj instanceof Map) {
      for (final Object value : ((Map<?, ?>) obj).values())
        collectDeleteTargets(value, edges, other);
    } else
      other.add(obj);
  }

  private Object resolveDeleteTarget(final String variable, final Result result) {
    // Try direct property lookup first
    Object obj = result.getProperty(variable);
    if (obj != null)
      return obj;

    // Parse the expression into segments for chained access (e.g., "map.key.key2[0]")
    return resolveChainedAccess(variable, result);
  }

  private Object resolveChainedAccess(final String expr, final Result result) {
    // Split into base variable and access chain
    // Parse: varName(.propName)*([index])*
    Object current = null;
    int pos = 0;

    // Find the base variable name (up to first . or [)
    int endOfBase = expr.length();
    for (int i = 0; i < expr.length(); i++) {
      if (expr.charAt(i) == '.' || expr.charAt(i) == '[') {
        endOfBase = i;
        break;
      }
    }
    final String baseName = expr.substring(0, endOfBase);
    current = result.getProperty(baseName);
    if (current == null)
      return null;
    pos = endOfBase;

    // Process access chain
    while (pos < expr.length()) {
      final char ch = expr.charAt(pos);
      if (ch == '.') {
        // Property access
        pos++;
        int propEnd = pos;
        while (propEnd < expr.length() && expr.charAt(propEnd) != '.' && expr.charAt(propEnd) != '[')
          propEnd++;
        final String prop = expr.substring(pos, propEnd);
        pos = propEnd;
        if (current instanceof Map)
          current = ((Map<?, ?>) current).get(prop);
        else if (current instanceof Result)
          current = ((Result) current).getProperty(prop);
        else
          return null;
      } else if (ch == '[') {
        // Index access
        pos++;
        final int closeBracket = expr.indexOf(']', pos);
        if (closeBracket < 0)
          return null;
        String indexStr = expr.substring(pos, closeBracket).trim();
        pos = closeBracket + 1;
        if (current instanceof List) {
          int index;
          if (indexStr.startsWith("$")) {
            final Object paramVal = context.getInputParameters().get(indexStr.substring(1));
            index = paramVal instanceof Number ? ((Number) paramVal).intValue() : Integer.parseInt(paramVal.toString());
          } else {
            index = Integer.parseInt(indexStr);
          }
          final List<?> list = (List<?>) current;
          if (index >= 0 && index < list.size())
            current = list.get(index);
          else
            return null;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
    return current;
  }

  private void deleteObject(final Object obj, final Set<Object> deleted) {
    if (obj == null || deleted.contains(obj))
      return;

    if (obj instanceof Vertex) {
      deleted.add(obj);
      try {
        deleteVertex((Vertex) obj);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
    } else if (obj instanceof Edge) {
      deleted.add(obj);
      try {
        deleteEdge((Edge) obj);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
    } else if (obj instanceof Document) {
      deleted.add(obj);
      try {
        ((Document) obj).delete();
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
    } else if (obj instanceof TraversalPath path) {
      // Delete edges first, then vertices (edges must be removed before non-DETACH vertex delete)
      for (final Edge e : path.getEdges())
        deleteObject(e, deleted);
      for (final Vertex v : path.getVertices())
        deleteObject(v, deleted);
    } else if (obj instanceof List) {
      // Delete edges first, then vertices/documents (edges must be removed before non-DETACH vertex delete)
      final List<?> list = (List<?>) obj;
      for (final Object elem : list)
        if (elem instanceof Edge)
          deleteObject(elem, deleted);
      for (final Object elem : list)
        if (!(elem instanceof Edge))
          deleteObject(elem, deleted);
    } else if (obj instanceof Map) {
      // Delete each value in the map
      for (final Object value : ((Map<?, ?>) obj).values())
        deleteObject(value, deleted);
    }
  }

  /**
   * Deletes a vertex from the graph.
   * If detach is true, deletes all connected relationships first.
   *
   * @param vertex vertex to delete
   */
  private void deleteVertex(final Vertex vertex) {
    if (deleteClause.isDetach()) {
      // DETACH DELETE: Remove all connected relationships first
      deleteAllEdges(vertex);
    } else {
      // Non-DETACH DELETE: check for connected edges
      if (vertex.getEdges(Vertex.DIRECTION.OUT).iterator().hasNext() ||
          vertex.getEdges(Vertex.DIRECTION.IN).iterator().hasNext())
        throw new CommandExecutionException("DeleteConnectedNode: Cannot delete node " + vertex.getIdentity() +
            " because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE");
    }

    vertex.delete();
  }

  /**
   * Deletes all edges connected to a vertex.
   *
   * @param vertex vertex whose edges should be deleted
   */
  private void deleteAllEdges(final Vertex vertex) {
    // Collect all edges to delete (both incoming and outgoing)
    // We collect first to avoid concurrent modification
    final List<Edge> edgesToDelete = new ArrayList<>();

    // Collect outgoing edges
    final Iterator<Edge> outgoing = vertex.getEdges(Vertex.DIRECTION.OUT).iterator();
    while (outgoing.hasNext()) {
      edgesToDelete.add(outgoing.next());
    }

    // Collect incoming edges
    final Iterator<Edge> incoming = vertex.getEdges(Vertex.DIRECTION.IN).iterator();
    while (incoming.hasNext()) {
      edgesToDelete.add(incoming.next());
    }

    // Delete all collected edges
    for (final Edge edge : edgesToDelete) {
      edge.delete();
    }
  }

  /**
   * Deletes an edge from the graph.
   *
   * @param edge edge to delete
   */
  private void deleteEdge(final Edge edge) {
    edge.delete();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    if (deleteClause != null && deleteClause.isDetach()) {
      builder.append("+ DETACH DELETE");
    } else {
      builder.append("+ DELETE");
    }
    if (deleteClause != null && !deleteClause.isEmpty()) {
      builder.append(" (").append(deleteClause.getVariables().size()).append(" items)");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
