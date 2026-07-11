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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

  /**
   * Elements already deleted (and counted) by this DELETE, shared across all input rows of the
   * statement. A single DeleteStep is built per statement execution, so this de-dups an element
   * bound by more than one row - e.g. an undirected relationship matched from both endpoints
   * ({@code MATCH (u)-[r]-() DELETE r}) - so it is removed once and counted once, matching Neo4j.
   * Records equal by RID (see BaseRecord.equals), so different instances of the same edge collapse.
   */
  private final Set<Object> deleted = new HashSet<>();

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
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Capture type info for relationships before deletion
            final Map<String, String> relTypes = new HashMap<>();
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
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
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

  /** CommandContext variable holding the deferred-delete batch within a FOREACH iteration. */
  public static final String DEFERRED_DELETE_BATCH_VAR = "__cypherDeferredDelete__";

  /** Target queued by DeleteStep while running inside a FOREACH iteration. */
  public record DeferredDeleteTarget(Object target, boolean detach) {
  }

  /**
   * Applies all DELETE operations to a result.
   *
   * @param result the result containing variables to delete
   */
  private void applyDeleteOperations(final Result result, final Set<String> deletedVars) {
    if (deleteClause == null || deleteClause.isEmpty())
      return;

    @SuppressWarnings("unchecked")
    final List<DeferredDeleteTarget> deferredBatch =
        (List<DeferredDeleteTarget>) context.getVariable(DEFERRED_DELETE_BATCH_VAR);

    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      if (deferredBatch == null && !wasInTransaction)
        context.getDatabase().begin();

      // Collect all delete targets first, then delete edges before vertices
      // to avoid "DeleteConnectedNode" errors when multiple targets share nodes
      final List<Object> allEdges = new ArrayList<>();
      final List<Object> allOther = new ArrayList<>();

      final List<String> variables = deleteClause.getVariables();
      final List<Expression> expressions = deleteClause.getExpressions();
      for (int i = 0; i < variables.size(); i++) {
        final String variable = variables.get(i);
        final Object obj;
        if (expressions != null && i < expressions.size() && expressions.get(i) != null
            && isFunctionLikeTarget(variable)) {
          obj = expressions.get(i).evaluate(result, context);
        } else {
          obj = resolveDeleteTarget(variable, result);
        }
        if (obj == null)
          continue;
        collectDeleteTargets(obj, allEdges, allOther);
        if (!variable.contains(".") && !variable.contains("[") && !variable.contains("("))
          deletedVars.add(variable);
      }

      if (deferredBatch != null) {
        final boolean detach = deleteClause.isDetach();
        for (final Object edge : allEdges)
          deferredBatch.add(new DeferredDeleteTarget(edge, detach));
        for (final Object other : allOther)
          deferredBatch.add(new DeferredDeleteTarget(other, detach));
        return;
      }

      // Delete all edges first, then all other objects (vertices, documents). The `deleted` set is
      // a step-instance field shared across rows so an element bound by multiple rows is deleted once.
      for (final Object edge : allEdges)
        deleteObject(edge, deleted);
      for (final Object other : allOther)
        deleteObject(other, deleted);

      if (!wasInTransaction)
        context.getDatabase().commit();
    } catch (final Exception e) {
      if (deferredBatch == null && !wasInTransaction && context.getDatabase().isTransactionActive())
        context.getDatabase().rollback();
      throw e;
    }
  }

  /** Flush deferred DELETE batch built up by a FOREACH iteration: edges first, then vertices. */
  public static void flushDeferredDeletes(final CommandContext context, final List<DeferredDeleteTarget> batch) {
    if (batch == null || batch.isEmpty())
      return;
    final List<Object> edges = new ArrayList<>();
    final List<DeferredDeleteTarget> others = new ArrayList<>();
    for (final DeferredDeleteTarget entry : batch) {
      if (entry.target() instanceof Edge)
        edges.add(entry.target());
      else
        others.add(entry);
    }
    final QueryStatistics stats = context.getStatistics();
    final Set<Object> deleted = new HashSet<>();
    for (final Object edge : edges)
      deleteObjectStatic(edge, deleted, stats);
    for (final DeferredDeleteTarget entry : others) {
      final Object target = entry.target();
      if (target instanceof Vertex v && !deleted.contains(v)) {
        if (entry.detach() || hasNoEdges(v)) {
          if (entry.detach())
            // DETACH removes connected relationships; count each one not already deleted so
            // relationships-deleted matches Neo4j (deleteObjectStatic skips any already in `deleted`).
            for (final Edge edge : collectConnectedEdges(v))
              deleteObjectStatic(edge, deleted, stats);
          v.delete();
          stats.incNodesDeleted();
          deleted.add(v);
        } else {
          throw new CommandExecutionException("DeleteConnectedNode: Cannot delete node "
              + v.getIdentity() + " because it still has relationships. To delete this node, you must first delete"
              + " its relationships, or use DETACH DELETE");
        }
      } else {
        deleteObjectStatic(target, deleted, stats);
      }
    }
    batch.clear();
  }

  private static boolean hasNoEdges(final Vertex v) {
    try {
      return v.countEdges(Vertex.DIRECTION.BOTH) == 0L;
    } catch (final RecordNotFoundException ignored) {
      // vertex was already removed by the batch flush - treat as isolated
      return true;
    }
  }

  private static List<Edge> collectConnectedEdges(final Vertex v) {
    final List<Edge> edges = new ArrayList<>();
    try {
      for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT))
        edges.add(e);
      for (final Edge e : v.getEdges(Vertex.DIRECTION.IN))
        edges.add(e);
    } catch (final RecordNotFoundException ignored) {
      // vertex was already removed by the batch flush - return what was collected so far
    }
    return edges;
  }

  private static void deleteObjectStatic(final Object obj, final Set<Object> deleted, final QueryStatistics stats) {
    if (obj == null || deleted.contains(obj))
      return;
    try {
      if (obj instanceof Edge e) {
        e.delete();
        stats.incRelationshipsDeleted();
      } else if (obj instanceof Vertex v) {
        v.delete();
        stats.incNodesDeleted();
      } else if (obj instanceof Document)
        ((Document) obj).delete();
      deleted.add(obj);
    } catch (final RecordNotFoundException ignored) {
      deleted.add(obj);
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

  /** True when the textual target looks like a function call - needs Expression-based evaluation. */
  private static boolean isFunctionLikeTarget(final String variable) {
    final int paren = variable.indexOf('(');
    if (paren <= 0)
      return false;
    // Ensure the parenthesis is not preceded by a dot (e.g. a.b() would still be unusual).
    // A leading identifier followed by '(' is the marker we care about.
    for (int i = 0; i < paren; i++) {
      final char c = variable.charAt(i);
      if (!Character.isJavaIdentifierPart(c))
        return false;
    }
    return true;
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
        deleteVertex((Vertex) obj, deleted);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
    } else if (obj instanceof Edge) {
      deleted.add(obj);
      try {
        deleteEdge((Edge) obj, deleted);
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
   * @param vertex  vertex to delete
   * @param deleted shared set of already-deleted objects, so a relationship removed both by DETACH
   *                and by an explicit edge delete in the same statement is counted only once
   */
  private void deleteVertex(final Vertex vertex, final Set<Object> deleted) {
    if (deleteClause.isDetach()) {
      // DETACH DELETE: Remove all connected relationships first
      deleteAllEdges(vertex, deleted);
    } else {
      // Non-DETACH DELETE: check for connected edges
      if (vertex.getEdges(Vertex.DIRECTION.OUT).iterator().hasNext() ||
          vertex.getEdges(Vertex.DIRECTION.IN).iterator().hasNext())
        throw new CommandExecutionException("DeleteConnectedNode: Cannot delete node " + vertex.getIdentity() +
            " because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE");
    }

    vertex.delete();
    context.getStatistics().incNodesDeleted();
  }

  /**
   * Deletes all edges connected to a vertex.
   *
   * @param vertex  vertex whose edges should be deleted
   * @param deleted shared set of already-deleted objects; edges already removed and counted earlier
   *                in the same statement (by an explicit DELETE or another vertex's DETACH pass) are
   *                skipped here
   */
  private void deleteAllEdges(final Vertex vertex, final Set<Object> deleted) {
    // Collect connected edges in both directions, de-duplicating self-loops (which appear in both
    // OUT and IN) so a self-loop relationship is deleted and counted exactly once.
    final List<Edge> edgesToDelete = new ArrayList<>();
    final Set<RID> seen = new HashSet<>();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);

    final QueryStatistics stats = context.getStatistics();
    for (final Edge edge : edgesToDelete) {
      // Skip any relationship already removed and counted by an explicit DELETE in the same statement.
      if (deleted.contains(edge))
        continue;
      try {
        edge.delete();
        stats.incRelationshipsDeleted();
        deleted.add(edge);
      } catch (final RecordNotFoundException ignored) {
        // already removed (e.g. a shared edge deleted when the other endpoint was detached) - do not count again
        deleted.add(edge);
      }
    }
  }

  /**
   * Deletes an edge from the graph.
   *
   * @param edge    edge to delete
   * @param deleted shared set of already-deleted objects, updated so a later DETACH pass does not
   *                recount this edge
   */
  private void deleteEdge(final Edge edge, final Set<Object> deleted) {
    edge.delete();
    context.getStatistics().incRelationshipsDeleted();
    deleted.add(edge);
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
