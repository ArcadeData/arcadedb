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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
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
   */
  private final DeletedSoFar deleted = new DeletedSoFar();

  /**
   * True once the occurrences of every lightweight edge the rest of the statement will delete are resolved: the first
   * row that deletes a lightweight edge with a twin reads the remaining rows in (when they are not already) and
   * resolves them, before its own delete changes the list they were read from (issue #10008). The rows are otherwise
   * pipelined, and a row read after the first twin is gone numbers the survivor as the first: a dedup keyed on the
   * occurrence would skip it as already deleted.
   */
  private boolean twinsPinned = false;

  /**
   * The targets of the rows {@link #twinsPinned} read in, evaluated once when their edges were numbered and taken back
   * when the row is processed: a target computed by an expression is not evaluated twice, so the edge deleted is the edge
   * that was numbered.
   */
  private Map<Result, RowTargets> pinnedTargets;

  /** What a row deletes: its relationships, everything else, and the plain variables it names. */
  private record RowTargets(List<Object> edges, List<Object> other, Set<String> variables) {
  }

  /**
   * Vertices of a non-DETACH DELETE that still had a relationship when their row was processed. Another row of the
   * same statement can delete that relationship (a node bound to several relationships by {@code MATCH (n)-[r]-()
   * DELETE r, n}), so the {@code DeleteConnectedNode} check runs once every row is processed, not per row (#8997). Only
   * the RIDs are kept: a vertex is reloaded when it is checked. A consumer that stops pulling before the last row (a
   * LIMIT downstream) leaves the pending vertices undeleted, as it leaves the rows it never pulled unprocessed.
   */
  private final Set<RID> pendingVertices = new LinkedHashSet<>();

  /**
   * True while the transaction this step opened itself (no outer one) is kept open across rows, because a vertex is
   * pending: the rows deleting its relationships and the final check are one unit, so a vertex still connected at the
   * end rolls them all back instead of leaving the relationships deleted (#8997). Rows processed before the first pending
   * vertex are committed one by one, as before. Every exit from {@code fetchMore} while this is true either commits at the
   * end of the statement or leaves the rollback to {@code close()}.
   */
  private boolean holdingTransaction = false;

  /**
   * When true, the whole upstream row set is read to completion before the first DELETE is applied.
   * Needed when the MATCH feeding this DELETE can let a later row observe what an earlier row's DELETE
   * already removed, which happens in two independent shapes:
   * <ul>
   *   <li>disconnected path patterns (see
   *       {@link com.arcadedb.query.opencypher.ast.MatchClause#hasDisconnectedPathPatterns()}): such a
   *       MATCH re-enumerates one component per row of the other, so it can bind the same underlying
   *       vertex/edge across more than one output row, and deleting it while a later row is still being
   *       produced makes that row dereference an already-removed record (issue #6491);</li>
   *   <li>a variable-length or quantified relationship (see
   *       {@link com.arcadedb.query.opencypher.ast.MatchClause#hasVariableLengthRelationships(java.util.List)}):
   *       its depth-first traverser keeps edge-segment cursors open across the rows it is still
   *       producing, so a {@code DETACH DELETE} of a bound node unlinks the edge chunks that same cursor
   *       is about to follow and the next {@code hasNext()} dereferences a removed segment record
   *       (issue #7023).</li>
   * </ul>
   * Ordinary fixed-length connected patterns bind each row's variables independently and keep no cursor
   * open across rows, so they are left on the cheaper streaming path.
   * <p>
   * The cost of this path is the whole upstream row set held in memory at once (see
   * {@code materializedInput} below) rather than streamed - acceptable for the shapes this guards, but a
   * disconnected pattern deliberately combined with a very large cartesian product, or an unbounded
   * {@code -[*]->} over a dense graph, would still pay that memory cost in full.
   */
  private final boolean eagerMaterialize;

  // THE ROWS MATERIALIZED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591). ON
  // THE STEP: THE CLOSE() OF A QUERY REACHES THE STEPS, NOT THEIR RESULT SETS
  private OperationHeapLimit heapLimit;

  public DeleteStep(final DeleteClause deleteClause, final CommandContext context) {
    this(deleteClause, context, false);
  }

  public DeleteStep(final DeleteClause deleteClause, final CommandContext context, final boolean eagerMaterialize) {
    super(context);
    this.deleteClause = deleteClause;
    this.eagerMaterialize = eagerMaterialize;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("DeleteStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private List<Result> materializedInput = null;
      private int materializedIndex = 0;
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
        try {
          fetchMore(nRecords);
        } catch (final RuntimeException e) {
          // a failure from the upstream (or a row) must not leave the step's own transaction open
          abortHeldTransaction();
          throw e;
        }
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private boolean hasMoreInput() {
        return materializedInput != null ? materializedIndex < materializedInput.size() : prevResults.hasNext();
      }

      private Result nextInput() {
        return materializedInput != null ? materializedInput.get(materializedIndex++) : prevResults.next();
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
          if (eagerMaterialize) {
            // Read the whole upstream MATCH before the first DELETE runs: a disconnected-pattern MATCH
            // (e.g. a self-loop cross-joined with an unrelated pattern) can bind the very same vertex or
            // edge from more than one output row, and deleting it while a later row is still being
            // produced makes that row dereference an already-removed record (issue #6491).
            final long eagerBegin = context.isProfiling() ? System.nanoTime() : 0;
            materializedInput = new ArrayList<>();
            heapLimit = OperationHeapLimit.of(context, "DELETE of a disconnected pattern");
            try {
              while (prevResults.hasNext()) {
                final Result row = prevResults.next();
                materializedInput.add(row);
                heapLimit.add(materializedInput.size(), row);
              }
            } catch (final RuntimeException e) {
              materializedInput = null;
              releaseHeap();
              throw e;
            }
            if (context.isProfiling())
              cost += System.nanoTime() - eagerBegin;
          }
        }

        // Process each input result
        while (buffer.size() < n && hasMoreInput()) {
          final Result inputResult = nextInput();
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
            applyDeleteOperations(inputResult, deletedVars, this::pinRemainingRows);

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

        if (!hasMoreInput()) {
          finished = true;
          // THE ROWS ARE ALL PROCESSED: THE VERTICES LEFT PENDING MUST BE FREE OF RELATIONSHIPS BY NOW
          flushPendingVertices();
          // EVERY INPUT ROW WAS PROCESSED: THE MATERIALIZED INPUT IS NOT NEEDED ANYMORE (THE UPSTREAM IS DRAINED)
          if (materializedInput != null) {
            materializedInput = null;
            releaseHeap();
          }
        }
      }

      /**
       * Reads every remaining row in, when the upstream is still streaming, and resolves the occurrence of every
       * lightweight edge they delete, before the first twin is deleted (see {@link #twinsPinned}).
       */
      private void pinRemainingRows() {
        if (twinsPinned)
          return;
        twinsPinned = true;
        if (materializedInput == null) {
          final List<Result> remaining = new ArrayList<>();
          heapLimit = OperationHeapLimit.of(context, "DELETE of parallel lightweight edges");
          try {
            while (prevResults.hasNext()) {
              final Result row = prevResults.next();
              remaining.add(row);
              heapLimit.add(remaining.size(), row);
            }
          } catch (final RuntimeException e) {
            releaseHeap();
            throw e;
          }
          materializedInput = remaining;
          materializedIndex = 0;
        }
        pinnedTargets = new IdentityHashMap<>();
        for (int i = materializedIndex; i < materializedInput.size(); i++) {
          final Result row = materializedInput.get(i);
          final RowTargets targets = new RowTargets(new ArrayList<>(), new ArrayList<>(), new HashSet<>());
          collectRowTargets(row, targets.edges(), targets.other(), targets.variables());
          pinOccurrences(targets.edges());
          pinnedTargets.put(row, targets);
        }
      }

      @Override
      public void close() {
        DeleteStep.this.close();
      }
    };
  }

  /**
   * Resolves which twin each lightweight edge of {@code edges} is, while the edge lists still hold every one of them:
   * {@link LightEdgeRID#getOccurrence()} resolves it once and caches it on the RID, so a delete that renumbers the list
   * afterwards does not change it. EVERY edge has to be resolved: this loop must never stop at the first twin it meets.
   *
   * @return true when one of them has a twin, i.e. when deleting it changes how the others are numbered
   */
  private static boolean pinOccurrences(final List<?> edges) {
    boolean twinned = false;
    for (final Object edge : edges)
      if (edge instanceof Edge e && e.getIdentity() instanceof LightEdgeRID light) {
        light.getOccurrence();
        if (light.getTwinCount() > 1)
          twinned = true;
      }
    return twinned;
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
  private void applyDeleteOperations(final Result result, final Set<String> deletedVars, final Runnable pinRemainingRows) {
    if (deleteClause == null || deleteClause.isEmpty())
      return;

    @SuppressWarnings("unchecked")
    final List<DeferredDeleteTarget> deferredBatch =
        (List<DeferredDeleteTarget>) context.getVariable(DEFERRED_DELETE_BATCH_VAR);

    // the transaction kept open by an earlier row of this statement is this step's own, not an outer one
    final boolean wasInTransaction = context.getDatabase().isTransactionActive() && !holdingTransaction;

    try {
      if (deferredBatch == null && !context.getDatabase().isTransactionActive())
        context.getDatabase().begin();

      // Collect all delete targets first, then delete edges before vertices
      // to avoid "DeleteConnectedNode" errors when multiple targets share nodes
      final List<Object> allEdges;
      final List<Object> allOther;
      final RowTargets pinned = pinnedTargets != null ? pinnedTargets.remove(result) : null;
      if (pinned != null) {
        allEdges = pinned.edges();
        allOther = pinned.other();
        deletedVars.addAll(pinned.variables());
      } else {
        allEdges = new ArrayList<>();
        allOther = new ArrayList<>();
        collectRowTargets(result, allEdges, allOther, deletedVars);
      }

      if (deferredBatch != null) {
        final boolean detach = deleteClause.isDetach();
        for (final Object edge : allEdges)
          deferredBatch.add(new DeferredDeleteTarget(edge, detach));
        for (final Object other : allOther)
          deferredBatch.add(new DeferredDeleteTarget(other, detach));
        return;
      }

      // The lightweight edges of the row are numbered before anything is deleted; the first one with a twin numbers
      // the rest of the statement's too, while their lists still hold every twin (#10008)
      if (pinOccurrences(allEdges) && pinRemainingRows != null)
        pinRemainingRows.run();

      // Delete all edges first, then all other objects (vertices, documents). The `deleted` set is
      // a step-instance field shared across rows so an element bound by multiple rows is deleted once.
      for (final Object edge : allEdges)
        deleteObject(edge);
      for (final Object other : allOther)
        deleteObject(other);

      if (!wasInTransaction) {
        // a pending vertex keeps the transaction open until the statement ends
        holdingTransaction = !pendingVertices.isEmpty();
        if (!holdingTransaction)
          context.getDatabase().commit();
      }
    } catch (final Exception e) {
      if (deferredBatch == null && !wasInTransaction && context.getDatabase().isTransactionActive())
        context.getDatabase().rollback();
      holdingTransaction = false;
      pendingVertices.clear();
      throw e;
    }
  }

  /**
   * Evaluates the targets of the DELETE on {@code row}: relationships into {@code edges}, everything else into
   * {@code other}, and the plain variables deleted into {@code deletedVars} when not null.
   */
  private void collectRowTargets(final Result row, final List<Object> edges, final List<Object> other,
      final Set<String> deletedVars) {
    final List<String> variables = deleteClause.getVariables();
    final List<Expression> expressions = deleteClause.getExpressions();
    for (int i = 0; i < variables.size(); i++) {
      final String variable = variables.get(i);
      final Object obj;
      if (expressions != null && i < expressions.size() && expressions.get(i) != null
          && isFunctionLikeTarget(variable)) {
        obj = expressions.get(i).evaluate(row, context);
      } else {
        obj = resolveDeleteTarget(variable, row);
      }
      if (obj == null)
        continue;
      collectDeleteTargets(obj, edges, other);
      if (deletedVars != null && !variable.contains(".") && !variable.contains("[") && !variable.contains("("))
        deletedVars.add(variable);
    }
  }

  /**
   * Flush deferred DELETE batch built up by a FOREACH iteration: edges first, then vertices.
   *
   * @param deleted what the statement deleted so far, kept by the FOREACH across its iterations
   */
  public static void flushDeferredDeletes(final CommandContext context, final List<DeferredDeleteTarget> batch,
      final DeletedSoFar deleted) {
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
    // every twin is numbered before the first delete changes the lists (#10008)
    pinOccurrences(edges);
    for (final Object edge : edges)
      deleteObjectStatic(edge, deleted, stats);
    for (final DeferredDeleteTarget entry : others) {
      final Object target = entry.target();
      if (target instanceof Vertex v && !deleted.contains(v)) {
        if (entry.detach() || hasNoEdges(v)) {
          if (entry.detach()) {
            // DETACH removes connected relationships; count each one not already deleted so
            // relationships-deleted matches Neo4j (the ones already deleted are not in the lists any more).
            // A self-loop is in both lists: read once, by its identity, every edge collected (and numbered) before the
            // first of them is deleted (#10008)
            final EdgeIdentitySet seen = new EdgeIdentitySet();
            final List<Edge> connected = new ArrayList<>();
            for (final Edge edge : collectConnectedEdges(v))
              if (seen.add(edge.getIdentity()))
                connected.add(edge);
            for (final Edge edge : connected)
              if (deleted.claimJustRead(edge))
                deleteEdgeStatic(edge, stats);
          }
          try {
            v.delete();
            stats.incNodesDeleted();
          } catch (final RecordNotFoundException ignored) {
            // Already removed, by a flush this `deleted` did not see: it lives as long as its ForeachStep, so a
            // vertex deleted under another ForeachStep of the statement (a nested FOREACH runs on a plan of its
            // own per iteration) is not in it. Only ForeachStep's eager materialization (issue #6491) guarantees
            // every row still sees a consistent read; distinct rows can still legitimately bind the very same
            // vertex (a disconnected-pattern MATCH's cross join), and Neo4j treats deleting an already-deleted
            // node as a no-op, not an error.
          }
          deleted.vertexDeleted(v);
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
      // THE INCOMING SIDE OF A UNIDIRECTIONAL TYPE IS NOT STORED ON THE VERTEX: ASKED THROUGH THE SAME LOOKUP THE VERTEX DELETE USES
      return v.countEdges(Vertex.DIRECTION.BOTH) == 0L && incomingUnidirectionalEdges(v).isEmpty();
    } catch (final RecordNotFoundException ignored) {
      // vertex was already removed by the batch flush - treat as isolated
      return true;
    }
  }

  private static List<Edge> incomingUnidirectionalEdges(final Vertex v) {
    return IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) v.getDatabase(), v.getIdentity());
  }

  private static List<Edge> collectConnectedEdges(final Vertex v) {
    final List<Edge> edges = new ArrayList<>();
    try {
      for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT))
        edges.add(e);
      for (final Edge e : v.getEdges(Vertex.DIRECTION.IN))
        edges.add(e);
      edges.addAll(incomingUnidirectionalEdges(v));
    } catch (final RecordNotFoundException ignored) {
      // vertex was already removed by the batch flush - return what was collected so far
    }
    return edges;
  }

  private static void deleteObjectStatic(final Object obj, final DeletedSoFar deleted, final QueryStatistics stats) {
    if (obj == null)
      return;
    if (obj instanceof Edge e) {
      if (deleted.claim(e))
        deleteEdgeStatic(e, stats);
      return;
    }
    if (deleted.contains(obj))
      return;
    try {
      if (obj instanceof Vertex v) {
        v.delete();
        stats.incNodesDeleted();
        deleted.vertexDeleted(v);
      } else if (obj instanceof Document)
        ((Document) obj).delete();
      deleted.add(obj);
    } catch (final RecordNotFoundException ignored) {
      deleted.add(obj);
    }
  }

  private static void deleteEdgeStatic(final Edge edge, final QueryStatistics stats) {
    try {
      edge.delete();
      stats.incRelationshipsDeleted();
    } catch (final RecordNotFoundException ignored) {
      // already removed
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

  private void deleteObject(final Object obj) {
    if (obj == null || (!(obj instanceof Edge) && deleted.contains(obj)))
      return;

    if (obj instanceof Vertex vertex) {
      if (!deleteClause.isDetach() && hasEdges(vertex)) {
        // a later row of the statement can still delete the relationships: checked when every row is processed (#8997)
        pendingVertices.add(vertex.getIdentity());
        return;
      }
      deleted.add(obj);
      try {
        // the relationships were just looked up above: not asked again
        deleteVertex(vertex, true);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
      deleted.vertexDeleted(vertex);
    } else if (obj instanceof Edge edge) {
      if (!deleted.claim(edge))
        return;
      try {
        deleteEdge(edge);
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
        deleteObject(e);
      for (final Vertex v : path.getVertices())
        deleteObject(v);
    } else if (obj instanceof List) {
      // Delete edges first, then vertices/documents (edges must be removed before non-DETACH vertex delete)
      final List<?> list = (List<?>) obj;
      for (final Object elem : list)
        if (elem instanceof Edge)
          deleteObject(elem);
      for (final Object elem : list)
        if (!(elem instanceof Edge))
          deleteObject(elem);
    } else if (obj instanceof Map) {
      // Delete each value in the map
      for (final Object value : ((Map<?, ?>) obj).values())
        deleteObject(value);
    }
  }

  private static boolean hasEdges(final Vertex vertex) {
    return vertex.getEdges(Vertex.DIRECTION.OUT).iterator().hasNext() || vertex.getEdges(Vertex.DIRECTION.IN).iterator().hasNext()
        || !incomingUnidirectionalEdges(vertex).isEmpty();
  }

  /**
   * Deletes the vertices left pending by the rows of a non-DETACH DELETE. One that still has a relationship after every
   * row was processed fails with {@code DeleteConnectedNode}.
   */
  private void flushPendingVertices() {
    if (pendingVertices.isEmpty())
      return;

    final List<RID> pending = new ArrayList<>(pendingVertices);
    pendingVertices.clear();

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final boolean wasInTransaction = database.isTransactionActive() && !holdingTransaction;
    try {
      if (!database.isTransactionActive())
        database.begin();
      for (final RID rid : pending) {
        final Vertex current;
        try {
          current = rid.asVertex();
        } catch (final RecordNotFoundException e) {
          // already deleted
          continue;
        }
        // records equal by RID (see BaseRecord.equals), so a freshly loaded vertex matches the ones stored earlier
        if (deleted.contains(current))
          continue;
        deleted.add(current);
        deleteVertex(current, false);
        deleted.vertexDeleted(current);
      }
      if (!wasInTransaction)
        database.commit();
      holdingTransaction = false;
    } catch (final RuntimeException e) {
      if (!wasInTransaction && database.isTransactionActive())
        database.rollback();
      holdingTransaction = false;
      throw e;
    }
  }

  /**
   * Deletes a vertex from the graph.
   * If detach is true, deletes all connected relationships first.
   *
   * A relationship removed both by DETACH and by an explicit edge delete in the same statement is counted only once,
   * through {@link #deleted}.
   *
   * @param vertex       vertex to delete
   * @param edgesChecked true when the caller just verified the vertex has no relationship (non-DETACH)
   */
  private void deleteVertex(final Vertex vertex, final boolean edgesChecked) {
    if (deleteClause.isDetach()) {
      // DETACH DELETE: Remove all connected relationships first
      deleteAllEdges(vertex);
    } else if (!edgesChecked) {
      // Non-DETACH DELETE: check for connected edges
      if (hasEdges(vertex))
        throw new CommandExecutionException("DeleteConnectedNode: Cannot delete node " + vertex.getIdentity() +
            " because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE");
    }

    vertex.delete();
    context.getStatistics().incNodesDeleted();
  }

  /**
   * Deletes all edges connected to a vertex.
   *
   * Edges already removed and counted earlier in the same statement (by an explicit DELETE or another vertex's DETACH
   * pass, see {@link #deleted}) are not in its lists any more; the ones read here exist, and are deleted.
   *
   * @param vertex vertex whose edges should be deleted
   */
  private void deleteAllEdges(final Vertex vertex) {
    // Collect connected edges in both directions, de-duplicating self-loops (which appear in both
    // OUT and IN) so a self-loop relationship is deleted and counted exactly once.
    // An EdgeIdentitySet, not a set of RIDs: parallel lightweight edges of one type over the same pair share their RID,
    // and a self-loop's twins are as many relationships as there are twins (#10008)
    final List<Edge> edgesToDelete = new ArrayList<>();
    final EdgeIdentitySet seen = new EdgeIdentitySet();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);
    for (final Edge edge : incomingUnidirectionalEdges(vertex))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);
    final QueryStatistics stats = context.getStatistics();
    for (final Edge edge : edgesToDelete) {
      // Skip any relationship already removed and counted by an explicit DELETE in the same statement.
      if (!deleted.claimJustRead(edge))
        continue;
      try {
        edge.delete();
        stats.incRelationshipsDeleted();
      } catch (final RecordNotFoundException ignored) {
        // already removed (e.g. a shared edge deleted when the other endpoint was detached) - do not count again
      }
    }
  }

  /**
   * Deletes an edge from the graph.
   *
   * @param edge edge to delete, already claimed in {@link #deleted} so a later DETACH pass does not recount it
   */
  private void deleteEdge(final Edge edge) {
    edge.delete();
    context.getStatistics().incRelationshipsDeleted();
  }

  /**
   * Rolls back the transaction this step kept open for a pending vertex. A consumer that stops pulling before the last
   * row (a LIMIT downstream over more rows than one batch) never reaches the final check, so the statement did not
   * complete: the rows already applied from the first pending vertex onwards are rolled back with it, and the statistics
   * counters still show them.
   */
  private void abortHeldTransaction() {
    if (!holdingTransaction)
      return;
    holdingTransaction = false;
    pendingVertices.clear();
    final Database database = context.getDatabase();
    if (database.isTransactionActive())
      database.rollback();
  }

  private void releaseHeap() {
    if (heapLimit != null)
      heapLimit.release();
  }

  @Override
  public void close() {
    abortHeldTransaction();
    releaseHeap();
    super.close();
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
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }

  /**
   * What one statement has deleted so far: shared by every row of a DELETE, and by every iteration of a FOREACH, so an
   * element bound more than once is removed once and counted once.
   * <p>
   * Relationships are kept in an {@link EdgeIdentitySet} rather than a set of records: two parallel lightweight edges of
   * one type over the same ordered pair share their RID and are still two relationships, so a RID-keyed set deleted one
   * of them and skipped the others as "already deleted" (issue #10008). Telling them apart needs the occurrence of each,
   * resolved by walking the edge list, so it is resolved while the list still holds every twin ({@link #pinOccurrences}).
   * The edges a DETACH reads off a vertex are read after the deletes before it, so their lightweight twins are numbered
   * anew and are never compared with the ones already deleted: they exist, so they are deleted. And an edge whose
   * endpoint the statement already deleted went with it.
   */
  public static final class DeletedSoFar {
    // VERTICES AND DOCUMENTS: RECORDS EQUAL BY RID (SEE BaseRecord.equals), SO INSTANCES OF ONE RECORD COLLAPSE
    private final Set<Object>     records  = new HashSet<>();
    private final EdgeIdentitySet edges    = new EdgeIdentitySet();
    private final RidHashSet      vertices = new RidHashSet();

    /**
     * @return true when {@code edge} is still to delete, recording it as deleted: neither deleted already, nor taken
     * away with an endpoint the statement deleted
     */
    boolean claim(final Edge edge) {
      try {
        if (vertices.contains(edge.getOut()) || vertices.contains(edge.getIn()))
          return false;
      } catch (final RecordNotFoundException e) {
        // THE EDGE RECORD IS GONE ALREADY
        return false;
      }
      return edges.add(edge.getIdentity());
    }

    /**
     * {@link #claim} for an edge a DETACH has just read off its vertex: it exists now, so a lightweight one is deleted
     * whatever an occurrence numbered before an earlier delete of the statement says. A record-backed one cannot be in
     * the list once deleted, and is claimed as usual.
     * <p>
     * PRECONDITION: {@code edge} was read off the live edge list right before this call, with no delete in between. An
     * edge read earlier (a row, a collected list) goes through {@link #claim}: this method would delete and count a
     * lightweight one twice.
     */
    boolean claimJustRead(final Edge edge) {
      return edges.add(edge.getIdentity()) || edge.getIdentity() instanceof LightEdgeRID;
    }

    boolean contains(final Object record) {
      return records.contains(record);
    }

    void add(final Object record) {
      records.add(record);
    }

    void vertexDeleted(final Vertex vertex) {
      records.add(vertex);
      vertices.add(vertex.getIdentity());
    }
  }
}
