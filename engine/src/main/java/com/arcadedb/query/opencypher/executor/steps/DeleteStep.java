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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ObjectInUseException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.executor.DeletedReferences;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.LongRangeList;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Collections;
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
   *       (issue #7023);</li>
   *   <li>a {@code DETACH DELETE} behind a fixed-length relationship pattern (see
   *       {@code EagerBarriers.detachDeleteNeedsEagerRead()}): the MATCH walks the edge list of a node while its rows are
   *       still being produced, and the first row's DETACH removes the relationships of that list the walk has not
   *       reached, so the rows they would have produced are lost without an error (issue #12254);</li>
   *   <li>a DELETE of a relationship that an expansion starting after the delete reads again (see
   *       {@code EagerBarriers.relationshipsMayBeReadAgain()}): a second relationship pattern, a relationship without a
   *       direction, or an UNWIND, LOAD CSV, CALL or earlier segment ahead of the pattern, so the expansion of a later row
   *       no longer finds the relationship an earlier row deleted and the rows it would have produced are lost without an
   *       error (issue #12521).</li>
   * </ul>
   * A fixed-length connected pattern read by a DELETE without DETACH, or by a DETACH DELETE that removes only the
   * relationship the walk stands on, binds each row's variables independently and loses nothing to the rows before it, as
   * long as it is one relationship of one direction fed by the scan of the MATCH alone, which reaches each relationship once:
   * it is left on the cheaper streaming path.
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

  /**
   * Lines the rows this step serves up with what the statement deleted: the entity the clause deleted is the marker in its
   * own column already, and the rest of the row - the other aliases of it, and the lists, maps and paths that hold it - is
   * handled by {@link DeletedReferences} when the row is served (issue #12548).
   */
  private final DeletedReferences rowReferences = new DeletedReferences(this::isDeletedOrPending);

  /**
   * Inside a FOREACH body the DELETE runs when the iteration ends (see {@link #DEFERRED_DELETE_BATCH_VAR}), so what it will
   * delete is noted in the {@code DeletedSoFar} of the FOREACH hierarchy ({@link ForeachStep#FOREACH_DELETED_SO_FAR_VAR}), which
   * is also what the rows are read through here: the clauses after it in the body read the targets as deleted already, as they
   * read the variable it deleted, and the rows are wrapped once with the check the FOREACH uses, not once per iteration.
   * Null outside a FOREACH body.
   */
  private DeletedSoFar        foreachDeleted;
  private DeletedReferences   foreachReferences;

  // The lightweight edges the rows retain in collections, whose occurrence is resolved already: each collection once, by identity
  private Set<Object> pinnedVisited;

  /** @return true when the statement deleted {@code record}, or this DELETE found it connected and left it to the end */
  private boolean isDeletedOrPending(final Document record) {
    return deleted.isDeleted(record)
        || (!pendingVertices.isEmpty() && record instanceof Vertex vertex && pendingVertices.contains(vertex.getIdentity()));
  }

  /** @return what lines a row up with the deletions, or null while nothing was deleted, or is about to be, that a row can hold */
  private DeletedReferences referencesToApply() {
    if (foreachDeleted != null)
      return foreachDeleted.isEmpty() ? null : foreachReferences;
    return deleted.isEmpty() && pendingVertices.isEmpty() ? null : rowReferences;
  }

  private Set<Object> getPinnedVisited() {
    if (pinnedVisited == null)
      pinnedVisited = Collections.newSetFromMap(new IdentityHashMap<>());
    return pinnedVisited;
  }

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
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Result row = buffer.get(bufferIndex++);
        // Served after every row of the batch deleted what it deletes, so a node a later row deleted is read as deleted
        // through the aliases, lists, maps and paths this row holds (issue #12548). One check per row while nothing was deleted
        final DeletedReferences references = referencesToApply();
        if (references != null)
          references.mark(row);
        return row;
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
            heapLimit = OperationHeapLimit.of(context, "DELETE of the matched rows");
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

            // Apply DELETE operations to this result
            final Set<String> deletedVars = new HashSet<>();
            applyDeleteOperations(inputResult, deletedVars, this::pinRemainingRows);

            // Mark deleted variables in the result so subsequent access throws DeletedEntityAccess
            if (!deletedVars.isEmpty() && inputResult instanceof ResultInternal mutableResult) {
              // The marker keeps the identity the entity had, which id() and elementId() still answer (issue #13582), and
              // the type of a relationship, which type() still answers
              for (final String var : deletedVars)
                mutableResult.setProperty(var, DeletedEntityMarker.of(mutableResult.getProperty(var)));
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
          pinRetainedEdgeOccurrences(row, getPinnedVisited());
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

  /**
   * Resolves and caches the occurrence of lightweight edges retained in a row's properties, collections, or paths
   * before deleting any lightweight edge, so a surviving parallel edge does not collide with a deleted twin after
   * the vertex's edge list shrinks (issue #10008). A row reads whether such an edge is deleted when it is served, which is
   * after the delete renumbered its twins: left unnumbered, a survivor read as the first of its triple and collided with the
   * twin deleted first.
   * <p>
   * Walked only for a row that deletes a lightweight edge that has a twin, and once per collection ({@code visited}, by
   * identity), so the rows of a statement that share one {@code collect()} list pay for it once.
   */
  private static void pinRetainedEdgeOccurrences(final Object value, final Set<Object> visited) {
    if (value == null)
      return;
    if (value instanceof Edge edge) {
      if (edge.getIdentity() instanceof LightEdgeRID light)
        light.getOccurrence();
      return;
    }
    if (value instanceof TraversalPath path) {
      if (visited != null && !visited.add(path))
        return;
      for (final Edge edge : path.getEdges())
        if (edge.getIdentity() instanceof LightEdgeRID light)
          light.getOccurrence();
      return;
    }
    if (value instanceof List<?> list && !(value instanceof LongRangeList)) {
      if (visited != null && !visited.add(list))
        return;
      for (final Object item : list)
        pinRetainedEdgeOccurrences(item, visited);
      return;
    }
    if (value instanceof Map<?, ?> map) {
      if (visited != null && !visited.add(map))
        return;
      for (final Object item : map.values())
        pinRetainedEdgeOccurrences(item, visited);
      return;
    }
    if (value instanceof Result row) {
      if (visited != null && !visited.add(row))
        return;
      for (final String prop : row.getPropertyNames())
        pinRetainedEdgeOccurrences(row.getProperty(prop), visited);
    }
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

    // In the statement's transaction, which OpenCypherQueryEngine opens when the caller has none (issue #10190): a vertex
    // still connected once every row is processed rolls back the deletes of the rows before it with the rest (#8997)
    try {
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
        // The targets are noted as deleted for the rest of the body, which reads them before the iteration ends (see
        // foreachDeleted). Their lightweight edges are numbered first, while the lists still hold every twin: a twin that is
        // kept must not read as the one deleted
        if (foreachDeleted == null && context.getVariable(ForeachStep.FOREACH_DELETED_SO_FAR_VAR) instanceof DeletedSoFar shared) {
          foreachDeleted = shared;
          foreachReferences = new DeletedReferences(shared);
        }
        if (foreachDeleted != null && pinOccurrences(allEdges))
          pinRetainedEdgeOccurrences(result, getPinnedVisited());
        for (final Object edge : allEdges) {
          deferredBatch.add(new DeferredDeleteTarget(edge, detach));
          if (foreachDeleted != null)
            foreachDeleted.doom(edge, detach);
        }
        for (final Object other : allOther) {
          deferredBatch.add(new DeferredDeleteTarget(other, detach));
          if (foreachDeleted != null)
            foreachDeleted.doom(other, detach);
        }
        return;
      }

      // The lightweight edges of the row are numbered before anything is deleted; the first one with a twin numbers
      // the rest of the statement's too, while their lists still hold every twin (#10008)
      if (pinOccurrences(allEdges)) {
        pinRetainedEdgeOccurrences(result, getPinnedVisited());
        if (pinRemainingRows != null)
          pinRemainingRows.run();
      }

      // Delete all edges first, then all other objects (vertices, documents). The `deleted` set is
      // a step-instance field shared across rows so an element bound by multiple rows is deleted once.
      for (final Object edge : allEdges)
        deleteObject(edge);
      for (final Object other : allOther)
        deleteObject(other);
    } catch (final RuntimeException e) {
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
      final Expression expression = expressions != null && i < expressions.size() ? expressions.get(i) : null;
      // Decided on the parsed expression, never on the text, which the parser hands over with its whitespace stripped: a plain
      // variable is read off the row, anything else ([a][0], [a, b], (a), l[i], CASE ...) is evaluated (issue #12259)
      // (a) is a plain variable too: looked up by its name, not by the text
      final String name = expression instanceof VariableExpression plain ? plain.getVariableName() : expression == null ? variable : null;
      final Object obj = name != null ? resolveDeleteTarget(name, row) : expression.evaluate(row, context);
      if (obj == null)
        continue;
      collectDeleteTargets(obj, edges, other);
      if (deletedVars != null && name != null && !name.contains(".") && !name.contains("[") && !name.contains("("))
        deletedVars.add(name);
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
          if (entry.detach())
            try {
              detachEdges(v, deleted, stats);
            } catch (final RecordNotFoundException ignored) {
              // vertex was already removed by the batch flush: the delete below finds it gone too
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
          throw new ObjectInUseException("DeleteConnectedNode: Cannot delete node "
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

  private static void deleteObjectStatic(final Object obj, final DeletedSoFar deleted, final QueryStatistics stats) {
    // A binding a row reads as deleted (an earlier iteration of the FOREACH deleted it) has nothing left to delete
    if (obj == null || obj instanceof DeletedEntityMarker)
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
    } else {
      requireDeletable(obj);
      other.add(obj);
    }
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
    } else
      requireDeletable(obj);
  }

  /**
   * Refuses a DELETE target that is no entity - a number, a string, a boolean, a temporal (issue #11975): the statement used
   * to run and delete nothing. A node, a relationship, a path, a record, null and an entity an earlier DELETE of the
   * statement already removed are deletable, and so are the lists and maps holding them, which are unwrapped before this.
   */
  private static void requireDeletable(final Object obj) {
    if (obj == null || obj instanceof Identifiable || obj instanceof Result || obj instanceof DeletedEntityMarker)
      return;
    throw new TypeMismatchException(
        "Type mismatch: DELETE expects a node, a relationship or a path, but was " + CypherFunctionHelper.cypherTypeName(obj));
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
  }

  /**
   * Deletes a vertex from the graph.
   * With DETACH its relationships are counted first and deleted with it (see {@link #detachEdges}).
   *
   * A relationship removed both by DETACH and by an explicit edge delete in the same statement is counted only once,
   * through {@link #deleted}.
   *
   * @param vertex       vertex to delete
   * @param edgesChecked true when the caller just verified the vertex has no relationship (non-DETACH)
   */
  private void deleteVertex(final Vertex vertex, final boolean edgesChecked) {
    if (deleteClause.isDetach()) {
      detachEdges(vertex, deleted, context.getStatistics());
    } else if (!edgesChecked) {
      // Non-DETACH DELETE: check for connected edges
      if (hasEdges(vertex))
        throw new ObjectInUseException("DeleteConnectedNode: Cannot delete node " + vertex.getIdentity() +
            " because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE");
    }

    vertex.delete();
    context.getStatistics().incNodesDeleted();
  }

  /**
   * The relationship half of DETACH DELETE, run right before {@code vertex.delete()}: counts every relationship of the vertex
   * the statement has not deleted yet, and leaves deleting them to the vertex delete.
   * <p>
   * Issue #11681: each relationship used to be deleted here with {@code edge.delete()}, which also removed it from the
   * vertex's own list. On a hub that list is striped and every removal scanned a stripe for the entry, so the statement grew
   * faster than the number of relationships, all to maintain a list {@code GraphEngine.deleteVertex} drops whole a moment
   * later. That delete disconnects each relationship from its far endpoint only (#5760) and reports each one to the delete
   * listeners and triggers, once.
   * <p>
   * A relationship of a unidirectional type that ends in the vertex is stored on its source only: it is deleted here, from
   * the source's list, which leaves the vertex delete nothing to find for it (#8676).
   * <p>
   * Each relationship is counted once: a self-loop is in both lists and is read once by its identity, and parallel
   * lightweight edges of one type over one pair are as many relationships as there are twins (#10008). The lists are only
   * read here, so every edge meets the precondition of {@link DeletedSoFar#claimJustRead}; one an earlier DELETE of the
   * statement removed is not in them any more.
   */
  private static void detachEdges(final Vertex vertex, final DeletedSoFar deleted, final QueryStatistics stats) {
    final EdgeIdentitySet seen = new EdgeIdentitySet();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      if (seen.add(edge.getIdentity()) && deleted.claimJustRead(edge))
        stats.incRelationshipsDeleted();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      if (seen.add(edge.getIdentity()) && deleted.claimJustRead(edge))
        stats.incRelationshipsDeleted();
    for (final Edge edge : incomingUnidirectionalEdges(vertex))
      if (seen.add(edge.getIdentity()) && deleted.claimJustRead(edge))
        deleteEdgeStatic(edge, stats);
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

  private void releaseHeap() {
    if (heapLimit != null)
      heapLimit.release();
  }

  @Override
  public void close() {
    pendingVertices.clear();
    rowReferences.clear();
    if (foreachReferences != null)
      foreachReferences.clear();
    if (pinnedVisited != null)
      pinnedVisited.clear();
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
   * <p>
   * It is also what a row asks, through {@link DeletedReferences.Deletions}, whether a node or a relationship it holds is gone.
   */
  public static final class DeletedSoFar implements DeletedReferences.Deletions {
    // VERTICES AND DOCUMENTS: RECORDS EQUAL BY RID (SEE BaseRecord.equals), SO INSTANCES OF ONE RECORD COLLAPSE
    private final Set<Object>     records  = new HashSet<>();
    private final EdgeIdentitySet edges    = new EdgeIdentitySet();
    private final RidHashSet      vertices = new RidHashSet();
    // WHAT THE ITERATION OF A FOREACH IN PROGRESS WILL DELETE WHEN IT ENDS, KEPT SEPARATE: THE DELETE ITSELF IS CLAIMED IN THE SETS ABOVE
    private RidHashSet            doomedRecords;
    private RidHashSet            doomedDetached;
    private EdgeIdentitySet       doomedLightEdges;

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

    /** @return true when {@code edge} was deleted with an endpoint the statement deleted */
    boolean wentWithDeletedVertex(final Edge edge) {
      try {
        return vertices.contains(edge.getOut()) || vertices.contains(edge.getIn());
      } catch (final RecordNotFoundException e) {
        // THE EDGE RECORD IS GONE ALREADY
        return true;
      }
    }

    boolean isEmpty() {
      return records.isEmpty() && edges.isEmpty() && vertices.isEmpty() && doomedRecords == null && doomedLightEdges == null;
    }

    /**
     * Notes a target of a FOREACH iteration's DELETE, which runs when the iteration ends: the clauses after it in the body
     * read it as deleted already, as they read the variable it deleted (issue #12548). A DETACH of a vertex dooms its
     * relationships too, and a lightweight edge is doomed by its occurrence, so a twin that is kept is not read as deleted.
     * The occurrence of a lightweight edge has to be numbered before this, while the edge lists hold every twin.
     */
    void doom(final Object target, final boolean detach) {
      if (!(target instanceof Document record) || record.getIdentity() == null)
        return;
      final RID rid = record.getIdentity();
      if (record instanceof Edge && rid instanceof LightEdgeRID) {
        if (doomedLightEdges == null)
          doomedLightEdges = new EdgeIdentitySet();
        doomedLightEdges.add(rid);
        return;
      }
      if (doomedRecords == null)
        doomedRecords = new RidHashSet();
      doomedRecords.add(rid);
      if (detach && record instanceof Vertex) {
        if (doomedDetached == null)
          doomedDetached = new RidHashSet();
        doomedDetached.add(rid);
      }
    }

    /** Forgets the targets doomed: the iterations that queued them ran their batch, so they are deleted, or the statement failed. */
    void clearDoomed() {
      doomedRecords = null;
      doomedDetached = null;
      doomedLightEdges = null;
    }

    /**
     * @return true when the statement deleted {@code record}, or a FOREACH iteration will when it ends: a node or a record, a
     * relationship, or a relationship that went with an endpoint
     */
    @Override
    public boolean isDeleted(final Document record) {
      if (record instanceof Edge edge)
        return edges.contains(edge.getIdentity()) || (!vertices.isEmpty() && wentWithDeletedVertex(edge)) || isDoomed(edge);
      return records.contains(record)
          || (doomedRecords != null && record.getIdentity() != null && doomedRecords.contains(record.getIdentity()));
    }

    private boolean isDoomed(final Edge edge) {
      if (doomedRecords == null && doomedLightEdges == null)
        return false;
      final RID rid = edge.getIdentity();
      if (rid instanceof LightEdgeRID) {
        if (doomedLightEdges != null && doomedLightEdges.contains(rid))
          return true;
      } else if (doomedRecords != null && doomedRecords.contains(rid))
        return true;
      if (doomedDetached == null)
        return false;
      try {
        return doomedDetached.contains(edge.getOut()) || doomedDetached.contains(edge.getIn());
      } catch (final RecordNotFoundException e) {
        // THE EDGE RECORD IS GONE ALREADY
        return true;
      }
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
