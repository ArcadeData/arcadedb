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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.path.AbstractPathProcedure.Filters;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The walk behind {@code path.expandConfig}, {@code path.expand} and {@code path.spanningTree}: the paths from a start node
 * that APOC's traversal framework would answer, one at a time and in the order a breadth-first or a depth-first walk meets them.
 * <p>
 * What makes a branch of the walk is decided in the order APOC decides it: the relationship the walk arrived by is never
 * taken again; the {@link PathUniqueness} admits the branch, and spends the node or the relationship it reaches; then the
 * {@link Filters} say whether the branch is an answer and whether the walk goes on past it. A branch the filters turn away
 * has used up its node or relationship all the same.
 * <p>
 * It is a plain iterator over what it has not walked yet, so a {@code limit}, or a {@code LIMIT} further down the query, ends
 * the walk instead of trimming the answer of a walk that finished. A trail walk of a graph with cycles is exponential in the
 * depth: nothing is held that the answer so far does not need. A breadth-first walk keeps its frontier; a depth-first walk
 * keeps one cursor per level of the path it is on, in a heap-allocated stack, so a path as long as the graph is deep cannot
 * overflow the thread's.
 * <p>
 * Each path is the {@code [v0, e0, v1, ...]} list {@link AbstractPathProcedure#buildPath} turns into a path value. An adjacency
 * entry whose far endpoint, or whose own record, is gone is skipped whole and reported, as in every other path walk (#7976).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PathExpansion implements Iterator<List<Object>> {
  /** A point of the walk: the node reached, and the branch it was reached from, which is how the path is read back. */
  static final class Branch {
    final Branch parent;
    /** The relationship the branch was reached by, null for the start node. */
    final Edge   edge;
    final RID    edgeId;
    final Vertex vertex;
    final RID    vertexId;
    final int    depth;

    Branch(final Branch parent, final Edge edge, final RID edgeId, final Vertex vertex, final RID vertexId, final int depth) {
      this.parent = parent;
      this.edge = edge;
      this.edgeId = edgeId;
      this.vertex = vertex;
      this.vertexId = vertexId;
      this.depth = depth;
    }

    /** The path from the start node to this branch, as {@code [v0, e0, v1, ..., vn]}. */
    List<Object> toElements() {
      final Object[] elements = new Object[2 * depth + 1];
      int index = elements.length;
      for (Branch branch = this; branch != null; branch = branch.parent) {
        elements[--index] = branch.vertex;
        if (branch.edge != null)
          elements[--index] = branch.edge;
      }
      return Arrays.asList(elements);
    }
  }

  /** The expansion of one branch, resumed each time the walk comes back to it: its two sides, and the edges of the side it is on. */
  private static final class Cursor {
    final Branch branch;
    int                side;
    Vertex.DIRECTION   direction;
    Iterator<Edge>     edges;

    Cursor(final Branch branch) {
      this.branch = branch;
    }
  }

  // What a branch waiting to be expanded holds on the heap: the branch, its cursor and its slot in the queue. An estimate, and
  // a low one on purpose: the vertex and edge records a branch points to are shared with the paths already built and with the
  // record cache, so counting them per branch would charge the same record many times over
  private static final int CURSOR_BYTES = 128;

  private final Database                 database;
  private final Filters                  filters;
  private final PathUniqueness.Admission admission;
  private final boolean                  breadthFirst;
  private final int                      minLevel;
  private final int                      maxLevel;
  private final int                      limit;
  private final CommandContext           context;
  private final RidHashSet               ghostNodes = new RidHashSet(16);
  // The branches waiting to be expanded are what a trail walk of a graph with cycles grows without bound: they are held
  // against the query's heap, so a walk too wide for it fails the query instead of the server
  private final OperationHeapLimit       heap;
  // A walk that rejects every edge it meets spends its whole time inside one hasNext(): the command timeout, and the
  // termination of the statement, are checked from inside it
  private final WorkGuard                guard;
  private       int                      examined;

  // Breadth-first: a queue, expanded from its head; depth-first: a stack, expanded from its top. Either way new cursors go last
  private final ArrayDeque<Cursor> open = new ArrayDeque<>();

  private List<Object>        pending;
  private int                 emitted;
  private LabelFilter.Verdict childVerdict;

  /**
   * @param breadthFirst whether the paths come shortest first, or along the first route to each end
   * @param minLevel     the shortest answer: a path with fewer hops is not returned, though the walk goes through it
   * @param maxLevel     the longest path the walk takes, {@link Integer#MAX_VALUE} for no bound
   * @param limit        how many paths to return, {@link Integer#MAX_VALUE} for all of them
   */
  PathExpansion(final Vertex startNode, final Filters filters, final PathUniqueness uniqueness, final boolean breadthFirst,
      final int minLevel, final int maxLevel, final int limit, final CommandContext context) {
    this.database = startNode.getDatabase();
    this.filters = filters;
    this.breadthFirst = breadthFirst;
    this.minLevel = minLevel;
    this.maxLevel = maxLevel;
    this.limit = limit;
    this.context = context;
    this.heap = OperationHeapLimit.of(context, "branches", "path expansion");
    this.guard = WorkGuard.forCommand(context, "path expansion");

    final RID startId = startNode.getIdentity();
    this.admission = uniqueness.newAdmission(!breadthFirst, startId);

    final LabelFilter.Verdict startVerdict = filters.evaluate(database, startId, 0);
    if (startVerdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
      return;

    final Branch start = new Branch(null, null, null, startNode, startId, 0);
    if (startVerdict.includes && minLevel <= 0)
      pending = start.toElements();
    if (startVerdict.continues && maxLevel > 0)
      enqueue(start);
  }

  @Override
  public boolean hasNext() {
    if (pending == null && emitted < limit)
      advance();
    if (pending == null || emitted >= limit)
      // Nothing more will be asked of the walk: what it holds is given back now, not when the query is collected
      heap.release();
    return pending != null && emitted < limit;
  }

  /** Gives back the heap the walk holds, for a consumer that stops before the walk does. Idempotent. */
  void release() {
    heap.release();
  }

  private void enqueue(final Branch branch) {
    heap.check(open.size() + 1L);
    heap.charge(CURSOR_BYTES);
    open.addLast(new Cursor(branch));
  }

  @Override
  public List<Object> next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final List<Object> path = pending;
    pending = null;
    ++emitted;
    return path;
  }

  /** Walks until the next answer is found, or there is nothing left to walk. */
  private void advance() {
    while (pending == null) {
      final Cursor cursor = breadthFirst ? open.peekFirst() : open.peekLast();
      if (cursor == null)
        return;

      final Branch child = nextChild(cursor);
      if (child == null) {
        // The branch has no more edges to take: a depth-first walk backs out of it, and gives its node or relationship back
        if (breadthFirst)
          open.pollFirst();
        else {
          open.pollLast();
          if (cursor.branch.edge != null)
            admission.release(cursor.branch.edge, cursor.branch.vertexId);
        }
        heap.release(CURSOR_BYTES);
        continue;
      }

      if (childVerdict.includes && child.depth >= minLevel)
        pending = child.toElements();

      if (childVerdict.continues && child.depth < maxLevel)
        enqueue(child);
      else
        admission.release(child.edge, child.vertexId);
    }
  }

  /** The next branch the cursor's node leads to; null when it leads to no more. {@link #childVerdict} is the filters' verdict on it. */
  private Branch nextChild(final Cursor cursor) {
    final Branch branch = cursor.branch;
    final int depth = branch.depth + 1;

    while (true) {
      if (cursor.edges == null) {
        if (cursor.side >= 2)
          return null;
        cursor.direction = AbstractPathProcedure.walkDirection(cursor.side++);
        final Iterable<Edge> hop = AbstractPathProcedure.hopEdges(context, branch.vertex, cursor.direction,
            filters.relationships(), branch.depth);
        if (hop == null)
          continue;
        cursor.edges = hop.iterator();
      }

      if (!cursor.edges.hasNext()) {
        cursor.edges = null;
        continue;
      }

      final Edge edge = cursor.edges.next();
      guard.checkPeriodically(++examined);
      try {
        // Reading the endpoint is what forces a lazily loaded edge, so a ghost edge record surfaces here
        final RID neighborId = cursor.direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
        if (neighborId == null)
          continue;
        final RID edgeId = edge.getIdentity();

        // APOC never leaves a node by the relationship it arrived by, whatever the uniqueness
        if (branch.edgeId != null && LightEdgeRID.isSameEdge(branch.edgeId, edgeId))
          continue;
        // A self-loop is in both lists of its node: it is one relationship, so it is not taken again on the incoming side
        // when the outgoing side has taken it already
        if (cursor.direction == Vertex.DIRECTION.IN && neighborId.equals(branch.vertexId)
            && filters.relationships().follows(branch.depth, Vertex.DIRECTION.OUT, edge))
          continue;

        if (!admission.admit(branch, edge, neighborId, depth))
          continue;

        final LabelFilter.Verdict verdict = filters.evaluate(database, neighborId, depth);
        if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE) {
          admission.release(edge, neighborId);
          continue;
        }

        // The neighbour is tested by its RID and loaded only once it is going into the path (issue #7976)
        final Vertex neighbor = AbstractPathProcedure.resolveNeighbor(neighborId, ghostNodes);
        if (neighbor == null) {
          admission.release(edge, neighborId);
          continue;
        }

        childVerdict = verdict;
        return new Branch(branch, edge, edgeId, neighbor, neighborId, depth);
      } catch (final RecordNotFoundException e) {
        // Only the reads of the edge's endpoints and identity, which come before the admission, can land here: the admission,
        // the filters and resolveNeighbor() (which keeps a missing vertex to itself) read no record that can be missing, so
        // nothing has been spent that would have to be given back
        GhostEdgeReporter.reportSkipped(e);
      }
    }
  }
}
