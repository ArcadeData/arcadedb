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
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.BoundedBreadthFirstSearch;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The distinct-endpoint walk of a variable-length pattern ({@link BreadthFirstTraverser#traverseDistinctEndpoints}) read from a
 * Graph Analytical View's adjacency instead of the edge lists of the records (issue #10548). It answers the same set of end
 * vertices for every direction, path mode and lower bound of 0 or 1, the start included under the same rules: a cycle back to it
 * makes it an end vertex of a pattern whose lower bound is 1 (any closed walk for WALK and a directed TRAIL, never for ACYCLIC),
 * and an undirected TRAIL needs a simple cycle through it of at most {@code maxHops} relationships, found with the same depth and
 * branch labels the record walk uses.
 * <p>
 * Only for a pattern the view can answer alone: no inline property map and no inline {@code WHERE} on the relationship, which the
 * view does not store. The caller checks that, and walks the records for a start vertex the view does not map
 * ({@link #traverse} answers null).
 * <p>
 * One instance serves one execution of the operator: its search state is reused from one start vertex to the next, so a walk
 * costs what it reaches and not what the graph holds, and only the iterator of the latest {@link #traverse} call may be read.
 */
public final class GAVDistinctEndpointTraverser {
  private final GraphTraversalProvider    provider;
  private final Database                  database;
  private final int                       minHops;
  private final int                       maxHops;
  private final boolean                   startNeedsCycle;
  private final boolean                   startIsEndOnReturn;
  private final BoundedBreadthFirstSearch search;
  private final long                      expectedVisits;
  // Depth and branch of every vertex found, kept only by the pattern that looks for the cycle through the start itself
  private       int[]                     depthOf;
  private       int[]                     branchOf;

  /**
   * @param provider  the view, pinned for the statement
   * @param direction the direction the walk follows from its start
   * @param edgeTypes the relationship types, empty for every type
   * @param minHops   0 or 1
   * @param maxHops   the upper bound
   * @param pathMode  the pattern's path mode, null for TRAIL
   */
  public GAVDistinctEndpointTraverser(final GraphTraversalProvider provider, final Direction direction, final String[] edgeTypes,
      final int minHops, final int maxHops, final PathMode pathMode, final CommandContext context) {
    if (minHops > 1)
      throw new IllegalStateException("A distinct-endpoint traversal needs a lower bound of 0 or 1, got " + minHops);
    this.provider = provider;
    this.database = context.getDatabase();
    this.minHops = minHops;
    this.maxHops = maxHops;
    final PathMode mode = pathMode != null ? pathMode : PathMode.TRAIL;
    this.startNeedsCycle = minHops == 1 && direction == Direction.BOTH && mode == PathMode.TRAIL;
    this.startIsEndOnReturn = minHops == 1 && mode != PathMode.ACYCLIC && !startNeedsCycle;
    final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    this.search = new BoundedBreadthFirstSearch(provider, arcadeDirection, edgeTypes, guard::check);
    this.expectedVisits = expectedVisits(provider, arcadeDirection, edgeTypes, maxHops);
  }

  /**
   * The distinct end vertices of the walk from {@code start}, lazily and level by level, or null when the view does not map
   * {@code start} and the caller has to walk the records.
   */
  public Iterator<Vertex> traverse(final Vertex start) {
    final Endpoints endpoints = walk(start);
    return endpoints != null ? new EndpointIterator(endpoints) : null;
  }

  /** The dense node id of {@code vertex} in the view this walk reads, or -1 when the view does not map it. */
  public int nodeIdOf(final Vertex vertex) {
    return GAVVertex.nodeIdIn(vertex, provider);
  }

  /**
   * Adds the distinct end vertices of the walk from {@code start} to {@code counted}, by node id, without loading or building a
   * vertex: what {@code count(DISTINCT b)} over the rows of the walk counts (issue #10548).
   *
   * @param excludeStart leave the start out, for a {@code WHERE b <> a}
   * @param targetLabel  the label an end vertex must carry, or null
   * @param counted      the node ids counted so far, by every walk of the statement
   *
   * @return how many end vertices were not in {@code counted} yet, or -1 when the view does not map {@code start} and the
   * caller has to walk the records
   */
  public long countInto(final Vertex start, final boolean excludeStart, final String targetLabel, final BitSet counted) {
    final Endpoints endpoints = walk(start);
    if (endpoints == null)
      return -1L;
    final Schema schema = targetLabel != null ? database.getSchema() : null;
    long added = 0;
    for (int nodeId = endpoints.nextId(); nodeId >= 0; nodeId = endpoints.nextId())
      if (!counted.get(nodeId) && isEnd(nodeId, endpoints.startId, excludeStart, targetLabel, schema)) {
        counted.set(nodeId);
        ++added;
      }
    return added;
  }

  /**
   * Whether the walk from {@code start} reaches at least one end vertex, stopping at the first: what {@code count(DISTINCT a)}
   * over the rows of the walk counts for its start {@code a} (issue #10548).
   *
   * @return 1 if it does, 0 if it does not, or -1 when the view does not map {@code start} and the caller has to walk the records
   */
  public int reachesAny(final Vertex start, final boolean excludeStart, final String targetLabel) {
    final Endpoints endpoints = walk(start);
    if (endpoints == null)
      return -1;
    final Schema schema = targetLabel != null ? database.getSchema() : null;
    for (int nodeId = endpoints.nextId(); nodeId >= 0; nodeId = endpoints.nextId())
      if (isEnd(nodeId, endpoints.startId, excludeStart, targetLabel, schema))
        return 1;
    return 0;
  }

  private boolean isEnd(final int nodeId, final int startId, final boolean excludeStart, final String targetLabel,
      final Schema schema) {
    if (excludeStart && nodeId == startId)
      return false;
    // A node id whose vertex the view no longer maps names nothing
    final int bucketId = provider.getBucketId(nodeId);
    if (bucketId < 0)
      return false;
    if (schema == null)
      return true;
    final DocumentType type = schema.getTypeByBucketId(bucketId);
    return type != null && type.instanceOf(targetLabel);
  }

  private Endpoints walk(final Vertex start) {
    final int startId = GAVVertex.nodeIdIn(start, provider);
    if (startId < 0)
      return null;
    search.start(startId, expectedVisits);
    if (startNeedsCycle && depthOf == null) {
      final int bound = Math.max(provider.getNodeIdUpperBound(), startId + 1);
      depthOf = new int[bound];
      branchOf = new int[bound];
    }
    return new Endpoints(start, startId);
  }

  /** The end vertices of {@link Endpoints}, the start as the vertex the walk began from and the others as view vertices. */
  private final class EndpointIterator implements Iterator<Vertex> {
    private final Endpoints endpoints;
    private       Vertex    next;

    EndpointIterator(final Endpoints endpoints) {
      this.endpoints = endpoints;
    }

    @Override
    public boolean hasNext() {
      while (next == null) {
        final int nodeId = endpoints.nextId();
        if (nodeId < 0)
          return false;
        if (nodeId == endpoints.startId) {
          next = endpoints.start;
          break;
        }
        final RID rid = provider.getRID(nodeId);
        // A node id whose vertex the view no longer maps names nothing
        if (rid != null)
          next = new GAVVertex(rid, nodeId, provider, database);
      }
      return true;
    }

    @Override
    public Vertex next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Vertex result = next;
      next = null;
      return result;
    }
  }

  /** The walk from one start, level by level: each end vertex by node id, the start among them under the rules above. */
  private final class Endpoints implements BoundedBreadthFirstSearch.EntryListener {
    private final Vertex  start;
    private final int     startId;
    private       int     emitted = 1;
    private       boolean startPending;
    private       boolean startReported;
    private       int     branches;

    Endpoints(final Vertex start, final int startId) {
      this.start = start;
      this.startId = startId;
      // A lower bound of 0 makes the start the first end vertex; otherwise only a cycle back to it does
      this.startPending = minHops == 0;
      this.startReported = minHops == 0;
    }

    /** The next end vertex, or -1 once the walk has reached every one within the bound. */
    int nextId() {
      while (true) {
        if (startPending) {
          startPending = false;
          return startId;
        }
        if (emitted < search.size())
          return search.node(emitted++);
        if (search.depth() >= maxHops || !search.hasFrontier())
          return -1;
        search.expandLevel(this);
      }
    }

    @Override
    public void discovered(final int from, final int to) {
      if (depthOf == null)
        return;
      final int depth = search.depth();
      depthOf[to] = depth + 1;
      branchOf[to] = depth == 0 ? branches++ : branchOf[from];
    }

    @Override
    public void revisited(final int from, final int to) {
      if (startReported)
        return;
      if (startNeedsCycle ? closesCycleThroughStart(from, to) : startIsEndOnReturn && to == startId) {
        startReported = true;
        startPending = true;
      }
    }

    /**
     * Whether an entry from the expanded vertex to one already found closes a simple cycle through the start of at most
     * {@code maxHops} relationships: the same test as the record walk's, see {@link BreadthFirstTraverser#traverseDistinctEndpoints}.
     */
    private boolean closesCycleThroughStart(final int from, final int to) {
      final int depth = search.depth();
      if (depth == 0)
        // The start lists itself (a self loop) or a vertex it has already found (a parallel relationship, a cycle of two)
        return to == startId || maxHops >= 2;
      if (to == startId)
        // Only a vertex next to the start lists it, through the relationship it was found by or a parallel one, which the start
        // listed twice itself
        return false;
      return branchOf[to] != branchOf[from] && depth + depthOf[to] + 1 <= maxHops;
    }
  }

  /**
   * About how many adjacency entries one walk reads, which decides whether the view's packed adjacency is worth building for it:
   * the mean degree to the power of the hops, capped at every entry the view holds. Zero when the view does not know its mean
   * degree, so the packed adjacency is used only when it is there already.
   */
  private static long expectedVisits(final GraphTraversalProvider provider, final Vertex.DIRECTION direction,
      final String[] edgeTypes, final int maxHops) {
    final double meanDegree = provider.getMeanDegree(direction, edgeTypes);
    if (meanDegree <= 0)
      return 0L;
    final double all = meanDegree * provider.getNodeCount();
    double visits = 0;
    double level = 1;
    // Capped at 64 levels: an unbounded pattern over a sparse view converges long before, and would otherwise loop to the bound
    for (int depth = 0, levels = Math.min(maxHops, 64); depth < levels && visits < all; depth++) {
      level *= meanDegree;
      visits += level;
    }
    return (long) Math.min(visits, all);
  }
}
