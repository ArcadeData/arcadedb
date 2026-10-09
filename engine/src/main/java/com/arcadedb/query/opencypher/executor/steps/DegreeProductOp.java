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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.EdgeBucketMask;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.IntHashSet;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Count operator for star-join patterns (Q4, Q7).
 * For each central node, the path count is the product of degrees along each arm.
 * OPTIONAL MATCH arms use {@code max(1, degree)}.
 */
public final class DegreeProductOp implements CountOp {
  /** The one-filter self-loop flag of an undirected last hop read off the IN list. Shared, so never written. */
  private static final boolean[] SKIP_SELF_LOOP = { true };

  private final String          centralLabel;
  /** The property filter on the central node, on top of its label; null for none (issue #9595). */
  private final VertexPredicate centralPredicate;
  private final Arm[]           arms;
  private final String[]        allEdgeTypes;

  /**
   * A single arm extending from the central node.
   */
  public static final class Arm {
    final String[]            edgeTypes;
    final Vertex.DIRECTION[]  directions;
    final boolean             optional;
    /** One entry per hop: the label of the node that hop reaches, or null for none. The array itself is null when no hop has one. */
    final String[]            endpointLabels;
    /**
     * One entry per hop: the property filter on the node that hop reaches, or null for none. The array itself is null when no
     * hop has one (issue #9595).
     */
    final VertexPredicate[]   endpointPredicates;

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional) {
      this(edgeTypes, directions, optional, null);
    }

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional,
        final String[] endpointLabels) {
      this(edgeTypes, directions, optional, endpointLabels, null);
    }

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional,
        final String[] endpointLabels, final VertexPredicate[] endpointPredicates) {
      this.edgeTypes = edgeTypes;
      this.directions = directions;
      this.optional = optional;
      boolean any = false;
      if (endpointLabels != null)
        for (final String label : endpointLabels)
          any |= label != null;
      this.endpointLabels = any ? endpointLabels : null;
      this.endpointPredicates = VertexPredicate.any(endpointPredicates) ? endpointPredicates : null;
    }

    boolean hasEndpointLabel() {
      return endpointLabels != null;
    }

    boolean hasEndpointPredicate() {
      return endpointPredicates != null;
    }

    /** The bucket ids each hop's endpoint label stands for (sub-types included), null where the hop has no label. */
    IntHashSet[] endpointBuckets(final Database db) {
      if (endpointLabels == null)
        return null;
      final IntHashSet[] buckets = new IntHashSet[edgeTypes.length];
      for (int h = 0; h < buckets.length; h++)
        buckets[h] = CSRCountUtils.buildValidBuckets(db, endpointLabels[h]);
      return buckets;
    }
  }

  public DegreeProductOp(final String centralLabel, final Arm[] arms) {
    this(centralLabel, null, arms);
  }

  public DegreeProductOp(final String centralLabel, final VertexPredicate centralPredicate, final Arm[] arms) {
    this.centralLabel = centralLabel;
    this.centralPredicate = centralPredicate;
    this.arms = arms;

    // Pre-compute all edge types
    int total = 0;
    for (final Arm arm : arms)
      total += arm.edgeTypes.length;
    this.allEdgeTypes = new String[total];
    int idx = 0;
    for (final Arm arm : arms)
      for (final String et : arm.edgeTypes)
        allEdgeTypes[idx++] = et;
  }

  @Override
  public String[] edgeTypes() {
    return allEdgeTypes;
  }

  /** The degree product is computed per central node, which are the vertices carrying the central label. */
  @Override
  public boolean canEnumerateAnchors() {
    return centralLabel != null;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    // With no mandatory arm there is no degree filter to exclude non-central-type nodes from the
    // provider's node domain, and zero-degree central nodes must still contribute 1 row each
    // (OPTIONAL MATCH preserves the left-hand row). The CSR scan cannot distinguish the two
    // cases, so fall back to the OLTP path which iterates the central type. See issue #5094.
    if (!hasMandatoryArm())
      return executeOLTP(db, guard);

    if (hasPredicate())
      return executeFiltered(provider, db, guard);

    final int nodeIdUpperBound = provider.getNodeIdUpperBound();

    // An endpoint label is a filter on the far end of the arm, which the degree of the central node alone does not carry
    // (#6337). Resolve each arm's label to bucket ids once. A label the edge type already implies - every edge of that
    // type and direction ends in the label's buckets - changes nothing, so the arm keeps the plain degree and the array
    // arithmetic below; only an arm whose label really filters pays for a filtered degree array.
    final IntHashSet[][] armBuckets = new IntHashSet[arms.length][];
    boolean anyLabelled = false;
    for (int a = 0; a < arms.length; a++) {
      armBuckets[a] = arms[a].endpointBuckets(db);
      anyLabelled |= armBuckets[a] != null;
    }

    // Decide the path first: a multi-hop labelled arm or an arm without a view sends every arm down the per-node path, so
    // the filtered degrees of the arms before it would be computed for nothing.
    boolean needsPerNode = false;
    for (int a = 0; a < arms.length && !needsPerNode; a++)
      if (armBuckets[a] != null
          && (arms[a].edgeTypes.length != 1 || provider.getNeighborView(arms[a].directions[0], arms[a].edgeTypes[0]) == null))
        needsPerNode = true;

    final int[] bucketIds = anyLabelled ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;
    final int[][] filteredDegrees = new int[arms.length][];
    boolean anyFiltered = false;
    if (!needsPerNode)
      for (int a = 0; a < arms.length; a++) {
        if (arms[a].edgeTypes.length != 1)
          continue;
        final IntHashSet far = armBuckets[a] == null ? null : armBuckets[a][0];
        final boolean undirected = arms[a].directions[0] == Vertex.DIRECTION.BOTH;
        // An undirected view holds a self loop twice, so its plain degree over-counts and the arm pays for a filtered
        // degree array even without a label (#9539)
        if (far == null && !undirected)
          continue;
        final NeighborView view = provider.getNeighborView(arms[a].directions[0], arms[a].edgeTypes[0]);
        if (view == null)
          continue;
        if (far != null && endpointLabelIsImplied(provider, arms[a], view, far, bucketIds, nodeIdUpperBound, guard))
          continue;
        filteredDegrees[a] = filteredDegrees(provider, view, far, bucketIds, undirected, nodeIdUpperBound, guard);
        anyFiltered = true;
      }

    if (!needsPerNode) {
      // Fast path: when all arms are single-hop, pre-fetch NeighborViews and scan
      // degree offset arrays directly. This is pure array arithmetic — no method dispatch,
      // no getRID calls, no object allocation in the hot loop. Mandatory-arm degree=0
      // naturally filters non-central-type nodes (e.g., only Messages have both
      // HAS_TAG OUT > 0 and HAS_CREATOR OUT > 0).
      final NeighborView[] armViews = new NeighborView[arms.length];
      boolean allSingleHopViews = true;
      for (int a = 0; a < arms.length; a++) {
        if (arms[a].edgeTypes.length != 1) {
          allSingleHopViews = false;
          break;
        }
        armViews[a] = provider.getNeighborView(arms[a].directions[0], arms[a].edgeTypes[0]);
        if (armViews[a] == null) {
          allSingleHopViews = false;
          break;
        }
      }

      if (allSingleHopViews)
        return executeFastScan(provider, armViews, anyFiltered ? filteredDegrees : null, nodeIdUpperBound, guard);
    }

    // Slow path: per-node CSR lookup (fallback for multi-hop arms or missing views)
    return executePerNode(provider, armBuckets, bucketIds, nodeIdUpperBound, guard);
  }

  /** Whether the central node or a node an arm reaches carries a property predicate. */
  private boolean hasPredicate() {
    if (centralPredicate != null)
      return true;
    for (final Arm arm : arms)
      if (arm.hasEndpointPredicate())
        return true;
    return false;
  }

  /**
   * The degree product when a property predicate filters the central node or a node an arm reaches (issue #9595).
   * <p>
   * The predicate-free paths above precompute a degree array per arm over every node, which would ask a predicate of
   * every vertex of the graph. This walks the central nodes the label keeps instead and asks each predicate only of the
   * vertices a central node reaches, once per vertex whatever the number of central nodes reaching it. The checks run
   * cheapest first: the mandatory arms with no predicate settle a central node on adjacency alone, so a vertex they rule
   * out has neither its own predicate nor its neighbors' asked.
   */
  private long executeFiltered(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    final IntHashSet centralBuckets = CSRCountUtils.buildValidBuckets(db, centralLabel);
    if (centralBuckets != null && centralBuckets.isEmpty())
      return 0;
    final int[] bucketIds = precomputeBucketIds(provider, nodeIdUpperBound, guard);
    final VertexPredicate.Evaluation central = centralPredicate != null ? centralPredicate.evaluation(db, provider) : null;

    final IntHashSet[][] armBuckets = new IntHashSet[arms.length][];
    final VertexPredicate.Evaluation[][] armFilters = new VertexPredicate.Evaluation[arms.length][];
    final NeighborView[] firstHopViews = new NeighborView[arms.length];
    for (int a = 0; a < arms.length; a++) {
      armBuckets[a] = arms[a].endpointBuckets(db);
      armFilters[a] = VertexPredicate.evaluations(arms[a].endpointPredicates, db, provider);
      if (arms[a].edgeTypes.length == 1)
        firstHopViews[a] = provider.getNeighborView(arms[a].directions[0], arms[a].edgeTypes[0]);
    }

    // mandatory arms that read adjacency only, then the central predicate, then the mandatory arms that read vertices,
    // then the optional arms, which never rule a central node out
    final int[] order = new int[arms.length];
    int ordered = 0;
    for (int a = 0; a < arms.length; a++)
      if (!arms[a].optional && !arms[a].hasEndpointPredicate())
        order[ordered++] = a;
    final int adjacencyOnly = ordered;
    for (int a = 0; a < arms.length; a++)
      if (!arms[a].optional && arms[a].hasEndpointPredicate())
        order[ordered++] = a;
    for (int a = 0; a < arms.length; a++)
      if (arms[a].optional)
        order[ordered++] = a;

    long total = 0;
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v) || (centralBuckets != null && !centralBuckets.contains(bucketIds[v])))
        continue;
      long product = 1;
      for (int i = 0; i < order.length && product != 0; i++) {
        if (i == adjacencyOnly && central != null && !central.acceptsNode(v)) {
          product = 0;
          break;
        }
        final int a = order[i];
        final long degree = filteredArmDegree(provider, v, arms[a], firstHopViews[a], armBuckets[a], armFilters[a], bucketIds);
        product *= arms[a].optional ? Math.max(1, degree) : degree;
      }
      // every arm adjacency-only: the central predicate was not reached in the loop
      if (product != 0 && adjacencyOnly == order.length && central != null && !central.acceptsNode(v))
        product = 0;
      total += product;
    }
    return total;
  }

  /**
   * The paths of one arm from a central node, each node it reaches checked against the hop's label and property
   * predicate. An undirected hop reaches a self loop once, as the pattern matches it.
   */
  private static long filteredArmDegree(final GraphTraversalProvider provider, final int centralNode, final Arm arm,
      final NeighborView firstHopView, final IntHashSet[] buckets, final VertexPredicate.Evaluation[] filters,
      final int[] bucketIds) {
    if (arm.edgeTypes.length == 1 && firstHopView != null) {
      final int[] neighbors = firstHopView.neighbors();
      final boolean undirected = arm.directions[0] == Vertex.DIRECTION.BOTH;
      long count = 0;
      boolean skipSelf = false;
      for (int j = firstHopView.offset(centralNode), end = firstHopView.offsetEnd(centralNode); j < end; j++) {
        final int neighbor = neighbors[j];
        if (undirected && neighbor == centralNode) {
          // A SELF LOOP IS TWO ENTRIES OF THE MERGED RANGE AND ONE RELATIONSHIP (ISSUES #8750, #9540)
          skipSelf = !skipSelf;
          if (!skipSelf)
            continue;
        }
        if (reaches(neighbor, 0, buckets, filters, bucketIds))
          count++;
      }
      return count;
    }

    int[] frontier = { centralNode };
    for (int h = 0; h < arm.edgeTypes.length; h++) {
      int size = 0;
      int[] next = new int[0];
      for (final int node : frontier)
        for (final int neighbor : CSRCountUtils.hopNeighborIds(provider, node, arm.directions[h], arm.edgeTypes[h]))
          if (reaches(neighbor, h, buckets, filters, bucketIds)) {
            if (size == next.length)
              next = Arrays.copyOf(next, Math.max(8, size * 2));
            next[size++] = neighbor;
          }
      if (size == 0)
        return 0;
      frontier = size == next.length ? next : Arrays.copyOf(next, size);
    }
    return frontier.length;
  }

  /** Whether a node an arm reaches at {@code hop} passes the hop's label and property predicate. */
  private static boolean reaches(final int node, final int hop, final IntHashSet[] buckets,
      final VertexPredicate.Evaluation[] filters, final int[] bucketIds) {
    if (buckets != null && buckets[hop] != null && !buckets[hop].contains(bucketIds[node]))
      return false;
    return filters == null || filters[hop] == null || filters[hop].acceptsNode(node);
  }

  /**
   * Whether every edge of the arm's type and direction already ends in the label's buckets, which makes the label a
   * no-op for this arm. The edges that end in the label's vertices are counted off the opposite direction's degrees
   * (one pass over the nodes, not over the edges) and compared to all the edges of the type.
   */
  private static boolean endpointLabelIsImplied(final GraphTraversalProvider provider, final Arm arm, final NeighborView view,
      final IntHashSet farBuckets, final int[] bucketIds, final int nodeIdUpperBound, final WorkGuard guard) {
    final Vertex.DIRECTION direction = arm.directions[0];
    if (direction == Vertex.DIRECTION.BOTH)
      return false;
    final Vertex.DIRECTION opposite = direction == Vertex.DIRECTION.OUT ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT;
    final NeighborView reverse = provider.getNeighborView(opposite, arm.edgeTypes[0]);
    if (reverse == null || reverse.edgeCount() != view.edgeCount())
      return false;
    long endingInLabel = 0;
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (provider.isNodeLive(v) && farBuckets.contains(bucketIds[v]))
        endingInLabel += reverse.degree(v);
    }
    return endingInLabel == view.edgeCount();
  }

  /**
   * The degree of every node counting only the neighbors whose bucket is in {@code farBuckets}, all of them when it is
   * null. An undirected view holds each self loop twice, once per adjacency list of its vertex, and the relationship
   * pattern matches it once: one of the two copies is left out (#9539).
   */
  private static int[] filteredDegrees(final GraphTraversalProvider provider, final NeighborView view,
      final IntHashSet farBuckets, final int[] bucketIds, final boolean undirected, final int nodeIdUpperBound,
      final WorkGuard guard) {
    final int[] degrees = new int[nodeIdUpperBound];
    final int[] neighbors = view.neighbors();
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      int count = 0;
      int selfCopies = 0;
      for (int j = view.offset(v), end = view.offsetEnd(v); j < end; j++) {
        final int neighbor = neighbors[j];
        if (farBuckets == null || farBuckets.contains(bucketIds[neighbor])) {
          count++;
          if (neighbor == v)
            selfCopies++;
        }
      }
      degrees[v] = undirected ? count - selfCopies / 2 : count;
    }
    return degrees;
  }

  /**
   * The bucket id of every node, {@code -1} for a node that is not live: bucket 0 is a real bucket, so a default of 0 would
   * make a non-live neighbor read as a member of whatever label owns bucket 0.
   */
  private static int[] precomputeBucketIds(final GraphTraversalProvider provider, final int nodeIdUpperBound,
      final WorkGuard guard) {
    final int[] bucketIds = new int[nodeIdUpperBound];
    Arrays.fill(bucketIds, -1);
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (provider.isNodeLive(v))
        bucketIds[v] = provider.getRID(v).getBucketId();
    }
    return bucketIds;
  }

  /**
   * Vectorized degree-product scan using pre-fetched NeighborView offset arrays.
   * Pure array arithmetic in the hot loop — no method calls, no object allocation.
   * <p>
   * For Q4/Q7 with ~5M CSR nodes and 4 arms: ~40M array reads at ~1ns = ~40ms.
   * Compared to per-node countEdges: ~20M method calls at ~150ns = ~3s (75x slower).
   */
  private long executeFastScan(final GraphTraversalProvider provider, final NeighborView[] armViews,
      final int[][] filteredDegrees, final int nodeIdUpperBound, final WorkGuard guard) {
    // Reorder: check mandatory arms first for early exit, optional arms last
    final int[] mandatoryIdx = new int[arms.length];
    final int[] optionalIdx = new int[arms.length];
    int mandatoryCount = 0, optionalCount = 0;
    for (int a = 0; a < arms.length; a++) {
      if (arms[a].optional)
        optionalIdx[optionalCount++] = a;
      else
        mandatoryIdx[mandatoryCount++] = a;
    }

    long total = 0;
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      // Mandatory arms: skip if any degree is 0
      long product = 1;
      boolean skip = false;
      for (int i = 0; i < mandatoryCount; i++) {
        final int[] filtered = filteredDegrees == null ? null : filteredDegrees[mandatoryIdx[i]];
        final int degree = filtered != null ? filtered[v] : armViews[mandatoryIdx[i]].degree(v);
        if (degree == 0) {
          skip = true;
          break;
        }
        product *= degree;
      }
      if (skip)
        continue;

      // Optional arms: use max(1, degree)
      for (int i = 0; i < optionalCount; i++) {
        final int[] filtered = filteredDegrees == null ? null : filteredDegrees[optionalIdx[i]];
        product *= Math.max(1, filtered != null ? filtered[v] : armViews[optionalIdx[i]].degree(v));
      }

      total += product;
    }
    return total;
  }

  /**
   * Pre-compute degree arrays via bulk getDegrees, then scan them.
   * <p>
   * Uses the provider's bulk getDegrees API which computes degrees directly from
   * CSR offset arrays in a single pass per arm — no per-node HashMap lookups,
   * no volatile reads, no method dispatch. For 5M nodes × 4 arms:
   * Bulk: 4 array scans × 5M reads ≈ 40ms.
   * Per-node countEdges: 20M method calls × 150ns ≈ 3s.
   */
  private long executePerNode(final GraphTraversalProvider provider, final IntHashSet[][] armBuckets,
      final int[] bucketIds, final int nodeIdUpperBound, final WorkGuard guard) {
    // Pre-compute degree arrays: one int[] per arm, indexed by nodeId
    final int[][] armDegrees = new int[arms.length][];
    for (int a = 0; a < arms.length; a++) {
      final int[] degrees = new int[nodeIdUpperBound];
      if (armBuckets[a] != null && arms[a].edgeTypes.length == 1) {
        // One hop: count the neighbors of the label directly, without the frontier arrays walkArm builds per vertex
        final IntHashSet far = armBuckets[a][0];
        for (int v = 0; v < nodeIdUpperBound; v++) {
          guard.checkPeriodically(v);
          if (!provider.isNodeLive(v))
            continue;
          int count = 0;
          for (final int neighbor : CSRCountUtils.hopNeighborIds(provider, v, arms[a].directions[0], arms[a].edgeTypes[0]))
            if (far.contains(bucketIds[neighbor]))
              count++;
          degrees[v] = count;
        }
      } else if (armBuckets[a] != null) {
        // A labelled multi-hop arm counts only the endpoints of the label, hop by hop (#6337)
        for (int v = 0; v < nodeIdUpperBound; v++) {
          guard.checkPeriodically(v);
          if (!provider.isNodeLive(v))
            continue;
          degrees[v] = CSRCountUtils.walkArm(provider, v, arms[a].edgeTypes, arms[a].directions, armBuckets[a]).length;
        }
      } else if (arms[a].edgeTypes.length == 1 && arms[a].directions[0] != Vertex.DIRECTION.BOTH) {
        // Bulk degree computation — single pass over CSR offset arrays. Not for an undirected arm, whose bulk degree
        // counts a self loop twice: that one walks its neighbors below, which keep one copy of it (#9539)
        provider.getDegrees(degrees, arms[a].directions[0], arms[a].edgeTypes[0]);
      } else {
        for (int v = 0; v < nodeIdUpperBound; v++) {
          guard.checkPeriodically(v);
          if (!provider.isNodeLive(v))
            continue;
          degrees[v] = CSRCountUtils.walkArm(provider, v, arms[a].edgeTypes, arms[a].directions).length;
        }
      }
      armDegrees[a] = degrees;
    }

    // Reorder: mandatory arms first for early exit
    final int[] mandatoryIdx = new int[arms.length];
    final int[] optionalIdx = new int[arms.length];
    int mandatoryCount = 0, optionalCount = 0;
    for (int a = 0; a < arms.length; a++) {
      if (arms[a].optional)
        optionalIdx[optionalCount++] = a;
      else
        mandatoryIdx[mandatoryCount++] = a;
    }

    // Tight scan loop: pure array arithmetic, no method calls
    long total = 0;
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      long product = 1;
      boolean skip = false;
      for (int i = 0; i < mandatoryCount; i++) {
        final int degree = armDegrees[mandatoryIdx[i]][v];
        if (degree == 0) {
          skip = true;
          break;
        }
        product *= degree;
      }
      if (skip)
        continue;
      for (int i = 0; i < optionalCount; i++)
        product *= Math.max(1, armDegrees[optionalIdx[i]][v]);
      total += product;
    }
    return total;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    // The degree of each arm is read from the edge lists of the central vertices, never from the records of the edge
    // type: an edge may keep no record at all (light edges, declared LIGHTWEIGHT or not, alone or mixed with record
    // edges in one type), and a degree built by iterating the type's records answers 0 for them (#9484). The walk is
    // kept as cheap as the record scan it replaced by reading each edge list once for every arm leaving in its
    // direction, and by checking the far-end label on the bucket the entry already carries instead of looking up the
    // neighbor (#9539).
    return new EdgeListDegreeCounter((DatabaseInternal) db).count(guard);
  }

  private boolean hasMandatoryArm() {
    for (final Arm arm : arms)
      if (!arm.optional)
        return true;
    return false;
  }

  /**
   * The out-of-view count over the edge lists of the central vertices. Built per execution, because the operator itself
   * may be shared by concurrent executions of a cached plan and this holds the per-vertex scratch counters.
   * <p>
   * The single-hop arms, which is every arm of the LSQB star shapes, are counted on raw edge-list entries: one walk of
   * the OUT list and one of the IN list per central vertex, each answering every arm in that direction at once through
   * {@link EdgeLinkedList#countInto}. No edge record and no neighbor record is read: the edge type is the entry's edge
   * bucket and the far-end label is the entry's vertex bucket. A multi-hop arm has to load the vertices it passes
   * through to reach their edge lists, but only those its intermediate labels accept, and its last hop is counted on
   * raw entries too.
   */
  private final class EdgeListDegreeCounter {
    private final DatabaseInternal   database;
    private final GraphEngine        graphEngine;
    /** [arm][hop]: the buckets of the hop's edge type, null when the type holds no edge at all. */
    private final EdgeBucketMask[][] edgeMasks;
    /** [arm][hop]: the buckets of the hop's endpoint label, null when the hop has no label. */
    private final EdgeBucketMask[][] reachedMasks;
    /** True for an arm that cannot match any edge: an undeclared edge type, or an endpoint label with no bucket. */
    private final boolean[]          matchesNothing;
    private final DirectionGroup     outGroup;
    private final DirectionGroup     inGroup;
    private final int[]              multiHopArms;
    private final long[]             armCounts;
    /** [arm]: the one-filter arguments the last hop of a multi-hop arm counts with, built once instead of per call. */
    private final EdgeBucketMask[][] lastHopEdgeMasks;
    private final EdgeBucketMask[][] lastHopNeighborMasks;
    private final long[]             lastHopCount = new long[1];
    /** [arm][hop]: the property predicate of the node the hop reaches, asked by RID; null where there is none. */
    private final VertexPredicate.Evaluation[][] armFilters;
    private final VertexPredicate.Evaluation     centralFilter;
    private       WorkGuard          guard;
    private       int                neighborsVisited;

    private EdgeListDegreeCounter(final DatabaseInternal database) {
      this.database = database;
      this.graphEngine = database.getGraphEngine();
      this.edgeMasks = new EdgeBucketMask[arms.length][];
      this.reachedMasks = new EdgeBucketMask[arms.length][];
      this.matchesNothing = new boolean[arms.length];
      this.armCounts = new long[arms.length];
      this.armFilters = new VertexPredicate.Evaluation[arms.length][];
      this.centralFilter = centralPredicate != null ? centralPredicate.evaluation(database, null) : null;

      int multiHop = 0;
      for (int a = 0; a < arms.length; a++) {
        final Arm arm = arms[a];
        armFilters[a] = VertexPredicate.evaluations(arm.endpointPredicates, database, null);
        final IntHashSet[] endpointBuckets = arm.endpointBuckets(database);
        edgeMasks[a] = new EdgeBucketMask[arm.edgeTypes.length];
        reachedMasks[a] = new EdgeBucketMask[arm.edgeTypes.length];
        for (int h = 0; h < arm.edgeTypes.length; h++) {
          // An undeclared edge type has no edges: an OPTIONAL arm over it contributes 1 per central vertex instead of
          // failing the query, and a mandatory one makes the whole count 0 (#5790)
          edgeMasks[a][h] = EdgeBucketMask.of(database, new String[] { arm.edgeTypes[h] });
          if (edgeMasks[a][h] == null)
            matchesNothing[a] = true;
          if (endpointBuckets != null && endpointBuckets[h] != null) {
            reachedMasks[a][h] = EdgeBucketMask.ofBucketIds(endpointBuckets[h].toArray());
            // A label the schema does not know, or one without buckets, is reached by nothing (#6337)
            if (reachedMasks[a][h] == null)
              matchesNothing[a] = true;
          }
        }
        if (walksPaths(a))
          ++multiHop;
      }

      this.multiHopArms = new int[multiHop];
      this.lastHopEdgeMasks = new EdgeBucketMask[arms.length][];
      this.lastHopNeighborMasks = new EdgeBucketMask[arms.length][];
      multiHop = 0;
      for (int a = 0; a < arms.length; a++)
        if (walksPaths(a)) {
          multiHopArms[multiHop++] = a;
          final int lastHop = arms[a].edgeTypes.length - 1;
          lastHopEdgeMasks[a] = new EdgeBucketMask[] { edgeMasks[a][lastHop] };
          lastHopNeighborMasks[a] = new EdgeBucketMask[] { reachedMasks[a][lastHop] };
        }

      // OUT is walked first, so only an OUT arm is settled by it; a BOTH arm is settled once the IN list is walked too,
      // where it leaves out the self loops the OUT list already gave it
      this.outGroup = buildGroup(Vertex.DIRECTION.OUT, Vertex.DIRECTION.OUT);
      this.inGroup = buildGroup(Vertex.DIRECTION.IN, null);
    }

    /**
     * The single-hop arms that read the edge list of the given direction: those going that way and the BOTH ones.
     * {@code settledDirection} names the arms whose count is complete after this list, null for all of them.
     */
    private DirectionGroup buildGroup(final Vertex.DIRECTION direction, final Vertex.DIRECTION settledDirection) {
      int size = 0;
      int settled = 0;
      for (int a = 0; a < arms.length; a++)
        if (readsList(a, direction)) {
          ++size;
          if (settles(a, settledDirection))
            ++settled;
        }
      if (size == 0)
        return null;

      final DirectionGroup group = new DirectionGroup(direction, size, settled);
      size = 0;
      settled = 0;
      for (int a = 0; a < arms.length; a++)
        if (readsList(a, direction)) {
          group.arms[size] = a;
          group.edgeMasks[size] = edgeMasks[a][0];
          group.neighborMasks[size] = reachedMasks[a][0];
          group.skipSelfLoops[size] = direction == Vertex.DIRECTION.IN && arms[a].directions[0] == Vertex.DIRECTION.BOTH;
          ++size;
          if (settles(a, settledDirection))
            group.settledMandatoryArms[settled++] = a;
        }
      return group;
    }

    private boolean readsList(final int a, final Vertex.DIRECTION direction) {
      final Arm arm = arms[a];
      return arm.edgeTypes.length == 1 && !matchesNothing[a] && !arm.hasEndpointPredicate()
          && (arm.directions[0] == direction || arm.directions[0] == Vertex.DIRECTION.BOTH);
    }

    /**
     * Whether the arm is counted by walking its paths rather than off one raw edge-list pass: a multi-hop arm, and one
     * whose reached node carries a property predicate, which only the vertex itself can answer (issue #9595).
     */
    private boolean walksPaths(final int a) {
      return !matchesNothing[a] && (arms[a].edgeTypes.length > 1 || arms[a].hasEndpointPredicate());
    }

    private boolean settles(final int a, final Vertex.DIRECTION settledDirection) {
      return !arms[a].optional && (settledDirection == null || arms[a].directions[0] == settledDirection);
    }

    private long count(final WorkGuard guard) {
      this.guard = guard;
      for (int a = 0; a < arms.length; a++)
        if (matchesNothing[a] && !arms[a].optional)
          return 0;

      long total = 0;
      for (final Iterator<? extends Identifiable> it = database.iterateType(centralLabel, true); it.hasNext(); ) {
        guard.check();
        final VertexInternal vertex = (VertexInternal) it.next().asVertex();
        Arrays.fill(armCounts, 0);
        if (!countList(vertex, outGroup) || !countList(vertex, inGroup))
          continue;
        // the scan already holds the central vertex, so its predicate reads nothing more
        if (centralFilter != null && !centralFilter.accepts(vertex))
          continue;

        boolean skip = false;
        for (final int a : multiHopArms) {
          armCounts[a] = countPath(vertex, a, 0);
          if (armCounts[a] == 0 && !arms[a].optional) {
            skip = true;
            break;
          }
        }
        if (skip)
          continue;

        // a mandatory arm at 0 never gets here: its direction group or the multi-hop loop skipped the vertex
        long product = 1;
        for (int a = 0; a < arms.length; a++)
          product *= arms[a].optional ? Math.max(1, armCounts[a]) : armCounts[a];
        total += product;
      }
      return total;
    }

    /** Adds the counts of the group's arms read off one edge list; false when a mandatory arm it settles is at 0. */
    private boolean countList(final VertexInternal vertex, final DirectionGroup group) {
      if (group == null)
        return true;
      final EdgeLinkedList list = graphEngine.getEdgeHeadChunk(vertex, group.direction);
      if (list != null) {
        Arrays.fill(group.counts, 0);
        list.countInto(group.edgeMasks, group.neighborMasks, group.skipSelfLoops, group.counts);
        for (int i = 0; i < group.arms.length; i++)
          armCounts[group.arms[i]] += group.counts[i];
      }
      for (final int a : group.settledMandatoryArms)
        if (armCounts[a] == 0)
          return false;
      return true;
    }

    /**
     * The paths of a multi-hop arm from {@code vertex}, starting at {@code hop}. An undirected hop reads both lists of the
     * vertex and takes a self loop from the OUT one only, as the single-hop arms do.
     */
    private long countPath(final VertexInternal vertex, final int a, final int hop) {
      final Arm arm = arms[a];
      final Vertex.DIRECTION direction = arm.directions[hop];
      final boolean undirected = direction == Vertex.DIRECTION.BOTH;
      if (hop == arm.edgeTypes.length - 1 && (armFilters[a] == null || armFilters[a][hop] == null)) {
        // THE LAST HOP IS COUNTED ON RAW ENTRIES, ITS LABEL ON THE ENTRY'S VERTEX BUCKET
        final EdgeBucketMask[] edgeMask = lastHopEdgeMasks[a];
        final EdgeBucketMask[] neighborMask = lastHopNeighborMasks[a];
        lastHopCount[0] = 0;
        if (direction != Vertex.DIRECTION.IN) {
          final EdgeLinkedList list = graphEngine.getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
          if (list != null)
            list.countInto(edgeMask, neighborMask, null, lastHopCount);
        }
        if (direction != Vertex.DIRECTION.OUT) {
          final EdgeLinkedList list = graphEngine.getEdgeHeadChunk(vertex, Vertex.DIRECTION.IN);
          if (list != null)
            list.countInto(edgeMask, neighborMask, undirected ? SKIP_SELF_LOOP : null, lastHopCount);
        }
        return lastHopCount[0];
      }

      long count = 0;
      if (direction != Vertex.DIRECTION.IN)
        count += countNextHop(vertex, a, hop, Vertex.DIRECTION.OUT, false);
      if (direction != Vertex.DIRECTION.OUT)
        count += countNextHop(vertex, a, hop, Vertex.DIRECTION.IN, undirected);
      return count;
    }

    private long countNextHop(final VertexInternal vertex, final int a, final int hop, final Vertex.DIRECTION direction,
        final boolean skipSelfLoops) {
      final EdgeBucketMask reached = reachedMasks[a][hop];
      final VertexPredicate.Evaluation filter = armFilters[a] != null ? armFilters[a][hop] : null;
      final boolean lastHop = hop == arms[a].edgeTypes.length - 1;
      final RID self = vertex.getIdentity();
      long count = 0;
      for (final RID neighbor : graphEngine.getConnectedVertexRIDs(vertex, direction, arms[a].edgeTypes[hop])) {
        // a multi-hop arm out of one super-node can walk a large part of the graph: keep it interruptible
        guard.checkPeriodically(neighborsVisited++);
        // the label is checked on the RID, so a neighbor it rejects is never looked up
        if ((reached != null && !reached.matches(neighbor.getBucketId())) || (skipSelfLoops && neighbor.equals(self)))
          continue;
        // the property predicate reads the neighbor, once per neighbor across the whole count (issue #9595)
        if (filter != null && !filter.acceptsRid(neighbor))
          continue;
        count += lastHop ? 1 : countPath((VertexInternal) database.lookupByRID(neighbor, false), a, hop + 1);
      }
      return count;
    }
  }

  /** The single-hop arms answered by one walk of the edge list of one direction. */
  private static final class DirectionGroup {
    private final Vertex.DIRECTION direction;
    private final int[]            arms;
    private final EdgeBucketMask[] edgeMasks;
    private final EdgeBucketMask[] neighborMasks;
    /** True for an undirected arm read off the IN list: its self loops were already counted on the OUT list. */
    private final boolean[]        skipSelfLoops;
    private final long[]           counts;
    /** The mandatory arms whose count is complete after this list: one at 0 ends the vertex. */
    private final int[]            settledMandatoryArms;

    private DirectionGroup(final Vertex.DIRECTION direction, final int size, final int settled) {
      this.direction = direction;
      this.arms = new int[size];
      this.edgeMasks = new EdgeBucketMask[size];
      this.neighborMasks = new EdgeBucketMask[size];
      this.skipSelfLoops = new boolean[size];
      this.counts = new long[size];
      this.settledMandatoryArms = new int[settled];
    }
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT STAR JOIN (CSR degree product)\n");
    sb.append(ind).append("  central: ").append(centralLabel);
    if (centralPredicate != null)
      sb.append(' ').append(centralPredicate.describe());
    sb.append(", arms: ").append(arms.length);
    for (int i = 0; i < arms.length; i++) {
      sb.append("\n").append(ind).append("  arm ").append(i).append(arms[i].optional ? " [OPTIONAL]" : "").append(": ");
      for (int j = 0; j < arms[i].edgeTypes.length; j++) {
        if (j > 0) sb.append(" → ");
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
        sb.append(arms[i].edgeTypes[j]);
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "]->" : "]-");
        final String label = arms[i].endpointLabels != null ? arms[i].endpointLabels[j] : null;
        final VertexPredicate predicate = arms[i].endpointPredicates != null ? arms[i].endpointPredicates[j] : null;
        if (label != null || predicate != null) {
          sb.append("(");
          if (label != null)
            sb.append(':').append(label);
          if (predicate != null)
            sb.append(label != null ? " " : "").append(predicate.describe());
          sb.append(")");
        }
      }
    }
    return sb.toString();
  }
}
