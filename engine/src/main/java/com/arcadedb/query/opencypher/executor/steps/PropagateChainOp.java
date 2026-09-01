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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;

import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.RidLongHashMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Count operator for linear chain patterns (Q1, Q5, Q6).
 * Propagates path counts level-by-level through the CSR adjacency structure
 * using two {@code long[]} arrays indexed by dense node IDs.
 */
public final class PropagateChainOp implements CountOp {
  private final String[]           nodeLabels;
  private final String[]           edgeTypes;
  private final Vertex.DIRECTION[] directions;
  private final int                inequalityIdxA;
  private final int                inequalityIdxB;
  private final RID                anchorRid;

  public PropagateChainOp(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions,
      final int inequalityIdxA, final int inequalityIdxB) {
    this(nodeLabels, edgeTypes, directions, inequalityIdxA, inequalityIdxB, null);
  }

  /**
   * @param anchorRid the one vertex position 0 is already bound to, or null when position 0 is a label whose buckets
   *                  are enumerated. A seeded anchor is what keeps the push-down for a <b>correlated</b> body: the
   *                  outer row bound the start of the chain, so the same propagation runs from that single node
   *                  instead of from every vertex carrying a label (issue #5758).
   */
  public PropagateChainOp(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions,
      final int inequalityIdxA, final int inequalityIdxB, final RID anchorRid) {
    this.nodeLabels = nodeLabels;
    this.edgeTypes = edgeTypes;
    this.directions = directions;
    this.inequalityIdxA = inequalityIdxA;
    this.inequalityIdxB = inequalityIdxB;
    this.anchorRid = anchorRid;
  }

  @Override
  public String[] edgeTypes() {
    return edgeTypes;
  }

  /**
   * The chain is walked from position 0. With no label there, the anchors are every vertex, which only a provider
   * whose node domain <i>is</i> every vertex can enumerate (issue #5757). An unlabelled position seeded from the
   * outer row's bound vertex (issue #5758) is still a single unlabelled anchor, so the same full-coverage
   * requirement decides whether the seeded walk may use a CSR provider too.
   */
  @Override
  public boolean requiresFullVertexCoverage() {
    return nodeLabels[0] == null;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    if (anchorRid != null)
      return executeFromSeededAnchor(provider, db, guard);

    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    final int hops = edgeTypes.length;

    // Pre-compute valid bucket sets for type filtering at each level. null = unlabelled, so no filter.
    final IntHashSet[] validBuckets = new IntHashSet[hops + 1];
    for (int i = 0; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    // When inequality is present and the inequality source is at position 0,
    // choose between per-source expansion and dense propagation.
    // Dense propagation + self-loop subtraction is preferred because it processes
    // all edges sequentially (cache-friendly). The self-loop subtraction uses
    // edge-scan with sorted merge for 3-hop sub-chains (O(|E_middle|) ops),
    // or per-anchor frontier expansion for shorter sub-chains.
    // Only fall back to per-source for very short chains (≤ 2 hops) where the
    // dense array allocation overhead isn't justified.
    if (inequalityIdxA >= 0 && inequalityIdxB >= 0
        && Math.min(inequalityIdxA, inequalityIdxB) == 0
        && hops <= 2)
      return executePerSourceInequality(provider, nodeIdUpperBound, validBuckets, guard);

    // Standard dense array propagation for chains without inequality
    // (or with inequality source not at position 0)

    // Pre-compute bucket IDs once for all filtering operations. A pattern that labels no position needs none of
    // them, and the scan costs one getRID per node.
    final int[] bucketIds = anyLabelled(validBuckets)
        ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;

    // Initialize anchor counts via bucket filtering (avoids OLTP iterateType). An unlabelled anchor accepts every
    // vertex, which is the whole node domain (issue #5757).
    long[] current = new long[nodeIdUpperBound];
    final IntHashSet anchorBuckets = validBuckets[0];
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      if (anchorBuckets == null || anchorBuckets.contains(bucketIds[v]))
        current[v] = 1;
    }

    for (int hop = 0; hop < hops; hop++) {
      // One hop is a full O(V + E) pass, so a per-hop check is the natural granularity here - the pass itself is
      // sequential array arithmetic that a per-element check would only slow down (issue #6266).
      guard.check();
      current = CSRCountUtils.propagateOneHop(provider, current, directions[hop], edgeTypes[hop]);
      CSRCountUtils.filterByBuckets(bucketIds, current, validBuckets[hop + 1]);
    }

    long total = 0;
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      total += current[v];
    }

    if (inequalityIdxA >= 0 && inequalityIdxB >= 0)
      total -= countSelfLoopPaths(provider, validBuckets, guard);

    return total;
  }

  /** Whether any position of the chain carries a label, i.e. whether per-node bucket ids are worth computing. */
  private static boolean anyLabelled(final IntHashSet[] validBuckets) {
    for (final IntHashSet buckets : validBuckets)
      if (buckets != null)
        return true;
    return false;
  }

  /**
   * The chain walked from the one vertex the outer row bound to position 0.
   * <p>
   * Nothing here is sized by the graph: the dense {@code long[nodeCount]} arrays of the enumerating path would be
   * allocated once per outer row, which is the cost this push-down exists to avoid. The walk is a sparse frontier
   * instead - one entry per partial path - and the <b>last</b> hop is never expanded at all, only counted, so a
   * one-hop body costs a degree read of the anchor's adjacency and allocates nothing per neighbour.
   * <p>
   * A label written on the seeded position filters the bound vertex rather than naming a set to enumerate:
   * {@code COUNT { (n:Q)-[:LINKS]->(:Q) }} under an {@code n} of another label is 0, not the count over {@code Q}.
   */
  private long executeFromSeededAnchor(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    if (!anchorMatchesLabel(db, nodeLabels[0], anchorRid.getBucketId()))
      return 0;

    final int hops = edgeTypes.length;
    // Position 0 is one known bucket, tested above without building a set for it: this runs once per outer row.
    final IntHashSet[] validBuckets = new IntHashSet[hops + 1];
    for (int i = 1; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    final int anchorId = provider.getNodeId(anchorRid);
    if (anchorId < 0)
      return 0;

    int[] frontier = new int[] { anchorId };
    for (int h = 0; h < hops - 1; h++) {
      guard.check();
      frontier = expandFrontier(provider, frontier, provider.getNeighborView(directions[h], edgeTypes[h]), h,
          validBuckets[h + 1]);
      if (frontier.length == 0)
        return 0;
    }

    final int lastHop = hops - 1;
    final IntHashSet targetBuckets = validBuckets[hops];
    long total = 0;
    if (targetBuckets == null || targetBuckets.isEmpty()) {
      for (final int node : frontier)
        total += provider.countEdges(node, directions[lastHop], edgeTypes[lastHop]);
      return total;
    }

    final NeighborView lastView = provider.getNeighborView(directions[lastHop], edgeTypes[lastHop]);
    if (lastView != null) {
      final int[] nbrs = lastView.neighbors();
      for (final int node : frontier)
        for (int j = lastView.offset(node), end = lastView.offsetEnd(node); j < end; j++)
          if (targetBuckets.contains(provider.getRID(nbrs[j]).getBucketId()))
            total++;
    } else {
      for (final int node : frontier)
        for (final int neighbor : provider.getNeighborIds(node, directions[lastHop], edgeTypes[lastHop]))
          if (targetBuckets.contains(provider.getRID(neighbor).getBucketId()))
            total++;
    }
    return total;
  }

  /**
   * Whether the one bucket a seeded anchor lives in passes the label written on its position.
   * <p>
   * The same question {@link CSRCountUtils#buildValidBuckets} answers for a whole level, and read the same way - a
   * null or undeclared label builds no filter there, so it filters nothing here either - but asked of a single known
   * bucket, so it walks the supertype chain instead of allocating a bucket set. That matters because a seeded
   * operator is built and run once per outer row, where the enumerating one is built once per query.
   */
  private static boolean anchorMatchesLabel(final Database db, final String label, final int bucketId) {
    if (label == null || !db.getSchema().existsType(label))
      return true;

    // A bucket belongs to exactly one type, so "in this label's polymorphic buckets" is "of this label or below it".
    final DocumentType type = db.getSchema().getTypeByBucketId(bucketId);
    return type != null && type.instanceOf(label);
  }

  /**
   * Single-pass per-source expansion for chains with inequality starting at position 0.
   * <p>
   * For each anchor vertex, expands through the chain using sparse frontier arrays,
   * applies the inequality filter inline (excluding paths where endpoints match),
   * and computes tail contributions via degree lookups. This combines countAll and
   * countEqual into a single traversal, avoiding dense node-indexed arrays entirely.
   * <p>
   * For Q5 (16K tags × ~90 frontier nodes each): ~1.4M CSR ops vs ~6M for dense + self-loop.
   * For Q6 (10K persons × ~1.7K frontier nodes each): ~17M CSR ops (comparable to dense + self-loop).
   */
  private long executePerSourceInequality(final GraphTraversalProvider provider,
      final int nodeIdUpperBound, final IntHashSet[] validBuckets, final WorkGuard guard) {
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);

    // Pre-fetch NeighborViews for hops up to the inequality target
    final NeighborView[] views = new NeighborView[idxB];
    for (int h = 0; h < idxB; h++)
      views[h] = provider.getNeighborView(directions[h], edgeTypes[h]);

    // Pre-compute bucket IDs for all nodes — eliminates per-node getRID calls during
    // frontier type filtering. One-time O(nodeCount) cost, amortized across all sources.
    final int[] bucketIds = anyLabelled(validBuckets)
        ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;

    // Identify anchor nodes via bucket filtering (avoids db.iterateType OLTP reads). null accepts every vertex,
    // empty accepts none (issue #5757).
    final IntHashSet anchorBuckets = validBuckets[0];
    if (anchorBuckets != null && anchorBuckets.isEmpty())
      return 0;

    long totalCount = 0;

    for (int srcId = 0; srcId < nodeIdUpperBound; srcId++) {
      guard.check();
      if (!provider.isNodeLive(srcId))
        continue;
      if (anchorBuckets != null && !anchorBuckets.contains(bucketIds[srcId]))
        continue;

      // Expand from srcId through hops [0, idxB)
      int[] frontier = new int[]{srcId};
      for (int h = 0; h < idxB; h++) {
        frontier = expandFrontierFast(provider, frontier, views[h], h, validBuckets[h + 1], bucketIds);
        if (frontier.length == 0)
          break;
      }

      if (frontier.length == 0)
        continue;

      // frontier now contains nodes at position idxB (the inequality target).
      // Count contributions, excluding nodes that equal srcId (inequality filter).
      // For each surviving node, compute the "tail" count (remaining hops after idxB).
      for (final int p : frontier) {
        if (p == srcId)
          continue; // inequality: skip paths where endpoints match

        // Compute tail: degree product for hops [idxB, end)
        if (idxB >= edgeTypes.length) {
          totalCount++; // no tail hops — endpoint is the chain end
        } else {
          long tailCount = 1;
          for (int h = idxB; h < edgeTypes.length; h++) {
            tailCount *= provider.countEdges(p, directions[h], edgeTypes[h]);
            if (tailCount == 0)
              break;
          }
          totalCount += tailCount;
        }
      }
    }
    return totalCount;
  }

  private long countSelfLoopPaths(final GraphTraversalProvider provider,
      final IntHashSet[] validBuckets, final WorkGuard guard) {
    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    final int idxA = Math.min(inequalityIdxA, inequalityIdxB);
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);
    final int subChainLength = idxB - idxA;

    // FAST PATH: 3-hop self-loop via edge-scan with sorted merge intersection.
    // Instead of expanding per-anchor (O(|anchors| × frontier)), scan the middle edge
    // and compute |E0_reverse_nbrs(m) ∩ E2_nbrs(c)| for each middle edge (m,c).
    // For Q5: 2.6M REPLY_OF edges × ~2 merge ops = ~5M ops (vs 16K × 300 = ~14M per-anchor).
    if (subChainLength == 3 && idxA == 0 && idxB == edgeTypes.length) {
      return countSelfLoop3HopEdgeScan(provider, nodeIdUpperBound, validBuckets, guard);
    }

    // GENERAL PATH: per-anchor expansion
    long selfLoopTotal = 0;

    final NeighborView[] subViews = new NeighborView[subChainLength];
    for (int h = 0; h < subChainLength; h++)
      subViews[h] = provider.getNeighborView(directions[idxA + h], edgeTypes[idxA + h]);

    // The sub-chain is walked from every vertex the inequality's earlier position accepts: the label's own when it
    // carries one, the whole node domain when it does not (issue #5757). Enumerating them off the CSR bucket ids
    // rather than db.iterateType also keeps the self-loop subtraction from reading records.
    final IntHashSet anchorBuckets = validBuckets[idxA];
    if (anchorBuckets != null && anchorBuckets.isEmpty())
      return 0;
    final int[] anchorBucketIds = anchorBuckets == null ? null
        : precomputeBucketIds(provider, nodeIdUpperBound, guard);

    for (int vId = 0; vId < nodeIdUpperBound; vId++) {
      guard.check();
      if (!provider.isNodeLive(vId))
        continue;
      if (anchorBuckets != null && !anchorBuckets.contains(anchorBucketIds[vId]))
        continue;

      long loopCount = countLoopsFromNode(provider, vId, subViews, idxA, subChainLength, validBuckets);

      // Multiply by the remaining chain after idxB
      if (loopCount > 0 && idxB < edgeTypes.length) {
        long tailCount = 1;
        for (int h = idxB; h < edgeTypes.length; h++) {
          final long degree = provider.countEdges(vId, directions[h], edgeTypes[h]);
          if (degree == 0) {
            tailCount = 0;
            break;
          }
          tailCount *= degree;
        }
        loopCount *= tailCount;
      }

      // Multiply by prefix chain before idxA
      if (loopCount > 0 && idxA > 0) {
        long prefixCount = 1;
        for (int h = idxA - 1; h >= 0; h--) {
          final long degree = provider.countEdges(vId, directions[h], edgeTypes[h]);
          prefixCount *= degree;
          if (prefixCount == 0)
            break;
        }
        loopCount *= prefixCount;
      }

      selfLoopTotal += loopCount;
    }
    return selfLoopTotal;
  }

  /**
   * Fast self-loop counting for 3-hop full-chain sub-chains via edge scanning.
   * <p>
   * For chain (pos0) -[E0]- (pos1) -[E1]- (pos2) -[E2]- (pos3) where pos0 == pos3:
   * Iterates all E1 (middle edge) instances. For each edge connecting pos1 vertex 'b' to
   * pos2 vertex 'c': computes |reverse_E0_neighbors(b) ∩ E2_neighbors(c)| using sorted
   * merge on CSR neighbor arrays. No per-anchor iteration needed.
   * <p>
   * For Q5 (Tag←HAS_TAG-Message←REPLY_OF-Comment-HAS_TAG→Tag):
   * Scans 2.6M REPLY_OF edges, merge-counts tags(message) ∩ tags(comment).
   * Avg ~2 merge comparisons per edge → ~5M ops total at ~3-5ns = ~15-25ms.
   */
  private long countSelfLoop3HopEdgeScan(final GraphTraversalProvider provider,
      final int nodeIdUpperBound, final IntHashSet[] validBuckets, final WorkGuard guard) {
    // E0: edge from pos0 to pos1, direction directions[0]
    // E1: edge from pos1 to pos2, direction directions[1] (middle, iterated)
    // E2: edge from pos2 to pos3, direction directions[2]
    //
    // For self-loop: pos0 == pos3
    // reverse_E0_neighbors(pos1_node) = neighbors at pos0 reachable by reversing E0
    // E2_neighbors(pos2_node) = neighbors at pos3 reachable by following E2

    final Vertex.DIRECTION revDir0 = reverseDir(directions[0]);
    final NeighborView viewA = provider.getNeighborView(revDir0, edgeTypes[0]);
    final NeighborView viewE1 = provider.getNeighborView(directions[1], edgeTypes[1]);
    final NeighborView viewC = provider.getNeighborView(directions[2], edgeTypes[2]);

    if (viewA == null || viewE1 == null || viewC == null) {
      // Fallback: can't get NeighborViews, use per-anchor approach
      return countSelfLoopPerAnchorFallback(provider, nodeIdUpperBound, validBuckets, guard);
    }

    final int[] aNbrs = viewA.neighbors();
    final int[] e1Nbrs = viewE1.neighbors();
    final int[] cNbrs = viewC.neighbors();

    // Optional: filter middle edge endpoints by type. null is "no label, so no filter"; an empty set is a label that
    // matches nothing, and then no path exists to subtract at all (issue #5757).
    final IntHashSet pos1Buckets = validBuckets[1];
    final IntHashSet pos2Buckets = validBuckets[2];
    if ((pos1Buckets != null && pos1Buckets.isEmpty()) || (pos2Buckets != null && pos2Buckets.isEmpty()))
      return 0;
    final int[] bucketIds = (pos1Buckets != null || pos2Buckets != null)
        ? precomputeBucketIds(provider, nodeIdUpperBound, guard) : null;

    long selfLoop = 0;

    // Scan all E1 edges by iterating pos1 nodes
    for (int b = 0; b < nodeIdUpperBound; b++) {
      guard.check();
      if (!provider.isNodeLive(b))
        continue;
      // Check pos1 type filter
      if (pos1Buckets != null && !pos1Buckets.contains(bucketIds[b]))
        continue;

      final int e1Start = viewE1.offset(b);
      final int e1End = viewE1.offsetEnd(b);
      if (e1Start == e1End) continue;

      // Get setA = reverse-E0 neighbors of b (pos0 candidates)
      final int aStart = viewA.offset(b);
      final int aEnd = viewA.offsetEnd(b);
      if (aStart == aEnd) continue;

      // For each E1 neighbor c (pos2 node):
      for (int j = e1Start; j < e1End; j++) {
        final int c = e1Nbrs[j];

        // Check pos2 type filter
        if (pos2Buckets != null && !pos2Buckets.contains(bucketIds[c]))
          continue;

        // Get setC = E2 neighbors of c (pos3 candidates)
        final int cStart = viewC.offset(c);
        final int cEnd = viewC.offsetEnd(c);
        if (cStart == cEnd) continue;

        // Count |setA ∩ setC| via sorted merge (both CSR ranges are sorted)
        selfLoop += sortedIntersectionCount(aNbrs, aStart, aEnd, cNbrs, cStart, cEnd);
      }
    }
    return selfLoop;
  }

  /**
   * Counts the size of the intersection of two sorted int[] sub-arrays using merge scan.
   * O(|a| + |b|) time, O(1) space.
   */
  private static long sortedIntersectionCount(final int[] a, int aStart, final int aEnd,
      final int[] b, int bStart, final int bEnd) {
    long count = 0;
    while (aStart < aEnd && bStart < bEnd) {
      final int av = a[aStart], bv = b[bStart];
      if (av < bv) aStart++;
      else if (av > bv) bStart++;
      else { count++; aStart++; bStart++; }
    }
    return count;
  }

  private static Vertex.DIRECTION reverseDir(final Vertex.DIRECTION dir) {
    if (dir == Vertex.DIRECTION.OUT) return Vertex.DIRECTION.IN;
    if (dir == Vertex.DIRECTION.IN) return Vertex.DIRECTION.OUT;
    return Vertex.DIRECTION.BOTH;
  }

  /**
   * Fallback per-anchor self-loop when NeighborViews are unavailable.
   */
  private long countSelfLoopPerAnchorFallback(final GraphTraversalProvider provider,
      final int nodeIdUpperBound, final IntHashSet[] validBuckets, final WorkGuard guard) {
    // Reuse existing per-anchor expansion logic
    final int subChainLength = Math.max(inequalityIdxA, inequalityIdxB);
    final NeighborView[] subViews = new NeighborView[subChainLength];
    for (int h = 0; h < subChainLength; h++)
      subViews[h] = provider.getNeighborView(directions[h], edgeTypes[h]);

    final IntHashSet anchorBuckets = validBuckets[0];
    if (anchorBuckets != null && anchorBuckets.isEmpty())
      return 0;
    final int[] bucketIds = anchorBuckets == null ? null
        : precomputeBucketIds(provider, nodeIdUpperBound, guard);

    long selfLoopTotal = 0;
    for (int vId = 0; vId < nodeIdUpperBound; vId++) {
      guard.check();
      if (!provider.isNodeLive(vId))
        continue;
      if (anchorBuckets != null && !anchorBuckets.contains(bucketIds[vId]))
        continue;
      selfLoopTotal += countLoopsFromNode(provider, vId, subViews, 0, subChainLength, validBuckets);
    }
    return selfLoopTotal;
  }

  /**
   * Counts the number of paths from node v through the sub-chain that return to v.
   * <p>
   * Uses sparse frontier expansion instead of dense node-indexed arrays.
   * For each hop, only the actual reachable nodes are tracked (not all nodeCount entries).
   * At the final hop, uses binary search on sorted CSR neighbor arrays to count arrivals at v.
   * <p>
   * Complexity: O(reachable_per_source × hops) instead of O(nodeCount × hops).
   * For Q5 (16K tags, ~200K total nodes): ~210 ops per tag vs ~600K ops per tag.
   */
  private long countLoopsFromNode(final GraphTraversalProvider provider, final int vId,
      final NeighborView[] subViews, final int startHop, final int subChainLength,
      final IntHashSet[] validBuckets) {
    if (subChainLength == 0)
      return 1;

    // Sparse frontier: track only reached nodes as a compact int[] array.
    // Duplicates are allowed — each entry represents one path reaching that node.
    int[] frontier = new int[]{vId};

    // Expand through all hops except the last
    for (int h = 0; h < subChainLength - 1; h++) {
      frontier = expandFrontier(provider, frontier, subViews[h], startHop + h, validBuckets[startHop + h + 1]);
      if (frontier.length == 0)
        return 0;
    }

    // Last hop: instead of expanding fully, count only paths that arrive back at vId.
    // Use binary search on sorted CSR neighbor arrays for O(log d) per frontier node.
    final int lastHopIdx = startHop + subChainLength - 1;
    final IntHashSet lastBuckets = validBuckets[lastHopIdx + 1];
    // If there are type filters at the target position and vId doesn't match, no self-loops possible
    if (lastBuckets != null) {
      final RID vRid = provider.getRID(vId);
      if (!lastBuckets.contains(vRid.getBucketId()))
        return 0;
    }

    long loopCount = 0;
    final NeighborView lastView = subViews[subChainLength - 1];
    if (lastView != null) {
      final int[] nbrs = lastView.neighbors();
      for (final int node : frontier) {
        // Binary search for vId in node's sorted neighbor list
        final int start = lastView.offset(node);
        final int end = lastView.offsetEnd(node);
        if (Arrays.binarySearch(nbrs, start, end, vId) >= 0)
          loopCount++;
      }
    } else {
      for (final int node : frontier) {
        final int[] neighbors = provider.getNeighborIds(node, directions[lastHopIdx], edgeTypes[lastHopIdx]);
        if (Arrays.binarySearch(neighbors, vId) >= 0)
          loopCount++;
      }
    }
    return loopCount;
  }

  /**
   * Expands the frontier by one hop, returning the new frontier of reached nodes.
   * Applies type filtering at the target position if needed.
   */
  private int[] expandFrontier(final GraphTraversalProvider provider, final int[] frontier,
      final NeighborView view, final int hopIdx, final IntHashSet targetBuckets) {
    // Count total next-level nodes
    int totalNext = 0;
    if (view != null) {
      for (final int node : frontier)
        totalNext += view.degree(node);
    } else {
      for (final int node : frontier)
        totalNext += provider.getNeighborIds(node, directions[hopIdx], edgeTypes[hopIdx]).length;
    }
    if (totalNext == 0)
      return new int[0];

    // Expand
    final int[] next = new int[totalNext];
    int pos = 0;
    if (view != null) {
      final int[] nbrs = view.neighbors();
      for (final int node : frontier) {
        for (int j = view.offset(node), end = view.offsetEnd(node); j < end; j++)
          next[pos++] = nbrs[j];
      }
    } else {
      for (final int node : frontier) {
        final int[] neighbors = provider.getNeighborIds(node, directions[hopIdx], edgeTypes[hopIdx]);
        System.arraycopy(neighbors, 0, next, pos, neighbors.length);
        pos += neighbors.length;
      }
    }

    // Apply type filter if needed. An empty set is a label that matches nothing, so it empties the frontier.
    if (targetBuckets != null) {
      int writePos = 0;
      for (int i = 0; i < pos; i++) {
        final RID rid = provider.getRID(next[i]);
        if (targetBuckets.contains(rid.getBucketId()))
          next[writePos++] = next[i];
      }
      return Arrays.copyOf(next, writePos);
    }
    return pos < next.length ? Arrays.copyOf(next, pos) : next;
  }

  /**
   * Like expandFrontier but uses pre-computed bucketIds for type filtering.
   * Eliminates per-node getRID calls (~200ns each × millions of nodes).
   */
  private int[] expandFrontierFast(final GraphTraversalProvider provider, final int[] frontier,
      final NeighborView view, final int hopIdx, final IntHashSet targetBuckets,
      final int[] bucketIds) {
    int totalNext = 0;
    if (view != null) {
      for (final int node : frontier)
        totalNext += view.degree(node);
    } else {
      for (final int node : frontier)
        totalNext += provider.getNeighborIds(node, directions[hopIdx], edgeTypes[hopIdx]).length;
    }
    if (totalNext == 0)
      return new int[0];

    final int[] next = new int[totalNext];
    int pos = 0;
    if (view != null) {
      final int[] nbrs = view.neighbors();
      for (final int node : frontier) {
        for (int j = view.offset(node), end = view.offsetEnd(node); j < end; j++)
          next[pos++] = nbrs[j];
      }
    } else {
      for (final int node : frontier) {
        final int[] neighbors = provider.getNeighborIds(node, directions[hopIdx], edgeTypes[hopIdx]);
        System.arraycopy(neighbors, 0, next, pos, neighbors.length);
        pos += neighbors.length;
      }
    }

    if (targetBuckets != null) {
      int writePos = 0;
      for (int i = 0; i < pos; i++) {
        if (targetBuckets.contains(bucketIds[next[i]]))
          next[writePos++] = next[i];
      }
      return Arrays.copyOf(next, writePos);
    }
    return pos < next.length ? Arrays.copyOf(next, pos) : next;
  }

  /** Pre-computes bucket IDs for all live CSR nodes, amortized across all per-source expansions. */
  private static int[] precomputeBucketIds(final GraphTraversalProvider provider, final int nodeIdUpperBound,
      final WorkGuard guard) {
    final int[] bucketIds = new int[nodeIdUpperBound];
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      bucketIds[v] = provider.getRID(v).getBucketId();
    }
    return bucketIds;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    // Try to find a GAV provider for accelerated neighbor lookups even in the OLTP path. One that holds a subset of
    // the vertex types cannot serve: it answers for the vertices it maps with the adjacency it holds, which is
    // missing every edge that leaves the view (issue #5757).
    final GraphTraversalProvider provider = CSRCountUtils.findAcceleratingProvider(db, edgeTypes);

    // BFS count propagation: each unique vertex at each level is processed once,
    // with its count capturing path multiplicity. This reduces O(paths) traversals
    // to O(unique edges), which is dramatically faster for high-fanout chains.
    // E.g., Q5 with 13.8M paths but only ~3.5M unique edges: ~4x fewer OLTP scans.
    // An unlabelled anchor starts from every vertex in the schema (issue #5757).
    RidLongHashMap current = new RidLongHashMap();
    if (anchorRid != null) {
      // A seeded anchor is an anchor set of one, and its label - if the body wrote one - filters it (issue #5758).
      if (!anchorMatchesLabel(db, nodeLabels[0], anchorRid.getBucketId()))
        return 0;
      current.put(anchorRid, 1L);
    } else {
      for (final Iterator<? extends Identifiable> it = CSRCountUtils.iterateAnchors(db, nodeLabels[0]); it.hasNext(); ) {
        guard.check();
        current.put(it.next().getIdentity(), 1L);
      }
    }

    for (int hop = 0; hop < edgeTypes.length; hop++) {
      guard.check();
      final IntHashSet targetBuckets = CSRCountUtils.buildValidBuckets(db, nodeLabels[hop + 1]);

      final RidLongHashMap next = new RidLongHashMap();
      final int h = hop;
      current.forEach((bucketId, offset, pathCount) -> {
        guard.check();
        final RID rid = db.newRID(bucketId, offset);
        expandNeighbors(db, provider, rid, directions[h], edgeTypes[h], targetBuckets,
            neighborRid -> next.add(neighborRid, pathCount));
      });
      current = next;
    }

    final long[] total = {0};
    current.forEach((bucketId, offset, value) -> total[0] += value);

    // Subtract self-loop paths for inequality
    if (inequalityIdxA >= 0 && inequalityIdxB >= 0)
      total[0] -= countSelfLoopPathsOLTP(db, provider, guard);

    return total[0];
  }

  /**
   * OLTP self-loop counting: for each anchor vertex, BFS through the sub-chain
   * and check for paths that return to the anchor.
   */
  /**
   * Expands neighbors from a vertex RID, using GAV/CSR when available, falling back to OLTP.
   */
  private static void expandNeighbors(final Database db, final GraphTraversalProvider provider,
      final RID vertexRid, final Vertex.DIRECTION direction, final String edgeType,
      final IntHashSet targetBuckets, final Consumer<RID> consumer) {
    if (provider != null) {
      final int nodeId = provider.getNodeId(vertexRid);
      if (nodeId >= 0) {
        final int[] neighborIds = provider.getNeighborIds(nodeId, direction, edgeType);
        for (final int nid : neighborIds) {
          final RID neighborRid = provider.getRID(nid);
          if (neighborRid != null && (targetBuckets == null || targetBuckets.contains(neighborRid.getBucketId())))
            consumer.accept(neighborRid);
        }
        return;
      }
    }
    // OLTP fallback
    final Vertex v = (Vertex) db.lookupByRID(vertexRid, true);
    for (final RID neighborRid : v.getConnectedVertexRIDs(direction, edgeType)) {
      if (targetBuckets == null || targetBuckets.contains(neighborRid.getBucketId()))
        consumer.accept(neighborRid);
    }
  }

  private long countSelfLoopPathsOLTP(final Database db, final GraphTraversalProvider provider,
      final WorkGuard guard) {
    final int idxA = Math.min(inequalityIdxA, inequalityIdxB);
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);
    final int subLength = idxB - idxA;

    // One bucket set per sub-chain hop, built once rather than per anchor
    final IntHashSet[] subChainBuckets = new IntHashSet[subLength];
    for (int h = 0; h < subLength; h++)
      subChainBuckets[h] = CSRCountUtils.buildValidBuckets(db, nodeLabels[idxA + h + 1]);

    long selfLoopTotal = 0;

    for (final Iterator<? extends Identifiable> it = CSRCountUtils.iterateAnchors(db, nodeLabels[idxA]); it.hasNext(); ) {
      guard.check();
      final Vertex anchor = it.next().asVertex();
      final RID anchorRid = anchor.getIdentity();

      // Sparse BFS from anchor through sub-chain [idxA, idxB)
      // using per-source map to deduplicate within this source's expansion
      RidLongHashMap cur = new RidLongHashMap();
      cur.put(anchorRid, 1L);

      for (int h = 0; h < subLength; h++) {
        final int hopIdx = idxA + h;
        final IntHashSet targetBuckets = subChainBuckets[h];

        final RidLongHashMap next = new RidLongHashMap();
        cur.forEach((bucketId, offset, pathCount) -> {
          final RID rid = db.newRID(bucketId, offset);
          expandNeighbors(db, provider, rid, directions[hopIdx], edgeTypes[hopIdx], targetBuckets,
              neighborRid -> next.add(neighborRid, pathCount));
        });
        cur = next;
      }

      long loopCount = cur.get(anchorRid, 0);

      // Multiply by tail after idxB
      if (loopCount > 0 && idxB < edgeTypes.length) {
        long tailCount = 1;
        for (int h = idxB; h < edgeTypes.length; h++) {
          tailCount *= anchor.countEdges(directions[h], edgeTypes[h]);
          if (tailCount == 0)
            break;
        }
        loopCount *= tailCount;
      }

      // Multiply by prefix before idxA
      if (loopCount > 0 && idxA > 0) {
        long prefixCount = 1;
        for (int h = idxA - 1; h >= 0; h--) {
          prefixCount *= anchor.countEdges(directions[h], edgeTypes[h]);
          if (prefixCount == 0)
            break;
        }
        loopCount *= prefixCount;
      }

      selfLoopTotal += loopCount;
    }
    return selfLoopTotal;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("+ COUNT CHAIN PATHS (CSR count-push-down)\n");
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    if (anchorRid != null) {
      sb.append("  seeded anchor: ").append(anchorRid).append('\n');
      sb.append("  ".repeat(Math.max(0, depth * indent)));
    }
    sb.append("  chain: ");
    for (int i = 0; i < edgeTypes.length; i++) {
      if (i > 0)
        sb.append(" → ");
      sb.append("(").append(nodeLabels[i] != null ? nodeLabels[i] : "?").append(")");
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
      sb.append(edgeTypes[i]);
      sb.append(directions[i] == Vertex.DIRECTION.OUT ? "]->" : "]-");
    }
    sb.append("(").append(nodeLabels[edgeTypes.length] != null ? nodeLabels[edgeTypes.length] : "?").append(")");
    return sb.toString();
  }
}
