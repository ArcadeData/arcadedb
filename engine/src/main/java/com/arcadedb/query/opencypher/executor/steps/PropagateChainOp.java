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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Count operator for linear chain patterns (Q1, Q5, Q6).
 * Propagates path counts level-by-level through the CSR adjacency structure
 * using two {@code long[]} arrays indexed by dense node IDs.
 */
public final class PropagateChainOp implements CountOp {
  private final String[] nodeLabels;
  private final String[] edgeTypes;
  private final Vertex.DIRECTION[] directions;
  private final int inequalityIdxA;
  private final int inequalityIdxB;

  public PropagateChainOp(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions,
      final int inequalityIdxA, final int inequalityIdxB) {
    this.nodeLabels = nodeLabels;
    this.edgeTypes = edgeTypes;
    this.directions = directions;
    this.inequalityIdxA = inequalityIdxA;
    this.inequalityIdxB = inequalityIdxB;
  }

  @Override
  public String[] edgeTypes() {
    return edgeTypes;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();
    final int hops = edgeTypes.length;

    // Pre-compute valid bucket sets for type filtering at each level
    final Set<Integer>[] validBuckets = new Set[hops + 1];
    for (int i = 0; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    // When inequality is present and the inequality source is at position 0,
    // use single-pass per-source expansion. This computes countAll and countEqual
    // simultaneously, avoids dense long[nodeCount] arrays, and eliminates the
    // separate self-loop subtraction pass entirely.
    if (inequalityIdxA >= 0 && inequalityIdxB >= 0
        && Math.min(inequalityIdxA, inequalityIdxB) == 0)
      return executePerSourceInequality(provider, db, nodeCount, validBuckets);

    // Standard dense array propagation for chains without inequality
    // (or with inequality source not at position 0)
    long[] current = new long[nodeCount];
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel != null && db.getSchema().existsType(anchorLabel)) {
      for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
        final RID rid = it.next().getIdentity();
        final int nodeId = provider.getNodeId(rid);
        if (nodeId >= 0)
          current[nodeId] = 1;
      }
    }

    for (int hop = 0; hop < hops; hop++) {
      current = CSRCountUtils.propagateOneHop(provider, current, directions[hop], edgeTypes[hop]);
      CSRCountUtils.filterByBuckets(provider, current, validBuckets[hop + 1]);
    }

    long total = 0;
    for (int v = 0; v < nodeCount; v++)
      total += current[v];

    if (inequalityIdxA >= 0 && inequalityIdxB >= 0)
      total -= countSelfLoopPaths(provider, db, validBuckets);

    return total;
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
  private long executePerSourceInequality(final GraphTraversalProvider provider, final Database db,
      final int nodeCount, final Set<Integer>[] validBuckets) {
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);

    // Pre-fetch NeighborViews for hops up to the inequality target
    final NeighborView[] views = new NeighborView[idxB];
    for (int h = 0; h < idxB; h++)
      views[h] = provider.getNeighborView(directions[h], edgeTypes[h]);

    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    long totalCount = 0;

    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final RID rid = it.next().getIdentity();
      final int srcId = provider.getNodeId(rid);
      if (srcId < 0)
        continue;

      // Expand from srcId through hops [0, idxB)
      int[] frontier = new int[]{srcId};
      for (int h = 0; h < idxB; h++) {
        frontier = expandFrontier(provider, frontier, views[h], h, validBuckets[h + 1]);
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

  private long countSelfLoopPaths(final GraphTraversalProvider provider, final Database db,
      final Set<Integer>[] validBuckets) {
    final int nodeCount = provider.getNodeCount();
    final int idxA = Math.min(inequalityIdxA, inequalityIdxB);
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);
    final int subChainLength = idxB - idxA;

    long selfLoopTotal = 0;

    final NeighborView[] subViews = new NeighborView[subChainLength];
    for (int h = 0; h < subChainLength; h++)
      subViews[h] = provider.getNeighborView(directions[idxA + h], edgeTypes[idxA + h]);

    final String anchorLabel = nodeLabels[idxA];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final int vId = provider.getNodeId(it.next().getIdentity());
      if (vId < 0)
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
      final Set<Integer>[] validBuckets) {
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
    final Set<Integer> lastBuckets = validBuckets[lastHopIdx + 1];
    // If there are type filters at the target position and vId doesn't match, no self-loops possible
    if (lastBuckets != null && !lastBuckets.isEmpty()) {
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
      final NeighborView view, final int hopIdx, final Set<Integer> targetBuckets) {
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

    // Apply type filter if needed
    if (targetBuckets != null && !targetBuckets.isEmpty()) {
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

  @Override
  public long executeOLTP(final Database db) {
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    // BFS count propagation: each unique vertex at each level is processed once,
    // with its count capturing path multiplicity. This reduces O(paths) traversals
    // to O(unique edges), which is dramatically faster for high-fanout chains.
    // E.g., Q5 with 13.8M paths but only ~3.5M unique edges: ~4x fewer OLTP scans.
    HashMap<RID, Long> current = new HashMap<>();
    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); )
      current.put(it.next().getIdentity(), 1L);

    for (int hop = 0; hop < edgeTypes.length; hop++) {
      final String targetLabel = nodeLabels[hop + 1];
      final HashMap<RID, Long> next = new HashMap<>();
      for (final Map.Entry<RID, Long> entry : current.entrySet()) {
        final Vertex v = (Vertex) db.lookupByRID(entry.getKey(), true);
        final long pathCount = entry.getValue();
        final Iterator<Vertex> neighbors = v.getVertices(directions[hop], edgeTypes[hop]).iterator();
        while (neighbors.hasNext()) {
          final Vertex neighbor = neighbors.next();
          if (targetLabel != null && !neighbor.getType().instanceOf(targetLabel))
            continue;
          next.merge(neighbor.getIdentity(), pathCount, Long::sum);
        }
      }
      current = next;
    }

    long total = 0;
    for (final long c : current.values())
      total += c;

    // Subtract self-loop paths for inequality
    if (inequalityIdxA >= 0 && inequalityIdxB >= 0)
      total -= countSelfLoopPathsOLTP(db);

    return total;
  }

  /**
   * OLTP self-loop counting: for each anchor vertex, BFS through the sub-chain
   * and check for paths that return to the anchor.
   */
  private long countSelfLoopPathsOLTP(final Database db) {
    final int idxA = Math.min(inequalityIdxA, inequalityIdxB);
    final int idxB = Math.max(inequalityIdxA, inequalityIdxB);
    final int subLength = idxB - idxA;

    final String anchorLabel = nodeLabels[idxA];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    long selfLoopTotal = 0;

    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final Vertex anchor = it.next().asVertex();
      final RID anchorRid = anchor.getIdentity();

      // Sparse BFS from anchor through sub-chain [idxA, idxB)
      // using per-source map to deduplicate within this source's expansion
      HashMap<RID, Long> cur = new HashMap<>();
      cur.put(anchorRid, 1L);

      for (int h = 0; h < subLength; h++) {
        final int hopIdx = idxA + h;
        final String targetLabel = nodeLabels[hopIdx + 1];
        final HashMap<RID, Long> next = new HashMap<>();
        for (final Map.Entry<RID, Long> entry : cur.entrySet()) {
          final Vertex v = (Vertex) db.lookupByRID(entry.getKey(), true);
          final Iterator<Vertex> neighbors = v.getVertices(directions[hopIdx], edgeTypes[hopIdx]).iterator();
          while (neighbors.hasNext()) {
            final Vertex neighbor = neighbors.next();
            if (targetLabel != null && !neighbor.getType().instanceOf(targetLabel))
              continue;
            next.merge(neighbor.getIdentity(), entry.getValue(), Long::sum);
          }
        }
        cur = next;
      }

      long loopCount = cur.getOrDefault(anchorRid, 0L);

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
