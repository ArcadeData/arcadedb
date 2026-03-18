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

import java.util.Iterator;
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

    // Initialize counts for the anchor type
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

    // Propagate counts through each hop
    for (int hop = 0; hop < hops; hop++) {
      current = CSRCountUtils.propagateOneHop(provider, current, directions[hop], edgeTypes[hop]);
      CSRCountUtils.filterByBuckets(provider, current, validBuckets[hop + 1]);
    }

    // Sum all remaining counts
    long total = 0;
    for (int v = 0; v < nodeCount; v++)
      total += current[v];

    // Subtract self-loop paths if inequality filter is active
    if (inequalityIdxA >= 0 && inequalityIdxB >= 0)
      total -= countSelfLoopPaths(provider, db, validBuckets);

    return total;
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

  private long countLoopsFromNode(final GraphTraversalProvider provider, final int vId,
      final NeighborView[] subViews, final int startHop, final int subChainLength,
      final Set<Integer>[] validBuckets) {
    if (subChainLength == 0)
      return 1;

    final int nodeCount = provider.getNodeCount();
    long[] cur = new long[nodeCount];
    cur[vId] = 1;

    for (int h = 0; h < subChainLength; h++) {
      final long[] next = new long[nodeCount];
      final NeighborView view = subViews[h];
      if (view != null) {
        final int[] nbrs = view.neighbors();
        for (int n = 0; n < nodeCount; n++) {
          if (cur[n] == 0) continue;
          for (int j = view.offset(n), end = view.offsetEnd(n); j < end; j++)
            next[nbrs[j]] += cur[n];
        }
      } else {
        for (int n = 0; n < nodeCount; n++) {
          if (cur[n] == 0) continue;
          final int[] neighbors = provider.getNeighborIds(n, directions[startHop + h], edgeTypes[startHop + h]);
          for (final int neighbor : neighbors)
            next[neighbor] += cur[n];
        }
      }

      final Set<Integer> targetBuckets = validBuckets[startHop + h + 1];
      if (targetBuckets != null && !targetBuckets.isEmpty()) {
        for (int n = 0; n < nodeCount; n++) {
          if (next[n] > 0 && !targetBuckets.contains(provider.getRID(n).getBucketId()))
            next[n] = 0;
        }
      }
      cur = next;
    }
    return cur[vId];
  }

  @Override
  public long executeOLTP(final Database db) {
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final Vertex v = it.next().asVertex();
      final RID anchorRid = inequalityIdxA == 0 ? v.getIdentity() : null;
      total += countPathsFrom(v, 0, db, anchorRid);
    }
    return total;
  }

  private long countPathsFrom(final Vertex vertex, final int hopIndex, final Database db, final RID anchorRid) {
    if (hopIndex >= edgeTypes.length)
      return 1;

    final String targetLabel = nodeLabels[hopIndex + 1];
    long count = 0;

    final Iterator<Vertex> neighbors = vertex.getVertices(directions[hopIndex], edgeTypes[hopIndex]).iterator();
    while (neighbors.hasNext()) {
      final Vertex neighbor = neighbors.next();
      if (targetLabel != null && !neighbor.getType().instanceOf(targetLabel))
        continue;
      if (anchorRid != null && (hopIndex + 1) == inequalityIdxB
          && neighbor.getIdentity().equals(anchorRid))
        continue;
      count += countPathsFrom(neighbor, hopIndex + 1, db, anchorRid);
    }
    return count;
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
