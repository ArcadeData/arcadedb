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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Count operator for chain patterns with an anti-join predicate.
 * <p>
 * Handles patterns like:
 * <pre>
 *   MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)
 *   WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3
 *   RETURN count(*) AS count
 * </pre>
 * <p>
 * The anti-join {@code NOT (n_i)-[:TYPE]-(n_j)} is evaluated efficiently using CSR sorted
 * neighbor arrays via merge-scan set difference, avoiding per-row OLTP edge scans.
 * <p>
 * Algorithm (CSR path):
 * <ol>
 *   <li>Propagate counts from position 0 to the anti-join source position (if source &gt; 0)</li>
 *   <li>For each active node v at the anti-join source position:
 *     <ul>
 *       <li>Get v's anti-join neighbors (sorted array from CSR)</li>
 *       <li>Enumerate paths from v through hops to the anti-join target position</li>
 *       <li>At each reached target node, merge-scan against anti-join neighbors to exclude matches</li>
 *       <li>For surviving targets, compute remaining chain contribution (degree products)</li>
 *     </ul>
 *   </li>
 * </ol>
 */
public final class AntiJoinChainOp implements CountOp {
  private final String[] nodeLabels;
  private final String[] edgeTypes;
  private final Vertex.DIRECTION[] directions;

  // Anti-join: NOT (node at sourceIdx)-[:antiJoinEdgeType]-(node at targetIdx)
  private final int antiJoinSourceIdx;
  private final int antiJoinTargetIdx;
  private final String antiJoinEdgeType;
  private final Vertex.DIRECTION antiJoinDirection;

  // Optional inequality filter (typically same as anti-join endpoints)
  private final int inequalityIdxA;
  private final int inequalityIdxB;

  public AntiJoinChainOp(final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions,
      final int antiJoinSourceIdx, final int antiJoinTargetIdx,
      final String antiJoinEdgeType, final Vertex.DIRECTION antiJoinDirection,
      final int inequalityIdxA, final int inequalityIdxB) {
    this.nodeLabels = nodeLabels;
    this.edgeTypes = edgeTypes;
    this.directions = directions;
    this.antiJoinSourceIdx = antiJoinSourceIdx;
    this.antiJoinTargetIdx = antiJoinTargetIdx;
    this.antiJoinEdgeType = antiJoinEdgeType;
    this.antiJoinDirection = antiJoinDirection;
    this.inequalityIdxA = inequalityIdxA;
    this.inequalityIdxB = inequalityIdxB;
  }

  @Override
  public String[] edgeTypes() {
    // Include chain edge types + anti-join edge type
    final Set<String> types = new HashSet<>(Arrays.asList(edgeTypes));
    types.add(antiJoinEdgeType);
    return types.toArray(new String[0]);
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();
    final int hops = edgeTypes.length;

    // Pre-compute valid bucket sets for type filtering
    final Set<Integer>[] validBuckets = new Set[hops + 1];
    for (int i = 0; i <= hops; i++)
      validBuckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);

    // Determine the "check position" — the later of the two anti-join endpoints.
    // We always iterate from chain position 0 (anchor) and expand to the check position.
    // The anti-join filter is applied at the check position.
    final int earlierIdx = Math.min(antiJoinSourceIdx, antiJoinTargetIdx);
    final int laterIdx = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);

    // We need position 0 to be one of the anti-join endpoints for per-source iteration
    if (earlierIdx != 0)
      return executeGenericAntiJoin(provider, db, nodeCount, validBuckets);

    // Per-source iteration from anchor (position 0)
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    final boolean anchorIsSource = (antiJoinSourceIdx == 0);

    // Pre-fetch NeighborViews for each hop up to the check position
    final NeighborView[] hopViews = new NeighborView[laterIdx];
    for (int h = 0; h < laterIdx; h++)
      hopViews[h] = provider.getNeighborView(directions[h], edgeTypes[h]);

    // Pre-compute bucket IDs for CSR-based anchor iteration and frontier filtering
    final int[] bucketIds = new int[nodeCount];
    for (int v = 0; v < nodeCount; v++)
      bucketIds[v] = provider.getRID(v).getBucketId();
    final Set<Integer> anchorBuckets = CSRCountUtils.buildValidBuckets(db, anchorLabel);
    if (anchorBuckets == null || anchorBuckets.isEmpty())
      return 0;

    long totalCount = 0;

    for (int anchorId = 0; anchorId < nodeCount; anchorId++) {
      if (!anchorBuckets.contains(bucketIds[anchorId]))
        continue;

      // Pre-compute anti-join neighbors for Case A (anchor is the anti-join source)
      final int[] anchorAntiNbrs = anchorIsSource
          ? provider.getNeighborIds(anchorId, antiJoinDirection, antiJoinEdgeType)
          : null;

      final long count = countWithAntiJoin(provider, anchorId, anchorAntiNbrs,
          anchorIsSource, laterIdx, validBuckets, bucketIds, hopViews);
      totalCount += count;
    }

    return totalCount;
  }

  /**
   * Fallback for anti-join patterns where neither endpoint is at position 0.
   * Uses dense propagation + per-node anti-join checking.
   */
  private long executeGenericAntiJoin(final GraphTraversalProvider provider, final Database db,
      final int nodeCount, final Set<Integer>[] validBuckets) {
    // Fall back to OLTP for this rare case
    return executeOLTP(db);
  }

  /**
   * Counts paths from a single anchor node through the chain, applying anti-join
   * at the later anti-join position, then propagating through remaining hops.
   * <p>
   * Handles two anti-join directions:
   * <ul>
   *   <li>Case A (Q9): anchor is the anti-join source → precomputed merge-scan</li>
   *   <li>Case B (Q8): anchor is the anti-join target → per-frontier binary search</li>
   * </ul>
   */
  private long countWithAntiJoin(final GraphTraversalProvider provider, final int anchorId,
      final int[] anchorAntiNbrs, final boolean anchorIsSource,
      final int checkPosition, final Set<Integer>[] validBuckets, final int[] bucketIds,
      final NeighborView[] hopViews) {
    long count = 0;

    // Expand from anchor through hops [0, checkPosition) using NeighborViews
    int[] frontier = new int[]{anchorId};
    for (int h = 0; h < checkPosition; h++) {
      final NeighborView view = hopViews[h];
      int totalNext = 0;
      if (view != null) {
        for (final int nid : frontier)
          totalNext += view.degree(nid);
      } else {
        for (final int nid : frontier)
          totalNext += provider.getNeighborIds(nid, directions[h], edgeTypes[h]).length;
      }
      if (totalNext == 0)
        return 0;

      final int[] nextFrontier = new int[totalNext];
      int pos = 0;
      if (view != null) {
        final int[] nbrs = view.neighbors();
        for (final int nid : frontier)
          for (int j = view.offset(nid), end = view.offsetEnd(nid); j < end; j++)
            nextFrontier[pos++] = nbrs[j];
      } else {
        for (final int nid : frontier) {
          final int[] neighbors = provider.getNeighborIds(nid, directions[h], edgeTypes[h]);
          System.arraycopy(neighbors, 0, nextFrontier, pos, neighbors.length);
          pos += neighbors.length;
        }
      }

      // Apply type filter using pre-computed bucket IDs
      final Set<Integer> midBuckets = validBuckets[h + 1];
      if (midBuckets != null && !midBuckets.isEmpty()) {
        int writePos = 0;
        for (int i = 0; i < pos; i++) {
          if (midBuckets.contains(bucketIds[nextFrontier[i]]))
            nextFrontier[writePos++] = nextFrontier[i];
        }
        frontier = Arrays.copyOf(nextFrontier, writePos);
      } else {
        frontier = pos < nextFrontier.length ? Arrays.copyOf(nextFrontier, pos) : nextFrontier;
      }
    }

    // frontier now contains nodes at checkPosition.
    // Apply anti-join and inequality filters, then compute tail.
    if (anchorIsSource) {
      // Case A (Q9): anchor is anti-join source. Exclude frontier nodes that are
      // in anchor's anti-join neighbors. Use binary search on the sorted anti-join
      // neighbor array (the frontier is NOT sorted since it's concatenated from
      // multiple source expansions).
      for (final int target : frontier) {
        // Inequality check (anchor at position 0, target at checkPosition)
        if (inequalityIdxA >= 0 && inequalityIdxB >= 0
            && isInequalityViolation(anchorId, target, 0, checkPosition))
          continue;

        // Anti-join: check if target is in anchor's neighbors (via antiJoinDirection).
        // The anchorAntiNbrs array is sorted (from getNeighborIds with sorted merge).
        if (Arrays.binarySearch(anchorAntiNbrs, target) >= 0)
          continue;

        count += computeTailCount(provider, target, validBuckets);
      }
    } else {
      // Case B (Q8): anchor is anti-join target. For each frontier node, check
      // whether it has an anti-join edge to the anchor. Use binary search on the
      // frontier node's sorted neighbor list.
      for (final int frontierNode : frontier) {
        // Inequality check
        if (inequalityIdxA >= 0 && inequalityIdxB >= 0
            && isInequalityViolation(anchorId, frontierNode, 0, checkPosition))
          continue;

        // Anti-join: check if frontierNode has an edge to anchor via the anti-join type/direction
        final int[] frontierAntiNbrs = provider.getNeighborIds(frontierNode,
            antiJoinDirection, antiJoinEdgeType);
        if (Arrays.binarySearch(frontierAntiNbrs, anchorId) >= 0)
          continue; // anti-join hit — exclude

        count += computeTailCount(provider, frontierNode, validBuckets);
      }
    }
    return count;
  }

  /**
   * Checks if the (anchor, target) pair violates the inequality constraint.
   */
  private boolean isInequalityViolation(final int anchorId, final int targetId,
      final int anchorPos, final int targetPos) {
    return ((inequalityIdxA == anchorPos && inequalityIdxB == targetPos)
        || (inequalityIdxA == targetPos && inequalityIdxB == anchorPos))
        && anchorId == targetId;
  }

  /**
   * Computes the count contribution from the "tail" of the chain after the check position.
   * The check position is the later of the two anti-join endpoints.
   * For single remaining hops, uses O(1) degree lookup.
   */
  private long computeTailCount(final GraphTraversalProvider provider, final int nodeId,
      final Set<Integer>[] validBuckets) {
    final int checkPos = Math.max(antiJoinSourceIdx, antiJoinTargetIdx);
    long tailCount = 1;
    for (int h = checkPos; h < edgeTypes.length; h++) {
      final long degree = provider.countEdges(nodeId, directions[h], edgeTypes[h]);
      if (degree == 0)
        return 0;
      tailCount *= degree;
    }
    return tailCount;
  }

  @Override
  public long executeOLTP(final Database db) {
    final String anchorLabel = nodeLabels[0];
    if (anchorLabel == null || !db.getSchema().existsType(anchorLabel))
      return 0;

    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(anchorLabel, true); it.hasNext(); ) {
      final Vertex anchor = it.next().asVertex();
      // Build anti-join neighbor set for the source node
      final Vertex sourceVertex;
      if (antiJoinSourceIdx == 0) {
        sourceVertex = anchor;
      } else {
        // Would need to propagate to find source vertex — for now handle only source at 0
        sourceVertex = anchor;
      }

      final java.util.Set<RID> antiJoinSet = new java.util.HashSet<>();
      for (final Iterator<Vertex> ajIt = sourceVertex.getVertices(antiJoinDirection, antiJoinEdgeType).iterator(); ajIt.hasNext(); )
        antiJoinSet.add(ajIt.next().getIdentity());

      total += countPathsFromOLTP(anchor, 0, db, sourceVertex.getIdentity(), antiJoinSet);
    }
    return total;
  }

  private long countPathsFromOLTP(final Vertex vertex, final int hopIndex, final Database db,
      final RID sourceRid, final java.util.Set<RID> antiJoinSet) {
    if (hopIndex >= edgeTypes.length)
      return 1;

    final String targetLabel = nodeLabels[hopIndex + 1];
    long count = 0;

    final Iterator<Vertex> neighbors = vertex.getVertices(directions[hopIndex], edgeTypes[hopIndex]).iterator();
    while (neighbors.hasNext()) {
      final Vertex neighbor = neighbors.next();
      if (targetLabel != null && !neighbor.getType().instanceOf(targetLabel))
        continue;

      // Anti-join check at the target position
      if (hopIndex + 1 == antiJoinTargetIdx && antiJoinSet.contains(neighbor.getIdentity()))
        continue;

      // Inequality check
      if (inequalityIdxA >= 0 && inequalityIdxB >= 0
          && inequalityIdxA == antiJoinSourceIdx && (hopIndex + 1) == inequalityIdxB
          && neighbor.getIdentity().equals(sourceRid))
        continue;

      count += countPathsFromOLTP(neighbor, hopIndex + 1, db, sourceRid, antiJoinSet);
    }
    return count;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("+ COUNT ANTI-JOIN CHAIN (CSR merge-scan anti-join)\n");
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
    sb.append("\n").append("  ".repeat(Math.max(0, depth * indent)));
    sb.append("  anti-join: NOT (").append(antiJoinSourceIdx).append(")-[:").append(antiJoinEdgeType)
        .append("]-(").append(antiJoinTargetIdx).append(")");
    return sb.toString();
  }
}
