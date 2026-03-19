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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Count operator for two-pattern pair-join queries (Q2).
 * Build phase: walk arms from start node to get (ep1, ep2) pairs.
 * Probe phase: iterate probe edges and look up pair counts.
 */
public final class PairHashJoinOp implements CountOp {
  private final String buildStartLabel;
  private final String[] arm1EdgeTypes;
  private final Vertex.DIRECTION[] arm1Directions;
  private final String[] arm1IntermediateLabels; // labels for nodes reached at each hop (null = no filter)
  private final String[] arm2EdgeTypes;
  private final Vertex.DIRECTION[] arm2Directions;
  private final String[] arm2IntermediateLabels;
  private final String probeEdgeType;
  private final Vertex.DIRECTION probeDirection;
  private final String[] allEdgeTypes;

  public PairHashJoinOp(final String buildStartLabel,
      final String[] arm1EdgeTypes, final Vertex.DIRECTION[] arm1Directions,
      final String[] arm1IntermediateLabels,
      final String[] arm2EdgeTypes, final Vertex.DIRECTION[] arm2Directions,
      final String[] arm2IntermediateLabels,
      final String probeEdgeType, final Vertex.DIRECTION probeDirection) {
    this.buildStartLabel = buildStartLabel;
    this.arm1EdgeTypes = arm1EdgeTypes;
    this.arm1Directions = arm1Directions;
    this.arm1IntermediateLabels = arm1IntermediateLabels;
    this.arm2EdgeTypes = arm2EdgeTypes;
    this.arm2Directions = arm2Directions;
    this.arm2IntermediateLabels = arm2IntermediateLabels;
    this.probeEdgeType = probeEdgeType;
    this.probeDirection = probeDirection;

    final HashSet<String> allTypesSet = new HashSet<>();
    for (final String et : arm1EdgeTypes) allTypesSet.add(et);
    for (final String et : arm2EdgeTypes) allTypesSet.add(et);
    allTypesSet.add(probeEdgeType);
    this.allEdgeTypes = allTypesSet.toArray(new String[0]);
  }

  @Override
  public String[] edgeTypes() {
    return allEdgeTypes;
  }

  @Override
  @SuppressWarnings("unchecked")
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();

    // Pre-compute intermediate label bucket sets ONCE (not per vertex!)
    final Set<Integer>[] arm1Buckets = buildValidBucketSets(db, arm1IntermediateLabels);
    final Set<Integer>[] arm2Buckets = buildValidBucketSets(db, arm2IntermediateLabels);

    // Identify build-start nodes via bucket filtering on CSR (avoids OLTP iterateType)
    final Set<Integer> buildBuckets = CSRCountUtils.buildValidBuckets(db, buildStartLabel);
    if (buildBuckets == null || buildBuckets.isEmpty())
      return 0;

    // BUILD: for single-hop arms, use NeighborView for direct edge iteration
    // instead of per-node walkArm (avoids ~6M getNeighborIds method calls).
    // For multi-hop arms, fall back to per-node walkArm.
    final HashMap<Long, Long> pairCounts = new HashMap<>();

    final boolean arm1SingleHop = arm1EdgeTypes.length == 1;
    final boolean arm2SingleHop = arm2EdgeTypes.length == 1;
    final NeighborView arm1View = arm1SingleHop ? provider.getNeighborView(arm1Directions[0], arm1EdgeTypes[0]) : null;
    final NeighborView arm2View = arm2SingleHop ? provider.getNeighborView(arm2Directions[0], arm2EdgeTypes[0]) : null;

    // FAST PATH: Inline probe — skip the HashMap entirely.
    // For each build node, compute (ep1, ep2) and immediately check if the probe
    // edge exists via binary search on sorted CSR neighbor arrays. O(log d) per check.
    // This eliminates ~3M HashMap operations (merge + get + boxing) ≈ 300ms savings.
    final NeighborView probeView = provider.getNeighborView(probeDirection, probeEdgeType);

    if (arm1View != null && probeView != null) {
      // Pre-fetch arm2 NeighborViews
      final NeighborView[] arm2Views = new NeighborView[arm2EdgeTypes.length];
      boolean allArm2Views = true;
      for (int h = 0; h < arm2EdgeTypes.length; h++) {
        arm2Views[h] = provider.getNeighborView(arm2Directions[h], arm2EdgeTypes[h]);
        if (arm2Views[h] == null) { allArm2Views = false; break; }
      }

      int[] bucketIds = null;
      if (arm2Buckets != null) {
        bucketIds = new int[nodeCount];
        for (int v = 0; v < nodeCount; v++)
          bucketIds[v] = provider.getRID(v).getBucketId();
      }

      if (allArm2Views) {
        return buildAndProbeInline(arm1View, arm2Views, arm2Buckets, probeView, nodeCount, bucketIds);
      }
    }

    // FALLBACK: HashMap build + probe (for cases without full NeighborView coverage)
    if (arm1View != null && arm2View != null) {
      buildWithViews(arm1View, arm2View, arm2Buckets, pairCounts, nodeCount, provider);
    } else if (arm1View != null) {
      buildWithArm1View(arm1View, provider, arm2Buckets, pairCounts, nodeCount);
    } else {
      for (int startId = 0; startId < nodeCount; startId++) {
        final int[] ep1Ids = CSRCountUtils.walkArm(provider, startId, arm1EdgeTypes, arm1Directions, arm1Buckets);
        if (ep1Ids.length == 0)
          continue;
        final int[] ep2Ids = CSRCountUtils.walkArm(provider, startId, arm2EdgeTypes, arm2Directions, arm2Buckets);
        if (ep2Ids.length == 0)
          continue;
        for (final int ep1 : ep1Ids)
          for (final int ep2 : ep2Ids)
            pairCounts.merge(CSRCountUtils.packPair(ep1, ep2), 1L, Long::sum);
      }
    }

    final NeighborView probeViewFallback = probeView != null ? probeView : provider.getNeighborView(probeDirection, probeEdgeType);
    long total = 0;
    if (probeViewFallback != null) {
      final int[] probeNbrs = probeViewFallback.neighbors();
      for (int p1 = 0; p1 < nodeCount; p1++) {
        for (int j = probeViewFallback.offset(p1), end = probeViewFallback.offsetEnd(p1); j < end; j++) {
          final Long cnt = pairCounts.get(CSRCountUtils.packPair(p1, probeNbrs[j]));
          if (cnt != null)
            total += cnt;
        }
      }
    } else {
      for (int p1 = 0; p1 < nodeCount; p1++) {
        final int[] neighbors = provider.getNeighborIds(p1, probeDirection, probeEdgeType);
        for (final int p2 : neighbors) {
          final Long cnt = pairCounts.get(CSRCountUtils.packPair(p1, p2));
          if (cnt != null)
            total += cnt;
        }
      }
    }
    return total;
  }

  @Override
  public long executeOLTP(final Database db) {
    final HashMap<String, Long> pairCounts = new HashMap<>();

    for (final Iterator<? extends Identifiable> it = db.iterateType(buildStartLabel, true); it.hasNext(); ) {
      final Vertex start = it.next().asVertex();
      final List<RID> ep1List = walkArmOLTP(db, start, arm1EdgeTypes, arm1Directions, arm1IntermediateLabels);
      final List<RID> ep2List = walkArmOLTP(db, start, arm2EdgeTypes, arm2Directions, arm2IntermediateLabels);
      for (final RID ep1 : ep1List)
        for (final RID ep2 : ep2List)
          pairCounts.merge(ep1 + "|" + ep2, 1L, Long::sum);
    }

    long total = 0;
    for (final DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof VertexType))
        continue;
      for (final Iterator<? extends Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        final Vertex p1 = it.next().asVertex();
        for (final RID p2Rid : p1.getConnectedVertexRIDs(probeDirection, probeEdgeType)) {
          final Long cnt = pairCounts.get(p1.getIdentity() + "|" + p2Rid);
          if (cnt != null)
            total += cnt;
        }
      }
    }
    return total;
  }

  private List<RID> walkArmOLTP(final Database db, final Vertex start, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions, final String[] intermediateLabels) {
    List<RID> current = List.of(start.getIdentity());
    for (int hop = 0; hop < edgeTypes.length; hop++) {
      final Set<Integer> labelBuckets;
      final String label = intermediateLabels != null ? intermediateLabels[hop] : null;
      if (label != null && db.getSchema().existsType(label))
        labelBuckets = new HashSet<>(db.getSchema().getType(label).getBucketIds(true));
      else
        labelBuckets = null;

      final List<RID> next = new ArrayList<>();
      for (final RID rid : current) {
        final Vertex v = rid.asVertex();
        for (final RID neighborRid : v.getConnectedVertexRIDs(directions[hop], edgeTypes[hop])) {
          if (labelBuckets != null && !labelBuckets.contains(neighborRid.getBucketId()))
            continue;
          next.add(neighborRid);
        }
      }
      current = next;
    }
    return current;
  }

  /**
   * Inline build+probe: for each build node, compute (ep1, ep2) pair and immediately
   * check if the probe edge exists via binary search. No HashMap at all.
   * <p>
   * For Q2 (2.6M Comments, arm1=1 hop, arm2=2 hops, probe=KNOWS BOTH):
   * Each Comment: 1 arm1 lookup + 2 arm2 lookups + 1 binary search = ~5 ops.
   * Total: 2.6M × 5 = 13M ops at ~3ns = ~39ms (vs ~400ms with HashMap).
   */
  private long buildAndProbeInline(final NeighborView arm1View, final NeighborView[] arm2Views,
      final Set<Integer>[] arm2Buckets, final NeighborView probeView,
      final int nodeCount, final int[] bucketIds) {
    final int[] arm1Nbrs = arm1View.neighbors();
    final int[] probeNbrs = probeView.neighbors();
    long total = 0;

    for (int startId = 0; startId < nodeCount; startId++) {
      final int a1Start = arm1View.offset(startId);
      final int a1End = arm1View.offsetEnd(startId);
      if (a1Start == a1End) continue;

      // Walk arm2 to get endpoints
      final int[] ep2Ids = walkArmWithViews(startId, arm2Views, arm2Buckets, bucketIds);
      if (ep2Ids.length == 0) continue;

      // For each (ep1, ep2) pair, check if probe edge exists via binary search
      for (int i = a1Start; i < a1End; i++) {
        final int ep1 = arm1Nbrs[i];
        final int pStart = probeView.offset(ep1);
        final int pEnd = probeView.offsetEnd(ep1);
        if (pStart == pEnd) continue; // ep1 has no probe edges

        for (final int ep2 : ep2Ids) {
          if (java.util.Arrays.binarySearch(probeNbrs, pStart, pEnd, ep2) >= 0)
            total++;
        }
      }
    }
    return total;
  }

  /**
   * Fast build when both arms are single-hop with NeighborViews.
   * For Q2: arm1=HAS_CREATOR(1 hop), arm2=REPLY_OF+HAS_CREATOR(2 hops) → arm2 is NOT single-hop.
   * But for simpler pair-joins where both arms are 1 hop, this avoids all per-node method calls.
   */
  private void buildWithViews(final NeighborView arm1View, final NeighborView arm2View,
      final Set<Integer>[] arm2Buckets, final HashMap<Long, Long> pairCounts,
      final int nodeCount, final GraphTraversalProvider provider) {
    final int[] arm1Nbrs = arm1View.neighbors();
    final int[] arm2Nbrs = arm2View.neighbors();
    for (int startId = 0; startId < nodeCount; startId++) {
      final int a1Start = arm1View.offset(startId);
      final int a1End = arm1View.offsetEnd(startId);
      if (a1Start == a1End) continue;
      final int a2Start = arm2View.offset(startId);
      final int a2End = arm2View.offsetEnd(startId);
      if (a2Start == a2End) continue;

      for (int i = a1Start; i < a1End; i++)
        for (int j = a2Start; j < a2End; j++)
          pairCounts.merge(CSRCountUtils.packPair(arm1Nbrs[i], arm2Nbrs[j]), 1L, Long::sum);
    }
  }

  /**
   * Build when arm1 is single-hop (NeighborView) and arm2 may be multi-hop.
   * Uses pre-fetched NeighborViews for arm2 hops to avoid per-node getNeighborIds calls.
   * For Q2: arm1=HAS_CREATOR OUT (Comment→Person), arm2=REPLY_OF+HAS_CREATOR (Comment→Post→Person).
   */
  private void buildWithArm1View(final NeighborView arm1View, final GraphTraversalProvider provider,
      final Set<Integer>[] arm2Buckets, final HashMap<Long, Long> pairCounts,
      final int nodeCount) {
    final int[] arm1Nbrs = arm1View.neighbors();

    // Pre-fetch NeighborViews for arm2 hops
    final NeighborView[] arm2Views = new NeighborView[arm2EdgeTypes.length];
    boolean allArm2Views = true;
    for (int h = 0; h < arm2EdgeTypes.length; h++) {
      arm2Views[h] = provider.getNeighborView(arm2Directions[h], arm2EdgeTypes[h]);
      if (arm2Views[h] == null) { allArm2Views = false; break; }
    }

    // Pre-compute bucketIds for arm2 intermediate type filtering
    int[] bucketIds = null;
    if (arm2Buckets != null && allArm2Views) {
      bucketIds = new int[nodeCount];
      for (int v = 0; v < nodeCount; v++)
        bucketIds[v] = provider.getRID(v).getBucketId();
    }

    for (int startId = 0; startId < nodeCount; startId++) {
      final int a1Start = arm1View.offset(startId);
      final int a1End = arm1View.offsetEnd(startId);
      if (a1Start == a1End) continue;

      // Walk arm2 using NeighborViews when available
      final int[] ep2Ids;
      if (allArm2Views)
        ep2Ids = walkArmWithViews(startId, arm2Views, arm2Buckets, bucketIds);
      else
        ep2Ids = CSRCountUtils.walkArm(provider, startId, arm2EdgeTypes, arm2Directions, arm2Buckets);
      if (ep2Ids.length == 0) continue;

      for (int i = a1Start; i < a1End; i++)
        for (final int ep2 : ep2Ids)
          pairCounts.merge(CSRCountUtils.packPair(arm1Nbrs[i], ep2), 1L, Long::sum);
    }
  }

  /**
   * Walks an arm using pre-fetched NeighborViews (zero per-node method dispatch).
   */
  private static int[] walkArmWithViews(final int startId, final NeighborView[] views,
      final Set<Integer>[] intermediateBuckets, final int[] bucketIds) {
    int[] current = new int[]{startId};
    for (int hop = 0; hop < views.length; hop++) {
      final NeighborView view = views[hop];
      int totalNext = 0;
      for (final int nid : current)
        totalNext += view.degree(nid);
      if (totalNext == 0)
        return new int[0];

      final int[] next = new int[totalNext];
      final int[] nbrs = view.neighbors();
      int pos = 0;
      for (final int nid : current)
        for (int j = view.offset(nid), end = view.offsetEnd(nid); j < end; j++)
          next[pos++] = nbrs[j];

      // Type filter using pre-computed bucketIds
      if (intermediateBuckets != null && intermediateBuckets[hop] != null
          && !intermediateBuckets[hop].isEmpty() && bucketIds != null) {
        int writePos = 0;
        for (int i = 0; i < pos; i++)
          if (intermediateBuckets[hop].contains(bucketIds[next[i]]))
            next[writePos++] = next[i];
        current = java.util.Arrays.copyOf(next, writePos);
      } else {
        current = pos < next.length ? java.util.Arrays.copyOf(next, pos) : next;
      }
    }
    return current;
  }

  /**
   * Builds valid bucket sets from intermediate labels for use with CSRCountUtils.walkArm.
   */
  @SuppressWarnings("unchecked")
  private static Set<Integer>[] buildValidBucketSets(final Database db, final String[] intermediateLabels) {
    if (intermediateLabels == null)
      return null;
    boolean hasAny = false;
    for (final String label : intermediateLabels)
      if (label != null) { hasAny = true; break; }
    if (!hasAny)
      return null;
    final Set<Integer>[] result = new Set[intermediateLabels.length];
    for (int i = 0; i < intermediateLabels.length; i++)
      result[i] = CSRCountUtils.buildValidBuckets(db, intermediateLabels[i]);
    return result;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT PAIR JOIN (build-probe hash join)\n");
    sb.append(ind).append("  build: ").append(buildStartLabel);
    sb.append(", arm1[");
    for (int i = 0; i < arm1EdgeTypes.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(arm1EdgeTypes[i]);
    }
    sb.append("], arm2[");
    for (int i = 0; i < arm2EdgeTypes.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(arm2EdgeTypes[i]);
    }
    sb.append("], probe: ").append(probeEdgeType);
    return sb.toString();
  }
}
