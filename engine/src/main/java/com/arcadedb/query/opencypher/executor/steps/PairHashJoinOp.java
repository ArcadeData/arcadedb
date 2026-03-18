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
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final HashMap<Long, Long> pairCounts = new HashMap<>();

    for (final Iterator<? extends Identifiable> it = db.iterateType(buildStartLabel, true); it.hasNext(); ) {
      final int startId = provider.getNodeId(it.next().getIdentity());
      if (startId < 0)
        continue;

      final int[] ep1Ids = CSRCountUtils.walkArm(provider, startId, arm1EdgeTypes, arm1Directions,
          buildValidBucketSets(db, arm1IntermediateLabels));
      if (ep1Ids.length == 0)
        continue;

      final int[] ep2Ids = CSRCountUtils.walkArm(provider, startId, arm2EdgeTypes, arm2Directions,
          buildValidBucketSets(db, arm2IntermediateLabels));
      if (ep2Ids.length == 0)
        continue;

      for (final int ep1 : ep1Ids)
        for (final int ep2 : ep2Ids)
          pairCounts.merge(CSRCountUtils.packPair(ep1, ep2), 1L, Long::sum);
    }

    final NeighborView probeView = provider.getNeighborView(probeDirection, probeEdgeType);
    final int nodeCount = provider.getNodeCount();
    long total = 0;

    if (probeView != null) {
      final int[] probeNbrs = probeView.neighbors();
      for (int p1 = 0; p1 < nodeCount; p1++) {
        for (int j = probeView.offset(p1), end = probeView.offsetEnd(p1); j < end; j++) {
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
      final List<RID> ep1List = walkArmOLTP(start, arm1EdgeTypes, arm1Directions, arm1IntermediateLabels);
      final List<RID> ep2List = walkArmOLTP(start, arm2EdgeTypes, arm2Directions, arm2IntermediateLabels);
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
        for (final Iterator<Vertex> vIt = p1.getVertices(probeDirection, probeEdgeType).iterator(); vIt.hasNext(); ) {
          final Vertex p2 = vIt.next();
          final Long cnt = pairCounts.get(p1.getIdentity() + "|" + p2.getIdentity());
          if (cnt != null)
            total += cnt;
        }
      }
    }
    return total;
  }

  private List<RID> walkArmOLTP(final Vertex start, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions, final String[] intermediateLabels) {
    List<Vertex> current = List.of(start);
    for (int hop = 0; hop < edgeTypes.length; hop++) {
      final String label = intermediateLabels != null ? intermediateLabels[hop] : null;
      final List<Vertex> next = new ArrayList<>();
      for (final Vertex v : current)
        for (final Iterator<Vertex> it = v.getVertices(directions[hop], edgeTypes[hop]).iterator(); it.hasNext(); ) {
          final Vertex neighbor = it.next();
          if (label != null && !neighbor.getType().instanceOf(label))
            continue;
          next.add(neighbor);
        }
      current = next;
    }
    final List<RID> result = new ArrayList<>(current.size());
    for (final Vertex v : current)
      result.add(v.getIdentity());
    return result;
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
