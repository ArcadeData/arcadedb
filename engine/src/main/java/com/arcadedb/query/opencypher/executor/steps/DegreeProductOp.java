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
 * Count operator for star-join patterns (Q4, Q7).
 * For each central node, the path count is the product of degrees along each arm.
 * OPTIONAL MATCH arms use {@code max(1, degree)}.
 */
public final class DegreeProductOp implements CountOp {
  private final String centralLabel;
  private final Arm[] arms;
  private final String[] allEdgeTypes;

  /**
   * A single arm extending from the central node.
   */
  public static final class Arm {
    final String[] edgeTypes;
    final Vertex.DIRECTION[] directions;
    final boolean optional;

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional) {
      this.edgeTypes = edgeTypes;
      this.directions = directions;
      this.optional = optional;
    }
  }

  public DegreeProductOp(final String centralLabel, final Arm[] arms) {
    this.centralLabel = centralLabel;
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

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();

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
      return executeFastScan(armViews, nodeCount);

    // Slow path: per-node CSR lookup (fallback for multi-hop arms or missing views)
    return executePerNode(provider, db, nodeCount);
  }

  /**
   * Vectorized degree-product scan using pre-fetched NeighborView offset arrays.
   * Pure array arithmetic in the hot loop — no method calls, no object allocation.
   * <p>
   * For Q4/Q7 with ~5M CSR nodes and 4 arms: ~40M array reads at ~1ns = ~40ms.
   * Compared to per-node countEdges: ~20M method calls at ~150ns = ~3s (75x slower).
   */
  private long executeFastScan(final NeighborView[] armViews, final int nodeCount) {
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
    for (int v = 0; v < nodeCount; v++) {
      // Mandatory arms: skip if any degree is 0
      long product = 1;
      boolean skip = false;
      for (int i = 0; i < mandatoryCount; i++) {
        final int degree = armViews[mandatoryIdx[i]].degree(v);
        if (degree == 0) {
          skip = true;
          break;
        }
        product *= degree;
      }
      if (skip)
        continue;

      // Optional arms: use max(1, degree)
      for (int i = 0; i < optionalCount; i++)
        product *= Math.max(1, armViews[optionalIdx[i]].degree(v));

      total += product;
    }
    return total;
  }

  /**
   * Fallback: per-node CSR iteration with bucket-based type filtering.
   */
  private long executePerNode(final GraphTraversalProvider provider, final Database db,
      final int nodeCount) {
    final Set<Integer> centralBuckets = CSRCountUtils.buildValidBuckets(db, centralLabel);
    if (centralBuckets == null || centralBuckets.isEmpty())
      return 0;

    long total = 0;
    for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
      final RID rid = provider.getRID(nodeId);
      if (!centralBuckets.contains(rid.getBucketId()))
        continue;

      long product = 1;
      for (final Arm arm : arms) {
        final long armCount;
        if (arm.edgeTypes.length == 1)
          armCount = provider.countEdges(nodeId, arm.directions[0], arm.edgeTypes[0]);
        else
          armCount = CSRCountUtils.walkArm(provider, nodeId, arm.edgeTypes, arm.directions).length;

        if (arm.optional) {
          product *= Math.max(1, armCount);
        } else {
          if (armCount == 0) {
            product = 0;
            break;
          }
          product *= armCount;
        }
      }
      total += product;
    }
    return total;
  }

  @Override
  public long executeOLTP(final Database db) {
    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(centralLabel, true); it.hasNext(); ) {
      final Vertex v = it.next().asVertex();
      long product = 1;
      for (final Arm arm : arms) {
        long armCount;
        if (arm.edgeTypes.length == 1)
          armCount = v.countEdges(arm.directions[0], arm.edgeTypes[0]);
        else
          armCount = countArmOLTP(v, arm, 0);

        if (arm.optional)
          product *= Math.max(1, armCount);
        else {
          if (armCount == 0) {
            product = 0;
            break;
          }
          product *= armCount;
        }
      }
      total += product;
    }
    return total;
  }

  private long countArmOLTP(final Vertex vertex, final Arm arm, final int hopIndex) {
    if (hopIndex >= arm.edgeTypes.length)
      return 1;
    long count = 0;
    final Iterator<Vertex> neighbors = vertex.getVertices(arm.directions[hopIndex], arm.edgeTypes[hopIndex]).iterator();
    while (neighbors.hasNext())
      count += countArmOLTP(neighbors.next(), arm, hopIndex + 1);
    return count;
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT STAR JOIN (CSR degree product)\n");
    sb.append(ind).append("  central: ").append(centralLabel).append(", arms: ").append(arms.length);
    for (int i = 0; i < arms.length; i++) {
      sb.append("\n").append(ind).append("  arm ").append(i).append(arms[i].optional ? " [OPTIONAL]" : "").append(": ");
      for (int j = 0; j < arms[i].edgeTypes.length; j++) {
        if (j > 0) sb.append(" → ");
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "-[:" : "<-[:");
        sb.append(arms[i].edgeTypes[j]);
        sb.append(arms[i].directions[j] == Vertex.DIRECTION.OUT ? "]->" : "]-");
      }
    }
    return sb.toString();
  }
}
