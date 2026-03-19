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
    // Iterate CSR node IDs directly with bucket-based type filtering.
    // This avoids db.iterateType() which reads vertices from OLTP storage — for
    // 3.8M Messages at ~5μs/read, that's ~19s of pure storage I/O.
    // CSR iteration with O(1) bucket checks + O(1) degree lookups: <100ms.
    final Set<Integer> centralBuckets = CSRCountUtils.buildValidBuckets(db, centralLabel);
    if (centralBuckets == null || centralBuckets.isEmpty())
      return 0;

    final int nodeCount = provider.getNodeCount();
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
