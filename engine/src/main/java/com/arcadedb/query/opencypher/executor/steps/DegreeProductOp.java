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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
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
  private final String centralLabel;
  private final Arm[] arms;
  private final String[] allEdgeTypes;

  /**
   * A single arm extending from the central node.
   */
  public static final class Arm {
    final String[]            edgeTypes;
    final Vertex.DIRECTION[]  directions;
    final boolean             optional;
    /** One entry per hop: the label of the node that hop reaches, or null for none. The array itself is null when no hop has one. */
    final String[]            endpointLabels;

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional) {
      this(edgeTypes, directions, optional, null);
    }

    public Arm(final String[] edgeTypes, final Vertex.DIRECTION[] directions, final boolean optional,
        final String[] endpointLabels) {
      this.edgeTypes = edgeTypes;
      this.directions = directions;
      this.optional = optional;
      boolean any = false;
      if (endpointLabels != null)
        for (final String label : endpointLabels)
          any |= label != null;
      this.endpointLabels = any ? endpointLabels : null;
    }

    boolean hasEndpointLabel() {
      return endpointLabels != null;
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
        if (armBuckets[a] == null || armBuckets[a][0] == null)
          continue;
        final IntHashSet far = armBuckets[a][0];
        final NeighborView view = provider.getNeighborView(arms[a].directions[0], arms[a].edgeTypes[0]);
        if (endpointLabelIsImplied(provider, arms[a], view, far, bucketIds, nodeIdUpperBound, guard))
          continue;
        filteredDegrees[a] = filteredDegrees(provider, view, far, bucketIds, nodeIdUpperBound, guard);
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

  /** The degree of every node counting only the neighbors whose bucket is in {@code farBuckets}. */
  private static int[] filteredDegrees(final GraphTraversalProvider provider, final NeighborView view,
      final IntHashSet farBuckets, final int[] bucketIds, final int nodeIdUpperBound, final WorkGuard guard) {
    final int[] degrees = new int[nodeIdUpperBound];
    final int[] neighbors = view.neighbors();
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (!provider.isNodeLive(v))
        continue;
      int count = 0;
      for (int j = view.offset(v), end = view.offsetEnd(v); j < end; j++)
        if (farBuckets.contains(bucketIds[neighbors[j]]))
          count++;
      degrees[v] = count;
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
          for (final int neighbor : provider.getNeighborIds(v, arms[a].directions[0], arms[a].edgeTypes[0]))
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
      } else if (arms[a].edgeTypes.length == 1) {
        // Bulk degree computation — single pass over CSR offset arrays
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
    // edges in one type), and a degree built by iterating the type's records answers 0 for them (#9484).
    return executeOLTPPerVertex(db, guard);
  }

  private boolean hasMandatoryArm() {
    for (final Arm arm : arms)
      if (!arm.optional)
        return true;
    return false;
  }

  /**
   * Per-vertex iteration: every arm is counted on the edge lists of the central vertex, so light edges and record edges
   * weigh the same.
   */
  private long executeOLTPPerVertex(final Database db, final WorkGuard guard) {
    final IntHashSet[][] armBuckets = new IntHashSet[arms.length][];
    for (int a = 0; a < arms.length; a++)
      armBuckets[a] = arms[a].endpointBuckets(db);

    long total = 0;
    for (final Iterator<? extends Identifiable> it = db.iterateType(centralLabel, true); it.hasNext(); ) {
      guard.check();
      final Vertex v = it.next().asVertex();
      long product = 1;
      for (int a = 0; a < arms.length; a++) {
        final Arm arm = arms[a];
        long armCount;
        if (arm.edgeTypes.length == 1 && armBuckets[a] == null)
          armCount = v.countEdges(arm.directions[0], arm.edgeTypes[0]);
        else
          armCount = countArmOLTP(v, arm, armBuckets[a], 0);

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

  private long countArmOLTP(final Vertex vertex, final Arm arm, final IntHashSet[] buckets, final int hopIndex) {
    if (hopIndex >= arm.edgeTypes.length)
      return 1;
    final IntHashSet reached = buckets == null ? null : buckets[hopIndex];
    // Tail optimization: at the last hop with nothing to check on the endpoint, use countEdges instead of loading all
    // neighbor vertices
    if (hopIndex == arm.edgeTypes.length - 1 && reached == null)
      return vertex.countEdges(arm.directions[hopIndex], arm.edgeTypes[hopIndex]);
    long count = 0;
    final Iterator<Vertex> neighbors = vertex.getVertices(arm.directions[hopIndex], arm.edgeTypes[hopIndex]).iterator();
    while (neighbors.hasNext()) {
      final Vertex next = neighbors.next();
      if (reached != null && !reached.contains(next.getIdentity().getBucketId()))
        continue;
      count += countArmOLTP(next, arm, buckets, hopIndex + 1);
    }
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
        if (arms[i].endpointLabels != null && arms[i].endpointLabels[j] != null)
          sb.append("(:").append(arms[i].endpointLabels[j]).append(")");
      }
    }
    return sb.toString();
  }
}
