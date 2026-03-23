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
import com.arcadedb.utility.RidHashSet;

import com.arcadedb.query.QueryEngineManager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Count operator for country-partitioned triangle patterns (Q3).
 * Uses sorted intersection of CSR adjacency lists for triangle counting.
 */
public final class PartitionedTriangleOp implements CountOp {
  private final String[] partitionEdgeTypes;
  private final Vertex.DIRECTION[] partitionDirections;
  private final String triangleEdgeType;
  private final String[] allEdgeTypes;

  public PartitionedTriangleOp(final String[] partitionEdgeTypes,
      final Vertex.DIRECTION[] partitionDirections,
      final String triangleEdgeType) {
    this.partitionEdgeTypes = partitionEdgeTypes;
    this.partitionDirections = partitionDirections;
    this.triangleEdgeType = triangleEdgeType;

    this.allEdgeTypes = new String[partitionEdgeTypes.length + 1];
    System.arraycopy(partitionEdgeTypes, 0, allEdgeTypes, 0, partitionEdgeTypes.length);
    allEdgeTypes[partitionEdgeTypes.length] = triangleEdgeType;
  }

  @Override
  public String[] edgeTypes() {
    return allEdgeTypes;
  }

  @Override
  public long execute(final GraphTraversalProvider provider, final Database db) {
    final int nodeCount = provider.getNodeCount();
    final int[] personPartition = buildPartitionMapping(provider, nodeCount);

    final NeighborView knowsView = provider.getNeighborView(Vertex.DIRECTION.BOTH, triangleEdgeType);
    if (knowsView == null)
      return countTrianglesPerNode(provider, personPartition, nodeCount);

    final int[] nbrs = knowsView.neighbors();

    final int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    final long[] partialCounts = new long[threadCount];

    if (nodeCount < 1000) {
      partialCounts[0] = countRange(knowsView, nbrs, personPartition, 0, nodeCount);
    } else {
      final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
      final Future<?>[] futures = new Future<?>[threadCount];
      final int chunkSize = (nodeCount + threadCount - 1) / threadCount;
      for (int t = 0; t < threadCount; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, nodeCount);
        final int threadIdx = t;
        futures[t] = executor.submit(() ->
            partialCounts[threadIdx] = countRange(knowsView, nbrs, personPartition, start, end));
      }
      for (final Future<?> future : futures) {
        try {
          future.get();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (final ExecutionException e) {
          throw new RuntimeException("Parallel triangle counting failed", e.getCause());
        }
      }
    }

    long total = 0;
    for (final long pc : partialCounts)
      total += pc;
    return total;
  }

  private static long countRange(final NeighborView knowsView, final int[] nbrs,
      final int[] personPartition, final int start, final int end) {
    long count = 0;
    for (int u = start; u < end; u++) {
      final int country = personPartition[u];
      if (country < 0)
        continue;

      final int uStart = knowsView.offset(u);
      final int uEnd = knowsView.offsetEnd(u);

      for (int k = uStart; k < uEnd; k++) {
        final int v = nbrs[k];
        if (personPartition[v] != country)
          continue;

        final int vStart = knowsView.offset(v);
        final int vEnd = knowsView.offsetEnd(v);
        int iu = uStart, iv = vStart;
        while (iu < uEnd && iv < vEnd) {
          final int nu = nbrs[iu], nv = nbrs[iv];
          if (nu < nv)
            iu++;
          else if (nu > nv)
            iv++;
          else {
            if (personPartition[nu] == country)
              count++;
            iu++;
            iv++;
          }
        }
      }
    }
    return count;
  }

  private int[] buildPartitionMapping(final GraphTraversalProvider provider, final int nodeCount) {
    final int[] partition = new int[nodeCount];
    Arrays.fill(partition, -1);

    final int chainLength = partitionEdgeTypes.length;
    final NeighborView[] views = new NeighborView[chainLength];
    for (int h = 0; h < chainLength; h++)
      views[h] = provider.getNeighborView(partitionDirections[h], partitionEdgeTypes[h]);

    for (final NeighborView v : views)
      if (v == null)
        return partition;

    final NeighborView firstView = views[0];
    final int[] firstNbrs = firstView.neighbors();

    for (int p = 0; p < nodeCount; p++) {
      final int fStart = firstView.offset(p);
      final int fEnd = firstView.offsetEnd(p);
      if (fStart == fEnd)
        continue;

      int current = firstNbrs[fStart];
      boolean valid = true;
      for (int h = 1; h < chainLength; h++) {
        final int hStart = views[h].offset(current);
        final int hEnd = views[h].offsetEnd(current);
        if (hStart == hEnd) {
          valid = false;
          break;
        }
        current = views[h].neighbors()[hStart];
      }
      if (valid)
        partition[p] = current;
    }
    return partition;
  }

  private long countTrianglesPerNode(final GraphTraversalProvider provider,
      final int[] personPartition, final int nodeCount) {
    long total = 0;
    for (int u = 0; u < nodeCount; u++) {
      final int country = personPartition[u];
      if (country < 0)
        continue;
      final int[] uNeighbors = provider.getNeighborIds(u, Vertex.DIRECTION.BOTH, triangleEdgeType);
      for (final int v : uNeighbors) {
        if (personPartition[v] != country)
          continue;
        final int[] vNeighbors = provider.getNeighborIds(v, Vertex.DIRECTION.BOTH, triangleEdgeType);
        int iu = 0, iv = 0;
        while (iu < uNeighbors.length && iv < vNeighbors.length) {
          if (uNeighbors[iu] < vNeighbors[iv])
            iu++;
          else if (uNeighbors[iu] > vNeighbors[iv])
            iv++;
          else {
            if (personPartition[uNeighbors[iu]] == country)
              total++;
            iu++;
            iv++;
          }
        }
      }
    }
    return total;
  }

  @Override
  public long executeOLTP(final Database db) {
    final HashMap<RID, RID> personToPartition = new HashMap<>();

    for (final DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof VertexType))
        continue;
      for (final Iterator<? extends Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        final Vertex v = it.next().asVertex();
        Vertex cursor = v;
        boolean valid = true;
        for (int h = 0; h < partitionEdgeTypes.length; h++) {
          final Iterator<Vertex> neighbors = cursor.getVertices(partitionDirections[h], partitionEdgeTypes[h]).iterator();
          if (!neighbors.hasNext()) {
            valid = false;
            break;
          }
          cursor = neighbors.next();
        }
        if (valid)
          personToPartition.put(v.getIdentity(), cursor.getIdentity());
      }
    }

    // Try GAV provider for accelerated neighbor lookups
    final GraphTraversalProvider gavProvider = com.arcadedb.graph.GraphTraversalProviderRegistry.findProvider(db, triangleEdgeType);

    long total = 0;
    for (final Map.Entry<RID, RID> entry : personToPartition.entrySet()) {
      final RID uRid = entry.getKey();
      final RID uCountry = entry.getValue();

      final RID[] uNeighbors = getNeighborRIDs(db, gavProvider, uRid, Vertex.DIRECTION.BOTH, triangleEdgeType);
      for (final RID vRid : uNeighbors) {
        final RID vCountry = personToPartition.get(vRid);
        if (vCountry == null || !vCountry.equals(uCountry))
          continue;

        final RidHashSet uNeighborSet = new RidHashSet();
        for (final RID nRid : uNeighbors) {
          final RID nCountry = personToPartition.get(nRid);
          if (nCountry != null && nCountry.equals(uCountry))
            uNeighborSet.add(nRid);
        }
        final RID[] vNeighbors = getNeighborRIDs(db, gavProvider, vRid, Vertex.DIRECTION.BOTH, triangleEdgeType);
        for (final RID wRid : vNeighbors) {
          if (uNeighborSet.contains(wRid))
            total++;
        }
      }
    }
    return total;
  }

  /**
   * Gets neighbor RIDs using GAV/CSR when available, falling back to OLTP.
   */
  private static RID[] getNeighborRIDs(final Database db, final GraphTraversalProvider provider,
      final RID vertexRid, final Vertex.DIRECTION direction, final String edgeType) {
    if (provider != null) {
      final int nodeId = provider.getNodeId(vertexRid);
      if (nodeId >= 0) {
        final int[] neighborIds = provider.getNeighborIds(nodeId, direction, edgeType);
        final RID[] rids = new RID[neighborIds.length];
        for (int i = 0; i < neighborIds.length; i++)
          rids[i] = provider.getRID(neighborIds[i]);
        return rids;
      }
    }
    // OLTP fallback
    final Vertex v = (Vertex) db.lookupByRID(vertexRid, true);
    final java.util.List<RID> list = new java.util.ArrayList<>();
    for (final RID rid : v.getConnectedVertexRIDs(direction, edgeType))
      list.add(rid);
    return list.toArray(new RID[0]);
  }

  @Override
  public String describe(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT TRIANGLES (CSR sorted intersection, country-partitioned)\n");
    sb.append(ind).append("  triangle edge: ").append(triangleEdgeType);
    sb.append(", partition chain: ");
    for (int i = 0; i < partitionEdgeTypes.length; i++) {
      if (i > 0) sb.append(" → ");
      sb.append(partitionEdgeTypes[i]);
    }
    return sb.toString();
  }
}
