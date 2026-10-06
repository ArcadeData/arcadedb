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
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import com.arcadedb.query.QueryEngineManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  public long execute(final GraphTraversalProvider provider, final Database db, final WorkGuard guard) {
    final int nodeIdUpperBound = provider.getNodeIdUpperBound();
    final int[] personPartition = buildPartitionMapping(provider, nodeIdUpperBound, guard);
    if (personPartition == null)
      return executeWeighted(provider, nodeIdUpperBound, guard);

    final NeighborView knowsView = provider.getNeighborView(Vertex.DIRECTION.BOTH, triangleEdgeType);
    if (knowsView == null)
      return countTrianglesPerNode(provider, personPartition, nodeIdUpperBound, guard);

    final int[] nbrs = knowsView.neighbors();

    final int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    final long[] partialCounts = new long[threadCount];

    if (nodeIdUpperBound < 1000) {
      partialCounts[0] = countRange(provider, knowsView, nbrs, personPartition, 0, nodeIdUpperBound, guard);
    } else {
      final ExecutorService executor = QueryEngineManager.getInstance().getExecutorService();
      final Future<?>[] futures = new Future<?>[threadCount - 1];
      final int chunkSize = (nodeIdUpperBound + threadCount - 1) / threadCount;
      int launched = 0;
      for (int t = 1; t < threadCount; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, nodeIdUpperBound);
        if (start >= nodeIdUpperBound)
          break;
        final int threadIdx = t;
        futures[launched++] = executor.submit(() ->
            partialCounts[threadIdx] = countRange(provider, knowsView, nbrs, personPartition, start, end, guard));
      }
      // #4952: the calling thread runs chunk 0 itself (same discipline as GraphAlgorithms.parallelForRange)
      // instead of submitting ALL chunks and blocking. Submitting everything meant that, when this operator
      // was reached from a pool thread (or with the pool full of blocked producers), the caller waited on
      // tasks queued behind threads that were themselves waiting: a pool-starvation deadlock.
      // #6568: the caller-runs rejection was never the mitigation it was taken for - it fires only once the
      // queue is FULL. What makes the wait below safe is that awaitFutures RECLAIMS a still-queued chunk and
      // runs it here; chunk 0 on the caller is now a latency win rather than the liveness argument.
      // #5063: chunk 0 runs inside try/finally so the submitted chunks are ALWAYS awaited.
      // Without it, a chunk-0 exception unwound this frame while chunks 1..N-1 kept running and writing
      // into partialCounts (and holding pool threads) behind the caller's back.
      boolean chunk0Completed = false;
      try {
        partialCounts[0] = countRange(provider, knowsView, nbrs, personPartition, 0,
            Math.min(chunkSize, nodeIdUpperBound), guard);
        chunk0Completed = true;
      } finally {
        // #4951: awaitFutures throws on interrupt (cancelling the outstanding chunks) instead of returning,
        // so a killed/timed-out query can never sum partial (and still-being-written) partialCounts as a
        // complete answer.
        try {
          GraphAlgorithms.awaitFutures(futures, launched);
        } catch (final RuntimeException awaitError) {
          if (chunk0Completed)
            throw awaitError;
          // Chunk 0's own exception is already propagating and stays primary; the await above only had to
          // guarantee that no background chunk outlives this frame.
        }
      }
    }

    long total = 0;
    for (final long pc : partialCounts)
      total += pc;
    return total;
  }

  private static long countRange(final GraphTraversalProvider provider, final NeighborView knowsView, final int[] nbrs,
      final int[] personPartition, final int start, final int end, final WorkGuard guard) {
    long count = 0;
    for (int u = start; u < end; u++) {
      // Triangle counting is superlinear in the degree, so the per-anchor check is what bounds the whole
      // range: one high-degree anchor already costs O(d^2) intersections (issue #6266).
      guard.check();
      if (!provider.isNodeLive(u))
        continue;
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
            // a value occurring m times in one range and n times in the other is m * n matches (issue #9298)
            final int ue = runEnd(nbrs, iu, uEnd), ve = runEnd(nbrs, iv, vEnd);
            if (personPartition[nu] == country)
              count += (long) (ue - iu) * (ve - iv);
            iu = ue;
            iv = ve;
          }
        }
      }
    }
    return count;
  }

  /** End (exclusive) of the run of values equal to {@code a[from]} in the sorted range {@code [from, to)}. */
  private static int runEnd(final int[] a, final int from, final int to) {
    final int value = a[from];
    int end = from + 1;
    while (end < to && a[end] == value)
      end++;
    return end;
  }

  /**
   * One partition per node, or {@code null} when some node on a partition chain has more than one neighbour: such a node is
   * in several partitions (or reaches one through several paths), and a single partition per node would count it in one
   * only (issue #9350). The caller then takes {@link #executeWeighted}.
   */
  private int[] buildPartitionMapping(final GraphTraversalProvider provider, final int nodeIdUpperBound,
      final WorkGuard guard) {
    final int[] partition = new int[nodeIdUpperBound];
    Arrays.fill(partition, -1);

    final int chainLength = partitionEdgeTypes.length;
    final NeighborView[] views = new NeighborView[chainLength];
    boolean allViewsAvailable = true;
    for (int h = 0; h < chainLength; h++) {
      views[h] = provider.getNeighborView(partitionDirections[h], partitionEdgeTypes[h]);
      if (views[h] == null)
        allViewsAvailable = false;
    }

    // #6943: a NeighborView is unavailable while a delta overlay is active (GraphAnalyticalView.getNeighborView
    // returns null unconditionally in that state) - the normal condition of a view between commits, not an
    // exotic one. Falling straight through to an all-(-1) mapping made every consumer read "no vertex is in any
    // partition" instead of "the fast path is unavailable", so the whole query silently answered 0.
    //
    // Rather than bailing out of the CSR path entirely (the OLTP fallback below still exercises it), walk the
    // chain with the per-node accessor: it is specified to work regardless of overlay state (see
    // GraphTraversalProvider#getNeighborIds javadoc) and is the same fallback #countTrianglesPerNode already
    // relies on when the triangle-edge view itself is missing. This keeps the mapping on the CSR node-id path -
    // no RID/Vertex materialization - even when the fast array-offset scan below cannot be used.
    if (!allViewsAvailable) {
      for (int p = 0; p < nodeIdUpperBound; p++) {
        guard.checkPeriodically(p);
        if (!provider.isNodeLive(p))
          continue;
        int current = p;
        boolean valid = true;
        for (int h = 0; h < chainLength; h++) {
          final int[] hNbrs = provider.getNeighborIds(current, partitionDirections[h], partitionEdgeTypes[h]);
          if (hNbrs.length == 0) {
            valid = false;
            break;
          }
          if (hNbrs.length > 1)
            return null;
          current = hNbrs[0];
        }
        if (valid)
          partition[p] = current;
      }
      return partition;
    }

    final NeighborView firstView = views[0];
    final int[] firstNbrs = firstView.neighbors();

    for (int p = 0; p < nodeIdUpperBound; p++) {
      guard.checkPeriodically(p);
      if (!provider.isNodeLive(p))
        continue;
      final int fStart = firstView.offset(p);
      final int fEnd = firstView.offsetEnd(p);
      if (fStart == fEnd)
        continue;
      if (fEnd - fStart > 1)
        return null;

      int current = firstNbrs[fStart];
      boolean valid = true;
      for (int h = 1; h < chainLength; h++) {
        final int hStart = views[h].offset(current);
        final int hEnd = views[h].offsetEnd(current);
        if (hStart == hEnd) {
          valid = false;
          break;
        }
        if (hEnd - hStart > 1)
          return null;
        current = views[h].neighbors()[hStart];
      }
      if (valid)
        partition[p] = current;
    }
    return partition;
  }

  /**
   * Exact count for a partition chain that is not a function (a person with two cities, a city in two countries): every path
   * of the chain from a node to a country is a match of its own, so a node carries a weight per country, the number of paths
   * that reach it, and a triangle counts the product of the three weights of each country they share (issue #9350).
   */
  private long executeWeighted(final GraphTraversalProvider provider, final int nodeIdUpperBound, final WorkGuard guard) {
    final int[][] countries = new int[nodeIdUpperBound][];
    final long[][] weights = new long[nodeIdUpperBound][];
    for (int p = 0; p < nodeIdUpperBound; p++) {
      guard.checkPeriodically(p);
      if (!provider.isNodeLive(p))
        continue;
      Map<Integer, Long> current = new HashMap<>();
      current.put(p, 1L);
      for (int h = 0; h < partitionEdgeTypes.length && !current.isEmpty(); h++) {
        final Map<Integer, Long> next = new HashMap<>();
        for (final Map.Entry<Integer, Long> e : current.entrySet())
          for (final int n : provider.getNeighborIds(e.getKey(), partitionDirections[h], partitionEdgeTypes[h]))
            next.merge(n, e.getValue(), Long::sum);
        current = next;
      }
      if (current.isEmpty())
        continue;
      final int[] keys = new int[current.size()];
      int i = 0;
      for (final Integer k : current.keySet())
        keys[i++] = k;
      Arrays.sort(keys);
      final long[] w = new long[keys.length];
      for (i = 0; i < keys.length; i++)
        w[i] = current.get(keys[i]);
      countries[p] = keys;
      weights[p] = w;
    }

    long total = 0;
    for (int u = 0; u < nodeIdUpperBound; u++) {
      guard.check();
      if (countries[u] == null)
        continue;
      final int[] uNeighbors = provider.getNeighborIds(u, Vertex.DIRECTION.BOTH, triangleEdgeType);
      for (final int v : uNeighbors) {
        if (countries[v] == null)
          continue;
        final int[] vNeighbors = provider.getNeighborIds(v, Vertex.DIRECTION.BOTH, triangleEdgeType);
        int iu = 0, iv = 0;
        while (iu < uNeighbors.length && iv < vNeighbors.length) {
          if (uNeighbors[iu] < vNeighbors[iv])
            iu++;
          else if (uNeighbors[iu] > vNeighbors[iv])
            iv++;
          else {
            final int w = uNeighbors[iu];
            final int ue = runEnd(uNeighbors, iu, uNeighbors.length), ve = runEnd(vNeighbors, iv, vNeighbors.length);
            if (countries[w] != null)
              total += (long) (ue - iu) * (ve - iv) * sharedWeight(countries[u], weights[u], countries[v], weights[v], countries[w],
                  weights[w]);
            iu = ue;
            iv = ve;
          }
        }
      }
    }
    return total;
  }

  /** Sum over the countries the three sorted arrays share of the product of the three weights. */
  private static long sharedWeight(final int[] ca, final long[] wa, final int[] cb, final long[] wb, final int[] cc, final long[] wc) {
    long sum = 0;
    int ia = 0, ib = 0, ic = 0;
    while (ia < ca.length && ib < cb.length && ic < cc.length) {
      final int a = ca[ia], b = cb[ib], c = cc[ic];
      if (a == b && b == c) {
        sum += wa[ia] * wb[ib] * wc[ic];
        ia++;
        ib++;
        ic++;
      } else {
        final int max = Math.max(a, Math.max(b, c));
        if (a < max)
          ia++;
        if (b < max)
          ib++;
        if (c < max)
          ic++;
      }
    }
    return sum;
  }

  private long countTrianglesPerNode(final GraphTraversalProvider provider,
      final int[] personPartition, final int nodeIdUpperBound, final WorkGuard guard) {
    long total = 0;
    for (int u = 0; u < nodeIdUpperBound; u++) {
      guard.check();
      if (!provider.isNodeLive(u))
        continue;
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
            final int value = uNeighbors[iu];
            final int ue = runEnd(uNeighbors, iu, uNeighbors.length), ve = runEnd(vNeighbors, iv, vNeighbors.length);
            if (personPartition[value] == country)
              total += (long) (ue - iu) * (ve - iv);
            iu = ue;
            iv = ve;
          }
        }
      }
    }
    return total;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    // every path of the partition chain is a match of its own, so a vertex carries the number of paths per country (issue #9350)
    final HashMap<RID, HashMap<RID, Long>> personToPartitions = new HashMap<>();

    for (final DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof VertexType))
        continue;
      for (final Iterator<? extends Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        guard.check();
        final Vertex v = it.next().asVertex();
        HashMap<RID, Long> current = new HashMap<>();
        current.put(v.getIdentity(), 1L);
        for (int h = 0; h < partitionEdgeTypes.length && !current.isEmpty(); h++) {
          final HashMap<RID, Long> next = new HashMap<>();
          for (final Map.Entry<RID, Long> e : current.entrySet())
            for (final RID n : ((Vertex) db.lookupByRID(e.getKey(), true)).getConnectedVertexRIDs(partitionDirections[h],
                partitionEdgeTypes[h]))
              next.merge(n, e.getValue(), Long::sum);
          current = next;
        }
        if (!current.isEmpty())
          personToPartitions.put(v.getIdentity(), current);
      }
    }

    // Try GAV provider for accelerated neighbor lookups
    final GraphTraversalProvider gavProvider = GraphTraversalProviderRegistry.findProvider(db, triangleEdgeType);

    long total = 0;
    for (final Map.Entry<RID, HashMap<RID, Long>> entry : personToPartitions.entrySet()) {
      guard.check();
      final RID uRid = entry.getKey();
      final HashMap<RID, Long> uCountries = entry.getValue();

      final RID[] uNeighbors = getNeighborRIDs(db, gavProvider, uRid, Vertex.DIRECTION.BOTH, triangleEdgeType);
      for (final RID vRid : uNeighbors) {
        final HashMap<RID, Long> vCountries = personToPartitions.get(vRid);
        if (vCountries == null)
          continue;

        // multiplicity of each in-partition neighbour of u: a parallel edge is a match of its own (issue #9298). The boxing is
        // deliberate: this is the no-view fallback, which already materializes RIDs and a RID-keyed partition map per call
        final HashMap<RID, Integer> uNeighborCounts = new HashMap<>();
        for (final RID nRid : uNeighbors)
          if (personToPartitions.containsKey(nRid))
            uNeighborCounts.merge(nRid, 1, Integer::sum);
        final RID[] vNeighbors = getNeighborRIDs(db, gavProvider, vRid, Vertex.DIRECTION.BOTH, triangleEdgeType);
        for (final RID wRid : vNeighbors) {
          final Integer multiplicity = uNeighborCounts.get(wRid);
          if (multiplicity == null)
            continue;
          final HashMap<RID, Long> wCountries = personToPartitions.get(wRid);
          long shared = 0;
          for (final Map.Entry<RID, Long> c : uCountries.entrySet()) {
            final Long wv = vCountries.get(c.getKey()), ww = wCountries.get(c.getKey());
            if (wv != null && ww != null)
              shared += c.getValue() * wv * ww;
          }
          total += multiplicity * shared;
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
    final List<RID> list = new ArrayList<>();
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
