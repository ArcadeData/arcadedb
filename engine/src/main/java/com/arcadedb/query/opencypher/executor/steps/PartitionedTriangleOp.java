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
import com.arcadedb.utility.IntHashSet;

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
  /** One label per node of the partition chain (the first is the node itself, the last the partition), null where unlabelled. */
  private final String[] nodeLabels;

  /** The bucket ids each labelled position stands for, and the bucket of every node: what a label filter reads. */
  private static final class LabelFilter {
    final IntHashSet[] buckets;
    final int[]        bucketIds;

    LabelFilter(final IntHashSet[] buckets, final int[] bucketIds) {
      this.buckets = buckets;
      this.bucketIds = bucketIds;
    }

    boolean accepts(final int position, final int node) {
      final IntHashSet allowed = buckets[position];
      return allowed == null || allowed.contains(bucketIds[node]);
    }
  }

  public PartitionedTriangleOp(final String[] partitionEdgeTypes,
      final Vertex.DIRECTION[] partitionDirections,
      final String triangleEdgeType) {
    this(partitionEdgeTypes, partitionDirections, triangleEdgeType, null);
  }

  public PartitionedTriangleOp(final String[] partitionEdgeTypes,
      final Vertex.DIRECTION[] partitionDirections,
      final String triangleEdgeType, final String[] nodeLabels) {
    this.partitionEdgeTypes = partitionEdgeTypes;
    this.partitionDirections = partitionDirections;
    this.triangleEdgeType = triangleEdgeType;
    boolean anyLabel = false;
    if (nodeLabels != null)
      for (final String label : nodeLabels)
        anyLabel |= label != null;
    this.nodeLabels = anyLabel ? nodeLabels : null;

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
    final LabelFilter filter = buildLabelFilter(provider, db, nodeIdUpperBound, guard);
    final int[] personPartition = buildPartitionMapping(provider, nodeIdUpperBound, guard, filter);
    if (personPartition == null)
      return executeWeighted(provider, nodeIdUpperBound, guard, filter);

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

  /** Resolves each label to the buckets of its type (sub-types included); null when no position is labelled. */
  private LabelFilter buildLabelFilter(final GraphTraversalProvider provider, final Database db, final int nodeIdUpperBound,
      final WorkGuard guard) {
    if (nodeLabels == null)
      return null;
    final IntHashSet[] buckets = new IntHashSet[nodeLabels.length];
    for (int i = 0; i < buckets.length; i++)
      buckets[i] = CSRCountUtils.buildValidBuckets(db, nodeLabels[i]);
    final int[] bucketIds = new int[nodeIdUpperBound];
    Arrays.fill(bucketIds, -1);
    for (int v = 0; v < nodeIdUpperBound; v++) {
      guard.checkPeriodically(v);
      if (provider.isNodeLive(v))
        bucketIds[v] = provider.getRID(v).getBucketId();
    }
    return new LabelFilter(buckets, bucketIds);
  }

  /** The neighbours of {@code node} over hop {@code hop} that the label of the node the hop reaches lets through. */
  private int[] acceptedNeighbors(final GraphTraversalProvider provider, final int node, final int hop, final LabelFilter filter) {
    final int[] neighbors = provider.getNeighborIds(node, partitionDirections[hop], partitionEdgeTypes[hop]);
    if (filter == null || filter.buckets[hop + 1] == null)
      return neighbors;
    int kept = 0;
    for (final int n : neighbors)
      if (filter.accepts(hop + 1, n))
        kept++;
    if (kept == neighbors.length)
      return neighbors;
    final int[] result = new int[kept];
    kept = 0;
    for (final int n : neighbors)
      if (filter.accepts(hop + 1, n))
        result[kept++] = n;
    return result;
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
      final WorkGuard guard, final LabelFilter filter) {
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
        if (!provider.isNodeLive(p) || (filter != null && !filter.accepts(0, p)))
          continue;
        int current = p;
        boolean valid = true;
        for (int h = 0; h < chainLength; h++) {
          final int[] hNbrs = acceptedNeighbors(provider, current, h, filter);
          if (hNbrs.length == 0) {
            valid = false;
            break;
          }
          if (hNbrs.length > 1)
            return null; // ambiguous chain: the caller takes the weighted path
          current = hNbrs[0];
        }
        if (valid)
          partition[p] = current;
      }
      return partition;
    }

    final NeighborView firstView = views[0];
    final int[] firstNbrs = firstView.neighbors();
    final int[][] hopNbrs = new int[chainLength][];
    for (int h = 1; h < chainLength; h++)
      hopNbrs[h] = views[h].neighbors();

    for (int p = 0; p < nodeIdUpperBound; p++) {
      guard.checkPeriodically(p);
      if (!provider.isNodeLive(p) || (filter != null && !filter.accepts(0, p)))
        continue;
      // the neighbours the label of the node a hop reaches lets through are the ones that count: a neighbour it rejects is
      // no match, so it is neither the single step nor part of an ambiguity
      int current = singleStep(firstView, firstNbrs, p, 0, filter);
      if (current == NO_STEP)
        continue;
      if (current == AMBIGUOUS_STEP)
        return null; // ambiguous chain: the caller takes the weighted path
      boolean valid = true;
      for (int h = 1; h < chainLength; h++) {
        final int next = singleStep(views[h], hopNbrs[h], current, h, filter);
        if (next == NO_STEP) {
          valid = false;
          break;
        }
        if (next == AMBIGUOUS_STEP)
          return null; // ambiguous chain: the caller takes the weighted path
        current = next;
      }
      if (valid)
        partition[p] = current;
    }
    return partition;
  }

  private static final int NO_STEP        = -1;
  private static final int AMBIGUOUS_STEP = -2;

  /** The one neighbour of {@code node} over hop {@code hop} the label lets through, {@link #NO_STEP} for none, {@link #AMBIGUOUS_STEP} for several. */
  private static int singleStep(final NeighborView view, final int[] nbrs, final int node, final int hop, final LabelFilter filter) {
    final int start = view.offset(node), end = view.offsetEnd(node);
    if (filter == null || filter.buckets[hop + 1] == null) {
      if (start == end)
        return NO_STEP;
      return end - start > 1 ? AMBIGUOUS_STEP : nbrs[start];
    }
    int found = NO_STEP;
    for (int k = start; k < end; k++) {
      if (!filter.accepts(hop + 1, nbrs[k]))
        continue;
      if (found != NO_STEP)
        return AMBIGUOUS_STEP;
      found = nbrs[k];
    }
    return found;
  }

  /**
   * Exact count for a partition chain that is not a function (a person with two cities, a city in two countries): every path
   * of the chain from a node to a country is a match of its own, so a node carries a weight per country, the number of paths
   * that reach it, and a triangle counts the product of the three weights of each country they share (issue #9350). This is
   * the slow path, taken only for an ambiguous chain: it boxes per node and hop, and the products are exact, so an overflow
   * raises an {@link ArithmeticException} instead of returning a wrong count.
   */
  private long executeWeighted(final GraphTraversalProvider provider, final int nodeIdUpperBound, final WorkGuard guard,
      final LabelFilter filter) {
    final int[][] countries = new int[nodeIdUpperBound][];
    final long[][] weights = new long[nodeIdUpperBound][];
    for (int p = 0; p < nodeIdUpperBound; p++) {
      guard.checkPeriodically(p);
      if (!provider.isNodeLive(p) || (filter != null && !filter.accepts(0, p)))
        continue;
      Map<Integer, Long> current = new HashMap<>();
      current.put(p, 1L);
      for (int h = 0; h < partitionEdgeTypes.length && !current.isEmpty(); h++) {
        final Map<Integer, Long> next = new HashMap<>();
        for (final Map.Entry<Integer, Long> e : current.entrySet())
          for (final int n : acceptedNeighbors(provider, e.getKey(), h, filter))
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
              total = Math.addExact(total, Math.multiplyExact((long) (ue - iu) * (ve - iv),
                  sharedWeight(countries[u], weights[u], countries[v], weights[v], countries[w], weights[w])));
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
        sum = Math.addExact(sum, Math.multiplyExact(Math.multiplyExact(wa[ia], wb[ib]), wc[ic]));
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

  /**
   * The partitions a vertex reaches, each with the number of paths of the partition chain that reach it (issue #9350). Most
   * vertices reach exactly one, so that case is a pair of fields and no map: the common case of one neighbour per hop pays
   * nothing for the weighted answer (issue #9400).
   */
  private static final class Partitions {
    private static final int INDEXED_FROM = 8;

    final RID[]              rids;
    final long[]             weights;
    // only for a vertex reaching many partitions, where a linear scan per lookup would cost more than the map
    final HashMap<RID, Long> index;

    Partitions(final RID[] rids, final long[] weights) {
      this.rids = rids;
      this.weights = weights;
      if (rids.length > INDEXED_FROM) {
        index = new HashMap<>(rids.length * 2);
        for (int i = 0; i < rids.length; i++)
          index.put(rids[i], weights[i]);
      } else
        index = null;
    }

    /** The number of paths reaching {@code partition}, 0 when none does. */
    long weightOf(final RID partition) {
      if (index != null) {
        final Long w = index.get(partition);
        return w == null ? 0L : w;
      }
      for (int i = 0; i < rids.length; i++)
        if (rids[i].equals(partition))
          return weights[i];
      return 0L;
    }
  }

  /** Whether {@code n}, reached by hop {@code h} of the chain, carries the label the pattern asks for there, if any. */
  private boolean passesHopLabel(final Database db, final RID n, final int h) {
    return nodeLabels == null || nodeLabels[h + 1] == null
        || typeCarriesLabel(db.getSchema().getTypeByBucketId(n.getBucketId()), nodeLabels[h + 1]);
  }

  /** The partitions of {@code v}, or null when its chain ends before reaching any. */
  private Partitions partitionsOf(final Database db, final Vertex v) {
    RID[] rids = { v.getIdentity() };
    long[] weights = { 1L };
    for (int h = 0; h < partitionEdgeTypes.length; h++) {
      if (rids.length == 1) {
        // fast path: one vertex to expand, and when it has one neighbour that neighbour inherits its weight as it is
        final Vertex from = h == 0 ? v : db.lookupByRID(rids[0], true).asVertex();
        final Iterator<RID> neighbors = from.getConnectedVertexRIDs(partitionDirections[h], partitionEdgeTypes[h]).iterator();
        RID first = null;
        while (first == null && neighbors.hasNext()) {
          final RID candidate = neighbors.next();
          if (passesHopLabel(db, candidate, h))
            first = candidate;
        }
        if (first == null)
          return null;
        RID second = null;
        while (second == null && neighbors.hasNext()) {
          final RID candidate = neighbors.next();
          if (passesHopLabel(db, candidate, h))
            second = candidate;
        }
        if (second == null) {
          rids = new RID[] { first };
          continue;
        }
        // several neighbours: every path is a match of its own, so a neighbour reached twice weighs twice
        final HashMap<RID, Long> next = new HashMap<>();
        next.merge(first, weights[0], Long::sum);
        next.merge(second, weights[0], Long::sum);
        while (neighbors.hasNext()) {
          final RID n = neighbors.next();
          if (passesHopLabel(db, n, h))
            next.merge(n, weights[0], Long::sum);
        }
        final int size = next.size();
        rids = new RID[size];
        weights = new long[size];
        int i = 0;
        for (final Map.Entry<RID, Long> e : next.entrySet()) {
          rids[i] = e.getKey();
          weights[i++] = e.getValue();
        }
        continue;
      }

      final HashMap<RID, Long> next = new HashMap<>();
      for (int i = 0; i < rids.length; i++)
        for (final RID n : db.lookupByRID(rids[i], true).asVertex().getConnectedVertexRIDs(partitionDirections[h], partitionEdgeTypes[h]))
          if (passesHopLabel(db, n, h))
            next.merge(n, weights[i], Long::sum);
      if (next.isEmpty())
        return null;
      final int size = next.size();
      rids = new RID[size];
      weights = new long[size];
      int i = 0;
      for (final Map.Entry<RID, Long> e : next.entrySet()) {
        rids[i] = e.getKey();
        weights[i++] = e.getValue();
      }
    }
    return new Partitions(rids, weights);
  }

  /** The number of paths of the partition chain that reach the same partition from the three vertices. */
  private static long sharedWeight(final Partitions u, final Partitions v, final Partitions w) {
    long shared = 0;
    for (int i = 0; i < u.rids.length; i++) {
      final long wv = v.weightOf(u.rids[i]);
      if (wv == 0L)
        continue;
      final long ww = w.weightOf(u.rids[i]);
      if (ww != 0L)
        shared = Math.addExact(shared, Math.multiplyExact(Math.multiplyExact(u.weights[i], wv), ww));
    }
    return shared;
  }

  /** The length of the run of {@code value} that starts at {@code from} in {@code sorted}. */
  private static int runLength(final int[] sorted, final int from) {
    int to = from + 1;
    while (to < sorted.length && sorted[to] == sorted[from])
      ++to;
    return to - from;
  }

  @Override
  public long executeOLTP(final Database db, final WorkGuard guard) {
    // every path of the partition chain is a match of its own, so a vertex carries the number of paths per country (issue #9350)
    final ArrayList<RID> persons = new ArrayList<>();
    final ArrayList<Partitions> personPartitions = new ArrayList<>();
    final HashMap<RID, Integer> indexOf = new HashMap<>();

    for (final DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof VertexType) || !typeCarriesLabel(dt, nodeLabels == null ? null : nodeLabels[0]))
        continue;
      for (final Iterator<? extends Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        guard.check();
        final Vertex v = it.next().asVertex();
        final Partitions partitions = partitionsOf(db, v);
        if (partitions != null) {
          indexOf.put(v.getIdentity(), persons.size());
          persons.add(v.getIdentity());
          personPartitions.add(partitions);
        }
      }
    }

    // Try GAV provider for accelerated neighbor lookups
    final GraphTraversalProvider gavProvider = GraphTraversalProviderRegistry.findProvider(db, triangleEdgeType);

    // The edge list of every vertex is read ONCE (issue #9400): the triangle loop below meets each vertex once per
    // neighbour, and reloading its edges each time dominated the whole count. Only the in-partition neighbours are kept,
    // as sorted indexes, a neighbour reached by parallel edges once per edge (a parallel edge is a match of its own, issue #9298)
    final int count = persons.size();
    final int[][] adjacency = new int[count][];
    int[] buffer = new int[16];
    for (int i = 0; i < count; i++) {
      guard.check();
      final RID[] neighbors = getNeighborRIDs(db, gavProvider, persons.get(i), Vertex.DIRECTION.BOTH, triangleEdgeType);
      if (buffer.length < neighbors.length)
        buffer = new int[Math.max(neighbors.length, buffer.length * 2)];
      int size = 0;
      for (final RID n : neighbors) {
        final Integer index = indexOf.get(n);
        if (index != null)
          buffer[size++] = index;
      }
      final int[] sorted = Arrays.copyOf(buffer, size);
      Arrays.sort(sorted);
      adjacency[i] = sorted;
    }

    final Partitions[] partitions = personPartitions.toArray(new Partitions[0]);
    long total = 0;
    for (int u = 0; u < count; u++) {
      guard.check();
      final int[] uAdjacent = adjacency[u];
      for (final int v : uAdjacent) {
        // the triangles closing the wedge u-v: the vertices w both are connected to, with the multiplicity of each edge
        final int[] vAdjacent = adjacency[v];
        int i = 0, j = 0;
        while (i < uAdjacent.length && j < vAdjacent.length) {
          final int a = uAdjacent[i], b = vAdjacent[j];
          if (a < b)
            ++i;
          else if (a > b)
            ++j;
          else {
            final int uRun = runLength(uAdjacent, i), vRun = runLength(vAdjacent, j);
            final long shared = sharedWeight(partitions[u], partitions[v], partitions[a]);
            if (shared != 0L)
              total = Math.addExact(total, Math.multiplyExact(Math.multiplyExact((long) uRun, vRun), shared));
            i += uRun;
            j += vRun;
          }
        }
      }
    }
    return total;
  }

  /** Whether {@code type} is the labelled type or one of its sub-types; a null label lets everything through. */
  private static boolean typeCarriesLabel(final DocumentType type, final String label) {
    return label == null || (type != null && type.instanceOf(label));
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
