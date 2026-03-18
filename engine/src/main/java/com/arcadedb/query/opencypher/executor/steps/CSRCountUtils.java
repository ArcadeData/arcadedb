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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;

import java.util.HashSet;
import java.util.Set;

/**
 * Shared static utilities for CSR count-push-down operators.
 */
public final class CSRCountUtils {

  private CSRCountUtils() {
  }

  /**
   * Propagates counts one hop through the CSR adjacency. Uses NeighborView when available,
   * falls back to per-node neighbor lookup.
   *
   * @param provider the CSR provider
   * @param current  current path counts indexed by node ID
   * @param dir      traversal direction
   * @param edgeType edge type label
   * @return next-level counts
   */
  public static long[] propagateOneHop(final GraphTraversalProvider provider, final long[] current,
      final Vertex.DIRECTION dir, final String edgeType) {
    final int nodeCount = current.length;
    final long[] next = new long[nodeCount];
    final NeighborView view = provider.getNeighborView(dir, edgeType);
    if (view != null) {
      final int[] nbrs = view.neighbors();
      for (int v = 0; v < nodeCount; v++) {
        if (current[v] == 0)
          continue;
        final long pathCount = current[v];
        for (int j = view.offset(v), end = view.offsetEnd(v); j < end; j++)
          next[nbrs[j]] += pathCount;
      }
    } else {
      for (int v = 0; v < nodeCount; v++) {
        if (current[v] == 0)
          continue;
        final long pathCount = current[v];
        final int[] neighbors = provider.getNeighborIds(v, dir, edgeType);
        for (final int neighbor : neighbors)
          next[neighbor] += pathCount;
      }
    }
    return next;
  }

  /**
   * Zeros out entries in counts where the node's bucket ID is not in the valid set.
   */
  public static void filterByBuckets(final GraphTraversalProvider provider, final long[] counts,
      final Set<Integer> validBuckets) {
    if (validBuckets == null || validBuckets.isEmpty())
      return;
    for (int v = 0; v < counts.length; v++) {
      if (counts[v] > 0) {
        final RID rid = provider.getRID(v);
        if (!validBuckets.contains(rid.getBucketId()))
          counts[v] = 0;
      }
    }
  }

  /**
   * Pre-computes the set of valid bucket IDs for a vertex type label.
   *
   * @return bucket ID set, or null if no filtering needed
   */
  public static Set<Integer> buildValidBuckets(final Database db, final String label) {
    if (label == null || !db.getSchema().existsType(label))
      return null;
    final Set<Integer> buckets = new HashSet<>();
    for (final var b : db.getSchema().getType(label).getBuckets(true))
      buckets.add(b.getFileId());
    return buckets;
  }

  /**
   * Walks a multi-hop arm from a single start node, returning all reachable endpoint node IDs.
   * For single-hop arms, returns direct neighbors.
   */
  public static int[] walkArm(final GraphTraversalProvider provider, final int startId,
      final String[] edgeTypes, final Vertex.DIRECTION[] directions) {
    return walkArm(provider, startId, edgeTypes, directions, null);
  }

  /**
   * Walks a multi-hop arm from a single start node with optional intermediate node type filtering.
   * The {@code intermediateLabels} array (if non-null) has one entry per hop, specifying the
   * required type label for nodes reached at that hop. Null entries mean no filtering.
   * <p>
   * This is critical for queries like Q2 where REPLY_OF reaches both Posts and Comments
   * but only Posts should match the intermediate {@code (po:Post)} node pattern.
   */
  public static int[] walkArm(final GraphTraversalProvider provider, final int startId,
      final String[] edgeTypes, final Vertex.DIRECTION[] directions,
      final Set<Integer>[] intermediateValidBuckets) {
    int[] current = new int[]{startId};
    for (int hop = 0; hop < edgeTypes.length; hop++) {
      int totalNext = 0;
      for (final int nid : current)
        totalNext += provider.getNeighborIds(nid, directions[hop], edgeTypes[hop]).length;
      if (totalNext == 0)
        return new int[0];
      final int[] next = new int[totalNext];
      int pos = 0;
      for (final int nid : current) {
        final int[] neighbors = provider.getNeighborIds(nid, directions[hop], edgeTypes[hop]);
        System.arraycopy(neighbors, 0, next, pos, neighbors.length);
        pos += neighbors.length;
      }

      // Apply intermediate node type filter if specified
      if (intermediateValidBuckets != null && intermediateValidBuckets[hop] != null
          && !intermediateValidBuckets[hop].isEmpty()) {
        int writePos = 0;
        for (int i = 0; i < pos; i++) {
          final RID rid = provider.getRID(next[i]);
          if (intermediateValidBuckets[hop].contains(rid.getBucketId()))
            next[writePos++] = next[i];
        }
        current = java.util.Arrays.copyOf(next, writePos);
      } else {
        current = pos < next.length ? java.util.Arrays.copyOf(next, pos) : next;
      }
    }
    return current;
  }

  /**
   * Packs two int node IDs into a single long key for hash map lookups.
   */
  public static long packPair(final int a, final int b) {
    return ((long) a << 32) | (b & 0xFFFFFFFFL);
  }
}
