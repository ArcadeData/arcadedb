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
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.utility.IntHashSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
   * <p>
   * A {@code null} set is "no filter" and leaves every count alone; an <b>empty</b> one is "matches nothing" and
   * zeroes all of them. See {@link #buildValidBuckets}.
   */
  public static void filterByBuckets(final GraphTraversalProvider provider, final long[] counts,
      final IntHashSet validBuckets) {
    if (validBuckets == null)
      return;
    if (validBuckets.isEmpty()) {
      Arrays.fill(counts, 0L);
      return;
    }
    for (int v = 0; v < counts.length; v++) {
      if (counts[v] > 0) {
        final RID rid = provider.getRID(v);
        if (!validBuckets.contains(rid.getBucketId()))
          counts[v] = 0;
      }
    }
  }

  /**
   * Zeros out entries using pre-computed bucket IDs (avoids per-node getRID calls).
   * <p>
   * {@code bucketIds} is read only when the filter can keep something, so a caller with no label on any position may
   * pass {@code null} for it.
   */
  public static void filterByBuckets(final int[] bucketIds, final long[] counts,
      final IntHashSet validBuckets) {
    if (validBuckets == null)
      return;
    if (validBuckets.isEmpty()) {
      Arrays.fill(counts, 0L);
      return;
    }
    for (int v = 0; v < counts.length; v++) {
      if (counts[v] > 0 && !validBuckets.contains(bucketIds[v]))
        counts[v] = 0;
    }
  }

  /**
   * Pre-computes the set of bucket IDs a vertex type label's records live in.
   * <p>
   * <b>The two "nothing to filter on" answers are not the same answer</b>, and issue #5757 is that they used to
   * share one {@code null}:
   * <ul>
   *   <li>{@code null} - <b>no label was given</b>, so the position accepts every vertex and nothing is filtered;</li>
   *   <li>an <b>empty</b> set - a label was given and no record can carry it, so the position accepts nothing.</li>
   * </ul>
   * A caller that reads the empty set as "no filter" counts an unfiltered pattern, and one that reads the {@code
   * null} as "no vertices" answers 0 for a pattern that does match. Both were live wrong answers in issue #5715, from
   * the two ends of this one overload. Every consumer here now branches on {@code null} alone and lets an empty set
   * fall through to the membership test, which keeps nothing.
   *
   * @return null when no label was given, otherwise the label's bucket ids - empty when it is not a declared type
   */
  public static IntHashSet buildValidBuckets(final Database db, final String label) {
    if (label == null)
      return null;
    final IntHashSet buckets = new IntHashSet();
    if (!db.getSchema().existsType(label))
      return buckets;
    for (final var b : db.getSchema().getType(label).getBuckets(true))
      buckets.add(b.getFileId());
    return buckets;
  }

  /**
   * The provider the OLTP paths may use to expand one vertex's neighbors, or null when none may be used.
   * <p>
   * These paths walk the vertices themselves and consult a provider per vertex, falling back to the edge list for
   * any vertex the provider does not map. That fallback covers a vertex <b>outside</b> the provider's node domain,
   * and nothing covers a vertex inside it whose neighbors are outside: {@code getNeighborIds} answers with the
   * adjacency the view holds, and the edges leading out of the view are simply not in it. A view built over a
   * subset of the vertex types is therefore not usable as a per-vertex accelerator at all, however well it covers
   * the edge types, and the count has to come off the edge lists (issue #5757).
   */
  public static GraphTraversalProvider findAcceleratingProvider(final Database db, final String... edgeTypes) {
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, edgeTypes);
    return provider != null && provider.coversVertexType(null) ? provider : null;
  }

  /**
   * The vertices an anchor position starts from: the label's own when one was given, <b>every vertex in the
   * schema</b> when none was (issue #5757).
   * <p>
   * An unlabelled anchor is what {@code MATCH ()-[:TYPE]->() RETURN count(*)} has, and there is no cheaper question
   * to ask a graph. It used to leave every operator with no set to enumerate; "every vertex" is the set it means.
   * Both walks are the one in {@code Labels.iterateMatchingVertices}, which visits no vertex twice and which the
   * MATCH anchor and the start of a pattern comprehension scan with as well (issue #6352). Sharing it also means a
   * label that names an edge or document type answers with no anchors rather than with records that are not
   * vertices: labels and relationship types are separate namespaces, the rule the rest of the engine already
   * applies.
   *
   * @return an iterator over the anchor vertices, empty when no vertex type carries the label
   */
  public static Iterator<? extends Identifiable> iterateAnchors(final Database db, final String label) {
    return Labels.iterateMatchingVertices(db, label != null ? List.of(label) : null, false);
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
      final IntHashSet[] intermediateValidBuckets) {
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

      // Apply intermediate node type filter if specified. An empty set is a label that matches nothing, so it drops
      // the whole frontier rather than being read as "no filter" (issue #5757).
      if (intermediateValidBuckets != null && intermediateValidBuckets[hop] != null) {
        int writePos = 0;
        for (int i = 0; i < pos; i++) {
          final RID rid = provider.getRID(next[i]);
          if (intermediateValidBuckets[hop].contains(rid.getBucketId()))
            next[writePos++] = next[i];
        }
        current = Arrays.copyOf(next, writePos);
      } else {
        current = pos < next.length ? Arrays.copyOf(next, pos) : next;
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
