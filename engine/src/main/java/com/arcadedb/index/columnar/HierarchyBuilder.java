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
package com.arcadedb.index.columnar;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.EdgeEntryRIDIterator;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.facet.FacetValueComparator;
import com.arcadedb.utility.RidLongHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;

/**
 * Builds a {@link HierarchyView} from the tag graph a {@link HierarchyDefinition} names, through the database API (so the caller's
 * transaction and security apply: a user without READ on the tag buckets is refused by the scan).
 * <p>
 * Two scans of the vertex type, polymorphic. The first assigns a node to every distinct key (vertices sharing a key share the node,
 * counted as duplicates; vertices with a null key are skipped and counted) and maps each vertex RID to its node. The second reads the
 * parent RIDs of each vertex from its edge list in {@code parentDirection}, without loading the parent vertices; a parent that is not
 * a vertex of the type, and the entry of an edge whose record is gone (a ghost), are ignored. The parent lists are deduplicated and
 * sorted by value ({@link FacetValueComparator}), so the first parent is the same on every rebuild whatever order the edge list
 * iterates in. Then the depth of every node by a breadth-first walk from the roots, and the nodes on cycles by an iterative Tarjan
 * strongly connected components walk (exact where counting the open nodes of each back edge of a plain depth-first walk is not).
 * <p>
 * Cost: O(V + E log E) time, heap proportional to the number of tags and parent links.
 */
public final class HierarchyBuilder {
  private static final int INITIAL_CAPACITY = 64;

  private HierarchyBuilder() {
  }

  /** @param indexName the COLUMNAR index the hierarchy belongs to, named by the cycle WARNING */
  static HierarchyView build(final DatabaseInternal db, final HierarchyDefinition def, final long generation, final String indexName) {
    // PASS 1: A NODE PER DISTINCT KEY, VERTEX RID TO NODE
    final RidLongHashMap ridToNode = new RidLongHashMap();
    final HashMap<Object, Integer> keyToNode = new HashMap<>();
    Object[] values = new Object[INITIAL_CAPACITY];
    int nodeCount = 0;
    int duplicateKeys = 0;
    int nullKeys = 0;

    final Iterator<Record> vertices = db.iterateType(def.vertexType(), true);
    while (vertices.hasNext()) {
      final Vertex vertex = vertices.next().asVertex();
      final Object key = FacetValueComparator.normalize(vertex.get(def.key()));
      if (key == null) {
        nullKeys++;
        continue;
      }
      Integer node = keyToNode.get(key);
      if (node == null) {
        node = nodeCount;
        keyToNode.put(key, node);
        if (nodeCount == values.length)
          values = Arrays.copyOf(values, nodeCount * 2);
        values[nodeCount++] = key;
      } else
        duplicateKeys++;
      ridToNode.put(vertex.getIdentity(), node);
    }
    values = Arrays.copyOf(values, nodeCount);

    // rank of every node by value: the parent lists are sorted by it
    final int[] nodeOfRank = new int[nodeCount];
    for (int n = 0; n < nodeCount; n++)
      nodeOfRank[n] = n;
    sortByValue(nodeOfRank, new int[nodeCount], 0, nodeCount, values);
    final int[] rank = new int[nodeCount];
    for (int r = 0; r < nodeCount; r++)
      rank[nodeOfRank[r]] = r;

    // PASS 2: PARENT LINKS AS (CHILD << 32 | RANK OF PARENT), SO ONE SORT GROUPS BY CHILD AND ORDERS EACH GROUP BY VALUE
    final GraphEngine graph = db.getGraphEngine();
    long[] links = new long[INITIAL_CAPACITY];
    int linkCount = 0;
    final Iterator<Record> children = db.iterateType(def.vertexType(), true);
    while (children.hasNext()) {
      final Vertex vertex = children.next().asVertex();
      // -1: a null key, or a vertex created after the first pass
      final int child = (int) ridToNode.get(vertex.getIdentity(), -1);
      if (child < 0)
        continue;
      final EdgeEntryRIDIterator entries = graph.getConnectedVertexEntries((VertexInternal) vertex, def.parentDirection(),
          def.parentEdge());
      while (entries.hasNext()) {
        final RID parentRid = entries.next();
        final int parent = (int) ridToNode.get(parentRid, -1);
        // the membership test first: it turns away the parents outside the type before the ghost probe is paid
        if (parent < 0 || !GhostEdgeReporter.isLiveEntry(db, entries.getEdgeRID()))
          continue;
        if (linkCount == links.length)
          links = Arrays.copyOf(links, linkCount * 2);
        links[linkCount++] = ((long) child << 32) | rank[parent];
      }
    }
    Arrays.sort(links, 0, linkCount);

    // CSR, DUPLICATES (TWO EDGES, OR TWO VERTICES OF ONE KEY LINKED TO ONE PARENT) DROPPED
    final int[] parentOffsets = new int[nodeCount + 1];
    int[] parents = new int[linkCount];
    int parentCount = 0;
    for (int i = 0; i < linkCount; i++) {
      if (i > 0 && links[i] == links[i - 1])
        continue;
      parentOffsets[(int) (links[i] >>> 32) + 1]++;
      parents[parentCount++] = nodeOfRank[(int) links[i]];
    }
    for (int n = 0; n < nodeCount; n++)
      parentOffsets[n + 1] += parentOffsets[n];
    if (parentCount < parents.length)
      parents = Arrays.copyOf(parents, parentCount);

    final int[] depth = depths(nodeCount, parentOffsets, parents);
    final int[] cycleSample = new int[] { -1 };
    final int cycleNodes = cycleNodes(nodeCount, parentOffsets, parents, cycleSample);
    if (cycleNodes > 0)
      LogManager.instance().log(HierarchyBuilder.class, Level.WARNING,
          "Hierarchy of index '%s' (%s.%s over %s %s, database '%s') has %d node(s) on a cycle of parent links, for example '%s':"
              + " they are counted once per document and have depth -1 when no root reaches them", indexName, def.vertexType(),
          def.key(), def.parentEdge(), def.parentDirection(), db.getName(), cycleNodes, values[cycleSample[0]]);

    // no orphan node in a view built from the graph: every node has a vertex
    return new HierarchyView(generation, parentOffsets, parents, values, depth, null, keyToNode, cycleNodes, duplicateKeys, nullKeys);
  }

  /** Breadth-first from the nodes without parents over the child links: the shortest distance from a root, -1 when none reaches. */
  private static int[] depths(final int nodeCount, final int[] parentOffsets, final int[] parents) {
    // child CSR, the transpose of the parent CSR
    final int[] childOffsets = new int[nodeCount + 1];
    for (final int parent : parents)
      childOffsets[parent + 1]++;
    for (int n = 0; n < nodeCount; n++)
      childOffsets[n + 1] += childOffsets[n];
    final int[] fill = Arrays.copyOf(childOffsets, nodeCount);
    final int[] childList = new int[parents.length];
    for (int child = 0; child < nodeCount; child++)
      for (int p = parentOffsets[child]; p < parentOffsets[child + 1]; p++)
        childList[fill[parents[p]]++] = child;

    final int[] depth = new int[nodeCount];
    Arrays.fill(depth, -1);
    final int[] queue = new int[nodeCount];
    int tail = 0;
    for (int n = 0; n < nodeCount; n++)
      if (parentOffsets[n] == parentOffsets[n + 1]) {
        depth[n] = 0;
        queue[tail++] = n;
      }
    for (int head = 0; head < tail; head++) {
      final int node = queue[head];
      for (int c = childOffsets[node]; c < childOffsets[node + 1]; c++) {
        final int child = childList[c];
        if (depth[child] < 0) {
          depth[child] = depth[node] + 1;
          queue[tail++] = child;
        }
      }
    }
    return depth;
  }

  /**
   * Counts the nodes on a cycle: the members of every strongly connected component of more than one node, plus the nodes that are
   * their own parent. Iterative Tarjan, so a deep chain cannot overflow the thread stack.
   *
   * @param sample receives one node on a cycle, when there is one
   */
  private static int cycleNodes(final int nodeCount, final int[] parentOffsets, final int[] parents, final int[] sample) {
    final int[] index = new int[nodeCount];
    Arrays.fill(index, -1);
    final int[] low = new int[nodeCount];
    final boolean[] onStack = new boolean[nodeCount];
    final int[] stack = new int[nodeCount];
    final int[] callNode = new int[nodeCount];
    final int[] callEdge = new int[nodeCount];
    int stackSize = 0;
    int nextIndex = 0;
    int count = 0;

    for (int start = 0; start < nodeCount; start++) {
      if (index[start] >= 0)
        continue;
      index[start] = low[start] = nextIndex++;
      stack[stackSize++] = start;
      onStack[start] = true;
      callNode[0] = start;
      callEdge[0] = parentOffsets[start];
      int calls = 1;

      while (calls > 0) {
        final int node = callNode[calls - 1];
        if (callEdge[calls - 1] < parentOffsets[node + 1]) {
          final int next = parents[callEdge[calls - 1]++];
          if (index[next] < 0) {
            index[next] = low[next] = nextIndex++;
            stack[stackSize++] = next;
            onStack[next] = true;
            callNode[calls] = next;
            callEdge[calls] = parentOffsets[next];
            calls++;
          } else if (onStack[next])
            low[node] = Math.min(low[node], index[next]);
          continue;
        }

        calls--;
        if (calls > 0) {
          final int caller = callNode[calls - 1];
          low[caller] = Math.min(low[caller], low[node]);
        }
        if (low[node] == index[node]) {
          int size = 0;
          int member;
          do {
            member = stack[--stackSize];
            onStack[member] = false;
            size++;
          } while (member != node);
          if (size > 1 || isOwnParent(node, parentOffsets, parents)) {
            count += size;
            if (sample[0] < 0)
              sample[0] = node;
          }
        }
      }
    }
    return count;
  }

  /** Merge sort of node ids by their value ({@link FacetValueComparator} over normalized values), stable, no boxing. */
  private static void sortByValue(final int[] nodes, final int[] temp, final int from, final int to, final Object[] values) {
    if (to - from < 2)
      return;
    final int mid = (from + to) >>> 1;
    sortByValue(nodes, temp, from, mid, values);
    sortByValue(nodes, temp, mid, to, values);
    int i = from;
    int j = mid;
    int k = from;
    while (i < mid && j < to)
      temp[k++] = FacetValueComparator.INSTANCE.compareNormalized(values[nodes[i]], values[nodes[j]]) <= 0 ? nodes[i++] : nodes[j++];
    while (i < mid)
      temp[k++] = nodes[i++];
    while (j < to)
      temp[k++] = nodes[j++];
    System.arraycopy(temp, from, nodes, from, to - from);
  }

  private static boolean isOwnParent(final int node, final int[] parentOffsets, final int[] parents) {
    for (int p = parentOffsets[node]; p < parentOffsets[node + 1]; p++)
      if (parents[p] == node)
        return true;
    return false;
  }
}
