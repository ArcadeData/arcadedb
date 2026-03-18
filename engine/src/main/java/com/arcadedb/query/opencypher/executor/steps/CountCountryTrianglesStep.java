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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Arrays;
import java.util.List;

/**
 * Optimized execution step for counting triangles in a graph partitioned by a grouping attribute.
 * <p>
 * Handles Q3-like patterns:
 * <pre>
 *   MATCH (co:Country)
 *   MATCH (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co)
 *   MATCH (p2:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co)
 *   MATCH (p3:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co)
 *   MATCH (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1)
 *   RETURN count(*) AS count
 * </pre>
 * <p>
 * Algorithm:
 * <ol>
 *   <li>Build partition mapping: person → country via IS_LOCATED_IN → IS_PART_OF chain</li>
 *   <li>Get sorted undirected KNOWS adjacency via CSR NeighborView(BOTH)</li>
 *   <li>For each person u, for each KNOWS neighbor v in same country,
 *       count common KNOWS neighbors w also in same country using sorted intersection</li>
 *   <li>Sum = ordered triple count (each triangle counted 6 times = 6 ordered triples)</li>
 * </ol>
 * <p>
 * Complexity: O(sum_edges |N(u) ∩ N(v)|) with CSR sorted arrays.
 * For LSQB SF1 (~10K persons, ~200K KNOWS edges): completes in &lt;1 second.
 */
public final class CountCountryTrianglesStep extends AbstractExecutionStep {
  // Chain from person to partition node: edgeTypes[0] = IS_LOCATED_IN, edgeTypes[1] = IS_PART_OF
  private final String[] partitionEdgeTypes;
  private final Vertex.DIRECTION[] partitionDirections;
  // The triangle edge type (KNOWS)
  private final String triangleEdgeType;
  private final String countAlias;

  public CountCountryTrianglesStep(final String[] partitionEdgeTypes,
      final Vertex.DIRECTION[] partitionDirections,
      final String triangleEdgeType,
      final String countAlias,
      final CommandContext context) {
    super(context);
    this.partitionEdgeTypes = partitionEdgeTypes;
    this.partitionDirections = partitionDirections;
    this.triangleEdgeType = triangleEdgeType;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database db = context.getDatabase();

      // Need all edge types: partition chain + triangle type
      final String[] allTypes = new String[partitionEdgeTypes.length + 1];
      System.arraycopy(partitionEdgeTypes, 0, allTypes, 0, partitionEdgeTypes.length);
      allTypes[partitionEdgeTypes.length] = triangleEdgeType;

      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, allTypes);

      final long count;
      if (provider != null)
        count = countTrianglesWithCSR(provider);
      else
        count = countTrianglesOLTP(db);

      if (context.isProfiling()) {
        cost = System.nanoTime() - begin;
        rowCount = 1;
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, true);
      }

      final ResultInternal result = new ResultInternal();
      result.setProperty(countAlias, count);
      return new IteratorResultSet(List.of((Result) result).iterator());
    } finally {
      if (context.isProfiling() && cost < 0)
        cost = System.nanoTime() - begin;
    }
  }

  private long countTrianglesWithCSR(final GraphTraversalProvider provider) {
    final int nodeCount = provider.getNodeCount();

    // Step 1: Build person → partition (country) mapping via the chain.
    // personPartition[nodeId] = partition node ID, or -1 if not partitioned.
    final int[] personPartition = buildPartitionMapping(provider, nodeCount);

    // Step 2: Get sorted undirected KNOWS adjacency
    final NeighborView knowsView = provider.getNeighborView(Vertex.DIRECTION.BOTH, triangleEdgeType);
    if (knowsView == null) {
      // Fallback: use getNeighborIds per node (slower but correct)
      return countTrianglesPerNode(provider, personPartition, nodeCount);
    }

    final int[] nbrs = knowsView.neighbors();

    // Step 3: Country-restricted triangle counting via sorted intersection.
    // For each person u, for each KNOWS neighbor v in same country,
    // count |N(u) ∩ N(v) ∩ sameCountry|.
    // Each triangle (A,B,C) contributes 6 to the total (= 6 ordered triples).
    // Use multi-threaded processing for large graphs.
    final int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    final long[] partialCounts = new long[threadCount];

    if (nodeCount < 1000) {
      // Small graph: single-threaded
      partialCounts[0] = countRange(knowsView, nbrs, personPartition, 0, nodeCount);
    } else {
      // Large graph: parallel
      final Thread[] threads = new Thread[threadCount];
      final int chunkSize = (nodeCount + threadCount - 1) / threadCount;
      for (int t = 0; t < threadCount; t++) {
        final int start = t * chunkSize;
        final int end = Math.min(start + chunkSize, nodeCount);
        final int threadIdx = t;
        threads[t] = new Thread(() ->
            partialCounts[threadIdx] = countRange(knowsView, nbrs, personPartition, start, end));
        threads[t].start();
      }
      for (final Thread thread : threads) {
        try {
          thread.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    long total = 0;
    for (final long pc : partialCounts)
      total += pc;
    return total;
  }

  /**
   * Counts country-restricted ordered triples for persons in [start, end).
   */
  private static long countRange(final NeighborView knowsView, final int[] nbrs,
      final int[] personPartition, final int start, final int end) {
    long count = 0;
    for (int u = start; u < end; u++) {
      final int country = personPartition[u];
      if (country < 0)
        continue; // not a partitioned person

      final int uStart = knowsView.offset(u);
      final int uEnd = knowsView.offsetEnd(u);

      for (int k = uStart; k < uEnd; k++) {
        final int v = nbrs[k];
        if (personPartition[v] != country)
          continue; // different country

        // Sorted intersection of N(u) and N(v), counting only same-country members
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

  /**
   * Builds the partition mapping by propagating through the partition chain.
   * E.g., Person -[IS_LOCATED_IN]-> City -[IS_PART_OF]-> Country.
   */
  private int[] buildPartitionMapping(final GraphTraversalProvider provider, final int nodeCount) {
    final int[] partition = new int[nodeCount];
    Arrays.fill(partition, -1);

    // Walk the partition chain for each node
    // Hop 0: get neighbors via partitionEdgeTypes[0] / partitionDirections[0]
    // Hop 1: from those, get neighbors via partitionEdgeTypes[1] / partitionDirections[1]
    // etc. Last hop's neighbor = partition ID.
    final int chainLength = partitionEdgeTypes.length;
    final NeighborView[] views = new NeighborView[chainLength];
    for (int h = 0; h < chainLength; h++)
      views[h] = provider.getNeighborView(partitionDirections[h], partitionEdgeTypes[h]);

    // Check all views are available
    for (final NeighborView v : views)
      if (v == null)
        return partition; // Can't build mapping without CSR views

    // For each node, walk the chain
    final NeighborView firstView = views[0];
    final int[] firstNbrs = firstView.neighbors();

    for (int p = 0; p < nodeCount; p++) {
      final int fStart = firstView.offset(p);
      final int fEnd = firstView.offsetEnd(p);
      if (fStart == fEnd)
        continue; // no first-hop edge → not a candidate

      // Follow the chain from the first neighbor
      int current = firstNbrs[fStart]; // take first neighbor
      boolean valid = true;
      for (int h = 1; h < chainLength; h++) {
        final int hStart = views[h].offset(current);
        final int hEnd = views[h].offsetEnd(current);
        if (hStart == hEnd) {
          valid = false;
          break;
        }
        current = views[h].neighbors()[hStart]; // take first neighbor at each hop
      }
      if (valid)
        partition[p] = current;
    }
    return partition;
  }

  /**
   * Fallback triangle counting when NeighborView is unavailable.
   */
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
        // Intersection (both arrays are sorted)
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

  /**
   * OLTP fallback for triangle counting without GAV/CSR.
   * Iterates vertices using the standard graph API.
   */
  private long countTrianglesOLTP(final Database db) {
    // Build person → partition mapping using OLTP
    final java.util.HashMap<com.arcadedb.database.RID, com.arcadedb.database.RID> personToPartition = new java.util.HashMap<>();

    // Find all "person" type vertices: those that have the first partition edge type
    final String firstEdgeType = partitionEdgeTypes[0];
    final Vertex.DIRECTION firstDir = partitionDirections[0];

    for (final com.arcadedb.schema.DocumentType dt : db.getSchema().getTypes()) {
      if (!(dt instanceof com.arcadedb.schema.VertexType))
        continue;
      for (final java.util.Iterator<? extends com.arcadedb.database.Identifiable> it = db.iterateType(dt.getName(), false); it.hasNext(); ) {
        final Vertex v = it.next().asVertex();
        // Walk the partition chain
        com.arcadedb.database.RID current = null;
        Vertex cursor = v;
        boolean valid = true;
        for (int h = 0; h < partitionEdgeTypes.length; h++) {
          final java.util.Iterator<Vertex> neighbors = cursor.getVertices(partitionDirections[h], partitionEdgeTypes[h]).iterator();
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

    // Triangle counting via OLTP
    long total = 0;
    for (final java.util.Map.Entry<com.arcadedb.database.RID, com.arcadedb.database.RID> entry : personToPartition.entrySet()) {
      final Vertex u = (Vertex) db.lookupByRID(entry.getKey(), true);
      final com.arcadedb.database.RID uCountry = entry.getValue();

      for (final java.util.Iterator<Vertex> vIt = u.getVertices(Vertex.DIRECTION.BOTH, triangleEdgeType).iterator(); vIt.hasNext(); ) {
        final Vertex vVertex = vIt.next();
        final com.arcadedb.database.RID vCountry = personToPartition.get(vVertex.getIdentity());
        if (vCountry == null || !vCountry.equals(uCountry))
          continue;

        // Check for common neighbors in the same country
        final java.util.Set<com.arcadedb.database.RID> uNeighborSet = new java.util.HashSet<>();
        for (final java.util.Iterator<Vertex> nIt = u.getVertices(Vertex.DIRECTION.BOTH, triangleEdgeType).iterator(); nIt.hasNext(); ) {
          final Vertex n = nIt.next();
          final com.arcadedb.database.RID nCountry = personToPartition.get(n.getIdentity());
          if (nCountry != null && nCountry.equals(uCountry))
            uNeighborSet.add(n.getIdentity());
        }
        for (final java.util.Iterator<Vertex> wIt = vVertex.getVertices(Vertex.DIRECTION.BOTH, triangleEdgeType).iterator(); wIt.hasNext(); ) {
          final Vertex w = wIt.next();
          if (uNeighborSet.contains(w.getIdentity()))
            total++;
        }
      }
    }
    return total;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    sb.append(ind).append("+ COUNT TRIANGLES (CSR sorted intersection, country-partitioned)\n");
    sb.append(ind).append("  triangle edge: ").append(triangleEdgeType);
    sb.append(", partition chain: ");
    for (int i = 0; i < partitionEdgeTypes.length; i++) {
      if (i > 0) sb.append(" → ");
      sb.append(partitionEdgeTypes[i]);
    }
    if (context.isProfiling())
      sb.append("\n").append(ind).append("  (").append(getCostFormatted()).append(")");
    return sb.toString();
  }
}
