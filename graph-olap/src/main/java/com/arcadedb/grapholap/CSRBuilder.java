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
package com.arcadedb.grapholap;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;

/**
 * Builds a CSR adjacency index from the OLTP graph.
 * <p>
 * The build process is two-pass for memory efficiency:
 * <ol>
 *   <li>Pass 1: Scan all vertices, assign dense IDs, count degrees</li>
 *   <li>Pass 2: Compute prefix sums (offsets), fill neighbor arrays</li>
 * </ol>
 * <p>
 * All arrays use {@code int[]} since Java arrays are capped at ~2.1B entries.
 * After building, neighbor lists for each node are sorted to enable
 * binary search and set intersection (needed for WCOJ in future phases).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRBuilder {
  private final Database database;

  public CSRBuilder(final Database database) {
    this.database = database;
  }

  /**
   * Builds a CSR index for specific vertex and edge types.
   *
   * @param vertexTypes vertex type names to include (null or empty = all vertex types)
   * @param edgeTypes   edge type names to include (null or empty = all edge types)
   *
   * @return the built CSR adjacency index along with the node ID mapping
   */
  public CSRResult build(final String[] vertexTypes, final String[] edgeTypes) {
    final long startTime = System.currentTimeMillis();

    // Resolve edge type bucket IDs for filtering
    final int[] edgeBucketFilter = resolveEdgeBucketFilter(edgeTypes);

    // PASS 1: Collect all vertices and assign dense IDs
    final NodeIdMapping mapping = collectVertices(vertexTypes);
    final int nodeCount = mapping.size();

    if (nodeCount == 0)
      return new CSRResult(
          new CSRAdjacencyIndex(new int[] { 0 }, new int[0], new int[] { 0 }, new int[0], 0, 0),
          mapping);

    // Count degrees in parallel arrays
    final int[] outDegrees = new int[nodeCount];
    final int[] inDegrees = new int[nodeCount];
    int totalEdges = 0;

    // Scan all edges by iterating vertices
    final Iterator<Record> vertexIter = createVertexIterator(vertexTypes);
    while (vertexIter.hasNext()) {
      final Vertex vertex = (Vertex) vertexIter.next();
      final int srcId = mapping.getId(vertex.getIdentity());
      if (srcId < 0)
        continue;

      final EdgeLinkedList outList = loadOutEdgeList(vertex);
      if (outList != null) {
        final Iterator<Pair<RID, RID>> entries = outList.entryIterator();
        while (entries.hasNext()) {
          final Pair<RID, RID> entry = entries.next();
          final RID edgeRID = entry.getFirst();
          final RID targetVertexRID = entry.getSecond();

          if (edgeBucketFilter != null && !containsBucket(edgeBucketFilter, edgeRID.getBucketId()))
            continue;

          final int targetId = mapping.getId(targetVertexRID);
          if (targetId < 0)
            continue;

          outDegrees[srcId]++;
          inDegrees[targetId]++;
          totalEdges++;
        }
      }
    }

    // PASS 2: Build CSR arrays using prefix sums
    final int[] fwdOffsets = new int[nodeCount + 1];
    final int[] bwdOffsets = new int[nodeCount + 1];

    // Compute prefix sums
    for (int i = 0; i < nodeCount; i++) {
      fwdOffsets[i + 1] = fwdOffsets[i] + outDegrees[i];
      bwdOffsets[i + 1] = bwdOffsets[i] + inDegrees[i];
    }

    final int[] fwdNeighbors = new int[totalEdges];
    final int[] bwdNeighbors = new int[totalEdges];

    // Use degree arrays as running insertion cursors (reset them)
    Arrays.fill(outDegrees, 0);
    Arrays.fill(inDegrees, 0);

    // Second scan to fill neighbor arrays
    final Iterator<Record> vertexIter2 = createVertexIterator(vertexTypes);
    while (vertexIter2.hasNext()) {
      final Vertex vertex = (Vertex) vertexIter2.next();
      final int srcId = mapping.getId(vertex.getIdentity());
      if (srcId < 0)
        continue;

      final EdgeLinkedList outList = loadOutEdgeList(vertex);
      if (outList != null) {
        final Iterator<Pair<RID, RID>> entries = outList.entryIterator();
        while (entries.hasNext()) {
          final Pair<RID, RID> entry = entries.next();
          final RID edgeRID = entry.getFirst();
          final RID targetVertexRID = entry.getSecond();

          if (edgeBucketFilter != null && !containsBucket(edgeBucketFilter, edgeRID.getBucketId()))
            continue;

          final int targetId = mapping.getId(targetVertexRID);
          if (targetId < 0)
            continue;

          fwdNeighbors[fwdOffsets[srcId] + outDegrees[srcId]++] = targetId;
          bwdNeighbors[bwdOffsets[targetId] + inDegrees[targetId]++] = srcId;
        }
      }
    }

    // Sort neighbor lists for binary search and set intersection support
    for (int i = 0; i < nodeCount; i++) {
      final int fwdStart = fwdOffsets[i];
      final int fwdEnd = fwdOffsets[i + 1];
      if (fwdEnd - fwdStart > 1)
        Arrays.sort(fwdNeighbors, fwdStart, fwdEnd);

      final int bwdStart = bwdOffsets[i];
      final int bwdEnd = bwdOffsets[i + 1];
      if (bwdEnd - bwdStart > 1)
        Arrays.sort(bwdNeighbors, bwdStart, bwdEnd);
    }

    mapping.compact();

    final CSRAdjacencyIndex index = new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors,
        nodeCount, totalEdges);

    final long elapsedMs = System.currentTimeMillis() - startTime;
    LogManager.instance().log(this, Level.INFO,
        "CSR built: %d nodes, %d edges, %.1f MB, %d ms",
        nodeCount, totalEdges, index.getMemoryUsageBytes() / (1024.0 * 1024.0), elapsedMs);

    return new CSRResult(index, mapping);
  }

  private NodeIdMapping collectVertices(final String[] vertexTypes) {
    final NodeIdMapping mapping = new NodeIdMapping(1024);

    final Iterator<Record> iter = createVertexIterator(vertexTypes);
    while (iter.hasNext()) {
      final Record record = iter.next();
      mapping.addRID(record.getIdentity());
    }

    return mapping;
  }

  private Iterator<Record> createVertexIterator(final String[] vertexTypes) {
    if (vertexTypes == null || vertexTypes.length == 0) {
      // Iterate all vertex types
      final com.arcadedb.utility.MultiIterator<Record> multi = new com.arcadedb.utility.MultiIterator<>();
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof VertexType)
          multi.addIterator(database.iterateType(dt.getName(), false));
      return multi;
    }

    if (vertexTypes.length == 1)
      return database.iterateType(vertexTypes[0], false);

    final com.arcadedb.utility.MultiIterator<Record> multi = new com.arcadedb.utility.MultiIterator<>();
    for (final String typeName : vertexTypes)
      multi.addIterator(database.iterateType(typeName, false));
    return multi;
  }

  private int[] resolveEdgeBucketFilter(final String[] edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0)
      return null;

    final Set<Integer> bucketIds = new HashSet<>();
    for (final String edgeType : edgeTypes)
      bucketIds.addAll(database.getSchema().getType(edgeType).getBucketIds(true));

    final int[] result = new int[bucketIds.size()];
    int i = 0;
    for (final int id : bucketIds)
      result[i++] = id;
    Arrays.sort(result);
    return result;
  }

  private EdgeLinkedList loadOutEdgeList(final Vertex vertex) {
    final VertexInternal vertexInternal = (VertexInternal) vertex;
    final RID outEdgesHead = vertexInternal.getOutEdgesHeadChunk();
    if (outEdgesHead == null)
      return null;

    try {
      return new EdgeLinkedList(vertex, Vertex.DIRECTION.OUT,
          (EdgeSegment) database.lookupByRID(outEdgesHead, true));
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot load OUT edge list chunk (%s) for vertex %s", e, outEdgesHead, vertex.getIdentity());
      return null;
    }
  }

  private static boolean containsBucket(final int[] sortedBucketIds, final int bucketId) {
    return Arrays.binarySearch(sortedBucketIds, bucketId) >= 0;
  }

  /**
   * Result of CSR building: the index plus the node ID mapping.
   */
  public static class CSRResult {
    private final CSRAdjacencyIndex index;
    private final NodeIdMapping     mapping;

    public CSRResult(final CSRAdjacencyIndex index, final NodeIdMapping mapping) {
      this.index = index;
      this.mapping = mapping;
    }

    public CSRAdjacencyIndex getIndex() {
      return index;
    }

    public NodeIdMapping getMapping() {
      return mapping;
    }
  }
}
