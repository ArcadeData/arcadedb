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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

/**
 * Builds per-edge-type CSR adjacency indexes from the OLTP graph.
 * <p>
 * Each edge type gets its own {@link CSRAdjacencyIndex},
 * while all vertex types share a single {@link NodeIdMapping}. This enables:
 * <ul>
 *   <li>Traversal filtered to a specific edge type with zero overhead</li>
 *   <li>Degree counting per edge type</li>
 *   <li>Multi-type traversal by iterating the relevant CSRs</li>
 * </ul>
 * <p>
 * The build process is two-pass per edge type for memory efficiency:
 * <ol>
 *   <li>Pass 1: Scan all vertices, assign dense IDs, count degrees per edge type</li>
 *   <li>Pass 2: Compute prefix sums (offsets), fill neighbor arrays</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRBuilder {
  private final Database database;

  public CSRBuilder(final Database database) {
    this.database = database;
  }

  /**
   * Builds per-edge-type CSR indexes for the specified vertex and edge types.
   *
   * @param vertexTypes vertex type names to include (null or empty = all vertex types)
   * @param edgeTypes   edge type names to include (null or empty = all edge types)
   *
   * @return the result containing per-edge-type CSRs and the shared node ID mapping
   */
  public CSRResult build(final String[] vertexTypes, final String[] edgeTypes) {
    final long startTime = System.currentTimeMillis();

    // Build bucket-to-edge-type-name lookup for classifying edges
    final Map<Integer, String> bucketToEdgeType = buildBucketToEdgeTypeMap(edgeTypes);

    // PASS 1: Collect all vertices and assign dense IDs (with type tracking)
    final NodeIdMapping mapping = collectVertices(vertexTypes);
    final int nodeCount = mapping.size();

    if (nodeCount == 0) {
      final long elapsedMs = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO, "CSR built: 0 nodes, 0 edges, 0.0 MB, %d ms", elapsedMs);
      return new CSRResult(new HashMap<>(), mapping);
    }

    // Count degrees per edge type: edgeTypeName → int[nodeCount] for out and in
    final Map<String, int[]> outDegreesPerType = new HashMap<>();
    final Map<String, int[]> inDegreesPerType = new HashMap<>();
    final Map<String, Integer> totalEdgesPerType = new HashMap<>();

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

          final String edgeTypeName = bucketToEdgeType.get(edgeRID.getBucketId());
          if (edgeTypeName == null)
            continue; // edge type not selected

          final int targetId = mapping.getId(targetVertexRID);
          if (targetId < 0)
            continue;

          outDegreesPerType.computeIfAbsent(edgeTypeName, k -> new int[nodeCount])[srcId]++;
          inDegreesPerType.computeIfAbsent(edgeTypeName, k -> new int[nodeCount])[targetId]++;
          totalEdgesPerType.merge(edgeTypeName, 1, Integer::sum);
        }
      }
    }

    // PASS 2: Build CSR arrays per edge type using prefix sums
    final Map<String, CSRAdjacencyIndex> csrPerType = new HashMap<>();
    int totalAllEdges = 0;

    for (final Map.Entry<String, int[]> entry : outDegreesPerType.entrySet()) {
      final String edgeTypeName = entry.getKey();
      final int[] outDegrees = entry.getValue();
      final int[] inDegrees = inDegreesPerType.getOrDefault(edgeTypeName, new int[nodeCount]);
      final int totalEdges = totalEdgesPerType.getOrDefault(edgeTypeName, 0);

      final int[] fwdOffsets = new int[nodeCount + 1];
      final int[] bwdOffsets = new int[nodeCount + 1];
      for (int i = 0; i < nodeCount; i++) {
        fwdOffsets[i + 1] = fwdOffsets[i] + outDegrees[i];
        bwdOffsets[i + 1] = bwdOffsets[i] + inDegrees[i];
      }

      final int[] fwdNeighbors = new int[totalEdges];
      final int[] bwdNeighbors = new int[totalEdges];

      // Reset degrees as insertion cursors
      final int[] outCursors = new int[nodeCount];
      final int[] inCursors = new int[nodeCount];

      // Second scan to fill neighbor arrays for this edge type
      final Iterator<Record> vertexIter2 = createVertexIterator(vertexTypes);
      while (vertexIter2.hasNext()) {
        final Vertex vertex = (Vertex) vertexIter2.next();
        final int srcId = mapping.getId(vertex.getIdentity());
        if (srcId < 0)
          continue;

        final EdgeLinkedList outList = loadOutEdgeList(vertex);
        if (outList != null) {
          final Iterator<Pair<RID, RID>> edgeEntries = outList.entryIterator();
          while (edgeEntries.hasNext()) {
            final Pair<RID, RID> edgeEntry = edgeEntries.next();
            final RID edgeRID = edgeEntry.getFirst();
            final RID targetVertexRID = edgeEntry.getSecond();

            if (!edgeTypeName.equals(bucketToEdgeType.get(edgeRID.getBucketId())))
              continue;

            final int targetId = mapping.getId(targetVertexRID);
            if (targetId < 0)
              continue;

            fwdNeighbors[fwdOffsets[srcId] + outCursors[srcId]++] = targetId;
            bwdNeighbors[bwdOffsets[targetId] + inCursors[targetId]++] = srcId;
          }
        }
      }

      // Sort neighbor lists for binary search and set intersection
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

      csrPerType.put(edgeTypeName, new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors,
          nodeCount, totalEdges));
      totalAllEdges += totalEdges;
    }

    mapping.compact();

    final long elapsedMs = System.currentTimeMillis() - startTime;
    long totalMemory = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      totalMemory += csr.getMemoryUsageBytes();
    LogManager.instance().log(this, Level.INFO,
        "CSR built: %d nodes, %d edges (%d edge types), %.1f MB, %d ms",
        nodeCount, totalAllEdges, csrPerType.size(), totalMemory / (1024.0 * 1024.0), elapsedMs);

    return new CSRResult(csrPerType, mapping);
  }

  private NodeIdMapping collectVertices(final String[] vertexTypes) {
    final NodeIdMapping mapping = new NodeIdMapping(1024);

    final Iterator<Record> iter = createVertexIterator(vertexTypes);
    while (iter.hasNext()) {
      final Record record = iter.next();
      final String typeName = database.getSchema().getTypeByBucketId(record.getIdentity().getBucketId()).getName();
      mapping.addRID(record.getIdentity(), typeName);
    }

    return mapping;
  }

  private Map<Integer, String> buildBucketToEdgeTypeMap(final String[] edgeTypes) {
    final Map<Integer, String> map = new HashMap<>();

    if (edgeTypes == null || edgeTypes.length == 0) {
      // All edge types
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof com.arcadedb.schema.EdgeType)
          for (final int bucketId : dt.getBucketIds(true))
            map.put(bucketId, dt.getName());
    } else {
      for (final String edgeType : edgeTypes)
        for (final int bucketId : database.getSchema().getType(edgeType).getBucketIds(true))
          map.put(bucketId, edgeType);
    }

    return map;
  }

  private Iterator<Record> createVertexIterator(final String[] vertexTypes) {
    if (vertexTypes == null || vertexTypes.length == 0) {
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

  /**
   * Result of CSR building: per-edge-type CSR indexes plus the shared node ID mapping.
   */
  public static class CSRResult {
    private final Map<String, CSRAdjacencyIndex> csrPerType;
    private final NodeIdMapping                  mapping;

    public CSRResult(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping mapping) {
      this.csrPerType = csrPerType;
      this.mapping = mapping;
    }

    public Map<String, CSRAdjacencyIndex> getCsrPerType() {
      return csrPerType;
    }

    public NodeIdMapping getMapping() {
      return mapping;
    }
  }
}
