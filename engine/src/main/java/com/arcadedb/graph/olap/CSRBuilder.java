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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
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
import java.util.Set;
import java.util.logging.Level;

/**
 * Builds per-edge-type CSR adjacency indexes and per-bucket columnar property storage
 * from the OLTP graph in 2 passes.
 * <p>
 * <b>Pass 1</b> (single scan): Iterates all selected buckets and for each vertex:
 * <ul>
 *   <li>Assigns dense IDs per bucket in the {@link NodeIdMapping}</li>
 *   <li>Counts outgoing degrees per edge type</li>
 *   <li>Detects property types and extracts property values into per-bucket {@link ColumnStore}</li>
 * </ul>
 * <b>Pass 2</b> (single scan): Iterates vertices again to fill CSR neighbor arrays
 * using prefix sums computed from Pass 1 degree counts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRBuilder {
  private final Database database;
  private final String[] propertyFilter; // null = all properties, empty = no properties

  public CSRBuilder(final Database database) {
    this(database, null);
  }

  public CSRBuilder(final Database database, final String[] propertyFilter) {
    this.database = database;
    this.propertyFilter = propertyFilter;
  }

  /**
   * Builds per-edge-type CSR indexes and columnar property storage.
   *
   * @param vertexTypes vertex type names to include (null or empty = all vertex types)
   * @param edgeTypes   edge type names to include (null or empty = all edge types)
   *
   * @return the result containing per-edge-type CSRs, node ID mapping, and per-bucket column stores
   */
  public CSRResult build(final String[] vertexTypes, final String[] edgeTypes) {
    return buildClean(vertexTypes, edgeTypes, System.currentTimeMillis());
  }

  private CSRResult buildClean(final String[] vertexTypes, final String[] edgeTypes, final long startTime) {
    final Map<Integer, String> bucketToEdgeType = buildBucketToEdgeTypeMap(edgeTypes);

    // --- Phase A: Collect all vertices, assign to buckets ---
    final NodeIdMapping mapping = new NodeIdMapping(16);
    registerVertexBuckets(mapping, vertexTypes);

    // Quick scan: just collect RID positions per bucket (no property access)
    final Iterator<Record> collectIter = createVertexIterator(vertexTypes);
    while (collectIter.hasNext()) {
      final Record record = collectIter.next();
      final RID rid = record.getIdentity();
      final int bucketIdx = mapping.getBucketIdxForBucketId(rid.getBucketId());
      if (bucketIdx >= 0)
        mapping.addNode(bucketIdx, rid.getPosition());
    }
    mapping.compact();

    final int nodeCount = mapping.size();
    if (nodeCount == 0) {
      final long elapsedMs = System.currentTimeMillis() - startTime;
      LogManager.instance().log(this, Level.INFO, "CSR built: 0 nodes, 0 edges, 0.0 MB, %d ms", elapsedMs);
      return new CSRResult(new HashMap<>(), mapping, createEmptyBucketColumns(mapping));
    }

    // --- Phase B: Single scan — count degrees + extract properties + collect edge list ---
    final Map<String, int[]> outDegrees = new HashMap<>();
    final Map<String, int[]> inDegrees = new HashMap<>();
    final Map<String, Integer> totalEdges = new HashMap<>();
    final boolean extractProps = propertyFilter == null || propertyFilter.length > 0;

    // Per-bucket column stores
    final ColumnStore[] bucketColumns = new ColumnStore[mapping.getNumBuckets()];
    final Map<String, Column.Type> detectedTypes = new HashMap<>();

    // First detect property types (quick scan of first few records per bucket)
    if (extractProps)
      detectPropertyTypes(vertexTypes, detectedTypes);

    // Create per-bucket column stores
    for (int bi = 0; bi < mapping.getNumBuckets(); bi++) {
      bucketColumns[bi] = new ColumnStore(mapping.getBucketSize(bi));
      for (final Map.Entry<String, Column.Type> e : detectedTypes.entrySet())
        bucketColumns[bi].createColumn(e.getKey(), e.getValue());
    }

    // Full scan: degrees + properties
    final Iterator<Record> pass2 = createVertexIterator(vertexTypes);
    while (pass2.hasNext()) {
      final Vertex vertex = (Vertex) pass2.next();
      final RID rid = vertex.getIdentity();
      final int globalId = mapping.getGlobalId(rid);
      if (globalId < 0)
        continue;

      final int bucketIdx = mapping.getBucketIdx(globalId);
      final int localId = mapping.getLocalId(globalId);

      // Extract properties into per-bucket column store
      if (extractProps)
        fillProperties(vertex, bucketColumns[bucketIdx], localId, detectedTypes);

      // Count degrees
      final EdgeLinkedList outList = loadOutEdgeList(vertex);
      if (outList != null) {
        final Iterator<Pair<RID, RID>> entries = outList.entryIterator();
        while (entries.hasNext()) {
          final Pair<RID, RID> entry = entries.next();
          final String edgeTypeName = bucketToEdgeType.get(entry.getFirst().getBucketId());
          if (edgeTypeName == null)
            continue;
          final int targetGlobalId = mapping.getGlobalId(entry.getSecond());
          if (targetGlobalId < 0)
            continue;

          outDegrees.computeIfAbsent(edgeTypeName, k -> new int[nodeCount])[globalId]++;
          inDegrees.computeIfAbsent(edgeTypeName, k -> new int[nodeCount])[targetGlobalId]++;
          totalEdges.merge(edgeTypeName, 1, Integer::sum);
        }
      }
    }

    // --- Phase C: Build CSR arrays per edge type using prefix sums ---
    final Map<String, CSRAdjacencyIndex> csrPerType = new HashMap<>();
    int totalAllEdges = 0;

    for (final Map.Entry<String, int[]> entry : outDegrees.entrySet()) {
      final String edgeTypeName = entry.getKey();
      final int[] outDeg = entry.getValue();
      final int[] inDeg = inDegrees.getOrDefault(edgeTypeName, new int[nodeCount]);
      final int edgeCount = totalEdges.getOrDefault(edgeTypeName, 0);

      // Prefix sums
      final int[] fwdOffsets = new int[nodeCount + 1];
      final int[] bwdOffsets = new int[nodeCount + 1];
      for (int i = 0; i < nodeCount; i++) {
        fwdOffsets[i + 1] = fwdOffsets[i] + outDeg[i];
        bwdOffsets[i + 1] = bwdOffsets[i] + inDeg[i];
      }

      final int[] fwdNeighbors = new int[edgeCount];
      final int[] bwdNeighbors = new int[edgeCount];
      final int[] outCursors = new int[nodeCount];
      final int[] inCursors = new int[nodeCount];

      // Fill neighbor arrays (need another scan for this edge type)
      final Iterator<Record> fillIter = createVertexIterator(vertexTypes);
      while (fillIter.hasNext()) {
        final Vertex vertex = (Vertex) fillIter.next();
        final int srcGlobalId = mapping.getGlobalId(vertex.getIdentity());
        if (srcGlobalId < 0)
          continue;

        final EdgeLinkedList outList = loadOutEdgeList(vertex);
        if (outList != null) {
          final Iterator<Pair<RID, RID>> edgeEntries = outList.entryIterator();
          while (edgeEntries.hasNext()) {
            final Pair<RID, RID> edgeEntry = edgeEntries.next();
            if (!edgeTypeName.equals(bucketToEdgeType.get(edgeEntry.getFirst().getBucketId())))
              continue;
            final int targetGlobalId = mapping.getGlobalId(edgeEntry.getSecond());
            if (targetGlobalId < 0)
              continue;

            fwdNeighbors[fwdOffsets[srcGlobalId] + outCursors[srcGlobalId]++] = targetGlobalId;
            bwdNeighbors[bwdOffsets[targetGlobalId] + inCursors[targetGlobalId]++] = srcGlobalId;
          }
        }
      }

      // Sort neighbor lists for binary search and set intersection
      for (int i = 0; i < nodeCount; i++) {
        final int fs = fwdOffsets[i], fe = fwdOffsets[i + 1];
        if (fe - fs > 1)
          Arrays.sort(fwdNeighbors, fs, fe);
        final int bs = bwdOffsets[i], be = bwdOffsets[i + 1];
        if (be - bs > 1)
          Arrays.sort(bwdNeighbors, bs, be);
      }

      csrPerType.put(edgeTypeName, new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors,
          nodeCount, edgeCount));
      totalAllEdges += edgeCount;
    }

    final long elapsedMs = System.currentTimeMillis() - startTime;
    long totalMemory = 0;
    int totalColumns = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      totalMemory += csr.getMemoryUsageBytes();
    for (final ColumnStore cs : bucketColumns) {
      totalMemory += cs.getMemoryUsageBytes();
      totalColumns += cs.getColumnCount();
    }
    LogManager.instance().log(this, Level.INFO,
        "CSR built: %d nodes (%d buckets), %d edges (%d edge types), %d columns, %.1f MB, %d ms",
        nodeCount, mapping.getNumBuckets(), totalAllEdges, csrPerType.size(), totalColumns,
        totalMemory / (1024.0 * 1024.0), elapsedMs);

    return new CSRResult(csrPerType, mapping, bucketColumns);
  }

  private void registerVertexBuckets(final NodeIdMapping mapping, final String[] vertexTypes) {
    if (vertexTypes == null || vertexTypes.length == 0) {
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof VertexType)
          for (final int bucketId : dt.getBucketIds(false))
            mapping.registerBucket(bucketId, dt.getName(), (int) database.countBucket(
                database.getSchema().getBucketById(bucketId).getName()));
    } else {
      for (final String typeName : vertexTypes)
        for (final int bucketId : database.getSchema().getType(typeName).getBucketIds(false))
          mapping.registerBucket(bucketId, typeName, (int) database.countBucket(
              database.getSchema().getBucketById(bucketId).getName()));
    }
  }

  private void detectPropertyTypes(final String[] vertexTypes, final Map<String, Column.Type> detectedTypes) {
    final Iterator<Record> iter = createVertexIterator(vertexTypes);
    int sampled = 0;
    while (iter.hasNext() && sampled < 1000) {
      final Document doc = (Document) iter.next();
      final Set<String> propNames = doc.getPropertyNames();
      for (final String propName : propNames) {
        if (detectedTypes.containsKey(propName))
          continue;
        if (propertyFilter != null && !containsProperty(propName))
          continue;
        final Object value = doc.get(propName);
        if (value == null)
          continue;
        final Column.Type colType = detectColumnType(value);
        if (colType != null) {
          detectedTypes.put(propName, colType);
          sampled++;
        }
      }
    }
  }

  private boolean containsProperty(final String propName) {
    for (final String p : propertyFilter)
      if (p.equals(propName))
        return true;
    return false;
  }

  private void fillProperties(final Document doc, final ColumnStore store, final int localId,
      final Map<String, Column.Type> detectedTypes) {
    for (final Map.Entry<String, Column.Type> entry : detectedTypes.entrySet()) {
      final String propName = entry.getKey();
      final Object value = doc.get(propName);
      if (value == null)
        continue;

      final Column column = store.getColumn(propName);
      if (column == null)
        continue;

      switch (entry.getValue()) {
      case INT:
        column.setInt(localId, ((Number) value).intValue());
        break;
      case LONG:
        column.setLong(localId, ((Number) value).longValue());
        break;
      case DOUBLE:
        column.setDouble(localId, ((Number) value).doubleValue());
        break;
      case STRING:
        column.setString(localId, value.toString());
        break;
      }
    }
  }

  private ColumnStore[] createEmptyBucketColumns(final NodeIdMapping mapping) {
    final ColumnStore[] result = new ColumnStore[mapping.getNumBuckets()];
    for (int i = 0; i < result.length; i++)
      result[i] = new ColumnStore(0);
    return result;
  }

  private Map<Integer, String> buildBucketToEdgeTypeMap(final String[] edgeTypes) {
    final Map<Integer, String> map = new HashMap<>();
    if (edgeTypes == null || edgeTypes.length == 0) {
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

  static Column.Type detectColumnType(final Object value) {
    if (value instanceof Integer || value instanceof Short || value instanceof Byte)
      return Column.Type.INT;
    if (value instanceof Long)
      return Column.Type.LONG;
    if (value instanceof Double || value instanceof Float)
      return Column.Type.DOUBLE;
    if (value instanceof String)
      return Column.Type.STRING;
    return null;
  }

  /**
   * Result of CSR building.
   */
  public static class CSRResult {
    private final Map<String, CSRAdjacencyIndex> csrPerType;
    private final NodeIdMapping                  mapping;
    private final ColumnStore[]                  bucketColumns;

    public CSRResult(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping mapping,
        final ColumnStore[] bucketColumns) {
      this.csrPerType = csrPerType;
      this.mapping = mapping;
      this.bucketColumns = bucketColumns;
    }

    public Map<String, CSRAdjacencyIndex> getCsrPerType() {
      return csrPerType;
    }

    public NodeIdMapping getMapping() {
      return mapping;
    }

    public ColumnStore[] getBucketColumns() {
      return bucketColumns;
    }
  }
}
