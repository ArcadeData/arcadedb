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
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
  static final int DEFAULT_PROPERTY_SAMPLE_SIZE = 100;

  private final Database database;
  private final Set<String> propertyFilterSet; // null = all properties, empty = no properties
  private final Set<String> edgePropertyFilterSet; // null = no edge properties (default)
  private final int propertySampleSize;

  public CSRBuilder(final Database database) {
    this(database, null, null, DEFAULT_PROPERTY_SAMPLE_SIZE);
  }

  public CSRBuilder(final Database database, final String[] propertyFilter) {
    this(database, propertyFilter, null, DEFAULT_PROPERTY_SAMPLE_SIZE);
  }

  public CSRBuilder(final Database database, final String[] propertyFilter, final int propertySampleSize) {
    this(database, propertyFilter, null, propertySampleSize);
  }

  public CSRBuilder(final Database database, final String[] propertyFilter, final String[] edgePropertyFilter,
      final int propertySampleSize) {
    this.database = database;
    this.propertyFilterSet = propertyFilter != null ? new HashSet<>(Arrays.asList(propertyFilter)) : null;
    this.edgePropertyFilterSet = edgePropertyFilter != null && edgePropertyFilter.length > 0
        ? new HashSet<>(Arrays.asList(edgePropertyFilter)) : null;
    this.propertySampleSize = propertySampleSize;
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
    final boolean extractProps = propertyFilterSet == null || !propertyFilterSet.isEmpty();

    // --- Phase A: Collect RID positions for node ID mapping ---
    final NodeIdMapping mapping = new NodeIdMapping(16);
    registerVertexBuckets(mapping, vertexTypes);

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

    // Detect property types from schema (instant, no scanning needed).
    // Falls back to runtime sampling for schemaless properties.
    final Map<String, Column.Type> detectedTypes = extractProps ? detectPropertyTypesFromSchema(vertexTypes) : new HashMap<>();
    final boolean schemaComplete = !detectedTypes.isEmpty();

    // Create per-bucket column stores upfront (columns added as types are discovered)
    final ColumnStore[] bucketColumns = new ColumnStore[mapping.getNumBuckets()];
    for (int bi = 0; bi < mapping.getNumBuckets(); bi++) {
      bucketColumns[bi] = new ColumnStore(mapping.getBucketSize(bi));
      for (final Map.Entry<String, Column.Type> e : detectedTypes.entrySet())
        bucketColumns[bi].createColumn(e.getKey(), e.getValue());
    }

    // Detect edge property types from schema before Phase B (so we can extract inline)
    final Map<String, Column.Type> edgePropTypes = edgePropertyFilterSet != null
        ? detectEdgePropertyTypesFromSchema(edgeTypes) : null;
    final boolean inlineEdgeProps = edgePropTypes != null && !edgePropTypes.isEmpty();

    // --- Phase B: Single scan — degrees + edge pairs + inline edge property extraction ---
    // When edge properties are configured, edge documents are loaded during this scan
    // (when pages are warm from the linked list traversal) and property values are stored
    // in insertion-order growing arrays. This avoids a separate 34M lookupByRID pass later.
    final Map<String, int[]> outDegrees = new HashMap<>();
    final Map<String, int[]> inDegrees = new HashMap<>();
    final Map<String, IntPairList> edgePairs = new HashMap<>();
    // Per edge type, per property: growing arrays in insertion order. Remapped to CSR order in Phase C.
    final Map<String, Map<String, GrowingPropertyArray>> insertionOrderEdgeProps = inlineEdgeProps ? new HashMap<>() : null;
    int propertySampleCount = 0;
    boolean newTypesInLastSample = false;

    final Iterator<Record> mainIter = createVertexIterator(vertexTypes);
    while (mainIter.hasNext()) {
      final Vertex vertex = (Vertex) mainIter.next();
      final RID rid = vertex.getIdentity();
      final int globalId = mapping.getGlobalId(rid);
      if (globalId < 0)
        continue;

      // For schemaless properties: detect types from sampled records, creating columns lazily
      if (extractProps && !schemaComplete && propertySampleCount < propertySampleSize) {
        for (final String propName : vertex.getPropertyNames()) {
          if (detectedTypes.containsKey(propName))
            continue;
          if (propertyFilterSet != null && !propertyFilterSet.contains(propName))
            continue;
          final Object value = vertex.get(propName);
          if (value == null)
            continue;
          final Column.Type colType = detectColumnType(value);
          if (colType != null) {
            detectedTypes.put(propName, colType);
            for (final ColumnStore cs : bucketColumns)
              cs.createColumn(propName, colType);
            newTypesInLastSample = true;
          }
        }
        propertySampleCount++;
      }

      // Extract properties into per-bucket column store
      if (extractProps && !detectedTypes.isEmpty()) {
        final long packed = mapping.getBucketIdxAndLocalId(globalId);
        fillProperties(vertex, bucketColumns[NodeIdMapping.unpackBucketIdx(packed)],
            NodeIdMapping.unpackLocalId(packed), detectedTypes);
      }

      // Count degrees and collect edge pairs
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
          edgePairs.computeIfAbsent(edgeTypeName, k -> new IntPairList()).add(globalId, targetGlobalId);

          // Extract edge properties inline — page is warm from linked list traversal
          if (insertionOrderEdgeProps != null) {
            final Map<String, GrowingPropertyArray> propArrays = insertionOrderEdgeProps.computeIfAbsent(
                edgeTypeName, k -> {
                  final Map<String, GrowingPropertyArray> m = new HashMap<>();
                  for (final Map.Entry<String, Column.Type> pt : edgePropTypes.entrySet())
                    m.put(pt.getKey(), new GrowingPropertyArray(pt.getValue()));
                  return m;
                });
            final RID edgeRid = entry.getFirst();
            if (edgeRid.getPosition() >= 0) {
              try {
                final Document edgeDoc = (Document) database.lookupByRID(edgeRid, true);
                for (final Map.Entry<String, Column.Type> pt : edgePropTypes.entrySet()) {
                  final Object value = edgeDoc.get(pt.getKey());
                  propArrays.get(pt.getKey()).add(value);
                }
              } catch (final RecordNotFoundException e) {
                for (final GrowingPropertyArray arr : propArrays.values())
                  arr.addNull();
              }
            } else {
              // Lightweight edge — no properties
              for (final GrowingPropertyArray arr : propArrays.values())
                arr.addNull();
            }
          }
        }
      }
    }

    // Warn if schema sampling stopped while still discovering new property types
    if (!schemaComplete && propertySampleCount >= propertySampleSize && newTypesInLastSample && nodeCount > propertySampleSize)
      LogManager.instance().log(this, Level.WARNING,
          "Property type sampling stopped after %d records (of %d total). Properties appearing only beyond this point will be "
              + "excluded from the columnar store. Use withPropertySampleSize() to increase the sample size or define properties in the schema.",
          propertySampleSize, nodeCount);

    // --- Phase C: Build CSR arrays + remap edge properties to forward CSR order ---
    // Always uses plain Arrays.sort (no satellite arrays). Edge properties extracted in Phase B
    // are remapped from insertion order to sorted forward CSR order via binary search.
    final Map<String, CSRAdjacencyIndex> csrPerType = new HashMap<>();
    final Map<String, ColumnStore> edgeColumnStores = insertionOrderEdgeProps != null ? new HashMap<>() : null;
    final Map<String, int[]> bwdToFwdMap = insertionOrderEdgeProps != null ? new HashMap<>() : null;
    int totalAllEdges = 0;

    for (final Map.Entry<String, int[]> entry : outDegrees.entrySet()) {
      final String edgeTypeName = entry.getKey();
      final int[] outDeg = entry.getValue();
      final int[] inDeg = inDegrees.getOrDefault(edgeTypeName, new int[nodeCount]);
      final IntPairList pairs = edgePairs.get(edgeTypeName);
      final int edgeCount = pairs != null ? pairs.size() : 0;

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

      // Fill from collected edge pairs
      if (pairs != null) {
        final int[] sources = pairs.sources();
        final int[] targets = pairs.targets();
        for (int i = 0; i < edgeCount; i++) {
          final int src = sources[i];
          final int tgt = targets[i];
          fwdNeighbors[fwdOffsets[src] + outCursors[src]++] = tgt;
          bwdNeighbors[bwdOffsets[tgt] + inCursors[tgt]++] = src;
        }
      }

      // Sort neighbor lists — always plain Arrays.sort, no satellite arrays
      for (int i = 0; i < nodeCount; i++) {
        final int fs = fwdOffsets[i], fe = fwdOffsets[i + 1];
        if (fe - fs > 1)
          Arrays.sort(fwdNeighbors, fs, fe);
        final int bs = bwdOffsets[i], be = bwdOffsets[i + 1];
        if (be - bs > 1)
          Arrays.sort(bwdNeighbors, bs, be);
      }

      // Remap edge properties from insertion order to forward CSR order via binary search
      if (insertionOrderEdgeProps != null && insertionOrderEdgeProps.containsKey(edgeTypeName) && edgeCount > 0) {
        final Map<String, GrowingPropertyArray> tempProps = insertionOrderEdgeProps.get(edgeTypeName);
        final int[] sources = pairs.sources();
        final int[] targets = pairs.targets();

        // Detect parallel edges for position matching
        boolean hasParallelEdges = false;
        for (int u = 0; u < nodeCount && !hasParallelEdges; u++)
          for (int f = fwdOffsets[u] + 1; f < fwdOffsets[u + 1]; f++)
            if (fwdNeighbors[f] == fwdNeighbors[f - 1]) {
              hasParallelEdges = true;
              break;
            }
        final boolean[] matched = hasParallelEdges ? new boolean[edgeCount] : null;

        // Create final forward-aligned ColumnStore and remap each insertion-order entry
        final ColumnStore finalStore = new ColumnStore(edgeCount);
        for (final Map.Entry<String, Column.Type> pt : edgePropTypes.entrySet())
          finalStore.createColumn(pt.getKey(), pt.getValue());

        for (int i = 0; i < edgeCount; i++) {
          int fwdPos = Arrays.binarySearch(fwdNeighbors, fwdOffsets[sources[i]], fwdOffsets[sources[i] + 1], targets[i]);
          if (fwdPos < 0)
            continue;
          if (matched != null) {
            while (fwdPos > fwdOffsets[sources[i]] && fwdNeighbors[fwdPos - 1] == targets[i])
              fwdPos--;
            while (matched[fwdPos])
              fwdPos++;
            matched[fwdPos] = true;
          }
          // Copy each property value from insertion-order position i to forward CSR position fwdPos
          for (final Map.Entry<String, Column.Type> pt : edgePropTypes.entrySet()) {
            final GrowingPropertyArray srcArr = tempProps.get(pt.getKey());
            if (srcArr == null || srcArr.isNull(i))
              continue;
            final Column dstCol = finalStore.getColumn(pt.getKey());
            if (dstCol == null)
              continue;
            switch (pt.getValue()) {
            case INT:
              dstCol.setInt(fwdPos, srcArr.getInt(i));
              break;
            case LONG:
              dstCol.setLong(fwdPos, srcArr.getLong(i));
              break;
            case DOUBLE:
              dstCol.setDouble(fwdPos, srcArr.getDouble(i));
              break;
            case STRING:
              dstCol.setString(fwdPos, srcArr.getString(i));
              break;
            }
          }
        }
        edgeColumnStores.put(edgeTypeName, finalStore);

        // Build bwdToFwd via binary search on the sorted CSR
        final int[] bwdToFwd = new int[edgeCount];
        for (int u = 0; u < nodeCount; u++) {
          int b = bwdOffsets[u];
          final int bEnd = bwdOffsets[u + 1];
          while (b < bEnd) {
            final int src = bwdNeighbors[b];
            int count = 1;
            while (b + count < bEnd && bwdNeighbors[b + count] == src)
              count++;
            int fPos = Arrays.binarySearch(fwdNeighbors, fwdOffsets[src], fwdOffsets[src + 1], u);
            if (fPos >= 0) {
              while (fPos > fwdOffsets[src] && fwdNeighbors[fPos - 1] == u)
                fPos--;
              for (int k = 0; k < count; k++)
                bwdToFwd[b + k] = fPos + k;
            }
            b += count;
          }
        }
        bwdToFwdMap.put(edgeTypeName, bwdToFwd);
      }

      csrPerType.put(edgeTypeName, new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors,
          nodeCount, edgeCount));
      totalAllEdges += edgeCount;
    }

    // --- Phase D: BFS vertex reordering for cache locality ---
    // Renumber dense IDs using BFS from the highest-degree node so that graph-nearby
    // nodes get nearby IDs. This improves cache locality for ALL subsequent algorithms.
    if (nodeCount > 1 && !csrPerType.isEmpty()) {
      final int[] oldToNew = computeBfsOrdering(nodeCount, csrPerType);

      // Build inverse mapping
      final int[] newToOld = new int[nodeCount];
      for (int i = 0; i < nodeCount; i++)
        newToOld[oldToNew[i]] = i;

      // Reorder each CSR and remap edge properties + bwdToFwd
      for (final Map.Entry<String, CSRAdjacencyIndex> csrEntry : new HashMap<>(csrPerType).entrySet()) {
        final String edgeTypeName = csrEntry.getKey();
        final boolean hasEdgePropsForType = edgeColumnStores != null && edgeColumnStores.containsKey(edgeTypeName);
        final ReorderedCSR reordered = reorderCSR(csrEntry.getValue(), oldToNew, newToOld, nodeCount, hasEdgePropsForType);
        csrPerType.put(edgeTypeName, reordered.csr);

        // Remap edge property ColumnStore if present using satellite array
        if (hasEdgePropsForType && reordered.oldPosAtNewPos != null) {
          final ColumnStore oldStore = edgeColumnStores.get(edgeTypeName);
          final int edgeCount = reordered.csr.getEdgeCount();
          final ColumnStore newStore = new ColumnStore(edgeCount);
          for (final String colName : oldStore.getPropertyNames()) {
            final Column oldCol = oldStore.getColumn(colName);
            newStore.createColumn(colName, oldCol.getType());
            final Column newCol = newStore.getColumn(colName);
            // oldPosAtNewPos[newPos] = oldPos: copy old value at oldPos to newPos
            for (int newPos = 0; newPos < edgeCount; newPos++) {
              final int oldPos = reordered.oldPosAtNewPos[newPos];
              switch (oldCol.getType()) {
              case INT:
                if (!oldCol.isNull(oldPos))
                  newCol.setInt(newPos, oldCol.getInt(oldPos));
                break;
              case LONG:
                if (!oldCol.isNull(oldPos))
                  newCol.setLong(newPos, oldCol.getLong(oldPos));
                break;
              case DOUBLE:
                if (!oldCol.isNull(oldPos))
                  newCol.setDouble(newPos, oldCol.getDouble(oldPos));
                break;
              case STRING:
                if (!oldCol.isNull(oldPos))
                  newCol.setString(newPos, oldCol.getString(oldPos));
                break;
              }
            }
          }
          edgeColumnStores.put(edgeTypeName, newStore);
        }

        // Rebuild bwdToFwd mapping for reordered CSR
        if (bwdToFwdMap != null && bwdToFwdMap.containsKey(edgeTypeName)) {
          final CSRAdjacencyIndex newCSR = reordered.csr;
          final int[] newFwdOffsets = newCSR.getForwardOffsets();
          final int[] newFwdNeighbors = newCSR.getForwardNeighbors();
          final int[] newBwdOffsets = newCSR.getBackwardOffsets();
          final int[] newBwdNeighbors = newCSR.getBackwardNeighbors();
          final int edgeCount = newCSR.getEdgeCount();
          final int[] newBwdToFwd = new int[edgeCount];
          for (int u = 0; u < nodeCount; u++) {
            int b = newBwdOffsets[u];
            final int bEnd = newBwdOffsets[u + 1];
            while (b < bEnd) {
              final int src = newBwdNeighbors[b];
              int count = 1;
              while (b + count < bEnd && newBwdNeighbors[b + count] == src)
                count++;
              int fPos = Arrays.binarySearch(newFwdNeighbors, newFwdOffsets[src], newFwdOffsets[src + 1], u);
              if (fPos >= 0) {
                while (fPos > newFwdOffsets[src] && newFwdNeighbors[fPos - 1] == u)
                  fPos--;
                for (int k = 0; k < count; k++)
                  newBwdToFwd[b + k] = fPos + k;
              }
              b += count;
            }
          }
          bwdToFwdMap.put(edgeTypeName, newBwdToFwd);
        }
      }

      // Apply permutation to NodeIdMapping (transparent to all lookups)
      mapping.applyReordering(oldToNew);
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
    int edgePropColumns = 0;
    if (edgeColumnStores != null)
      for (final ColumnStore ecs : edgeColumnStores.values()) {
        totalMemory += ecs.getMemoryUsageBytes();
        edgePropColumns += ecs.getColumnCount();
      }
    if (bwdToFwdMap != null)
      for (final int[] bwdToFwd : bwdToFwdMap.values())
        totalMemory += (long) bwdToFwd.length * Integer.BYTES;
    LogManager.instance().log(this, Level.INFO,
        "CSR built: %d nodes (%d buckets), %d edges (%d edge types), %d columns (%d edge prop columns), %.1f MB, %d ms",
        nodeCount, mapping.getNumBuckets(), totalAllEdges, csrPerType.size(), totalColumns, edgePropColumns,
        totalMemory / (1024.0 * 1024.0), elapsedMs);

    return new CSRResult(csrPerType, mapping, bucketColumns, edgeColumnStores, bwdToFwdMap);
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

  private Map<String, Column.Type> detectPropertyTypesFromSchema(final String[] vertexTypes) {
    final Map<String, Column.Type> result = new HashMap<>();
    if (vertexTypes == null || vertexTypes.length == 0) {
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof VertexType)
          collectSchemaProperties(dt, result);
    } else {
      for (final String typeName : vertexTypes)
        collectSchemaProperties(database.getSchema().getType(typeName), result);
    }
    return result;
  }

  private void collectSchemaProperties(final DocumentType type, final Map<String, Column.Type> result) {
    for (final Property prop : type.getProperties()) {
      if (propertyFilterSet != null && !propertyFilterSet.contains(prop.getName()))
        continue;
      final Column.Type colType = schemaTypeToColumnType(prop.getType());
      if (colType == null)
        continue;
      final Column.Type existing = result.get(prop.getName());
      final Column.Type merged = mergeColumnType(existing, colType);
      if (merged != null)
        result.put(prop.getName(), merged);
      else {
        // Type conflict across vertex types — remove to prevent ClassCastException
        LogManager.instance().log(this, Level.WARNING,
            "Property '%s' excluded from columnar storage: type conflict (%s vs %s) across vertex types",
            prop.getName(), existing, colType);
        result.remove(prop.getName());
      }
    }
  }

  private static Column.Type schemaTypeToColumnType(final com.arcadedb.schema.Type schemaType) {
    return switch (schemaType) {
      case INTEGER, SHORT, BYTE -> Column.Type.INT;
      case LONG -> Column.Type.LONG;
      case DOUBLE, FLOAT -> Column.Type.DOUBLE;
      case STRING -> Column.Type.STRING;
      default -> null;
    };
  }

  /**
   * Same property name can have different schema types across vertex types (e.g., LONG vs DATETIME_MICROS).
   * When types conflict, drop the property from columnar storage to avoid ClassCastException.
   */
  private static Column.Type mergeColumnType(final Column.Type existing, final Column.Type incoming) {
    if (existing == null)
      return incoming;
    if (incoming == null || existing == incoming)
      return existing;
    // Type conflict — cannot safely store in a single column
    return null;
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

      // Skip values whose runtime type doesn't match the detected column type
      // (e.g., LocalDateTime in a property detected as LONG from schema or sampling)
      switch (entry.getValue()) {
      case INT:
      case LONG:
      case DOUBLE:
        if (!(value instanceof Number))
          continue;
        break;
      }

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
        if (dt instanceof EdgeType)
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
      final MultiIterator<Record> multi = new MultiIterator<>();
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof VertexType)
          multi.addIterator(database.iterateType(dt.getName(), false));
      return multi;
    }
    if (vertexTypes.length == 1)
      return database.iterateType(vertexTypes[0], false);

    final MultiIterator<Record> multi = new MultiIterator<>();
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

  private Map<String, Column.Type> detectEdgePropertyTypesFromSchema(final String[] edgeTypes) {
    final Map<String, Column.Type> result = new HashMap<>();
    if (edgeTypes == null || edgeTypes.length == 0) {
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof EdgeType)
          collectEdgeSchemaProperties(dt, result);
    } else {
      for (final String typeName : edgeTypes)
        collectEdgeSchemaProperties(database.getSchema().getType(typeName), result);
    }
    return result;
  }

  private void collectEdgeSchemaProperties(final DocumentType type, final Map<String, Column.Type> result) {
    for (final Property prop : type.getProperties()) {
      if (edgePropertyFilterSet != null && !edgePropertyFilterSet.contains(prop.getName()))
        continue;
      final Column.Type colType = schemaTypeToColumnType(prop.getType());
      if (colType == null)
        continue;
      final Column.Type existing = result.get(prop.getName());
      final Column.Type merged = mergeColumnType(existing, colType);
      if (merged != null)
        result.put(prop.getName(), merged);
      else {
        LogManager.instance().log(this, Level.WARNING,
            "Edge property '%s' excluded from columnar storage: type conflict (%s vs %s) across edge types",
            prop.getName(), existing, colType);
        result.remove(prop.getName());
      }
    }
  }

  private void fillEdgeProperties(final Document edgeDoc, final ColumnStore store, final int edgeIdx,
      final Map<String, Column.Type> detectedTypes) {
    for (final Map.Entry<String, Column.Type> entry : detectedTypes.entrySet()) {
      final String propName = entry.getKey();
      final Object value = edgeDoc.get(propName);
      if (value == null)
        continue;

      final Column column = store.getColumn(propName);
      if (column == null)
        continue;

      switch (entry.getValue()) {
      case INT:
      case LONG:
      case DOUBLE:
        if (!(value instanceof Number))
          continue;
        break;
      }

      switch (entry.getValue()) {
      case INT:
        column.setInt(edgeIdx, ((Number) value).intValue());
        break;
      case LONG:
        column.setLong(edgeIdx, ((Number) value).longValue());
        break;
      case DOUBLE:
        column.setDouble(edgeIdx, ((Number) value).doubleValue());
        break;
      case STRING:
        column.setString(edgeIdx, value.toString());
        break;
      }
    }
  }

  /**
   * Sorts keys[from..to) while applying the same permutation to satellite[from..to).
   * Zero GC pressure: encodes key|index into a primitive long[], sorts, then unpacks.
   */
  static void parallelSort(final int[] keys, final int[] satellite, final int from, final int to) {
    final int len = to - from;
    // Pack key (high 32 bits) | original index (low 32 bits) into long[] for primitive sort
    final long[] encoded = new long[len];
    for (int i = 0; i < len; i++)
      encoded[i] = ((long) keys[from + i] << 32) | (i & 0xFFFFFFFFL);

    Arrays.sort(encoded);

    // Unpack sorted order into both arrays
    final int[] tmpSat = new int[len];
    for (int i = 0; i < len; i++) {
      final int origIdx = (int) encoded[i];
      keys[from + i] = (int) (encoded[i] >>> 32);
      tmpSat[i] = satellite[from + origIdx];
    }
    System.arraycopy(tmpSat, 0, satellite, from, len);
  }

  /**
   * Computes a BFS-order vertex renumbering from the highest-degree node.
   * After renumbering, graph-nearby nodes have nearby dense IDs,
   * dramatically improving cache locality for all graph algorithms.
   *
   * @return oldToNew mapping: oldToNew[oldId] = newId
   */
  static int[] computeBfsOrdering(final int nodeCount, final Map<String, CSRAdjacencyIndex> csrPerType) {
    final int[] oldToNew = new int[nodeCount];
    Arrays.fill(oldToNew, -1);

    // Build combined degree for all edge types to find the best root
    int root = 0;
    int maxDeg = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values()) {
      final int[] fwdOffsets = csr.getForwardOffsets();
      final int[] bwdOffsets = csr.getBackwardOffsets();
      for (int u = 0; u < nodeCount; u++) {
        final int deg = (fwdOffsets[u + 1] - fwdOffsets[u]) + (bwdOffsets[u + 1] - bwdOffsets[u]);
        if (deg > maxDeg) {
          maxDeg = deg;
          root = u;
        }
      }
    }

    // BFS renumbering from highest-degree root
    int nextId = 0;
    final int[] queue = new int[nodeCount];
    int head = 0, tail = 0;
    oldToNew[root] = nextId++;
    queue[tail++] = root;

    while (head < tail) {
      final int u = queue[head++];
      for (final CSRAdjacencyIndex csr : csrPerType.values()) {
        // Traverse forward neighbors
        final int[] fwdOffsets = csr.getForwardOffsets();
        final int[] fwdNeighbors = csr.getForwardNeighbors();
        for (int j = fwdOffsets[u]; j < fwdOffsets[u + 1]; j++) {
          final int v = fwdNeighbors[j];
          if (oldToNew[v] < 0) {
            oldToNew[v] = nextId++;
            queue[tail++] = v;
          }
        }
        // Traverse backward neighbors (for directed graphs, reach "parent" nodes too)
        final int[] bwdOffsets = csr.getBackwardOffsets();
        final int[] bwdNeighbors = csr.getBackwardNeighbors();
        for (int j = bwdOffsets[u]; j < bwdOffsets[u + 1]; j++) {
          final int v = bwdNeighbors[j];
          if (oldToNew[v] < 0) {
            oldToNew[v] = nextId++;
            queue[tail++] = v;
          }
        }
      }
    }

    // Assign remaining disconnected nodes
    for (int u = 0; u < nodeCount; u++)
      if (oldToNew[u] < 0)
        oldToNew[u] = nextId++;

    return oldToNew;
  }

  /**
   * Rebuilds a CSR adjacency index with remapped node IDs.
   * Creates new offsets based on the reordered node IDs, copies and translates
   * neighbor references, and re-sorts neighbor lists. Uses a satellite array to
   * track old edge positions through the sort (for edge property remapping).
   *
   * @param oldCSR       the original CSR
   * @param oldToNew     oldId → newId permutation
   * @param newToOld     newId → oldId permutation
   * @param nodeCount    total number of nodes
   * @param hasEdgeProps whether edge properties exist and need position tracking
   *
   * @return the reordered CSR with remapped IDs and a mapping from old to new edge positions
   */
  static ReorderedCSR reorderCSR(final CSRAdjacencyIndex oldCSR, final int[] oldToNew,
      final int[] newToOld, final int nodeCount, final boolean hasEdgeProps) {
    final int edgeCount = oldCSR.getEdgeCount();
    final int[] oldFwdOffsets = oldCSR.getForwardOffsets();
    final int[] oldFwdNeighbors = oldCSR.getForwardNeighbors();
    final int[] oldBwdOffsets = oldCSR.getBackwardOffsets();
    final int[] oldBwdNeighbors = oldCSR.getBackwardNeighbors();

    // Build new forward CSR: iterate in new ID order, copy old edges with translated IDs
    final int[] newFwdOffsets = new int[nodeCount + 1];
    for (int newId = 0; newId < nodeCount; newId++) {
      final int oldId = newToOld[newId];
      newFwdOffsets[newId + 1] = newFwdOffsets[newId] + (oldFwdOffsets[oldId + 1] - oldFwdOffsets[oldId]);
    }

    final int[] newFwdNeighbors = new int[edgeCount];
    // Satellite array: tracks which old edge position each new position came from.
    // Used to remap edge properties after the sort changes positions.
    final int[] oldPosAtNewPos = hasEdgeProps && edgeCount > 0 ? new int[edgeCount] : null;
    for (int newId = 0; newId < nodeCount; newId++) {
      final int oldId = newToOld[newId];
      final int oldStart = oldFwdOffsets[oldId];
      final int oldEnd = oldFwdOffsets[oldId + 1];
      final int newStart = newFwdOffsets[newId];
      for (int j = oldStart; j < oldEnd; j++) {
        final int newPos = newStart + (j - oldStart);
        newFwdNeighbors[newPos] = oldToNew[oldFwdNeighbors[j]];
        if (oldPosAtNewPos != null)
          oldPosAtNewPos[newPos] = j;
      }
      // Re-sort neighbor list after ID translation, carrying satellite data along
      final int newEnd = newFwdOffsets[newId + 1];
      if (newEnd - newStart > 1) {
        if (oldPosAtNewPos != null)
          parallelSort(newFwdNeighbors, oldPosAtNewPos, newStart, newEnd);
        else
          Arrays.sort(newFwdNeighbors, newStart, newEnd);
      }
    }

    // Build new backward CSR the same way
    final int[] newBwdOffsets = new int[nodeCount + 1];
    for (int newId = 0; newId < nodeCount; newId++) {
      final int oldId = newToOld[newId];
      newBwdOffsets[newId + 1] = newBwdOffsets[newId] + (oldBwdOffsets[oldId + 1] - oldBwdOffsets[oldId]);
    }

    final int[] newBwdNeighbors = new int[edgeCount];
    for (int newId = 0; newId < nodeCount; newId++) {
      final int oldId = newToOld[newId];
      final int oldStart = oldBwdOffsets[oldId];
      final int oldEnd = oldBwdOffsets[oldId + 1];
      final int newStart = newBwdOffsets[newId];
      for (int j = oldStart; j < oldEnd; j++)
        newBwdNeighbors[newStart + (j - oldStart)] = oldToNew[oldBwdNeighbors[j]];
      final int newEnd = newBwdOffsets[newId + 1];
      if (newEnd - newStart > 1)
        Arrays.sort(newBwdNeighbors, newStart, newEnd);
    }

    return new ReorderedCSR(
        new CSRAdjacencyIndex(newFwdOffsets, newFwdNeighbors, newBwdOffsets, newBwdNeighbors, nodeCount, edgeCount),
        oldPosAtNewPos);
  }

  /**
   * Result of CSR reordering: the new CSR plus the satellite array mapping
   * new forward edge positions back to old forward edge positions (for edge property remapping).
   */
  static final class ReorderedCSR {
    final CSRAdjacencyIndex csr;
    final int[]             oldPosAtNewPos; // newFwdPos → oldFwdPos

    ReorderedCSR(final CSRAdjacencyIndex csr, final int[] oldPosAtNewPos) {
      this.csr = csr;
      this.oldPosAtNewPos = oldPosAtNewPos;
    }
  }

  static long packRid(final RID rid) {
    return ((long) rid.getBucketId() << 32) | (rid.getPosition() & 0xFFFFFFFFL);
  }

  static RID unpackRid(final long packed) {
    final int bucketId = (int) (packed >>> 32);
    final long position = (int) packed; // sign-extend for lightweight edges (negative positions)
    return new RID(null, bucketId, position);
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
   * Compact int-pair list for collecting (source, target) edge pairs without boxing.
   * Optionally stores edge RIDs (packed as long) when edge properties are requested.
   */
  static final class IntPairList {
    private int[]  src;
    private int[]  tgt;
    private long[] edgeRids; // only allocated when edge properties are requested
    private int    count;

    IntPairList() {
      src = new int[256];
      tgt = new int[256];
    }

    IntPairList(final boolean trackEdgeRids) {
      this();
      if (trackEdgeRids)
        edgeRids = new long[256];
    }

    void add(final int source, final int target) {
      if (count == src.length) {
        final int newLen = src.length * 2;
        src = Arrays.copyOf(src, newLen);
        tgt = Arrays.copyOf(tgt, newLen);
      }
      src[count] = source;
      tgt[count] = target;
      count++;
    }

    void add(final int source, final int target, final long edgeRid) {
      if (count == src.length) {
        final int newLen = src.length * 2;
        src = Arrays.copyOf(src, newLen);
        tgt = Arrays.copyOf(tgt, newLen);
        if (edgeRids != null)
          edgeRids = Arrays.copyOf(edgeRids, newLen);
      }
      src[count] = source;
      tgt[count] = target;
      if (edgeRids != null)
        edgeRids[count] = edgeRid;
      count++;
    }

    int size() { return count; }
    int[] sources() { return src; }
    int[] targets() { return tgt; }
    long[] edgeRids() { return edgeRids; }
  }

  /**
   * Result of CSR building. Edge properties (if configured) are fully materialized during build.
   */
  public static class CSRResult {
    private final Map<String, CSRAdjacencyIndex> csrPerType;
    private final NodeIdMapping                  mapping;
    private final ColumnStore[]                  bucketColumns;
    private final Map<String, ColumnStore>       edgeColumnStores;
    private final Map<String, int[]>             bwdToFwd;

    public CSRResult(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping mapping,
        final ColumnStore[] bucketColumns) {
      this(csrPerType, mapping, bucketColumns, null, null);
    }

    public CSRResult(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping mapping,
        final ColumnStore[] bucketColumns, final Map<String, ColumnStore> edgeColumnStores,
        final Map<String, int[]> bwdToFwd) {
      this.csrPerType = csrPerType;
      this.mapping = mapping;
      this.bucketColumns = bucketColumns;
      this.edgeColumnStores = edgeColumnStores;
      this.bwdToFwd = bwdToFwd;
    }

    public Map<String, CSRAdjacencyIndex> getCsrPerType() { return csrPerType; }
    public NodeIdMapping getMapping() { return mapping; }
    public ColumnStore[] getBucketColumns() { return bucketColumns; }
    public Map<String, ColumnStore> getEdgeColumnStores() { return edgeColumnStores; }
    public Map<String, int[]> getBwdToFwd() { return bwdToFwd; }
  }

  /**
   * Growing typed array for collecting edge property values during Phase B.
   * Uses primitive arrays with doubling growth (like IntPairList) to avoid boxing.
   */
  static final class GrowingPropertyArray {
    private final Column.Type type;
    private int[]    intData;
    private long[]   longData;
    private double[] doubleData;
    private String[] stringData;
    private long[]   nullBitset;
    private int      count;
    private int      capacity;

    GrowingPropertyArray(final Column.Type type) {
      this.type = type;
      this.capacity = 256;
      this.nullBitset = new long[(capacity + 63) >>> 6];
      Arrays.fill(nullBitset, ~0L); // all null initially
      switch (type) {
      case INT:
        intData = new int[capacity];
        break;
      case LONG:
        longData = new long[capacity];
        break;
      case DOUBLE:
        doubleData = new double[capacity];
        break;
      case STRING:
        stringData = new String[capacity];
        break;
      }
    }

    void add(final Object value) {
      if (count == capacity)
        grow();
      if (value != null) {
        switch (type) {
        case INT:
          if (value instanceof Number n) {
            intData[count] = n.intValue();
            clearNull(count);
          }
          break;
        case LONG:
          if (value instanceof Number n) {
            longData[count] = n.longValue();
            clearNull(count);
          }
          break;
        case DOUBLE:
          if (value instanceof Number n) {
            doubleData[count] = n.doubleValue();
            clearNull(count);
          }
          break;
        case STRING:
          stringData[count] = value.toString();
          clearNull(count);
          break;
        }
      }
      count++;
    }

    void addNull() {
      if (count == capacity)
        grow();
      count++; // null bit already set
    }

    boolean isNull(final int idx) {
      return (nullBitset[idx >>> 6] & (1L << (idx & 63))) != 0;
    }

    int getInt(final int idx) { return intData[idx]; }
    long getLong(final int idx) { return longData[idx]; }
    double getDouble(final int idx) { return doubleData[idx]; }
    String getString(final int idx) { return stringData[idx]; }

    private void clearNull(final int idx) {
      nullBitset[idx >>> 6] &= ~(1L << (idx & 63));
    }

    private void grow() {
      final int newCap = capacity * 2;
      nullBitset = Arrays.copyOf(nullBitset, (newCap + 63) >>> 6);
      // Set new bits to null
      for (int i = (capacity + 63) >>> 6; i < nullBitset.length; i++)
        nullBitset[i] = ~0L;
      switch (type) {
      case INT:
        intData = Arrays.copyOf(intData, newCap);
        break;
      case LONG:
        longData = Arrays.copyOf(longData, newCap);
        break;
      case DOUBLE:
        doubleData = Arrays.copyOf(doubleData, newCap);
        break;
      case STRING:
        stringData = Arrays.copyOf(stringData, newCap);
        break;
      }
      capacity = newCap;
    }
  }
}
