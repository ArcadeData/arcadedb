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

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bidirectional mapping between ArcadeDB RIDs and dense integer IDs (0..N-1),
 * with vertex type tracking for each node.
 * <p>
 * Dense IDs enable array-based access patterns for CSR and columnar storage,
 * eliminating hash lookups in the hot path.
 * <p>
 * The internal arrays are laid out contiguously for SIMD-friendly sequential access:
 * - bucketIds[] and offsets[] store the RID components as parallel primitive arrays
 * - typeIds[] stores the vertex type index for each node (0-based into the type catalog)
 * - The forward map (RID→int) uses a HashMap for building phase only
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NodeIdMapping {
  private final Map<RID, Integer>    ridToId;
  private final Map<String, Integer> typeNameToIndex;
  private final List<String>         typeNames;
  private       int[]                bucketIds;
  private       long[]               offsets;
  private       int[]                typeIds;
  private       int                  size;

  public NodeIdMapping(final int initialCapacity) {
    this.ridToId = new HashMap<>(initialCapacity);
    this.typeNameToIndex = new HashMap<>();
    this.typeNames = new ArrayList<>();
    this.bucketIds = new int[initialCapacity];
    this.offsets = new long[initialCapacity];
    this.typeIds = new int[initialCapacity];
    this.size = 0;
  }

  /**
   * Adds a RID with its vertex type name and assigns it the next dense ID.
   *
   * @return the assigned dense ID
   */
  public int addRID(final RID rid, final String typeName) {
    final Integer existing = ridToId.get(rid);
    if (existing != null)
      return existing;

    final int id = size;
    if (id >= bucketIds.length)
      grow();

    bucketIds[id] = rid.getBucketId();
    offsets[id] = rid.getPosition();
    typeIds[id] = getOrCreateTypeIndex(typeName);
    ridToId.put(rid, id);
    size++;
    return id;
  }

  /**
   * Returns the dense ID for a RID, or -1 if not mapped.
   */
  public int getId(final RID rid) {
    final Integer id = ridToId.get(rid);
    return id != null ? id : -1;
  }

  /**
   * Returns the bucket ID component of the RID at the given dense ID.
   */
  public int getBucketId(final int denseId) {
    return bucketIds[denseId];
  }

  /**
   * Returns the offset component of the RID at the given dense ID.
   */
  public long getOffset(final int denseId) {
    return offsets[denseId];
  }

  /**
   * Returns the vertex type index for the given dense ID.
   */
  public int getTypeId(final int denseId) {
    return typeIds[denseId];
  }

  /**
   * Returns the vertex type name for the given dense ID.
   */
  public String getTypeName(final int denseId) {
    return typeNames.get(typeIds[denseId]);
  }

  /**
   * Returns the type index for a given type name, or -1 if not present.
   */
  public int getTypeIndex(final String typeName) {
    final Integer idx = typeNameToIndex.get(typeName);
    return idx != null ? idx : -1;
  }

  /**
   * Returns the type name for a given type index.
   */
  public String getTypeNameByIndex(final int typeIndex) {
    return typeNames.get(typeIndex);
  }

  /**
   * Returns the number of distinct vertex types.
   */
  public int getTypeCount() {
    return typeNames.size();
  }

  /**
   * Returns the direct type IDs array for batch processing.
   * Do NOT modify the returned array.
   */
  public int[] getTypeIds() {
    return typeIds;
  }

  /**
   * Reconstructs the RID for a given dense ID. Note: this creates a new RID object.
   * Avoid calling in hot loops; use getBucketId/getOffset for batch operations.
   */
  public RID getRID(final int denseId) {
    return new RID(bucketIds[denseId], offsets[denseId]);
  }

  public int size() {
    return size;
  }

  /**
   * Compacts internal arrays to exact size, freeing unused memory.
   * Call after building is complete.
   */
  public void compact() {
    if (bucketIds.length > size) {
      final int[] newBucketIds = new int[size];
      System.arraycopy(bucketIds, 0, newBucketIds, 0, size);
      bucketIds = newBucketIds;

      final long[] newOffsets = new long[size];
      System.arraycopy(offsets, 0, newOffsets, 0, size);
      offsets = newOffsets;

      final int[] newTypeIds = new int[size];
      System.arraycopy(typeIds, 0, newTypeIds, 0, size);
      typeIds = newTypeIds;
    }
  }

  private int getOrCreateTypeIndex(final String typeName) {
    final Integer existing = typeNameToIndex.get(typeName);
    if (existing != null)
      return existing;

    final int index = typeNames.size();
    typeNames.add(typeName);
    typeNameToIndex.put(typeName, index);
    return index;
  }

  private void grow() {
    final int newCapacity = Math.max(bucketIds.length * 2, 16);

    final int[] newBucketIds = new int[newCapacity];
    System.arraycopy(bucketIds, 0, newBucketIds, 0, size);
    bucketIds = newBucketIds;

    final long[] newOffsets = new long[newCapacity];
    System.arraycopy(offsets, 0, newOffsets, 0, size);
    offsets = newOffsets;

    final int[] newTypeIds = new int[newCapacity];
    System.arraycopy(typeIds, 0, newTypeIds, 0, size);
    typeIds = newTypeIds;
  }
}
