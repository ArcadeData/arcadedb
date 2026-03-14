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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.RID;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-bucket bidirectional mapping between ArcadeDB RIDs and dense integer IDs.
 * <p>
 * Organized per bucket for:
 * <ul>
 *   <li>Each bucket has its own dense ID space [0..bucketSize), enabling billions of nodes
 *       across multiple buckets (each bucket can hold up to 2.1B nodes)</li>
 *   <li>1:1 alignment with ArcadeDB's storage architecture</li>
 *   <li>Per-bucket parallel building and property scanning</li>
 *   <li>No HashMap for RID→ID lookup (uses sorted position arrays with binary search)</li>
 * </ul>
 * <p>
 * Global dense IDs are computed as {@code bucketBase[bucketIdx] + localId},
 * where localId is the node's index within its bucket's sorted position array.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NodeIdMapping {
  // Compact bucket index: maps ArcadeDB bucketId → bucketIdx (0..numBuckets-1)
  private final Map<Integer, Integer> bucketIdToIdx;
  private       int[]                 bucketIds;     // bucketIdx → ArcadeDB bucketId
  private       String[]              bucketTypeNames; // bucketIdx → vertex type name

  // Per-bucket sorted RID positions: positions[bucketIdx][localId] = RID.getPosition()
  // localId = index in sorted array. Binary search for reverse lookup.
  private       long[][]              positions;

  // Global ID computation: globalId = bucketBase[bucketIdx] + localId
  private       int[]                 bucketBase;
  private       int[]                 bucketSizes;
  private       int                   totalSize;
  private       int                   numBuckets;

  // Building phase: temporary lists before compact()
  private       long[][]              positionsBuilder;
  private       int[]                 builderSizes;

  public NodeIdMapping(final int expectedBuckets) {
    this.bucketIdToIdx = new HashMap<>(expectedBuckets);
    this.bucketIds = new int[expectedBuckets];
    this.bucketTypeNames = new String[expectedBuckets];
    this.positionsBuilder = new long[expectedBuckets][];
    this.builderSizes = new int[expectedBuckets];
    this.numBuckets = 0;
    this.totalSize = 0;
  }

  /**
   * Registers a bucket and prepares it for node collection.
   * Must be called before addNode().
   *
   * @return the compact bucket index
   */
  public int registerBucket(final int bucketId, final String typeName, final int estimatedSize) {
    Integer idx = bucketIdToIdx.get(bucketId);
    if (idx != null)
      return idx;

    idx = numBuckets++;
    if (idx >= bucketIds.length)
      growBucketArrays();

    bucketIds[idx] = bucketId;
    bucketTypeNames[idx] = typeName;
    positionsBuilder[idx] = new long[Math.max(estimatedSize, 64)];
    builderSizes[idx] = 0;
    bucketIdToIdx.put(bucketId, idx);
    return idx;
  }

  /**
   * Adds a node (RID position) to a bucket during the building phase.
   *
   * @return the local ID within the bucket
   */
  public int addNode(final int bucketIdx, final long ridPosition) {
    if (positionsBuilder == null)
      throw new IllegalStateException("NodeIdMapping has been compacted; addNode() is not allowed");
    final int localId = builderSizes[bucketIdx];
    if (localId >= positionsBuilder[bucketIdx].length) {
      final long[] old = positionsBuilder[bucketIdx];
      positionsBuilder[bucketIdx] = new long[old.length * 2];
      System.arraycopy(old, 0, positionsBuilder[bucketIdx], 0, localId);
    }
    positionsBuilder[bucketIdx][localId] = ridPosition;
    builderSizes[bucketIdx] = localId + 1;
    return localId;
  }

  /**
   * Compacts the mapping after building is complete.
   * Sorts positions per bucket and computes global base offsets.
   * After this call, the mapping is immutable and ready for lookups.
   */
  public void compact() {
    bucketBase = new int[numBuckets];
    bucketSizes = new int[numBuckets];
    positions = new long[numBuckets][];
    totalSize = 0;

    for (int i = 0; i < numBuckets; i++) {
      bucketBase[i] = totalSize;
      final int size = builderSizes[i];
      bucketSizes[i] = size;

      // Trim and sort positions for binary search
      positions[i] = new long[size];
      System.arraycopy(positionsBuilder[i], 0, positions[i], 0, size);
      Arrays.sort(positions[i]);

      totalSize += size;
    }

    // Release builder structures
    positionsBuilder = null;
    builderSizes = null;

    // Trim bucket arrays
    if (bucketIds.length > numBuckets) {
      bucketIds = Arrays.copyOf(bucketIds, numBuckets);
      bucketTypeNames = Arrays.copyOf(bucketTypeNames, numBuckets);
    }
  }

  // --- Lookup methods (call after compact()) ---

  /**
   * Returns the global dense ID for a RID, or -1 if not mapped.
   */
  public int getGlobalId(final RID rid) {
    final Integer bucketIdx = bucketIdToIdx.get(rid.getBucketId());
    if (bucketIdx == null)
      return -1;
    final int localId = Arrays.binarySearch(positions[bucketIdx], rid.getPosition());
    if (localId < 0)
      return -1;
    return bucketBase[bucketIdx] + localId;
  }

  /**
   * Returns the global dense ID for a RID, or -1 if not mapped.
   * Alias for getGlobalId() for backward compatibility.
   */
  public int getId(final RID rid) {
    return getGlobalId(rid);
  }

  /**
   * Returns the bucket index for a global dense ID.
   */
  public int getBucketIdx(final int globalId) {
    // Binary search on bucketBase to find which bucket this ID falls in
    int lo = 0, hi = numBuckets - 1;
    while (lo < hi) {
      final int mid = (lo + hi + 1) >>> 1;
      if (bucketBase[mid] <= globalId)
        lo = mid;
      else
        hi = mid - 1;
    }
    return lo;
  }

  /**
   * Returns the local ID within a bucket for a global dense ID.
   */
  public int getLocalId(final int globalId) {
    return globalId - bucketBase[getBucketIdx(globalId)];
  }

  /**
   * Returns the ArcadeDB bucket ID for a given bucket index.
   */
  public int getBucketId(final int bucketIdx) {
    return bucketIds[bucketIdx];
  }

  /**
   * Returns the bucket index for an ArcadeDB bucket ID, or -1 if not registered.
   */
  public int getBucketIdxForBucketId(final int bucketId) {
    final Integer idx = bucketIdToIdx.get(bucketId);
    return idx != null ? idx : -1;
  }

  /**
   * Returns the RID for a given global dense ID.
   * Creates a new RID object on each call to avoid pre-allocating an RID per node
   * (which would add ~40 bytes/node of heap for a cache that may never be fully accessed).
   */
  public RID getRID(final int globalId) {
    return getRID(null, globalId);
  }

  /**
   * Returns the RID for a given global dense ID, with an explicit database reference.
   * Passing the database avoids reliance on thread-local context (which can be null
   * when multiple databases are open in tests or concurrent scenarios).
   */
  public RID getRID(final BasicDatabase database, final int globalId) {
    final int bucketIdx = getBucketIdx(globalId);
    final int localId = globalId - bucketBase[bucketIdx];
    return new RID(database, bucketIds[bucketIdx], positions[bucketIdx][localId]);
  }

  /**
   * Returns the vertex type name for a given global dense ID.
   */
  public String getTypeName(final int globalId) {
    return bucketTypeNames[getBucketIdx(globalId)];
  }

  /**
   * Returns the vertex type name for a given bucket index.
   */
  public String getBucketTypeName(final int bucketIdx) {
    return bucketTypeNames[bucketIdx];
  }

  /**
   * Returns the number of nodes in a specific bucket.
   */
  public int getBucketSize(final int bucketIdx) {
    return bucketSizes[bucketIdx];
  }

  /**
   * Returns the global base offset for a bucket.
   */
  public int getBucketBase(final int bucketIdx) {
    return bucketBase[bucketIdx];
  }

  /**
   * Returns the total number of nodes across all buckets.
   */
  public int size() {
    return totalSize;
  }

  /**
   * Returns the number of registered buckets.
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Returns the RID position for a given bucket index and local ID.
   */
  public long getPosition(final int bucketIdx, final int localId) {
    return positions[bucketIdx][localId];
  }

  /**
   * Returns the estimated memory footprint in bytes.
   */
  public long getMemoryUsageBytes() {
    long bytes = 0;
    // bucketIds, bucketBase, bucketSizes arrays
    bytes += (long) bucketIds.length * Integer.BYTES;
    if (bucketBase != null)
      bytes += (long) bucketBase.length * Integer.BYTES;
    if (bucketSizes != null)
      bytes += (long) bucketSizes.length * Integer.BYTES;
    // positions arrays (long[][])
    if (positions != null) {
      for (int i = 0; i < numBuckets; i++)
        if (positions[i] != null)
          bytes += (long) positions[i].length * Long.BYTES;
    }
    // bucketIdToIdx HashMap: ~48 bytes per entry (key Integer 16B + value Integer 16B + Entry 32B)
    bytes += (long) bucketIdToIdx.size() * 48;
    // bucketTypeNames: reference array + rough estimate for String objects
    bytes += (long) numBuckets * 8; // references
    return bytes;
  }

  private void growBucketArrays() {
    final int newCap = Math.max(numBuckets * 2, 8);
    bucketIds = Arrays.copyOf(bucketIds, newCap);
    bucketTypeNames = Arrays.copyOf(bucketTypeNames, newCap);
    positionsBuilder = Arrays.copyOf(positionsBuilder, newCap);
    builderSizes = Arrays.copyOf(builderSizes, newCap);
  }
}
