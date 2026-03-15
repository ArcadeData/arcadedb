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
  // Uses a flat int[] lookup table instead of HashMap to avoid autoboxing on every getGlobalId() call.
  // Bucket IDs in ArcadeDB are small contiguous integers (typically < 100), so the array is compact.
  private       int[]                 bucketIdToIdx; // bucketId → bucketIdx, -1 = unmapped
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

  // Optional BFS reordering permutation (set after CSR build for cache locality)
  private       int[]                 oldToNew;  // naturalId → reorderedId
  private       int[]                 newToOld;  // reorderedId → naturalId

  // Building phase: temporary lists before compact()
  private       long[][]              positionsBuilder;
  private       int[]                 builderSizes;

  public NodeIdMapping(final int expectedBuckets) {
    this.bucketIdToIdx = new int[Math.max(expectedBuckets, 16)];
    Arrays.fill(this.bucketIdToIdx, -1);
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
    if (bucketId < bucketIdToIdx.length) {
      final int existing = bucketIdToIdx[bucketId];
      if (existing >= 0)
        return existing;
    } else {
      // Grow the lookup table to accommodate the bucket ID
      final int newLen = Math.max(bucketId + 1, bucketIdToIdx.length * 2);
      final int[] grown = new int[newLen];
      System.arraycopy(bucketIdToIdx, 0, grown, 0, bucketIdToIdx.length);
      Arrays.fill(grown, bucketIdToIdx.length, newLen, -1);
      bucketIdToIdx = grown;
    }

    final int idx = numBuckets++;
    if (idx >= bucketIds.length)
      growBucketArrays();

    bucketIds[idx] = bucketId;
    bucketTypeNames[idx] = typeName;
    positionsBuilder[idx] = new long[Math.max(estimatedSize, 64)];
    builderSizes[idx] = 0;
    bucketIdToIdx[bucketId] = idx;
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
   * If BFS reordering is applied, returns the reordered ID.
   */
  public int getGlobalId(final RID rid) {
    final int bucketId = rid.getBucketId();
    if (bucketId < 0 || bucketId >= bucketIdToIdx.length)
      return -1;
    final int bucketIdx = bucketIdToIdx[bucketId];
    if (bucketIdx < 0)
      return -1;
    final int localId = Arrays.binarySearch(positions[bucketIdx], rid.getPosition());
    if (localId < 0)
      return -1;
    final int naturalId = bucketBase[bucketIdx] + localId;
    return oldToNew != null ? oldToNew[naturalId] : naturalId;
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
   * If BFS reordering is applied, translates through the permutation first.
   */
  public int getBucketIdx(final int globalId) {
    final int naturalId = newToOld != null ? newToOld[globalId] : globalId;
    return bucketIdxForNaturalId(naturalId);
  }

  /**
   * Returns the local ID within a bucket for a global dense ID.
   * If BFS reordering is applied, translates through the permutation first.
   */
  public int getLocalId(final int globalId) {
    final int naturalId = newToOld != null ? newToOld[globalId] : globalId;
    return naturalId - bucketBase[bucketIdxForNaturalId(naturalId)];
  }

  /**
   * Returns both bucket index and local ID for a global dense ID in a single call,
   * avoiding the double binary search that would occur when calling getBucketIdx()
   * and getLocalId() separately. The two values are packed into a single long to
   * avoid object allocation: bucket index in the upper 32 bits, local ID in the lower 32 bits.
   * Use {@link #unpackBucketIdx(long)} and {@link #unpackLocalId(long)} to extract.
   */
  public long getBucketIdxAndLocalId(final int globalId) {
    final int naturalId = newToOld != null ? newToOld[globalId] : globalId;
    final int bucketIdx = bucketIdxForNaturalId(naturalId);
    return ((long) bucketIdx << 32) | (naturalId - bucketBase[bucketIdx]);
  }

  /** Extracts the bucket index from the packed result of {@link #getBucketIdxAndLocalId(int)}. */
  public static int unpackBucketIdx(final long packed) {
    return (int) (packed >>> 32);
  }

  /** Extracts the local ID from the packed result of {@link #getBucketIdxAndLocalId(int)}. */
  public static int unpackLocalId(final long packed) {
    return (int) packed;
  }

  private int bucketIdxForNaturalId(final int naturalId) {
    int lo = 0, hi = numBuckets - 1;
    while (lo < hi) {
      final int mid = (lo + hi + 1) >>> 1;
      if (bucketBase[mid] <= naturalId)
        lo = mid;
      else
        hi = mid - 1;
    }
    return lo;
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
    if (bucketId < 0 || bucketId >= bucketIdToIdx.length)
      return -1;
    return bucketIdToIdx[bucketId];
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
   * If BFS reordering is applied, translates through the permutation first.
   */
  public RID getRID(final BasicDatabase database, final int globalId) {
    final int naturalId = newToOld != null ? newToOld[globalId] : globalId;
    final int bucketIdx = bucketIdxForNaturalId(naturalId);
    final int localId = naturalId - bucketBase[bucketIdx];
    return new RID(database, bucketIds[bucketIdx], positions[bucketIdx][localId]);
  }

  /**
   * Returns the vertex type name for a given global dense ID.
   * If BFS reordering is applied, translates through the permutation first.
   */
  public String getTypeName(final int globalId) {
    final int naturalId = newToOld != null ? newToOld[globalId] : globalId;
    return bucketTypeNames[bucketIdxForNaturalId(naturalId)];
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
   * Applies a BFS-based vertex reordering for improved cache locality.
   * After this call, all global IDs returned by this mapping are BFS-ordered.
   *
   * @param oldToNewMapping naturalId → reorderedId permutation
   */
  public void applyReordering(final int[] oldToNewMapping) {
    this.oldToNew = oldToNewMapping;
    this.newToOld = new int[oldToNewMapping.length];
    for (int i = 0; i < oldToNewMapping.length; i++)
      newToOld[oldToNewMapping[i]] = i;
  }

  /**
   * Returns true if BFS vertex reordering has been applied.
   */
  public boolean isReordered() {
    return oldToNew != null;
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
    // bucketIdToIdx int[] lookup table
    bytes += (long) bucketIdToIdx.length * Integer.BYTES;
    // bucketTypeNames: reference array + rough estimate for String objects
    bytes += (long) numBuckets * 8; // references
    // BFS reordering permutation arrays
    if (oldToNew != null)
      bytes += (long) oldToNew.length * Integer.BYTES;
    if (newToOld != null)
      bytes += (long) newToOld.length * Integer.BYTES;
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
