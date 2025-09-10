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
package com.arcadedb.index.lsm.compaction;

import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.lsm.LSMTreeIndexUnderlyingPageCursor;
import com.arcadedb.serializer.BinaryComparator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Processes key merging during LSM-tree index compaction.
 * <p>
 * This class is responsible for finding the minimum key across multiple page iterators,
 * merging duplicate keys from different pages, and collecting all RIDs associated
 * with merged keys. It handles the complex logic of comparing keys across pages
 * and advancing iterators appropriately.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class KeyMergeProcessor {
  private final BinaryComparator comparator;
  private final byte[]           keyTypes;
  private final Set<RID>         rids = new LinkedHashSet<>();

  /**
   * Creates a new KeyMergeProcessor with the specified comparator and key types.
   *
   * @param comparator the binary comparator for key comparison
   * @param keyTypes   the byte array describing key types
   */
  public KeyMergeProcessor(BinaryComparator comparator, byte[] keyTypes) {
    this.comparator = comparator;
    this.keyTypes = keyTypes;
  }

  /**
   * Result of finding the minimum key across iterators.
   *
   * @param minorKey        the minimum key found, or null if no keys available
   * @param iteratorIndexes list of iterator indexes that contain this minimum key
   * @param hasMoreItems    true if there are more items to process
   */
  public record MinorKeyResult(Object[] minorKey, List<Integer> iteratorIndexes, boolean hasMoreItems) {

    /**
     * Creates a result indicating no more items are available.
     *
     * @return a MinorKeyResult with no items
     */
    public static MinorKeyResult noMoreItems() {
      return new MinorKeyResult(null, new ArrayList<>(), false);
    }

    /**
     * Returns true if a valid minor key was found.
     *
     * @return true if minor key is not null
     */
    public boolean hasMinorKey() {
      return minorKey != null;
    }
  }

  /**
   * Result of merging keys and collecting RIDs.
   *
   * @param rids    array of RIDs collected from all matching keys
   * @param isEmpty true if no RIDs were collected
   */
  public record MergeResult(RID[] rids, boolean isEmpty) {

    /**
     * Creates an empty merge result.
     *
     * @return a MergeResult with no RIDs
     */
    public static MergeResult empty() {
      return new MergeResult(new RID[0], true);
    }

    /**
     * Creates a merge result with the specified RIDs.
     *
     * @param rids the RIDs array
     *
     * @return a MergeResult containing the RIDs
     */
    public static MergeResult of(RID[] rids) {
      return new MergeResult(rids, rids.length == 0);
    }
  }

  /**
   * Finds the minimum key across all active iterators.
   * This method compares keys from all iterators and identifies which iterators
   * contain the minimum key value.
   *
   * @param keys array of current keys from each iterator (null if iterator exhausted)
   *
   * @return MinorKeyResult containing the minimum key and associated iterator indexes
   */
  public MinorKeyResult findMinorKey(Object[][] keys) {
    Object[] minorKey = null;
    final List<Integer> minorKeyIndexes = new ArrayList<>();
    boolean moreItems = false;

    // Find the minimum key across all iterators
    for (int p = 0; p < keys.length; ++p) {
      if (keys[p] == null) {
        continue; // Iterator exhausted
      }

      moreItems = true;

      if (minorKey == null) {
        // First valid key found
        minorKey = keys[p];
        minorKeyIndexes.add(p);
      } else {
        // Compare with current minimum
        final int cmp = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[p], minorKey);
        if (cmp == 0) {
          // Equal key - add to list of matching iterators
          minorKeyIndexes.add(p);
        } else if (cmp < 0) {
          // New minimum found
          minorKey = keys[p];
          minorKeyIndexes.clear();
          minorKeyIndexes.add(p);
        }
        // cmp > 0: current minorKey is still minimum, continue
      }
    }

    return new MinorKeyResult(minorKey, minorKeyIndexes, moreItems);
  }

  /**
   * Merges all values for the specified key from the given iterators.
   * This method advances each iterator that contains the minimum key,
   * collecting all RIDs and handling consecutive entries with the same key.
   *
   * @param minorKey        the key to merge
   * @param minorKeyIndexes indexes of iterators containing this key
   * @param iterators       array of all page iterators
   * @param keys            array to update with new keys after advancement
   * @param metrics         metrics to update during processing
   *
   * @return MergeResult containing all RIDs for the merged key
   */
  public MergeResult mergeKey(Object[] minorKey, List<Integer> minorKeyIndexes,
      LSMTreeIndexUnderlyingPageCursor[] iterators,
      Object[][] keys, CompactionMetrics metrics) {
    rids.clear();
    int totalNewValues = 0;

    for (int i = 0; i < minorKeyIndexes.size(); ++i) {
      final int idx = minorKeyIndexes.get(i);
      final LSMTreeIndexUnderlyingPageCursor iter = iterators[idx];

      if (iter == null) {
        continue;
      }

      // Process all consecutive entries with the same key from this iterator
      while (true) {
        final Object[] value = iter.getValue();
        if (value != null) {
          // Not deleted - collect all RIDs
          for (int r = 0; r < value.length; ++r) {
            final RID rid = (RID) value[r];
            // Add all RIDs, including removed ones for later cleanup during 2nd level compaction
            rids.add(rid);
            totalNewValues++;
          }
        }

        // Check if the next element has the same key
        if (iter.hasNext()) {
          iter.next();
          keys[idx] = iter.getKeys();

          // If next key is different, break and keep it for next iteration
          if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[idx], minorKey) != 0) {
            break;
          }

          // Same key continues - increment merged keys counter
          metrics.incrementTotalMergedKeys();
        } else {
          // Iterator exhausted
          iterators[idx].close();
          iterators[idx] = null;
          keys[idx] = null;
          break;
        }
      }
    }

    // Update metrics with total values processed for this key (not accumulated set size)
    if (totalNewValues > 0) {
      metrics.addTotalMergedValues(totalNewValues);
    }

    if (rids.isEmpty()) {
      return MergeResult.empty();
    }

    final RID[] ridsArray = new RID[rids.size()];
    rids.toArray(ridsArray);

    return MergeResult.of(ridsArray);
  }

  /**
   * Returns the number of unique RIDs currently held in the internal buffer.
   * This is useful for monitoring memory usage during merging.
   *
   * @return the number of unique RIDs in the buffer
   */
  public int getCurrentRidCount() {
    return rids.size();
  }

  /**
   * Clears the internal RID buffer to free memory.
   * This method is automatically called during merge operations but can be
   * called manually if needed for memory management.
   */
  public void clearRidBuffer() {
    rids.clear();
  }
}
