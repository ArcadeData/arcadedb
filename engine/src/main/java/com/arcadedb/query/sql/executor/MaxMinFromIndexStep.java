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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Efficiently retrieves MAX or MIN value from a RangeIndex.
 * For MAX: iterates in descending order and returns the first non-null key.
 * For MIN: iterates in ascending order and returns the first non-null key.
 * <p>
 * This is an optimization for queries like "SELECT MAX(indexed_field) FROM type"
 * that avoids a full table scan when an index is available.
 * <p>
 * The aggregate is over the records of the target type only. The index of a subtype is routinely the one declared on an ancestor,
 * which holds the entries of the ancestor and of every sibling subtype too, so the step reads the sub-indexes of the target's own
 * buckets, and skips an entry of any other bucket whatever index it is handed (issue #10748).
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3304">Issue #3304</a>
 */
public class MaxMinFromIndexStep extends AbstractExecutionStep {

  private final RangeIndex index;
  private final String     alias;
  private final boolean    max; // true for MAX, false for MIN
  /** The buckets of the target type, its subtypes included, sorted; null to read the whole index. */
  private final int[]      bucketIds;

  private boolean executed = false;

  /**
   * @param index     The range index to read from
   * @param alias     The alias for the result property
   * @param max       true for MAX, false for MIN
   * @param bucketIds the buckets of the target type, its subtypes included: the entries of any other bucket are not read. Null
   *                  to read the whole index
   * @param context   The command context
   */
  public MaxMinFromIndexStep(final RangeIndex index, final String alias, final boolean max, final int[] bucketIds,
      final CommandContext context) {
    super(context);
    this.index = index;
    this.alias = alias;
    this.max = max;
    if (bucketIds != null) {
      this.bucketIds = bucketIds.clone();
      Arrays.sort(this.bucketIds);
    } else
      this.bucketIds = null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed)
          throw new NoSuchElementException();

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          // Reading the index's own key range directly never loads a record through its bucket, so apply the same
          // per-type read check a normal record load would go through.
          SecurityHelper.checkAccessOnIndex((DatabaseInternal) context.getDatabase(), index, SecurityDatabaseUser.ACCESS.READ_RECORD);

          Object resultValue = null;

          // For MAX: iterate descending (false), for MIN: iterate ascending (true)
          // The first non-null key of a record of the target is the result
          final IndexCursor cursor = openCursor();

          try {
            while (cursor != null && cursor.hasNext()) {
              // #5635: the cursor answers hasNext() exactly, so this entry is live and the key it leaves behind is a
              // key that still has a record. While hasNext() was optimistic, a fully tombstoned end of the key space
              // let the loop consume a null and read the DELETED key it had stepped over.
              final Identifiable entry = cursor.next(); // advance to get the key
              if (bucketIds != null && Arrays.binarySearch(bucketIds, entry.getIdentity().getBucketId()) < 0)
                // a record of another type than the target, which an ancestor's index holds (issue #10748)
                continue;
              final Object key = cursor.getKeys();

              if (key != null) {
                // Handle single-key vs multi-key indexes
                if (key instanceof final Object[] keys) {
                  if (keys.length > 0 && keys[0] != null) {
                    resultValue = keys[0];
                    break;
                  }
                } else {
                  resultValue = key;
                  break;
                }
              }
            }
          } finally {
            // MIN/MAX stops on the FIRST entry, so the scan is abandoned by design: a compacted-series cursor left
            // registered here would keep a retired file alive until the next database restart.
            if (cursor != null)
              cursor.close();
          }

          executed = true;
          final ResultInternal result = new ResultInternal(context.getDatabase());
          result.setProperty(alias, resultValue);
          return result;

        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public void reset() {
        MaxMinFromIndexStep.this.reset();
      }
    };
  }

  /**
   * The cursor over the target's entries: of a type index, the merge of the sub-indexes on the target's buckets only, so the entries
   * of a sibling type are not even walked. Null when the index has no sub-index on any of them: the target holds no entry.
   */
  private IndexCursor openCursor() {
    if (bucketIds == null || !(index instanceof TypeIndex typeIndex))
      return index.iterator(!max);
    final List<IndexInternal> onTargetBuckets = new ArrayList<>();
    for (final IndexInternal subIndex : typeIndex.getIndexesOnBuckets())
      if (Arrays.binarySearch(bucketIds, subIndex.getAssociatedBucketId()) >= 0)
        onTargetBuckets.add(subIndex);
    return onTargetBuckets.isEmpty() ? null : new MultiIndexCursor(onTargetBuckets, !max, -1);
  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ " + (max ? "MAX" : "MIN") + " FROM INDEX " + index.getName();
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
