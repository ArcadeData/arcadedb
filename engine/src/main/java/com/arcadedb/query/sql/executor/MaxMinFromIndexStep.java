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
 * Efficiently retrieves MAX or MIN values from range indexes, one per projected aggregate.
 * For MAX: iterates in descending order and returns the first non-null key.
 * For MIN: iterates in ascending order and returns the first non-null key.
 * <p>
 * This is an optimization for queries like "SELECT MAX(indexed_field) FROM type"
 * that avoids a full table scan when an index is available. Several aggregates in one statement
 * ({@code SELECT min(k), max(k), count(*) FROM T}) are each read on their own and assembled into the one row (issue #12284); a
 * {@code count(*)} part counts the type as {@link CountFromTypeStep} does.
 * <p>
 * The aggregate is over the records of the target type only. The index of a subtype is routinely the one declared on an ancestor,
 * which holds the entries of the ancestor and of every sibling subtype too, so the step reads the sub-indexes of the target's own
 * buckets, and skips an entry of any other bucket whatever index it is handed (issue #10748).
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3304">Issue #3304</a>
 */
public class MaxMinFromIndexStep extends AbstractExecutionStep {

  /**
   * One projected aggregate: a MIN or a MAX read from {@code index}, or, with no index, the {@code count(*)} of the target type.
   *
   * @param alias the property of the result row it fills
   * @param index the range index to read from; null for a count
   * @param max   true for MAX, false for MIN; ignored for a count
   */
  public record Part(String alias, RangeIndex index, boolean max) {
    public static Part count(final String alias) {
      return new Part(alias, null, false);
    }

    boolean isCount() {
      return index == null;
    }
  }

  private final Part[] parts;
  /** The target type, what a count part counts. */
  private final String typeName;
  /** The buckets of the target type, its subtypes included, sorted; null to read the whole index. */
  private final int[]  bucketIds;

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
    this(List.of(new Part(alias, index, max)), null, bucketIds, context);
  }

  /**
   * @param parts     the aggregates of the row, in projection order
   * @param typeName  the target type, needed when a part is a count
   * @param bucketIds the buckets of the target type, its subtypes included: the entries of any other bucket are not read. Null
   *                  to read the whole index
   */
  public MaxMinFromIndexStep(final List<Part> parts, final String typeName, final int[] bucketIds, final CommandContext context) {
    super(context);
    this.parts = parts.toArray(new Part[0]);
    this.typeName = typeName;
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
          final ResultInternal result = new ResultInternal(context.getDatabase());
          for (final Part part : parts)
            result.setProperty(part.alias(), part.isCount() ? CountFromTypeStep.countOf(context, typeName) : firstKey(context, part));
          executed = true;
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

  /** The first non-null key of a record of the target, in the order of the part: its MIN or its MAX. */
  private Object firstKey(final CommandContext context, final Part part) {
    final RangeIndex index = part.index();
    // Reading the index's own key range directly never loads a record through its bucket, so apply the same
    // per-type read check a normal record load would go through.
    SecurityHelper.checkAccessOnIndex((DatabaseInternal) context.getDatabase(), index, SecurityDatabaseUser.ACCESS.READ_RECORD);
    // For MAX: iterate descending (false), for MIN: iterate ascending (true)
    // The first non-null key of a record of the target is the result
    final IndexCursor cursor = openCursor(index, part.max());
    try {
      while (cursor != null && cursor.hasNext()) {
        // #5635: the cursor answers hasNext() exactly, so this entry is live and the key it leaves behind is a
        // key that still has a record. While hasNext() was optimistic, a fully tombstoned end of the key space
        // let the loop consume a null and read the DELETED key it had stepped over.
        final Identifiable entry = cursor.next(); // advance to get the key
        if (bucketIds != null && Arrays.binarySearch(bucketIds, entry.getIdentity().getBucketId()) < 0)
          // a record of another type than the target, which an ancestor's index holds (issue #10748). The cursor of a type
          // index never hands one, as it reads the target's sub-indexes only; the planner hands no other kind of index, so this
          // is what keeps the step exact for any index it is given rather than a filter the planner relies on
          continue;
        final Object key = cursor.getKeys();
        if (key != null) {
          // Handle single-key vs multi-key indexes
          if (key instanceof final Object[] keys) {
            if (keys.length > 0 && keys[0] != null)
              return keys[0];
          } else
            return key;
        }
      }
      return null;
    } finally {
      // MIN/MAX stops on the FIRST entry, so the scan is abandoned by design: a compacted-series cursor left
      // registered here would keep a retired file alive until the next database restart.
      if (cursor != null)
        cursor.close();
    }
  }

  /**
   * The cursor over the target's entries: of a type index, the merge of the sub-indexes on the target's buckets only, so the entries
   * of a sibling type are not even walked. Null when the index has no sub-index on any of them: the target holds no entry.
   */
  private IndexCursor openCursor(final RangeIndex index, final boolean max) {
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
    final StringBuilder result = new StringBuilder(spaces).append("+ ");
    for (int i = 0; i < parts.length; i++) {
      if (i > 0)
        result.append(", ");
      if (parts[i].isCount())
        result.append("COUNT FROM TYPE ").append(typeName);
      else
        result.append(parts[i].max() ? "MAX" : "MIN").append(" FROM INDEX ").append(parts[i].index().getName());
    }
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
