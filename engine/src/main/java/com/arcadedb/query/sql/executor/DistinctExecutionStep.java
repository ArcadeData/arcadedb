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

import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.Type;

import java.util.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Created by luigidellaquila on 08/07/16.
 *
 * Optimized to store only distinct field values instead of full Result objects to reduce memory footprint.
 */
public class DistinctExecutionStep extends AbstractExecutionStep {
  // A DistinctKey, and the entry of the set that holds it
  private static final int DISTINCT_KEY_OVERHEAD_BYTES = HeapEstimator.HASH_ENTRY_BYTES + 24;

  final Set<DistinctKey> pastItems = new HashSet<>();
  final RidSet           pastRids;
  ResultSet lastResult = null;
  Result    nextValue;
  // THE KEYS REMEMBERED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591). THE
  // RIDS OF THE FAST PATH ARE NOT ELEMENTS UNDER THE CAP - A BITMAP TAKES A BIT PER RECORD POSITION - BUT THE BITMAP
  // GROWS TO THE HIGHEST POSITION OF EACH BUCKET, SO WHAT IT TAKES IS CHARGED TO THE BUDGET
  private final OperationHeapLimit heapLimit;

  public DistinctExecutionStep(final CommandContext context) {
    super(context);
    this.pastRids = new RidSet(context);
    heapLimit = OperationHeapLimit.of(context, "DISTINCT");
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {

    return new ResultSet() {
      int nextLocal = 0;

      @Override
      public boolean hasNext() {
        if (nextLocal >= nRecords) {
          return false;
        }
        if (nextValue != null) {
          return true;
        }
        fetchNext(nRecords);
        return nextValue != null;
      }

      @Override
      public Result next() {
        if (nextLocal >= nRecords) {
          throw new NoSuchElementException();
        }
        if (nextValue == null) {
          fetchNext(nRecords);
        }
        if (nextValue == null) {
          throw new NoSuchElementException();
        }
        final Result result1 = nextValue;
        nextValue = null;
        nextLocal++;
        return result1;
      }

    };
  }

  private void fetchNext(final int nRecords) {
    while (true) {
      if (nextValue != null) {
        return;
      }
      if (lastResult == null || !lastResult.hasNext()) {
        lastResult = getPrev().syncPull(context, nRecords);
      }
      if (lastResult == null || !lastResult.hasNext()) {
        // EVERY INPUT ROW WAS SEEN: NO KEY IS NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
        releaseBuffer();
        return;
      }
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        nextValue = lastResult.next();
        if (alreadyVisited(nextValue)) {
          nextValue = null;
        } else {
          markAsVisited(nextValue);
        }
      } finally {
        if (context.isProfiling()) {
          cost += System.nanoTime() - begin;
        }
      }
    }
  }

  private void markAsVisited(final Result nextValue) {
    if (canUseRidFastPath(nextValue)) {
      final long bitmapBytes = pastRids.getAllocatedBytes();
      pastRids.add(nextValue.getElement().get().getIdentity());
      final long grown = pastRids.getAllocatedBytes() - bitmapBytes;
      if (grown > 0) {
        try {
          heapLimit.charge(grown);
        } catch (final RuntimeException e) {
          releaseBuffer();
          throw e;
        }
      }
      return;
    }
    // Store only the property values, not the full Result object
    final DistinctKey key = new DistinctKey(nextValue);
    pastItems.add(key);
    try {
      heapLimit.add(pastItems.size(), key.properties, DISTINCT_KEY_OVERHEAD_BYTES);
    } catch (final RuntimeException e) {
      releaseBuffer();
      throw e;
    }
  }

  private void releaseBuffer() {
    pastItems.clear();
    pastRids.clear();
    heapLimit.release();
  }

  private boolean alreadyVisited(final Result nextValue) {
    if (canUseRidFastPath(nextValue))
      return pastRids.contains(nextValue.getElement().get().getIdentity());
    // Check using only the property values
    return pastItems.contains(new DistinctKey(nextValue));
  }

  /**
   * The RID-based fast path deduplicates by record identity instead of by the projected row, which
   * is a memory optimization that is only valid when identity still uniquely determines the output.
   * It is valid for bare element wrappers (whole-record selects and internal index dedup) and for
   * projected rows that still expose {@code @rid}. When a projection has reshaped the row and dropped
   * the identity (e.g. {@code SELECT DISTINCT *, !@rid}), two different records can collapse to the
   * same output and must be compared by value, so we fall back to the property-based {@link DistinctKey}.
   */
  private boolean canUseRidFastPath(final Result value) {
    if (!value.isElement())
      return false;
    final RID identity = value.getElement().get().getIdentity();
    if (identity.getBucketId() < 0 || identity.getPosition() < 0)
      return false;
    if (value instanceof ResultInternal ri && ri.hasProjectedProperties())
      return value.getPropertyNames().contains(RID_PROPERTY);
    return true;
  }

  @Override
  public void sendTimeout() {
    // DO NOT PROPAGATE TIMEOUT
  }

  @Override
  public void close() {
    releaseBuffer();
    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ DISTINCT";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  /**
   * Lightweight wrapper that stores only the property values from a Result for DISTINCT comparison.
   * This dramatically reduces memory usage compared to storing full Result objects.
   */
  private static class DistinctKey {
    private final Map<String, Object> properties;
    private final int hashCode;

    DistinctKey(final Result result) {
      // Extract only the properties (not the element reference, metadata, etc.), normalising numeric values to a
      // canonical form so that the same logical number represented with different boxed numeric types (e.g.
      // Integer(1) vs Long(1)) is deduplicated as one value instead of splitting into separate rows (issue #6676),
      // matching the canonicalization SQL GROUP BY already applies (AggregateProjectionCalculationStep.GroupByKey).
      final Set<String> propertyNames = result.getPropertyNames();
      this.properties = new HashMap<>(propertyNames.size());
      for (final String propName : propertyNames) {
        this.properties.put(propName, Type.normalizeNumberForKey(result.getProperty(propName)));
      }
      // Pre-compute hashCode for performance
      this.hashCode = properties.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other)
        return true;
      if (!(other instanceof DistinctKey))
        return false;
      return this.properties.equals(((DistinctKey) other).properties);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
