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

import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.BucketIterator;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * A combined scan + filter step that evaluates the WHERE predicate immediately after loading each record,
 * avoiding the overhead of creating ResultInternal wrappers for records that don't match the filter.
 * <p>
 * A surviving record is handed downstream as a plain wrapper around the {@link ImmutableDocument}
 * it was read from, so every column is still deserialized one at a time, on demand, and only if something actually asks
 * for it. This step used to carry a second "column projection push-down" phase that pre-extracted the SELECT-listed
 * columns into the row instead; it was unreachable by construction and has been removed - see issue #5756 and the note
 * on {@code SelectExecutionPlanner#handleTypeAsTarget}.
 */
public class ScanWithFilterStep extends AbstractExecutionStep {

  private final int         bucketId;
  private final WhereClause whereClause;
  private       Object      order;
  private       long        totalFetched  = 0L;
  private       long        totalFiltered = 0L;
  private       boolean     warnedAboutSkippedRecords;

  private Iterator<Record> iterator;

  public ScanWithFilterStep(final int bucketId, final WhereClause whereClause, final CommandContext context) {
    super(context);
    this.bucketId = bucketId;
    this.whereClause = whereClause;
  }

  public void setOrder(final Object order) {
    this.order = order;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    // A filter that rejects every record scans the whole bucket inside one hasNext() (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        if (FetchFromClusterExecutionStep.ORDER_DESC == order)
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).inverseIterator();
        else
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).iterator();
      }

      return new ResultSet() {
        int nFetched = 0;
        Result nextItem = null;

        private void fetchNextItem() {
          nextItem = null;
          while (iterator.hasNext()) {
            guard.check();
            final Record record = iterator.next();
            totalFetched++;

            // Evaluate WHERE on the document itself: every field access is lazily deserialized on demand
            final ResultInternal candidate = new ResultInternal(record);

            // Set $current before WHERE evaluation so method calls (e.g. split()) resolve correctly
            context.setVariable("current", candidate);

            final long filterBegin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (whereClause.matchesFilters(candidate, context)) {
                nextItem = candidate;
                context.setVariable("current", nextItem);
                return;
              }
              totalFiltered++;
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - filterBegin;
            }
          }
          warnIfRecordsSkipped();
        }

        @Override
        public boolean hasNext() {
          if (nFetched >= nRecords)
            return false;
          if (nextItem == null)
            fetchNextItem();
          return nextItem != null;
        }

        @Override
        public Result next() {
          if (nFetched >= nRecords)
            throw new NoSuchElementException();
          if (nextItem == null)
            fetchNextItem();
          if (nextItem == null)
            throw new NoSuchElementException();

          final Result result = nextItem;
          nextItem = null;
          nFetched++;
          return result;
        }
      };
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Surfaces #6015's {@link BucketIterator#getSkippedRecordCount()} to a real caller: once the scan is exhausted,
   * log a one-time warning if any record was skipped as corrupted or a confirmed-broken multi-page chain, so a
   * truncated result does not silently look like "the bucket just has fewer records".
   */
  private void warnIfRecordsSkipped() {
    if (warnedAboutSkippedRecords || !(iterator instanceof final BucketIterator bucketIterator))
      return;

    final long skipped = bucketIterator.getSkippedRecordCount();
    if (skipped > 0) {
      warnedAboutSkippedRecords = true;
      LogManager.instance().log(this, Level.WARNING,
          "Scan of bucket %d skipped %d record(s) that could not be read (corrupted or a broken multi-page chain); "
              + "the result may be incomplete, run CHECK DATABASE to investigate", bucketId, skipped);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ SCAN WITH FILTER BUCKET " + bucketId
        + " (" + context.getDatabase().getSchema().getBucketById(bucketId).getName() + ")";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + whereClause;
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    final ScanWithFilterStep copy = new ScanWithFilterStep(this.bucketId, this.whereClause.copy(), context);
    copy.setOrder(this.order);
    return copy;
  }
}
