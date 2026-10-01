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

import com.arcadedb.database.Record;
import com.arcadedb.engine.BucketIterator;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.TimeoutException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Scans one bucket (or a range of its pages) and hands every record to a {@link ParallelRecordScan.RowMapper}, which
 * turns it into the row the caller wants or rejects it. The unit of work of a {@link ParallelRecordScan}: what
 * {@link ScanWithFilterStep} is for a SQL WHERE clause, for a caller whose filter and row shape are not SQL's
 * (issue #8725).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MappedScanStep extends AbstractExecutionStep {
  private final int                          bucketId;
  private final ParallelRecordScan.RowMapper mapper;
  // The page range [fromPage, toPage) a parallel scan assigned to this step, or -1/-1 for the whole bucket
  private       int                          fromPage = -1;
  private       int                          toPage   = -1;
  private       boolean                      warnedAboutSkippedRecords;

  private Iterator<Record> iterator;

  MappedScanStep(final int bucketId, final ParallelRecordScan.RowMapper mapper, final CommandContext context) {
    super(context);
    this.bucketId = bucketId;
    this.mapper = mapper;
  }

  int getBucketId() {
    return bucketId;
  }

  void setPageRange(final int fromPage, final int toPage) {
    this.fromPage = fromPage;
    this.toPage = toPage;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // A filter that rejects every record scans the whole bucket inside one hasNext() (issue #6266)
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    if (iterator == null) {
      if (fromPage > -1)
        iterator = ((LocalBucket) context.getDatabase().getSchema().getBucketById(bucketId)).iterator(fromPage, toPage);
      else
        iterator = context.getDatabase().getSchema().getBucketById(bucketId).iterator();
    }

    return new ResultSet() {
      int    nFetched = 0;
      Result nextItem = null;

      private void fetchNextItem() {
        nextItem = null;
        while (iterator.hasNext()) {
          guard.check();
          final Result row = mapper.map(iterator.next(), context);
          if (row != null) {
            nextItem = row;
            return;
          }
        }
        if (!warnedAboutSkippedRecords && iterator instanceof BucketIterator bucketIterator
            && bucketIterator.getSkippedRecordCount() > 0) {
          warnedAboutSkippedRecords = true;
          ParallelRecordScan.warnSkipped(bucketId, bucketIterator.getSkippedRecordCount());
        }
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
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        nFetched++;
        return result;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ SCAN BUCKET " + bucketId;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new MappedScanStep(bucketId, mapper, context);
  }
}
