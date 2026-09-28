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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.TimeoutException;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;

/**
 * One unit of the load of the records an index range matched, in physical order (issue #8333): a slice of the sorted
 * positions of one bucket, or the entries of the range that are not record addresses. The positions are read through
 * the bucket, {@link LocalBucket#iterator(long[], int, int)}, which reads every page once and builds its records in
 * batches as a scan does, rather than looking them up one by one.
 * <p>
 * A worker of a {@link ParallelTypeScan} runs a unit as it is, through its own context, so the records are loaded and
 * deserialized on the workers; a sequential load runs the units one after the other on the thread of the query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PhysicalOrderSliceStep extends AbstractExecutionStep {
  private final int                                       bucketId;
  private final long[]                                    positions;
  private final int                                       from;
  private final int                                       to;
  private final List<Object>                              entries;
  // Turns an entry that is not a record address into its row, or null to skip it
  private final BiFunction<Object, CommandContext, Result> entryLoader;
  private       Iterator<Record>                          records;
  private       int                                       nextEntry;

  private PhysicalOrderSliceStep(final CommandContext context, final int bucketId, final long[] positions, final int from, final int to,
      final List<Object> entries, final BiFunction<Object, CommandContext, Result> entryLoader) {
    super(context);
    this.bucketId = bucketId;
    this.positions = positions;
    this.from = from;
    this.to = to;
    this.entries = entries;
    this.entryLoader = entryLoader;
  }

  /** The records at {@code positions[from, to)}, sorted ascending, of the bucket {@code bucketId}. */
  static PhysicalOrderSliceStep ofPositions(final CommandContext context, final int bucketId, final long[] positions, final int from,
      final int to) {
    return new PhysicalOrderSliceStep(context, bucketId, positions, from, to, null, null);
  }

  /** The entries of a range that are not record addresses, served as they came. */
  static PhysicalOrderSliceStep ofEntries(final CommandContext context, final List<Object> entries,
      final BiFunction<Object, CommandContext, Result> entryLoader) {
    return new PhysicalOrderSliceStep(context, -1, null, 0, entries.size(), entries, entryLoader);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (entries == null && records == null) {
      // A bucket dropped since the index was read holds none of the records any more
      final Bucket bucket = context.getDatabase().getSchema().getBucketByIdIfExists(bucketId);
      records = bucket instanceof LocalBucket localBucket ? localBucket.iterator(positions, from, to) : null;
    }

    return new ResultSet() {
      Result nextItem = null;
      int    fetched  = 0;

      @Override
      public boolean hasNext() {
        if (nextItem == null && fetched < nRecords)
          nextItem = fetchNext(context);
        return nextItem != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }
    };
  }

  private Result fetchNext(final CommandContext context) {
    if (entries == null)
      return records != null && records.hasNext() ? new ResultInternal(records.next()) : null;

    while (nextEntry < to) {
      final Result result = entryLoader.apply(entries.get(nextEntry++), context);
      if (result != null)
        return result;
    }
    return null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ LOAD " + (entries != null ?
        entries.size() + " INDEX ENTRIES" :
        (to - from) + " RECORDS OF BUCKET " + bucketId + " IN PHYSICAL ORDER");
  }
}
