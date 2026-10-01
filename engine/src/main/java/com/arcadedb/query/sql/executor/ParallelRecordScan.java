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
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * A parallel scan of a type for a caller that is not the SQL planner (issue #8725): the openCypher label scan evaluates
 * its filter in the workers of the same {@link ParallelTypeScan} a SQL scan with a WHERE clause uses, so the two languages
 * share the units, the workers, the pool, the sequential row order and the rules for when a scan stays sequential.
 * <p>
 * The caller supplies a {@link RowMapper} that turns a scanned record into its row, or rejects it. It runs on several
 * threads at once, each with a context of its own, so it must not keep state between calls.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelRecordScan {
  private final ParallelTypeScan scan;

  /** Turns a scanned record into the row to hand on, or {@code null} to drop it. Called on the workers, concurrently. */
  @FunctionalInterface
  public interface RowMapper {
    Result map(Record record, CommandContext workerContext);
  }

  /** Consumes, in a worker, the rows of the units that worker scans. */
  @FunctionalInterface
  public interface PartialSink<P> {
    /**
     * @param position where the row is in the sequential scan: positions compare as the sequential scan orders the rows
     */
    void accept(P partial, Result row, long position, CommandContext workerContext);
  }

  private ParallelRecordScan(final ParallelTypeScan scan) {
    this.scan = scan;
  }

  /**
   * Plans a parallel scan of every bucket of the type, or returns {@code null} when it must stay sequential: parallel
   * scans are disabled, the caller is in a transaction, on an async or scan thread, or there is too little to share.
   */
  public static ParallelRecordScan plan(final CommandContext context, final String typeName, final RowMapper mapper) {
    final DocumentType type = context.getDatabase().getSchema().getType(typeName);
    final int[] bucketIds = type.getBuckets(true).stream().mapToInt(Bucket::getFileId).distinct().sorted().toArray();
    final List<ExecutionStep> steps = new ArrayList<>(bucketIds.length);
    for (final int bucketId : bucketIds)
      if (bucketId > 0) // as the SQL planner's scan steps do
        steps.add(new MappedScanStep(bucketId, mapper, context));

    final ParallelTypeScan scan = ParallelTypeScan.plan(context, typeName, steps);
    return scan == null ? null : new ParallelRecordScan(scan);
  }

  /** The rows, in the order a sequential scan would return them. Closing the result set stops the workers. */
  public ResultSet pull(final CommandContext context) {
    return scan.pull(context, Integer.MAX_VALUE);
  }

  /**
   * Runs every row of the scan, in the workers, through a partial state each worker creates with {@code partialFactory}
   * in its own context, and returns the partials - at most one per worker, the caller being one of them - once every row
   * has gone through one (issue #8797). The caller merges them. Blocks; a worker's failure cancels the others and is
   * rethrown. Takes the place of {@link #pull}: a scan is either pulled or aggregated.
   *
   * @param onWait called on the caller's thread after every batch it scans and about every 50ms while it waits, e.g. to
   *               enforce a timeout
   */
  public <P> List<P> aggregate(final CommandContext context, final Function<CommandContext, P> partialFactory,
      final PartialSink<P> sink, final Runnable onWait) {
    return scan.aggregate(context, partialFactory, sink::accept, onWait);
  }

  /**
   * Runs independent CPU-bound tasks - the merge of partial aggregations - on the caller and, when {@code inParallel}, on
   * the producer pool too, and returns once all are done.
   */
  public void run(final List<Runnable> tasks, final boolean inParallel) {
    scan.run(tasks, inParallel);
  }

  /** How many workers the scan runs, the caller included. */
  public int getWorkerCount() {
    return scan.getWorkerCount();
  }

  static void warnSkipped(final int bucketId, final long skipped) {
    LogManager.instance().log(ParallelRecordScan.class, Level.WARNING,
        "Scan of bucket %d skipped %d record(s) that could not be read (corrupted or a broken multi-page chain); "
            + "the result may be incomplete, run CHECK DATABASE to investigate", bucketId, skipped);
  }
}
