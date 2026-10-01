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

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;

import java.util.ArrayList;
import java.util.List;

/**
 * A barrier that reads every record address of the previous step BEFORE the first one is passed downstream, then serves
 * the records, loaded by their address, in physical order (issue #8814, the Halloween problem).
 * <p>
 * An {@code UPDATE} that rewrites the key it searched by must not walk the index range it is changing: the new keys land
 * ahead of the cursor and the statement meets its own output, updating the same record again until it leaves the range
 * (and never finishing, on a range that is not bounded by the data). Reading the addresses first makes the set of records
 * to update the one the statement saw when it started, whatever the plan underneath. Only the addresses are held - one
 * {@code long} per record, in the primitive arrays of {@link PhysicalOrderRidBuffer} - never the records.
 * <p>
 * A record that was updated or deleted in the meantime is loaded as it is now: the WHERE was evaluated on the state the
 * statement started from, and a record changed by an earlier row of the same statement is still one the statement owes an
 * update. A record deleted since is skipped. Each address is served once, however many index entries returned it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MaterializeRecordsStep extends AbstractExecutionStep {
  private record Slice(int bucketId, long[] positions, int from, int to) {
  }

  private final int         limit;
  private       List<Slice> slices;
  private       int         nextSlice;
  private       int         nextInSlice;
  private       long        previousPosition = -1L;
  private       boolean     drained;

  /**
   * @param limit the most records the statement can update, or 0 for no bound: once that many addresses are held the
   *              rest of the source is not read
   */
  public MaterializeRecordsStep(final CommandContext context, final int limit) {
    super(context);
    this.limit = limit;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    if (!drained) {
      drained = true;
      drain(context, prevStep);
    }

    final Database database = context.getDatabase();
    final InternalResultSet result = new InternalResultSet();
    int served = 0;
    while (served < nRecords && nextSlice < slices.size()) {
      final Slice slice = slices.get(nextSlice);
      if (slice.from() + nextInSlice >= slice.to()) {
        ++nextSlice;
        nextInSlice = 0;
        previousPosition = -1L;
        continue;
      }
      final long position = slice.positions()[slice.from() + nextInSlice++];
      // The same address can come from more than one index entry: serve it once. Positions are sorted inside a bucket,
      // so the duplicates are adjacent
      if (position == previousPosition)
        continue;
      previousPosition = position;
      try {
        final Record record = database.newRID(slice.bucketId(), position).getRecord();
        result.add(new ResultInternal(record));
        ++served;
      } catch (final RecordNotFoundException e) {
        // DELETED SINCE THE ADDRESS WAS READ: NOTHING TO UPDATE
      }
    }
    return result;
  }

  private void drain(final CommandContext context, final ExecutionStepInternal prevStep) {
    final PhysicalOrderRidBuffer buffer = new PhysicalOrderRidBuffer();
    while (limit <= 0 || buffer.size() < limit) {
      final ResultSet upstream = prevStep.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
      if (!upstream.hasNext())
        break;

      while (upstream.hasNext() && (limit <= 0 || buffer.size() < limit)) {
        final Result item = upstream.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final RID rid = ridOf(item);
          if (rid != null && rid.getBucketId() >= 0 && rid.getPosition() >= 0)
            buffer.add(rid.getBucketId(), rid.getPosition());
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }
    }

    // The upstream steps are done: free the index cursors now, before the first record is touched
    prevStep.close();

    buffer.sort();
    slices = new ArrayList<>();
    // The slices share the buffer's arrays, which stay reachable from them: one long per record, no copy
    buffer.slices(Integer.MAX_VALUE, (bucketId, positions, from, to) -> slices.add(new Slice(bucketId, positions, from, to)));
  }

  private static RID ridOf(final Result item) {
    if (item.isElement())
      return item.getIdentity().orElse(null);
    final Object ridValue = item.getProperty("@rid");
    return ridValue instanceof Identifiable identifiable ? identifiable.getIdentity() : null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder(ExecutionStepInternal.getIndent(depth, indent)).append("+ MATERIALIZE RECORD ADDRESSES");
    if (limit > 0)
      result.append(" (up to ").append(limit).append(")");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    return result.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new MaterializeRecordsStep(context, limit);
  }
}
