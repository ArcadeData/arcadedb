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

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.ContinuousAggregateRefresher;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code DELETE FROM <timeseries type> WHERE <time range> AND <tag> = <value> ...} (issue #10342): removes the samples the WHERE names
 * from the engine of the type and answers one row, {@code {count: <samples removed>}}, as a DELETE of records does.
 * <p>
 * The statement does not read the samples it removes: the planner has already proved that the time range and the tag filter are the
 * WHERE, exactly ({@link SelectExecutionPlanner#timeSeriesDeleteScope}), and the engine applies them on the block directory first, so
 * deleting a range of old data opens no block it drops whole.
 * <p>
 * <b>It is not part of the transaction the statement runs in</b>, like the append of {@code INSERT} (issue #7410): the samples are
 * gone, durably, when the statement returns, and a {@code ROLLBACK} does not bring them back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DeleteFromTimeSeriesStep extends AbstractExecutionStep {
  private final LocalTimeSeriesType tsType;
  private final long                fromTs;
  private final long                toTs;
  private final TagFilter           tagFilter;
  private       boolean             executed = false;

  public DeleteFromTimeSeriesStep(final LocalTimeSeriesType tsType, final long fromTs, final long toTs, final TagFilter tagFilter,
      final CommandContext context) {
    super(context);
    this.tsType = tsType;
    this.fromTs = fromTs;
    this.toTs = toTs;
    this.tagFilter = tagFilter;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new InternalResultSet();
    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final TimeSeriesEngine engine = tsType.requireEngine(SecurityDatabaseUser.ACCESS.DELETE_RECORD);
      final long deleted;
      try {
        deleted = engine.deleteSamples(fromTs, toTs, tagFilter);
      } catch (final IOException e) {
        // the shards before the one that failed are deleted: say so, and that running the statement again finishes the job
        throw new CommandExecutionException("Error deleting samples from the TIMESERIES type '" + tsType.getName() + "': some of the "
            + "samples may already be deleted, and running the statement again deletes the rest", e);
      }

      // the engine told the continuous aggregates over the type which range changed (issue #10428); refresh them as an INSERT does.
      // A delete that failed part way threw above and refreshes nothing: the range is recorded all the same, and the next refresh
      // (the next INSERT triggers one) recomputes it
      if (deleted > 0L)
        ContinuousAggregateRefresher.refreshAggregatesOver(context.getDatabase(), tsType.getName());

      final ResultInternal row = new ResultInternal(context.getDatabase());
      row.setProperty("count", deleted);
      // so a caller can tell, as the result of TRUNCATE TYPE does, that its ROLLBACK will not put the samples back
      row.setProperty("transactional", false);
      final InternalResultSet result = new InternalResultSet();
      result.add(row);
      return result;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder sb = new StringBuilder();
    sb.append(ExecutionStepInternal.getIndent(depth, indent)).append("+ DELETE FROM TIMESERIES ").append(tsType.getName());
    sb.append(" [").append(fromTs).append(" - ").append(toTs).append("]");
    if (tagFilter != null) {
      final List<String> names = new ArrayList<>();
      for (final ColumnDefinition column : tsType.getTsColumns())
        if (column.getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
          names.add(column.getName());
      sb.append(" TAGS ").append(tagFilter.describe(names.toArray(new String[0])));
    }
    if (context.isProfiling())
      sb.append(" (").append(getCostFormatted()).append(")");
    return sb.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new DeleteFromTimeSeriesStep(tsType, fromTs, toTs, tagFilter, context);
  }
}
