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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * {@code COMPACT TIMESERIES TYPE <name>} statement (issue #8574).
 * <p>
 * Seals the mutable tail of every shard of a TimeSeries type now, instead of at the next pass of the maintenance
 * scheduler, so a client-server user who has just loaded data can ask for the settled layout the same way
 * {@code COMPACT INDEX} asks it of an index. Runs the same {@link TimeSeriesEngine#compactAll()} and {@link TimeSeriesEngine#mergeSmallBlocks()} the scheduler runs,
 * so it takes the same locks and, under HA, the same leader-only replicated path.
 * <p>
 * The result reports the mutable samples left behind: compaction can legitimately leave some (rows appended while it
 * ran, or a shard the HA size valve declined this cycle), and a caller waiting for the settled state should read that
 * rather than assume it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CompactTimeSeriesTypeStatement extends DDLStatement {
  public Identifier name;

  public CompactTimeSeriesTypeStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    // Same gate as COMPACT INDEX: a maintenance operation that rewrites on-disk files. No-op with no bound user.
    final DatabaseInternal database = context.getDatabase();
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    final DocumentType type = database.getSchema().getType(name.getStringValue());
    if (!(type instanceof LocalTimeSeriesType tsType))
      throw new CommandExecutionException("Type '" + name.getStringValue() + "' is not a TimeSeries type");

    final TimeSeriesEngine engine = tsType.getEngine();
    if (engine == null)
      throw new CommandExecutionException("TimeSeries type '" + name.getStringValue() + "' has no engine");

    final long before;
    final long after;
    try {
      before = mutableSamples(engine);
      engine.compactAll();
      engine.mergeSmallBlocks();
      after = mutableSamples(engine);
    } catch (final IOException e) {
      throw new CommandExecutionException("Error on compacting TimeSeries type '" + name.getStringValue() + "'", e);
    }

    final ResultInternal result = new ResultInternal(database);
    result.setProperty("operation", "compact timeseries type");
    result.setProperty("typeName", name.getStringValue());
    result.setProperty("mutableSamplesBefore", before);
    result.setProperty("mutableSamples", after);
    return new InternalResultSet(result);
  }

  private static long mutableSamples(final TimeSeriesEngine engine) throws IOException {
    long total = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      total += engine.getShard(s).getMutableBucket().getSampleCount();
    return total;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("COMPACT TIMESERIES TYPE ");
    name.toString(params, builder);
  }

  @Override
  public CompactTimeSeriesTypeStatement copy() {
    final CompactTimeSeriesTypeStatement result = new CompactTimeSeriesTypeStatement();
    result.name = name == null ? null : name.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    return Objects.equals(name, ((CompactTimeSeriesTypeStatement) o).name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }
}
