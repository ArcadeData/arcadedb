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
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL statement: ALTER TIMESERIES TYPE
 */
public class AlterTimeSeriesTypeStatement extends DDLStatement {

  public Identifier              name;
  public boolean                 addPolicy;
  public List<DownsamplingTier>  tiers = new ArrayList<>();
  /** Set by {@code ALTER TIMESERIES TYPE <name> PRECISION <unit>}, upper-cased; null for the downsampling forms. */
  public String                  precision;

  public AlterTimeSeriesTypeStatement() {
  }

  /**
   * Batchable into a DDL script's bulk schema scope (issue #6990). Altering a TimeSeries type rewrites its definition; samples are untouched.
   */
  @Override
  public boolean isBulkSchemaScopeSafe(final DatabaseInternal database) {
    return true;
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(name.getStringValue());
    if (!(type instanceof LocalTimeSeriesType tsType))
      throw new CommandExecutionException("Type '" + name.getStringValue() + "' is not a TimeSeries type");

    if (precision != null)
      return alterPrecision(context, tsType);

    if (addPolicy)
      tsType.setDownsamplingTiers(tiers);
    else
      tsType.setDownsamplingTiers(new ArrayList<>());
    // No save here: the setter saves, and replicates, the schema itself (issue #9747)

    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", addPolicy ? "add downsampling policy" : "drop downsampling policy");
    result.setProperty("typeName", name.getStringValue());
    return new InternalResultSet(result);
  }

  /**
   * Confirms that the timestamps of a type declared with another precision before issue #9800 are milliseconds, the
   * unit every time computation uses, which resumes the retention and downsampling suspended on it. The declaration
   * only changes: no stored timestamp is rewritten, so no other unit can be the target.
   */
  private ResultSet alterPrecision(final CommandContext context, final LocalTimeSeriesType tsType) {
    if (!TimeSeriesType.MILLISECOND_PRECISION.equals(precision))
      throw new CommandExecutionException("Cannot set the precision of TimeSeries type '" + name.getStringValue()
          + "' to " + precision + ": timestamps are stored, ingested, retained and queried in milliseconds, so only "
          + "MILLISECOND can be declared");

    final String previous = tsType.getPrecision();
    // No save here: the setter saves, and replicates, the schema itself (issue #9747)
    if (!TimeSeriesType.MILLISECOND_PRECISION.equalsIgnoreCase(previous))
      tsType.setPrecision(precision);

    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "set precision");
    result.setProperty("typeName", name.getStringValue());
    result.setProperty("previousPrecision", previous);
    result.setProperty("precision", precision);
    return new InternalResultSet(result);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("ALTER TIMESERIES TYPE ");
    name.toString(params, builder);

    if (precision != null)
      builder.append(" PRECISION ").append(precision);
    else if (addPolicy) {
      builder.append(" ADD DOWNSAMPLING POLICY");
      for (final DownsamplingTier tier : tiers) {
        // Rendered through the unit, not the bare millisecond count: downsamplingTierClause's grammar rule
        // requires a tsTimeUnit after each INTEGER_LITERAL, so a bare count does not survive print-and-reparse
        // (issue #7791, the same defect #7689 fixed on the sibling CREATE statement).
        builder.append(" AFTER ").append(renderDuration(tier.afterMs()));
        builder.append(" GRANULARITY ").append(renderDuration(tier.granularityMs()));
        builder.append(renderTierOffset(tier.offsetMs()));
      }
    } else
      builder.append(" DROP DOWNSAMPLING POLICY");
  }

  @Override
  public AlterTimeSeriesTypeStatement copy() {
    final AlterTimeSeriesTypeStatement result = new AlterTimeSeriesTypeStatement();
    result.name = name == null ? null : name.copy();
    result.addPolicy = addPolicy;
    result.tiers = new ArrayList<>(tiers);
    result.precision = precision;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AlterTimeSeriesTypeStatement that = (AlterTimeSeriesTypeStatement) o;
    return addPolicy == that.addPolicy && Objects.equals(name, that.name) && Objects.equals(tiers, that.tiers)
        && Objects.equals(precision, that.precision);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, addPolicy, tiers, precision);
  }
}
