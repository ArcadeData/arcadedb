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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL statement: CREATE TIMESERIES TYPE
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CreateTimeSeriesTypeStatement extends DDLStatement {

  public Identifier name;
  public boolean    ifNotExists;
  public Identifier timestampColumn;
  public String     precision;
  /** Codec named by {@code TIMESTAMP ts CODEC ...}, or {@code null} for the role/type default (issue #7689). */
  public String     timestampCodec;
  public PInteger   shards;
  public long       retentionMs;
  public long       compactionIntervalMs;

  public List<ColumnDef> tags   = new ArrayList<>();
  public List<ColumnDef> fields = new ArrayList<>();

  /**
   * Tiers declared by the statement's own {@code DOWNSAMPLING POLICY} clause (issue #7689). Empty when the
   * statement names none, in which case the type is created with no policy - exactly as before the clause existed.
   */
  public List<DownsamplingTier> tiers = new ArrayList<>();

  /**
   * The time units {@code RETENTION} and {@code COMPACTION_INTERVAL} accept, largest first, paired with their
   * millisecond value. Used by {@link #toString(Map, StringBuilder)}, which has to re-render a duration in the SAME
   * units the parser reads: the parser's default for a bare {@code RETENTION 90} is DAYS, so printing the raw
   * millisecond count would multiply it by 86,400,000 the next time the printed form is parsed.
   */
  private static final long[]   SQL_UNIT_MS    = { 86_400_000L, 3_600_000L, 60_000L, 1_000L };
  private static final String[] SQL_UNIT_NAMES = { "DAYS", "HOURS", "MINUTES", "SECONDS" };

  public CreateTimeSeriesTypeStatement() {
  }

  /**
   * Batchable into a DDL script's bulk schema scope (issue #6990). Creating a TimeSeries type builds its engine and registers the type. Nothing is ingested at creation.
   */
  @Override
  public boolean isBulkSchemaScopeSafe(final DatabaseInternal database) {
    return true;
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Schema schema = context.getDatabase().getSchema();

    if (schema.existsType(name.getStringValue())) {
      if (ifNotExists)
        // One row, exactly as the creating branch below, with created=false telling the two apart. Returning
        // an empty result set on a retry made a caller that checks the row count read success as failure,
        // which is precisely what IF NOT EXISTS exists to prevent (issue #7143).
        return new InternalResultSet(describe(context, false));
      else
        throw new CommandExecutionException("Type '" + name.getStringValue() + "' already exists");
    }

    TimeSeriesTypeBuilder builder = schema.buildTimeSeriesType().withName(name.getStringValue());

    if (timestampColumn != null)
      builder = addColumn(builder, timestampColumn, Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP, timestampCodec);

    if (precision != null)
      builder = builder.withPrecision(precision);

    for (final ColumnDef tag : tags)
      builder = addColumn(builder, tag.name, Type.getTypeByName(tag.type.getStringValue()),
          ColumnDefinition.ColumnRole.TAG, tag.codec);

    for (final ColumnDef field : fields)
      builder = addColumn(builder, field.name, Type.getTypeByName(field.type.getStringValue()),
          ColumnDefinition.ColumnRole.FIELD, field.codec);

    if (shards != null)
      builder = builder.withShards(shards.getValue().intValue());

    if (retentionMs > 0)
      builder = builder.withRetention(retentionMs);

    if (compactionIntervalMs > 0)
      builder = builder.withCompactionBucketInterval(compactionIntervalMs);

    if (!tiers.isEmpty())
      builder = builder.withDownsamplingTiers(tiers);

    builder.create();

    return new InternalResultSet(describe(context, true));
  }

  /**
   * Adds one column to the builder, with the codec the statement named or the role/type default when it named none.
   * <p>
   * The DEFAULT path deliberately goes through {@code withTimestamp}/{@code withTag}/{@code withField} rather than
   * through {@code withColumn} with an explicitly resolved default: those three are what every pre-#7689 statement
   * called, and {@code withTimestamp} additionally records the column as the type's timestamp.
   */
  private static TimeSeriesTypeBuilder addColumn(final TimeSeriesTypeBuilder builder, final Identifier columnName,
      final Type dataType, final ColumnDefinition.ColumnRole role, final String codecName) {
    final String column = columnName.getStringValue();
    if (codecName == null)
      return switch (role) {
        case TIMESTAMP -> builder.withTimestamp(column);
        case TAG -> builder.withTag(column, dataType);
        case FIELD -> builder.withField(column, dataType);
      };

    return builder.withColumn(new ColumnDefinition(column, dataType, role, resolveCodec(column, codecName)));
  }

  /**
   * Resolves a {@code CODEC} name to its enum constant, reporting an unknown one as a command error that names the
   * codecs there are. {@code TimeSeriesCodec.valueOf} alone raises an {@code IllegalArgumentException} whose message
   * carries the enum's binary name and nothing about the column it came from.
   */
  private static TimeSeriesCodec resolveCodec(final String column, final String codecName) {
    try {
      return TimeSeriesCodec.valueOf(codecName);
    } catch (final IllegalArgumentException e) {
      final StringBuilder valid = new StringBuilder();
      for (final TimeSeriesCodec codec : TimeSeriesCodec.values())
        valid.append(valid.isEmpty() ? "" : ", ").append(codec.name());
      throw new CommandExecutionException(
          "Column '" + column + "' declares the unknown codec '" + codecName + "'. Supported codecs: " + valid, e);
    }
  }

  private ResultInternal describe(final CommandContext context, final boolean created) {
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "create timeseries type");
    result.setProperty("typeName", name.getStringValue());
    result.setProperty("created", created);
    return result;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CREATE TIMESERIES TYPE ");
    name.toString(params, builder);

    if (ifNotExists)
      builder.append(" IF NOT EXISTS");

    if (timestampColumn != null) {
      builder.append(" TIMESTAMP ");
      timestampColumn.toString(params, builder);
      if (precision != null)
        builder.append(" PRECISION ").append(precision);
      if (timestampCodec != null)
        builder.append(" CODEC ").append(timestampCodec);
    }

    appendColumnList(params, builder, " TAGS (", tags);
    appendColumnList(params, builder, " FIELDS (", fields);

    if (shards != null) {
      builder.append(" SHARDS ");
      shards.toString(params, builder);
    }

    // With a unit, never as a bare millisecond count: the parser reads `RETENTION 90` with no unit as 90 DAYS, so
    // an unqualified number does not survive a print-and-reparse round trip (issue #7689).
    if (retentionMs > 0)
      builder.append(" RETENTION ").append(renderDuration(retentionMs));

    if (compactionIntervalMs > 0)
      builder.append(" COMPACTION_INTERVAL ").append(renderDuration(compactionIntervalMs));

    if (!tiers.isEmpty()) {
      builder.append(" DOWNSAMPLING POLICY");
      for (final DownsamplingTier tier : tiers)
        builder.append(" AFTER ").append(renderDuration(tier.afterMs()))
            .append(" GRANULARITY ").append(renderDuration(tier.granularityMs()));
    }
  }

  private static void appendColumnList(final Map<String, Object> params, final StringBuilder builder,
      final String header, final List<ColumnDef> columns) {
    if (columns.isEmpty())
      return;
    builder.append(header);
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0)
        builder.append(", ");
      final ColumnDef column = columns.get(i);
      column.name.toString(params, builder);
      builder.append(" ");
      column.type.toString(params, builder);
      if (column.codec != null)
        builder.append(" CODEC ").append(column.codec);
    }
    builder.append(")");
  }

  /**
   * {@code <count> <unit>} for a duration in milliseconds, using the largest unit that divides it exactly. A
   * duration that is not a whole number of seconds - which the grammar cannot express at all - falls back to
   * milliseconds with no unit rather than throwing: {@code toString} is a rendering of whatever the statement
   * happens to carry, including a value that got there through a parameter or a hand-built AST, and a printer that
   * throws would take {@code EXPLAIN} and the statement cache down with it.
   */
  private static String renderDuration(final long millis) {
    for (int i = 0; i < SQL_UNIT_MS.length; i++)
      if (millis % SQL_UNIT_MS[i] == 0)
        return (millis / SQL_UNIT_MS[i]) + " " + SQL_UNIT_NAMES[i];
    return String.valueOf(millis);
  }

  @Override
  public CreateTimeSeriesTypeStatement copy() {
    final CreateTimeSeriesTypeStatement result = new CreateTimeSeriesTypeStatement();
    result.name = name == null ? null : name.copy();
    result.ifNotExists = ifNotExists;
    result.timestampColumn = timestampColumn == null ? null : timestampColumn.copy();
    result.precision = precision;
    result.timestampCodec = timestampCodec;
    result.shards = shards == null ? null : shards.copy();
    result.retentionMs = retentionMs;
    result.compactionIntervalMs = compactionIntervalMs;
    result.tags = copyColumns(tags);
    result.fields = copyColumns(fields);
    // DownsamplingTier is a record of two longs, so the list is the only mutable part to copy.
    result.tiers = new ArrayList<>(tiers);
    return result;
  }

  private static List<ColumnDef> copyColumns(final List<ColumnDef> columns) {
    final List<ColumnDef> copy = new ArrayList<>(columns.size());
    for (final ColumnDef cd : columns)
      copy.add(new ColumnDef(cd.name == null ? null : cd.name.copy(), cd.type == null ? null : cd.type.copy(), cd.codec));
    return copy;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final CreateTimeSeriesTypeStatement that = (CreateTimeSeriesTypeStatement) o;
    return ifNotExists == that.ifNotExists && retentionMs == that.retentionMs
        && compactionIntervalMs == that.compactionIntervalMs && Objects.equals(name, that.name)
        && Objects.equals(timestampColumn, that.timestampColumn) && Objects.equals(precision, that.precision)
        && Objects.equals(timestampCodec, that.timestampCodec) && Objects.equals(shards, that.shards)
        && Objects.equals(tags, that.tags) && Objects.equals(fields, that.fields) && Objects.equals(tiers, that.tiers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, ifNotExists, timestampColumn, precision, timestampCodec, shards, retentionMs,
        compactionIntervalMs, tags, fields, tiers);
  }

  public static class ColumnDef {
    public Identifier name;
    public Identifier type;
    /** Codec named by {@code CODEC ...} on this column, or {@code null} for the role/type default (issue #7689). */
    public String     codec;

    public ColumnDef(final Identifier name, final Identifier type) {
      this(name, type, null);
    }

    public ColumnDef(final Identifier name, final Identifier type, final String codec) {
      this.name = name;
      this.type = type;
      this.codec = codec;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      final ColumnDef that = (ColumnDef) o;
      return Objects.equals(name, that.name) && Objects.equals(type, that.type) && Objects.equals(codec, that.codec);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type, codec);
    }
  }
}
