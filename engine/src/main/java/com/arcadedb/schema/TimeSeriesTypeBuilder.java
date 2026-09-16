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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.SchemaException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Fluent builder for creating TimeSeries types.
 * <p>
 * The builder varies over {@link BasicDatabase}, not over {@code DatabaseInternal}, and its terminal operation
 * returns the {@link TimeSeriesType} abstraction rather than a local class, so the same body of builder code runs
 * against an embedded database and against {@code RemoteDatabase} (issue #7399). This class is the embedded
 * implementation: {@link #create()} builds the type in place. A remote schema subclasses it and overrides
 * {@link #create()} to issue {@link #toSQL()} through the server instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesTypeBuilder {

  /**
   * The timestamp precisions {@code CREATE TIMESERIES TYPE} accepts, upper-cased. A builder carrying anything else
   * has no SQL expression and {@link #toSQL()} refuses it rather than emitting DDL the server will reject.
   */
  private static final Set<String> SQL_PRECISIONS = Set.of("NANOSECOND", "MICROSECOND", "MILLISECOND", "SECOND");

  /**
   * The time units {@code RETENTION}, {@code COMPACTION_INTERVAL} and {@code DOWNSAMPLING POLICY} accept, largest
   * first, paired with their millisecond value. The smallest is SECONDS, which is why a duration that is not a whole
   * number of seconds cannot be rendered.
   */
  private static final long[]   SQL_UNIT_MS    = { 86_400_000L, 3_600_000L, 60_000L, 1_000L };
  private static final String[] SQL_UNIT_NAMES = { "DAYS", "HOURS", "MINUTES", "SECONDS" };

  protected final BasicDatabase          database;
  protected       String                 typeName;
  protected       String                 timestampColumn;
  protected       String                 precision;
  protected       int                    shards                     = 0; // 0 = default (async worker threads)
  protected       long                   retentionMs                = 0;
  protected       long                   compactionBucketIntervalMs = 0;
  protected       List<DownsamplingTier> downsamplingTiers          = new ArrayList<>();
  protected final List<ColumnDefinition> columns                    = new ArrayList<>();

  /**
   * @param database the database the type will be created in. An embedded {@code DatabaseInternal} for this class;
   *                 a subclass may accept any other {@link BasicDatabase}, {@code RemoteDatabase} included.
   */
  public TimeSeriesTypeBuilder(final BasicDatabase database) {
    this.database = database;
  }

  public TimeSeriesTypeBuilder withName(final String name) {
    this.typeName = name;
    return this;
  }

  public TimeSeriesTypeBuilder withTimestamp(final String name) {
    this.timestampColumn = name;
    this.columns.add(new ColumnDefinition(name, Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP));
    return this;
  }

  /**
   * Declares the timestamp precision. The value is upper-cased, because that is the only form any other path
   * produces: {@code SQLASTBuilder} upper-cases the token it parses out of {@code PRECISION ...}, so a type created
   * by SQL - which includes every type a remote builder creates, since it renders DDL - already reports the
   * canonical form. Storing the caller's spelling verbatim made the same builder code report {@code "nanosecond"}
   * embedded and {@code "NANOSECOND"} remotely (issue #7399), a divergence in exactly the invariant the remote
   * builder exists to establish.
   * <p>
   * The value is NOT validated here: {@link #toSQL()} is where an unrenderable precision has to fail, and rejecting
   * one at declaration time would refuse it for the embedded path too, which accepts it today.
   */
  public TimeSeriesTypeBuilder withPrecision(final String precision) {
    this.precision = precision != null ? precision.toUpperCase(Locale.ENGLISH) : null;
    return this;
  }

  public TimeSeriesTypeBuilder withTag(final String name, final Type type) {
    this.columns.add(new ColumnDefinition(name, type, ColumnDefinition.ColumnRole.TAG));
    return this;
  }

  public TimeSeriesTypeBuilder withField(final String name, final Type type) {
    this.columns.add(new ColumnDefinition(name, type, ColumnDefinition.ColumnRole.FIELD));
    return this;
  }

  /**
   * Adds an already-resolved column, codec included.
   * <p>
   * {@link #withTimestamp}/{@link #withTag}/{@link #withField} build the {@link ColumnDefinition} from the name and
   * the type alone, which leaves the codec at the current default table's choice. That is right for a user creating
   * a type, and wrong for anything RESTORING one - a logical restore (the JSONL importer) has the exported schema in
   * hand, where the codec is recorded per column precisely because it is not re-derivable (issue #5475), and
   * re-deriving it would silently re-encode the restored type differently from the one it came from.
   * <p>
   * The codec has a {@code CREATE TIMESERIES TYPE} expression since issue #7689 - {@code name TYPE CODEC NAME} -
   * so a column carrying an explicit one renders like any other and a logical restore runs against a remote
   * database as well as an embedded one.
   *
   * @param column the column to add; a TIMESTAMP-role column also becomes the type's timestamp column
   */
  public TimeSeriesTypeBuilder withColumn(final ColumnDefinition column) {
    if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
      this.timestampColumn = column.getName();
    this.columns.add(column);
    return this;
  }

  public TimeSeriesTypeBuilder withShards(final int shards) {
    this.shards = shards;
    return this;
  }

  public TimeSeriesTypeBuilder withRetention(final long retentionMs) {
    this.retentionMs = retentionMs;
    return this;
  }

  public TimeSeriesTypeBuilder withCompactionBucketInterval(final long compactionBucketIntervalMs) {
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;
    return this;
  }

  /**
   * Declares the downsampling policy. The tiers are normalized to ascending {@code afterMs}, which is what
   * {@code ALTER TIMESERIES TYPE ... ADD DOWNSAMPLING POLICY} already does to the tiers it parses: without this, a
   * caller who listed them oldest-first got one order from an embedded {@code create()} and the sorted one from the
   * remote builder that renders that SQL, for the same builder code (issue #7399). A tier list is a handful of age
   * thresholds declared once on the DDL path, so sorting it costs nothing worth measuring.
   */
  public TimeSeriesTypeBuilder withDownsamplingTiers(final List<DownsamplingTier> tiers) {
    this.downsamplingTiers = tiers != null ? new ArrayList<>(tiers) : new ArrayList<>();
    this.downsamplingTiers.sort(Comparator.comparingLong(DownsamplingTier::afterMs));
    return this;
  }

  /**
   * The type name accumulated so far, or {@code null} when {@link #withName} has not been called. For subclasses
   * that have to name the type they just created when reading it back.
   */
  public String getName() {
    return typeName;
  }

  /**
   * Renders the accumulated state as the DDL that creates the same type, for an implementation that can only reach
   * the schema through {@code command("sql", ...)}.
   * <p>
   * <b>Exactly one statement</b>, in every builder state this method accepts. It used to be one or two - a
   * {@code CREATE TIMESERIES TYPE} followed by an {@code ALTER TIMESERIES TYPE ... ADD DOWNSAMPLING POLICY} when
   * tiers were declared, because the create grammar had no downsampling clause - and a caller that got the CREATE
   * through and not the ALTER was left with a type missing its policy and an exception in hand. The grammar now
   * carries the policy and the per-column codecs, so there is nothing left to tear (issue #7689). The return type
   * stays a list, and callers still execute it in order, so that a clause the grammar may one day still not carry
   * has somewhere to go.
   * <p>
   * Throws {@link SchemaException} rather than emitting DDL the server would reject, for every builder state the
   * grammar cannot express: a precision outside the four the grammar names, and a retention, compaction interval or
   * downsampling threshold that is not a whole number of seconds (the grammar's smallest unit). Failing here, at
   * build time, is the point: the alternative is invalid SQL reaching the server.
   */
  public List<String> toSQL() {
    validate();
    validateForSQL();

    return List.of(renderCreate());
  }

  private String renderCreate() {
    final StringBuilder sql = new StringBuilder(128);
    sql.append("CREATE TIMESERIES TYPE ").append(quote(typeName));

    sql.append(" TIMESTAMP ").append(quote(timestampColumn));
    if (precision != null) {
      // Already upper-cased by withPrecision; what is checked here is membership, because the grammar names
      // exactly four and anything else has no expression at all.
      if (!SQL_PRECISIONS.contains(precision))
        throw new SchemaException("Precision '" + precision + "' has no CREATE TIMESERIES TYPE expression. Supported: "
            + String.join(", ", SQL_PRECISIONS));
      sql.append(" PRECISION ").append(precision);
    }

    appendCodec(sql, timestampColumnDefinition());

    appendColumnList(sql, " TAGS (", ColumnDefinition.ColumnRole.TAG);
    appendColumnList(sql, " FIELDS (", ColumnDefinition.ColumnRole.FIELD);

    if (shards > 0)
      sql.append(" SHARDS ").append(shards);

    if (retentionMs > 0)
      sql.append(" RETENTION ").append(renderDuration(retentionMs, "retention"));

    if (compactionBucketIntervalMs > 0)
      sql.append(" COMPACTION_INTERVAL ").append(renderDuration(compactionBucketIntervalMs, "compaction bucket interval"));

    appendDownsamplingPolicy(sql);

    return sql.toString();
  }

  private void appendColumnList(final StringBuilder sql, final String header, final ColumnDefinition.ColumnRole role) {
    boolean first = true;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() != role)
        continue;
      sql.append(first ? header : ", ").append(quote(col.getName())).append(' ').append(col.getDataType().name());
      appendCodec(sql, col);
      first = false;
    }
    if (!first)
      sql.append(')');
  }

  /**
   * Appends {@code CODEC <name>} when the column carries a codec that is NOT the default for its data type and
   * role, and nothing at all when it carries the default.
   * <p>
   * The clause is omitted for a default because the column is always constructed with one - "explicit" is only
   * visible as "differs from {@link ColumnDefinition#defaultCodecFor}" - and naming it anyway would pin today's
   * default table into every rendered statement, so a type recreated from that DDL after the table changes would
   * get the old codec rather than the new one the same builder code gets embedded.
   */
  private static void appendCodec(final StringBuilder sql, final ColumnDefinition column) {
    if (column == null)
      return;
    final TimeSeriesCodec codec = column.getCompressionHint();
    if (codec != null && codec != ColumnDefinition.defaultCodecFor(column.getDataType(), column.getRole()))
      sql.append(" CODEC ").append(codec.name());
  }

  /**
   * The TIMESTAMP-role column, or {@code null} when the builder carries none. {@link #validate()} has already
   * refused a builder with no timestamp column by the time {@link #renderCreate()} asks, so the null is for the
   * column list carrying a timestamp column under a different name than {@link #timestampColumn}, which
   * {@link #withColumn} cannot produce and a subclass could.
   */
  private ColumnDefinition timestampColumnDefinition() {
    for (final ColumnDefinition col : columns)
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP && col.getName().equals(timestampColumn))
        return col;
    return null;
  }

  private void appendDownsamplingPolicy(final StringBuilder sql) {
    if (downsamplingTiers.isEmpty())
      return;

    sql.append(" DOWNSAMPLING POLICY");
    for (final DownsamplingTier tier : downsamplingTiers)
      sql.append(" AFTER ").append(renderDuration(tier.afterMs(), "downsampling threshold"))
          .append(" GRANULARITY ").append(renderDuration(tier.granularityMs(), "downsampling granularity"));
  }

  /**
   * {@code <count> <unit>} for a duration in milliseconds, using the largest of DAYS/HOURS/MINUTES/SECONDS that
   * divides it exactly, or {@code null} when none of them does.
   * <p>
   * The unit is never omitted: the parser's default for a bare {@code RETENTION 90} is DAYS, so emitting the raw
   * millisecond count without a unit would multiply it by 86,400,000. That is why this returns {@code null} instead
   * of a bare number - the two callers have to answer an unrenderable duration differently, and neither answer is a
   * unit-less count. This one throws, because DDL the server would reject must not leave the client;
   * {@code CreateTimeSeriesTypeStatement.toString()} falls back to the raw count, because a printer that throws
   * would take {@code EXPLAIN} and the statement cache with it.
   * <p>
   * Public and shared so the DAYS/HOURS/MINUTES/SECONDS table exists once: two copies would have to be kept in step
   * by hand if a unit were ever added (claude review on PR #7721).
   */
  public static String renderSQLDuration(final long millis) {
    for (int i = 0; i < SQL_UNIT_MS.length; i++)
      if (millis % SQL_UNIT_MS[i] == 0)
        return (millis / SQL_UNIT_MS[i]) + " " + SQL_UNIT_NAMES[i];
    return null;
  }

  private static String renderDuration(final long millis, final String what) {
    final String rendered = renderSQLDuration(millis);
    if (rendered != null)
      return rendered;

    throw new SchemaException("A " + what + " of " + millis
        + "ms has no CREATE TIMESERIES TYPE expression: the grammar's smallest time unit is SECONDS, so the value must be a whole number of seconds");
  }

  /**
   * Back-quotes an identifier so a name that collides with a keyword still parses. A name that itself contains a
   * back-quote is refused: there is no escape for it in the grammar, and silently concatenating it would let a
   * caller-supplied name close the quote and continue the statement.
   */
  private static String quote(final String identifier) {
    if (identifier.indexOf('`') >= 0)
      throw new SchemaException("Identifier '" + identifier + "' cannot be used in SQL: it contains a back-quote");
    return "`" + identifier + "`";
  }

  /**
   * The checks that hold wherever the type is created: they are the reason a remote {@code create()} fails on the
   * same builder states an embedded one does, instead of on the server's parse error.
   */
  protected void validate() {
    if (typeName == null || typeName.isEmpty())
      throw new SchemaException("TimeSeries type name is required");
    if (timestampColumn == null)
      throw new SchemaException("TimeSeries type requires a TIMESTAMP column");

    // Negative durations are refused rather than carried: renderCreate() omits both clauses below zero, while
    // the embedded create() hands the value straight to LocalTimeSeriesType, so the SAME builder body would
    // produce a type whose getRetentionMs() is the caller's negative number embedded and 0 remotely. Neither is
    // what the caller asked for, and the difference contradicts the one-body-of-builder-code contract this
    // builder exists for. Zero keeps its meaning of "no policy" (claude/CodeRabbit review on PR #7692).
    if (retentionMs < 0)
      throw new SchemaException("TimeSeries retention cannot be negative, was " + retentionMs + "ms");
    if (compactionBucketIntervalMs < 0)
      throw new SchemaException(
          "TimeSeries compaction bucket interval cannot be negative, was " + compactionBucketIntervalMs + "ms");

    // A TimeSeries row is a fixed-stride record and a sealed block column is one of three primitive
    // codecs, so a type with neither a fixed width nor a bounded text form cannot be stored. Declaring
    // one used to be accepted and then corrupted the columns after it in the row (issue #5475).
    for (final ColumnDefinition col : columns)
      if (!ColumnDefinition.isStorableType(col.getDataType()))
        throw new SchemaException("Column '" + col.getName() + "' of type " + col.getDataType()
            + " cannot be used in a TIMESERIES type. Supported types: " + ColumnDefinition.storableTypeNames());
  }

  /**
   * Same as {@link #validate()} plus the checks that only a SQL-rendered create has to pass.
   */
  private void validateForSQL() {
    // A non-default per-column codec used to be refused here: the grammar could not name one, so rendering the
    // column without it would have silently recreated it with the default (issue #5475's failure). CREATE
    // TIMESERIES TYPE carries a CODEC clause since issue #7689, so the codec renders instead.
    int timestampColumns = 0;
    for (final ColumnDefinition col : columns)
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        ++timestampColumns;
    if (timestampColumns > 1)
      throw new SchemaException(
          "CREATE TIMESERIES TYPE declares exactly one TIMESTAMP column, and this builder carries " + timestampColumns);
  }

  /**
   * Creates the type and returns it.
   * <p>
   * This implementation creates it in place, in the embedded database the builder was constructed with. A remote
   * schema returns a subclass that overrides this to issue {@link #toSQL()} through the server.
   */
  public TimeSeriesType create() {
    validate();

    if (!(database instanceof DatabaseInternal))
      throw new SchemaException("Cannot create the TimeSeries type '" + typeName + "' in place: "
          + database.getClass().getSimpleName() + " is not an embedded database. Use the builder returned by its own Schema");

    final DatabaseInternal databaseInternal = (DatabaseInternal) database;
    final LocalSchema schema = (LocalSchema) databaseInternal.getSchema();
    if (schema.existsType(typeName))
      throw new SchemaException("Type '" + typeName + "' already exists");

    final LocalTimeSeriesType type = new LocalTimeSeriesType(schema, typeName);
    type.setTimestampColumn(timestampColumn);
    type.setPrecision(precision);
    type.setShardCount(
        shards > 0 ? shards : databaseInternal.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS));
    type.setRetentionMs(retentionMs);
    type.setCompactionBucketIntervalMs(compactionBucketIntervalMs);
    type.setDownsamplingTiers(downsamplingTiers);

    for (final ColumnDefinition col : columns)
      type.addTsColumn(col);

    // Register properties for each column
    for (final ColumnDefinition col : columns)
      type.createProperty(col.getName(), col.getDataType());

    // Wrap engine initialization + type registration in recordFileChanges so that, under HA, the
    // shard bucket-file creation, their header-page writes and the schema JSON are captured and
    // shipped to followers atomically in a single SCHEMA_ENTRY. Without this the TIMESERIES type
    // never appears on followers ("Type 'x' was not found") - issue #4382. On a standalone database
    // recordFileChanges simply runs the callback and persists the schema.
    schema.recordFileChanges(() -> {
      final DatabaseInternal db = databaseInternal.getWrappedDatabaseInstance();
      db.begin();
      try {
        type.initEngine();
        db.commit();
      } catch (final Exception e) {
        if (db.isTransactionActive())
          db.rollback();
        throw new SchemaException("Failed to initialize TimeSeries engine for type '" + typeName + "'", e);
      }

      // Register the type with the schema only after successful engine initialization
      schema.registerType(type);
      return null;
    });

    // Schedule automatic retention/downsampling if policies are defined
    schema.getTimeSeriesMaintenanceScheduler().schedule(databaseInternal, type);

    return type;
  }
}
