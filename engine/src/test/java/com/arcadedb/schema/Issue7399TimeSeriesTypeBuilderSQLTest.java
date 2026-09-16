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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7399: {@link TimeSeriesTypeBuilder} renders its accumulated state as DDL, so an implementation that can
 * only reach the schema through {@code command("sql", ...)} - {@code RemoteSchema} - can create the same type the
 * embedded builder creates.
 * <p>
 * These are the engine-side half of the acceptance criteria. They assert two things the remote half cannot:
 * <ul>
 * <li>that the rendered SQL <b>parses</b>, for every column type a TIMESERIES type may declare, so a builder state
 * with no SQL expression fails at build time instead of producing invalid DDL that only the server rejects;</li>
 * <li>that executing the rendered SQL yields the same type declaration as calling {@code create()} on the very same
 * builder state - the local-to-local parity that the remote path then rides on.</li>
 * </ul>
 * The end-to-end embedded-vs-remote comparison lives in {@code Issue7399RemoteTimeSeriesTypeBuilderIT}.
 */
class Issue7399TimeSeriesTypeBuilderSQLTest extends TestHelper {

  /**
   * Every type {@link ColumnDefinition#isStorableType} admits. A type the builder accepts and the grammar cannot
   * name would ship as DDL the server refuses, which is exactly the failure mode this test exists to prevent.
   */
  private static final Type[] STORABLE_TYPES = {
      Type.BOOLEAN, Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.STRING,
      Type.DATE, Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS };

  private static void assertParses(final String sql) {
    assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
  }

  private TimeSeriesTypeBuilder builder(final String name) {
    return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
  }

  @Test
  void renderedSQLParsesForEveryStorableColumnType() {
    final TimeSeriesTypeBuilder builder = builder("AllStorableTypes");
    for (final Type type : STORABLE_TYPES) {
      builder.withTag("tag_" + type.name(), type);
      builder.withField("field_" + type.name(), type);
    }

    final List<String> statements = builder.toSQL();
    assertThat(statements).hasSize(1);
    for (final String sql : statements)
      assertParses(sql);

    // Both roles of every type must appear: a renderer that silently dropped a role would still parse.
    for (final Type type : STORABLE_TYPES) {
      assertThat(statements.getFirst()).contains("`tag_" + type.name() + "` " + type.name());
      assertThat(statements.getFirst()).contains("`field_" + type.name() + "` " + type.name());
    }
  }

  @Test
  void renderedSQLCreatesTheSameTypeAsCreate() {
    // One body of builder code, applied twice: once through create(), once through the SQL it renders. Anything
    // the renderer drops or mistranslates shows up as a difference between the two declarations.
    final TimeSeriesType fromBuilder = configure(builder("ViaBuilder")).create();

    for (final String sql : configure(builder("ViaSQL")).toSQL())
      database.command("sql", sql);

    final DocumentType created = database.getSchema().getType("ViaSQL");
    assertThat(created).isInstanceOf(TimeSeriesType.class);
    final TimeSeriesType fromSQL = (TimeSeriesType) created;

    assertThat(fromSQL.getTimestampColumn()).isEqualTo(fromBuilder.getTimestampColumn());
    assertThat(fromSQL.getPrecision()).isEqualTo(fromBuilder.getPrecision());
    assertThat(fromSQL.getShardCount()).isEqualTo(fromBuilder.getShardCount());
    assertThat(fromSQL.getRetentionMs()).isEqualTo(fromBuilder.getRetentionMs());
    assertThat(fromSQL.getCompactionBucketIntervalMs()).isEqualTo(fromBuilder.getCompactionBucketIntervalMs());
    assertThat(fromSQL.getDownsamplingTiers()).isEqualTo(fromBuilder.getDownsamplingTiers());

    assertThat(fromSQL.getTsColumnNames()).isEqualTo(fromBuilder.getTsColumnNames());
    for (final ColumnDefinition expected : fromBuilder.getTsColumns()) {
      final ColumnDefinition actual = fromSQL.getTsColumn(expected.getName());
      assertThat(actual).as(expected.getName()).isNotNull();
      assertThat(actual.getDataType()).as(expected.getName()).isEqualTo(expected.getDataType());
      assertThat(actual.getRole()).as(expected.getName()).isEqualTo(expected.getRole());
    }
  }

  private static TimeSeriesTypeBuilder configure(final TimeSeriesTypeBuilder builder) {
    return builder
        .withPrecision("MILLISECOND")
        .withTag("host", Type.STRING)
        .withTag("zone", Type.INTEGER)
        .withField("cpu", Type.DOUBLE)
        .withField("mem", Type.LONG)
        .withShards(3)
        .withRetention(90L * 86_400_000L)
        .withCompactionBucketInterval(2L * 3_600_000L)
        .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
            new DownsamplingTier(30L * 86_400_000L, 86_400_000L)));
  }

  @Test
  void downsamplingTiersRenderAsASecondStatementBecauseCreateHasNoClauseForThem() {
    final List<String> statements = builder("Tiered")
        .withField("value", Type.DOUBLE)
        .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L)))
        .toSQL();

    assertThat(statements).hasSize(2);
    assertThat(statements.get(0)).startsWith("CREATE TIMESERIES TYPE `Tiered`");
    assertThat(statements.get(1))
        .isEqualTo("ALTER TIMESERIES TYPE `Tiered` ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");
    assertParses(statements.get(0));
    assertParses(statements.get(1));
  }

  @Test
  void tiersDeclaredOutOfOrderEndUpInTheSameOrderWhicheverPathCreatesTheType() {
    // The SQL path sorts the tiers it parses, the builder used to keep the caller's order, so one body of builder
    // code that listed them oldest-first produced two different declarations. This pins them together.
    final List<DownsamplingTier> unordered = List.of(
        new DownsamplingTier(30L * 86_400_000L, 86_400_000L),
        new DownsamplingTier(7L * 86_400_000L, 3_600_000L));

    final TimeSeriesType viaBuilder = builder("UnorderedBuilder").withField("value", Type.DOUBLE)
        .withDownsamplingTiers(unordered).create();

    for (final String sql : builder("UnorderedSQL").withField("value", Type.DOUBLE).withDownsamplingTiers(unordered).toSQL())
      database.command("sql", sql);
    final TimeSeriesType viaSQL = (TimeSeriesType) database.getSchema().getType("UnorderedSQL");

    assertThat(viaBuilder.getDownsamplingTiers()).containsExactly(
        new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
        new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
    assertThat(viaSQL.getDownsamplingTiers()).isEqualTo(viaBuilder.getDownsamplingTiers());
  }

  @Test
  void durationsRenderWithTheLargestUnitThatDividesThemExactly() {
    // Never a bare millisecond count: the parser reads `RETENTION 90` with no unit as 90 DAYS, so an unqualified
    // number would be multiplied by 86,400,000 on the far side.
    assertThat(retentionClauseOf(86_400_000L)).isEqualTo("RETENTION 1 DAYS");
    assertThat(retentionClauseOf(7_200_000L)).isEqualTo("RETENTION 2 HOURS");
    assertThat(retentionClauseOf(300_000L)).isEqualTo("RETENTION 5 MINUTES");
    assertThat(retentionClauseOf(90_000L)).isEqualTo("RETENTION 90 SECONDS");
  }

  private String retentionClauseOf(final long retentionMs) {
    final String sql = builder("Retention" + retentionMs).withField("value", Type.DOUBLE).withRetention(retentionMs)
        .toSQL().getFirst();
    assertParses(sql);
    return sql.substring(sql.indexOf("RETENTION "));
  }

  @Test
  void aColumnWithAnExplicitCodecHasNoSQLExpression() {
    // withColumn() carries a codec the grammar cannot name, so the builder refuses to render rather than emitting
    // DDL that would silently recreate the column with the DEFAULT codec (issue #5475's failure, re-entered).
    final TimeSeriesTypeBuilder builder = builder("ExplicitCodec")
        .withColumn(new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.NONE));

    assertThatThrownBy(builder::toSQL)
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("value")
        .hasMessageContaining("NONE");

    // The same builder still creates the type in place: the restriction is the SQL rendering's, not the builder's.
    assertThat(builder.create().getTsColumn("value").getCompressionHint()).isEqualTo(TimeSeriesCodec.NONE);
  }

  @Test
  void aColumnWhoseExplicitCodecIsTheDefaultOneStillRenders() {
    final ColumnDefinition.ColumnRole role = ColumnDefinition.ColumnRole.FIELD;
    final TimeSeriesCodec defaultCodec = ColumnDefinition.defaultCodecFor(Type.DOUBLE, role);

    final List<String> statements = builder("DefaultCodec")
        .withColumn(new ColumnDefinition("value", Type.DOUBLE, role, defaultCodec)).toSQL();

    assertThat(statements).hasSize(1);
    assertParses(statements.getFirst());
  }

  @Test
  void aPrecisionTheGrammarDoesNotNameHasNoSQLExpression() {
    assertThatThrownBy(() -> builder("BadPrecision").withField("value", Type.DOUBLE).withPrecision("PICOSECOND").toSQL())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("PICOSECOND");
  }

  @Test
  void aBuilderThatDeclaresNoPrecisionRendersNoPrecisionClause() {
    final String sql = builder("NoPrecision").withField("value", Type.DOUBLE).toSQL().getFirst();
    assertThat(sql).doesNotContain("PRECISION");
    assertParses(sql);

    // And the type it creates reports none, rather than a default standing in for one that was never declared.
    assertThat(builder("NoPrecisionCreated").withField("value", Type.DOUBLE).create().getPrecision()).isNull();
  }

  @Test
  void aLowerCasePrecisionIsCanonicalizedOnEveryPathThatCreatesTheType() {
    // The SQL path upper-cases the token it parses, so a type created through DDL always reports the canonical
    // form. A builder that stored the caller's spelling verbatim therefore reported "nanosecond" when it created
    // the type in place and "NANOSECOND" when the same builder code ran against a remote schema - a divergence in
    // exactly the invariant the remote builder exists to establish. All three paths are pinned together here.
    assertThat(builder("LowerCasePrecision").withField("value", Type.DOUBLE).withPrecision("nanosecond").toSQL().getFirst())
        .contains("PRECISION NANOSECOND");

    assertThat(builder("LowerCaseCreated").withField("value", Type.DOUBLE).withPrecision("nanosecond").create()
        .getPrecision()).isEqualTo("NANOSECOND");

    for (final String sql : builder("LowerCaseViaSQL").withField("value", Type.DOUBLE).withPrecision("nanosecond").toSQL())
      database.command("sql", sql);
    assertThat(((TimeSeriesType) database.getSchema().getType("LowerCaseViaSQL")).getPrecision()).isEqualTo("NANOSECOND");
  }

  @Test
  void aDurationFinerThanOneSecondHasNoSQLExpression() {
    assertThatThrownBy(() -> builder("SubSecondRetention").withField("value", Type.DOUBLE).withRetention(1_500L).toSQL())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("retention")
        .hasMessageContaining("whole number of seconds");

    assertThatThrownBy(
        () -> builder("SubSecondCompaction").withField("value", Type.DOUBLE).withCompactionBucketInterval(1_500L).toSQL())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("compaction bucket interval");

    assertThatThrownBy(() -> builder("SubSecondTier").withField("value", Type.DOUBLE)
        .withDownsamplingTiers(List.of(new DownsamplingTier(86_400_000L, 1_500L))).toSQL())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("downsampling granularity");
  }

  @Test
  void anIdentifierCarryingABackQuoteIsRefusedRatherThanConcatenated() {
    // There is no escape for a back-quote inside a quoted identifier, so a name carrying one would close the quote
    // and let whatever follows it run as statement text.
    assertThatThrownBy(() -> builder("Injected`; DROP TYPE `V").withField("value", Type.DOUBLE).toSQL())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("back-quote");
  }

  @Test
  void identifiersAreQuotedSoAKeywordNameStillParses() {
    final String sql = database.getSchema().buildTimeSeriesType().withName("select").withTimestamp("order")
        .withField("limit", Type.DOUBLE).toSQL().getFirst();

    assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `select` TIMESTAMP `order` FIELDS (`limit` DOUBLE)");
    assertParses(sql);
  }

  @Test
  void anIncompleteBuilderFailsTheSameWayWhetherItIsCreatedOrRendered() {
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withTimestamp("ts").toSQL())
        .isInstanceOf(SchemaException.class).hasMessageContaining("name is required");
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withTimestamp("ts").create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("name is required");

    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("NoTimestamp").toSQL())
        .isInstanceOf(SchemaException.class).hasMessageContaining("TIMESTAMP column");
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("NoTimestamp").create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("TIMESTAMP column");

    assertThatThrownBy(() -> builder("UnstorableColumn").withField("blob", Type.BINARY).toSQL())
        .isInstanceOf(SchemaException.class).hasMessageContaining("cannot be used in a TIMESERIES type");
    assertThatThrownBy(() -> builder("UnstorableColumn").withField("blob", Type.BINARY).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("cannot be used in a TIMESERIES type");
  }

  @Test
  void aNegativeDurationIsRefusedOnBothPathsRatherThanRenderedAwayOnOne() {
    // Without the check, this is a silent divergence and not an error: renderCreate() omits a clause that is not
    // > 0, so the remote path would have stored 0, while the embedded path stores the negative number as given.
    assertThatThrownBy(() -> builder("NegativeRetention").withField("v", Type.DOUBLE).withRetention(-1L).toSQL())
        .isInstanceOf(SchemaException.class).hasMessageContaining("retention cannot be negative");
    assertThatThrownBy(() -> builder("NegativeRetention").withField("v", Type.DOUBLE).withRetention(-1L).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("retention cannot be negative");

    assertThatThrownBy(() -> builder("NegativeCompaction").withField("v", Type.DOUBLE)
        .withCompactionBucketInterval(-1L).toSQL())
        .isInstanceOf(SchemaException.class).hasMessageContaining("compaction bucket interval cannot be negative");
    assertThatThrownBy(() -> builder("NegativeCompaction").withField("v", Type.DOUBLE)
        .withCompactionBucketInterval(-1L).create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("compaction bucket interval cannot be negative");
  }

  @Test
  void zeroKeepsItsMeaningOfNoPolicyOnBothPaths() {
    // The bound is on NEGATIVE only: zero is how a caller says "no retention, no compaction interval", it is the
    // field default, and refusing it would break every builder that never names either.
    final TimeSeriesType created = builder("ZeroDurations").withField("v", Type.DOUBLE)
        .withRetention(0L).withCompactionBucketInterval(0L).create();

    assertThat(created.getRetentionMs()).isZero();
    assertThat(created.getCompactionBucketIntervalMs()).isZero();

    final String sql = builder("ZeroDurationsSQL").withField("v", Type.DOUBLE)
        .withRetention(0L).withCompactionBucketInterval(0L).toSQL().getFirst();
    assertThat(sql).doesNotContain("RETENTION").doesNotContain("COMPACTION_INTERVAL");
    assertParses(sql);
  }
}
