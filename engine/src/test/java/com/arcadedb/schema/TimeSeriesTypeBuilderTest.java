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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the TimeSeries type builder and its rendering as SQL, one nested class per fixed issue.
 */
class TimeSeriesTypeBuilderTest {
  /**
   * Regression for #7399: {@link TimeSeriesTypeBuilder} renders its accumulated state as DDL, so an implementation that can
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
  @Nested
  class TimeSeriesTypeBuilderSQLTest extends TestHelper {

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
    void downsamplingTiersRenderInsideTheCreateSoThereIsNoSecondStatement() {
      // This used to render two statements - a CREATE and an ALTER ... ADD DOWNSAMPLING POLICY - because the create
      // grammar had no downsampling clause, which left a window where the CREATE landed and the ALTER did not.
      // CREATE TIMESERIES TYPE carries the policy since issue #7689.
      final List<String> statements = builder("Tiered")
          .withField("value", Type.DOUBLE)
          .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L)))
          .toSQL();

      assertThat(statements).hasSize(1);
      assertThat(statements.getFirst())
          .startsWith("CREATE TIMESERIES TYPE `Tiered`")
          .endsWith(" DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS")
          .doesNotContain("ALTER");
      assertParses(statements.getFirst());
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
    void aColumnWithAnExplicitCodecRendersThatCodecRatherThanBeingRefused() {
      // This used to throw: the grammar could not name a codec, so rendering the column without it would have
      // silently recreated it with the DEFAULT codec (issue #5475's failure, re-entered). CREATE TIMESERIES TYPE
      // carries a CODEC clause since issue #7689, so the codec renders and survives the round trip instead.
      final TimeSeriesTypeBuilder builder = builder("ExplicitCodec")
          .withColumn(new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY));

      final String sql = builder.toSQL().getFirst();
      assertThat(sql).contains("`value` DOUBLE CODEC DICTIONARY");
      assertParses(sql);

      // Both paths agree on the codec, which is the parity the remote builder rides on.
      assertThat(builder.create().getTsColumn("value").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);

      database.command("sql", sql.replace("`ExplicitCodec`", "`ExplicitCodecViaSQL`"));
      assertThat(((TimeSeriesType) database.getSchema().getType("ExplicitCodecViaSQL")).getTsColumn("value")
          .getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
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
      assertThat(builder("LowerCasePrecision").withField("value", Type.DOUBLE).withPrecision("millisecond").toSQL().getFirst())
          .contains("PRECISION MILLISECOND");

      assertThat(builder("LowerCaseCreated").withField("value", Type.DOUBLE).withPrecision("millisecond").create()
          .getPrecision()).isEqualTo("MILLISECOND");

      for (final String sql : builder("LowerCaseViaSQL").withField("value", Type.DOUBLE).withPrecision("millisecond").toSQL())
        database.command("sql", sql);
      assertThat(((TimeSeriesType) database.getSchema().getType("LowerCaseViaSQL")).getPrecision()).isEqualTo("MILLISECOND");
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

    /**
     * A back-quote in a name used to be refused outright, on the ground that the grammar has no escape for it. It
     * has one - {@code QUOTED_IDENTIFIER : BACKTICK ( ~[`\\] | '\\' . )+ BACKTICK} - so the name is escaped and
     * carried instead (issue #7740), and the injection it was refused to prevent is still prevented: the whole
     * thing stays ONE identifier and nothing in it runs as statement text.
     */
    @Test
    void anIdentifierCarryingABackQuoteIsEscapedRatherThanConcatenated() {
      database.command("sql", "CREATE DOCUMENT TYPE V");

      final String injected = "Injected`; DROP TYPE `V";
      final String sql = builder(injected).withField("value", Type.DOUBLE).toSQL().getFirst();
      assertParses(sql);

      database.command("sql", sql);

      assertThat(database.getSchema().existsType(injected)).as("the name is a name, odd as it is").isTrue();
      assertThat(database.getSchema().existsType("V")).as("and the statement it tried to smuggle never ran").isTrue();
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

  /**
   * Regression for #7581: the TimeSeries hierarchy check covers the whole transitive hierarchy.
   * <p>
   * Regression test for a gap in the #7581 fix found during PR #7627's review: {@code checkTimeSeriesHierarchy}
   * originally checked only {@code this} and the super type about to be linked, so it missed a TIMESERIES type
   * sitting deeper in {@code this}'s existing descendant subtree.
   * <p>
   * That subtree can only exist on a database written before #7581 was fixed - the direct case
   * ({@code TimeSeriesType.addSuperType(ordinaryType)}) is refused unconditionally at runtime, so it is simulated
   * here the same way a pre-fix export/reload would produce it: wiring the in-memory super/subtype lists directly,
   * bypassing {@code addSuperType} and its checks entirely, exactly as {@code LocalSchema.readConfiguration()} does
   * for a hierarchy it is loading rather than validating.
   * <p>
   * Once that legacy shape exists, linking a brand new, entirely ordinary super type onto the ordinary type in the
   * middle must still be refused: the new super type's properties flow down through the middle type to the
   * TIMESERIES type at the bottom, which is exactly the silent-drop {@code TimeSeriesColumnConstraintsTest} closes
   * for the direct case.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TransitiveTimeSeriesHierarchyTest extends TestHelper {

    @Test
    void aSuperTypeLinkIsRefusedWhenATimeSeriesTypeIsBelowInTheExistingSubtree() {
      database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (s STRING) FIELDS (v DOUBLE)");
      database.command("sql", "CREATE DOCUMENT TYPE Middle");
      database.command("sql", "CREATE DOCUMENT TYPE NewParent");

      // Wires "Reading extends Middle" directly, bypassing addSuperType()/checkTimeSeriesHierarchy() - the only way
      // to reach this shape, since the direct call is refused unconditionally outside a schema-file load.
      final LocalDocumentType reading = (LocalDocumentType) database.getSchema().getType("Reading");
      final LocalDocumentType middle = (LocalDocumentType) database.getSchema().getType("Middle");
      reading.superTypes.add(middle);
      middle.subTypes.add(reading);

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Middle SUPERTYPE +NewParent"))
          .hasMessageContaining("TIMESERIES")
          .hasMessageContaining("Reading");
    }
  }

  /**
   * Regression for #7689: {@code CREATE TIMESERIES TYPE} now expresses a WHOLE TimeSeries type - the per-column codecs and the
   * downsampling policy included - so a caller that can only reach the schema through {@code command("sql", ...)}
   * creates the type in one statement.
   * <p>
   * The two things that used to be missing had one consequence each:
   * <ul>
   * <li>the policy lived only in {@code ALTER TIMESERIES TYPE ... ADD DOWNSAMPLING POLICY}, so
   * {@code RemoteTimeSeriesTypeBuilder} issued two statements and a failure between them left the type on the server
   * WITHOUT its policy while the caller got an exception - a torn create with no compensation;</li>
   * <li>a column's codec had no expression at all, so a builder carrying an explicit one was refused outright, which
   * made recreating a TIMESERIES type from a logical export impossible against a remote database - the JSONL importer
   * reads the codec out of the export precisely because it is not re-derivable (issue #5475).</li>
   * </ul>
   * Both close the same way: one statement that says everything, which removes the window by construction rather than
   * by retry or compensation. The remote half of the acceptance criteria lives in
   * {@code Issue7689RemoteTimeSeriesTypeCompleteCreateIT}.
   */
  @Nested
  class CreateTimeSeriesTypeCompleteDeclarationTest extends TestHelper {

    private TimeSeriesTypeBuilder builder(final String name) {
      return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
    }

    private TimeSeriesType typeOf(final String name) {
      final DocumentType type = database.getSchema().getType(name);
      assertThat(type).isInstanceOf(TimeSeriesType.class);
      return (TimeSeriesType) type;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Entry point 1: the grammar and the SQL execution path
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aFieldCodecNamedInTheCreateStatementReachesTheColumn() {
      database.command("sql", "CREATE TIMESERIES TYPE FieldCodec TIMESTAMP ts "
          + "FIELDS (temperature DOUBLE CODEC DICTIONARY, humidity DOUBLE)");

      final TimeSeriesType type = typeOf("FieldCodec");
      assertThat(type.getTsColumn("temperature").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
      // The column that named none keeps the default, which for a DOUBLE field is GORILLA_XOR: a CODEC clause is
      // per column, not a switch for the statement.
      assertThat(type.getTsColumn("humidity").getCompressionHint())
          .isEqualTo(ColumnDefinition.defaultCodecFor(Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    @Test
    void aTagCodecNamedInTheCreateStatementReachesTheColumn() {
      database.command("sql", "CREATE TIMESERIES TYPE TagCodec TIMESTAMP ts "
          + "TAGS (zone INTEGER CODEC SIMPLE8B, host STRING) FIELDS (v DOUBLE)");

      final TimeSeriesType type = typeOf("TagCodec");
      assertThat(type.getTsColumn("zone").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
      assertThat(type.getTsColumn("host").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
    }

    @Test
    void aTimestampCodecNamedInTheCreateStatementReachesTheColumn() {
      database.command("sql",
          "CREATE TIMESERIES TYPE TsCodec TIMESTAMP ts PRECISION MILLISECOND CODEC DELTA_OF_DELTA FIELDS (v DOUBLE)");

      final TimeSeriesType type = typeOf("TsCodec");
      assertThat(type.getTimestampColumn()).isEqualTo("ts");
      assertThat(type.getPrecision()).isEqualTo("MILLISECOND");
      assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
      assertThat(type.getTsColumn("ts").getRole()).isEqualTo(ColumnDefinition.ColumnRole.TIMESTAMP);
    }

    @Test
    void aDownsamplingPolicyNamedInTheCreateStatementReachesTheType() {
      database.command("sql", "CREATE TIMESERIES TYPE PolicyInCreate TIMESTAMP ts FIELDS (v DOUBLE) "
          + "DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS");

      assertThat(typeOf("PolicyInCreate").getDownsamplingTiers()).containsExactly(
          new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
          new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
    }

    @Test
    void tiersDeclaredOutOfOrderInTheCreateEndUpSortedLikeTheAlterSortsThem() {
      // ALTER ... ADD DOWNSAMPLING POLICY sorts the tiers it parses. A policy has to mean the same thing whichever
      // statement declares it, so the CREATE clause sorts them too.
      database.command("sql", "CREATE TIMESERIES TYPE UnorderedCreate TIMESTAMP ts FIELDS (v DOUBLE) "
          + "DOWNSAMPLING POLICY AFTER 30 DAYS GRANULARITY 1 DAYS AFTER 7 DAYS GRANULARITY 1 HOURS");

      database.command("sql", "CREATE TIMESERIES TYPE UnorderedAlter TIMESTAMP ts FIELDS (v DOUBLE)");
      database.command("sql", "ALTER TIMESERIES TYPE UnorderedAlter ADD DOWNSAMPLING POLICY "
          + "AFTER 30 DAYS GRANULARITY 1 DAYS AFTER 7 DAYS GRANULARITY 1 HOURS");

      assertThat(typeOf("UnorderedCreate").getDownsamplingTiers())
          .isEqualTo(typeOf("UnorderedAlter").getDownsamplingTiers())
          .containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
              new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
    }

    @Test
    void everyDeclaredClauseCanAppearInOneStatement() {
      database.command("sql", """
          CREATE TIMESERIES TYPE Everything
            TIMESTAMP ts PRECISION MILLISECOND CODEC DELTA_OF_DELTA
            TAGS (host STRING, zone INTEGER CODEC DICTIONARY)
            FIELDS (cpu DOUBLE CODEC DICTIONARY, mem LONG)
            SHARDS 3
            RETENTION 90 DAYS
            COMPACTION_INTERVAL 2 HOURS
            DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS
          """);

      final TimeSeriesType type = typeOf("Everything");
      assertThat(type.getPrecision()).isEqualTo("MILLISECOND");
      assertThat(type.getShardCount()).isEqualTo(3);
      assertThat(type.getRetentionMs()).isEqualTo(90L * 86_400_000L);
      assertThat(type.getCompactionBucketIntervalMs()).isEqualTo(2L * 3_600_000L);
      assertThat(type.getDownsamplingTiers()).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));
      assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
      assertThat(type.getTsColumn("zone").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
      assertThat(type.getTsColumn("cpu").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
      assertThat(type.getTsColumn("mem").getCompressionHint())
          .isEqualTo(ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD));
    }

    @Test
    void anUnknownCodecNameIsRefusedWithTheCodecsThereAre() {
      assertThatThrownBy(() -> database.command("sql",
          "CREATE TIMESERIES TYPE BadCodec TIMESTAMP ts FIELDS (v DOUBLE CODEC ZSTD)"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("v")
          .hasMessageContaining("ZSTD")
          .hasMessageContaining("GORILLA_XOR");

      assertThat(database.getSchema().existsType("BadCodec")).isFalse();
    }

    @Test
    void codecRemainsUsableAsAnOrdinaryIdentifier() {
      // CODEC is a new keyword token. Every keyword the TIMESERIES clauses introduced is in the parser's
      // identifier allow-list for exactly this reason: a column, or a type, may already be called `codec`.
      database.command("sql", "CREATE TIMESERIES TYPE codec TIMESTAMP codec TAGS (policy STRING) FIELDS (v DOUBLE)");

      final TimeSeriesType type = typeOf("codec");
      assertThat(type.getTimestampColumn()).isEqualTo("codec");
      assertThat(type.getTsColumnNames()).contains("codec", "policy", "v");

      // And as a column name in a statement that also uses CODEC as the keyword, which is where a grammar that
      // resolved the ambiguity the wrong way would show up.
      database.command("sql", "CREATE TIMESERIES TYPE CodecNamedColumn TIMESTAMP ts FIELDS (codec DOUBLE CODEC DICTIONARY)");
      assertThat(typeOf("CodecNamedColumn").getTsColumn("codec").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);

      database.command("sql", "CREATE VERTEX TYPE CodecVertex");
      database.command("sql", "CREATE PROPERTY CodecVertex.codec STRING");
      assertThat(database.getSchema().getType("CodecVertex").existsProperty("codec")).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Entry point 2: TimeSeriesTypeBuilder.toSQL() - what a remote create issues
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void aBuilderCarryingEverythingRendersAsExactlyOneStatement() {
      // This is the whole point of the issue: with the policy in the CREATE there is no second statement, so there is
      // no window in which the type exists without it.
      final List<String> statements = builder("OneStatement")
          .withColumn(new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY))
          .withField("mem", Type.LONG)
          .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
              new DownsamplingTier(30L * 86_400_000L, 86_400_000L)))
          .toSQL();

      assertThat(statements).hasSize(1);
      assertThat(statements.getFirst())
          .startsWith("CREATE TIMESERIES TYPE `OneStatement`")
          .contains("`cpu` DOUBLE CODEC DICTIONARY")
          .contains("DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS")
          .doesNotContain("ALTER");
    }

    @Test
    void aColumnCarryingItsDefaultCodecRendersWithoutACodecClause() {
      // Every ColumnDefinition carries a codec, so "explicit" is only visible as "differs from the default". Naming
      // the default anyway would pin today's default table into the DDL, and a type recreated from that statement
      // after the table changed would get the old codec where the same builder code gets the new one embedded.
      final String sql = builder("DefaultCodecs").withField("v", Type.DOUBLE).withTag("host", Type.STRING)
          .toSQL().getFirst();

      assertThat(sql).doesNotContain("CODEC");
    }

    @Test
    void theRenderedStatementRecreatesTheSameTypeTheBuilderCreatesInPlace() {
      // One body of builder code, applied twice: once through create(), once through the SQL it renders. The codec is
      // the column property this pins that issue #7399's parity test could not, because the builder refused to render
      // a column that carried one.
      final TimeSeriesType viaBuilder = configure(builder("CodecViaBuilder")).create();

      for (final String sql : configure(builder("CodecViaSQL")).toSQL())
        database.command("sql", sql);
      final TimeSeriesType viaSQL = typeOf("CodecViaSQL");

      assertThat(viaSQL.getPrecision()).isEqualTo(viaBuilder.getPrecision());
      assertThat(viaSQL.getShardCount()).isEqualTo(viaBuilder.getShardCount());
      assertThat(viaSQL.getRetentionMs()).isEqualTo(viaBuilder.getRetentionMs());
      assertThat(viaSQL.getCompactionBucketIntervalMs()).isEqualTo(viaBuilder.getCompactionBucketIntervalMs());
      assertThat(viaSQL.getDownsamplingTiers()).isEqualTo(viaBuilder.getDownsamplingTiers());
      assertThat(viaSQL.getTsColumnNames()).isEqualTo(viaBuilder.getTsColumnNames());

      for (final ColumnDefinition expected : viaBuilder.getTsColumns()) {
        final ColumnDefinition actual = viaSQL.getTsColumn(expected.getName());
        assertThat(actual).as(expected.getName()).isNotNull();
        assertThat(actual.getDataType()).as(expected.getName()).isEqualTo(expected.getDataType());
        assertThat(actual.getRole()).as(expected.getName()).isEqualTo(expected.getRole());
        assertThat(actual.getCompressionHint()).as(expected.getName()).isEqualTo(expected.getCompressionHint());
      }
    }

    /**
     * The shape a logical restore hands the builder: every column resolved, codec included, plus the policy. This is
     * what {@code JsonlImporterFormat.createTimeSeriesType} builds out of an export, and what used to be refused by
     * {@code toSQL()} and so could not be restored against a remote database at all.
     */
    private static TimeSeriesTypeBuilder configure(final TimeSeriesTypeBuilder builder) {
      return builder
          .withPrecision("MILLISECOND")
          .withColumn(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.DICTIONARY))
          .withColumn(new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG,
              TimeSeriesCodec.SIMPLE8B))
          .withColumn(new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD,
              TimeSeriesCodec.DICTIONARY))
          .withField("mem", Type.LONG)
          .withShards(3)
          .withRetention(90L * 86_400_000L)
          .withCompactionBucketInterval(2L * 3_600_000L)
          .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
              new DownsamplingTier(30L * 86_400_000L, 86_400_000L)));
    }

    @Test
    void aTimestampColumnCarryingAnExplicitCodecRendersAndReparsesToTheSameCodec() {
      final String sql = database.getSchema().buildTimeSeriesType().withName("TsCodecRender")
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP,
              TimeSeriesCodec.DELTA_OF_DELTA))
          .withField("v", Type.DOUBLE)
          .toSQL().getFirst();

      assertThat(sql).contains("TIMESTAMP `ts` CODEC DELTA_OF_DELTA");

      database.command("sql", sql);
      assertThat(typeOf("TsCodecRender").getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Entry point 3: CreateTimeSeriesTypeStatement.toString() - the statement's own rendering
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void theStatementPrintsBackToSQLThatParsesToAnEqualStatement() {
      // toString() is what EXPLAIN and the statement cache show. It used to print RETENTION and COMPACTION_INTERVAL as
      // a bare millisecond count, which the parser reads as DAYS - so the printed form declared a retention 86.4
      // million times the original. Parsing the print and comparing statements is the assertion that catches that for
      // every clause at once, the two this issue adds included.
      final String original = "CREATE TIMESERIES TYPE RoundTrip TIMESTAMP ts PRECISION MILLISECOND CODEC SIMPLE8B "
          + "TAGS (host STRING, zone INTEGER CODEC SIMPLE8B) FIELDS (cpu DOUBLE CODEC DICTIONARY, mem LONG) "
          + "SHARDS 3 RETENTION 90 DAYS COMPACTION_INTERVAL 2 HOURS "
          + "DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS";

      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final Statement parsed = parser.parse(original);
      final String printed = parsed.toString();

      assertThat(printed).contains("CODEC SIMPLE8B").contains("CODEC DICTIONARY")
          .contains("RETENTION 90 DAYS").contains("COMPACTION_INTERVAL 2 HOURS")
          .contains("DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");

      assertThat(parser.parse(printed)).isEqualTo(parsed);
    }

    @Test
    void aCopiedStatementCarriesTheCodecsAndTheTiers() {
      final Statement parsed = new SQLAntlrParser(null).parse(
          "CREATE TIMESERIES TYPE Copied TIMESTAMP ts CODEC SIMPLE8B FIELDS (v DOUBLE CODEC NONE) "
              + "DOWNSAMPLING POLICY AFTER 1 DAYS GRANULARITY 1 HOURS");

      assertThat(parsed.copy()).isEqualTo(parsed);
      assertThat(parsed.copy().toString()).isEqualTo(parsed.toString());
    }
  }

  /**
   * Regression for #7702: a SQL-rendered {@code CREATE TIMESERIES TYPE} keeps the column order it was given, roles
   * interleaved included.
   * <p>
   * The grammar used to have exactly one {@code TAGS} slot and one {@code FIELDS} slot, so
   * {@link TimeSeriesTypeBuilder#toSQL()} could only regroup a declaration that interleaved them. That order is not
   * presentation: a column index is a position in the type's column list and a TimeSeries sample is a positional
   * array, so the SAME builder body created one type embedded - where {@code create()} stores the declaration as
   * given - and a DIFFERENT one through SQL, which is what a remote schema builder issues. A logical restore that
   * rebuilt a type through that path and then mapped the export's sample arrays onto it would have put every value
   * in the wrong column; {@code JsonlImporterFormat} could only refuse.
   * <p>
   * {@code tsColumnGroup} is now repeatable and may appear in either order, so the rendering carries the whole
   * declaration. A run of consecutive same-role columns still renders as ONE group, which is why every statement
   * written against the old grammar renders and parses exactly as it did.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7702">issue #7702</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class InterleavedTagAndFieldOrderTest extends TestHelper {

    /** {@code t1} and {@code f1} deliberately share a data type: a shift between them is silent, not an error. */
    private TimeSeriesTypeBuilder interleaved(final String name) {
      return database.getSchema().buildTimeSeriesType().withName(name)
          .withTimestamp("ts")
          .withField("f1", Type.DOUBLE)
          .withTag("t1", Type.STRING)
          .withField("f2", Type.DOUBLE)
          .withTag("t2", Type.STRING);
    }

    private static List<String> columnNames(final TimeSeriesType type) {
      final List<String> names = new ArrayList<>(type.getTsColumns().size());
      for (final ColumnDefinition col : type.getTsColumns())
        names.add(col.getName());
      return names;
    }

    private TimeSeriesType typeOf(final String name) {
      return (TimeSeriesType) database.getSchema().getType(name);
    }

    /**
     * The measurement issue #7702 was filed on: the embedded create and the SQL rendering of the same builder body
     * produced two different types.
     */
    @Test
    void theSameBuilderBodyProducesTheSameColumnOrderEmbeddedAndThroughSQL() {
      final TimeSeriesType embedded = interleaved("InterleavedEmbedded").create();
      assertThat(columnNames(embedded)).containsExactly("ts", "f1", "t1", "f2", "t2");

      final List<String> sql = interleaved("InterleavedSQL").toSQL();
      assertThat(sql).hasSize(1);
      database.command("sql", sql.getFirst());

      assertThat(columnNames(typeOf("InterleavedSQL")))
          .as("the DDL the builder renders must create the type the builder creates")
          .isEqualTo(columnNames(embedded));
    }

    /** The rendering itself, so a regrouping regression is readable rather than inferred from a column list. */
    @Test
    void theRenderedStatementSpellsOneGroupPerRunOfConsecutiveColumns() {
      assertThat(interleaved("Rendered").toSQL().getFirst()).isEqualTo(
          "CREATE TIMESERIES TYPE `Rendered` TIMESTAMP `ts` FIELDS (`f1` DOUBLE) TAGS (`t1` STRING) "
              + "FIELDS (`f2` DOUBLE) TAGS (`t2` STRING)");
    }

    /**
     * A declaration that does NOT interleave renders exactly the one TAGS group and one FIELDS group it always did.
     * This is the compatibility half: every {@code CREATE TIMESERIES TYPE} anyone has ever written is one of these.
     */
    @Test
    void aCanonicalDeclarationRendersTheSameSingleGroupsAsBefore() {
      final String sql = database.getSchema().buildTimeSeriesType().withName("Canonical")
          .withTimestamp("ts")
          .withTag("host", Type.STRING).withTag("zone", Type.STRING)
          .withField("v1", Type.DOUBLE).withField("v2", Type.DOUBLE)
          .toSQL().getFirst();

      assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `Canonical` TIMESTAMP `ts` "
          + "TAGS (`host` STRING, `zone` STRING) FIELDS (`v1` DOUBLE, `v2` DOUBLE)");
    }

    /** The grammar accepts the groups in either order, which is what makes a FIELD-first type spellable at all. */
    @Test
    void theParserAcceptsFieldsBeforeTagsAndKeepsTheOrder() {
      database.command("sql",
          "CREATE TIMESERIES TYPE FieldsFirst TIMESTAMP ts FIELDS (v DOUBLE) TAGS (host STRING)");

      assertThat(columnNames(typeOf("FieldsFirst"))).containsExactly("ts", "v", "host");
    }

    /** Repeated groups of the same role stay separate columns in the order written, not a merged one. */
    @Test
    void repeatedGroupsOfTheSameRoleKeepTheirPositions() {
      database.command("sql", "CREATE TIMESERIES TYPE RepeatedGroups TIMESTAMP ts "
          + "TAGS (a STRING) FIELDS (x DOUBLE) TAGS (b STRING) TAGS (c STRING) FIELDS (y DOUBLE)");

      final TimeSeriesType type = typeOf("RepeatedGroups");
      assertThat(columnNames(type)).containsExactly("ts", "a", "x", "b", "c", "y");
      assertThat(type.getTsColumn("b").getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);
      assertThat(type.getTsColumn("y").getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
    }

    /**
     * The statement must PRINT what it parsed: {@code toString()} feeds {@code EXPLAIN} and the statement cache, and
     * a printer that regrouped would reintroduce the divergence one level further down.
     */
    @Test
    void printingAndReparsingAnInterleavedStatementIsIdempotent() {
      final String sql = "CREATE TIMESERIES TYPE `RoundTrip` TIMESTAMP `ts` FIELDS (`f1` DOUBLE) "
          + "TAGS (`t1` STRING) FIELDS (`f2` DOUBLE)";

      final Statement parsed = new SQLAntlrParser(null).parse(sql);
      assertThat(parsed.toString()).isEqualTo(sql);

      final Statement reparsed = new SQLAntlrParser(null).parse(parsed.toString());
      assertThat(reparsed).isEqualTo(parsed);
    }

    /** Per-column codecs travel with their column, wherever in the order it sits (issue #7689 crossed with this). */
    @Test
    void aCodecFollowsItsColumnAcrossTheInterleaving() {
      final String sql = database.getSchema().buildTimeSeriesType().withName("InterleavedCodecs")
          .withTimestamp("ts")
          .withColumn(new ColumnDefinition("f1", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY))
          .withColumn(new ColumnDefinition("t1", Type.INTEGER, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.SIMPLE8B))
          .withColumn(new ColumnDefinition("f2", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.GORILLA_XOR))
          .toSQL().getFirst();

      assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `InterleavedCodecs` TIMESTAMP `ts` "
          + "FIELDS (`f1` DOUBLE CODEC DICTIONARY) TAGS (`t1` INTEGER CODEC SIMPLE8B) "
          + "FIELDS (`f2` DOUBLE CODEC GORILLA_XOR)");

      database.command("sql", sql);
      final TimeSeriesType type = typeOf("InterleavedCodecs");
      assertThat(columnNames(type)).containsExactly("ts", "f1", "t1", "f2");
      assertThat(type.getTsColumn("f1").getCompressionHint()).isEqualTo(TimeSeriesCodec.DICTIONARY);
      assertThat(type.getTsColumn("t1").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
      assertThat(type.getTsColumn("f2").getCompressionHint()).isEqualTo(TimeSeriesCodec.GORILLA_XOR);
    }

    /**
     * The TIMESTAMP clause is a member of the same ordered list, so a type whose timestamp is not its first column -
     * which {@code create()} has always accepted and stored - is now spellable too. This was the second axis of the
     * same divergence: {@code TimeSeriesTypeSchemaTest.TimeSeriesBuilderParityTest} pinned it as a regrouping the rendering could
     * not avoid.
     */
    @Test
    void aTimestampDeclaredAfterAColumnKeepsItsPosition() {
      final TimeSeriesType embedded = database.getSchema().buildTimeSeriesType().withName("TimestampSecondEmbedded")
          .withField("f1", Type.DOUBLE)
          .withTimestamp("ts")
          .withTag("t1", Type.STRING)
          .create();
      assertThat(columnNames(embedded)).containsExactly("f1", "ts", "t1");

      final String sql = database.getSchema().buildTimeSeriesType().withName("TimestampSecondSQL")
          .withField("f1", Type.DOUBLE)
          .withTimestamp("ts")
          .withTag("t1", Type.STRING)
          .toSQL().getFirst();
      assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `TimestampSecondSQL` FIELDS (`f1` DOUBLE) TIMESTAMP `ts` "
          + "TAGS (`t1` STRING)");

      database.command("sql", sql);
      final TimeSeriesType rendered = typeOf("TimestampSecondSQL");
      assertThat(columnNames(rendered)).isEqualTo(columnNames(embedded));
      assertThat(rendered.getTimestampColumn()).isEqualTo("ts");
    }

    /** The PRECISION and CODEC clauses travel with the TIMESTAMP wherever it sits. */
    @Test
    void theTimestampClauseKeepsItsPrecisionAndCodecOutOfPosition() {
      final String sql = database.getSchema().buildTimeSeriesType().withName("TimestampClauses")
          .withTag("t1", Type.STRING)
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP, TimeSeriesCodec.DELTA_OF_DELTA))
          .withPrecision("millisecond")
          .withField("f1", Type.DOUBLE)
          .toSQL().getFirst();

      assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `TimestampClauses` TAGS (`t1` STRING) "
          + "TIMESTAMP `ts` PRECISION MILLISECOND CODEC DELTA_OF_DELTA FIELDS (`f1` DOUBLE)");

      database.command("sql", sql);
      final TimeSeriesType type = typeOf("TimestampClauses");
      assertThat(columnNames(type)).containsExactly("t1", "ts", "f1");
      assertThat(type.getPrecision()).isEqualTo("MILLISECOND");
      assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.DELTA_OF_DELTA);
    }

    /**
     * A repeatable member list can spell a SECOND timestamp, which the type does not have. Refused where it is read,
     * naming the column - the same rule {@code TimeSeriesTypeBuilder.validate()} enforces for every other path
     * (issue #7740).
     */
    @Test
    void aSecondTimestampClauseIsRefusedByName() {
      assertThatThrownBy(() -> database.command("sql",
          "CREATE TIMESERIES TYPE TwoTimestamps TIMESTAMP ts FIELDS (v DOUBLE) TIMESTAMP ts2"))
          .hasMessageContaining("exactly one TIMESTAMP column")
          .hasMessageContaining("ts2");

      assertThat(database.getSchema().existsType("TwoTimestamps")).isFalse();
    }

    /**
     * The rendering refuses a second TIMESTAMP column itself rather than dropping it, so the statement it produces
     * cannot depend on the caller having run {@code validate()} first - which {@code toSQL()} does, making this
     * unreachable through it (code review on PR #7757).
     */
    @Test
    void theRenderingRefusesASecondTimestampColumnRatherThanDroppingIt() {
      final TimeSeriesTypeBuilder builder = database.getSchema().buildTimeSeriesType().withName("TwoTimestampColumns")
          .withTimestamp("ts")
          .withField("f1", Type.DOUBLE)
          .withColumn(new ColumnDefinition("ts2", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP));

      // Through toSQL(), validate() speaks first and the rendering is never reached.
      assertThatThrownBy(builder::toSQL)
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("exactly one TIMESTAMP column");
    }

    /**
     * And the type WORKS in that order: a sample is a positional array, so the point of preserving the order is that
     * a value written under a name reads back under the same name.
     */
    @Test
    void samplesReadBackUnderTheColumnTheyWereWrittenTo() {
      database.command("sql", "CREATE TIMESERIES TYPE InterleavedSamples TIMESTAMP ts "
          + "FIELDS (f1 DOUBLE) TAGS (t1 STRING) FIELDS (f2 DOUBLE) SHARDS 1");

      database.transaction(() -> database.command("sql",
          "INSERT INTO InterleavedSamples SET ts = 1000, f1 = 1.5, t1 = 'west', f2 = 2.5"));

      final var result = database.query("sql", "SELECT f1, t1, f2 FROM InterleavedSamples").next();
      assertThat(result.<Double>getProperty("f1")).isEqualTo(1.5);
      assertThat(result.<String>getProperty("t1")).isEqualTo("west");
      assertThat(result.<Double>getProperty("f2")).isEqualTo(2.5);
    }
  }

  /**
   * Regression for #7703: a rendered {@code CREATE TIMESERIES TYPE} must name a codec its caller NAMED, even when that codec
   * is what this build's default table would have resolved anyway.
   * <p>
   * The renderer used to omit the clause on "equals the default", because a {@link ColumnDefinition} always carries a
   * codec and that was the only way to ask whether one had been chosen. That is the right answer for
   * {@code withTag}/{@code withField}, which never named one: pinning the resolved codec into the DDL would freeze
   * today's table into every rendered statement. It is the wrong answer for a RESTORE. A {@code .jsonl} export
   * records {@code "compression"} per column precisely because the codec is NOT re-derivable (issue #5475), and
   * {@code JsonlImporterFormat.createTimeSeriesType} reads it back for the same reason; rendered through the DDL path
   * into a build whose default for that {@code (type, role)} pair has since moved, the omission silently substitutes
   * the NEW default for the exported codec, and the restored type is not the exported one.
   * <p>
   * {@code ColumnDefinition.legacyCodecFor} is the proof the table moves: it exists because it already has.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExplicitCodecSurvivesDefaultTableChangeTest extends TestHelper {

    private TimeSeriesTypeBuilder builder(final String name) {
      return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
    }

    /**
     * The defect itself: a column that named the codec the default table happens to return today renders WITH the
     * clause, so the receiving build applies it rather than re-deriving it.
     */
    @Test
    void aNamedCodecThatEqualsTodaysDefaultStillRenders() {
      final TimeSeriesCodec todaysDefault =
          ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD);

      final String sql = builder("NamedDefaultCodec")
          .withColumn(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD, todaysDefault))
          .toSQL().getFirst();

      assertThat(sql)
          .as("the codec was NAMED, so the statement must carry it instead of leaving it to the receiver's table")
          .contains("`v` LONG CODEC " + todaysDefault.name());

      database.command("sql", sql);
      assertThat(((TimeSeriesType) database.getSchema().getType("NamedDefaultCodec")).getTsColumn("v")
          .getCompressionHint()).isEqualTo(todaysDefault);
    }

    /**
     * The other half, unchanged: a column built by {@code withTag}/{@code withField}/{@code withTimestamp} named
     * nothing, so the statement names nothing and the receiver resolves its own default. This is what stops the fix
     * from freezing the default table into every create a user writes.
     */
    @Test
    void aDerivedCodecStillRendersWithoutAClause() {
      final String sql = builder("DerivedCodecs").withField("v", Type.DOUBLE).withTag("host", Type.STRING)
          .toSQL().getFirst();

      assertThat(sql).doesNotContain("CODEC");
    }

    /**
     * The flag is a property of the column, not of the renderer, so every hop a definition makes carries it: the two
     * constructors are the whole distinction, and the 4-argument one is what the importer, the SQL parser's
     * {@code CODEC} arm and {@code LocalTimeSeriesType.fromJSON} all use.
     */
    @Test
    void theConstructorIsWhatDecidesWhetherACodecWasNamed() {
      assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD).isExplicitCodec())
          .as("derived from the default table")
          .isFalse();
      assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.DICTIONARY)
          .isExplicitCodec())
          .as("named, and different from the default")
          .isTrue();
      assertThat(new ColumnDefinition("v", Type.LONG, ColumnDefinition.ColumnRole.FIELD,
          ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.FIELD)).isExplicitCodec())
          .as("named, and identical to the default - the case the old 'differs from the default' test could not see")
          .isTrue();
    }

    /**
     * A {@code null} codec is "none named" rather than a stored null: the column resolves the default table and is
     * indistinguishable from the 3-argument one, so no read path has to cope with a null codec and no renderer emits
     * a clause for a codec nobody chose.
     */
    @Test
    void aNullCodecIsNoneNamedRatherThanAStoredNull() {
      final ColumnDefinition column = new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, null);

      assertThat(column.isExplicitCodec()).isFalse();
      assertThat(column.getCompressionHint())
          .isEqualTo(ColumnDefinition.defaultCodecFor(Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    /**
     * The timestamp column goes through the same rule, on the path {@code renderCreate} gives it of its own:
     * {@code withTimestamp} names nothing, {@code withColumn} does.
     */
    @Test
    void theTimestampColumnFollowsTheSameRule() {
      final TimeSeriesCodec todaysDefault =
          ColumnDefinition.defaultCodecFor(Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP);

      final String named = database.getSchema().buildTimeSeriesType().withName("NamedTsCodec")
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP, todaysDefault))
          .withField("v", Type.DOUBLE)
          .toSQL().getFirst();

      assertThat(named).contains("TIMESTAMP `ts` CODEC " + todaysDefault.name());
      assertThat(builder("DerivedTsCodec").withField("v", Type.DOUBLE).toSQL().getFirst()).doesNotContain("CODEC");
    }
  }
}
