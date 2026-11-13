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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: time series type schema.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class TimeSeriesTypeSchemaTest {
  /**
   * Regression for #7740: time series builder parity.
   *
   * Issue #7740: three divergences between the embedded and the remote TimeSeries schema paths, which are meant to
   * be the same builder body producing the same type.
   * <ol>
   * <li>{@code quote()} refused a back-quote and passed a BACKSLASH through, which is the defect #5849 fixed once
   * already in {@code MCPToolUtils}: the grammar's {@code QUOTED_IDENTIFIER : BACKTICK ( ~[`\\] | '\\' . )+ BACKTICK}
   * gives the backslash its escaping meaning, so {@code a\b} rendered as a name the server stored as {@code ab} -
   * and {@code create()} then failed with "Type with name 'a\b' was not found" over a fully built type under a name
   * nobody asked for. A trailing backslash was a parse error instead. The embedded path accepted both names.</li>
   * <li>The column ORDER: the SQL rendering had one slot for the timestamp, one for TAGS and one for FIELDS, so the
   * remote path regrouped; the embedded one replayed insertion order. One builder body, two types. Closed on the
   * grammar side by issue #7702, which made the column members repeatable and orderable, so both paths now keep the
   * declaration.</li>
   * <li>The single-TIMESTAMP rule was enforced only when rendering SQL, so a two-TIMESTAMP builder - which
   * {@code JsonlImporterFormat} can produce by looping {@code withColumn} over an export - built an embedded type
   * whose {@code getTimestampColumn()} and {@code findTimestampColumnIndex()} named different columns.</li>
   * </ol>
   * The fourth item of the issue, {@code RemoteSchema.reload()} keeping a cached type's Java class, is pinned in
   * {@code Issue7740RemoteSchemaReloadKindIT} - it needs a server.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7740">issue #7740</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesBuilderParity extends TestHelper {

    private TimeSeriesTypeBuilder builder(final String name) {
      return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
    }

    private static List<String> columnNames(final TimeSeriesType type) {
      final List<String> names = new ArrayList<>();
      for (final ColumnDefinition col : type.getTsColumns())
        names.add(col.getName());
      return names;
    }

    // ---- 1. the identifier escaping ----

    /**
     * A name with a backslash renders as DDL that parses AND that names the column the caller asked for. Rendering
     * alone is not enough to see the bug: {@code `a\b`} parses fine, it simply means a different name.
     */
    @Test
    void aBackslashInANameIsEscapedRatherThanPassedThrough() {
      final String odd = "a\\b";
      final String sql = builder("Backslash").withField(odd, Type.DOUBLE).toSQL().getFirst();

      assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
      assertThat(sql).as("the backslash is escaped, so the name survives the round trip").contains("`a\\\\b`");

      database.command("sql", sql);
      assertThat(columnNames((TimeSeriesType) database.getSchema().getType("Backslash")))
          .as("the server built the columns the builder declared, not a name with the backslash eaten")
          .containsExactly("ts", odd);
    }

    /** A name ENDING with a backslash used to be a parse error: unescaped, it swallowed the closing back-quote. */
    @Test
    void aTrailingBackslashDoesNotSwallowTheClosingQuote() {
      final String sql = builder("TrailingBackslash").withField("weird\\", Type.DOUBLE).toSQL().getFirst();

      assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
      database.command("sql", sql);
      assertThat(columnNames((TimeSeriesType) database.getSchema().getType("TrailingBackslash")))
          .containsExactly("ts", "weird\\");
    }

    /** And the back-quote, which used to be refused outright, is escaped like any other special character. */
    @Test
    void aBackQuoteInANameIsEscapedInsteadOfRefused() {
      final String sql = builder("BackQuote").withField("we`ird", Type.DOUBLE).toSQL().getFirst();

      assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
      database.command("sql", sql);
      assertThat(columnNames((TimeSeriesType) database.getSchema().getType("BackQuote")))
          .containsExactly("ts", "we`ird");
    }

    /** An ordinary name renders exactly as it always did: the escaping is not a new quoting style. */
    @Test
    void anOrdinaryNameIsUnchanged() {
      assertThat(builder("Ordinary").withTag("host", Type.STRING).withField("value", Type.DOUBLE).toSQL().getFirst())
          .isEqualTo("CREATE TIMESERIES TYPE `Ordinary` TIMESTAMP `ts` TAGS (`host` STRING) FIELDS (`value` DOUBLE)");
    }

    // ---- 2. the column order ----

    /**
     * A recipe the grammar CAN spell produces the same type either way, which is the parity #7740 asks for.
     */
    @Test
    void theEmbeddedAndTheRenderedPathOrderTheColumnsAlike() {
      final TimeSeriesType embedded = database.getSchema().buildTimeSeriesType().withName("Embedded")
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("cpu", Type.DOUBLE)
          .create();

      database.command("sql", database.getSchema().buildTimeSeriesType().withName("Rendered")
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("cpu", Type.DOUBLE)
          .toSQL().getFirst());
      final TimeSeriesType rendered = (TimeSeriesType) database.getSchema().getType("Rendered");

      assertThat(columnNames(embedded)).containsExactly("ts", "host", "cpu");
      assertThat(columnNames(rendered)).as("one builder body, one column order").isEqualTo(columnNames(embedded));
    }

    /**
     * And a recipe the grammar could not spell is NOT silently turned into a different type on either side.
     * <p>
     * Regrouping the embedded declaration was the first answer to #7740 and it was wrong in a way the engine can
     * feel: the stored order is the type's identity - column indices are positions in it, and a sample is a
     * positional array - so a logical restore, which maps an export's sample arrays onto the type it has just
     * rebuilt, would have put every value in the wrong column ({@code Issue7371PromQLDiscoveryProjectionIT} builds
     * exactly that layout on purpose, and caught it). The second answer was to leave the rendering regrouping and
     * document the difference; issue #7702 removed it instead, by making the grammar's column members repeatable
     * and orderable. This is that same recipe - a FIELD declared before the TIMESTAMP - rendered in full.
     *
     * @see TimeSeriesTypeBuilderTest.InterleavedTagAndFieldOrder
     */
    @Test
    void theRenderedPathSpellsTheDeclarationItWasGiven() {
      final String sql = database.getSchema().buildTimeSeriesType().withName("FieldFirst")
          .withField("cpu", Type.DOUBLE)
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .toSQL().getFirst();

      assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `FieldFirst` FIELDS (`cpu` DOUBLE) TIMESTAMP `ts` "
          + "TAGS (`host` STRING)");

      database.command("sql", sql);
      assertThat(columnNames((TimeSeriesType) database.getSchema().getType("FieldFirst")))
          .as("one builder body, one column order")
          .containsExactly("cpu", "ts", "host");
    }

    /** The embedded path still accepts it, and stores it as given: the engine supports the layout. */
    @Test
    void theEmbeddedPathKeepsTheOrderItWasGiven() {
      final TimeSeriesType type = database.getSchema().buildTimeSeriesType().withName("AsDeclared")
          .withField("cpu", Type.DOUBLE)
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .create();

      assertThat(columnNames(type)).containsExactly("cpu", "ts", "host");
    }

    /** Within a role, declaration order is kept on both paths. */
    @Test
    void declarationOrderSurvivesWithinEachRole() {
      final TimeSeriesType type = database.getSchema().buildTimeSeriesType().withName("WithinRole")
          .withTimestamp("ts")
          .withTag("rack", Type.STRING)
          .withTag("host", Type.STRING)
          .withField("zeta", Type.DOUBLE)
          .withField("alpha", Type.DOUBLE)
          .create();

      assertThat(columnNames(type)).containsExactly("ts", "rack", "host", "zeta", "alpha");
    }

    // ---- 3. the single-TIMESTAMP rule ----

    /** {@code create()} refuses a second TIMESTAMP column, as rendering SQL always did. */
    @Test
    void aSecondTimestampColumnIsRefusedByCreateToo() {
      final TimeSeriesTypeBuilder twoTimestamps = database.getSchema().buildTimeSeriesType().withName("TwoTimestamps")
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
          .withColumn(new ColumnDefinition("ts2", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
          .withField("value", Type.DOUBLE);

      assertThatThrownBy(twoTimestamps::create)
          .as("the rule is the type's, not the grammar's")
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("exactly one TIMESTAMP column");

      assertThatThrownBy(twoTimestamps::toSQL)
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("exactly one TIMESTAMP column");

      assertThat(database.getSchema().existsType("TwoTimestamps"))
          .as("and nothing half-built is left behind")
          .isFalse();
    }

    /** One TIMESTAMP column is still perfectly ordinary. */
    @Test
    void oneTimestampColumnIsAccepted() {
      assertThatNoException().isThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("OneTimestamp")
          .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
          .withField("value", Type.DOUBLE)
          .create());
    }
  }

  /**
   * Regression for #7791: alter time series type duration render.
   *
   * Issue #7791: {@code AlterTimeSeriesTypeStatement.toString()} rendered a downsampling tier as a bare millisecond
   * count - {@code tier.afterMs()}/{@code tier.granularityMs()} appended directly - instead of through
   * {@code renderDuration()} like the identical sibling {@code CreateTimeSeriesTypeStatement} does. The grammar rule
   * {@code downsamplingTierClause} requires a {@code tsTimeUnit} after each {@code INTEGER_LITERAL}, so the rendered
   * ALTER was not valid SQL at all: printing it and parsing it back threw a syntax error.
   * <p>
   * {@code renderDuration()} is now shared on {@link Statement}, next to {@code appendWithSettings}, so both DDL
   * statements that carry a duration in their AST render it the same way and cannot drift apart again.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AlterTimeSeriesTypeDurationRender extends TestHelper {

    @Test
    void theAlterStatementPrintsBackToSQLThatParsesToAnEqualStatement() {
      database.command("sql", "CREATE TIMESERIES TYPE SensorDDL TIMESTAMP ts FIELDS (v DOUBLE)");

      final String original = "ALTER TIMESERIES TYPE SensorDDL ADD DOWNSAMPLING POLICY "
          + "AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS";

      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final Statement parsed = parser.parse(original);
      final String printed = parsed.toString();

      // The bare-millisecond bug rendered "AFTER 604800000 GRANULARITY 3600000 ...", which parses as neither DAYS
      // nor HOURS nor anything else: the grammar requires a unit, so it threw before this assertion could even run.
      assertThat(printed).isEqualTo(original);

      final Statement reparsed = parser.parse(printed);
      assertThat(reparsed).isEqualTo(parsed);
    }

    @Test
    void aTierWithADurationThatIsNotAWholeNumberOfTheLargestUnitStillRendersAndReparses() {
      database.command("sql", "CREATE TIMESERIES TYPE SensorOddDuration TIMESTAMP ts FIELDS (v DOUBLE)");

      // 90 minutes is not a whole number of hours, so renderDuration() falls back to the next-smaller unit that
      // divides it evenly (MINUTES), exactly as CreateTimeSeriesTypeStatement already does for RETENTION/
      // COMPACTION_INTERVAL.
      final String original = "ALTER TIMESERIES TYPE SensorOddDuration ADD DOWNSAMPLING POLICY "
          + "AFTER 90 MINUTES GRANULARITY 5 MINUTES";

      final SQLAntlrParser parser = new SQLAntlrParser(null);
      final Statement parsed = parser.parse(original);
      final String printed = parsed.toString();

      assertThat(parser.parse(printed)).isEqualTo(parsed);
    }

    @Test
    void theExecutedAlterAppliesTheSameTiersTheRoundTrippedStatementWouldReparseTo() {
      database.command("sql", "CREATE TIMESERIES TYPE SensorApplied TIMESTAMP ts FIELDS (v DOUBLE)");
      database.command("sql", "ALTER TIMESERIES TYPE SensorApplied ADD DOWNSAMPLING POLICY "
          + "AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS");

      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("SensorApplied");
      assertThat(type.getDownsamplingTiers()).containsExactly(
          new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
          new DownsamplingTier(30L * 86_400_000L, 86_400_000L));
    }
  }

  /**
   * Regression for #9310: time series codec validation.
   *
   * Issue #9310: {@code CREATE TIMESERIES TYPE ... CODEC x} validated the codec NAME only, so a codec the sealed store has no
   * encoder for (or one that reads numbers, on a text column) was accepted and then failed in every later compaction,
   * permanently and for the whole type. The combination is now refused where the column is built.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesCodecValidation extends TestHelper {

    @Test
    void aCodecWithNoEncoderIsRefusedAtCreate() {
      assertRefused("CREATE TIMESERIES TYPE T1 TIMESTAMP ts FIELDS (v DOUBLE CODEC NONE)", "NONE");
      assertRefused("CREATE TIMESERIES TYPE T2 TIMESTAMP ts FIELDS (v DOUBLE CODEC DELTA_OF_DELTA)", "DELTA_OF_DELTA");
      assertRefused("CREATE TIMESERIES TYPE T3 TIMESTAMP ts TAGS (h STRING CODEC NONE) FIELDS (v DOUBLE)", "NONE");
      assertRefused("CREATE TIMESERIES TYPE T4 TIMESTAMP ts TAGS (h STRING CODEC DELTA_OF_DELTA) FIELDS (v DOUBLE)", "DELTA_OF_DELTA");
    }

    @Test
    void aNumericCodecOnATextColumnIsRefused() {
      assertRefused("CREATE TIMESERIES TYPE T5 TIMESTAMP ts TAGS (h STRING CODEC GORILLA_XOR) FIELDS (v DOUBLE)", "GORILLA_XOR");
      assertRefused("CREATE TIMESERIES TYPE T6 TIMESTAMP ts TAGS (h STRING CODEC SIMPLE8B) FIELDS (v DOUBLE)", "SIMPLE8B");
    }

    @Test
    void theTimestampColumnOnlyTakesDeltaOfDelta() {
      assertRefused("CREATE TIMESERIES TYPE T7 TIMESTAMP ts CODEC GORILLA_XOR FIELDS (v DOUBLE)", "TIMESTAMP");
      assertRefused("CREATE TIMESERIES TYPE T8 TIMESTAMP ts CODEC NONE FIELDS (v DOUBLE)", "TIMESTAMP");
      database.command("sql", "CREATE TIMESERIES TYPE T9 TIMESTAMP ts CODEC DELTA_OF_DELTA FIELDS (v DOUBLE)");
      assertThat(database.getSchema().existsType("T9")).isTrue();
    }

    @Test
    void theBuilderRefusesItToo() {
      final TimeSeriesTypeBuilder builder = new TimeSeriesTypeBuilder(database).withName("B1").withTimestamp("ts")
          .withColumn(new ColumnDefinition("v", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.NONE));
      assertThatThrownBy(builder::create).isInstanceOf(SchemaException.class).hasMessageContaining("NONE");
      assertThat(database.getSchema().existsType("B1")).isFalse();
    }

    @Test
    void aNumericCodecIsOnlyAcceptedForTypesThatRoundTrip() {
      assertRefused("CREATE TIMESERIES TYPE R1 TIMESTAMP ts FIELDS (v DOUBLE CODEC SIMPLE8B)", "SIMPLE8B");
      assertRefused("CREATE TIMESERIES TYPE R2 TIMESTAMP ts FIELDS (v FLOAT CODEC SIMPLE8B)", "SIMPLE8B");
      assertRefused("CREATE TIMESERIES TYPE R3 TIMESTAMP ts FIELDS (v LONG CODEC GORILLA_XOR)", "GORILLA_XOR");
      assertRefused("CREATE TIMESERIES TYPE R4 TIMESTAMP ts TAGS (z INTEGER CODEC GORILLA_XOR) FIELDS (v DOUBLE)", "GORILLA_XOR");
    }

    @Test
    void everyHonouredCombinationIsAcceptedCompactsAndReadsBackUnchanged() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Ok1 TIMESTAMP ts TAGS (h STRING CODEC DICTIONARY, z INTEGER CODEC SIMPLE8B, "
          + "y LONG CODEC DICTIONARY) FIELDS (a DOUBLE CODEC GORILLA_XOR, b LONG CODEC SIMPLE8B, c DOUBLE CODEC DICTIONARY, "
          + "f BOOLEAN CODEC SIMPLE8B) SHARDS 1");
      database.transaction(() -> database.command("sql",
          "INSERT INTO Ok1 SET ts = 1700000000000, h = 'x', z = 5, y = 6, a = 7.5, b = 9007199254740993, c = 1.5, f = true"));
      ((LocalTimeSeriesType) database.getSchema().getType("Ok1")).getEngine().compactAll();
      try (final ResultSet rs = database.query("sql", "SELECT FROM Ok1")) {
        final Result r = rs.next();
        assertThat(r.<String>getProperty("h")).isEqualTo("x");
        assertThat(r.<Number>getProperty("z").longValue()).isEqualTo(5L);
        assertThat(r.<Number>getProperty("a").doubleValue()).isEqualTo(7.5);
        assertThat(r.<Number>getProperty("b").longValue()).isEqualTo(9007199254740993L);
        assertThat(r.<Number>getProperty("c").doubleValue()).isEqualTo(1.5);
        assertThat(r.<Boolean>getProperty("f")).isTrue();
      }
    }

    /**
     * The check guards creation only: a database written before it, whose schema names a codec the encoders cannot run,
     * must still open (refusing to open would be worse than the failing compaction it already has).
     */
    @Test
    void aPersistedSchemaWithARefusedCodecStillOpens() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Legacy TIMESTAMP ts FIELDS (v DOUBLE CODEC DICTIONARY)");
      database.close();

      final Path schemaFile = Path.of(database.getDatabasePath(), "schema.json");
      final String schema = Files.readString(schemaFile);
      assertThat(schema).containsPattern("\"compression\"\\s*:\\s*\"DICTIONARY\"");
      Files.writeString(schemaFile, schema.replaceAll("(\"compression\"\\s*:\\s*)\"DICTIONARY\"", "$1\"NONE\""));

      reopenDatabase();
      assertThat(((LocalTimeSeriesType) database.getSchema().getType("Legacy")).getTsColumn("v").getCompressionHint())
          .isEqualTo(TimeSeriesCodec.NONE);
    }

    private void assertRefused(final String sql, final String mention) {
      assertThatThrownBy(() -> database.command("sql", sql)).hasMessageContaining(mention);
    }
  }
}
