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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7689: {@code CREATE TIMESERIES TYPE} now expresses a WHOLE TimeSeries type - the per-column codecs and the
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
class Issue7689CreateTimeSeriesTypeCompleteDeclarationTest extends TestHelper {

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
        "CREATE TIMESERIES TYPE TsCodec TIMESTAMP ts PRECISION MILLISECOND CODEC SIMPLE8B FIELDS (v DOUBLE)");

    final TimeSeriesType type = typeOf("TsCodec");
    assertThat(type.getTimestampColumn()).isEqualTo("ts");
    assertThat(type.getPrecision()).isEqualTo("MILLISECOND");
    assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
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
          TIMESTAMP ts PRECISION NANOSECOND CODEC SIMPLE8B
          TAGS (host STRING, zone INTEGER CODEC DICTIONARY)
          FIELDS (cpu DOUBLE CODEC DICTIONARY, mem LONG)
          SHARDS 3
          RETENTION 90 DAYS
          COMPACTION_INTERVAL 2 HOURS
          DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS
        """);

    final TimeSeriesType type = typeOf("Everything");
    assertThat(type.getPrecision()).isEqualTo("NANOSECOND");
    assertThat(type.getShardCount()).isEqualTo(3);
    assertThat(type.getRetentionMs()).isEqualTo(90L * 86_400_000L);
    assertThat(type.getCompactionBucketIntervalMs()).isEqualTo(2L * 3_600_000L);
    assertThat(type.getDownsamplingTiers()).containsExactly(new DownsamplingTier(7L * 86_400_000L, 3_600_000L));
    assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
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
    database.command("sql", "CREATE TIMESERIES TYPE CodecNamedColumn TIMESTAMP ts FIELDS (codec DOUBLE CODEC NONE)");
    assertThat(typeOf("CodecNamedColumn").getTsColumn("codec").getCompressionHint()).isEqualTo(TimeSeriesCodec.NONE);

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
        .withColumn(new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, TimeSeriesCodec.NONE))
        .withField("mem", Type.LONG)
        .withDownsamplingTiers(List.of(new DownsamplingTier(7L * 86_400_000L, 3_600_000L),
            new DownsamplingTier(30L * 86_400_000L, 86_400_000L)))
        .toSQL();

    assertThat(statements).hasSize(1);
    assertThat(statements.getFirst())
        .startsWith("CREATE TIMESERIES TYPE `OneStatement`")
        .contains("`cpu` DOUBLE CODEC NONE")
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
        .withColumn(new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.NONE))
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
            TimeSeriesCodec.SIMPLE8B))
        .withField("v", Type.DOUBLE)
        .toSQL().getFirst();

    assertThat(sql).contains("TIMESTAMP `ts` CODEC SIMPLE8B");

    database.command("sql", sql);
    assertThat(typeOf("TsCodecRender").getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
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
