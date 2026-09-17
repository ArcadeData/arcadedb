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
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7702: a SQL-rendered {@code CREATE TIMESERIES TYPE} keeps the column order it was given, roles
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
class Issue7702InterleavedTagAndFieldOrderTest extends TestHelper {

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
   * same divergence: {@code Issue7740TimeSeriesBuilderParityTest} pinned it as a regrouping the rendering could
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
        .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP, TimeSeriesCodec.SIMPLE8B))
        .withPrecision("microsecond")
        .withField("f1", Type.DOUBLE)
        .toSQL().getFirst();

    assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `TimestampClauses` TAGS (`t1` STRING) "
        + "TIMESTAMP `ts` PRECISION MICROSECOND CODEC SIMPLE8B FIELDS (`f1` DOUBLE)");

    database.command("sql", sql);
    final TimeSeriesType type = typeOf("TimestampClauses");
    assertThat(columnNames(type)).containsExactly("t1", "ts", "f1");
    assertThat(type.getPrecision()).isEqualTo("MICROSECOND");
    assertThat(type.getTsColumn("ts").getCompressionHint()).isEqualTo(TimeSeriesCodec.SIMPLE8B);
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
