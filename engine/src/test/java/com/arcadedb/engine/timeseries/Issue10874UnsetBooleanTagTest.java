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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10874: a BOOLEAN tag a sample left unset answered {@code null} in the mutable pages and {@code false} once sealed,
 * because {@link ColumnDefinition#boxString} parsed the empty text the sealed layer stores for "no value" as a boolean. The
 * sealed aggregation push-down, which tested the raw dictionary text, then disagreed with the row scan, which tested the
 * decoded value, about {@code WHERE flag = false}; and a rewrite of the block (a DELETE of an unrelated row) stored the
 * text {@code "false"} for the unset tag and changed the answer again.
 * <p>
 * Also issue #10878: which columns carry block statistics is ONE definition, {@link TimeSeriesCodec#isNumeric()}, which
 * every block writer and reader reaches through {@link ColumnDefinition#carriesBlockStatistics()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10874UnsetBooleanTagTest extends TestHelper {

  @Test
  void anUnsetBooleanTagAnswersTheSameInEveryLayerAndEveryPathAgrees() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (flag BOOLEAN) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO M SET ts = 1000, flag = true, v = 1");
      database.command("sql", "INSERT INTO M SET ts = 2000, flag = false, v = 2");
      database.command("sql", "INSERT INTO M SET ts = 3000, v = 3");
    });

    assertTheAnswersOfThreeRows("mutable");

    engine().compactAll();
    assertThat(engine().getShard(0).getSealedStore().getBlockCount()).as("the samples are sealed").isPositive();
    assertTheAnswersOfThreeRows("sealed");

    // A rewrite of the block that holds the unset tag, triggered by a row neither query below ever matched
    assertThat(engine().deleteSamples(1000, 1000, null)).isEqualTo(1L);
    assertThat(flagOf(3000)).as("rebuilt: the unset tag").isNull();
    assertThat(timestamps("SELECT ts FROM M WHERE flag IS NULL")).as("rebuilt: IS NULL").containsExactly(3000L);
    assertThat(timestamps("SELECT ts FROM M WHERE flag = false")).as("rebuilt: the scan").containsExactly(2000L);
    assertThat(single("SELECT count(*) AS c FROM M WHERE flag = false", "c")).as("rebuilt: the push-down count").isEqualTo(1L);
    assertThat(single("SELECT sum(v) AS s FROM M WHERE flag = false", "s")).as("rebuilt: the push-down sum").isEqualTo(2.0);
  }

  /**
   * A block whose rows hold more than one value of the tag goes through the per-row test of the aggregation push-down; this one
   * holds a set value, an unset one and the other set value in every block, so both of its paths (ungrouped, grouped) read rows.
   */
  @Test
  void theAggregationPushDownTestsTheValueTheScanReads() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE N TIMESTAMP ts TAGS (flag BOOLEAN, host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final long ts = 1000L * (i + 1);
        if (i % 3 == 2)
          database.command("sql", "INSERT INTO N SET ts = ?, host = ?, v = ?", ts, "h" + (i % 2), (double) i);
        else
          database.command("sql", "INSERT INTO N SET ts = ?, flag = ?, host = ?, v = ?", ts, i % 3 == 0, "h" + (i % 2), (double) i);
      }
    });
    ((LocalTimeSeriesType) database.getSchema().getType("N")).getEngine().compactAll();

    final List<Long> falseRows = timestamps("SELECT ts FROM N WHERE flag = false");
    assertThat(falseRows).as("one in three rows set the tag false").hasSize(10);
    assertThat(single("SELECT count(*) AS c FROM N WHERE flag = false", "c")).as("ungrouped").isEqualTo(10L);

    long grouped = 0;
    try (final ResultSet rs = database.query("sql", "SELECT host, count(*) AS c FROM N WHERE flag = false GROUP BY host")) {
      while (rs.hasNext())
        grouped += ((Number) rs.next().getProperty("c")).longValue();
    }
    assertThat(grouped).as("grouped").isEqualTo(10L);
    assertThat(timestamps("SELECT ts FROM N WHERE flag IS NULL")).hasSize(10);
  }

  /**
   * A tag declared with a numeric codec is not a dictionary: the per-row test of the push-down decodes it with its own codec, and a
   * sample that left it unset is {@code null} there as in the scan (review of PR #10902).
   */
  @Test
  void aTagStoredWithANumericCodecIsFilteredByItsOwnCodec() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE B TIMESTAMP ts TAGS (flag BOOLEAN CODEC SIMPLE8B, host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final long ts = 1000L * (i + 1);
        if (i % 3 == 2)
          database.command("sql", "INSERT INTO B SET ts = ?, host = ?, v = ?", ts, "h" + (i % 2), (double) i);
        else
          database.command("sql", "INSERT INTO B SET ts = ?, flag = ?, host = ?, v = ?", ts, i % 3 == 0, "h" + (i % 2), (double) i);
      }
    });
    ((LocalTimeSeriesType) database.getSchema().getType("B")).getEngine().compactAll();

    assertThat(timestamps("SELECT ts FROM B WHERE flag = false")).hasSize(10);
    assertThat(single("SELECT count(*) AS c FROM B WHERE flag = false", "c")).as("ungrouped").isEqualTo(10L);
    assertThat(single("SELECT count(*) AS c FROM B WHERE flag = true", "c")).isEqualTo(10L);
    long grouped = 0;
    try (final ResultSet rs = database.query("sql", "SELECT host, count(*) AS c FROM B WHERE flag = true GROUP BY host")) {
      while (rs.hasNext())
        grouped += ((Number) rs.next().getProperty("c")).longValue();
    }
    assertThat(grouped).as("grouped").isEqualTo(10L);
    assertThat(timestamps("SELECT ts FROM B WHERE flag IS NULL")).hasSize(10);
  }

  /** The empty text is "no value" for every type but STRING, BOOLEAN included. */
  @Test
  void theEmptyTextReadsBackAsNoValueForEveryNonStringType() {
    for (final Type type : new Type[] { Type.BOOLEAN, Type.INTEGER, Type.LONG, Type.DOUBLE, Type.DATETIME })
      assertThat(new ColumnDefinition("c", type, ColumnDefinition.ColumnRole.TAG).boxString("")).as("%s", type).isNull();
    assertThat(new ColumnDefinition("c", Type.STRING, ColumnDefinition.ColumnRole.TAG).boxString("")).isEqualTo("");
    assertThat(new ColumnDefinition("c", Type.BOOLEAN, ColumnDefinition.ColumnRole.TAG).boxString("false")).isEqualTo(Boolean.FALSE);
    assertThat(new ColumnDefinition("c", Type.BOOLEAN, ColumnDefinition.ColumnRole.TAG).boxString("true")).isEqualTo(Boolean.TRUE);
  }

  /** Issue #10878: the block statistics, the push-down and the codec agree on which columns are numeric, for every codec. */
  @Test
  void blockStatisticsAreKeyedOffTheOneNumericCodecDefinition() {
    for (final TimeSeriesCodec codec : TimeSeriesCodec.values()) {
      final ColumnDefinition column = new ColumnDefinition("c", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD, codec);
      assertThat(column.carriesBlockStatistics()).as("%s", codec).isEqualTo(codec.isNumeric());
      assertThat(column.isNumericallyAggregatable()).as("%s", codec).isEqualTo(codec.isNumeric());
    }
    assertThat(TimeSeriesCodec.GORILLA_XOR.isNumeric()).isTrue();
    assertThat(TimeSeriesCodec.SIMPLE8B.isNumeric()).isTrue();
    assertThat(TimeSeriesCodec.DICTIONARY.isNumeric()).isFalse();
    assertThat(TimeSeriesCodec.DELTA_OF_DELTA.isNumeric()).isFalse();
  }

  private void assertTheAnswersOfThreeRows(final String layer) {
    assertThat(flagOf(3000)).as("%s: the unset tag", layer).isNull();
    assertThat(flagOf(2000)).as("%s: the tag set false", layer).isEqualTo(Boolean.FALSE);
    assertThat(timestamps("SELECT ts FROM M WHERE flag IS NULL")).as("%s: IS NULL", layer).containsExactly(3000L);
    assertThat(timestamps("SELECT ts FROM M WHERE flag = false")).as("%s: the scan", layer).containsExactly(2000L);
    assertThat(single("SELECT count(*) AS c FROM M WHERE flag = false", "c")).as("%s: the push-down count", layer).isEqualTo(1L);
    assertThat(single("SELECT sum(v) AS s FROM M WHERE flag = false", "s")).as("%s: the push-down sum", layer).isEqualTo(2.0);
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
  }

  private Object flagOf(final long ts) {
    try (final ResultSet rs = database.query("sql", "SELECT flag FROM M WHERE ts = ?", ts)) {
      assertThat(rs.hasNext()).as("the row at %s", ts).isTrue();
      return rs.next().getProperty("flag");
    }
  }

  private List<Long> timestamps(final String query) {
    final List<Long> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Object ts = rs.next().getProperty("ts");
        result.add(switch (ts) {
          case Date date -> date.getTime();
          case LocalDateTime local -> local.toInstant(ZoneOffset.UTC).toEpochMilli();
          default -> ((Number) ts).longValue();
        });
      }
    }
    result.sort(null);
    return result;
  }

  private Object single(final String query, final String property) {
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Result row = rs.next();
      final Object value = row.getProperty(property);
      return value instanceof Number n && !(value instanceof Double) ? (Object) n.longValue() : value;
    }
  }
}
