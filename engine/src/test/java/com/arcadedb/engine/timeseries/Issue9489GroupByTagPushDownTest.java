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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9489: {@code SELECT host, ts.timeBucket('1h', ts) AS h, avg(uu) ... GROUP BY host, h} skipped the aggregation
 * push-down (it only took exactly one GROUP BY key, and only function calls in the projection) and ran the generic plan,
 * hundreds of times slower than the same query grouped by the bucket alone.
 * <p>
 * The push-down now groups by one to four TAG columns, with or without the bucket. Every query is run against a
 * TIMESERIES type and a document twin holding the same rows, in every storage state: only in the mutable buffer, sealed
 * into blocks, and sealed with a fresh tail in the buffer on top.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9489GroupByTagPushDownTest extends TestHelper {
  private static final long T0    = 1_767_225_600_000L;
  private static final long HOUR  = 3_600_000L;
  private static final int  HOSTS = 6;

  private void load(final int shards, final int fromSample, final int toSample) {
    database.transaction(() -> {
      for (int s = fromSample; s < toSample; s++)
        for (int h = 0; h < HOSTS; h++) {
          final long ts = T0 + s * 60_000L + h * 1_000L; // 3 hours of one sample a minute per host
          final Double uu = (s + h) % 7 == 0 ? null : (double) ((s * 31 + h * 17) % 101);
          final String host = h == 5 ? "" : "host_" + h; // one series with no host at all
          final String region = h % 2 == 0 ? "eu" : "us";
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", ts, host, region, uu, (long) s);
          database.command("sql", "INSERT INTO D SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", ts, host, region, uu, (long) s);
        }
    });
  }

  private void createTypes(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING, region STRING) FIELDS (uu DOUBLE, ui LONG) SHARDS "
        + shards);
    database.command("sql", "CREATE DOCUMENT TYPE D");
  }

  private void forEachState(final int shards, final Runnable check) {
    createTypes(shards);
    load(shards, 0, 120);
    check.run(); // everything in the mutable buffer
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run(); // everything sealed
    load(shards, 120, 180);
    check.run(); // sealed blocks plus a fresh tail
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    load(shards, 180, 200);
    check.run();
  }

  /** The rows as a sorted list of strings, with the bucket spelled the same whatever type the engine hands it back as. */
  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final TreeMap<String, String> sorted = new TreeMap<>();
        for (final String name : r.getPropertyNames()) {
          final Object value = r.getProperty(name);
          final String text;
          if (value instanceof LocalDateTime d)
            text = String.valueOf(d.toInstant(ZoneOffset.UTC).toEpochMilli());
          else if (value instanceof Date d)
            text = String.valueOf(d.getTime());
          else if (value instanceof Number n)
            // a SUM of a LONG field is a Double on the push-down and a Long on the generic path, as it already is for the bucket alone
            text = String.format(Locale.ROOT, "%.6f", n.doubleValue());
          else
            text = String.valueOf(value);
          sorted.put(name, text);
        }
        out.add(sorted.toString());
      }
    }
    out.sort(null);
    return out;
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static final String RANGE = " WHERE ts >= " + T0 + " AND ts < " + (T0 + 3 * HOUR);

  private void assertSameAsTheDocumentTwin(final String projection, final String groupBy, final String where) {
    final String tail = where + " GROUP BY " + groupBy;
    final List<String> expected = rows("SELECT " + projection + " FROM D" + tail);
    assertThat(expected).as("the twin answers something").isNotEmpty();
    assertThat(rows("SELECT " + projection + " FROM T" + tail)).as(projection + tail).isEqualTo(expected);
  }

  private static final String AGGREGATES = "avg(uu) AS a, count(*) AS c, min(uu) AS mn, max(uu) AS mx, sum(uu) AS sm, sum(ui) AS si";

  @Test
  void groupByATagAndTheBucketIsPushedDownAndAnswersLikeTheTwin() {
    forEachState(1, () -> {
      final String sql = "SELECT host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES + " FROM T" + RANGE + " GROUP BY host, h";
      assertThat(plan(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host").doesNotContain("FETCH FROM TIMESERIES");
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "host, h", RANGE);
    });
  }

  @Test
  void theOrderOfTheGroupByKeysAndOfTheProjectionDoesNotMatter() {
    forEachState(1, () -> {
      assertSameAsTheDocumentTwin("ts.timeBucket('30m', ts) AS h, host, " + AGGREGATES, "h, host", RANGE);
      assertSameAsTheDocumentTwin(AGGREGATES + ", host, ts.timeBucket('1h', ts) AS h", "h, host", RANGE);
    });
  }

  @Test
  void twoTagsAndTheBucket() {
    forEachState(1, () -> {
      final String sql = "SELECT host, region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES + " FROM T" + RANGE
          + " GROUP BY host, region, h";
      assertThat(plan(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host, region");
      assertSameAsTheDocumentTwin("host, region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "host, region, h", RANGE);
      assertSameAsTheDocumentTwin("region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "region, h", RANGE);
    });
  }

  @Test
  void groupByTagsAloneIsOneRowPerGroup() {
    forEachState(1, () -> {
      final String sql = "SELECT host, " + AGGREGATES + " FROM T" + RANGE + " GROUP BY host";
      assertThat(plan(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host");
      assertSameAsTheDocumentTwin("host, " + AGGREGATES, "host", RANGE);
      assertSameAsTheDocumentTwin("host, region, " + AGGREGATES, "host, region", RANGE);
    });
  }

  @Test
  void aTagFilterAndATagGroupTogether() {
    forEachState(1, () -> {
      final String where = RANGE + " AND region = 'eu'";
      assertThat(plan("SELECT host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES + " FROM T" + where + " GROUP BY host, h"))
          .contains("AGGREGATE FROM TIMESERIES");
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "host, h", where);
      assertSameAsTheDocumentTwin("region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "region, h", " WHERE host = 'host_1'");
    });
  }

  @Test
  void aliasedAndUnprojectedTags() {
    forEachState(1, () -> {
      assertSameAsTheDocumentTwin("host AS machine, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "machine, h", RANGE);
      // grouped by a tag the projection does not show
      assertThat(plan("SELECT ts.timeBucket('1h', ts) AS h, " + AGGREGATES + " FROM T" + RANGE + " GROUP BY host, h"))
          .contains("AGGREGATE FROM TIMESERIES").contains("group by host");
      assertSameAsTheDocumentTwin("ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "host, h", RANGE);
    });
  }

  @Test
  void severalShardsAnswerTheSame() {
    forEachState(3, () -> {
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES, "host, h", RANGE);
      assertSameAsTheDocumentTwin("host, " + AGGREGATES, "host", RANGE);
    });
  }

  /**
   * A TAG column stores a null as the empty string once it is sealed (its dictionary has no null), so a null tag reads back as
   * {@code ""} after a compaction on every plan. The push-down spells it that way in the mutable buffer too, which makes its
   * answer the same before and after the compaction; the document twin keeps the null, as a document type does.
   */
  @Test
  void aNullTagIsOneGroupWhetherItIsStillInTheMutableBufferOrSealed() {
    createTypes(1);
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        for (final String type : new String[] { "T", "D" }) {
          final String host = i % 3 == 0 ? null : "host_" + i % 3;
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", T0 + i * 1_000L, host, "eu",
              (double) i, (long) i);
        }
    });
    final String sql = "SELECT host, count(*) AS c, sum(uu) AS s FROM T GROUP BY host";
    assertThat(plan(sql)).contains("AGGREGATE FROM TIMESERIES");
    final List<String> mutable = rows(sql);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final List<String> sealed = rows(sql);

    assertThat(mutable).isEqualTo(sealed).hasSize(3);
    assertThat(sealed).contains("{c=20.000000, host=, s=570.000000}");
    // and the twin differs only in spelling the missing tag null
    assertThat(rows("SELECT host, count(*) AS c, sum(uu) AS s FROM D GROUP BY host")).contains("{c=20.000000, host=null, s=570.000000}");
  }

  @Test
  void orderByAndAQuotedTagNameKeepTheirMeaning() {
    forEachState(1, () -> {
      for (final String type : new String[] { "T", "D" }) {
        final String sql = "SELECT `host`, ts.timeBucket('1h', ts) AS h, count(*) AS c FROM " + type + RANGE
            + " GROUP BY host, h ORDER BY host DESC, h";
        final List<String> ordered = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", sql)) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            ordered.add(r.getProperty("host") + "/" + r.getProperty("c"));
          }
        }
        assertThat(ordered).as(type).isNotEmpty().isSortedAccordingTo(java.util.Comparator.comparing((String s) -> s.split("/")[0]).reversed());
      }
      assertSameAsTheDocumentTwin("`host`, ts.timeBucket('1h', ts) AS h, count(*) AS c", "`host`, h", RANGE);
    });
  }

  @Test
  void fourTagsArePushedDownAndFiveAreNot() {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (a STRING, b STRING, c STRING, d STRING, e STRING) FIELDS (v DOUBLE)");
    database.command("sql", "CREATE DOCUMENT TYPE MD");
    database.transaction(() -> {
      for (int i = 0; i < 400; i++)
        for (final String type : new String[] { "M", "MD" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, a = ?, b = ?, c = ?, d = ?, e = ?, v = ?", T0 + i * 1_000L,
              "a" + i % 2, "b" + i % 3, "c" + i % 4, "d" + i % 5, "e" + i % 6, (double) i);
    });
    for (int pass = 0; pass < 2; pass++) {
      final String four = "SELECT a, b, c, d, count(*) AS n, sum(v) AS s FROM %s GROUP BY a, b, c, d";
      assertThat(plan(String.format(four, "M"))).contains("AGGREGATE FROM TIMESERIES").contains("group by a, b, c, d");
      assertThat(rows(String.format(four, "M"))).isEqualTo(rows(String.format(four, "MD")));

      // a tag filter that cannot be answered from a block's declaration (the block holds several values of it) filters row by row
      final String filtered = "SELECT b, c, count(*) AS n, sum(v) AS s FROM %s WHERE a = 'a1' GROUP BY b, c";
      assertThat(rows(String.format(filtered, "M"))).isEqualTo(rows(String.format(filtered, "MD")));

      final String five = "SELECT a, b, c, d, e, count(*) AS n FROM %s GROUP BY a, b, c, d, e";
      assertThat(plan(String.format(five, "M"))).doesNotContain("AGGREGATE FROM TIMESERIES");
      assertThat(rows(String.format(five, "M"))).isEqualTo(rows(String.format(five, "MD")));

      database.command("sql", "COMPACT TIMESERIES TYPE M");
    }
  }

  @Test
  void aProjectedTagThatIsNotGroupedByStaysOnTheGenericPath() {
    createTypes(1);
    load(1, 0, 10);
    // `host` would be one arbitrary value per bucket: the engine must not decide which, so the generic plan answers
    final String sql = "SELECT host, ts.timeBucket('1h', ts) AS h, avg(uu) AS a FROM T" + RANGE + " GROUP BY h";
    assertThat(plan(sql)).doesNotContain("group by");
  }

  @Test
  void aFieldInTheGroupByStaysOnTheGenericPathAndIsStillRight() {
    createTypes(1);
    load(1, 0, 30);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final String sql = "SELECT ui, count(*) AS c FROM T" + RANGE + " GROUP BY ui";
    assertThat(plan(sql)).doesNotContain("AGGREGATE FROM TIMESERIES");
    assertSameAsTheDocumentTwin("ui, count(*) AS c", "ui", RANGE);
  }
}
