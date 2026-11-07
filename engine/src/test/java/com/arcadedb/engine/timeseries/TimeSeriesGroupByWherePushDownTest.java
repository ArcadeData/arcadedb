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
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * GROUP BY, WHERE and COUNT push-down of time-series aggregations: every layer (mutable, sealed, compacted)
 * must answer like the document-type twin.
 */
class TimeSeriesGroupByWherePushDownTest extends TestHelper {
  private String plan(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static final long T0   = 1_767_225_600_000L;
  private static final long HOUR = 3_600_000L;

  // ---- #9489 ----
  // Issue #9489: {@code SELECT host, ts.timeBucket('1h', ts) AS h, avg(uu) ... GROUP BY host, h} skipped the aggregation
  // push-down (it only took exactly one GROUP BY key, and only function calls in the projection) and ran the generic plan,
  // hundreds of times slower than the same query grouped by the bucket alone.
  // <p>
  // The push-down now groups by one to four TAG columns, with or without the bucket. Every query is run against a
  // TIMESERIES type and a document twin holding the same rows, in every storage state: only in the mutable buffer, sealed
  // into blocks, and sealed with a fresh tail in the buffer on top.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int  HOSTS9489 = 6;

  private void load9489(final int shards, final int fromSample, final int toSample) {
    database.transaction(() -> {
      for (int s = fromSample; s < toSample; s++)
        for (int h = 0; h < HOSTS9489; h++) {
          final long ts = T0 + s * 60_000L + h * 1_000L; // 3 hours of one sample a minute per host
          final Double uu = (s + h) % 7 == 0 ? null : (double) ((s * 31 + h * 17) % 101);
          final String host = h == 5 ? "" : "host_" + h; // one series with no host at all
          final String region = h % 2 == 0 ? "eu" : "us";
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", ts, host, region, uu, (long) s);
          database.command("sql", "INSERT INTO D SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", ts, host, region, uu, (long) s);
        }
    });
  }

  private void createTypes9489(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING, region STRING) FIELDS (uu DOUBLE, ui LONG) SHARDS "
        + shards);
    database.command("sql", "CREATE DOCUMENT TYPE D");
  }

  private void forEachState9489(final int shards, final Runnable check) {
    createTypes9489(shards);
    load9489(shards, 0, 120);
    check.run(); // everything in the mutable buffer
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run(); // everything sealed
    load9489(shards, 120, 180);
    check.run(); // sealed blocks plus a fresh tail
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    load9489(shards, 180, 200);
    check.run();
  }

  /** The rows9489 as a sorted list of strings, with the bucket spelled the same whatever type the engine hands it back as. */
  private List<String> rows9489(final String sql) {
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

  private String plan9489(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static final String RANGE9489 = " WHERE ts >= " + T0 + " AND ts < " + (T0 + 3 * HOUR);

  private void assertSameAsTheDocumentTwin(final String projection, final String groupBy, final String where) {
    final String tail = where + " GROUP BY " + groupBy;
    final List<String> expected = rows9489("SELECT " + projection + " FROM D" + tail);
    assertThat(expected).as("the twin answers something").isNotEmpty();
    assertThat(rows9489("SELECT " + projection + " FROM T" + tail)).as(projection + tail).isEqualTo(expected);
  }

  private static final String AGGREGATES9489 = "avg(uu) AS a, count(*) AS c, min(uu) AS mn, max(uu) AS mx, sum(uu) AS sm, sum(ui) AS si";

  // #9489
  @Test
  void groupByATagAndTheBucketIsPushedDownAndAnswersLikeTheTwin() {
    forEachState9489(1, () -> {
      final String sql = "SELECT host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489 + " FROM T" + RANGE9489 + " GROUP BY host, h";
      assertThat(plan9489(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host").doesNotContain("FETCH FROM TIMESERIES");
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "host, h", RANGE9489);
    });
  }

  // #9489
  @Test
  void theOrderOfTheGroupByKeysAndOfTheProjectionDoesNotMatter() {
    forEachState9489(1, () -> {
      assertSameAsTheDocumentTwin("ts.timeBucket('30m', ts) AS h, host, " + AGGREGATES9489, "h, host", RANGE9489);
      assertSameAsTheDocumentTwin(AGGREGATES9489 + ", host, ts.timeBucket('1h', ts) AS h", "h, host", RANGE9489);
    });
  }

  // #9489
  @Test
  void twoTagsAndTheBucket() {
    forEachState9489(1, () -> {
      final String sql = "SELECT host, region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489 + " FROM T" + RANGE9489
          + " GROUP BY host, region, h";
      assertThat(plan9489(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host, region");
      assertSameAsTheDocumentTwin("host, region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "host, region, h", RANGE9489);
      assertSameAsTheDocumentTwin("region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "region, h", RANGE9489);
    });
  }

  // #9489
  @Test
  void groupByTagsAloneIsOneRowPerGroup() {
    forEachState9489(1, () -> {
      final String sql = "SELECT host, " + AGGREGATES9489 + " FROM T" + RANGE9489 + " GROUP BY host";
      assertThat(plan9489(sql)).contains("AGGREGATE FROM TIMESERIES").contains("group by host");
      assertSameAsTheDocumentTwin("host, " + AGGREGATES9489, "host", RANGE9489);
      assertSameAsTheDocumentTwin("host, region, " + AGGREGATES9489, "host, region", RANGE9489);
    });
  }

  // #9489
  @Test
  void aTagFilterAndATagGroupTogether() {
    forEachState9489(1, () -> {
      final String where = RANGE9489 + " AND region = 'eu'";
      assertThat(plan9489("SELECT host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489 + " FROM T" + where + " GROUP BY host, h"))
          .contains("AGGREGATE FROM TIMESERIES");
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "host, h", where);
      assertSameAsTheDocumentTwin("region, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "region, h", " WHERE host = 'host_1'");
    });
  }

  // #9489
  @Test
  void aliasedAndUnprojectedTags() {
    forEachState9489(1, () -> {
      assertSameAsTheDocumentTwin("host AS machine, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "machine, h", RANGE9489);
      // grouped by a tag the projection does not show
      assertThat(plan9489("SELECT ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489 + " FROM T" + RANGE9489 + " GROUP BY host, h"))
          .contains("AGGREGATE FROM TIMESERIES").contains("group by host");
      assertSameAsTheDocumentTwin("ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "host, h", RANGE9489);
    });
  }

  // #9489
  @Test
  void severalShardsAnswerTheSame() {
    forEachState9489(3, () -> {
      assertSameAsTheDocumentTwin("host, ts.timeBucket('1h', ts) AS h, " + AGGREGATES9489, "host, h", RANGE9489);
      assertSameAsTheDocumentTwin("host, " + AGGREGATES9489, "host", RANGE9489);
    });
  }

  // #9489
  /**
   * A TAG column stores a null as the empty string once it is sealed (its dictionary has no null), so a null tag reads back as
   * {@code ""} after a compaction on every plan9489. The push-down spells it that way in the mutable buffer too, which makes its
   * answer the same before and after the compaction; the document twin keeps the null, as a document type does.
   */
  @Test
  void aNullTagIsOneGroupWhetherItIsStillInTheMutableBufferOrSealed() {
    createTypes9489(1);
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        for (final String type : new String[] { "T", "D" }) {
          final String host = i % 3 == 0 ? null : "host_" + i % 3;
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, region = ?, uu = ?, ui = ?", T0 + i * 1_000L, host, "eu",
              (double) i, (long) i);
        }
    });
    final String sql = "SELECT host, count(*) AS c, sum(uu) AS s FROM T GROUP BY host";
    assertThat(plan9489(sql)).contains("AGGREGATE FROM TIMESERIES");
    final List<String> mutable = rows9489(sql);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final List<String> sealed = rows9489(sql);

    assertThat(mutable).isEqualTo(sealed).hasSize(3);
    assertThat(sealed).contains("{c=20.000000, host=, s=570.000000}");
    // and the twin differs only in spelling the missing tag null
    assertThat(rows9489("SELECT host, count(*) AS c, sum(uu) AS s FROM D GROUP BY host")).contains("{c=20.000000, host=null, s=570.000000}");
  }

  // #9489
  @Test
  void orderByAndAQuotedTagNameKeepTheirMeaning() {
    forEachState9489(1, () -> {
      for (final String type : new String[] { "T", "D" }) {
        final String sql = "SELECT `host`, ts.timeBucket('1h', ts) AS h, count(*) AS c FROM " + type + RANGE9489
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
      assertSameAsTheDocumentTwin("`host`, ts.timeBucket('1h', ts) AS h, count(*) AS c", "`host`, h", RANGE9489);
    });
  }

  // #9489
  /** Shards can cross the flat-slot budget at different group counts, so a flat result is merged into a map-mode one and the reverse. */
  @Test
  void flatAndMapModeResultsMergeInBothDirections() {
    final List<MultiColumnAggregationRequest> requests = List.of(MultiColumnAggregationRequest.count("c"));
    for (final boolean flatIntoMap : new boolean[] { true, false }) {
      final MultiColumnAggregationResult flat = new MultiColumnAggregationResult(requests, 0L, 1_000L, 4);
      final MultiColumnAggregationResult map = new MultiColumnAggregationResult(requests);
      flat.accumulateRow(1_000L, new double[] { 1 });
      flat.accumulateRow(2_000L, new double[] { 1 });
      map.accumulateRow(2_000L, new double[] { 1 });
      map.accumulateRow(9_000L, new double[] { 1 });

      final MultiColumnAggregationResult target = flatIntoMap ? map : flat;
      target.mergeFrom(flatIntoMap ? flat : map);

      assertThat(target.getBucketTimestamps()).containsExactlyInAnyOrder(1_000L, 2_000L, 9_000L);
      assertThat(target.getValue(1_000L, 0)).isEqualTo(1.0);
      assertThat(target.getValue(2_000L, 0)).isEqualTo(2.0);
      assertThat(target.getValue(9_000L, 0)).isEqualTo(1.0);
    }
  }

  // #9489
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
      assertThat(plan9489(String.format(four, "M"))).contains("AGGREGATE FROM TIMESERIES").contains("group by a, b, c, d");
      assertThat(rows9489(String.format(four, "M"))).isEqualTo(rows9489(String.format(four, "MD")));

      // a tag filter that cannot be answered from a block's declaration (the block holds several values of it) filters row by row
      final String filtered = "SELECT b, c, count(*) AS n, sum(v) AS s FROM %s WHERE a = 'a1' GROUP BY b, c";
      assertThat(rows9489(String.format(filtered, "M"))).isEqualTo(rows9489(String.format(filtered, "MD")));

      final String five = "SELECT a, b, c, d, e, count(*) AS n FROM %s GROUP BY a, b, c, d, e";
      assertThat(plan9489(String.format(five, "M"))).doesNotContain("AGGREGATE FROM TIMESERIES");
      assertThat(rows9489(String.format(five, "M"))).isEqualTo(rows9489(String.format(five, "MD")));

      database.command("sql", "COMPACT TIMESERIES TYPE M");
    }
  }

  // #9489
  @Test
  void aProjectedTagThatIsNotGroupedByStaysOnTheGenericPath() {
    createTypes9489(1);
    load9489(1, 0, 10);
    // `host` would be one arbitrary value per bucket: the engine must not decide which, so the generic plan9489 answers
    final String sql = "SELECT host, ts.timeBucket('1h', ts) AS h, avg(uu) AS a FROM T" + RANGE9489 + " GROUP BY h";
    assertThat(plan9489(sql)).doesNotContain("group by");
  }

  // #9489
  @Test
  void aFieldInTheGroupByStaysOnTheGenericPathAndIsStillRight() {
    createTypes9489(1);
    load9489(1, 0, 30);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final String sql = "SELECT ui, count(*) AS c FROM T" + RANGE9489 + " GROUP BY ui";
    assertThat(plan9489(sql)).doesNotContain("AGGREGATE FROM TIMESERIES");
    assertSameAsTheDocumentTwin("ui, count(*) AS c", "ui", RANGE9489);
  }

  // ---- #8915 ----
  // Issues #8915, #8916 and #8917: the TIMESERIES planner answered differently from the same SQL over a document type.
  // <ul>
  //   <li>#8915: the aggregation push-down counted every row for {@code count(field)}, nulls included, as a Double;</li>
  //   <li>#8916: a time range on one OR branch was applied to the whole WHERE;</li>
  //   <li>#8917: two equalities on the same tag in one AND were unioned instead of intersected.</li>
  // </ul>
  // Each query runs against a TIMESERIES type and a document twin holding the same rows, before and after compaction.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private void load8915() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE, n LONG)");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 1; i <= 4; i++) {
        final Double v = i % 2 == 0 ? null : (double) i; // the even ones are null
        database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?, n = 1", i * 1000L, i % 2 == 1 ? "a" : "b", v);
        database.command("sql", "INSERT INTO D SET ts = ?, host = ?, v = ?, n = 1", i * 1000L, i % 2 == 1 ? "a" : "b", v);
      }
    });
  }

  private void forEachState8915(final Runnable check) {
    load8915();
    check.run();
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run();
  }

  private List<Object> ns(final String sql) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("n"));
    }
    return out;
  }

  // #8915
  @Test
  void countOfFieldSkipsNullsOnEveryPathAndIsALong() {
    forEachState8915(() -> {
      for (final String where : new String[] { "", "WHERE n > 0 " }) {
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS b, count(v) AS cv, count(*) AS c FROM T " + where + "GROUP BY b")) {
          assertThat(rs.hasNext()).isTrue();
          final Result r = rs.next();
          assertThat((Object) r.getProperty("cv")).as(where).isEqualTo(2L);
          assertThat((Object) r.getProperty("c")).as(where).isEqualTo(4L);
          assertThat(rs.hasNext()).isFalse();
        }
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(v) AS cv, count(*) AS c FROM T")) {
        final Result r = rs.next();
        assertThat((Object) r.getProperty("cv")).isEqualTo(2L);
        assertThat((Object) r.getProperty("c")).isEqualTo(4L);
      }
    });
  }

  // #8915
  @Test
  void orWithATimeRangeOnOneBranchKeepsTheOtherBranchRows() {
    forEachState8915(() -> {
      for (final String w : new String[] { "ts >= 3000 OR host = 'a'", "ts < 2000 OR ts >= 4000", "ts < 2000 OR ts >= 3000",
          "(ts >= 3000 AND host = 'b') OR ts <= 1000", "ts BETWEEN 1000 AND 1000 OR ts = 4000", "ts > 4000 OR ts < 1000" }) {
        final List<Object> expected = ns("SELECT v AS n FROM D WHERE " + w + " ORDER BY ts");
        assertThat(ns("SELECT v AS n FROM T WHERE " + w + " ORDER BY ts")).as(w).isEqualTo(expected);
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE " + w)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).as(w).isEqualTo(
              ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + w).next().getProperty("c")).longValue());
        }
        // grouped, whether or not it is pushed down
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE " + w + " GROUP BY b");
            final ResultSet ref = database.query("sql",
                "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM D WHERE " + w + " GROUP BY b")) {
          final long got = rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0L;
          final long want = ref.hasNext() ? ((Number) ref.next().getProperty("c")).longValue() : 0L;
          assertThat(got).as("grouped " + w).isEqualTo(want);
        }
      }
    });
  }

  /** All the rows fall in one hour bucket, so the first group row is the whole answer. */
  private long groupedCount(final String type, final String where) {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM " + type + " WHERE " + where + " GROUP BY b")) {
      return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0L;
    }
  }

  // #8915
  @Test
  void twoEqualitiesOnTheSameTagInOneAndMatchNothing() {
    forEachState8915(() -> {
      for (final String w : new String[] { "host = 'a' AND host = 'b'", "host = 'a' AND host = 'b' AND v > 0",
          "(host = 'a' AND host = 'b') OR host = 'a'", "host = 'a' AND host = 'a'",
          "host = 'a' AND host = 'b' AND host = 'a'" })
        assertThat(groupedCount("T", w)).as(w).isEqualTo(groupedCount("D", w));
      assertThat(groupedCount("T", "host = 'a' AND host = 'b'")).isZero();
    });
  }

  // #8915
  @Test
  void contradictoryBlocksAddNothingToTheTimeRangeUnion() {
    forEachState8915(() -> {
      for (final String w : new String[] { "(ts > 3000 AND ts < 2000) OR host = 'a'", "(ts > 3000 AND ts < 2000) OR ts >= 4000",
          "(ts > 3000 AND ts < 2000) OR (ts > 5000 AND ts < 1000)" }) {
        assertThat(ns("SELECT v AS n FROM T WHERE " + w + " ORDER BY ts")).as(w)
            .isEqualTo(ns("SELECT v AS n FROM D WHERE " + w + " ORDER BY ts"));
        assertThat(groupedCount("T", w)).as(w).isEqualTo(groupedCount("D", w));
      }
    });
  }

  // #8915
  @Test
  void aNullTagParameterMatchesNothingOnEveryPath() {
    forEachState8915(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE host = ? GROUP BY b", (Object) null)) {
        assertThat(rs.hasNext()).isFalse();
      }
      assertThat(ns("SELECT v AS n FROM T WHERE host = null")).isEqualTo(ns("SELECT v AS n FROM D WHERE host = null"));
      try (final ResultSet rs = database.query("sql", "SELECT v AS n FROM T WHERE host = ?", (Object) null)) {
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  // #8915
  @Test
  void pushDownOnlyTakesTheShapesItAnswersExactly() {
    load8915();
    final String head = "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE ";
    assertThat(plan(head + "host = 'a' AND ts >= 1000 GROUP BY b")).contains("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "host = 'a' OR host = 'b' GROUP BY b")).as("an exact IN stays pushed down")
        .contains("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "ts >= 1000 AND (host = 'a' OR host = 'b') GROUP BY b")).as("a shared time range keeps the IN exact")
        .contains("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "(ts >= 1000 AND host = 'a') OR (ts >= 2000 AND host = 'b') GROUP BY b"))
        .as("different ranges per block are not an exact union").doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "host = 'a' AND host = 'b' GROUP BY b")).doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "ts >= 3000 OR host = 'a' GROUP BY b")).doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(plan(head + "ts < 2000 OR ts >= 4000 GROUP BY b")).doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(plan("SELECT ts.timeBucket('1h', ts) AS b, count(v) AS c FROM T GROUP BY b"))
        .doesNotContain("AGGREGATE FROM TIMESERIES");
    assertThat(plan("SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T GROUP BY b"))
        .contains("AGGREGATE FROM TIMESERIES");
  }

  // #8915
  @Test
  void parametersInOrAndTimeRangesAgreeWithTheDocumentTwin() {
    forEachState8915(() -> {
      for (final String t : new String[] { "T", "D" })
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM " + t + " WHERE host = ? OR host = ? GROUP BY b", "a", "b")) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).as(t).isEqualTo(4L);
        }
      for (final String t : new String[] { "T", "D" })
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + t + " WHERE ts >= ? OR host = ?", 3000L, "a")) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).as(t).isEqualTo(3L);
        }
    });
  }

  // #8915
  @Test
  void aContradictoryRangeOrAnUnderstoodTimePredicateStaysExactOnThePushDown() {
    forEachState8915(() -> {
      for (final String w : new String[] { "ts > 3000 AND ts < 2000", "ts != 2000", "ts IN [1000, 2000]", "ts > 1000 AND host = n",
          "host = n" })
        assertThat(groupedCount("T", w)).as(w).isEqualTo(groupedCount("D", w));
      assertThat(plan("SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE ts != 2000 GROUP BY b"))
          .doesNotContain("AGGREGATE FROM TIMESERIES");
    });
  }

  // #8915
  @Test
  void orOfTagsUnderASharedTimeRangeStaysExact() {
    forEachState8915(() -> {
      for (final String w : new String[] { "ts >= 2000 AND (host = 'a' OR host = 'b')", "ts >= 2000 AND (host = 'a' OR host = 'c')",
          "(ts >= 1000 AND host = 'a') OR (ts >= 2000 AND host = 'b')", "ts > 3000 AND ts < 2000 AND (host = 'a' OR host = 'b')" })
        assertThat(groupedCount("T", w)).as(w).isEqualTo(groupedCount("D", w));
    });
  }

  // #8915
  @Test
  void parametersInTheTimePredicatesOfEachBlockAreComparedByValue() {
    forEachState8915(() -> {
      final String sql = "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM %s WHERE (ts >= ? AND host = ?) OR (ts >= ? AND host = ?) GROUP BY b";
      for (final Object[] params : new Object[][] { { 1000L, "a", 3000L, "b" }, { 2000L, "a", 2000L, "b" } })
        try (final ResultSet rs = database.query("sql", String.format(sql, "T"), params);
            final ResultSet ref = database.query("sql", String.format(sql, "D"), params)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(((Number) ref.next().getProperty("c")).longValue());
        }
      final String named = "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM %s WHERE (ts >= :x AND host = 'a') OR (ts >= :y AND host = 'b') GROUP BY b";
      final Map<String, Object> m = Map.of("x", 1000L, "y", 3000L);
      try (final ResultSet rs = database.query("sql", String.format(named, "T"), m);
          final ResultSet ref = database.query("sql", String.format(named, "D"), m)) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(((Number) ref.next().getProperty("c")).longValue());
      }
    });
  }

  // #8915
  @Test
  void orOfTagsAcrossBlocksStaysExact() {
    forEachState8915(() -> {
      final String w = "host = 'a' OR host = 'b'";
      assertThat(groupedCount("T", w)).isEqualTo(4L);
      assertThat(groupedCount("T", "host = 'a' OR host = 'c'")).isEqualTo(groupedCount("D", "host = 'a' OR host = 'c'"));
    });
  }

  // ---- #8152 ----
  // https://github.com/ArcadeData/arcadedb/issues/8152 (correlated finding)
  // <p>
  // {@code SelectExecutionPlanner.toEpochMs} is the fourth near-copy of the same date-to-millis conversion the
  // continuous-aggregate refresher got wrong, and the one that knew the fewest types: a {@link java.time.LocalDateTime},
  // which is the engine's own DATETIME representation and what {@code date()} answers by default, fell through to the
  // {@code Long.MIN_VALUE} "not a timestamp" sentinel.
  // <p>
  // On the binary-comparison path that sentinel was checked and the push-down declined, so the answer stayed right. On
  // the BETWEEN path it was not checked at all: it became the range's UPPER bound, the pushed-down scan range was
  // {@code [from, Long.MIN_VALUE]} - empty - and the query answered ZERO rows for a range that holds data. The
  // converter is now shared, and the sentinel is refused as a bound either way.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long HOUR8152 = 3_600_000L;

  @Override
  protected void beginTest() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Reading SET ts = ?, sensor = 'A', value = ?", i * HOUR8152, (double) i);
    });
  }

  // #8152
  @Test
  void betweenWithDateTimeBoundsReturnsTheRowsInTheRange() {
    // 1970-01-01T01:00 .. 1970-01-01T03:00 inclusive: hours 1, 2 and 3.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts BETWEEN date('1970-01-01 01:00:00') "
        + "AND date('1970-01-01 03:00:00')")).isEqualTo(3);
  }

  // #8152
  @Test
  void betweenWithEpochMillisBoundsIsUnchanged() {
    // The control: numeric bounds always worked, and must keep answering the same rows as the datetime ones.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts BETWEEN " + HOUR8152 + " AND " + (3 * HOUR8152)))
        .isEqualTo(3);
  }

  // #8152
  @Test
  void comparisonsWithDateTimeBoundsAgreeWithTheirNumericTwins() {
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts >= date('1970-01-01 02:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts >= " + (2 * HOUR8152)))
        .isEqualTo(4);
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts < date('1970-01-01 02:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts < " + (2 * HOUR8152)))
        .isEqualTo(2);
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts = date('1970-01-01 04:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts = " + (4 * HOUR8152)))
        .isEqualTo(1);
  }

  // #8152
  @Test
  void aBetweenOnANonTimestampValueDoesNotEmptyTheResult() {
    // A bound the converter cannot read must decline the push-down, never become a bound of its own.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE value BETWEEN 1 AND 3")).isEqualTo(3);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
