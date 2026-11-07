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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Row-scan push-down: SQL queries answered from the time series engine without the generic scan.
 */
class TimeSeriesRowScanPushDownTest extends TestHelper {

  // ---- #10324 ----
  // Issue #10324: a TIMESERIES row scan whose WHERE the fetch step already reproduces exactly - a time range, tag equalities and
  // range predicates on numeric FIELD columns - evaluated the whole WHERE a second time in a FILTER step over every row it returned.
  // The residual filter is now planned only for the predicates the engine does not reproduce. Every query is checked against a
  // document twin holding the same rows, in every storage state (mutable, sealed, sealed plus a fresh tail).
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long T0    = 1_767_225_600_000L;

  private static final long HOUR  = 3_600_000L;

  private static final int  HOSTS = 4;

  private static final String RANGE = "ts >= " + (T0 + 20 * 60_000L) + " AND ts < " + (T0 + 2 * HOUR + 7 * 60_000L);

  private void forEachState(final int shards, final Runnable check) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE, ui LONG, uf FLOAT) SHARDS "
        + shards + " COMPACTION_INTERVAL 1 HOURS");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    load(0, 120);
    check.run(); // everything in the mutable buffer
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run(); // everything sealed
    load(120, 170);
    check.run(); // sealed blocks plus a fresh tail
    database.command("sql", "DROP TYPE T");
    database.command("sql", "DROP TYPE D");
  }

  private void load(final int fromSample, final int toSample) {
    database.transaction(() -> {
      for (int s = fromSample; s < toSample; s++)
        for (int h = 0; h < HOSTS; h++) {
          final long ts = T0 + s * 60_000L + h * 1_000L;
          // ABSENT VALUES ON BOTH A FLOATING-POINT AND AN INTEGRAL COLUMN: A PUSHED PREDICATE MUST DROP THEM AS SQL DOES
          final Double uu = (s + h) % 9 == 0 ? null : (double) ((s * 31 + h * 17) % 101);
          final Long ui = (s + h) % 7 == 0 ? null : s * 3L + h;
          final float uf = ((s + h) % 10) / 10f;
          final String host = "host_" + h;
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?", ts, host, uu, ui, uf);
          database.command("sql", "INSERT INTO D SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?", ts, host, uu, ui, uf);
        }
    });
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final TreeMap<String, String> sorted = new TreeMap<>();
        for (final String name : r.getPropertyNames()) {
          final Object value = r.getProperty(name);
          final String text;
          // the twin stores the timestamp as the number it was given, the TIMESERIES type hands it back as a date-time
          if (value instanceof LocalDateTime d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.toInstant(ZoneOffset.UTC).toEpochMilli());
          else if (value instanceof Date d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.getTime());
          else if (value instanceof Number n)
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
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** {@code sql} names the type {@code X}; it is run against the TIMESERIES type and against its document twin. */
  private void assertSameAsTheTwin(final String sql) {
    final List<String> expected = rows(sql.replace(" X", " D"));
    assertThat(expected).as("the twin answers something: " + sql).isNotEmpty();
    assertThat(rows(sql.replace(" X", " T"))).as(sql).isEqualTo(expected);
  }

  private void assertNoResidualFilter(final String sql) {
    assertThat(plan(sql.replace(" X", " T"))).as(sql).contains("FETCH FROM TIMESERIES").doesNotContain("FILTER ITEMS");
    assertSameAsTheTwin(sql);
  }

  private void assertResidualFilter(final String sql) {
    assertThat(plan(sql.replace(" X", " T"))).as(sql).contains("FETCH FROM TIMESERIES").contains("FILTER ITEMS");
    assertSameAsTheTwin(sql);
  }

  // #10324
  @Test
  void aWhereTheFetchReproducesIsNotEvaluatedAgain() {
    for (final int shards : new int[] { 1, 3 })
      forEachState(shards, () -> {
        // THE REPORTED SHAPE: A TIME RANGE AND A FIELD PREDICATE
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE + " AND uu > 90.0");
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE);
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0");
        assertNoResidualFilter("SELECT uu FROM X WHERE " + RANGE);
        assertNoResidualFilter("SELECT FROM X WHERE host = 'host_2' AND " + RANGE + " AND ui BETWEEN 30 AND 200");
        assertNoResidualFilter("SELECT ts, ui FROM X WHERE ui >= 0");
        assertNoResidualFilter("SELECT ts, uu FROM X WHERE uu = 50");
        assertNoResidualFilter("SELECT host, ts FROM X WHERE host = 'host_1' ORDER BY ts DESC LIMIT 5");
        assertNoResidualFilter("SELECT host, ts FROM X WHERE " + RANGE + " LIMIT 7");
      });
  }

  // #10324
  @Test
  void aPredicateTheFetchDoesNotReproduceIsStillFiltered() {
    forEachState(2, () -> {
      // A FLOAT column, a negation and an OR of two different tags are left to the SQL filter
      assertResidualFilter("SELECT host, ts, uf FROM X WHERE " + RANGE + " AND uf > 0.5");
      assertResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE + " AND host <> 'host_1'");
      assertResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0 AND (host = 'host_1' OR ui > 300)");
      // a field predicate takes no row cap into the fetch, so it goes to the unbounded read, which reproduces it, and the LIMIT
      // runs above
      assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0 LIMIT 3");
      // the same tag twice is an intersection the tag filter does not model exactly
      assertResidualFilter("SELECT host, ts FROM X WHERE host = 'host_1' AND host = 'host_1' AND " + RANGE);
    });
  }

  // #10324
  @Test
  void aProjectionOfColumnsIsBuiltByTheFetch() {
    forEachState(2, () -> {
      // THE REPORTED SHAPE: THE ROW IS BUILT ONCE, IN THE FETCH, AND NO PROJECTION STEP COPIES IT
      final String reported = "SELECT host, ts, uu FROM X WHERE " + RANGE + " AND uu > 90.0";
      assertThat(plan(reported.replace(" X", " T"))).contains("PROJECTED host, ts, uu").doesNotContain("CALCULATE PROJECTIONS");
      assertSameAsTheTwin(reported);

      // ALIASES, IN PROJECTION ORDER
      assertThat(plan("SELECT uu AS value, host AS h FROM T WHERE " + RANGE)).contains("PROJECTED uu AS value, host AS h");
      assertSameAsTheTwin("SELECT uu AS value, host AS h FROM X WHERE " + RANGE);
      assertThat(columnNames("SELECT uu AS value, host AS h, ts FROM T WHERE " + RANGE)).containsExactly("value", "h", "ts");

      // AN ORDER BY ON A COLUMN THE PROJECTION LEAVES OUT: THE COLUMN IS PROJECTED FOR THE SORT AND DROPPED AFTER IT
      assertSameAsTheTwin("SELECT host, uu FROM X WHERE " + RANGE + " ORDER BY ui DESC, ts LIMIT 20");
      assertThat(columnNames("SELECT host, uu FROM T WHERE " + RANGE + " ORDER BY ui DESC, ts LIMIT 20")).containsExactly("host", "uu");
      assertSameAsTheTwin("SELECT host FROM X WHERE host = 'host_1' ORDER BY ts DESC LIMIT 5");
      assertSameAsTheTwin("SELECT DISTINCT host FROM X WHERE " + RANGE);
      assertSameAsTheTwin("SELECT uu FROM X WHERE " + RANGE + " SKIP 10 LIMIT 15");

      // NOT A PLAIN COLUMN, OR A WHERE THE FETCH DOES NOT REPRODUCE: THE PROJECTION STEP STAYS
      assertThat(plan("SELECT host, uu * 2 AS d FROM T WHERE " + RANGE)).contains("CALCULATE PROJECTIONS").doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT host, uu * 2 AS d FROM X WHERE " + RANGE);
      assertThat(plan("SELECT host, uf FROM T WHERE uf > 0.5")).contains("CALCULATE PROJECTIONS").doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT host, uf FROM X WHERE uf > 0.5");
      assertThat(plan("SELECT FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertThat(plan("SELECT *, uu AS v FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT count(*) AS c, host FROM X WHERE " + RANGE + " GROUP BY host");
    });
  }

  // #10324
  @Test
  void duplicatedColumnsCaseAndLiteralsTheTagFilterDoesNotModel() {
    forEachState(1, () -> {
      // THE SAME COLUMN TWICE, UNDER ITS NAME AND AN ALIAS
      assertThat(plan("SELECT uu, uu AS b, host, uu FROM T WHERE " + RANGE)).contains("PROJECTED");
      assertSameAsTheTwin("SELECT uu, uu AS b, host, uu FROM X WHERE " + RANGE);
      assertThat(columnNames("SELECT uu, uu AS b, host FROM T WHERE " + RANGE)).containsExactly("uu", "b", "host");
      // A NAME THAT IS NOT A COLUMN AS WRITTEN (NAMES ARE CASE-SENSITIVE) IS LEFT TO THE PROJECTION STEP, WHICH ANSWERS NULL
      assertThat(plan("SELECT HOST, uu FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT HOST, uu FROM X WHERE " + RANGE);
      // A NULL TAG LITERAL IS NO EXACT FILTER, A LITERAL OF ANOTHER CASE IS ONE THAT MATCHES NOTHING: BOTH ANSWER AS THE TWIN
      assertThat(plan("SELECT host FROM T WHERE host = null AND " + RANGE)).contains("FILTER ITEMS");
      assertThat(rows("SELECT host FROM T WHERE host = null AND " + RANGE)).isEqualTo(rows("SELECT host FROM D WHERE host = null AND " + RANGE));
      assertThat(plan("SELECT host FROM T WHERE host = 'HOST_1'")).doesNotContain("FILTER ITEMS");
      assertThat(rows("SELECT host FROM T WHERE host = 'HOST_1'")).isEmpty();
      assertThat(rows("SELECT host FROM D WHERE host = 'HOST_1'")).isEmpty();
    });
  }

  // #10324
  @Test
  void aTimestampReadsBackAsTheSameDateTimeAcrossDayBoundariesAndBefore1970() {
    database.command("sql", "CREATE TIMESERIES TYPE B TIMESTAMP ts FIELDS (v LONG) SHARDS 1");
    final long day = 86_400_000L;
    final long[] stamps = { -day - 1, -day, -1, 0, 1, day - 1, day, 3 * day + 12_345, T0 };
    for (final long ts : stamps)
      database.command("sql", "INSERT INTO B SET ts = ?, v = ?", ts, ts);
    for (int pass = 0; pass < 2; pass++) {
      try (final ResultSet rs = database.query("sql", "SELECT ts, v FROM B ORDER BY ts")) {
        int n = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          final long ts = r.<Number>getProperty("v").longValue();
          assertThat(r.<LocalDateTime>getProperty("ts")).as("ts " + ts)
              .isEqualTo(LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(ts), ZoneOffset.UTC));
          n++;
        }
        assertThat(n).isEqualTo(stamps.length);
      }
      database.command("sql", "COMPACT TIMESERIES TYPE B");
    }
  }

  private List<String> columnNames(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return new ArrayList<>(rs.next().getPropertyNames());
    }
  }

  // #10324
  @Test
  void aTemporalTagLiteralTheColumnCannotReadIsLeftToTheFilter() {
    database.command("sql", "CREATE TIMESERIES TYPE TD TIMESTAMP ts TAGS (day DATE) FIELDS (v LONG) SHARDS 1");
    database.command("sql", "INSERT INTO TD SET ts = 1000, day = '2026-01-01', v = 1");
    database.command("sql", "INSERT INTO TD SET ts = 2000, day = '2026-01-02', v = 2");
    // the literal is not pushed into the tag filter, so the fetch returns every row and only the filter can drop them
    assertThat(plan("SELECT FROM TD WHERE day = 'not a date'")).contains("FILTER ITEMS");
    assertThat(plan("SELECT FROM TD WHERE day = '2026-01-02'")).doesNotContain("FILTER ITEMS");
    // a literal finer than the tag's precision is truncated into the tag filter, which then keeps the whole day: only the filter
    // gives the SQL answer, no row
    try (final ResultSet rs = database.query("sql", "SELECT FROM TD WHERE day = ?", LocalDateTime.of(2026, 1, 2, 10, 30))) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT v FROM TD WHERE day = '2026-01-02'")) {
      assertThat(rs.next().<Number>getProperty("v").longValue()).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- #7965 ----
  // Issue #7965: an existence check answered by the sealed layer must not read the mutable bucket.
  // <p>
  // {@code hasRowsInRange} backs {@code GET /prom/api/v1/label/__name__/values} scoped to a range (#7709) and is
  // meant to be cheap: the sealed layer drops a block whose directory entry puts it outside the window, so a "yes"
  // costs one block read and a "no" costs none. It used to be folded over {@link TimeSeriesEngine#forEachRow} with
  // a visitor stopping on the first row, and #7897 made that fold pay a FULL mutable-bucket scan per shard whatever
  // the visitor did: the two layers are read in one lock window, up front, because a compaction landing between
  // them would seal the bucket's rows into blocks the sealed walk then hands over as well.
  // <p>
  // The probe no longer goes through {@code forEachRow} at all. It has a walk of its own that visits the layers one
  // after the other and stops at the first row either produces, under the shard's {@code compactionLock} read lock -
  // which {@code forEachRow} cannot hold, because the visitor there is the CALLER's code and its cost is unbounded
  // from inside the shard, while here the "visitor" is a constant {@code false} and the whole probe is bounded by
  // one block decode plus one page.
  // <p>
  // <b>Why the cheap assertion is paired with an expensive one.</b> Asserting that a sealed-answered probe scanned
  // zero mutable pages would have passed before this change as well - not because the bucket went unread, but
  // because {@code forEachRow} passed {@code null} metrics to the bucket scan, so the pages it read were never
  // counted. {@link #aMutableAnswerIsChargedForThePagesItReads} is what says the counter is wired to the probe's
  // own bucket read, and only with it does a zero in {@link #aSealedAnswerReadsNoMutablePage} mean the bucket was
  // not opened.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long BASE_TS   = 1_700_000_000_000L;

  private static final long STEP_MS   = 10L;

  private static final long BUCKET_MS = 1_000L;

  // #7965
  /** The defect: the sealed layer has the answer, and the mutable bucket is never opened to confirm it. */
  @Test
  void aSealedAnswerReadsNoMutablePage() throws Exception {
    final TimeSeriesEngine engine = createMetric("sealed_answer", 1);
    append(engine, 0, 500);
    engine.compactAll();
    // Enough rows to span several bucket pages, all INSIDE the queried range, so a scan of them is not something
    // the page headers could have skipped.
    append(engine, 100_000, 5_000);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS, Long.MAX_VALUE, metrics)).isTrue();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("one block read is what the answer costs")
        .isEqualTo(1);
    assertThat(metrics.getScannedPages() + metrics.getSkippedPages())
        .as("the mutable bucket is not opened at all once the sealed layer has answered")
        .isZero();
  }

  // #7965
  /**
   * The anti-vacuity half: when the answer DOES come from the mutable bucket, the pages it took to find it are
   * counted. Without this the zero above would say nothing about whether the bucket was read.
   */
  @Test
  void aMutableAnswerIsChargedForThePagesItReads() throws Exception {
    final TimeSeriesEngine engine = createMetric("mutable_answer", 1);
    append(engine, 0, 500);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS, Long.MAX_VALUE, metrics)).isTrue();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("nothing is sealed yet, so no block answers")
        .isZero();
    assertThat(metrics.getScannedPages()).as("the bucket page the row was found on").isEqualTo(1);
  }

  // #7965
  /**
   * A "no" consults BOTH layers, and says so: the sealed layer drops every block on its directory entry and the
   * bucket drops every page on its header, but neither is skipped as a layer.
   */
  @Test
  void aNoAnswerConsultsBothLayers() throws Exception {
    final TimeSeriesEngine engine = createMetric("no_answer", 1);
    append(engine, 0, 500);
    engine.compactAll();
    append(engine, 100_000, 500);

    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(BASE_TS + 10_000_000, BASE_TS + 20_000_000, metrics)).isFalse();

    assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
        .as("no block is read to answer 'nothing here'")
        .isZero();
    assertThat(metrics.getScannedPages()).as("no page is examined either").isZero();
    assertThat(metrics.getSkippedPages()).as("but the bucket WAS consulted, page header by page header")
        .isGreaterThanOrEqualTo(1);
  }

  // #7965
  /** The answers themselves, over every combination of the two layers, and over more than one shard. */
  @Test
  void theAnswerIsTheOneAFullScanWouldGive() throws Exception {
    final TimeSeriesEngine engine = createMetric("answers", 3);
    append(engine, 0, 500);
    engine.compactAll();
    append(engine, 900_000, 100);

    assertRangeAgreesWithScan(engine, Long.MIN_VALUE, Long.MAX_VALUE);
    assertRangeAgreesWithScan(engine, BASE_TS, BASE_TS + 100);
    assertRangeAgreesWithScan(engine, BASE_TS + 900_000, Long.MAX_VALUE);
    assertRangeAgreesWithScan(engine, BASE_TS + 10_000_000, BASE_TS + 20_000_000);
    // A window between the sealed rows and the mutable ones: both layers hold rows, neither holds one here.
    assertRangeAgreesWithScan(engine, BASE_TS + 500_000, BASE_TS + 600_000);

    final TimeSeriesEngine empty = createMetric("answers_empty", 2);
    assertThat(empty.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null))
        .as("a type holding no sample at all")
        .isFalse();
  }

  // #7965
  /**
   * A single sample on the far edge of the mutable bucket: the probe must walk past every page that cannot hold it
   * rather than stop at the first one, which is what a {@code hasNext()} on a lazy iterator gets wrong if the
   * iterator is not advanced across empty pages.
   */
  @Test
  void aLoneMutableRowIsFoundBehindManyPages() throws Exception {
    final TimeSeriesEngine engine = createMetric("lone_row", 1);
    append(engine, 0, 5_000);

    final long lastTs = BASE_TS + (5_000 - 1) * STEP_MS;
    final AggregationMetrics metrics = new AggregationMetrics();
    assertThat(engine.hasRowsInRange(lastTs, lastTs, metrics)).isTrue();
    assertThat(metrics.getScannedPages()).as("exactly the one page that holds it").isEqualTo(1);
    assertThat(metrics.getSkippedPages()).as("the pages before it are dropped on their headers")
        .isGreaterThanOrEqualTo(1);
  }

  // --- helpers ---

  private void assertRangeAgreesWithScan(final TimeSeriesEngine engine, final long fromTs, final long toTs)
      throws IOException {
    final boolean scanned = !engine.forEachRow(fromTs, toTs, new int[0], null, null, row -> false);
    assertThat(engine.hasRowsInRange(fromTs, toTs, null))
        .as("range [%d, %d]", fromTs, toTs)
        .isEqualTo(scanned);
  }

  private TimeSeriesEngine createMetric(final String typeName, final int shards) {
    new TimeSeriesTypeBuilder((DatabaseInternal) database)
        .withName(typeName)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(shards)
        .withCompactionBucketInterval(BUCKET_MS)
        .create();
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private void append(final TimeSeriesEngine engine, final long startOffsetMs, final int count) throws IOException {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  // ---- #7899 ----
  // Issue #7899: an engine row is {@code [timestamp, non-TIMESTAMP columns in schema order...]} whatever position
  // the TIMESTAMP column occupies in the declaration - {@code TimeSeriesBucket.readRow} writes {@code result[0]}
  // from the row's own timestamp slot and then skips the TIMESTAMP column while walking the schema. A consumer
  // that maps row position {@code i} onto {@code getTsColumns().get(i)} is therefore only right when the TIMESTAMP
  // column happens to be declared first, which issue #7702 stopped being a property of every declaration the
  // grammar can spell.
  // <p>
  // Both cases below declare a column BEFORE the timestamp. The STRING one makes a mis-mapping loud (a tag value
  // lands on the timestamp), the LONG one makes it silent (two numbers swap) - which is the variant that ships.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7899">issue #7899</a>

  private static final long TS = 1_700_000_000_000L;

  // #7899
  /**
   * The SQL read path. {@code SaveElementStep.saveToTimeSeries} resolves every column BY NAME, so the INSERT is
   * already correct and anything wrong in the row that comes back is the read mapping alone.
   */
  @Test
  void aSelectStarMapsEveryValueToItsOwnColumnWhenATagIsDeclaredBeforeTheTimestamp() {
    database.command("sql", "CREATE TIMESERIES TYPE TagFirst TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "INSERT INTO TagFirst SET ts = " + TS + ", host = 'srv-1', v = 42.5");

    try (final ResultSet rs = database.query("sql", "SELECT * FROM TagFirst")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("host")).as("the TAG value must not land on the timestamp").isEqualTo("srv-1");
      assertThat((Object) row.getProperty("ts")).isInstanceOf(LocalDateTime.class);
      assertThat((Object) row.getProperty("v")).isEqualTo(42.5);
    }
  }

  // #7899
  /** The silent variant: a LONG column before the timestamp swaps two numbers and raises nothing. */
  @Test
  void aSelectStarMapsEveryValueToItsOwnColumnWhenANumericFieldIsDeclaredBeforeTheTimestamp() {
    database.command("sql", "CREATE TIMESERIES TYPE SeqFirst FIELDS (seq LONG) TIMESTAMP ts FIELDS (v DOUBLE)");
    database.command("sql", "INSERT INTO SeqFirst SET ts = " + TS + ", seq = 7, v = 42.5");

    try (final ResultSet rs = database.query("sql", "SELECT * FROM SeqFirst")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("seq")).longValue()).as("seq must not receive the timestamp").isEqualTo(7L);
      assertThat((Object) row.getProperty("v")).isEqualTo(42.5);
    }
  }

  // #7899
  /**
   * The projection helper every wire surface names its response columns from. Its own javadoc already promises
   * "the timestamp column first, then the selected non-timestamp columns in schema order" - the {@code null}
   * (every column) branch returned the raw schema instead, so the names GET /ts/latest, POST /ts/query, the
   * Grafana frames and both gRPC time-series RPCs publish were off against the values beside them.
   */
  @Test
  void theFullRowProjectionListsTheTimestampFirstWhateverTheDeclarationSays() {
    database.command("sql", "CREATE TIMESERIES TYPE Projected TAGS (host STRING) TIMESTAMP ts FIELDS (v DOUBLE)");
    final List<ColumnDefinition> columns =
        ((LocalTimeSeriesType) database.getSchema().getType("Projected")).getTsColumns();

    assertThat(TimeSeriesGateway.columnNames(columns, null))
        .as("null means every column, in the order the engine returns their values")
        .containsExactly("ts", "host", "v");
    // The projecting branch was already right; pinned here so the two branches cannot drift apart again.
    assertThat(TimeSeriesGateway.columnNames(columns, new int[] { 1 })).containsExactly("ts", "v");
  }

  // #7899
  /**
   * The row the helper's names describe, read straight off the engine: proves the ordering above is the engine's
   * and not this test's opinion of it.
   */
  @Test
  void theEngineRowItselfCarriesTheTimestampInPositionZero() throws Exception {
    database.getSchema().buildTimeSeriesType().withName("Raw")
        .withTag("host", Type.STRING).withTimestamp("ts").withField("v", Type.DOUBLE).create();

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Raw");
    database.begin();
    tsType.getEngine().appendBatch(new long[] { TS }, new Object[][] { { "srv-1" }, { 42.5 } });
    database.commit();

    final List<Object[]> rows = tsType.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsExactly(TS, "srv-1", 42.5);
  }
}
