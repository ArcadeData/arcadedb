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
package com.arcadedb.function.sql.time;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for the SQL time functions (ts.*, sysdate(), duration(), date()).
 */
class SQLTimeFunctionRegressionTest {

  /**
   * Regression for #6388: several SQL time/date functions answer a typed argument error instead of a raw JDK exception.
   * <p>
   * Several SQL time/date functions threw a raw JDK exception - an HTTP 500 - for ordinary valid input, and
   * {@code sysdate()} silently ignored the argument it documents (issue #6388). Each of these is now either the right
   * answer or a typed argument error, which the HTTP layer maps to 400.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeFunctionArgumentValidation extends TestHelper {

    @Test
    void sysdateAppliesTheZoneGivenAsItsOnlyArgument() throws Exception {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);
      try (final ResultSet rs = database.query("sql",
          "SELECT sysdate('America/New_York') AS ny, sysdate('Europe/Rome') AS rome")) {
        final var row = rs.next();
        final ZonedDateTime ny = row.getProperty("ny");
        final ZonedDateTime rome = row.getProperty("rome");
        // The argument used to be read as params[1] and therefore dropped: both answers were the same server-local
        // LocalDateTime with no zone at all.
        assertThat(ny.getZone().getId()).isEqualTo("America/New_York");
        assertThat(rome.getZone().getId()).isEqualTo("Europe/Rome");
      }
    }

    @Test
    void sysdateRejectsAnUnknownZone() {
      assertThatThrownBy(() -> database.query("sql", "SELECT sysdate('Bogus/Zone') AS d").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Bogus/Zone");
    }

    @Test
    void durationSupportsWeeksAsAnExactAmountOfTime() {
      try (final ResultSet rs = database.query("sql", "SELECT duration(2, 'week') AS d")) {
        assertThat(rs.next().<Duration>getProperty("d")).isEqualTo(Duration.ofDays(14));
      }
      try (final ResultSet rs = database.query("sql", "SELECT duration(3, 'days') AS d")) {
        assertThat(rs.next().<Duration>getProperty("d")).isEqualTo(Duration.ofDays(3));
      }
    }

    @Test
    void durationRejectsCalendarUnitsWithAReadableMessage() {
      for (final String unit : new String[] { "year", "month", "years", "months" })
        assertThatThrownBy(() -> database.query("sql", "SELECT duration(1, '" + unit + "') AS d").next())
            .as(unit)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("no fixed length");
    }

    /**
     * The weeks conversion is the one place an amount can overflow, and it must answer the same typed error as every
     * other bad argument in this family rather than a bare ArithmeticException.
     */
    @Test
    void durationRejectsAnAmountThatOverflows() {
      assertThatThrownBy(() -> database.query("sql", "SELECT duration(1000000000000000000, 'week') AS d").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("overflows");
    }

    /**
     * An offset outside the int range must saturate rather than wrap: Number.intValue() on a huge long returns an
     * unrelated - possibly small and positive - number that would sail past the non-negative check.
     */
    @Test
    void lagAndLeadSaturateAnOutOfRangeOffset() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Huge6388");
        for (int i = 0; i < 3; i++)
          database.command("sql", "INSERT INTO Huge6388 SET v = ?, ts = ?", i, i);
      });

      database.transaction(() -> {
        // 2^32 wraps to 0 under intValue(); saturated it stays past the end of the rows, so every row takes the default.
        try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v, 4294967296, ts) AS r FROM Huge6388")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, null, null);
        }
        try (final ResultSet rs = database.query("sql", "SELECT ts.lead(v, 4294967296, ts) AS r FROM Huge6388")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, null, null);
        }
      });
    }

    @Test
    void timeBucketRejectsANonPositiveInterval() {
      assertThatThrownBy(() -> database.query("sql", "SELECT ts.timeBucket('0s', 1700000000000) AS b").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("positive");
    }

    @Test
    void timeBucketWithANullArgumentIsNullNotAnNpe() {
      try (final ResultSet rs = database.query("sql", "SELECT ts.timeBucket(null, 1700000000000) AS b")) {
        assertThat(rs.next().<Object>getProperty("b")).isNull();
      }
    }

    @Test
    void lagAndLeadRejectANegativeOffset() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Sample6388");
        for (int i = 0; i < 3; i++)
          database.command("sql", "INSERT INTO Sample6388 SET v = ?, ts = ?", i, i);
      });

      database.transaction(() -> {
        for (final String fn : new String[] { "ts.lag", "ts.lead" })
          assertThatThrownBy(() -> database.query("sql", "SELECT " + fn + "(v, -1, ts) AS r FROM Sample6388").next())
              .as(fn)
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("non-negative");
      });
    }

    @Test
    void lagWithoutATimestampKeepsArrivalOrderInsteadOfThrowing() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Arrival6388");
        for (final String v : new String[] { "a", "b", "c" })
          database.command("sql", "INSERT INTO Arrival6388 SET v = ?", v);
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v) AS r FROM Arrival6388")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, "a", "b");
        }
        try (final ResultSet rs = database.query("sql", "SELECT ts.lead(v) AS r FROM Arrival6388")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly("b", "c", null);
        }
      });
    }

    @Test
    void lagStillOrdersByTheTimestampWhenGivenOne() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Ordered6388");
        database.command("sql", "INSERT INTO Ordered6388 SET v = 'c', ts = 3");
        database.command("sql", "INSERT INTO Ordered6388 SET v = 'a', ts = 1");
        database.command("sql", "INSERT INTO Ordered6388 SET v = 'b', ts = 2");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v, 1, ts) AS r FROM Ordered6388")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, "a", "b");
        }
      });
    }

    @Test
    void dateRejectsAMalformedPatternInsteadOfLeakingTheJdkException() {
      // An unterminated literal quote: DateTimeFormatterBuilder.appendPattern() rejects it with an
      // IllegalArgumentException, which is NOT a DateTimeParseException and so escaped the catch untouched.
      assertThatThrownBy(() -> database.query("sql", "SELECT date('2020-01-01', \"yyyy-'\") AS d").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("invalid date format");
    }

    @Test
    void dateRejectsAnUnknownZoneInsteadOfLeakingTheJdkException() {
      assertThatThrownBy(() -> database.query("sql", "SELECT date('2020-01-01', 'yyyy-MM-dd', 'Bogus/Zone') AS d").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Bogus/Zone");
    }

    @Test
    void dateStillAnswersNullWhenTheValueDoesNotMatchTheFormat() {
      try (final ResultSet rs = database.query("sql", "SELECT date('not-a-date', 'yyyy-MM-dd') AS d")) {
        assertThat(rs.next().<Object>getProperty("d")).isNull();
      }
    }

    @Test
    void theFormatterCacheIsBounded() {
      final int before = DateUtils.getCachedFormatterCount();
      // Distinct caller-supplied patterns used to be remembered forever, one entry each.
      for (int i = 0; i < 5_000; i++)
        DateUtils.getFormatter("yyyy-MM-dd'" + ("x".repeat(i % 40 + 1)) + i + "'");

      assertThat(DateUtils.getCachedFormatterCount()).isGreaterThanOrEqualTo(before).isLessThanOrEqualTo(1_000 + 64);
    }
  }

  /**
   * Regression for #6824: ts.timeBucket floors pre-epoch timestamps.
   * <p>
   * Issue #6824: {@code ts.timeBucket()} computed its boundary with plain Java integer division, which truncates
   * toward zero rather than flooring. For any negative epoch value that puts the returned "bucket start" AFTER the
   * timestamp it is supposed to contain, and it collapses the last pre-epoch bucket into the first post-epoch one.
   * <p>
   * A bucket function has exactly one contract - {@code bucket(t) <= t}, and every {@code t} in one interval maps to
   * one boundary - so the two arms below are that contract, not a spot check of two constants: the containment arm
   * would fail for EVERY negative input under the old expression, and the partition arm is what a {@code GROUP BY}
   * key actually relies on. Truncation toward zero merges {@code [-1h, 0)} into the bucket for {@code [0, 1h)}, so a
   * historical series crossing 1970 silently aggregated two hours of samples under one boundary.
   * <p>
   * The engine's own bucket anchor was already floor-aligned for #4595
   * ({@code TimeSeriesNegativeTimestampBucketTest}); this is the same defect one layer up, in the SQL function, which
   * that fix did not reach. The {@code intervalMs <= 0} guard added for #6388 only ever guarded the divisor.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeBucketPreEpoch extends TestHelper {

    private static final long HOUR_MS = 3_600_000L;

    private long bucket(final String interval, final long timestampMs) {
      try (final ResultSet resultSet = database.query("sql",
          "SELECT ts.timeBucket('" + interval + "', " + timestampMs + ") AS b")) {
        // Issue #7610: the bucket is a UTC-anchored LocalDateTime, not a java.util.Date.
        return resultSet.next().<LocalDateTime>getProperty("b").toInstant(ZoneOffset.UTC).toEpochMilli();
      }
    }

    /**
     * The whole of the contract: the bucket start can never be later than the timestamp it buckets. Every negative
     * input below returned a LATER instant before the fix - {@code -1_800_000} (1969-12-31T23:30:00Z) came back as
     * the epoch itself, half an hour into its own future.
     */
    @Test
    void aBucketStartIsNeverLaterThanTheTimestampItBuckets() {
      for (final long timestampMs : new long[] { -1L, -1_800_000L, -HOUR_MS, -HOUR_MS - 1, -86_400_000L, -1_000_000_000_000L,
          0L, 1L, 1_700_000_000_000L })
        assertThat(bucket("1h", timestampMs)).as("bucket('1h', %d)", timestampMs).isLessThanOrEqualTo(timestampMs);
    }

    /**
     * The two documented repros, pinned as the exact boundaries they must be rather than only as "not in the future":
     * 1969-12-31T23:30:00Z belongs to the 23:00 hour, and -1ms belongs to 1969-12-31, not to 1970-01-01.
     */
    @Test
    void aPreEpochTimestampBucketsToItsOwnIntervalStart() {
      assertThat(bucket("1h", -1_800_000L)).isEqualTo(-HOUR_MS);
      assertThat(bucket("1d", -1L)).isEqualTo(-86_400_000L);
      // Exactly on a boundary is its own bucket start, on both sides of the epoch.
      assertThat(bucket("1h", -HOUR_MS)).isEqualTo(-HOUR_MS);
      assertThat(bucket("1h", 0L)).isZero();
    }

    /**
     * The reason this matters at all: used as a {@code GROUP BY} key, truncation toward zero merged the last
     * pre-epoch bucket into the first post-epoch one, so the two hours either side of 1970 aggregated as one.
     */
    @Test
    void thePreEpochAndPostEpochBucketsDoNotCollapseIntoEachOther() {
      final Map<Long, Integer> countByBucket = new HashMap<>();
      // Two samples in [-1h, 0) and three in [0, 1h): five samples, two buckets - never one bucket of five.
      for (final long timestampMs : new long[] { -HOUR_MS, -1L, 0L, 1L, HOUR_MS - 1 })
        countByBucket.merge(bucket("1h", timestampMs), 1, Integer::sum);

      assertThat(countByBucket).containsExactlyInAnyOrderEntriesOf(Map.of(-HOUR_MS, 2, 0L, 3));
    }

    /** The positive side is unchanged. */
    @Test
    void positiveTimestampsBucketExactlyAsBefore() {
      assertThat(bucket("1h", 3_900_000L)).isEqualTo(3_600_000L);
      assertThat(bucket("1d", 1_700_000_000_000L)).isEqualTo(1_699_920_000_000L);
    }

    /**
     * The typed overloads reach the same expression through {@code toEpochMs()}, which accepts a negative
     * {@code Date} and a pre-epoch {@code Instant} without complaint - so the fix has to hold for them too.
     */
    @Test
    void theTypedPreEpochOverloadsFloorAsWell() {
      try (final ResultSet resultSet = database.query("sql", "SELECT ts.timeBucket('1h', ?) AS b",
          new Date(-1_800_000L))) {
        assertThat(resultSet.next().<LocalDateTime>getProperty("b").toInstant(ZoneOffset.UTC).toEpochMilli())
            .isEqualTo(-HOUR_MS);
      }
      try (final ResultSet resultSet = database.query("sql", "SELECT ts.timeBucket('1h', ?) AS b",
          Instant.ofEpochMilli(-1_800_000L))) {
        assertThat(resultSet.next().<LocalDateTime>getProperty("b").toInstant(ZoneOffset.UTC).toEpochMilli())
            .isEqualTo(-HOUR_MS);
      }
    }
  }

  /**
   * Regression for #8892: sysdate() stores the real instant on any JVM zone.
   * <p>
   * {@code sysdate()} stored into a DATETIME held the real instant only when the JVM ran in UTC: the JVM-local wall clock
   * was handed to the engine, which stores a {@code LocalDateTime} as a UTC wall clock, and {@code sysdate('zone')}
   * labelled that same local wall clock with the zone instead of reading the clock there (issue #8892).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SysdateInstant extends TestHelper {
    private static final long TOLERANCE_MS = 10 * 60_000L; // the offsets under test are 4 hours and more

    private TimeZone previousZone;

    @BeforeEach
    void rememberZone() {
      previousZone = TimeZone.getDefault();
    }

    @AfterEach
    void restoreZone() {
      TimeZone.setDefault(previousZone);
    }

    @Test
    void sysdateStoresTheRealInstantOnAnyJvmZone() {
      for (final String jvmZone : new String[] { "UTC", "Asia/Seoul", "America/New_York" }) {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
        final String type = "E_" + jvmZone.replace('/', '_');
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".at DATETIME");

        final long before = System.currentTimeMillis();
        database.transaction(() -> {
          database.command("sql", "INSERT INTO " + type + " SET k = 'sysdate', at = sysdate()");
          database.command("sql", "INSERT INTO " + type + " SET k = 'date', at = date()");
          database.command("sql", "INSERT INTO " + type + " SET k = 'utc', at = sysdate('UTC')");
          database.command("sql", "INSERT INTO " + type + " SET k = 'seoul', at = sysdate('Asia/Seoul')");
          database.command("sql", "INSERT INTO " + type + " SET k = 'ny', at = sysdate('America/New_York')");
          database.newDocument(type).set("k", "java", "at", new Date()).save();
        });

        try (final ResultSet rs = database.query("sql", "SELECT k, at.asLong() AS ms FROM " + type)) {
          int rows = 0;
          while (rs.hasNext()) {
            final var row = rs.next();
            final long ms = row.<Long>getProperty("ms");
            assertThat(Math.abs(ms - before)).as("JVM zone %s, %s", jvmZone, row.<String>getProperty("k")).isLessThan(TOLERANCE_MS);
            rows++;
          }
          assertThat(rows).isEqualTo(6);
        }
      }
    }
  }

  /**
   * Regression for #9958: ts.movingAvg answers one element per input row.
   * <p>
   * Regression for issue #9958: {@code ts.movingAvg} must answer one element per input row (a row without a value keeps its
   * slot), and each window's average must depend only on the members of that window (a large sample that has left the
   * window must not leave a rounding residue behind).
   */
  @Nested
  class MovingAvgPositional extends TestHelper {

    private void insertSeries(final String type, final Double... values) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
        for (int i = 0; i < values.length; i++)
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, v = ?", (i + 1) * 1000L, values[i]);
      });
    }

    @SuppressWarnings("unchecked")
    private List<Object> single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        final Object value = rs.next().getProperty("r");
        return (List<Object>) value;
      }
    }

    @Test
    void nullSampleKeepsItsSlot() {
      insertSeries("Gap9958", 10.0, 20.0, null, 40.0, 50.0, 60.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM Gap9958");
      // window {20,null} averages the value it has; window {null,40} likewise
      assertThat(ma).containsExactly(10.0, 15.0, 20.0, 40.0, 45.0, 55.0);
    }

    @Test
    void windowWithNoValueAnswersNull() {
      insertSeries("AllNull9958", 1.0, null, null, 4.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM AllNull9958");
      assertThat(ma).containsExactly(1.0, 1.0, null, 4.0);
    }

    @Test
    void leadingNullsKeepTheirSlots() {
      insertSeries("Lead9958", null, null, 3.0, 5.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Lead9958");
      assertThat(ma).containsExactly(null, null, 3.0, 4.0);
    }

    @Test
    void largeSampleLeavingTheWindowLeavesNoResidue() {
      insertSeries("Spike9958", 1e16, 1.0, 1.0, 1.0, 1.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 2) AS r FROM Spike9958");
      assertThat(ma).containsExactly(1.0e16, 5.0e15, 1.0, 1.0, 1.0);
    }

    @Test
    void largeSampleInTheMiddleDoesNotCorruptLaterWindows() {
      insertSeries("Mid9958", 1.0, 2.0, 3.0, 1e17, 4.0, 5.0, 6.0, 7.0, 8.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Mid9958");
      assertThat(ma).hasSize(9);
      // windows that no longer hold the spike: {4,5,6}, {5,6,7}, {6,7,8}
      assertThat(ma.subList(6, 9)).containsExactly(5.0, 6.0, 7.0);
    }

    @Test
    void windowEqualToAndWiderThanTheSeries() {
      // small integers sum exactly in any order, so exact comparison is safe
      insertSeries("Wide9958", 2.0, null, 4.0, 9.0, 5.0);

      // w == n: the single block spans every row
      assertThat(single("SELECT ts.movingAvg(v, 5) AS r FROM Wide9958")).containsExactly(2.0, 2.0, 3.0, 5.0, 5.0);
      // w > n: every window is a prefix of the series
      assertThat(single("SELECT ts.movingAvg(v, 50) AS r FROM Wide9958")).containsExactly(2.0, 2.0, 3.0, 5.0, 5.0);
    }

    @Test
    void allNullSeriesAnswersOneNullPerRow() {
      insertSeries("OnlyNull9958", null, null, null);

      // before #9958 this answered [], now every row keeps its slot and no window holds a value
      assertThat(single("SELECT ts.movingAvg(v, 2) AS r FROM OnlyNull9958")).containsExactly(null, null, null);
    }

    @Test
    void controlSeriesUnchanged() {
      insertSeries("Ctl9958", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0);

      final List<Object> ma = single("SELECT ts.movingAvg(v, 3) AS r FROM Ctl9958");
      assertThat(ma).containsExactly(1.0, 1.5, 2.0, 3.0, 4.0, 5.0);
    }

    /**
     * The rule the issue names for the whole family: every positional {@code ts.*} function answers exactly one element per
     * input row, over a series that contains a null sample.
     */
    @Test
    void everyPositionalFunctionAnswersOneElementPerRow() {
      insertSeries("Family9958", 10.0, 20.0, null, 40.0, 50.0, 60.0);

      try (final ResultSet rs = database.query("sql",
          "SELECT count(*) AS c, ts.movingAvg(v, 2) AS ma, ts.rowNumber(ts) AS rn, ts.lag(v, 1, ts) AS lag, "
              + "ts.lead(v, 1, ts) AS lead, ts.rank(v, ts) AS rank, ts.interpolate(v, 'none', ts) AS ip FROM Family9958")) {
        final Result row = rs.next();
        final long rows = row.<Number>getProperty("c").longValue();
        assertThat(rows).isEqualTo(6L);
        for (final String fn : List.of("ma", "rn", "lag", "lead", "rank", "ip"))
          assertThat(row.<List<?>>getProperty(fn)).as(fn).hasSize((int) rows);
      }
    }

    @Test
    void groupByKeepsSlotsPerGroup() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Grp9958 BUCKETS 1");
        database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = 1.0");
        database.command("sql", "INSERT INTO Grp9958 SET g = 'b', v = 10.0");
        database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = null");
        database.command("sql", "INSERT INTO Grp9958 SET g = 'b', v = 30.0");
        database.command("sql", "INSERT INTO Grp9958 SET g = 'a', v = 5.0");
      });

      final Map<String, List<Object>> byGroup = new HashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT g, ts.movingAvg(v, 2) AS r FROM Grp9958 GROUP BY g")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          byGroup.put(row.getProperty("g"), row.getProperty("r"));
        }
      }
      assertThat(byGroup.get("a")).containsExactly(1.0, 1.0, 5.0);
      assertThat(byGroup.get("b")).containsExactly(10.0, 20.0);
    }

    /**
     * Each answer must equal the average of its own window's members, recomputed from scratch, over a long series with
     * nulls and occasional spikes. Small integers sum exactly, so a window free of spikes must match exactly.
     */
    @Test
    void matchesPerWindowReferenceOnLongSeries() {
      final Random random = new Random(9958);
      final int n = 500;
      final int w = 7;
      final Double[] values = new Double[n];
      for (int i = 0; i < n; i++) {
        final int pick = random.nextInt(20);
        values[i] = pick == 0 ? null : pick == 1 ? 1e16 : (double) random.nextInt(100);
      }
      insertSeries("Long9958", values);

      final List<Object> ma = single("SELECT ts.movingAvg(v, " + w + ") AS r FROM Long9958");
      assertThat(ma).hasSize(n);

      final List<Double> reference = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        double sum = 0;
        int count = 0;
        for (int j = Math.max(0, i - w + 1); j <= i; j++)
          if (values[j] != null) {
            sum += values[j];
            count++;
          }
        reference.add(count == 0 ? null : sum / count);
      }

      for (int i = 0; i < n; i++) {
        final Double expected = reference.get(i);
        final Object actual = ma.get(i);
        if (expected == null)
          assertThat(actual).as("slot %d", i).isNull();
        else
          assertThat((Double) actual).as("slot %d", i).isCloseTo(expected, within(Math.abs(expected) * 1e-12));
      }
    }
  }
}
