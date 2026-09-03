/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cypher's statement clock: every clock-reading function must answer from a single instant frozen once per
 * statement, the way Neo4j pins them to the transaction clock.
 * <p>
 * Issue #7052: {@code timestamp()} read {@link System#currentTimeMillis()} on every call, so
 * {@code RETURN timestamp() AS ts1, timestamp() AS ts2} returned two different values whenever the two
 * evaluations straddled a millisecond tick. The same helper also built its five frozen temporal values from
 * five separate clock readings, so {@code date()} and {@code datetime()} in one statement were not guaranteed
 * to describe the same instant either.
 */
class OpenCypherStatementClockTest extends TestHelper {

  /**
   * The deterministic form of the contract. One statement evaluates {@code timestamp()} 200,000 times over a
   * scan that unavoidably spans many milliseconds; a clock read per call therefore produces many distinct
   * values, while a statement-pinned value produces exactly one. Nothing here asserts on elapsed time - the
   * assertion is on the number of distinct values, which is 1 for any duration.
   */
  @Test
  void timestampIsPinnedAcrossManyEvaluationsInOneStatement() {
    final ResultSet result = database.command("opencypher",
        "UNWIND range(1, 200000) AS i RETURN collect(DISTINCT timestamp()) AS values");
    assertThat(result.hasNext()).isTrue();
    final List<Object> values = result.next().getProperty("values");
    assertThat(values).hasSize(1);
  }

  /**
   * The shape reported in the issue: two projections of {@code timestamp()} in one statement.
   */
  @Test
  void timestampIsPinnedAcrossTwoProjectionsInOneStatement() {
    final ResultSet result = database.command("opencypher", "RETURN timestamp() AS ts1, timestamp() AS ts2");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("ts1")).longValue()).isEqualTo(((Number) row.getProperty("ts2")).longValue());
  }

  /**
   * The pin is per statement, not forever: a statement issued after the clock has demonstrably advanced must see
   * a larger value.
   * <p>
   * The wait is on the JVM clock itself rather than on a fixed sleep, so what is asserted is "a new statement
   * re-reads the clock" and not "the sleep outlasts the clock's resolution" - the second is not true on every
   * platform, and a fixed 10 ms would be a coin flip wherever {@link System#currentTimeMillis()} ticks coarsely.
   * The bound only stops a broken clock from hanging the suite; a slow machine merely makes the assertion more
   * true.
   */
  @Test
  void timestampAdvancesBetweenStatements() throws InterruptedException {
    final long first = readTimestamp();

    final long giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.currentTimeMillis() <= first && System.nanoTime() < giveUpAt)
      Thread.sleep(1);

    final long second = readTimestamp();
    assertThat(second).isGreaterThan(first);
  }

  /**
   * Neo4j's documented relationship between the two: {@code timestamp()} is the epoch-milliseconds of the same
   * clock {@code datetime()} reports. Before the fix these came from two different readings - one from
   * {@code System.currentTimeMillis()} at call time, one from {@code ZonedDateTime.now()} when the statement
   * time was first frozen.
   */
  @Test
  void timestampAgreesWithDatetimeEpochMillis() {
    // datetime() is evaluated in the first WITH and timestamp() only after a scan long enough to cross many
    // millisecond ticks, so a per-call clock read cannot agree with the frozen datetime() by accident.
    final ResultSet result = database.command("opencypher",
        "WITH datetime() AS early UNWIND range(1, 200000) AS i WITH early, count(i) AS scanned "
            + "RETURN timestamp() AS ts, early.epochMillis AS epochMillis");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("ts")).longValue()).isEqualTo(((Number) row.getProperty("epochMillis")).longValue());
  }

  /**
   * The frozen temporal constructors describe one instant, not five successive ones: the calendar day
   * {@code date()} reports is the day {@code localdatetime()} and {@code datetime()} report, and the wall-clock
   * time {@code localdatetime()} reports is the one {@code datetime()} reports down to the second.
   */
  @Test
  void statementClockEntriesShareOneInstant() {
    final ResultSet result = database.command("opencypher", """
        RETURN date().year AS dYear, date().month AS dMonth, date().day AS dDay,
               localdatetime().year AS ldtYear, localdatetime().month AS ldtMonth, localdatetime().day AS ldtDay,
               localdatetime().hour AS ldtHour, localdatetime().minute AS ldtMinute, localdatetime().second AS ldtSecond,
               datetime().year AS dtYear, datetime().month AS dtMonth, datetime().day AS dtDay,
               datetime().hour AS dtHour, datetime().minute AS dtMinute, datetime().second AS dtSecond""");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    assertThat(asLong(row, "ldtYear")).isEqualTo(asLong(row, "dYear"));
    assertThat(asLong(row, "ldtMonth")).isEqualTo(asLong(row, "dMonth"));
    assertThat(asLong(row, "ldtDay")).isEqualTo(asLong(row, "dDay"));

    assertThat(asLong(row, "dtYear")).isEqualTo(asLong(row, "dYear"));
    assertThat(asLong(row, "dtMonth")).isEqualTo(asLong(row, "dMonth"));
    assertThat(asLong(row, "dtDay")).isEqualTo(asLong(row, "dDay"));

    assertThat(asLong(row, "dtHour")).isEqualTo(asLong(row, "ldtHour"));
    assertThat(asLong(row, "dtMinute")).isEqualTo(asLong(row, "ldtMinute"));
    assertThat(asLong(row, "dtSecond")).isEqualTo(asLong(row, "ldtSecond"));
  }

  /**
   * A {@code CALL { }} body runs on a CommandContext of its own, so the pin has to travel into it: without that
   * the reported defect simply moves one level up and the inner and outer {@code timestamp()} land a tick apart.
   * The body scans long enough to cross many ticks, so an un-shared clock cannot agree by accident.
   */
  @Test
  void timestampIsPinnedAcrossACallSubquery() {
    final ResultSet result = database.command("opencypher",
        "CALL { UNWIND range(1, 200000) AS i RETURN max(timestamp()) AS inner } RETURN timestamp() AS outer, inner");
    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(((Number) row.getProperty("outer")).longValue()).isEqualTo(((Number) row.getProperty("inner")).longValue());
  }

  /**
   * Each UNION branch runs on a CommandContext of its own too, and the branches are executed lazily as the
   * consumer pulls, so the second branch starts well after the first one finished.
   */
  @Test
  void timestampIsPinnedAcrossUnionBranches() {
    final ResultSet result = database.command("opencypher",
        "UNWIND range(1, 200000) AS i WITH max(timestamp()) AS t RETURN t UNION ALL RETURN timestamp() AS t");
    final List<Long> values = new ArrayList<>();
    while (result.hasNext())
      values.add(((Number) result.next().getProperty("t")).longValue());
    assertThat(values).hasSize(2);
    assertThat(values.get(1)).isEqualTo(values.get(0));
  }

  private long readTimestamp() {
    final ResultSet result = database.command("opencypher", "RETURN timestamp() AS ts");
    assertThat(result.hasNext()).isTrue();
    return ((Number) result.next().getProperty("ts")).longValue();
  }

  private static long asLong(final Result row, final String property) {
    return ((Number) row.getProperty(property)).longValue();
  }
}
