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
 */
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;

/**
 * Comprehensive tests for OpenCypher Temporal functions based on Neo4j Cypher documentation.
 * Tests cover: duration(), duration.between(), duration.inDays(), duration.inMonths(), duration.inSeconds(),
 * date(), date.realtime(), date.statement(), date.transaction(), date.truncate(),
 * datetime(), datetime.fromEpoch(), datetime.fromEpochMillis(), datetime.realtime(), datetime.statement(), datetime.transaction(), datetime.truncate(),
 * localdatetime(), localdatetime.realtime(), localdatetime.statement(), localdatetime.transaction(), localdatetime.truncate(),
 * localtime(), localtime.realtime(), localtime.statement(), localtime.transaction(), localtime.truncate(),
 * time(), time.realtime(), time.statement(), time.transaction(), time.truncate(),
 * format()
 */
class OpenCypherTemporalFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testOpenCypherTemporalFunctions");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== duration() Tests ====================

  @Test
  void durationFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration({days: 14, hours: 16, minutes: 12}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration('P14DT16H12M') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationWithDecimalComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration({months: 0.75}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationWithNegativeComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration({days: -5, hours: -3}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationNull() {
    final ResultSet result = database.command("opencypher", "RETURN duration(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== duration.between() Tests ====================

  @Test
  void durationBetweenDates() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.between(date('1984-10-11'), date('1985-11-25')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationBetweenNegative() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.between(date('1985-11-25'), date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationBetweenDateAndDatetime() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.between(date('1984-10-11'), datetime('1984-10-12T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationBetweenLocalDatetimes() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.between(localdatetime('2015-07-21T21:40:32.142'), localdatetime('2016-07-21T21:45:22.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== duration.inDays() Tests ====================

  @Test
  void durationInDaysBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inDays(date('1984-10-11'), date('1985-11-25')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInDaysNegative() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inDays(date('1985-11-25'), date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInDaysPartialDay() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inDays(date('1984-10-11'), datetime('1984-10-12T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== duration.inMonths() Tests ====================

  @Test
  void durationInMonthsBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inMonths(date('1984-10-11'), date('1985-11-25')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInMonthsNegative() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inMonths(date('1985-11-25'), date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInMonthsPartialMonth() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inMonths(date('1984-10-11'), datetime('1984-10-12T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== duration.inSeconds() Tests ====================

  @Test
  void durationInSecondsBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inSeconds(date('1984-10-11'), date('1984-10-12')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInSecondsNegative() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inSeconds(date('1984-10-12'), date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void durationInSecondsWithTime() {
    final ResultSet result = database.command("opencypher",
        "RETURN duration.inSeconds(date('1984-10-11'), datetime('1984-10-12T01:00:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== date() Tests ====================

  @Test
  void dateNow() {
    final ResultSet result = database.command("opencypher", "RETURN date() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN date({year: 1984, month: 10, day: 11}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN date('1984-10-11') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateWithDefaultComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN date({year: 1984, month: 10}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateWeekBased() {
    final ResultSet result = database.command("opencypher",
        "RETURN date({year: 1984, week: 10, dayOfWeek: 3}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateQuarterBased() {
    final ResultSet result = database.command("opencypher",
        "RETURN date({year: 1984, quarter: 3, dayOfQuarter: 45}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateOrdinal() {
    final ResultSet result = database.command("opencypher",
        "RETURN date({year: 1984, ordinalDay: 202}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateNull() {
    final ResultSet result = database.command("opencypher", "RETURN date(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== date.realtime() Tests ====================

  @Test
  void dateRealtime() {
    final ResultSet result = database.command("opencypher", "RETURN date.realtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateRealtimeWithTimezone() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.realtime('America/Los_Angeles') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== date.statement() Tests ====================

  @Test
  void dateStatement() {
    final ResultSet result = database.command("opencypher", "RETURN date.statement() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateStatementConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.statement() AS d1, date.statement() AS d2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("d1").equals(row.getProperty("d2"))).isTrue();
  }

  // ==================== date.transaction() Tests ====================

  @Test
  void dateTransaction() {
    final ResultSet result = database.command("opencypher", "RETURN date.transaction() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateTransactionConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.transaction() AS d1, date.transaction() AS d2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("d1").equals(row.getProperty("d2"))).isTrue();
  }

  // ==================== date.truncate() Tests ====================

  @Test
  void dateTruncateYear() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.truncate('year', date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateTruncateMonth() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.truncate('month', date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void dateTruncateDay() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.truncate('day', date('1984-10-11')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== datetime() Tests ====================

  @Test
  void datetimeNow() {
    final ResultSet result = database.command("opencypher", "RETURN datetime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime('2015-07-21T21:40:32.142+0100') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeNull() {
    final ResultSet result = database.command("opencypher", "RETURN datetime(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== datetime.fromEpoch() Tests ====================

  @Test
  void datetimeFromEpochBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.fromEpoch(0, 0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeFromEpochWithNanoseconds() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.fromEpoch(1000000000, 123456789) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== datetime.fromEpochMillis() Tests ====================

  @Test
  void datetimeFromEpochMillisBasic() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.fromEpochMillis(0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeFromEpochMillisLargeValue() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.fromEpochMillis(1000000000000) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== datetime.realtime() Tests ====================

  @Test
  void datetimeRealtime() {
    final ResultSet result = database.command("opencypher", "RETURN datetime.realtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeRealtimeWithTimezone() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.realtime('America/Los_Angeles') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== datetime.statement() Tests ====================

  @Test
  void datetimeStatement() {
    final ResultSet result = database.command("opencypher", "RETURN datetime.statement() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeStatementConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.statement() AS dt1, datetime.statement() AS dt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("dt1").equals(row.getProperty("dt2"))).isTrue();
  }

  // ==================== datetime.transaction() Tests ====================

  @Test
  void datetimeTransaction() {
    final ResultSet result = database.command("opencypher", "RETURN datetime.transaction() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeTransactionConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.transaction() AS dt1, datetime.transaction() AS dt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("dt1").equals(row.getProperty("dt2"))).isTrue();
  }

  // ==================== datetime.truncate() Tests ====================

  @Test
  void datetimeTruncateYear() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.truncate('year', datetime('2015-07-21T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeTruncateDay() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.truncate('day', datetime('2015-07-21T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void datetimeTruncateHour() {
    final ResultSet result = database.command("opencypher",
        "RETURN datetime.truncate('hour', datetime('2015-07-21T21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== localdatetime() Tests ====================

  @Test
  void localdatetimeNow() {
    final ResultSet result = database.command("opencypher", "RETURN localdatetime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 31, second: 14}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime('2015-07-21T21:40:32.142') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeNull() {
    final ResultSet result = database.command("opencypher", "RETURN localdatetime(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== localdatetime.realtime() Tests ====================

  @Test
  void localdatetimeRealtime() {
    final ResultSet result = database.command("opencypher", "RETURN localdatetime.realtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeRealtimeWithTimezone() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.realtime('America/Los_Angeles') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== localdatetime.statement() Tests ====================

  @Test
  void localdatetimeStatement() {
    final ResultSet result = database.command("opencypher", "RETURN localdatetime.statement() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeStatementConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.statement() AS ldt1, localdatetime.statement() AS ldt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("ldt1").equals(row.getProperty("ldt2"))).isTrue();
  }

  // ==================== localdatetime.transaction() Tests ====================

  @Test
  void localdatetimeTransaction() {
    final ResultSet result = database.command("opencypher", "RETURN localdatetime.transaction() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeTransactionConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.transaction() AS ldt1, localdatetime.transaction() AS ldt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("ldt1").equals(row.getProperty("ldt2"))).isTrue();
  }

  // ==================== localdatetime.truncate() Tests ====================

  @Test
  void localdatetimeTruncateYear() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.truncate('year', localdatetime('2015-07-21T21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeTruncateDay() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.truncate('day', localdatetime('2015-07-21T21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localdatetimeTruncateMinute() {
    final ResultSet result = database.command("opencypher",
        "RETURN localdatetime.truncate('minute', localdatetime('2015-07-21T21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== localtime() Tests ====================

  @Test
  void localtimeNow() {
    final ResultSet result = database.command("opencypher", "RETURN localtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime({hour: 12, minute: 31, second: 14}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime('21:40:32.142') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeNull() {
    final ResultSet result = database.command("opencypher", "RETURN localtime(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== localtime.realtime() Tests ====================

  @Test
  void localtimeRealtime() {
    final ResultSet result = database.command("opencypher", "RETURN localtime.realtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeRealtimeWithTimezone() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.realtime('America/Los_Angeles') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== localtime.statement() Tests ====================

  @Test
  void localtimeStatement() {
    final ResultSet result = database.command("opencypher", "RETURN localtime.statement() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeStatementConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.statement() AS lt1, localtime.statement() AS lt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("lt1").equals(row.getProperty("lt2"))).isTrue();
  }

  // ==================== localtime.transaction() Tests ====================

  @Test
  void localtimeTransaction() {
    final ResultSet result = database.command("opencypher", "RETURN localtime.transaction() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeTransactionConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.transaction() AS lt1, localtime.transaction() AS lt2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("lt1").equals(row.getProperty("lt2"))).isTrue();
  }

  // ==================== localtime.truncate() Tests ====================

  @Test
  void localtimeTruncateHour() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.truncate('hour', localtime('21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeTruncateMinute() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.truncate('minute', localtime('21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void localtimeTruncateSecond() {
    final ResultSet result = database.command("opencypher",
        "RETURN localtime.truncate('second', localtime('21:40:32.142')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== time() Tests ====================

  @Test
  void timeNow() {
    final ResultSet result = database.command("opencypher", "RETURN time() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeFromComponents() {
    final ResultSet result = database.command("opencypher",
        "RETURN time({hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeFromString() {
    final ResultSet result = database.command("opencypher",
        "RETURN time('21:40:32.142+0100') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeNull() {
    final ResultSet result = database.command("opencypher", "RETURN time(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== time.realtime() Tests ====================

  @Test
  void timeRealtime() {
    final ResultSet result = database.command("opencypher", "RETURN time.realtime() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeRealtimeWithTimezone() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.realtime('America/Los_Angeles') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== time.statement() Tests ====================

  @Test
  void timeStatement() {
    final ResultSet result = database.command("opencypher", "RETURN time.statement() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeStatementConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.statement() AS t1, time.statement() AS t2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("t1").equals(row.getProperty("t2"))).isTrue();
  }

  // ==================== time.transaction() Tests ====================

  @Test
  void timeTransaction() {
    final ResultSet result = database.command("opencypher", "RETURN time.transaction() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeTransactionConsistency() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.transaction() AS t1, time.transaction() AS t2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("t1").equals(row.getProperty("t2"))).isTrue();
  }

  // ==================== time.truncate() Tests ====================

  @Test
  void timeTruncateHour() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.truncate('hour', time('21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeTruncateMinute() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.truncate('minute', time('21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  @Test
  void timeTruncateSecond() {
    final ResultSet result = database.command("opencypher",
        "RETURN time.truncate('second', time('21:40:32.142+0100')) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") != null).isTrue();
  }

  // ==================== format() Tests ====================

  @Test
  void formatDatetime() {
    final ResultSet result = database.command("opencypher",
        "WITH datetime('1986-11-18T06:04:45.123456789+01:00') AS dt RETURN format(dt, 'MM/dd/yyyy') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("11/18/1986");
  }

  @Test
  void formatDatetimeDefault() {
    final ResultSet result = database.command("opencypher",
        "WITH datetime('1986-11-18T06:04:45.123456789+01:00') AS dt RETURN format(dt) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String formatted = (String) result.next().getProperty("result");
    assertThat(formatted).isNotNull();
    assertThat(formatted).isNotEmpty();
  }

  @Test
  void formatDatetimeComplexPattern() {
    final ResultSet result = database.command("opencypher",
        "WITH datetime('1986-11-18T06:04:45.123456789+01:00') AS dt RETURN format(dt, 'EEEE, MMMM d, yyyy') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String formatted = (String) result.next().getProperty("result");
    assertThat(formatted).isNotNull();
    assertThat(formatted).contains("1986");
  }

  @Test
  void formatDate() {
    final ResultSet result = database.command("opencypher",
        "WITH date('1986-11-18') AS d RETURN format(d, 'yyyy-MM-dd') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("1986-11-18");
  }

  @Test
  void formatLocaltime() {
    final ResultSet result = database.command("opencypher",
        "WITH localtime('12:30:45') AS t RETURN format(t, 'HH:mm:ss') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((String) result.next().getProperty("result")).isEqualTo("12:30:45");
  }

  @Test
  void formatDuration() {
    final ResultSet result = database.command("opencypher",
        "WITH duration('P1Y2M3DT4H5M6S') AS dur RETURN format(dur) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String formatted = (String) result.next().getProperty("result");
    assertThat(formatted).isNotNull();
    assertThat(formatted).isNotEmpty();
  }

  @Test
  void formatNull() {
    final ResultSet result = database.command("opencypher", "RETURN format(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void temporalTypeConversion() {
    final ResultSet result = database.command("opencypher",
        "WITH datetime('2015-07-21T21:40:32.142+0100') AS dt " +
            "RETURN date(dt) AS dateOnly, localtime(dt) AS timeOnly, localdatetime(dt) AS localDt");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("dateOnly") != null).isTrue();
    Assertions.assertThat(row.getProperty("timeOnly") != null).isTrue();
    Assertions.assertThat(row.getProperty("localDt") != null).isTrue();
  }

  @Test
  void durationArithmetic() {
    final ResultSet result = database.command("opencypher",
        "WITH duration({days: 10}) AS dur1, duration({hours: 24}) AS dur2 " +
            "RETURN dur1 AS d1, dur2 AS d2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("d1") != null).isTrue();
    Assertions.assertThat(row.getProperty("d2") != null).isTrue();
  }

  @Test
  void clockConsistencyComparison() {
    final ResultSet result = database.command("opencypher",
        "RETURN date.statement() AS s1, date.statement() AS s2, " +
            "date.transaction() AS t1, date.transaction() AS t2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    Assertions.assertThat(row.getProperty("s1").equals(row.getProperty("s2"))).isTrue();
    Assertions.assertThat(row.getProperty("t1").equals(row.getProperty("t2"))).isTrue();
  }

  @Test
  void truncateAndFormat() {
    final ResultSet result = database.command("opencypher",
        "WITH datetime('2015-07-21T21:40:32.142+0100') AS dt " +
            "RETURN format(datetime.truncate('day', dt), 'yyyy-MM-dd') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final String formatted = (String) result.next().getProperty("result");
    assertThat(formatted).isEqualTo("2015-07-21");
  }
}
