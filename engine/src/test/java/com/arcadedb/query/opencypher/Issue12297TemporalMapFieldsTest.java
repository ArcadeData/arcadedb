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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12297: the openCypher temporal maps dropped the week, quarter, ordinal fields next to a {@code datetime:} base and in
 * a {@code truncate()} adjustment map, read a base of the wrong kind (or a String, or null) and a missing year as January 1 of
 * the current year, and dropped one field of a contradictory set. Every expected value below is Neo4j 5's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12297TemporalMapFieldsTest extends TestHelper {

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      // fields that were dropped next to a datetime: base
      "datetime({datetime: localdatetime('2015-07-21T21:40:32.142'), week: 2})                                     | 2015-01-06T21:40:32.142Z",
      "localdatetime({datetime: localdatetime('2015-07-21T21:40:32.142'), dayOfWeek: 7})                           | 2015-07-26T21:40:32.142",
      "localdatetime({datetime: localdatetime('2015-07-21T21:40:32.142'), ordinalDay: 33})                         | 2015-02-02T21:40:32.142",
      "localdatetime({datetime: localdatetime('2015-07-21T21:40:32.142'), quarter: 1})                             | 2015-01-21T21:40:32.142",
      "datetime({datetime: datetime('2020-03-07T02:30:15.5+01:00[Europe/Paris]'), quarter: 4, dayOfQuarter: 3})    | 2020-10-03T02:30:15.5+02:00[Europe/Paris]",
      // and in a truncate() adjustment map
      "date.truncate('weekYear', date('2021-01-01'), {week: 2})                                                    | 2020-01-06",
      "date.truncate('year', date('2021-05-15'), {quarter: 3})                                                     | 2021-07-01",
      "date.truncate('quarter', date('2021-05-15'), {dayOfQuarter: 40})                                            | 2021-05-10",
      "date.truncate('year', date('2021-05-15'), {ordinalDay: 40})                                                 | 2021-02-09",
      "datetime.truncate('year', datetime('2020-03-07T02:30:15.5+01:00[Europe/Paris]'), {ordinalDay: 100, hour: 3}) | 2020-04-09T03:00:00+02:00[Europe/Paris]",
      "date.truncate('year', date('2021-05-15'), {week: 3})                                                        | 2020-01-17",
      "date.truncate('year', date('2021-05-15'), {week: 3, dayOfWeek: 2})                                          | 2020-01-14",
      "date.truncate('year', date('2021-05-15'), {quarter: 3, dayOfQuarter: 2})                                    | 2021-07-02",
      "date.truncate('month', date('2021-05-15'), {dayOfWeek: 1})                                                  | 2021-04-26",
      "date.truncate('month', date('2021-05-15'), {day: null})                                                     | 2021-05-01",
      "localdatetime.truncate('year', localdatetime('2021-05-15T10:11'), {quarter: 2, hour: 1})                    | 2021-04-01T01:00:00",
      "localdatetime.truncate('day', localdatetime('2021-05-15T10:11'), {week: 2})                                 | 2021-01-16T00:00:00",
      "localtime.truncate('hour', localtime('10:11'), {hour: null})                                                | 00:00:00",
      // controls that already matched
      "date({date: date('2021-05-15'), week: 2})                                                                   | 2021-01-16",
      "date.truncate('month', date('2021-05-15'), {day: 31})                                                       | 2021-05-31",
      // a base value: fields apply in order, a day past the end of the month is clamped
      "date({date: date('2020-02-29'), year: 2021})                                                                | 2021-02-28",
      "date({date: date('2024-02-29'), year: 2023, month: 3})                                                      | 2023-03-28",
      "date({date: date('2024-01-31'), month: 2})                                                                  | 2024-02-29",
      "date({date: date('2024-05-31'), quarter: 1})                                                                | 2024-02-29",
      "date({date: date('2024-05-31'), year: 2020, week: 53})                                                      | 2021-01-01",
      "date({date: date('2024-05-31'), year: 2020, ordinalDay: 60})                                                | 2020-02-29",
      "date({date: date('2024-02-29'), dayOfWeek: 1})                                                              | 2024-02-26",
      "date({date: localdatetime('2021-05-15T10:00')})                                                             | 2021-05-15",
      "datetime({date: datetime('2021-05-15T10:00Z')})                                                             | 2021-05-15T00:00:00Z",
      // a null field is given, as its smallest value
      "date({year: 2020, month: null})                                                                             | 2020-01-01",
      "date({year: 2020, month: 2, day: null})                                                                     | 2020-02-01",
      "date({year: 2020, week: 2, dayOfWeek: null})                                                                | 2020-01-06",
      "localtime({hour: null, minute: 3})                                                                          | 00:03:00",
      "localtime({hour: 1, minute: 2, second: 3, millisecond: null})                                               | 01:02:03",
      // complete field sets without a base
      "date({year: 2020})                                                                                          | 2020-01-01",
      "date({year: 2020, week: 3})                                                                                 | 2020-01-13",
      "date({year: 2020, quarter: 3})                                                                              | 2020-07-01",
      "localdatetime({year: 2020, ordinalDay: 2, hour: 1})                                                         | 2020-01-02T01:00:00",
      "localdatetime({year: 2020, week: 2, dayOfWeek: 3, hour: 1})                                                 | 2020-01-08T01:00:00",
      "datetime({year: 2020, timezone: '+01:00'})                                                                  | 2020-01-01T00:00:00+01:00",
      "datetime({epochSeconds: 1, timezone: '+01:00'})                                                             | 1970-01-01T01:00:01+01:00",
      "datetime({epochMillis: 1500})                                                                               | 1970-01-01T00:00:01.5Z",
      // with a timezone, the fields apply in the zone of the base, which then moves to the timezone at the same instant
      "datetime({datetime: datetime('2021-05-15T10:00+02:00'), timezone: '+01:00', hour: 3})                      | 2021-05-15T02:00:00+01:00",
      "datetime({datetime: datetime('2021-05-15T00:30+02:00'), timezone: '+01:00', day: 3})                       | 2021-05-02T23:30:00+01:00",
      "datetime({datetime: localdatetime('2021-05-15T10:00'), timezone: '+01:00', hour: 3})                       | 2021-05-15T03:00:00+01:00",
      "datetime({date: date('2021-05-15'), time: time('10:00+02:00'), timezone: '+01:00', hour: 3})               | 2021-05-15T02:00:00+01:00",
      "time({time: time('10:00+02:00'), timezone: '+01:00', hour: 3})                                             | 02:00:00+01:00",
      "time({time: localtime('10:00'), timezone: '+01:00', hour: 3})                                              | 03:00:00+01:00",
      // a local base: its overridden local date-time is read in the zone, so the gap of March 28 does not move March 29
      "datetime({datetime: localdatetime('2021-03-28T02:30'), timezone: 'Europe/Berlin', day: 29})                | 2021-03-29T02:30:00+02:00[Europe/Berlin]",
      "datetime({datetime: localdatetime('2021-03-28T02:30'), timezone: 'Europe/Berlin'})                         | 2021-03-28T03:30:00+02:00[Europe/Berlin]",
      // the Neo4j accessor name of dayOfQuarter
      "date('2021-05-15').quarterDay                                                                               | 45",
      "localdatetime('2021-05-15T10:00').quarterDay                                                                | 45" })
  void resolvesAsNeo4j(final String expression, final String expected) {
    assertThat(cypher(expression)).as(expression).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      // a base of the wrong kind, a String or null
      "datetime({datetime: date('2021-05-15')})                                         | Cannot get the time of",
      "datetime({datetime: date('2021-05-15'), month: 2})                               | Cannot get the time of",
      "date({date: '2020-07-01'})                                                       | Cannot construct date from",
      "datetime({datetime: '2020-07-01T00:00Z'})                                        | Cannot construct date time from",
      "date({date: null})                                                               | Cannot construct date from",
      "date({date: localtime('10:00')})                                                 | Cannot get the date of",
      "date({date: duration('P1D')})                                                    | Cannot construct date from",
      "time({time: date('2021-05-15')})                                                 | Cannot get the time of",
      "localdatetime({datetime: localtime('10:00')})                                    | Cannot get the date of",
      "localtime({time: null})                                                          | Cannot construct time from",
      // a missing year or hour
      "date({month: 3})                                                                 | year must be specified",
      "date({week: 2})                                                                  | year must be specified",
      "localtime({minute: 3})                                                           | hour must be specified",
      "localdatetime({time: localtime('10:00')})                                        | year must be specified",
      // contradictory or incomplete field sets
      "date({year: 2020, quarter: 1, month: 2})                                         | Cannot assign quarter to calendar date",
      "date({year: 2020, quarter: 1, day: 2})                                           | Cannot assign",
      "date({year: 2020, month: 2, ordinalDay: 3})                                      | Cannot assign ordinalDay to calendar date",
      "date({year: 2020, week: 2, day: 3})                                              | Cannot assign day to week date",
      "date({date: date('2024-05-31'), month: 1, week: 2})                              | Cannot assign month to week date",
      "date.truncate('year', date('2021-05-15'), {quarter: 3, month: 2})                | Cannot assign quarter to calendar date",
      "date({year: 2020, dayOfWeek: 3})                                                 | dayOfWeek cannot be specified without week",
      "date({year: 2020, day: 3})                                                       | day cannot be specified without month",
      "date({year: 2020, dayOfQuarter: 3})                                              | dayOfQuarter cannot be specified without quarter",
      "localtime({hour: 1, second: 3})                                                  | second cannot be specified without minute",
      "localtime({hour: 1, millisecond: 3})                                             | subsecond cannot be specified without minute",
      "localtime({hour: 1, minute: 2, millisecond: 3})                                  | subsecond cannot be specified without second",
      "localdatetime({year: 2020, hour: 3})                                             | month must be specified",
      "localdatetime({year: 2020, month: 3, hour: 3, second: 2})                        | day must be specified",
      "localdatetime({year: 2020, week: 2, hour: 1})                                    | dayOfWeek must be specified",
      "localdatetime({date: date('2020-01-02'), minute: 3})                             | hour must be specified",
      "datetime({date: date('2021-05-15'), datetime: datetime('2000-01-01T00:00Z')})   | datetime cannot be selected together with date or time",
      "datetime({epochSeconds: 1, datetime: datetime('2020-01-01T00:00Z')})            | epochSeconds cannot be selected together with datetime",
      "datetime({epochSeconds: 1, date: date('2020-01-01')})                            | epochSeconds cannot be selected together with date or time",
      // keys a constructor does not take, and values that are no integer
      "date({year: 2020, foo: 3})                                                       | No such field: foo",
      "date({year: 2020, hour: 3})                                                      | Not supported: hour",
      "localtime({hour: 1, year: 2020})                                                 | Not supported: year",
      "date.truncate('day', date('2021-05-15'), {hour: 2})                              | Not supported: hour",
      "date({date: date('2021-05-15'), timezone: '+01:00'})                             | Cannot assign time zone",
      "localtime.truncate('hour', localtime('10:11'), {timezone: '+01:00'})             | Cannot assign time zone",
      "date({})                                                                         | At least one temporal unit must be specified",
      "date({year: '2020'})                                                             | year must be an integer value",
      "date({year: 2020, month: 2.5})                                                   | Invalid value for month",
      "datetime({epochSeconds: 1.5})                                                    | Invalid value for epochSeconds",
      "datetime({epochSeconds: '1'})                                                    | Cannot construct date time from",
      "datetime({epochSeconds: null})                                                   | Cannot construct date time from",
      "duration({days: null})                                                           | days must be a number value",
      "duration({days: '1'})                                                            | days must be a number value",
      "duration({days: 1, foo: 1})                                                      | Unknown field: foo",
      // an invalid or null timezone is a client error
      "date({timezone: 'Not/AZone'})                                                    | date()",
      "datetime({timezone: 'Not/AZone'})                                                | datetime()",
      "time.truncate('hour', time('10:11Z'), {timezone: 'nope'})                        | time.truncate() timezone is not a valid",
      "time.truncate('hour', time('10:11Z'), {timezone: null})                          | time.truncate() timezone cannot be null",
      "datetime.truncate('hour', datetime('2021-05-15T10:11Z'), {timezone: null})       | datetime.truncate() timezone cannot be null",
      "datetime({year: 2020, timezone: null})                                           | Cannot convert to ZoneId",
      // ranges
      "date({date: date('2020-05-15'), month: -2147483648})                             | Invalid value for month",
      "date({year: 2021, quarter: 5})                                                   | QuarterOfYear",
      "date({year: 2021, ordinalDay: 366})                                              | 366",
      "date({year: 2015, quarter: 1, dayOfQuarter: 91})                                 | Quarter 1 of 2015 only has 90 days" })
  void isRefusedAsNeo4jRefusesIt(final String expression, final String message) {
    assertThatThrownBy(() -> cypher(expression)).as(expression).hasStackTraceContaining(message);
  }

  @Test
  void aTimezoneAloneIsTheCurrentValueInThatZone() {
    assertThat(cypher("date({timezone: 'Europe/Paris'}) = date.statement('Europe/Paris')")).isEqualTo("true");
    assertThat(cypher("datetime({timezone: 'Europe/Paris'}) = datetime.statement('Europe/Paris')")).isEqualTo("true");
    assertThat(cypher("localtime({timezone: 'Europe/Paris'}) = localtime.statement('Europe/Paris')")).isEqualTo("true");
    assertThat(cypher("time({timezone: '+05:00'}) = time.statement('+05:00')")).isEqualTo("true");
    assertThat(cypher("localdatetime({timezone: 'Europe/Paris'}) = localdatetime.statement('Europe/Paris')")).isEqualTo("true");
  }

  private String cypher(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }
}
