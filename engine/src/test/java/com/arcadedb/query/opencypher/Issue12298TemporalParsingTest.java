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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12298: the openCypher temporal text parsing replaced an offset that contradicts the zone the text names (so the value
 * named another instant), refused ISO 8601 forms Neo4j accepts and accepted forms Neo4j refuses. Every expected value below is
 * Neo4j 5's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12298TemporalParsingTest extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { //
      "datetime('2015-07-21T21:40:32.142-04:00[Europe/London]')", //
      "datetime('2020-03-08T03:30-05:00[America/New_York]')", //
      "datetime('2020-03-08T02:30-05:00[America/New_York]')", //
      "datetime('2020-03-08T02:30-04:00[America/New_York]')", //
      "datetime('2020-06-01T00:00+00:00[Etc/GMT-14]')", //
      "datetime('2015-07-21T21:40Z[Europe/London]')" })
  void anOffsetThatContradictsTheZoneIsRefused(final String expression) {
    assertThatThrownBy(() -> cypher(expression)).as(expression).hasStackTraceContaining("Timezone and offset do not match");
  }

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      // an offset that agrees with the zone is kept, the second 01:30 of a fall-back day included
      "datetime('2020-11-01T01:30-05:00[America/New_York]')    | 2020-11-01T01:30:00-05:00[America/New_York]",
      "datetime('2020-11-01T01:30-04:00[America/New_York]')    | 2020-11-01T01:30:00-04:00[America/New_York]",
      "datetime('2015-07-21T21:40+01:00[Europe/London]')       | 2015-07-21T21:40:00+01:00[Europe/London]",
      "datetime('2015-07-21T21:40+0100[Europe/London]')        | 2015-07-21T21:40:00+01:00[Europe/London]",
      "datetime('2015-07-21T21:40+01[Europe/London]')          | 2015-07-21T21:40:00+01:00[Europe/London]",
      // a local time the zone skips moves forward by the gap
      "datetime('2020-03-08T02:30[America/New_York]')          | 2020-03-08T03:30:00-04:00[America/New_York]",
      // ISO forms Neo4j accepts
      "date('2015-Q3')                                         | 2015-07-01",
      "date('2015-Q3-05')                                      | 2015-07-05",
      "date('2015Q305')                                        | 2015-07-05",
      "date('2015-Q4-92')                                      | 2015-12-31",
      "date('+2015-07-21')                                     | 2015-07-21",
      "date('-2015-07-21')                                     | -2015-07-21",
      "date('+12015-07-21')                                    | +12015-07-21",
      "date('2015-7-21')                                       | 2015-07-21",
      "date('2015-07-1')                                       | 2015-07-01",
      "date('2015-1-1')                                        | 2015-01-01",
      "date('2015-W1')                                         | 2014-12-29",
      "date('2015-W1-2')                                       | 2014-12-30",
      "date('2015W1')                                          | 2014-12-29",
      "date('2015W302')                                        | 2015-07-21",
      "date('2015-202')                                        | 2015-07-21",
      "date('+2015-W30')                                       | 2015-07-20",
      "date('+2015-Q3')                                        | 2015-07-01",
      "date('+2015')                                           | 2015-01-01",
      "localtime('21:40:32,142')                               | 21:40:32.142",
      "localtime('214032,5')                                   | 21:40:32.5",
      "localtime('21:4')                                       | 21:04:00",
      "localtime('2:40')                                       | 02:40:00",
      "localtime('2')                                          | 02:00:00",
      "localtime('21:40:3')                                    | 21:40:03",
      "localtime('T21:40')                                     | 21:40:00",
      "localtime('T214032')                                    | 21:40:32",
      "time('21:40-01')                                        | 21:40:00-01:00",
      "time('21:40z')                                          | 21:40:00Z",
      "time('21:40:32,142+01:00')                              | 21:40:32.142+01:00",
      "time('T21:40Z')                                         | 21:40:00Z",
      "datetime('2015-07-21t21:40Z')                           | 2015-07-21T21:40:00Z",
      "datetime('2015-07-21T21:40[europe/london]')             | 2015-07-21T21:40:00+01:00[Europe/London]",
      "datetime('2015-07-21T21:40[EUROPE/LONDON]')             | 2015-07-21T21:40:00+01:00[Europe/London]",
      "datetime('2015-07-21T21:40[Z]')                         | 2015-07-21T21:40:00Z",
      "datetime('2015-Q3-05T21:40Z')                           | 2015-07-05T21:40:00Z",
      "datetime('+2015-07-21T21:40Z')                          | 2015-07-21T21:40:00Z",
      "datetime('2015-7-21T2:4Z')                              | 2015-07-21T02:04:00Z",
      "datetime('2015-07-21T21:40:32,142Z')                    | 2015-07-21T21:40:32.142Z",
      "datetime('2015-07-21T21Z')                              | 2015-07-21T21:00:00Z",
      "datetime('2015-07-21')                                  | 2015-07-21T00:00:00Z",
      "localdatetime('2015-Q3-05T21:40')                       | 2015-07-05T21:40:00",
      "localdatetime('2015-07-21t21:40')                       | 2015-07-21T21:40:00",
      "localdatetime('2015-7-21T2:40')                         | 2015-07-21T02:40:00",
      "localdatetime('2015-07-21T21:40:32,5')                  | 2015-07-21T21:40:32.5",
      // forms that already worked
      "date('2015-07')                                         | 2015-07-01",
      "date('2015')                                            | 2015-01-01",
      "localtime('2140')                                       | 21:40:00",
      "time('21:40-0130')                                      | 21:40:00-01:30",
      "localtime('21:40:32.123456789')                         | 21:40:32.123456789" })
  void parsesAsNeo4j(final String expression, final String expected) {
    assertThat(cypher(expression)).as(expression).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = { //
      "localtime('21:40:32.')", "time('21:40:32.')", "datetime('2015-07-21T21:40:32.')", "localdatetime('2015-07-21T21:40:32.')",
      "time('21:40-01:30:15')", "datetime('2015-07-21T21:40-01:30:15')", //
      "datetime('2015-07-21T21:40[+01:00]')", "datetime('2015-07-21T21:40[UTC+01:00]')", //
      "date('2015-q3')", "date('2015-w30')", "date('2015-7')", "date('12015-07-21')", "date('2015-Q1-91')", "date('2015-Q5')",
      "localtime('t21:40')", "localtime('21:40:32.1234567891')", "localtime('21:40Z')", "localtime('21:40+01:00')", //
      "datetime('2015-07-21TT21:40Z')", "datetime('2015-07-21Z')", "datetime('2015-07-21[Europe/London]')", //
      "localdatetime('2015-07-21T21:40Z')", "localdatetime('2015-07-21T21:40[Europe/London]')", //
      "date('2015-07-21T21:40')", "date('2015-07-21Z')" })
  void aFormNeo4jRefusesIsRefused(final String expression) {
    assertThatThrownBy(() -> cypher(expression)).as(expression).isNotNull();
  }

  @Test
  void aZoneNameOnATimeIsRefused() {
    assertThatThrownBy(() -> cypher("time('21:40[Europe/London]')")).hasStackTraceContaining("named time zone");
    assertThatThrownBy(() -> cypher("time('21:40+01:00[Europe/London]')")).hasStackTraceContaining("named time zone");
  }

  @Test
  void theInstantTheTextStatesIsKept() {
    // The two used to be 5 hours apart: the -04:00 was replaced by London's +01:00
    assertThat(cypher("datetime('2015-07-21T21:40+01:00[Europe/London]').epochSeconds - datetime('2015-07-21T21:40+01:00').epochSeconds"))
        .isEqualTo("0");
  }

  @Test
  void aZonedDateTimePropertyRefusesAnOffsetThatContradictsTheZone() {
    database.command("sql", "CREATE DOCUMENT TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.zd " + Type.ZONED_DATETIME.name());
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO Z SET zd = '2015-07-21T21:40:32.142-04:00[Europe/London]'"))).hasStackTraceContaining(
        "Timezone and offset do not match");
    // A form only the general parser reads (an offset with seconds, a space before the time) is checked as well
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO Z SET zd = '2015-07-21T21:40:32-04:00:00[Europe/London]'"))).hasStackTraceContaining(
        "Timezone and offset do not match");
    assertThat(database.countType("Z", false)).isZero();

    database.transaction(() -> database.command("sql", "INSERT INTO Z SET zd = '2015-07-21T21:40:32.142+01:00[Europe/London]'"));
    try (final ResultSet rs = database.query("sql", "SELECT zd FROM Z")) {
      final ZonedDateTime stored = rs.next().getProperty("zd");
      assertThat(stored.toString()).isEqualTo("2015-07-21T21:40:32.142+01:00[Europe/London]");
    }
  }

  private String cypher(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN toString(" + expression + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }
}
