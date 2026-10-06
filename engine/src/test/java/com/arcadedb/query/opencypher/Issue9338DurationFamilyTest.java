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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9338: the Cypher DURATION type did not survive storage (short ISO texts were never
 * restored, nanoseconds drifted past ~97 days), its arithmetic wrapped or saturated silently, and temporal-by-duration
 * arithmetic accepted operators other than + and -.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9338DurationFamilyTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9338");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private Object scalar(final String query) {
    try (final ResultSet rs = database.query("cypher", query)) {
      return rs.next().getProperty("r");
    }
  }

  private void expectError(final String query) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("cypher", query)) {
        rs.next();
      }
    }).as(query).isInstanceOf(RuntimeException.class);
  }

  @Test
  void shortIsoDurationIsRestoredFromStorage() {
    database.transaction(() -> database.command("cypher", "CREATE (:T {d: duration({days: 1})})"));
    assertThat(scalar("MATCH (n:T) RETURN n.d = duration({days: 1}) AS r")).isEqualTo(true);
    assertThat(((Number) scalar("MATCH (n:T) RETURN n.d.days AS r")).longValue()).isEqualTo(1L);
  }

  @Test
  void storedDurationsOrderAndFilterAsDurations() {
    database.transaction(() -> {
      database.command("cypher", "CREATE (:S {n: '2d', d: duration({days: 2})})");
      database.command("cypher", "CREATE (:S {n: '10d', d: duration({days: 10})})");
      database.command("cypher", "CREATE (:S {n: '3d', d: duration({days: 3})})");
    });
    final List<Object> order = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", "MATCH (s:S) RETURN s.n AS r ORDER BY s.d")) {
      while (rs.hasNext())
        order.add(rs.next().getProperty("r"));
    }
    assertThat(order).containsExactly("2d", "3d", "10d");
    assertThat(((Number) scalar("MATCH (s:S) WHERE s.d > duration({days: 2}) RETURN count(s) AS r")).longValue()).isEqualTo(2L);
  }

  @Test
  void storageRoundTripKeepsEveryNanosecond() {
    for (final String[] c : new String[][] { { "9007199", "999999999" }, { "100000000", "123456789" },
        { "1000000000", "123456789" } }) {
      final String d = "duration({seconds: " + c[0] + ", nanoseconds: " + c[1] + "})";
      assertThat(scalar("RETURN duration(toString(" + d + ")) = " + d + " AS r")).as(d).isEqualTo(true);
    }
    assertThat(scalar("RETURN duration(toString(duration({seconds: 1234567890123456789}))) = duration({seconds: 1234567890123456789}) AS r"))
        .isEqualTo(true);
    assertThat(scalar("RETURN toString(duration('PT1000000000.123456789S')) AS r")).isEqualTo("PT277777H46M40.123456789S");
  }

  @Test
  void multiplyAndDivideByOneAreTheIdentity() {
    assertThat(scalar("RETURN (duration({seconds: 9007200, nanoseconds: 1}) * 1) = duration({seconds: 9007200, nanoseconds: 1}) AS r"))
        .isEqualTo(true);
    assertThat(scalar("RETURN toString(duration({seconds: 10000000, nanoseconds: 123456789}) * 1) AS r"))
        .isEqualTo("PT2777H46M40.123456789S");
    assertThat(scalar("RETURN toString(duration({seconds: 10000000, nanoseconds: 123456789}) / 1) AS r"))
        .isEqualTo("PT2777H46M40.123456789S");
    assertThat(scalar("RETURN toString(duration({days: 3}) / 3) AS r")).isEqualTo("P1D");
  }

  @Test
  void durationArithmeticRaisesOnOverflow() {
    expectError("RETURN duration({months: 9223372036854775807}) + duration({months: 1})");
    expectError("RETURN duration({seconds: 5000000000000000000}) + duration({seconds: 5000000000000000000})");
    expectError("RETURN duration({days: -9223372036854775808}) - duration({days: 1})");
    expectError("RETURN duration({hours: 1}) * 10000000000000000");
    // 3.6e15 seconds is representable, so the old nanosecond saturation is gone and the exact answer is returned
    assertThat(scalar("RETURN toString(duration({hours: 1}) * 1000000000000) AS r")).isEqualTo("PT1000000000000H");
    expectError("RETURN duration({months: 9223372036854775807}) * 2");
    expectError("RETURN duration({seconds: 9223372037}).nanoseconds");
    expectError("RETURN duration({seconds: 9223372036854776}).milliseconds");
    assertThatThrownBy(() -> new CypherDuration(Long.MAX_VALUE, 0, 0, 0).multiply(2)).isInstanceOf(ArithmeticErrorException.class);
  }

  @Test
  void localTimePlusLargeDurationWrapsTheClockCorrectly() {
    assertThat(scalar("RETURN toString(localtime('12:00') + duration({seconds: 9223372036})) AS r")).isEqualTo("11:47:16");
    // one second more than the case above: the clock must move forward by one second, never backwards
    assertThat(scalar("RETURN toString(localtime('12:00') + duration({seconds: 9223372037})) AS r")).isEqualTo("11:47:17");
  }

  @Test
  void wholeUnitFieldsStayExactWithManyKeys() {
    assertThat(((Number) scalar("RETURN duration({days: 1, seconds: 9007199254740993}).seconds AS r")).longValue())
        .isEqualTo(9007199254740993L);
    assertThat(((Number) scalar("RETURN duration({months: 1, days: 9007199254740993}).days AS r")).longValue())
        .isEqualTo(9007199254740993L);
  }

  @Test
  void temporalWithUnsupportedOperatorIsATypeError() {
    for (final String op : new String[] { "*", "/", "%" })
      for (final String t : new String[] { "date('2024-01-01')", "datetime('2024-01-01T00:00Z')",
          "localdatetime('2024-01-01T00:00')", "localtime('12:00')", "time('12:00Z')" })
        expectError("RETURN " + t + " " + op + " duration({days: 1})");
  }

  @Test
  void durationOnTheLeftOfPlusIsAccepted() {
    assertThat(scalar("RETURN toString(duration({days: 1}) + date('2024-01-01')) AS r")).isEqualTo("2024-01-02");
    assertThat(scalar("RETURN toString(duration({days: 1}) + datetime('2024-01-01T00:00Z')) AS r")).isEqualTo("2024-01-02T00:00Z");
    assertThat(scalar("RETURN toString(duration({days: 1}) + localdatetime('2024-01-01T00:00')) AS r")).isEqualTo("2024-01-02T00:00");
    assertThat(scalar("RETURN toString(duration({hours: 1}) + localtime('12:00')) AS r")).isEqualTo("13:00");
    expectError("RETURN duration({days: 1}) - date('2024-01-01')");
  }

  @Test
  void divisionByAnIntegerKeepsExactnessAndTruncatesTheRemainder() {
    assertThat(scalar("RETURN toString(duration({days: 4, hours: 2}) / 2) AS r")).isEqualTo("P2DT1H");
    // an inexact quotient truncates (as the double based cascade always did): 1 day / 3 is one nanosecond short of 8h
    assertThat(scalar("RETURN toString(duration({days: 1}) / 3) AS r")).isEqualTo("PT7H59M59.999999999S");
    assertThat(scalar("RETURN toString(duration({seconds: 1}) / 3) AS r")).isEqualTo("PT0.333333333S");
  }

  @Test
  void parsingRoundsTheDigitsBeyondTheNanosecondHalfUp() {
    assertThat(scalar("RETURN toString(duration('PT0.0000000015S')) AS r")).isEqualTo("PT0.000000002S");
    assertThat(scalar("RETURN toString(duration('PT0.0000000014S')) AS r")).isEqualTo("PT0.000000001S");
  }

  @Test
  void dividingTheMostNegativeDurationByMinusOneRaisesInsteadOfWrapping() {
    expectError("RETURN duration({days: -9223372036854775808}) / -1");
    assertThat(scalar("RETURN toString(duration({days: 4}) / -1) AS r")).isEqualTo("P-4D");
  }
}
