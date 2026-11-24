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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12706: a date or datetime moved by a duration out of the java.time range, and the epochMillis of a datetime past the long range,
 * answered the raw {@code DateTimeException} / {@code ArithmeticException} of java.time, which the HTTP layer answers as a 500. They are the
 * caller's values, an {@link ArithmeticErrorException} like every other overflow of a temporal value.
 */
class CypherTemporalRangeOverflowIssue12706Test extends TestHelper {
  @Test
  void aDateMovedPastTheSupportedYearsIsAnArithmeticError() {
    assertOverflow("RETURN date('2020-01-01') + duration('P1000000000Y') AS v");
    assertOverflow("RETURN duration('P1000000000Y') + date('2020-01-01') AS v");
    assertOverflow("RETURN date('+999999999-12-31') + duration('P1D') AS v");
    assertOverflow("RETURN date('-999999999-01-01') - duration('P1D') AS v");
    assertOverflow("RETURN date('2020-01-01') + duration({months: 9223372036854775807}) AS v");
    assertOverflow("RETURN date('2020-01-01') + duration({days: 999999999999}) AS v");
  }

  @Test
  void aDatetimeMovedPastTheSupportedRangeIsAnArithmeticError() {
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') + duration('P1000000000Y') AS v");
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') - duration({months: 9223372036854775807}) AS v");
    assertOverflow("RETURN datetime('2020-01-01T00:00Z') + duration({seconds: 9223372036854775807}) AS v");
    assertOverflow("RETURN localdatetime('2020-01-01T00:00') + duration({days: 999999999999}) AS v");
  }

  @Test
  void aStoredDateOrDatetimeMovedPastTheSupportedRangeIsAnArithmeticError() {
    // The way the issue reaches a real database: a stored DATE / DATETIME property plus a duration.
    database.getSchema().createVertexType("Issue12706").createProperty("d", Type.DATE);
    database.getSchema().getType("Issue12706").createProperty("dt", Type.DATETIME);
    database.transaction(() -> database.newVertex("Issue12706").set("d", LocalDate.of(2020, 1, 1))
        .set("dt", LocalDateTime.of(2020, 1, 1, 0, 0)).save());

    assertOverflow("MATCH (n:Issue12706) RETURN n.d + duration('P1000000000Y') AS v");
    assertOverflow("MATCH (n:Issue12706) RETURN duration('P1000000000Y') + n.d AS v");
    assertOverflow("MATCH (n:Issue12706) RETURN n.d - duration({days: 999999999999}) AS v");
    assertOverflow("MATCH (n:Issue12706) RETURN n.dt + duration('P1000000000Y') AS v");
    assertOverflow("MATCH (n:Issue12706) RETURN n.dt - duration({months: 9223372036854775807}) AS v");

    assertThat(single("MATCH (n:Issue12706) RETURN n.d + duration('P1M') AS v").toString()).isEqualTo("2020-02-01");
    assertThat(single("MATCH (n:Issue12706) RETURN n.dt + duration('PT1H') AS v").toString()).isEqualTo("2020-01-01T01:00");
  }

  @Test
  void aJavaTimeParameterMovedPastTheSupportedRangeIsAnArithmeticError() {
    assertParameterOverflow("RETURN $d + duration('P1D') AS v", LocalDate.MAX);
    assertParameterOverflow("RETURN duration('P1D') + $d AS v", LocalDate.MAX);
    assertParameterOverflow("RETURN $d - duration('P1D') AS v", LocalDate.MIN);
    assertParameterOverflow("RETURN $d + duration('P1D') AS v", LocalDateTime.MAX);
    assertParameterOverflow("RETURN $d - duration({months: 9223372036854775807}) AS v", LocalDateTime.of(2020, 1, 1, 0, 0));

    assertThat(single("RETURN $d - duration('P1D') AS v", "d", LocalDate.MAX).toString()).isEqualTo("+999999999-12-30");
    assertThat(single("RETURN $d + duration('PT1H') AS v", "d", LocalDateTime.of(2020, 1, 1, 0, 0)).toString())
        .isEqualTo("2020-01-01T01:00");
  }

  @Test
  void theOverflowKeepsTheJavaTimeCause() {
    assertThatThrownBy(() -> single("RETURN date('+999999999-12-31') + duration('P1D') AS v"))
        .isInstanceOf(ArithmeticErrorException.class).hasCauseInstanceOf(DateTimeException.class);
    assertThatThrownBy(() -> single("RETURN datetime('+999999999-01-01T00:00Z').epochMillis AS v"))
        .isInstanceOf(ArithmeticErrorException.class).hasCauseInstanceOf(ArithmeticException.class);
  }

  @Test
  void epochMillisPastTheLongRangeIsAnArithmeticError() {
    assertOverflow("RETURN datetime('+999999999-01-01T00:00Z').epochMillis AS v");
  }

  @Test
  void arithmeticInsideTheRangeIsUnchanged() {
    assertThat(single("RETURN date('2020-01-31') + duration('P1M') AS v").toString()).isEqualTo("2020-02-29");
    assertThat(single("RETURN date('2020-01-01') - duration('P1000000000Y') AS v").toString()).isEqualTo("-999997980-01-01");
    assertThat(single("RETURN datetime('2020-01-01T00:00Z') + duration('PT1.5S') AS v").toString()).isEqualTo("2020-01-01T00:00:01.500Z");
    assertThat(single("RETURN datetime('2020-01-01T00:00Z').epochMillis AS v")).isEqualTo(1577836800000L);
  }

  private void assertOverflow(final String query) {
    assertThatThrownBy(() -> single(query)).as(query).isInstanceOf(ArithmeticErrorException.class);
  }

  private void assertParameterOverflow(final String query, final Object value) {
    assertThatThrownBy(() -> single(query, "d", value)).as(query + " with $d = " + value).isInstanceOf(ArithmeticErrorException.class);
  }

  private Object single(final String query, final Object... params) {
    try (final ResultSet rs = database.command("opencypher", query, params)) {
      return rs.next().getProperty("v");
    }
  }
}
