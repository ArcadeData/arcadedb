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
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8300: {@code CypherDateTime.compareTo} compared instants only, while {@code equals}/{@code hashCode} compare
 * the zone too. {@code =} (built on compareTo) called two datetimes at one instant in different zones equal, while
 * DISTINCT and GROUP BY (built on equals/hashCode) kept them apart. compareTo now breaks the instant tie by offset and
 * then zone name, the openCypher order, so all of them agree the two are distinct values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8300DateTimeIdentityTest extends TestHelper {
  private static final String X = "datetime('2026-01-01T12:00:00+01:00')";
  private static final String Y = "datetime('2026-01-01T11:00:00Z')";

  @Test
  void compareToIsConsistentWithEquals() {
    final CypherDateTime x = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
    final CypherDateTime y = CypherDateTime.parse("2026-01-01T11:00:00Z");
    assertThat(x.equals(y)).isFalse();
    assertThat(x.compareTo(y)).isNotZero();
    assertThat(Integer.signum(x.compareTo(y))).isEqualTo(-Integer.signum(y.compareTo(x)));
    // West to east: at one instant, Z (offset 0) sorts before +01:00
    assertThat(y.compareTo(x)).isNegative();
    assertThat(x.compareTo(CypherDateTime.parse("2026-01-01T12:00:00+01:00"))).isZero();

    // Time already broke its offset tie: pinned so the two stay consistent
    final CypherTime t1 = CypherTime.parse("12:00:00+01:00");
    final CypherTime t2 = CypherTime.parse("11:00:00Z");
    assertThat(t1.compareTo(t2) == 0).isEqualTo(t1.equals(t2));
  }

  @Test
  void equalityAgreesWithDistinctAndGrouping() {
    assertThat(single("RETURN " + X + " = " + Y + " AS v")).isEqualTo(false);
    assertThat(single("RETURN " + X + " <> " + Y + " AS v")).isEqualTo(true);
    assertThat(single("RETURN " + X + " IN [" + Y + "] AS v")).isEqualTo(false);
    assertThat(((Number) single("UNWIND [" + X + ", " + Y + "] AS d RETURN count(DISTINCT d) AS v")).longValue()).isEqualTo(2L);

    try (final ResultSet rs = database.query("opencypher", "UNWIND [" + X + ", " + Y + "] AS d RETURN d AS k, count(*) AS c")) {
      assertThat(rs.stream().map(r -> ((Number) r.getProperty("c")).longValue()).toList()).containsExactly(1L, 1L);
    }

    // Same instant AND same zone: one value everywhere
    assertThat(single("RETURN " + X + " = " + X + " AS v")).isEqualTo(true);
    assertThat(((Number) single("UNWIND [" + X + ", " + X + "] AS d RETURN count(DISTINCT d) AS v")).longValue()).isEqualTo(1L);
  }

  @Test
  void orderingAndRangesStillFollowTheInstant() {
    assertThat(single("RETURN datetime('2026-01-01T12:30:00+01:00') > " + Y + " AS v")).isEqualTo(true);
    assertThat(single("RETURN datetime('2026-01-01T11:30:00+01:00') < " + Y + " AS v")).isEqualTo(true);
    assertThat(single("RETURN " + X + " >= " + Y + " AND " + X + " <= datetime('2026-01-01T11:00:01Z') AS v")).isEqualTo(true);
  }

  /**
   * compareTo is a total order across DateTime and LocalDateTime too (a LocalDateTime is read as UTC and sorts first at
   * the same instant), so ORDER BY over a mixed list is consistent, while = keeps comparing the two types by instant.
   */
  @Test
  void localAndZonedDateTimesAtOneInstant() {
    final CypherDateTime x = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
    final CypherDateTime y = CypherDateTime.parse("2026-01-01T11:00:00Z");
    final CypherLocalDateTime local = new CypherLocalDateTime(LocalDateTime.of(2026, 1, 1, 11, 0));
    assertThat(local.compareTo(x)).isNegative();
    assertThat(local.compareTo(y)).isNegative();
    assertThat(x.compareTo(local)).isPositive();
    assertThat(y.compareTo(local)).isPositive();

    assertThat(single("RETURN localdatetime('2026-01-01T11:00:00') = " + Y + " AS v")).isEqualTo(true);
    assertThat(single("RETURN localdatetime('2026-01-01T11:00:00') = " + X + " AS v")).isEqualTo(true);

    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [" + X + ", localdatetime('2026-01-01T11:00:00'), " + Y + ", localdatetime('2026-01-01T10:59:00')] AS d "
            + "RETURN toString(d) AS s ORDER BY d")) {
      assertThat(rs.stream().map(r -> (String) r.getProperty("s")).toList())
          .containsExactly("2026-01-01T10:59", "2026-01-01T11:00", "2026-01-01T11:00Z", "2026-01-01T12:00+01:00");
    }
  }

  /**
   * A java.util.Date or Instant parameter has no zone: it still equals a stored datetime at its instant, whatever that
   * datetime's zone is.
   */
  @Test
  void zonelessParametersMatchAnyZoneAtTheirInstant() {
    database.getSchema().createVertexType("Event8300");
    database.transaction(() -> database.command("opencypher", "CREATE (:Event8300 {ts: " + X + "})"));

    final Instant instant = ZonedDateTime.parse("2026-01-01T11:00:00Z").toInstant();
    for (final Object param : List.of(Date.from(instant), instant))
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:Event8300) WHERE e.ts = $p RETURN count(e) AS c",
          Map.of("p", param))) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).as(param.getClass().getSimpleName()).isEqualTo(1L);
      }
  }

  /**
   * The comparison memoizes a coerced parameter by identity: a java.util.Date moved with setTime() between two
   * evaluations must not be answered with the value it had before.
   */
  @Test
  void mutatedDateIsNotAnsweredFromTheMemo() {
    final ComparisonExpression equals = ComparisonExpression.valueComparator(ComparisonExpression.Operator.EQUALS);
    final CypherDateTime stored = CypherDateTime.parse("2026-01-01T12:00:00+01:00");
    final Date param = Date.from(ZonedDateTime.parse("2026-01-01T11:00:00Z").toInstant());

    assertThat(equals.evaluateWithValues(param, stored)).isEqualTo(true);
    param.setTime(param.getTime() + 60_000L);
    assertThat(equals.evaluateWithValues(param, stored)).isEqualTo(false);
    param.setTime(param.getTime() - 60_000L);
    assertThat(equals.evaluateWithValues(stored, param)).isEqualTo(true);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Result r = rs.next();
      return r.getProperty("v");
    }
  }
}
