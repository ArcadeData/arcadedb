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
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Result r = rs.next();
      return r.getProperty("v");
    }
  }
}
