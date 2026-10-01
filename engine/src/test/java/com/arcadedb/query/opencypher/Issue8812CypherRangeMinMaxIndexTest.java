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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8812: {@code MATCH (v:V) WHERE v.a > x RETURN min(v.a)} (and {@code max} with {@code <}) read
 * every vertex of the range; the first row of the index range is the answer. Every answer is compared with the same
 * query written with {@code v.a + 0}, which no index serves.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8812CypherRangeMinMaxIndexTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newVertex("V").set("a", (long) i * 2).save();
      // no value: a range must never return it
      database.newVertex("V").set("b", 1).save();
    });
  }

  @Test
  void minOverLowerBoundAndMaxOverUpperBound() {
    assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
        1002L);
    assertAnswer("MATCH (v:V) WHERE v.a >= 1002 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 >= 1002 RETURN min(v.a + 0) AS c", Map.of(),
        1002L);
    assertAnswer("MATCH (v:V) WHERE v.a < 1001 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 1001 RETURN max(v.a + 0) AS c", Map.of(),
        1000L);
    assertAnswer("MATCH (v:V) WHERE v.a <= 1000 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 <= 1000 RETURN max(v.a + 0) AS c", Map.of(),
        1000L);
    assertThat(profile("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", Map.of())).contains("MIN FROM INDEX");
    assertThat(profile("MATCH (v:V) WHERE v.a < 1001 RETURN max(v.a) AS c", Map.of())).contains("MAX FROM INDEX");
  }

  @Test
  void bothBoundsAndTheOppositeEnd() {
    assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a < 200 RETURN min(v.a) AS c",
        "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 < 200 RETURN min(v.a + 0) AS c", Map.of(), 102L);
    assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a < 200 RETURN max(v.a) AS c",
        "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 < 200 RETURN max(v.a + 0) AS c", Map.of(), 198L);
    assertAnswer("MATCH (v:V) WHERE v.a < 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 1001 RETURN min(v.a + 0) AS c", Map.of(), 0L);
    assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN max(v.a + 0) AS c", Map.of(),
        3998L);
  }

  @Test
  void comparisonWrittenTheOtherWayRoundAndParameters() {
    assertAnswer("MATCH (v:V) WHERE 1001 < v.a RETURN min(v.a) AS c", "MATCH (v:V) WHERE 1001 < v.a + 0 RETURN min(v.a + 0) AS c", Map.of(),
        1002L);
    for (final long lo : new long[] { 10, 1001, 3000 })
      assertAnswer("MATCH (v:V) WHERE v.a > $lo RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > $lo RETURN min(v.a + 0) AS c",
          Map.of("lo", lo), null);
  }

  @Test
  void boundsOfAnotherTypeAndRepeatedBounds() {
    // fractional bounds, a bound the key cannot be compared with, two lower bounds: the answer is the scan's
    assertAnswer("MATCH (v:V) WHERE v.a > 100.5 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 100.5 RETURN min(v.a + 0) AS c", Map.of(),
        102L);
    assertAnswer("MATCH (v:V) WHERE v.a < 101.5 RETURN max(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 < 101.5 RETURN max(v.a + 0) AS c", Map.of(),
        100L);
    assertAnswer("MATCH (v:V) WHERE v.a > 'x' RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 'x' RETURN min(v.a + 0) AS c", Map.of(), null);
    assertAnswer("MATCH (v:V) WHERE v.a > 500 AND v.a > 100 RETURN min(v.a) AS c",
        "MATCH (v:V) WHERE v.a + 0 > 500 AND v.a + 0 > 100 RETURN min(v.a + 0) AS c", Map.of(), 502L);
    assertAnswer("MATCH (v:V) WHERE v.a < 500 AND v.a < 100 RETURN max(v.a) AS c",
        "MATCH (v:V) WHERE v.a + 0 < 500 AND v.a + 0 < 100 RETURN max(v.a + 0) AS c", Map.of(), 98L);
  }

  @Test
  void emptyRangeReturnsOneNullRow() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) WHERE v.a > 999999 RETURN min(v.a) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("c")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aDeletedEndOfTheRangeIsSkipped() {
    database.transaction(() -> database.command("sql", "DELETE FROM V WHERE a = 1002"));
    assertAnswer("MATCH (v:V) WHERE v.a > 1001 RETURN min(v.a) AS c", "MATCH (v:V) WHERE v.a + 0 > 1001 RETURN min(v.a + 0) AS c", Map.of(),
        1004L);
  }

  @Test
  void anythingElseInTheWhereKeepsTheScan() {
    assertThat(profile("MATCH (v:V) WHERE v.a > 100 AND v.a <> 500 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(profile("MATCH (v:V) WHERE v.a > 100 OR v.a < 5 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(profile("MATCH (v:V) WHERE v.a = 100 RETURN min(v.a) AS c", Map.of())).doesNotContain("MIN FROM INDEX");
    assertAnswer("MATCH (v:V) WHERE v.a > 100 AND v.a <> 102 RETURN min(v.a) AS c",
        "MATCH (v:V) WHERE v.a + 0 > 100 AND v.a + 0 <> 102 RETURN min(v.a + 0) AS c", Map.of(), 104L);
  }

  private void assertAnswer(final String query, final String check, final Map<String, Object> params, final Object expected) {
    final Object actual = scalar(query, params);
    assertThat(actual).as(query).isEqualTo(scalar(check, params));
    if (expected != null)
      assertThat(actual).as(query).isEqualTo(expected);
  }

  private Object scalar(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return rs.next().getProperty("c");
    }
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
