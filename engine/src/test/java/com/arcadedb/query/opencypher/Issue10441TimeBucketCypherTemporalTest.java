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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10441: {@code ts.timeBucket()} called from Cypher refused Cypher's own temporal values, because the one
 * date/time converter ({@code DateUtils.toEpochMillis}) did not know the {@code com.arcadedb.query.opencypher.temporal}
 * wrappers. The same call through the explicit {@code sql.} prefix failed on the backticks kept in the function name.
 */
class Issue10441TimeBucketCypherTemporalTest extends TestHelper {

  private static final LocalDateTime BUCKET = LocalDateTime.of(2026, 10, 19, 8, 0);

  @Test
  void zonedDatetimeIsBucketed() {
    assertThat(single("RETURN ts.timeBucket('1m', datetime('2026-10-19T08:00:33Z')) AS b")).isEqualTo(BUCKET);
  }

  @Test
  void zonedDatetimeWithOffsetIsBucketedAtItsInstant() {
    // 10:00:33+02:00 is 08:00:33Z: the bucket is the instant's, read at UTC as every other zoned value is
    assertThat(single("RETURN ts.timeBucket('1m', datetime('2026-10-19T10:00:33+02:00')) AS b")).isEqualTo(BUCKET);
  }

  @Test
  void localDatetimeIsBucketed() {
    assertThat(single("RETURN ts.timeBucket('1m', localdatetime('2026-10-19T08:00:33')) AS b")).isEqualTo(BUCKET);
  }

  @Test
  void dateIsBucketed() {
    assertThat(single("RETURN ts.timeBucket('1d', date('2026-10-19')) AS b")).isEqualTo(LocalDateTime.of(2026, 10, 19, 0, 0));
  }

  @Test
  void epochMillisStillWorks() {
    assertThat(single("RETURN ts.timeBucket('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
  }

  @Test
  void datetimeOriginIsAccepted() {
    assertThat(single(
        "RETURN ts.timeBucket('1h', datetime('2026-10-19T08:10:00Z'), {origin: datetime('2026-10-19T00:30:00Z')}) AS b"))
        .isEqualTo(LocalDateTime.of(2026, 10, 19, 7, 30));
  }

  @Test
  void datetimeEdgePropertyCreatedInCypherIsBucketed() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Account {id: 1})-[:TRANSFERRED {ts: datetime('2026-10-19T08:00:33Z')}]->(:Account {id: 2}), "
            + "(:Account {id: 3})-[:TRANSFERRED {ts: datetime('2026-10-19T08:00:59Z')}]->(:Account {id: 4}), "
            + "(:Account {id: 5})-[:TRANSFERRED {ts: datetime('2026-10-19T08:01:02Z')}]->(:Account {id: 6})").close());

    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[t:TRANSFERRED]->() RETURN ts.timeBucket('1m', t.ts) AS b, count(*) AS c ORDER BY b")) {
      final Result first = rs.next();
      assertThat(asLocalDateTime(first.getProperty("b"))).isEqualTo(BUCKET);
      assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
      final Result second = rs.next();
      assertThat(asLocalDateTime(second.getProperty("b"))).isEqualTo(LocalDateTime.of(2026, 10, 19, 8, 1));
      assertThat(((Number) second.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }

    // the same properties read from SQL bucket the same way
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1m', ts) AS b, count(*) AS c FROM TRANSFERRED GROUP BY b ORDER BY b")) {
      final Result first = rs.next();
      assertThat(asLocalDateTime(first.getProperty("b"))).isEqualTo(BUCKET);
      assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
      assertThat(asLocalDateTime(rs.next().getProperty("b"))).isEqualTo(LocalDateTime.of(2026, 10, 19, 8, 1));
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void otherTimeSeriesFunctionsAcceptCypherDatetimeTimestamps() {
    // ts.first / ts.last read their timestamp through the same DateUtils conversion
    assertThat(single("UNWIND [{v: 1, t: datetime('2026-10-19T08:00:02Z')}, {v: 2, t: datetime('2026-10-19T08:00:01Z')}] AS r "
        + "RETURN ts.first(r.v, r.t) AS b")).isEqualTo(2L);
    assertThat(single("UNWIND [{v: 1, t: localdatetime('2026-10-19T08:00:02')}, {v: 2, t: localdatetime('2026-10-19T08:00:01')}] AS r "
        + "RETURN ts.last(r.v, r.t) AS b")).isEqualTo(1L);
  }

  @Test
  void aTimeOfDayIsStillRefused() {
    // a time without a date is not a moment: refused, never read as some number that means something else
    assertThatThrownBy(() -> single("RETURN ts.timeBucket('1m', localtime('08:00:33')) AS b"))
        .hasStackTraceContaining("Unsupported timestamp for time_bucket");
  }

  @Test
  void sqlPrefixedBacktickedNameReachesTheFunction() {
    assertThat(single("RETURN sql.`ts.timeBucket`('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
    assertThat(single("RETURN sql.`ts.timeBucket`('1m', datetime('2026-10-19T08:00:33Z')) AS b")).isEqualTo(BUCKET);
    assertThat(single("RETURN `ts`.`timeBucket`('1m', 1792396833000) AS b")).isEqualTo(BUCKET);
  }

  @Test
  void backtickedExistsStillTestsThePattern() {
    // the function name is unescaped before the exists(pattern) special case reads it, so the quoted spelling keeps
    // testing the pattern instead of becoming a non-null check that is always true
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})-[:R]->(:P {id: 2})").close());
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P) RETURN n.id AS id, `exists`((n)-[:R]->()) AS e ORDER BY id")) {
      final Result first = rs.next();
      assertThat((Boolean) first.getProperty("e")).isTrue();
      final Result second = rs.next();
      assertThat((Boolean) second.getProperty("e")).isFalse();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private Object single(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final Object value = rs.next().getProperty("b");
      assertThat(rs.hasNext()).isFalse();
      return value instanceof Number n ? n.longValue() : asLocalDateTime(value);
    }
  }

  private static Object asLocalDateTime(final Object value) {
    if (value instanceof LocalDateTime)
      return value;
    if (value instanceof TemporalAccessor accessor)
      return LocalDateTime.from(accessor);
    return value;
  }
}
