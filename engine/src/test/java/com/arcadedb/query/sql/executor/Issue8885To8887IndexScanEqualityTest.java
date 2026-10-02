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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #8885, #8886 and #8887: an equality lookup must answer the same through an index (LSM or hash) and through a
 * scan, for every operand the property's type can read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8885To8887IndexScanEqualityTest extends TestHelper {

  @Test
  void decimalScaleIsIgnoredInScan() {
    for (final String index : new String[] { "NOTUNIQUE", "NOTUNIQUE_HASH" }) {
      final String type = "D" + index;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a DECIMAL");
      database.command("sql", "CREATE PROPERTY " + type + ".b DECIMAL");
      database.command("sql", "CREATE INDEX ON " + type + " (a) " + index);
      database.transaction(() -> {
        database.newDocument(type).set("a", new BigDecimal("19.90"), "b", new BigDecimal("19.90")).save();
        database.newDocument(type).set("a", new BigDecimal("7"), "b", new BigDecimal("7")).save();
      });

      for (final Object v : new Object[] { new BigDecimal("19.9"), new BigDecimal("19.900"), "19.9", 19.9d }) {
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(index + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(index + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b IN [?]", v)).as(index + " b IN " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", v)).as(index + " a IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", new BigDecimal("7.00"))).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", new BigDecimal("19.91"))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = '19.9'")).isEqualTo(1);
    }
  }

  @Test
  void instantAndZonedDateTimeMatchInScan() {
    for (final String prop : new String[] { "DATETIME", "DATETIME_MICROS", "DATETIME_NANOS", "DATETIME_SECOND" }) {
      final String type = "T" + prop;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a " + prop);
      database.command("sql", "CREATE PROPERTY " + type + ".b " + prop);
      database.command("sql", "CREATE INDEX ON " + type + " (a) NOTUNIQUE");
      final LocalDateTime t = switch (prop) {
        case "DATETIME" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_000_000);
        case "DATETIME_MICROS" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_000);
        case "DATETIME_NANOS" -> LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_456);
        default -> LocalDateTime.of(2026, 10, 1, 12, 34, 56);
      };
      database.transaction(() -> database.newDocument(type).set("a", t, "b", t).save());

      final Instant i = t.toInstant(ZoneOffset.UTC);
      final Object[] operands = { t, i, t.atZone(ZoneOffset.UTC), t.atOffset(ZoneOffset.UTC),
          t.atOffset(ZoneOffset.UTC).withOffsetSameInstant(ZoneOffset.ofHours(2)) };
      for (final Object v : operands) {
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(prop + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(prop + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE b IN [?]", v)).as(prop + " b IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", i.plusSeconds(1))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", t.plusSeconds(1).atZone(ZoneOffset.UTC))).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", OffsetDateTime.of(t, ZoneOffset.ofHours(2)))).isEqualTo(0);
    }
    assertThat(count("SELECT FROM TDATETIME WHERE b = ?", new Date(Instant.parse("2026-10-01T12:34:56.789Z").toEpochMilli())))
        .isEqualTo(1);
  }

  @Test
  void preEpochInstantMatches() {
    database.command("sql", "CREATE DOCUMENT TYPE Old");
    database.command("sql", "CREATE PROPERTY Old.a DATETIME");
    database.command("sql", "CREATE PROPERTY Old.b DATETIME");
    database.command("sql", "CREATE INDEX ON Old (a) NOTUNIQUE");
    final LocalDateTime t = LocalDateTime.of(1960, 3, 4, 5, 6, 7, 89_000_000);
    database.transaction(() -> database.newDocument("Old").set("a", t, "b", t).save());
    final Instant i = t.toInstant(ZoneOffset.UTC);
    assertThat(count("SELECT FROM Old WHERE a = ?", i)).isEqualTo(1);
    assertThat(count("SELECT FROM Old WHERE b = ?", i)).isEqualTo(1);
    assertThat(count("SELECT FROM Old WHERE b = ?", t.atZone(ZoneOffset.UTC))).isEqualTo(1);
  }

  @Test
  void booleanIndexLookupByStringOrNumber() {
    for (final String index : new String[] { "NOTUNIQUE", "NOTUNIQUE_HASH" }) {
      final String type = "B" + index;
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a BOOLEAN");
      database.command("sql", "CREATE PROPERTY " + type + ".b BOOLEAN");
      database.command("sql", "CREATE INDEX ON " + type + " (a) " + index);
      database.transaction(() -> {
        database.newDocument(type).set("a", true, "b", true).save();
        database.newDocument(type).set("a", false, "b", false).save();
      });

      for (final Object v : new Object[] { true, "true", "TRUE", 1 }) {
        assertThat(count("SELECT FROM " + type + " WHERE b = ?", v)).as(index + " b = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a = ?", v)).as(index + " a = " + v).isEqualTo(1);
        assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", v)).as(index + " a IN " + v).isEqualTo(1);
      }
      assertThat(count("SELECT FROM " + type + " WHERE a = 'true'")).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", "false")).isEqualTo(1);
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", 0)).isEqualTo(1);
      // a key that does not read as a boolean matches nothing, as the scan does
      assertThat(count("SELECT FROM " + type + " WHERE a = ?", "maybe")).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE b = ?", "maybe")).isEqualTo(0);
      assertThat(count("SELECT FROM " + type + " WHERE a IN [?]", "maybe")).isEqualTo(0);

      // Cypher answers the same with and without the index (no row for a non-boolean operand, no exception)
      final long cypherIndexed = cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", "true");
      final long cypherScan = cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", "true");
      assertThat(cypherIndexed).isEqualTo(cypherScan);
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", 1))
          .isEqualTo(cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", 1));
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.a = $v RETURN n", "maybe")).isZero();
      assertThat(cypherCount("MATCH (n:" + type + ") WHERE n.b = $v RETURN n", "maybe")).isZero();
    }
  }

  @Test
  void binaryComparatorDecimalScale() {
    final BigDecimal a = new BigDecimal("19.9");
    final BigDecimal b = new BigDecimal("19.90");
    assertThat(BinaryComparator.equals(a, b)).isTrue();
    assertThat(BinaryComparator.equals(a, new BigDecimal("19.91"))).isFalse();
    // the exact twin stays scale-sensitive: hashed key structures pick their slot with BigDecimal.hashCode()
    assertThat(BinaryComparator.equalsExact(a, b)).isFalse();
    assertThat(BinaryComparator.equalsExact(a, new BigDecimal("19.9"))).isTrue();
  }

  private long count(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.stream().count();
    }
  }

  private long cypherCount(final String query, final Object value) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("v", value))) {
      return rs.stream().count();
    }
  }
}
