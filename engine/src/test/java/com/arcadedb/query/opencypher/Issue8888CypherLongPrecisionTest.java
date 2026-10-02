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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two Cypher lookups answered differently with and without an index (issue #8888): a LONG past 2^53 met a BigDecimal
 * or a Double in double precision in a scan, where 2^53 and 2^53 + 1 are the same number, and an indexed INTEGER
 * looked up by {@code "7.0"} threw NumberFormatException where the scan answered no row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8888CypherLongPrecisionTest extends TestHelper {
  private static final long TWO_POW_53 = 9007199254740992L;

  private void load() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.a LONG");
    database.command("sql", "CREATE PROPERTY V.b LONG");
    database.command("sql", "CREATE PROPERTY V.i INTEGER");
    database.command("sql", "CREATE PROPERTY V.j INTEGER");
    database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON V (i) NOTUNIQUE");
    database.transaction(() -> {
      database.newVertex("V").set("a", TWO_POW_53 + 1, "b", TWO_POW_53 + 1, "i", 7, "j", 7).save();
      database.newVertex("V").set("a", TWO_POW_53, "b", TWO_POW_53, "i", 8, "j", 8).save();
    });
  }

  private long cypher(final String property, final Object value) {
    return database.query("opencypher", "MATCH (n:V) WHERE n." + property + " = $v RETURN n", Map.of("v", value)).stream().count();
  }

  @Test
  void longPastTwoPow53MeetsABigDecimalExactly() {
    load();
    final BigDecimal value = new BigDecimal("9007199254740993");
    assertThat(cypher("a", value)).as("indexed").isEqualTo(1);
    assertThat(cypher("b", value)).as("scan").isEqualTo(1);
    assertThat(database.query("sql", "SELECT FROM V WHERE b = ?", value).stream().count()).as("sql").isEqualTo(1);
  }

  @Test
  void longPastTwoPow53MeetsADoubleExactly() {
    load();
    final double value = 9007199254740992.0;
    assertThat(cypher("a", value)).as("indexed").isEqualTo(1);
    assertThat(cypher("b", value)).as("scan").isEqualTo(1);
  }

  @Test
  void longPastTwoPow53MeetsABigIntegerExactly() {
    load();
    final BigInteger value = BigInteger.valueOf(TWO_POW_53 + 1);
    assertThat(cypher("b", value)).as("scan").isEqualTo(1);
    assertThat(cypher("b", BigInteger.valueOf(TWO_POW_53 + 2))).as("scan, absent").isEqualTo(0);
  }

  @Test
  void rangeComparisonsPastTwoPow53AreExact() {
    load();
    final BigDecimal value = new BigDecimal("9007199254740993");
    assertThat(database.query("opencypher", "MATCH (n:V) WHERE n.b > $v RETURN n", Map.of("v", new BigDecimal("9007199254740992"))).stream()
        .count()).isEqualTo(1);
    assertThat(database.query("opencypher", "MATCH (n:V) WHERE n.b >= $v RETURN n", Map.of("v", value)).stream().count()).isEqualTo(1);
    assertThat(database.query("opencypher", "MATCH (n:V) WHERE n.b < $v RETURN n", Map.of("v", value)).stream().count()).isEqualTo(1);
    assertThat(database.query("opencypher", "MATCH (n:V) WHERE n.b <> $v RETURN n", Map.of("v", value)).stream().count()).isEqualTo(1);
  }

  @Test
  void twoDecimalsPastTwoPow53CompareAsDecimals() {
    assertThat(database.query("opencypher", "RETURN $a = $b AS eq, $a > $b AS gt",
        Map.of("a", new BigDecimal("9007199254740993.0"), "b", new BigDecimal("9007199254740992.0"))).next().<Boolean>getProperty("eq"))
        .isFalse();
  }

  @Test
  void nonFiniteNumbersNeverEqualALongPastTwoPow53() {
    final Map<String, Object> params = Map.of("l", TWO_POW_53 + 1, "nan", Double.NaN, "inf", Double.POSITIVE_INFINITY, "f", 1.5f);
    final var row = database.query("opencypher",
        "RETURN $l = $nan AS a, $l < $inf AS b, $l > $f AS c, $l = $f AS d", params).next();
    assertThat(row.<Boolean>getProperty("a")).isFalse();
    assertThat(row.<Boolean>getProperty("b")).isTrue();
    assertThat(row.<Boolean>getProperty("c")).isTrue();
    assertThat(row.<Boolean>getProperty("d")).isFalse();
  }

  @Test
  void smallNumbersKeepTheirOrdinaryComparison() {
    load();
    assertThat(cypher("j", 7.0)).isEqualTo(1);
    assertThat(cypher("j", new BigDecimal("7.0"))).isEqualTo(1);
    assertThat(cypher("i", 7.0)).isEqualTo(1);
    assertThat(cypher("i", new BigDecimal("7.0"))).isEqualTo(1);
    assertThat(cypher("j", 7.5)).isEqualTo(0);
  }

  @Test
  void hashIndexedIntegerLookedUpByAnUnreadableStringAnswersNoRow() {
    database.command("sql", "CREATE VERTEX TYPE H");
    database.command("sql", "CREATE PROPERTY H.i INTEGER");
    database.getSchema().getType("H").createTypeIndex(Schema.INDEX_TYPE.HASH, false, "i");
    database.transaction(() -> database.newVertex("H").set("i", 7).save());
    assertThat(database.query("opencypher", "MATCH (n:H) WHERE n.i = $v RETURN n", Map.of("v", "7.0")).stream().count()).isZero();
    assertThat(database.query("sql", "SELECT FROM H WHERE i = ?", "7.0").stream().count()).isZero();
    assertThat(database.query("sql", "SELECT FROM H WHERE i = ?", 7).stream().count()).isEqualTo(1);
  }

  @Test
  void aBigDecimalAgainstADoubleStaysInDoublePrecision() {
    final var row = database.query("opencypher", "RETURN $d = $v AS eq, $l = $dl AS maxEq",
        Map.of("d", new BigDecimal("9007199254740993"), "v", 9007199254740992.0d, "l", Long.MAX_VALUE, "dl", 9.223372036854775807E18)).next();
    assertThat(row.<Boolean>getProperty("eq")).isTrue();
    assertThat(row.<Boolean>getProperty("maxEq")).isFalse();
  }

  @Test
  void indexedIntegerLookedUpByAnUnreadableStringAnswersNoRow() {
    load();
    assertThat(cypher("i", "7.0")).as("indexed").isEqualTo(0);
    assertThat(cypher("j", "7.0")).as("scan").isEqualTo(0);
    assertThat(cypher("i", "abc")).as("indexed, not a number").isEqualTo(0);
    assertThat(database.query("sql", "SELECT FROM V WHERE i = ?", "7.0").stream().count()).as("sql indexed").isEqualTo(0);
  }
}
