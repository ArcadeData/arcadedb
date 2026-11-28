/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.function.cypher;

import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CypherFunctionHelper#cypherCompare} is the order {@code min()}, {@code max()}, {@code coll.sort()} and {@code coll.max()}
 * use over values of any type, so it has to be a total preorder: reflexive, antisymmetric, transitive, and with the types in the
 * order openCypher lists them. Checked over every pair and every triple of a pool that holds each type and its neighbours.
 */
class CypherOrderingOracleTest {
  private static Map<String, Object> map(final Object... keysAndValues) {
    final Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return map;
  }

  /** The pool, from the lowest type to the highest, one list per type. */
  private static List<List<Object>> pool() {
    final ZonedDateTime zoned = ZonedDateTime.of(2024, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
    return List.of(
        // MAP
        Arrays.asList(map(), map("a", 1), map("a", 2), map("b", 1), map("a", 1, "b", 2), map("a", Arrays.asList(1, 2))),
        // LIST
        Arrays.asList(List.of(), List.of(1), List.of(1, 2), List.of(1.0), List.of(2), Arrays.asList((Object) null), List.of("a"), List.of(List.of())),
        // the six temporal types, in the order of the standard
        List.of(new CypherDateTime(zoned), new CypherDateTime(zoned.plusDays(1))),
        List.of(new CypherLocalDateTime(LocalDateTime.of(2024, 1, 2, 3, 4)), new CypherLocalDateTime(LocalDateTime.of(2025, 1, 2, 3, 4))),
        List.of(new CypherDate(LocalDate.of(2024, 1, 2)), new CypherDate(LocalDate.of(2024, 1, 3))),
        List.of(new CypherTime(OffsetTime.of(1, 2, 3, 0, ZoneOffset.UTC)), new CypherTime(OffsetTime.of(5, 2, 3, 0, ZoneOffset.UTC))),
        List.of(new CypherLocalTime(LocalTime.of(1, 2)), new CypherLocalTime(LocalTime.of(5, 2))),
        List.of(CypherDuration.parse("PT1H"), CypherDuration.parse("PT3H"), CypherDuration.parse("P1D")),
        // STRING
        List.of("", "a", "ab", "b", "B"),
        // BOOLEAN
        List.of(false, true),
        // NUMBER
        List.of(Double.NEGATIVE_INFINITY, -2, -1.5, -1L, BigDecimal.valueOf(-1), 0, 0L, 0.0, -0.0, BigDecimal.ZERO, 1, 1L, 1.0f, 1.0, BigDecimal.ONE,
            1.5, 2L, 1e10, Long.MAX_VALUE, Double.POSITIVE_INFINITY),
        // NaN, then NULL
        List.of(Double.NaN, Float.NaN),
        Arrays.asList((Object) null));
  }

  @Test
  void theOrderIsATotalPreorder() {
    final List<Object> values = new ArrayList<>();
    final List<Integer> typeOf = new ArrayList<>();
    final List<List<Object>> pool = pool();
    for (int type = 0; type < pool.size(); type++)
      for (final Object value : pool.get(type)) {
        values.add(value);
        typeOf.add(type);
      }

    final int size = values.size();
    final int[][] compare = new int[size][size];
    for (int i = 0; i < size; i++)
      for (int j = 0; j < size; j++)
        compare[i][j] = Integer.signum(CypherFunctionHelper.cypherCompare(values.get(i), values.get(j)));

    for (int i = 0; i < size; i++) {
      assertThat(compare[i][i]).as("reflexive " + values.get(i)).isZero();
      for (int j = 0; j < size; j++) {
        final String pair = describe(values.get(i)) + " against " + describe(values.get(j));
        assertThat(compare[i][j]).as("antisymmetric " + pair).isEqualTo(-compare[j][i]);
        // a value of a lower type sorts before one of a higher type, whatever they hold
        if (!typeOf.get(i).equals(typeOf.get(j)))
          assertThat(compare[i][j]).as("types " + pair).isEqualTo(Integer.signum(typeOf.get(i) - typeOf.get(j)));
      }
    }

    // transitive: a <= b and b <= c give a <= c, and a tie ties alike
    int triples = 0;
    for (int i = 0; i < size; i++)
      for (int j = 0; j < size; j++)
        for (int k = 0; k < size; k++) {
          if (compare[i][j] <= 0 && compare[j][k] <= 0)
            assertThat(compare[i][k]).as("transitive " + values.get(i) + " <= " + values.get(j) + " <= " + values.get(k)).isLessThanOrEqualTo(0);
          if (compare[i][j] == 0 && compare[j][k] == 0)
            assertThat(compare[i][k]).as("tie of " + values.get(i) + ", " + values.get(j) + ", " + values.get(k)).isZero();
          triples++;
        }
    assertThat(triples).isGreaterThan(100_000);
  }

  @Test
  void numbersOfEveryClassTieWhenTheyAreTheSameNumberAndNullIsLast() {
    assertThat(CypherFunctionHelper.cypherCompare(1, 1L)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(1L, 1.0)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(1.0f, BigDecimal.ONE)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(0.0, -0.0)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(1, 2.5)).isNegative();
    assertThat(CypherFunctionHelper.cypherCompare(Double.NaN, Double.POSITIVE_INFINITY)).isPositive();
    assertThat(CypherFunctionHelper.cypherCompare(null, null)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(null, Double.NaN)).isPositive();
    assertThat(CypherFunctionHelper.cypherCompare("z", null)).isNegative();
  }

  @Test
  void aNativeTemporalOrdersAsTheCypherTypeItIsReadAs() {
    final CypherDate cypherDate = new CypherDate(LocalDate.of(2024, 1, 2));
    assertThat(CypherFunctionHelper.cypherCompare(LocalDate.of(2024, 1, 2), cypherDate)).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(LocalDate.of(2024, 1, 3), cypherDate)).isPositive();
    assertThat(CypherFunctionHelper.cypherCompare(Instant.parse("2024-01-02T00:00:00Z"), cypherDate)).isNegative();
    assertThat(CypherFunctionHelper.cypherCompare(Duration.ofHours(3), CypherDuration.parse("PT3H"))).isZero();
    assertThat(CypherFunctionHelper.cypherCompare(Duration.ofHours(1), "a")).isNegative();
  }

  private static String describe(final Object value) {
    return value + " (" + (value == null ? "null" : value.getClass().getSimpleName()) + ")";
  }
}
