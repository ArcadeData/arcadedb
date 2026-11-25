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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12757: a SKIP / LIMIT value past the int range was read through {@code intValue()}, so it wrapped:
 * {@code LIMIT 4294967296} answered no rows, {@code SKIP 4294967296} skipped nothing and {@code LIMIT 2147483648} was refused as
 * negative. A string SKIP / LIMIT was parsed (or threw NumberFormatException) instead of being refused as the wrong type.
 */
class CypherSkipLimitBeyondIntIssue12757Test extends TestHelper {

  private List<Object> xs(final String query, final Map<String, Object> params) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("x"));
      }
    }
    return out;
  }

  private List<Object> xs(final String query) {
    return xs(query, Map.of());
  }

  @Test
  void limitPastIntKeepsEveryRow() {
    for (final String limit : new String[] { "2147483648", "3000000000", "4294967296", "4294967297", "9223372036854775807" }) {
      assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT " + limit)).as("LIMIT " + limit).hasSize(3);
      assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x ORDER BY x LIMIT " + limit)).as("ORDER BY LIMIT " + limit).hasSize(3);
      assertThat(xs("UNWIND [1, 2, 3] AS x WITH x LIMIT " + limit + " RETURN x")).as("WITH LIMIT " + limit).hasSize(3);
    }
  }

  @Test
  void skipPlusLimitPastIntDoesNotWrap() {
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x ORDER BY x SKIP 2000000000 LIMIT 2000000000")).isEmpty();
    assertThat(xs("UNWIND [1, 2, 3] AS x WITH x ORDER BY x SKIP 2000000000 LIMIT 2000000000 RETURN x")).isEmpty();
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x ORDER BY x SKIP 1 LIMIT 3000000000")).containsExactly(2L, 3L);
    assertThat(xs("UNWIND [1, 2, 3] AS x WITH x ORDER BY x DESC SKIP 1 LIMIT 3000000000 RETURN x")).containsExactly(2L, 1L);
  }

  /**
   * {@code LIMIT 2147483647} needs no wrap to break: the top-K heap of the ORDER BY was sized {@code Math.min(k + 1, 1000)}, which
   * overflowed to a negative capacity and failed the query with an IllegalArgumentException from the PriorityQueue.
   */
  @Test
  void limitAtIntMaxUnderOrderByKeepsEveryRow() {
    assertThat(xs("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x LIMIT 2147483647")).containsExactly(1L, 2L, 3L);
    assertThat(xs("UNWIND [3, 1, 2] AS x WITH x ORDER BY x DESC LIMIT 2147483647 RETURN x")).containsExactly(3L, 2L, 1L);
    assertThat(xs("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x SKIP 1 LIMIT 2147483646")).containsExactly(2L, 3L);
    assertThat(xs("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x LIMIT $n", Map.of("n", 2147483647L))).containsExactly(1L, 2L, 3L);
  }

  @Test
  void skipPastIntSkipsEveryRow() {
    for (final String skip : new String[] { "2147483648", "4294967296", "4294967297", "9223372036854775807" }) {
      assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x SKIP " + skip)).as("SKIP " + skip).isEmpty();
      assertThat(xs("UNWIND [1, 2, 3] AS x WITH x SKIP " + skip + " RETURN x")).as("WITH SKIP " + skip).isEmpty();
    }
  }

  @Test
  void parametersPastIntAreHonouredToo() {
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT $n", Map.of("n", 4294967296L))).hasSize(3);
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x SKIP $n", Map.of("n", 4294967296L))).isEmpty();
  }

  @Test
  void negativeValuesPastIntAreStillRefused() {
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT -4294967296")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument");
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x SKIP -4294967296")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NegativeIntegerArgument");
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT $n", Map.of("n", -4294967296L)))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("NegativeIntegerArgument");
  }

  @Test
  void ordinaryValuesStillWork() {
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x SKIP 1 LIMIT 1")).hasSize(1);
    assertThat(xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT 0")).isEmpty();
  }

  @Test
  void aStringIsNotAnInteger() {
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT '2'")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidArgumentType");
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x LIMIT 'te'")).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidArgumentType");
    assertThatThrownBy(() -> xs("UNWIND [1, 2, 3] AS x RETURN x SKIP $s", Map.of("s", "1"))).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidArgumentType");
  }

  /** {@code longValue()} keeps the low 64 bits of a BigInteger or a BigDecimal: 2^64 + 1 reads as 1, 2^63 as a negative, -2^63 - 1 as MAX. */
  @Test
  void bigNumbersAreReadBySignAndSizeNotByTheirLow64Bits() {
    final String limit = "UNWIND [1, 2, 3] AS x RETURN x LIMIT $n";
    final String skip = "UNWIND [1, 2, 3] AS x RETURN x SKIP $n";
    for (final Number big : new Number[] { new BigInteger("18446744073709551617"), new BigInteger("9223372036854775808"),
        new BigDecimal("1E+30"), new BigDecimal("9223372036854775808") }) {
      assertThat(xs(limit, Map.of("n", big))).as("LIMIT " + big).hasSize(3);
      assertThat(xs(skip, Map.of("n", big))).as("SKIP " + big).isEmpty();
    }
    for (final Number negative : new Number[] { new BigInteger("-9223372036854775809"), new BigInteger("-18446744073709551615"),
        new BigDecimal("-1E+30") })
      for (final String query : new String[] { limit, skip })
        assertThatThrownBy(() -> xs(query, Map.of("n", negative))).as(query + " " + negative).isInstanceOf(CommandParsingException.class)
            .hasMessageContaining("NegativeIntegerArgument");

    assertThat(xs(limit, Map.of("n", new BigInteger("2")))).hasSize(2);
    assertThat(xs(skip, Map.of("n", new BigDecimal("2.0")))).hasSize(1);
    assertThatThrownBy(() -> xs(limit, Map.of("n", new BigDecimal("1.5")))).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidArgumentType");
  }
}
