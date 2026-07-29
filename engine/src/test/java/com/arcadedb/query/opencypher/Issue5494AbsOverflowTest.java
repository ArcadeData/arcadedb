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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5494: {@code abs(-9223372036854775808)} silently returned its own
 * negative input, because {@code -Long.MIN_VALUE} is not representable in two's-complement 64-bit
 * arithmetic and {@code Math.abs()} wraps around instead of failing.
 * <p>
 * A silently negative "absolute value" is a wrong result that looks valid and can be persisted, so
 * the query fails instead. This is the same contract the {@code +}, {@code -}, {@code *} and
 * {@code Long.MIN_VALUE / -1} operators already implement in
 * {@link com.arcadedb.query.opencypher.ast.ArithmeticExpression#integerArithmetic}: a
 * {@code CommandExecutionException} carrying Neo4j's {@code long overflow} message.
 */
class Issue5494AbsOverflowTest extends TestHelper {

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("v");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  // ---- reporter's query ----

  @Test
  void absOfLongMinValueLiteralFailsInsteadOfReturningNegative() {
    assertThatThrownBy(() -> single("RETURN abs(-9223372036854775808) AS v"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("long overflow");
  }

  /**
   * The literal path and the stored-property path reach {@code abs()} through different evaluators,
   * so both need a witness: a persisted Long.MIN_VALUE is how the wrong value would escape into
   * user data in practice.
   */
  @Test
  void absOfStoredLongMinValuePropertyFails() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Sample {v: $v})", Map.of("v", Long.MIN_VALUE)));

    assertThatThrownBy(() -> single("MATCH (n:Sample) RETURN abs(n.v) AS v"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("long overflow");
  }

  // ---- the guard must not fire one value early ----

  @Test
  void absOfLongMinValuePlusOneIsStillComputed() {
    assertThat(single("RETURN abs(-9223372036854775807) AS v")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void absOfLongMaxValueIsStillComputed() {
    assertThat(single("RETURN abs(9223372036854775807) AS v")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void absKeepsWorkingOnOrdinaryValues() {
    assertThat(single("RETURN abs(-5) AS v")).isEqualTo(5L);
    assertThat(single("RETURN abs(0) AS v")).isEqualTo(0L);
    assertThat(single("RETURN abs(null) AS v")).isNull();
  }

  /**
   * FLOAT keeps IEEE 754 semantics: there is no unrepresentable magnitude, so the guard must not
   * leak into the floating-point branch.
   */
  @Test
  void absOfFloatsIsUnaffectedByTheIntegerGuard() {
    assertThat(single("RETURN abs(-5.5) AS v")).isEqualTo(5.5);
    assertThat(single("RETURN abs(-1.7976931348623157E308) AS v")).isEqualTo(Double.MAX_VALUE);
  }

  /**
   * {@code abs()} must report overflow exactly like the arithmetic operators do (issue #5164), so a
   * client can handle one error contract rather than two.
   */
  @Test
  void absOverflowMatchesTheArithmeticOperatorContract() {
    assertThatThrownBy(() -> single("RETURN 9223372036854775807 + 1 AS v"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("long overflow");
  }
}
