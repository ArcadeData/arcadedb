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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5647, covering the two arithmetic defects left in
 * {@link MathExpression.Operator} after #5631 closed #5545 for {@code abs()} alone.
 * <p>
 * Both are the SQL side of behaviour the Cypher engine already has (#5163, #5164, #5602), so the two engines now
 * answer the same way for the same caller mistake:
 * <ol>
 *   <li>64-bit {@code +}, {@code -} and {@code *} wrapped around silently. The {@code Integer} overload widens to
 *       {@code long} on overflow, but the {@code Long} overload has nowhere left to widen to, so it returned a
 *       mathematically wrong number that an {@code UPDATE ... SET} would then persist.</li>
 *   <li>Integer {@code /} and {@code %} by zero escaped as a raw {@code java.lang.ArithmeticException}, which is not
 *       an {@link ArithmeticErrorException} and so missed the classification the HTTP and Bolt layers apply - a
 *       caller mistake was reported as an internal fault.</li>
 * </ol>
 */
class Issue5647SqlIntegerArithmeticTest {

  /**
   * The wrap-around is only reachable on the {@code Long} overload: {@code Integer} operands widen to {@code long}
   * instead, which is why the two overloads must not be given the same treatment.
   */
  @Test
  void longAdditionSubtractionAndMultiplicationOverflowAreArithmeticErrors() {
    assertThatThrownBy(() -> MathExpression.Operator.STAR.apply(Long.MAX_VALUE, 2L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");

    assertThatThrownBy(() -> MathExpression.Operator.PLUS.apply(Long.MAX_VALUE, 1L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");

    assertThatThrownBy(() -> MathExpression.Operator.MINUS.apply(Long.MIN_VALUE, 1L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
  }

  /**
   * {@code Long.MIN_VALUE / -1} has no {@code long} answer either, and unlike a zero divisor the JDK does not raise
   * for it - {@code /} simply returns the dividend (JLS 15.17.2), which is the same silent wrap as the operators
   * above.
   */
  @Test
  void longDivisionOverflowIsAnArithmeticError() {
    assertThatThrownBy(() -> MathExpression.Operator.SLASH.apply(Long.MIN_VALUE, -1L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
  }

  /**
   * The same division does have an answer in {@code Integer}, because there the operator can widen - so it must
   * return the correct value rather than throw. This is the boundary that keeps the {@code Long} guard above from
   * being over-applied.
   */
  @Test
  void integerDivisionOverflowWidensInsteadOfWrapping() {
    assertThat(MathExpression.Operator.SLASH.apply(Integer.MIN_VALUE, -1)).isEqualTo(2147483648L);
  }

  /**
   * The {@code Integer} overloads of {@code +} and {@code -} widened only for two positive operands, because their
   * guards tested the sign of the operands rather than whether the answer fit. Overflow in the other direction was
   * therefore returned silently - the same defect as the {@code Long} overloads, on the path the issue assumed was
   * already correct. Unlike {@code long} these have somewhere to widen to, so the answer is a value, not an error.
   */
  @Test
  void integerAdditionAndSubtractionWidenInBothDirections() {
    assertThat(MathExpression.Operator.MINUS.apply(Integer.MAX_VALUE, Integer.MIN_VALUE)).isEqualTo(4294967295L);
    assertThat(MathExpression.Operator.PLUS.apply(Integer.MIN_VALUE, Integer.MIN_VALUE)).isEqualTo(-4294967296L);

    // the directions that already worked must keep working
    assertThat(MathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, Integer.MAX_VALUE)).isEqualTo(4294967294L);
    assertThat(MathExpression.Operator.MINUS.apply(Integer.MIN_VALUE, Integer.MAX_VALUE)).isEqualTo(-4294967295L);

    // and the ordinary case still narrows back to Integer rather than promoting everything to Long
    assertThat(MathExpression.Operator.PLUS.apply(2, 3)).isEqualTo(5);
    assertThat(MathExpression.Operator.PLUS.apply(2, 3).getClass()).isEqualTo(Integer.class);
    assertThat(MathExpression.Operator.MINUS.apply(3, 2).getClass()).isEqualTo(Integer.class);
  }

  /**
   * The raw {@code java.lang.ArithmeticException} the JDK throws here is what reached the HTTP layer uncaught. It
   * has to become an {@link ArithmeticErrorException} on both overloads, since {@code 1/0} uses the {@code Integer}
   * one and a stored {@code LONG} column uses the other.
   */
  @Test
  void divisionAndModuloByZeroAreArithmeticErrors() {
    assertThatThrownBy(() -> MathExpression.Operator.SLASH.apply(1, 0))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("/ by zero");

    assertThatThrownBy(() -> MathExpression.Operator.SLASH.apply(1L, 0L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("/ by zero");

    assertThatThrownBy(() -> MathExpression.Operator.REM.apply(1, 0))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("% by zero");

    assertThatThrownBy(() -> MathExpression.Operator.REM.apply(1L, 0L))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("% by zero");
  }

  /**
   * Floating-point division keeps IEEE 754 semantics, matching what the Cypher engine settled on in #5163: only
   * integer division has no representable answer for a zero divisor.
   */
  @Test
  void floatingPointDivisionByZeroKeepsIeeeSemantics() {
    assertThat(MathExpression.Operator.SLASH.apply(1d, 0d)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(MathExpression.Operator.SLASH.apply(0d, 0d).doubleValue()).isNaN();
    assertThat(MathExpression.Operator.SLASH.apply(1f, 0f)).isEqualTo(Float.POSITIVE_INFINITY);
  }

  /**
   * The null guard runs before the divisor check, so a null operand must still propagate as null rather than being
   * reported as a division by zero.
   */
  @Test
  void nullOperandsStillPropagateRatherThanFailing() {
    assertThat(MathExpression.Operator.SLASH.apply((Object) 20, null)).isNull();
    assertThat(MathExpression.Operator.REM.apply((Object) 20, null)).isNull();
  }

  /**
   * Ordinary arithmetic must be untouched, including the {@code Integer} widening the file already did, so the new
   * guards cannot turn valid data into a client error.
   */
  @Test
  void arithmeticWithoutOverflowIsUnchanged() {
    assertThat(MathExpression.Operator.STAR.apply(Long.MAX_VALUE, 1L)).isEqualTo(Long.MAX_VALUE);
    assertThat(MathExpression.Operator.PLUS.apply(Long.MAX_VALUE - 1, 1L)).isEqualTo(Long.MAX_VALUE);
    assertThat(MathExpression.Operator.MINUS.apply(Long.MIN_VALUE + 1, 1L)).isEqualTo(Long.MIN_VALUE);
    assertThat(MathExpression.Operator.SLASH.apply(10L, 2L)).isEqualTo(5L);
    assertThat(MathExpression.Operator.REM.apply(10L, 3L)).isEqualTo(1L);

    // the Integer overloads still widen instead of wrapping, and still narrow back when the answer fits
    assertThat(MathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, 1)).isEqualTo(2147483648L);
    assertThat(MathExpression.Operator.SLASH.apply(1, 1)).isEqualTo(1);
    assertThat(MathExpression.Operator.SLASH.apply(1, 1).getClass()).isEqualTo(Integer.class);
    assertThat(MathExpression.Operator.REM.apply(1, 1).getClass()).isEqualTo(Integer.class);
  }

  /**
   * {@code BigDecimal} is arbitrary precision so it cannot overflow, but its zero divisor raises the same raw
   * {@code java.lang.ArithmeticException} the integral overloads did, and reaches the wire layers the same way.
   */
  @Test
  void bigDecimalDivisionAndModuloByZeroAreArithmeticErrors() {
    assertThatThrownBy(() -> MathExpression.Operator.SLASH.apply(BigDecimal.ONE, BigDecimal.ZERO))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("/ by zero");

    assertThatThrownBy(() -> MathExpression.Operator.REM.apply(BigDecimal.ONE, BigDecimal.ZERO))
        .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("% by zero");
  }

  /**
   * End to end through the SQL engine, which is where the defect was reported. {@code Long.MAX_VALUE} has to arrive
   * as a stored property rather than a literal, matching how it would reach the operator in a real query.
   */
  @Test
  void queryOverStoredLongMaxValueFailsInsteadOfWrapping() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5647Overflow", db -> {
      db.getSchema().createDocumentType("Sample").createProperty("v", Type.LONG);
      db.transaction(() -> db.newDocument("Sample").set("v", Long.MAX_VALUE).save());

      for (final String expression : new String[] { "v * 2", "v + 1", "v - -1" }) {
        assertThatThrownBy(() -> {
          try (final ResultSet rs = db.query("sql", "select " + expression + " as r from Sample")) {
            rs.next().getProperty("r");
          }
        }).as("'%s' must fail rather than wrap", expression)
            .isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
      }
    });
  }

  /**
   * The write path is what makes the silent wrap a data-corruption bug rather than a display wart: before the fix
   * this stored {@code -2} without complaint. The record is re-read afterwards to prove nothing was persisted.
   */
  @Test
  void updateDoesNotPersistAWrappedValue() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5647Update", db -> {
      db.getSchema().createDocumentType("Sample").createProperty("v", Type.LONG);
      db.transaction(() -> db.newDocument("Sample").set("v", Long.MAX_VALUE).save());

      assertThatThrownBy(() -> db.command("sql", "update Sample set v = v * 2"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("long overflow");

      try (final ResultSet rs = db.query("sql", "select v from Sample")) {
        assertThat(rs.next().<Long>getProperty("v")).isEqualTo(Long.MAX_VALUE);
      }
    });
  }

  /**
   * The reported {@code select 1/0} and {@code select 1%0}, which answered HTTP 500 because a raw
   * {@code java.lang.ArithmeticException} is not something the handler can classify.
   */
  @Test
  void queryDividingByZeroIsAnArithmeticError() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testIssue5647DivZero", db -> {
      db.getSchema().createDocumentType("Sample");
      db.transaction(() -> db.newDocument("Sample").set("v", 1).save());

      assertThatThrownBy(() -> {
        try (final ResultSet rs = db.query("sql", "select 1/0 as r from Sample")) {
          rs.next().getProperty("r");
        }
      }).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("/ by zero");

      assertThatThrownBy(() -> {
        try (final ResultSet rs = db.query("sql", "select 1%0 as r from Sample")) {
          rs.next().getProperty("r");
        }
      }).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("% by zero");
    });
  }
}
