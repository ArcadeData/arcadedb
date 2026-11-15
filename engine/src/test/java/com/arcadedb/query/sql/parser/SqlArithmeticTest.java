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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.MathExpression.Operator;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL arithmetic and concatenation operators: integer overflow, literals, BigInteger, null operands.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class SqlArithmeticTest {
  /**
   * Regression for #5647: Operator after #5631 closed #5545 for abs() alone.
   * <p>
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
  @Nested
  class SqlIntegerArithmetic {

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

  /**
   * Regression for #5852: Long.MIN_VALUE (-9223372036854775808) could not be written as a SQL literal.
   * <p>
   * Regression test for issue #5852: {@code Long.MIN_VALUE} (-9223372036854775808) could not be written as a SQL
   * literal.
   * <p>
   * The positive magnitude {@code 9223372036854775808} is one past {@code Long.MAX_VALUE}, so it overflows both
   * {@code Integer.parseInt} and {@code Long.parseLong} on its own - the standard two's-complement-minimum hazard.
   * {@link SQLASTBuilder#visitIntegerLiteral} parsed that bare magnitude before the enclosing unary minus was ever
   * applied ({@code -X} was rewritten to {@code 0 - X}), so the conversion failed before the sign could rescue it.
   * Every other {@code long} value, including {@code Long.MIN_VALUE + 1} and {@code Long.MAX_VALUE}, parsed fine, and
   * the same value was reachable through arithmetic ({@code 0 - 9223372036854775807 - 1}) or through a bound
   * parameter - only the direct literal spelling failed.
   */
  @Nested
  class SqlLongMinValueLiteral {

    @Test
    void longMinValueLiteralParsesToTheCorrectValue() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue5852LiteralSelect", db -> {
        try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808 AS r")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
        }
      });
    }

    /**
     * The boundary just above {@code Long.MIN_VALUE} must keep working exactly as before: it is a normal literal
     * (its bare magnitude fits in a long), so it still goes through the pre-existing {@code 0 - X} path.
     */
    @Test
    void adjacentLongValuesStillParseCorrectly() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue5852Adjacent", db -> {
        try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775807 AS r")) {
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(-9223372036854775807L);
        }
        try (final ResultSet rs = db.query("sql", "SELECT 9223372036854775807 AS r")) {
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MAX_VALUE);
        }
        try (final ResultSet rs = db.query("sql", "SELECT -2147483648 AS r")) {
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(-2147483648L);
        }
      });
    }

    /**
     * The explicit {@code L}/{@code l}-suffixed spelling must fold the same way as the bare literal: the suffix only
     * ever forces the {@code long} conversion, which is already what the fold produces.
     */
    @Test
    void longSuffixedLongMinValueLiteralParsesToTheCorrectValue() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue5852LiteralSuffixSelect", db -> {
        try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808L AS r")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
        }
        try (final ResultSet rs = db.query("sql", "SELECT -9223372036854775808l AS r")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Long>getProperty("r")).isEqualTo(Long.MIN_VALUE);
        }
      });
    }

    /**
     * A magnitude that overflows even after the sign is folded in (more digits than {@code Long.MIN_VALUE}'s) must
     * still be rejected with the original error, not silently accepted.
     */
    @Test
    void largerOverflowIsStillRejected() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue5852StillInvalid", db -> {
        assertThatThrownBy(() -> db.query("sql", "SELECT -99223372036854775808 AS r").close())
            .isInstanceOf(CommandSQLParsingException.class)
            .hasMessageContaining("Invalid integer");
      });
    }

    /**
     * The end-to-end scenario from the issue: a stored {@code Long.MIN_VALUE} row, unreachable by its own literal
     * value in a {@code WHERE} predicate before the fix - including through a {@code UNIQUE} index.
     */
    @Test
    void storedLongMinValueIsReachableByItsOwnLiteral() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue5852IndexLookup", db -> {
        db.getSchema().createVertexType("V");
        db.getSchema().getType("V").createProperty("k", Type.LONG);
        db.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");

        db.transaction(() -> db.command("sql", "INSERT INTO V SET k = 0 - 9223372036854775807 - 1"));

        try (final ResultSet rs = db.query("sql", "SELECT FROM V WHERE k = -9223372036854775808")) {
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Long>getProperty("k")).isEqualTo(Long.MIN_VALUE);
        }
      });
    }
  }

  /**
   * Regression for #7917: MathExpression.Operator.apply(Number, Operator, Number) - the SQL ARITHMETIC promotion chain - had no...
   * <p>
   * Regression test for issue #7917: {@code MathExpression.Operator.apply(Number, Operator, Number)} - the SQL
   * ARITHMETIC promotion chain - had no {@code BigInteger} arm on either side and no {@code Byte} arm at all, so
   * every such operand fell off the end into {@code IllegalArgumentException("Cannot increment value ...")}, which
   * surfaced as HTTP 500 rather than as a 400 naming the caller's values. Comparing the identical pair has worked
   * since #7669 gave {@code Type.castComparableNumber} its full BigInteger branch; only the arithmetic path, a
   * separate chain in a separate file, was left behind.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MathExpressionBigInteger {

    private static final BigInteger FIVE = BigInteger.valueOf(5);

    @Test
    void bigIntegerOnTheLeftAddsToEveryOtherNumericType() {
      assertThat(sum(FIVE, 1)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(FIVE, 1L)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(FIVE, (short) 1)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(FIVE, (byte) 1)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(FIVE, 1.5f)).isEqualByComparingTo(new BigDecimal("6.5"));
      assertThat(sum(FIVE, 1.5d)).isEqualByComparingTo(new BigDecimal("6.5"));
      assertThat(sum(FIVE, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("6.5"));
      assertThat(sum(FIVE, BigInteger.ONE)).isEqualByComparingTo(new BigDecimal("6"));
    }

    @Test
    void bigIntegerOnTheRightAddsToEveryOtherNumericType() {
      assertThat(sum(1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(1L, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum((short) 1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum((byte) 1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
      assertThat(sum(1.5f, FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
      assertThat(sum(1.5d, FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
      assertThat(sum(new BigDecimal("1.5"), FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
    }

    /**
     * A BigInteger past 2^53 must not collapse onto a double on the way through: BigDecimal holds it exactly, which
     * is the whole reason the promotion goes there rather than to double.
     */
    @Test
    void aBigIntegerBeyondDoublesMantissaStaysExact() {
      final BigInteger huge = BigInteger.valueOf(2).pow(80).add(BigInteger.ONE);

      assertThat(sum(huge, 1)).isEqualByComparingTo(new BigDecimal(huge.add(BigInteger.ONE)));
      assertThat(sum(huge, BigInteger.ONE)).isEqualByComparingTo(new BigDecimal(huge.add(BigInteger.ONE)));
    }

    /**
     * NaN and the infinities have no BigDecimal form at all, so those pairs meet in double instead - the same guard
     * {@code Type.castComparableNumber}'s BigInteger arms carry.
     */
    @Test
    void nonFiniteFloatingPointMeetsABigIntegerInDouble() {
      assertThat(Operator.PLUS.apply(FIVE, Operator.PLUS, Double.POSITIVE_INFINITY).doubleValue())
          .isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(Operator.PLUS.apply(Double.NaN, Operator.PLUS, FIVE).doubleValue()).isNaN();
      assertThat(Operator.PLUS.apply(FIVE, Operator.PLUS, Float.NEGATIVE_INFINITY).doubleValue())
          .isEqualTo(Double.NEGATIVE_INFINITY);
      assertThat(Operator.PLUS.apply(Float.NaN, Operator.PLUS, FIVE).doubleValue()).isNaN();
    }

    @Test
    void byteOperandsAreAccepted() {
      // Byte was missing from every arm in both positions, so even Byte + Integer threw.
      assertThat(sum((byte) 2, 1)).isEqualByComparingTo(new BigDecimal("3"));
      assertThat(sum(1, (byte) 2)).isEqualByComparingTo(new BigDecimal("3"));
      assertThat(sum((byte) 2, (byte) 3)).isEqualByComparingTo(new BigDecimal("5"));
      assertThat(sum((byte) 2, 1L)).isEqualByComparingTo(new BigDecimal("3"));
      assertThat(sum((byte) 2, 1.5f)).isEqualByComparingTo(new BigDecimal("3.5"));
      assertThat(sum((byte) 2, 1.5d)).isEqualByComparingTo(new BigDecimal("3.5"));
      assertThat(sum((byte) 2, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("3.5"));
      assertThat(sum(new BigDecimal("1.5"), (byte) 2)).isEqualByComparingTo(new BigDecimal("3.5"));
    }

    /**
     * The first arm has always accepted a Short on the left, but reached it with {@code new BigDecimal((Integer) a)} -
     * a ClassCastException for exactly the type the arm was written to admit.
     */
    @Test
    void aShortMeetsABigDecimalWithoutAClassCastException() {
      assertThat(sum((short) 2, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("3.5"));
    }

    @Test
    void theOtherArithmeticOperatorsTakeTheSameOperands() {
      assertThat(apply(Operator.MINUS, FIVE, 2)).isEqualByComparingTo(new BigDecimal("3"));
      assertThat(apply(Operator.STAR, FIVE, 3)).isEqualByComparingTo(new BigDecimal("15"));
      assertThat(apply(Operator.REM, FIVE, 3)).isEqualByComparingTo(new BigDecimal("2"));
      assertThat(apply(Operator.SLASH, BigInteger.TEN, 2)).isEqualByComparingTo(new BigDecimal("5"));
    }

    /**
     * The existing promotions must not move: the arms this fix widened also carry every pair that already worked.
     */
    @Test
    void theExistingPromotionsAreUnchanged() {
      assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2)).isEqualTo(3);
      assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2L)).isEqualTo(3L);
      assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2.5f)).isEqualTo(3.5f);
      assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2.5d)).isEqualTo(3.5d);
      assertThat(Operator.PLUS.apply((short) 1, Operator.PLUS, (short) 2)).isEqualTo(3);
      assertThat(Operator.PLUS.apply(1L, Operator.PLUS, 2L)).isEqualTo(3L);
      assertThat(Operator.PLUS.apply(1.5f, Operator.PLUS, 2.5f)).isEqualTo(4.0f);
      assertThat(Operator.PLUS.apply(1.5d, Operator.PLUS, 2.5d)).isEqualTo(4.0d);
    }

    private static BigDecimal sum(final Number left, final Number right) {
      return apply(Operator.PLUS, left, right);
    }

    private static BigDecimal apply(final Operator operator, final Number left, final Number right) {
      final Number result = operator.apply(left, operator, right);
      return result instanceof BigDecimal decimal ? decimal : new BigDecimal(result.toString());
    }
  }

  /**
   * Regression for #8232: MINUS.apply(Object, Object) only guarded a null operand next to a Number (lines handling left instanceof...
   * <p>
   * Regression test for issue #8232: {@code MINUS.apply(Object, Object)} only guarded a null operand next to a
   * {@link Number} (lines handling {@code left instanceof Number && right == null} and its mirror). The date branch
   * that follows had no such guard, so {@code DateUtils.dateTimeToTimestamp(null, ...)} returned {@code null} and the
   * unboxing in {@code apply(Long, Long)} threw a {@link NullPointerException} - taking the whole query down, since the
   * everyday trigger is simply a missing/optional field next to a stored {@code DATETIME}.
   * <p>
   * {@code date - null} and {@code null - date} answer {@code null}, like {@code number - null} and the other arithmetic
   * operators with a NULL operand (#10186; before it {@code date - null} kept the date).
   */
  @Nested
  class DateTimeMinusNull {

    @Test
    void dateMinusNullOperatorIsNull() {
      final LocalDateTime dt = LocalDateTime.now();
      assertThat(MathExpression.Operator.MINUS.apply(dt, null)).isNull();
      assertThat(MathExpression.Operator.MINUS.apply(null, dt)).isNull();
      assertThat(MathExpression.Operator.MINUS.apply((Object) null, null)).isNull();
    }

    /**
     * End to end through the SQL engine, reproducing the issue's exact repro: a stored {@code DATETIME} minus a field
     * absent on the record, and minus an explicit {@code null}, in both operand orders.
     */
    @Test
    void queryOverMissingOrNullFieldNextToADatetimeDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue8232DateTimeMinusNull", db -> {
        db.getSchema().createDocumentType("D").createProperty("dt", Type.DATETIME);
        db.getSchema().getType("D").createProperty("n", Type.LONG);
        db.transaction(() -> db.newDocument("D").set("dt", LocalDateTime.now()).set("n", 5L).save());

        try (final ResultSet rs = db.query("sql", "select dt - nosuch as x, dt from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }

        try (final ResultSet rs = db.query("sql", "select dt - null as x, dt from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }

        try (final ResultSet rs = db.query("sql", "select nosuch - dt as x from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }

        try (final ResultSet rs = db.query("sql", "select null - dt as x from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }

        // a NULL operand of the arithmetic is NULL for a number and a date alike (#10186)
        try (final ResultSet rs = db.query("sql", "select n - nosuch as x from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }
        try (final ResultSet rs = db.query("sql", "select dt + nosuch as x, dt from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNull();
        }
        try (final ResultSet rs = db.query("sql", "select dt - dt as x from D")) {
          assertThat(rs.next().<Object>getProperty("x")).isNotNull();
        }
      });
    }
  }

  /**
   * Regression for #8303: MINUS.apply(Object, Object) dereferenced the right operand in its Map arm, so - aborted the query with a raw...
   * <p>
   * Regression test for issue #8303: {@code MINUS.apply(Object, Object)} dereferenced the right operand in its Map arm, so
   * {@code <map> - <null or missing field>} aborted the query with a raw {@link NullPointerException}, while
   * {@code <map> + <missing>}, {@code <date> - <missing>}, {@code <number> - <missing>} and {@code <list> - <missing>} all
   * answered the left operand. The map-minus-map arm also NPE'd on a null value in the right map, and the collection and
   * array arms on a null key.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MapMinusNull {

    @Test
    void mapMinusNullReturnsTheMapUnchanged() {
      final Map<String, Object> map = new HashMap<>(Map.of("a", 1));
      assertThat(MathExpression.Operator.MINUS.apply(map, null)).isEqualTo(Map.of("a", 1));
      assertThat(MathExpression.Operator.MINUS.apply(null, map)).isNull();
    }

    @Test
    void mapMinusMapWithNullValueDoesNotThrow() {
      final Map<String, Object> left = new HashMap<>();
      left.put("a", 1);
      left.put("b", null);
      left.put("c", 3);

      final Map<String, Object> right = new HashMap<>();
      right.put("a", null); // left has a=1: values differ, kept
      right.put("b", null); // left has b=null: values match, removed
      right.put("z", null); // left has no z: nothing to remove

      final Map<String, Object> result = (Map<String, Object>) MathExpression.Operator.MINUS.apply(left, right);
      assertThat(result).containsOnlyKeys("a", "c");
    }

    @Test
    void mapMinusKeysWithNullKeyDoesNotThrow() {
      final List<Object> keys = new ArrayList<>(Arrays.asList("a", null));
      assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(new HashMap<>(Map.of("a", 1, "b", 2)), keys))//
          .containsOnlyKeys("b");
      assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(new HashMap<>(Map.of("a", 1, "b", 2)),
          new Object[] { null, "b" })).containsOnlyKeys("a");
    }

    @Test
    void queryMapMinusMissingFieldDoesNotThrow() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue8303MapMinusNull", db -> {
        db.getSchema().createDocumentType("T").createProperty("m", Type.MAP);
        db.transaction(() -> db.newDocument("T").set("m", Map.of("a", 1)).set("n", 5).set("l", List.of(1, 2, 3)).save());

        try (final ResultSet rs = db.query("sql", "SELECT m - missing AS r FROM T")) {
          assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
        }
        try (final ResultSet rs = db.query("sql", "SELECT m - null AS r FROM T")) {
          assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
        }
        try (final ResultSet rs = db.query("sql", "SELECT m - {'a': null} AS r FROM T")) {
          assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
        }
        try (final ResultSet rs = db.query("sql", "SELECT missing - m AS r FROM T")) {
          assertThat(rs.next().<Object>getProperty("r")).isNull();
        }

        // control: MINUS and PLUS now agree on the same pair of operands
        try (final ResultSet rs = db.query("sql", "SELECT m + missing AS r FROM T")) {
          assertThat(rs.next().<Map<String, Object>>getProperty("r")).isEqualTo(Map.of("a", 1));
        }
        try (final ResultSet rs = db.query("sql", "SELECT n - missing AS r, l - missing AS l2 FROM T")) {
          final var row = rs.next();
          // a NULL operand of numeric arithmetic is NULL (#10186); a list or a map minus NULL keeps the collection
          assertThat(row.<Object>getProperty("r")).isNull();
          assertThat(row.<List<Object>>getProperty("l2")).containsExactly(1, 2, 3);
        }
      });
    }

    /**
     * Correlated defect found while fixing #8303: the collection and map arms of {@code +} and {@code -} edited their left
     * operand in place. That operand is the record's own property, so a read-only {@code SELECT l - 1} rewrote the
     * record's cached list, and {@code UPDATE ... SET l = l + 9} skipped the write (the "new" value was the very instance
     * the record held), so the change never reached disk.
     */
    @Test
    void operatorsNeverMutateTheRecordTheyRead() {
      final String path = "./target/databases/testIssue8303NoMutation";
      try (final DatabaseFactory factory = new DatabaseFactory(path)) {
        if (factory.exists())
          factory.open().drop();

        try (final Database db = factory.create()) {
          db.getSchema().createDocumentType("T");
          db.transaction(() -> db.command("sql", "INSERT INTO T SET l = [1, 2, 3], m = {'a': 1, 'b': 2}"));

          for (final String q : new String[] { "SELECT l - 1 AS r FROM T", "SELECT l + 9 AS r FROM T", "SELECT m - ['a'] AS r FROM T",
              "SELECT m + {'z': 1} AS r FROM T", "SELECT m - {'b': 2} AS r FROM T" })
            try (final ResultSet rs = db.query("sql", q)) {
              rs.next();
            }

          try (final ResultSet rs = db.query("sql", "SELECT l, m FROM T")) {
            final var row = rs.next();
            assertThat(row.<List<Object>>getProperty("l")).containsExactly(1, 2, 3);
            assertThat(row.<Map<String, Object>>getProperty("m")).isEqualTo(Map.of("a", 1, "b", 2));
          }

          db.transaction(() -> db.command("sql", "UPDATE T SET l = l + 9, m = m - ['a']"));
        }

        try (final Database db = factory.open()) {
          try (final ResultSet rs = db.query("sql", "SELECT l, m FROM T")) {
            final var row = rs.next();
            assertThat(row.<List<Object>>getProperty("l")).containsExactly(1, 2, 3, 9);
            assertThat(row.<Map<String, Object>>getProperty("m")).isEqualTo(Map.of("b", 2));
          }
          db.drop();
        }
      }
    }

    @Test
    void operatorsAcceptImmutableOperands() {
      assertThat((List<Object>) MathExpression.Operator.MINUS.apply(List.of(1, 2, 3), 2)).containsExactly(1, 3);
      assertThat((List<Object>) MathExpression.Operator.PLUS.apply(List.of(1, 2), 3)).containsExactly(1, 2, 3);
      assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(Map.of("a", 1), null)).isEqualTo(Map.of("a", 1));
      assertThat((Map<String, Object>) MathExpression.Operator.MINUS.apply(Map.of("a", 1), List.of("a"))).isEmpty();
    }

    @Test
    void sortedOperandsStaySortedByTheirComparator() {
      final TreeSet<Integer> set = new TreeSet<>(Comparator.reverseOrder());
      set.addAll(List.of(1, 2, 3));
      final Object plus = MathExpression.Operator.PLUS.apply(set, 4);
      assertThat(plus).isInstanceOf(TreeSet.class);
      assertThat((TreeSet<Integer>) plus).containsExactly(4, 3, 2, 1);
      assertThat(set).containsExactly(3, 2, 1);

      final TreeMap<String, Object> map = new TreeMap<>(Comparator.reverseOrder());
      map.put("a", 1);
      map.put("b", 2);
      final Object minus = MathExpression.Operator.MINUS.apply(map, List.of("a"));
      assertThat(minus).isInstanceOf(TreeMap.class);
      assertThat(((TreeMap<String, Object>) minus).comparator()).isEqualTo(Comparator.reverseOrder());
      assertThat(map).containsOnlyKeys("a", "b");
    }
  }

  /**
   * Regression for #11320: an operand of || is an ArrayConcatExpressionElement, an Expression subclass that re-listed its superclass's...
   * <p>
   * Regression tests for issue #11320 (children #11300 and #11306): an operand of {@code ||} is an
   * {@link ArrayConcatExpressionElement}, an {@link Expression} subclass that re-listed its superclass's fields by hand in
   * {@code copy()} and {@code splitForAggregation()} and fell behind it.
   * <ul>
   * <li>#11300: {@code copy()} left {@code whereCondition} and {@code arrayConcatExpression} behind, so a parenthesised
   * condition used as an operand of {@code ||} silently disappeared from the result (the planner executes the copy);</li>
   * <li>#11306: the aggregate split moved the operand's nested projection onto the aggregate item, where nothing applies
   * it, and {@code toString()} never printed it, so the projection was not applied and a materialized view persisted a
   * definition without it.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ArrayConcatElement extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("T");
        database.newDocument("T").set("a", 5).save();
      });
    }

    @Test
    void aParenthesisedConditionSurvivesTheCopyOfAConcatOperand() {
      for (final String sql : List.of("SELECT (1 > 0) || [1] AS v", "SELECT [1] || (2 > 1) AS v")) {
        final Statement parsed = parse(sql);
        final Statement copy = parsed.copy();
        assertThat(copy.toString()).as("copy of '%s'", sql).isEqualTo(parsed.toString());
        assertThat(copy).as("copy of '%s'", sql).isEqualTo(parsed);
      }
    }

    @Test
    void aParenthesisedConditionIsConcatenatedLikeAnyOtherOperand() {
      assertThat(single("SELECT (a > 1) || [1] AS v FROM T")).isEqualTo(List.of(true, 1));
      assertThat(single("SELECT [1] || (a > 1) AS v FROM T")).isEqualTo(List.of(1, true));
      assertThat(single("SELECT [1] || (a > 10) || [2] AS v FROM T")).isEqualTo(List.of(1, false, 2));
      // the controls of the report: a scalar and a boolean literal were always concatenated
      assertThat(single("SELECT true || [1] AS v FROM T")).isEqualTo(List.of(true, 1));
    }

    /**
     * The census: every field {@link Expression} declares (the authority {@code execute()} reads) must survive
     * {@link ArrayConcatExpressionElement#copy()}, so the next field added to {@link Expression} cannot be dropped by the
     * subclass again.
     */
    @Test
    void copyOfAConcatOperandCarriesEveryFieldExpressionDeclares() throws Exception {
      final List<String> checked = new ArrayList<>();
      for (final Field field : Expression.class.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()))
          continue;
        field.setAccessible(true);

        final ArrayConcatExpressionElement original = new ArrayConcatExpressionElement();
        final Object value = sampleValueOf(field.getType());
        field.set(original, value);

        final ArrayConcatExpressionElement copy = original.copy();
        final Object copied = field.get(copy);
        assertThat(copied).as("Expression.%s after ArrayConcatExpressionElement.copy()", field.getName()).isNotNull();
        if (value instanceof SimpleNode)
          assertThat(copied).as("Expression.%s must be deep-copied", field.getName()).isNotSameAs(value).isEqualTo(value);
        else
          assertThat(copied).as("Expression.%s after ArrayConcatExpressionElement.copy()", field.getName()).isEqualTo(value);
        checked.add(field.getName());
      }
      assertThat(checked).as("the census must still reach the fields of Expression that #11300 lost")
          .contains("whereCondition", "arrayConcatExpression", "mathExpression", "json", "rid", "booleanValue", "isNull");
    }

    @Test
    void aNestedProjectionOnAnAggregateOperandIsApplied() {
      assertThat(single("SELECT list({'x':1,'y':2}):{x} || [] AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
      assertThat(single("SELECT [] || list({'x':1,'y':2}):{x} AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
      assertThat(single("SELECT list({'x':1,'y':2}):{!x} || [3] AS v FROM T")).isEqualTo(List.of(Map.of("y", 2), 3));
      // the controls: without || the projection was always applied
      assertThat(single("SELECT list({'x':1,'y':2}):{x} AS v FROM T")).isEqualTo(List.of(Map.of("x", 1)));
    }

    @Test
    void aNestedProjectionOnAConcatOperandIsRenderedAndKeptByTheCopy() {
      final String sql = "SELECT list({'x':1}):{x} || [] AS v";
      final Statement parsed = parse(sql);
      assertThat(parsed.toString()).contains(":{x}");
      assertThat(parsed.copy().toString()).isEqualTo(parsed.toString());
      // the rendering parses back to the same statement
      assertThat(parse(parsed.toString())).isEqualTo(parsed);

      // equals() tells two operands apart by their projection
      assertThat(parse("SELECT list({'x':1}):{x} || [] AS v")).isNotEqualTo(parse("SELECT list({'x':1}):{y} || [] AS v"));
    }

    @Test
    void aMaterializedViewKeepsTheNestedProjectionOfAConcatOperand() {
      database.command("sql", "CREATE MATERIALIZED VIEW mvn AS SELECT list({'x':1,'y':2}):{x} || [] AS v FROM T");
      try {
        assertThat(database.getSchema().getMaterializedView("mvn").getQuery()).contains(":{x}");
        assertThat(single("SELECT v FROM mvn")).isEqualTo(List.of(Map.of("x", 1)));
      } finally {
        database.command("sql", "DROP MATERIALIZED VIEW mvn");
      }
    }

    /** The {@code v} column of the only row, with every projected {@link Result} turned into its map. */
    private Object single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        final Result row = rs.next();
        assertThat(rs.hasNext()).as(sql).isFalse();
        return normalize(row.getProperty("v"));
      }
    }

    private static Object normalize(final Object value) {
      if (value instanceof Result result)
        return result.toMap();
      if (value instanceof List<?> list) {
        final List<Object> normalized = new ArrayList<>(list.size());
        for (final Object element : list)
          normalized.add(normalize(element));
        return normalized;
      }
      return value;
    }

    private Statement parse(final String sql) {
      return ((DatabaseInternal) database).getStatementCache().get(sql);
    }

    /** A real, non-default value of each kind of field {@link Expression} declares, parsed so its own copy() works. */
    private Object sampleValueOf(final Class<?> type) {
      if (type == boolean.class)
        return true;
      if (type == Boolean.class)
        return Boolean.TRUE;
      final Object value = switch (type.getSimpleName()) {
        case "Rid" -> projectionOf("SELECT #1:2 AS v").rid;
        case "MathExpression" -> projectionOf("SELECT 1 + 2 AS v").mathExpression;
        case "ArrayConcatExpression" -> projectionOf("SELECT [1] || [2] AS v").arrayConcatExpression;
        // a map literal is parsed as an Expression holding the Json, wrapped in a BaseExpression
        case "Json" -> ((BaseExpression) projectionOf("SELECT {'k': 1} AS v").mathExpression).expression.json;
        case "WhereClause" -> projectionOf("SELECT (1 > 0) AS v").whereCondition;
        default -> throw new AssertionError("no sample for the Expression field type " + type.getName()
            + ": add one, so the census keeps covering every field");
      };
      assertThat(value).as("the sample statement must populate a %s", type.getSimpleName()).isNotNull();
      return value;
    }

    private Expression projectionOf(final String sql) {
      return ((SelectStatement) parse(sql)).getProjection().getItems().getFirst().getExpression();
    }
  }
}
