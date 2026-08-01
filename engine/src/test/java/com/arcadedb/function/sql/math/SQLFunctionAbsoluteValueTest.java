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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the absolute value function. The key is that the mathematical abs function is correctly
 * applied and that values retain their types.
 *
 * @author Michael MacFadden
 */
class SQLFunctionAbsoluteValueTest {

  private SQLFunctionAbsoluteValue function;

  @BeforeEach
  void setup() {
    function = new SQLFunctionAbsoluteValue();
  }

  @Test
  void empty() {
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void testNull() {
    function.execute(null, null, null, new Object[] { null }, null);
    final Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void positiveInteger() {
    function.execute(null, null, null, new Object[] { 10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(10);
  }

  @Test
  void negativeInteger() {
    function.execute(null, null, null, new Object[] { -10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Integer).isTrue();
    assertThat(result).isEqualTo(10);
  }

  @Test
  void positiveLong() {
    function.execute(null, null, null, new Object[] { 10L }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Long).isTrue();
    assertThat(result).isEqualTo(10L);
  }

  @Test
  void negativeLong() {
    function.execute(null, null, null, new Object[] { -10L }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Long).isTrue();
    assertThat(result).isEqualTo(10L);
  }

  @Test
  void positiveShort() {
    function.execute(null, null, null, new Object[] { (short) 10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Short).isTrue();
    assertThat((short) 10).isEqualTo(result);
  }

  @Test
  void negativeShort() {
    function.execute(null, null, null, new Object[] { (short) -10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Short).isTrue();
    assertThat((short) 10).isEqualTo(result);
  }

  /**
   * BYTE is a persisted ArcadeDB type, but this function had no branch for it at all, so
   * {@code abs()} over a BYTE property failed with "Argument to absolute value must be a number".
   */
  @Test
  void positiveByte() {
    function.execute(null, null, null, new Object[] { (byte) 10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Byte).isTrue();
    assertThat(result).isEqualTo((byte) 10);
  }

  @Test
  void negativeByte() {
    function.execute(null, null, null, new Object[] { (byte) -10 }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Byte).isTrue();
    assertThat(result).isEqualTo((byte) 10);
  }

  @Test
  void positiveDouble() {
    function.execute(null, null, null, new Object[] { 10.5D }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Double).isTrue();
    assertThat(result).isEqualTo(10.5D);
  }

  @Test
  void negativeDouble() {
    function.execute(null, null, null, new Object[] { -10.5D }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Double).isTrue();
    assertThat(result).isEqualTo(10.5D);
  }

  @Test
  void positiveFloat() {
    function.execute(null, null, null, new Object[] { 10.5F }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Float).isTrue();
    assertThat(result).isEqualTo(10.5F);
  }

  @Test
  void negativeFloat() {
    function.execute(null, null, null, new Object[] { -10.5F }, null);
    final Object result = function.getResult();
    assertThat(result instanceof Float).isTrue();
    assertThat(result).isEqualTo(10.5F);
  }

  @Test
  void positiveBigDecimal() {
    function.execute(null, null, null, new Object[] { new BigDecimal("10.5") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigDecimal).isTrue();
    assertThat(new BigDecimal("10.5")).isEqualTo(result);
  }

  @Test
  void negativeBigDecimal() {
    function.execute(null, null, null, new Object[] { BigDecimal.valueOf(-10.5D) }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigDecimal).isTrue();
    assertThat(new BigDecimal("10.5")).isEqualTo(result);
  }

  @Test
  void positiveBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("10") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigInteger).isTrue();
    assertThat(new BigInteger("10")).isEqualTo(result);
  }

  @Test
  void negativeBigInteger() {
    function.execute(null, null, null, new Object[] { new BigInteger("-10") }, null);
    final Object result = function.getResult();
    assertThat(result instanceof BigInteger).isTrue();
    assertThat(new BigInteger("10")).isEqualTo(result);
  }

  @Test
  void nonNumber() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[]{"abc"}, null)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Issue #5494. This function preserves the input type, so every fixed-width signed branch has a
   * value whose magnitude it cannot represent: {@code Math.abs(MIN_VALUE)} wraps around and returns
   * the negative input unchanged. Fail the query instead of returning a negative "absolute value".
   */
  @Test
  void longMinValueOverflows() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { Long.MIN_VALUE }, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("long overflow");
  }

  @Test
  void integerMinValueOverflows() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { Integer.MIN_VALUE }, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("integer overflow");
  }

  @Test
  void shortMinValueOverflows() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { Short.MIN_VALUE }, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("short overflow");
  }

  @Test
  void byteMinValueOverflows() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { Byte.MIN_VALUE }, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("byte overflow");
  }

  /**
   * The guard must fire on exactly one value per type, never one early.
   */
  @Test
  void minValuePlusOneIsStillComputedForEveryIntegralType() {
    function.execute(null, null, null, new Object[] { Long.MIN_VALUE + 1 }, null);
    assertThat(function.getResult()).isEqualTo(Long.MAX_VALUE);

    function.execute(null, null, null, new Object[] { Integer.MIN_VALUE + 1 }, null);
    assertThat(function.getResult()).isEqualTo(Integer.MAX_VALUE);

    function.execute(null, null, null, new Object[] { (short) (Short.MIN_VALUE + 1) }, null);
    assertThat(function.getResult()).isEqualTo(Short.MAX_VALUE);

    function.execute(null, null, null, new Object[] { (byte) (Byte.MIN_VALUE + 1) }, null);
    assertThat(function.getResult()).isEqualTo(Byte.MAX_VALUE);
  }

  /**
   * Every numeric ArcadeDB {@link Type} must round-trip through {@code abs()} keeping its own type,
   * so a future numeric type cannot silently fall through to the "not a number" error the way BYTE did.
   */
  @Test
  void everyNumericTypeIsHandledAndKeepsItsType() {
    final Object[] negatives = { (byte) -1, (short) -1, -1, -1L, -1.0F, -1.0D, new BigInteger("-1"), new BigDecimal("-1") };
    final Class<?>[] expected = { Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, BigInteger.class,
        BigDecimal.class };

    for (int i = 0; i < negatives.length; i++) {
      function.execute(null, null, null, new Object[] { negatives[i] }, null);
      assertThat(function.getResult()).isInstanceOf(expected[i]);
      assertThat(((Number) function.getResult()).intValue()).isEqualTo(1);
    }
  }

  /**
   * Arbitrary-precision types have no unrepresentable magnitude, so the guard must not leak into them.
   */
  @Test
  void arbitraryPrecisionTypesAreUnaffected() {
    function.execute(null, null, null, new Object[] { BigInteger.valueOf(Long.MIN_VALUE) }, null);
    assertThat(function.getResult()).isEqualTo(BigInteger.valueOf(Long.MIN_VALUE).negate());

    function.execute(null, null, null, new Object[] { new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE)) }, null);
    assertThat(function.getResult()).isEqualTo(new BigDecimal(BigInteger.valueOf(Long.MIN_VALUE).negate()));
  }

  @Test
  void fromQuery() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunction", db -> {
      final ResultSet result = db.query("sql", "select abs(-45.4) as abs");
      assertThat(((Number) result.next().getProperty("abs")).floatValue()).isEqualTo(45.4F);
    });
  }

  /**
   * End-to-end through the SQL engine. A {@code Long.MIN_VALUE} literal cannot be used here: the SQL
   * parser rejects it while reading the unsigned digits ("Invalid integer: 9223372036854775808"), so
   * the value has to reach {@code abs()} as a stored property - which is also how it would arrive in
   * a real query.
   */
  @Test
  void fromQueryOverStoredLongMinValue() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunctionOverflow", db -> {
      db.getSchema().createDocumentType("Sample").createProperty("v", Type.LONG);
      db.transaction(() -> db.newDocument("Sample").set("v", Long.MIN_VALUE).save());

      assertThatThrownBy(() -> {
        try (final ResultSet rs = db.query("sql", "select abs(v) as abs from Sample")) {
          rs.next().getProperty("abs");
        }
      }).isInstanceOf(CommandExecutionException.class).hasMessageContaining("long overflow");
    });
  }

  /**
   * End-to-end witness for the missing BYTE branch: this query used to fail outright with
   * "Argument to absolute value must be a number" on a perfectly ordinary BYTE column.
   */
  @Test
  void fromQueryOverStoredByte() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunctionByte", db -> {
      db.getSchema().createDocumentType("Sample").createProperty("v", Type.BYTE);
      db.transaction(() -> db.newDocument("Sample").set("v", (byte) -7).save());

      try (final ResultSet rs = db.query("sql", "select abs(v) as abs from Sample")) {
        assertThat(rs.next().<Byte>getProperty("abs")).isEqualTo((byte) 7);
      }
    });
  }

  /**
   * Issue #5545. The overflow is decided by the value the caller supplied, not by anything wrong with the
   * server, so it has to be the same {@link ArithmeticErrorException} the Cypher operators and the Cypher
   * {@code abs()} already raise - that subclass is what the HTTP and Bolt layers single out to answer 400
   * instead of 500. A bare {@code CommandExecutionException} is indistinguishable from a genuine internal
   * fault, which is why the SQL side kept answering 500 after #5602 fixed the Cypher side.
   */
  @Test
  void everyIntegralMinValueOverflowIsAnArithmeticError() {
    final Object[] minValues = { Long.MIN_VALUE, Integer.MIN_VALUE, Short.MIN_VALUE, Byte.MIN_VALUE };
    final String[] messages = { "long overflow", "integer overflow", "short overflow", "byte overflow" };

    for (int i = 0; i < minValues.length; i++) {
      final Object minValue = minValues[i];
      assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { minValue }, null))
          .isInstanceOf(ArithmeticErrorException.class)
          .hasMessageContaining(messages[i]);
    }
  }

  /**
   * The same assertion end to end through the SQL engine, where the value arrives as a stored property
   * because a {@code Long.MIN_VALUE} literal never survives the SQL parser.
   */
  @Test
  void fromQueryOverStoredLongMinValueIsAnArithmeticError() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunctionArithmeticError", db -> {
      db.getSchema().createDocumentType("Sample").createProperty("v", Type.LONG);
      db.transaction(() -> db.newDocument("Sample").set("v", Long.MIN_VALUE).save());

      assertThatThrownBy(() -> {
        try (final ResultSet rs = db.query("sql", "select abs(v) as abs from Sample")) {
          rs.next().getProperty("abs");
        }
      }).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
    });
  }

  /**
   * Issue #5649. A positive duration has to come back untouched whatever its magnitude. The
   * one-minute-and-over cases matter because the old implementation reasoned about
   * {@code toSecondsPart()}, which is the seconds component within the minute rather than the whole.
   */
  @Test
  void positiveDurationIsReturnedUnchanged() {
    final Duration[] positives = { Duration.ofSeconds(5), Duration.ofSeconds(90), Duration.ofNanos(500_000_000L),
        Duration.ofSeconds(90, 500_000_000L), Duration.ofHours(3) };

    for (final Duration positive : positives) {
      function.execute(null, null, null, new Object[] { positive }, null);
      assertThat(function.getResult()).isInstanceOf(Duration.class).isEqualTo(positive);
    }
  }

  /**
   * Issue #5649, the headline case: a negative duration whose magnitude is under a second. The old
   * implementation recombined {@code Math.abs(toSecondsPart())} with {@code Math.abs(toNanosPart())},
   * and because {@code Duration} normalizes a negative value as negative seconds plus a *positive*
   * nanos adjustment, {@code PT-0.5S} is stored as (-1s, +500000000ns) and came back as {@code PT1.5S}.
   */
  @Test
  void negativeSubSecondDurationDoesNotGainASecond() {
    function.execute(null, null, null, new Object[] { Duration.ofNanos(-500_000_000L) }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ofNanos(500_000_000L));

    function.execute(null, null, null, new Object[] { Duration.ofNanos(-1) }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ofNanos(1));
  }

  /**
   * Issue #5649, the worst class. For an exact multiple of a minute both components are zero, so the
   * old {@code seconds > -1 && nanos > -1} guard held for a negative input and handed it straight back:
   * {@code abs()} returned a negative duration.
   */
  @Test
  void negativeWholeMinuteDurationIsNotReturnedNegative() {
    final Duration[] negatives = { Duration.ofSeconds(-60), Duration.ofSeconds(-120), Duration.ofHours(-2) };

    for (final Duration negative : negatives) {
      function.execute(null, null, null, new Object[] { negative }, null);
      assertThat(function.getResult()).isEqualTo(negative.negated());
      assertThat(((Duration) function.getResult()).isNegative()).isFalse();
    }
  }

  /**
   * Issue #5649. Recombining the two parts drops every whole minute of the input, so {@code PT-1M-29.5S}
   * used to come back as {@code PT30.5S} - a magnitude a full minute short of the truth.
   */
  @Test
  void negativeDurationLongerThanAMinuteKeepsItsMinutes() {
    function.execute(null, null, null, new Object[] { Duration.ofSeconds(-90, 500_000_000L) }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ofSeconds(89, 500_000_000L));
  }

  /**
   * Issue #5649. The cases the old branch already got right, kept so the fix is pinned as a strict
   * improvement rather than a swap of one set of failures for another.
   */
  @Test
  void negativeWholeSecondDurationUnderAMinuteStillWorks() {
    function.execute(null, null, null, new Object[] { Duration.ofSeconds(-5) }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ofSeconds(5));

    function.execute(null, null, null, new Object[] { Duration.ofSeconds(-1) }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void zeroDuration() {
    function.execute(null, null, null, new Object[] { Duration.ZERO }, null);
    assertThat(function.getResult()).isEqualTo(Duration.ZERO);
  }

  /**
   * Issue #5649. {@code Duration}'s range is symmetric except for this single value: negating
   * {@code Duration.ofSeconds(Long.MIN_VALUE, 0)} needs {@code Long.MAX_VALUE + 1} seconds, which
   * {@code Duration} cannot hold. Left unguarded it escapes as a raw {@code java.lang.ArithmeticException}
   * and answers HTTP 500; it has to be the same {@link ArithmeticErrorException} the four integral
   * MIN_VALUEs already raise. Note the boundary is exactly one value - a positive nanos adjustment pulls
   * the magnitude back inside range, which the second half of this test pins.
   */
  @Test
  void durationOverflowIsAnArithmeticError() {
    assertThatThrownBy(() -> function.execute(null, null, null, new Object[] { Duration.ofSeconds(Long.MIN_VALUE) }, null))
        .isInstanceOf(ArithmeticErrorException.class)
        .hasMessageContaining("duration overflow");

    final Duration representable = Duration.ofSeconds(Long.MIN_VALUE, 500_000_000L);
    function.execute(null, null, null, new Object[] { representable }, null);
    assertThat(function.getResult()).isEqualTo(representable.negated());
  }

  /**
   * Issue #5649 end to end through the SQL engine, where the duration is built by {@code duration()}
   * rather than handed to the function directly.
   */
  @Test
  void fromQueryOverNegativeDuration() throws Exception {
    TestHelper.executeInNewDatabase("./target/databases/testAbsFunctionDuration", db -> {
      try (final ResultSet rs = db.query("sql", "select abs(duration(-90, 'second')) as abs")) {
        assertThat(rs.next().<Duration>getProperty("abs")).isEqualTo(Duration.ofSeconds(90));
      }

      try (final ResultSet rs = db.query("sql", "select abs(duration(-500, 'millisecond')) as abs")) {
        assertThat(rs.next().<Duration>getProperty("abs")).isEqualTo(Duration.ofMillis(500));
      }
    });
  }
}
