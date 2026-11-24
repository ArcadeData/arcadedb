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
import com.arcadedb.function.agg.CypherAvgFunction;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests of the SQL math and aggregate functions, one nested class per reported problem.
 */
class SQLFunctionMathRegressionTest {

  /**
   * Regression for #11047: pow() and sqrt() answer by value, not by the width of the base.
   * <p>
   * Issue #11047: {@code pow()} answered NULL for every negative base and narrowed its result back into the width of its
   * input ({@code pow(s, 3)} on a SHORT holding 300 answered -832, {@code pow(2, 31)} answered 2147483647), read its exponent
   * through a saturating int read ({@code pow(2, 2.5)} answered 4) and let a bare JDK {@code ArithmeticException} escape for a
   * DECIMAL base and a negative exponent. {@code sqrt()} narrowed the same way ({@code sqrt(2)} answered 1).
   * <p>
   * The guarantee: the answer of {@code pow()} and {@code sqrt()} is a function of the VALUES of their arguments, not of the Java
   * width the base happened to arrive in.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PowSqrtValue extends TestHelper {

    @Test
    void negativeBaseIsDefined() {
      assertThat(single("SELECT pow(-2, 2) AS v")).isEqualTo(4);
      assertThat(single("SELECT pow(-2, 3) AS v")).isEqualTo(-8);
      assertThat(single("SELECT pow(-2.0, 2) AS v")).isEqualTo(4.0);
      assertThat(single("SELECT pow(-2.0, 3) AS v")).isEqualTo(-8.0);
      // A negative base with a fractional exponent has no real result: NULL, as sqrt() of a negative number
      assertThat(single("SELECT pow(-8, 0.5) AS v")).isNull();
    }

    @Test
    void integralResultWidensInsteadOfSaturating() {
      assertThat(single("SELECT pow(2, 31) AS v")).isEqualTo(2147483648L);
      assertThat(single("SELECT pow(2, 30) AS v")).isEqualTo(1073741824);
      assertThat(single("SELECT pow(2, 63) AS v")).isEqualTo(new BigDecimal("9223372036854775808"));
      assertThat(single("SELECT pow(10, 30) AS v")).isEqualTo(new BigDecimal("1000000000000000000000000000000"));
      assertThat(single("SELECT pow(-2, 63) AS v")).isEqualTo(Long.MIN_VALUE);
      assertThat(single("SELECT pow(0, 0) AS v")).isEqualTo(1);
      assertThat(single("SELECT pow(1, 2000000000) AS v")).isEqualTo(1);
      assertThat(single("SELECT pow(-1, 2000000001) AS v")).isEqualTo(-1);
    }

    @Test
    void exponentIsReadAsTheValueTheCallerWrote() {
      assertThat((Double) single("SELECT pow(2, 2.5) AS v")).isCloseTo(Math.pow(2, 2.5), within(1e-12));
      assertThat(single("SELECT pow(2, -1) AS v")).isEqualTo(0.5);
      assertThat(single("SELECT pow(2.0, -1) AS v")).isEqualTo(0.5);
      assertThat(single("SELECT pow(4, 0.5) AS v")).isEqualTo(2.0);
      // A whole exponent written as a decimal is a whole exponent
      assertThat(single("SELECT pow(3, 2.0) AS v")).isEqualTo(9);
    }

    @Test
    void decimalBase() {
      assertThat((BigDecimal) single("SELECT pow(2.12345678901234567890, 2) AS v")).isEqualByComparingTo(
          new BigDecimal("2.12345678901234567890").pow(2));
      assertThat((BigDecimal) single("SELECT pow(2.12345678901234567890, -1) AS v")).isEqualByComparingTo(
          BigDecimal.ONE.divide(new BigDecimal("2.12345678901234567890"), MathContext.DECIMAL128));
      // No representable answer: the typed client error of the arithmetic family, not a bare JDK exception
      assertThatThrownBy(() -> new SQLFunctionPow().execute(null, null, null, new Object[] { BigDecimal.ZERO, -1 }, null))
          .isInstanceOf(ArithmeticErrorException.class);
    }

    @Test
    void negativeBaseIsDecidedOnTheExponentAsWritten() {
      final SQLFunctionPow pow = new SQLFunctionPow();
      // fractional, although its double is 1.0: no real power
      assertThat(pow.execute(null, null, null, new Object[] { -2, new BigDecimal("1.0000000000000000001") }, null)).isNull();
      assertThat(pow.execute(null, null, null, new Object[] { -2.0, new BigDecimal("1.0000000000000000001") }, null)).isNull();
      // an odd whole exponent past 2^53, whose double is even, keeps the sign
      assertThat(pow.execute(null, null, null, new Object[] { -1.0, 9007199254740993L }, null)).isEqualTo(-1.0);
      assertThat(pow.execute(null, null, null, new Object[] { -1, new BigInteger("9007199254740993") }, null)).isEqualTo(-1.0);
      assertThat(pow.execute(null, null, null, new Object[] { -1.0, 9007199254740994L }, null)).isEqualTo(1.0);
      // a whole decimal exponent is whole
      assertThat(pow.execute(null, null, null, new Object[] { -2.0, new BigDecimal("3.00") }, null)).isEqualTo(-8.0);
    }

    @Test
    void zeroToANegativePowerIsRefused() {
      // the same answer whatever the base's type and the exponent's shape: no value, a typed error rather than infinity
      assertThatThrownBy(() -> single("SELECT pow(0, -1) AS v")).isInstanceOf(ArithmeticErrorException.class);
      assertThatThrownBy(() -> single("SELECT pow(0, -1.5) AS v")).isInstanceOf(ArithmeticErrorException.class);
    }

    @Test
    void hugeResultIsRefusedNotTruncated() {
      assertThatThrownBy(() -> single("SELECT pow(2, 3000000000) AS v")).isInstanceOf(ArithmeticErrorException.class);
      // An exact integral answer with more digits than an exact decimal holds is rounded like a decimal, never wrapped
      final BigDecimal big = (BigDecimal) single("SELECT pow(7, 5000) AS v");
      assertThat(big.precision()).isEqualTo(34);
      assertThat(big.signum()).isEqualTo(1);
    }

    @Test
    void nullArgumentAnswersNull() {
      assertThat(single("SELECT pow(null, 2) AS v")).isNull();
      assertThat(single("SELECT pow(2, null) AS v")).isNull();
    }

    @Test
    void sqrtDoesNotTruncate() {
      assertThat(single("SELECT sqrt(2) AS v")).isEqualTo(Math.sqrt(2));
      assertThat(single("SELECT sqrt(4) AS v")).isEqualTo(2.0);
      assertThat(single("SELECT sqrt(-4) AS v")).isNull();
    }

    /**
     * The same base and exponent stored as each numeric property type answer the same value: 300 cubed and 300 to the fifth
     * leave SHORT and INTEGER respectively.
     */
    @Test
    void answerDoesNotDependOnTheWidthOfTheBase() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      for (final String type : new String[] { "SHORT", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DECIMAL" })
        database.command("sql", "CREATE PROPERTY M.p_" + type.toLowerCase() + " " + type);
      database.transaction(() -> database.command("sql",
          "INSERT INTO M SET p_short = 300, p_integer = 300, p_long = 300, p_float = 300, p_double = 300, p_decimal = 300"));

      for (final String type : new String[] { "short", "integer", "long", "float", "double", "decimal" }) {
        final Result row = database.query("sql",
            "SELECT pow(p_" + type + ", 3) AS cube, pow(p_" + type + ", 5) AS fifth, pow(-p_" + type + ", 3) AS negCube, sqrt(p_"
                + type + ") AS root FROM M").next();
        assertThat(new BigDecimal(row.<Number>getProperty("cube").toString())).as("pow(%s 300, 3)", type)
            .isEqualByComparingTo("27000000");
        assertThat(new BigDecimal(row.<Number>getProperty("fifth").toString())).as("pow(%s 300, 5)", type)
            .isEqualByComparingTo("2430000000000");
        assertThat(new BigDecimal(row.<Number>getProperty("negCube").toString())).as("pow(-%s 300, 3)", type)
            .isEqualByComparingTo("-27000000");
        assertThat(row.<Number>getProperty("root").doubleValue()).as("sqrt(%s 300)", type).isCloseTo(Math.sqrt(300), within(1e-6));
      }
    }

    @Test
    void bigIntegerBase() {
      final SQLFunctionPow pow = new SQLFunctionPow();
      assertThat(pow.execute(null, null, null, new Object[] { BigInteger.TEN, 20 }, null)).isEqualTo(BigInteger.TEN.pow(20));
      assertThat(pow.execute(null, null, null, new Object[] { BigInteger.valueOf(-3), 3 }, null)).isEqualTo(BigInteger.valueOf(-27));
      assertThat(pow.execute(null, null, null, new Object[] { (byte) -3, 3 }, null)).isEqualTo(-27);
    }

    private Object single(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.next().getProperty("v");
      }
    }
  }

  /**
   * Regression for #5906: SUM()/AVG() over an INTEGER column must not overflow.
   * <p>
   * Issue #5906: {@code SUM()}/{@code AVG()} over an INTEGER column silently overflowed once the running total
   * exceeded {@code Integer.MAX_VALUE}, because {@code Type.increment()}'s "upgrade to long" guard recomputed the
   * same overflowing {@code int} addition before widening it instead of widening the operands first.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SumAverageIntegerOverflow {

    @Test
    void sumOverIntegerColumnDoesNotOverflow() throws Exception {
      TestHelper.executeInNewDatabase("issue-5906-sum", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n INTEGER");
        // TRUE SUM = 10,000,000,000 - FITS IN A LONG, NOT AN INTEGER
        for (int i = 0; i < 5; i++)
          db.command("sql", "INSERT INTO V SET n = 2000000000");

        try (final ResultSet rs = db.query("sql", "SELECT sum(n) AS s, avg(n) AS a, count(n) AS c FROM V")) {
          final var row = rs.next();
          assertThat(((Number) row.getProperty("s")).longValue()).isEqualTo(10_000_000_000L);
          assertThat(((Number) row.getProperty("a")).doubleValue()).isEqualTo(2.0E9);
          assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(5L);
        }
      });
    }

    @Test
    void sumOverIntegerColumnHandlesNegativeOverflow() throws Exception {
      TestHelper.executeInNewDatabase("issue-5906-sum-negative", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n INTEGER");
        for (int i = 0; i < 3; i++)
          db.command("sql", "INSERT INTO V SET n = -2000000000");

        try (final ResultSet rs = db.query("sql", "SELECT sum(n) AS s FROM V")) {
          assertThat(((Number) rs.next().getProperty("s")).longValue()).isEqualTo(-6_000_000_000L);
        }
      });
    }
  }

  /**
   * Regression for #6390: aggregate and window siblings of sum() reject non-numeric input with a typed error.
   * <p>
   * {@code sum()} was hardened to reject non-numeric input with a typed error (issue #5799); its aggregate and window
   * siblings kept the raw {@code (Number)} cast and answered a ClassCastException - an HTTP 500 - for the same input
   * (issue #6390). They all share {@code SQLFunctionAbstract.requireNumericOrNull} now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AggregateNumericInput extends TestHelper {

    @Test
    void everySumSiblingRejectsANonNumericElement() {
      for (final String query : new String[] { "SELECT sum(['a', 1]) AS r", "SELECT avg(['a', 1]) AS r",
          "SELECT variance(['a']) AS r", "SELECT stddev(['a']) AS r", "SELECT variancep(['a']) AS r",
          "SELECT stddevp(['a']) AS r", "SELECT percentile(['a'], 0.5) AS r", "SELECT median(['a']) AS r" })
        assertThatThrownBy(() -> database.query("sql", query).next())
            .as(query)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires numeric input");
    }

    @Test
    void everySumSiblingRejectsANonNumericScalar() {
      for (final String query : new String[] { "SELECT sum('a') AS r", "SELECT avg('a') AS r", "SELECT variance('a') AS r",
          "SELECT percentile('a', 0.5) AS r" })
        assertThatThrownBy(() -> database.query("sql", query).next())
            .as(query)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires numeric input");
    }

    @Test
    void timeSeriesAggregatesRejectNonNumericInput() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Series6390");
        database.command("sql", "INSERT INTO Series6390 SET a = 'x', b = 'y', ts = 1000, v = 'z'");
        database.command("sql", "INSERT INTO Series6390 SET a = 'x', b = 'y', ts = 2000, v = 'z'");
      });

      database.transaction(() -> {
        for (final String query : new String[] { "SELECT ts.correlate(a, b) AS r FROM Series6390",
            "SELECT ts.rate(v, ts) AS r FROM Series6390", "SELECT ts.delta(v, ts) AS r FROM Series6390",
            "SELECT ts.percentile(v, 0.5) AS r FROM Series6390", "SELECT ts.movingAvg(v, 2) AS r FROM Series6390" })
          assertThatThrownBy(() -> database.query("sql", query).next())
              .as(query)
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("requires numeric input");
      });
    }

    @Test
    void windowConfigurationArgumentsAreCheckedToo() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Window6390");
        database.command("sql", "INSERT INTO Window6390 SET v = 1");
        database.command("sql", "INSERT INTO Window6390 SET v = 3");
      });

      database.transaction(() -> {
        assertThatThrownBy(() -> database.query("sql", "SELECT ts.movingAvg(v, 'wide') AS r FROM Window6390").next())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("window_size");
        assertThatThrownBy(() -> database.query("sql", "SELECT ts.percentile(v, 'half') AS r FROM Window6390").next())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("percentile");
      });
    }

    /**
     * The same bug class in three functions the first sweep missed: a raw {@code (Number)} on {@code pow()}'s
     * exponent, a raw {@code Comparable} sort in {@code ts.rank} (the {@code ts.lag}/{@code ts.lead} shape), and raw
     * casts in {@code ts.interpolate}'s linear branch, unlike the siblings around it.
     */
    @Test
    void theRemainingRawCastSiblingsAnswerTypedErrorsToo() {
      assertThatThrownBy(() -> database.query("sql", "SELECT pow(2, 'x') AS r").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("numeric");

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Sweep6390");
        // A gap in the middle, so the linear branch actually has something to interpolate across.
        database.command("sql", "INSERT INTO Sweep6390 SET v = 'a', ts = 1000");
        database.command("sql", "INSERT INTO Sweep6390 SET ts = 2000");
        database.command("sql", "INSERT INTO Sweep6390 SET v = 'b', ts = 3000");
      });

      database.transaction(() -> {
        assertThatThrownBy(() -> database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS r FROM Sweep6390").next())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires numeric input");
        // ts.rank orders by the timestamp, and a row whose timestamp is null keeps its arrival order at the end
        // rather than NPE the comparator - the ts.lag/ts.lead treatment, which rank had not had either.
        try (final ResultSet rs = database.query("sql", "SELECT ts.rank(v, ts) AS r FROM Sweep6390")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).hasSize(3);
        }
        database.command("sql", "INSERT INTO Sweep6390 SET v = 'c'");
        try (final ResultSet rs = database.query("sql", "SELECT ts.rank(v, ts) AS r FROM Sweep6390")) {
          assertThat(rs.next().<List<Object>>getProperty("r")).hasSize(4);
        }
      });
    }

    @Test
    void powStillRaisesNumbers() {
      try (final ResultSet rs = database.query("sql", "SELECT pow(2, 10) AS a, pow(2, '10') AS b")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1024);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1024);
      }
    }

    @Test
    void numericInputStillAggregatesNormally() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Ok6390");
        database.command("sql", "INSERT INTO Ok6390 SET v = 2, ts = 1000");
        database.command("sql", "INSERT INTO Ok6390 SET v = 4, ts = 2000");
        database.command("sql", "INSERT INTO Ok6390 SET v = 6, ts = 3000");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql",
            "SELECT sum(v) AS s, avg(v) AS a, variance(v) AS va, ts.delta(v, ts) AS d FROM Ok6390")) {
          final var row = rs.next();
          assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(12);
          assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0);
          assertThat(row.<Number>getProperty("va").doubleValue()).isEqualTo(4.0);
          assertThat(row.<Number>getProperty("d").doubleValue()).isEqualTo(4.0);
        }
        try (final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(v, 2) AS m FROM Ok6390")) {
          assertThat(rs.next().<List<Double>>getProperty("m")).containsExactly(2.0, 3.0, 5.0);
        }
        // A null is skipped, not rejected: that is the documented aggregation behaviour and it must survive the guard.
        try (final ResultSet rs = database.query("sql", "SELECT avg([1, null, 3]) AS a, variance([2, null, 4]) AS v")) {
          final var row = rs.next();
          assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(2.0);
          assertThat(row.<Number>getProperty("v").doubleValue()).isEqualTo(2.0);
        }
      });
    }
  }

  /**
   * Regression for #9496: unboxed sum()/avg() totals match the boxed fold.
   * <p>
   * Issue #9496: {@code sum()} and {@code avg()} keep their running total unboxed while the inputs are Double, Long or
   * Integer. The total must stay exactly what folding the inputs with {@link Type#increment(Number, Number)} gives, value
   * and type, including the int to long widening, the long overflow to BigDecimal, a first -0.0, and every other input type
   * falling back to the boxed path.
   */
  @Nested
  class NumericSumRegression {

    /** The total as the boxed accumulator built it before #9496. */
    private static Number reference(final List<Number> values) {
      Number sum = null;
      for (final Number v : values)
        sum = sum == null ? v : Type.increment(sum, v);
      return sum;
    }

    private static Number unboxed(final List<Number> values) {
      final NumericSum sum = new NumericSum();
      for (final Number v : values)
        sum.add(v);
      return sum.get();
    }

    private static void assertSame(final Number... values) {
      final List<Number> list = List.of(values);
      final Number expected = reference(list);
      final Number actual = unboxed(list);
      assertThat(actual).as("sum of %s", list).isEqualTo(expected);
      if (expected != null)
        assertThat(actual.getClass()).as("type of the sum of %s", list).isEqualTo(expected.getClass());
    }

    @Test
    void typesAndValuesMatchTheBoxedFold() {
      assertSame(1.5, 2.25, -3.0);
      assertSame(-0.0);
      assertSame(-0.0, -0.0);
      assertSame(1L, 2L, 3L);
      assertSame(1, 2, 3);
      assertSame(Integer.MAX_VALUE, 1);
      assertSame(Integer.MAX_VALUE, 1, -5);
      assertSame(Integer.MIN_VALUE, -1, 2);
      assertSame(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
      assertSame(1, 2L);
      assertSame(1L, 2);
      assertSame(Long.MAX_VALUE, 1L);
      assertSame(Long.MAX_VALUE, 1);
      assertSame(Long.MIN_VALUE, -1L, 5L);
      assertSame(Integer.MAX_VALUE, Long.MAX_VALUE);
      assertSame(1, 0.5);
      assertSame(1L, 0.5);
      assertSame(0.5, 1L, 2);
      assertSame(Long.MAX_VALUE, 1.0);
      assertSame(1.0, Long.MAX_VALUE, Long.MAX_VALUE);
      assertSame(Double.NaN, 1L);
      assertSame(1L, Double.POSITIVE_INFINITY, 2);
      assertSame(Long.MAX_VALUE, 1L, Double.NaN);
      assertSame(1.5f, 2.5);
      assertSame(1.5, 2.5f, 1L);
      assertSame(1, 2.5f);
      assertSame((short) 1, (short) 2);
      assertSame((short) 1, 2);
      assertSame(1, (short) 2, 3L);
      assertSame((byte) 1, 2.0);
      assertSame(1.0, (byte) 2);
      assertSame(new BigDecimal("1.10"), 2L, 3.5);
      assertSame(1L, new BigDecimal("1.10"), 2);
      assertSame(1.25, new BigDecimal("1.10"));
    }

    @Test
    void randomMixesMatchTheBoxedFold() {
      final Random random = new Random(9496);
      for (int round = 0; round < 2_000; round++) {
        final List<Number> values = new ArrayList<>();
        final int n = 1 + random.nextInt(12);
        for (int i = 0; i < n; i++)
          values.add(switch (random.nextInt(round % 3 == 0 ? 6 : 3)) {
            case 0 -> random.nextDouble() * 1e6 - 5e5;
            case 1 -> random.nextBoolean() ? random.nextLong() : (long) random.nextInt(1000);
            case 2 -> random.nextBoolean() ? random.nextInt() : random.nextInt(1000);
            case 3 -> (short) random.nextInt();
            case 4 -> random.nextFloat();
            default -> new BigDecimal(random.nextInt(1000)).movePointLeft(2);
          });
        final Number expected = reference(values);
        final Number actual = unboxed(values);
        assertSameTotal(actual, expected, values, "sum");
      }
    }

    /**
     * The class of the total is always the boxed fold's. Its value is too when no input is floating-point; with a floating-point
     * input the total is the exact sum correctly rounded (#12696) where the boxed fold rounds at every addition, so the two agree
     * within the fold's rounding error, which is bounded by the sum of the magnitudes.
     */
    private static void assertSameTotal(final Number actual, final Number expected, final List<Number> values, final String what) {
      assertThat(actual.getClass()).as("type of the %s of %s", what, values).isEqualTo(expected.getClass());
      boolean floating = false;
      boolean anyFloat = false;
      double magnitude = 0;
      for (final Number v : values) {
        floating |= v instanceof Double || v instanceof Float;
        anyFloat |= v instanceof Float;
        magnitude += Math.abs(v.doubleValue());
      }
      if (!floating || !Double.isFinite(expected.doubleValue()) || !Double.isFinite(actual.doubleValue())) {
        assertThat(actual).as("%s of %s", what, values).isEqualTo(expected);
        return;
      }
      final double tolerance = magnitude * (anyFloat ? 1e-5 : 1e-13);
      assertThat(actual.doubleValue()).as("%s of %s", what, values).isCloseTo(expected.doubleValue(), within(tolerance));
    }

    /** The total through the primitive adders, each input by its own class; any other class through {@link NumericSum#add}. */
    private static Number primitive(final List<Number> values) {
      final NumericSum sum = new NumericSum();
      for (final Number v : values)
        switch (v) {
        case Integer i -> sum.addInt(i);
        case Long l -> sum.addLong(l);
        case Double d -> sum.addDouble(d);
        case Float f -> sum.addFloat(f);
        case Short s -> sum.addShort(s);
        case Byte b -> sum.addByte(b);
        default -> sum.add(v);
        }
      return sum.get();
    }

    /**
     * The STATS facet feeds the sum without boxing (plan 5 Task 9): every primitive adder gives the boxed fold's total, value and class,
     * including a single SHORT or BYTE value (a {@code Short} or {@code Byte} total, as {@code sum()} of one row gives) and a FLOAT total
     * kept in float arithmetic.
     */
    @Test
    void primitiveAddersMatchBoxedAdd() {
      final List<List<Number>> fixed = List.of(List.of((short) 7), List.of((byte) -3), List.of((short) 1, (byte) 2), List.of((byte) 1, 2.5f),
          List.of((short) 3, Long.MAX_VALUE), List.of((short) 3, 0.5), List.of(1.5f, 2.25f, 1), List.of(1.5f, 2L), List.of(0.1f, 0.2),
          List.of(Integer.MAX_VALUE, (short) 1), List.of(Long.MAX_VALUE, (byte) 1), List.of(2.5f, new BigDecimal("1.5")),
          List.of(Float.NaN, 1.0f), List.of(1, 2.5f, (short) 1), List.of(-0.0f));
      for (final List<Number> values : fixed)
        assertPrimitiveMatches(values);

      final Random random = new Random(9);
      for (int round = 0; round < 5_000; round++) {
        final List<Number> values = new ArrayList<>();
        final int n = 1 + random.nextInt(12);
        // a third of the rounds one class only, the way a STATS column feeds it
        final int only = round % 3 == 0 ? random.nextInt(6) : -1;
        for (int i = 0; i < n; i++)
          values.add(switch (only >= 0 ? only : random.nextInt(7)) {
            case 0 -> random.nextBoolean() ? random.nextInt() : random.nextInt(1000);
            case 1 -> random.nextBoolean() ? random.nextLong() : (long) random.nextInt(1000);
            case 2 -> random.nextDouble() * 1e6 - 5e5;
            case 3 -> random.nextFloat() * 1e4f - 5e3f;
            case 4 -> (short) random.nextInt();
            case 5 -> (byte) random.nextInt();
            default -> new BigDecimal(random.nextInt(1000)).movePointLeft(2);
          });
        assertPrimitiveMatches(values);
      }
    }

    private static void assertPrimitiveMatches(final List<Number> values) {
      final Number expected = reference(values);
      final Number boxed = unboxed(values);
      final Number actual = primitive(values);
      assertSameTotal(actual, expected, values, "primitive sum");
      assertSameTotal(boxed, expected, values, "boxed sum");
      // THE PRIMITIVE ADDERS AND add() TAKE THE SAME TRANSITIONS: THE SAME TOTAL, BIT FOR BIT
      assertThat(actual).as("primitive against boxed sum of %s", values).isEqualTo(boxed);
    }

    /**
     * {@link NumericSum#merge} folds the other total in as one value; {@link SQLFunctionAverage#average} is what avg() returns (told
     * whether a value was a DECIMAL, which avg() tracks itself).
     */
    @Test
    void mergeAndAverageMatchTheFunctions() {
      final NumericSum left = new NumericSum();
      left.addInt(Integer.MAX_VALUE);
      final NumericSum right = new NumericSum();
      right.addInt(1);
      left.merge(right);
      left.merge(new NumericSum());
      assertThat(left.get()).isEqualTo(Type.increment(Integer.MAX_VALUE, 1)).isInstanceOf(Long.class);
      final NumericSum empty = new NumericSum();
      empty.merge(new NumericSum());
      assertThat(empty.get()).isNull();
      assertThat(SQLFunctionAverage.average(empty, 0)).isNull();

      final Object[][] inputs = { { 1, 2 }, { Long.MAX_VALUE, Long.MAX_VALUE }, { 0.5, 1L }, { 1.5f, 2.5f }, { new BigDecimal("1.10"), 2 },
          { (short) 3 } };
      for (final Object[] input : inputs) {
        final SQLFunctionAverage avg = new SQLFunctionAverage();
        final NumericSum sum = new NumericSum();
        for (final Object value : input) {
          avg.aggregate(null, new Object[] { value }, null);
          sum.add((Number) value);
        }
        final boolean decimalInput = input[0] instanceof BigDecimal;
        assertThat(decimalInput ? SQLFunctionAverage.average(sum, input.length, true) : SQLFunctionAverage.average(sum, input.length))
            .as(List.of(input).toString()).isEqualTo(avg.getResult());
      }
    }

    @Test
    void emptyIsNull() {
      assertThat(new NumericSum().get()).isNull();
      assertThat(new SQLFunctionSum().getResult()).isNull();
      assertThat(new SQLFunctionAverage().getResult()).isNull();
    }

    @Test
    void aggregateMatchesExecute() {
      final Object[][] rows = { { 1 }, { null }, { Integer.MAX_VALUE }, { 2L }, { List.of(3, 4L) }, { 0.5 } };
      final SQLFunctionSum viaExecute = new SQLFunctionSum();
      final SQLFunctionSum viaAggregate = new SQLFunctionSum();
      final SQLFunctionAverage avgViaExecute = new SQLFunctionAverage();
      final SQLFunctionAverage avgViaAggregate = new SQLFunctionAverage();
      for (final Object[] row : rows) {
        viaExecute.execute(null, null, null, row, null);
        viaAggregate.aggregate(null, row, null);
        avgViaExecute.execute(null, null, null, row, null);
        avgViaAggregate.aggregate(null, row, null);
      }
      assertThat(viaAggregate.getResult()).isEqualTo(viaExecute.getResult()).isInstanceOf(Double.class);
      assertThat(avgViaAggregate.getResult()).isEqualTo(avgViaExecute.getResult());
    }

    @Test
    void mergedPartialsMatchTheBoxedFold() {
      final SQLFunctionSum left = new SQLFunctionSum();
      final SQLFunctionSum right = new SQLFunctionSum();
      left.aggregate(null, new Object[] { Integer.MAX_VALUE }, null);
      right.aggregate(null, new Object[] { 1 }, null);
      left.mergePartial(right);
      assertThat(left.getResult()).isEqualTo(Type.increment(Integer.MAX_VALUE, 1)).isInstanceOf(Long.class);

      final SQLFunctionAverage avg = new SQLFunctionAverage();
      final SQLFunctionAverage other = new SQLFunctionAverage();
      avg.aggregate(null, new Object[] { Long.MAX_VALUE }, null);
      other.aggregate(null, new Object[] { Long.MAX_VALUE }, null);
      avg.mergePartial(other);
      // A long OVERFLOW WIDENS THE TOTAL TO BigDecimal, BUT THE INPUTS WERE NOT DECIMALS: A DOUBLE MEAN (#8974)
      assertThat(avg.getResult()).isEqualTo((double) Long.MAX_VALUE);
    }

    @Test
    void groupByKeepsTypesThroughSql() throws Exception {
      TestHelper.executeInNewDatabase("issue-9496-sum", db -> {
        db.command("sql", "CREATE DOCUMENT TYPE T");
        db.command("sql", "CREATE PROPERTY T.g STRING");
        db.command("sql", "CREATE PROPERTY T.i INTEGER");
        db.command("sql", "CREATE PROPERTY T.l LONG");
        db.command("sql", "CREATE PROPERTY T.d DOUBLE");
        db.transaction(() -> {
          db.newDocument("T").set("g", "a", "i", 1, "l", 1L, "d", 0.1).save();
          db.newDocument("T").set("g", "a", "i", 2, "l", 2L, "d", 0.2).save();
          db.newDocument("T").set("g", "b", "i", Integer.MAX_VALUE, "l", Long.MAX_VALUE, "d", -0.0).save();
          db.newDocument("T").set("g", "b", "i", 1, "l", 1L).save();
          db.newDocument("T").set("g", "c").save();
        });
        try (final ResultSet rs = db.query("sql",
            "SELECT g, sum(i) AS si, sum(l) AS sl, sum(d) AS sd, avg(i) AS ai, avg(l) AS al, avg(d) AS ad FROM T GROUP BY g ORDER BY g")) {
          Result r = rs.next();
          assertThat(r.<Object>getProperty("si")).isEqualTo(3);
          assertThat(r.<Object>getProperty("sl")).isEqualTo(3L);
          assertThat(r.<Object>getProperty("sd")).isEqualTo(0.1 + 0.2);
          assertThat(r.<Object>getProperty("ai")).isEqualTo(1.5);
          assertThat(r.<Object>getProperty("al")).isEqualTo(1.5);
          assertThat(r.<Object>getProperty("ad")).isEqualTo((0.1 + 0.2) / 2);
          r = rs.next();
          assertThat(r.<Object>getProperty("si")).isEqualTo((long) Integer.MAX_VALUE + 1);
          assertThat(r.<Object>getProperty("sl")).isEqualTo(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE));
          assertThat(r.<Object>getProperty("sd")).isEqualTo(-0.0);
          assertThat(r.<Object>getProperty("ad")).isEqualTo(-0.0);
          r = rs.next();
          assertThat(r.<Object>getProperty("si")).isNull();
          assertThat(r.<Object>getProperty("sd")).isNull();
          assertThat(r.<Object>getProperty("ad")).isNull();
        }
      });
    }
  }

  /**
   * Regression for #12696: a floating-point {@code sum()} / {@code avg()} total does not depend on the order of its values.
   * <p>
   * The parallel scan aggregates the units each worker happens to take into one partial and merges the partials, so which values
   * were added together, and in which order, depended on thread timing: a running {@code double} total answered a different value
   * on almost every run of the same query. {@link NumericSum} now keeps the exact sum and rounds it once, so every split of the
   * values in partials, merged in any order, answers the same value: the exact sum correctly rounded.
   *
   * @author lvca (l.garulli@arcadedata.com)
   */
  @Nested
  class ExactFloatingSum {

    private static double exact(final List<Double> values) {
      BigDecimal sum = BigDecimal.ZERO;
      for (final double v : values)
        sum = sum.add(new BigDecimal(v));
      return sum.doubleValue();
    }

    private static Number sequential(final List<? extends Number> values) {
      final NumericSum sum = new NumericSum();
      for (final Number v : values)
        sum.add(v);
      return sum.get();
    }

    /** The values cut in random partials, each summed on its own, merged in a random order. */
    private static Number split(final List<? extends Number> values, final Random random) {
      final List<NumericSum> partials = new ArrayList<>();
      final int count = 1 + random.nextInt(8);
      for (int i = 0; i < count; i++)
        partials.add(new NumericSum());
      for (final Number v : values)
        partials.get(random.nextInt(count)).add(v);
      Collections.shuffle(partials, random);
      final NumericSum total = new NumericSum();
      for (final NumericSum partial : partials)
        total.merge(partial);
      return total.get();
    }

    @Test
    void theTotalIsTheExactSumCorrectlyRounded() {
      assertThat(sequential(List.of(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1))).isEqualTo(1.0);
      // THE CLASSIC math.fsum CASES: CANCELLATION, AND A TIE THE PARTIALS BELOW IT DECIDE
      assertThat(sequential(List.of(1e100, 1.0, -1e100, 1e-100, 1e50, -1.0, -1e50))).isEqualTo(1e-100);
      assertThat(sequential(List.of(1e-16, 1.0, 1e16))).isEqualTo(10000000000000002.0);
      assertThat(sequential(List.of(1e300, 0.5, -1e300, 0.25))).isEqualTo(0.75);

      final Random random = new Random(12696);
      for (int round = 0; round < 2_000; round++) {
        final List<Double> values = new ArrayList<>();
        final int n = 1 + random.nextInt(40);
        for (int i = 0; i < n; i++)
          values.add(switch (round % 5 == 4 ? 3 + random.nextInt(2) : random.nextInt(3)) {
            case 0 -> Math.round(random.nextDouble() * 10_000) / 100.0;
            case 1 -> (random.nextDouble() - 0.5) * Math.pow(10, random.nextInt(40) - 20);
            case 2 -> random.nextBoolean() ? 1e20 : -1e20;
            // THE WHOLE EXPONENT RANGE, AND THE SUBNORMALS
            case 3 -> Math.scalb(random.nextDouble() - 0.5, random.nextInt(2_000) - 1_000);
            default -> Double.MIN_VALUE * (random.nextInt(1 << 20) - (1 << 19));
          });
        assertThat(sequential(values)).as("sum of %s", values).isEqualTo(exact(values));
      }
    }

    @Test
    void manyValuesAcrossCarryPropagationsStayExact() {
      // MORE VALUES THAN ONE CARRY PROPAGATION IS DEFERRED FOR, OF BOTH SIGNS AND A WIDE RANGE OF MAGNITUDES
      final Random random = new Random(21);
      final NumericSum sum = new NumericSum();
      final NumericSum firstHalf = new NumericSum();
      final NumericSum secondHalf = new NumericSum();
      BigDecimal exact = BigDecimal.ZERO;
      final int count = 1_500_000;
      for (int i = 0; i < count; i++) {
        final double v = Math.scalb(random.nextDouble() - 0.3, random.nextInt(120) - 60);
        sum.addDouble(v);
        (i < count / 3 ? firstHalf : secondHalf).addDouble(v);
        exact = exact.add(new BigDecimal(v));
      }
      assertThat(sum.get()).isEqualTo(exact.doubleValue());
      secondHalf.merge(firstHalf);
      assertThat(secondHalf.get()).isEqualTo(exact.doubleValue());
    }

    @Test
    void extremeTotalsRoundCorrectly() {
      assertThat(sequential(List.of(Double.MAX_VALUE, Double.MAX_VALUE))).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(sequential(List.of(-Double.MAX_VALUE, -Double.MAX_VALUE))).isEqualTo(Double.NEGATIVE_INFINITY);
      // HALF AN ULP OF Double.MAX_VALUE ABOVE IT ROUNDS TO EVEN, WHICH IS INFINITY; LESS STAYS
      assertThat(sequential(List.of(Double.MAX_VALUE, Math.ulp(Double.MAX_VALUE) / 2))).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(sequential(List.of(Double.MAX_VALUE, Math.ulp(Double.MAX_VALUE) / 4))).isEqualTo(Double.MAX_VALUE);
      assertThat(sequential(List.of(Double.MIN_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE))).isEqualTo(Double.MIN_VALUE);
      assertThat(sequential(List.of(Double.MIN_NORMAL, -Double.MIN_VALUE))).isEqualTo(Double.MIN_NORMAL - Double.MIN_VALUE);
      // TIES TO EVEN: 2^53 + 1 IS HALF WAY BETWEEN 2^53 AND 2^53 + 2
      assertThat(sequential(List.of(0x1p53, 1.0))).isEqualTo(0x1p53);
      assertThat(sequential(List.of(0x1p53, 1.0, Double.MIN_VALUE))).isEqualTo(0x1p53 + 2);
      assertThat(sequential(List.of(-0x1p53, -1.0, -Double.MIN_VALUE))).isEqualTo(-0x1p53 - 2);
      assertThat(sequential(List.of(Long.MIN_VALUE, 0.5))).isEqualTo((double) Long.MIN_VALUE);
      assertThat(sequential(List.of(0.5, Long.MIN_VALUE, Long.MAX_VALUE))).isEqualTo(-0.5);
    }

    @Test
    void mergingPartialsInAnyOrderGivesTheSequentialTotal() {
      final Random random = new Random(1);
      for (int round = 0; round < 2_000; round++) {
        final List<Number> values = new ArrayList<>();
        final int n = 1 + random.nextInt(60);
        final int only = round % 4;
        for (int i = 0; i < n; i++)
          // FLOATS AND DOUBLES ARE NOT MIXED: A Float TOTAL MEETING A Double WIDENS AS A WHOLE (Type.increment), A TYPE ORDER
          values.add(switch (only == 3 ? (random.nextBoolean() ? 0 : 2) : only) {
            case 0 -> Math.round(random.nextDouble() * 10_000) / 100.0;
            case 1 -> random.nextFloat() * 1e4f - 5e3f;
            default -> (random.nextDouble() - 0.5) * Math.pow(10, random.nextInt(600) - 300);
          });
        final Number expected = sequential(values);
        for (int attempt = 0; attempt < 5; attempt++)
          assertThat(split(values, random)).as("sum of %s", values).isEqualTo(expected);
      }
    }

    @Test
    void integralTotalsMeetingDoublesAreAddedExactly() {
      // A long TOTAL BEYOND 2^53 IS ADDED EXACTLY, IN WHICHEVER PARTIAL IT ENDS UP (NO PARTIAL CAN OVERFLOW A long HERE: A long
      // OVERFLOW WIDENS TO A BigDecimal, WHICH MEETS A double BY ITS DECIMAL FORM, AS Type.increment() DOES)
      final List<Number> values = List.of(0.5, Long.MAX_VALUE - 10, -(Long.MAX_VALUE - 10), 3, 0.25);
      assertThat(sequential(values)).isEqualTo(3.75);
      final Random random = new Random(7);
      for (int attempt = 0; attempt < 200; attempt++)
        assertThat(split(values, random)).isEqualTo(3.75);
    }

    @Test
    void infinitiesNanAndZerosDoNotDependOnTheOrder() {
      assertThat(sequential(List.of(1.0, Double.POSITIVE_INFINITY, -5.0))).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(sequential(List.of(Double.POSITIVE_INFINITY, 1.0, Double.NEGATIVE_INFINITY))).isEqualTo(Double.NaN);
      assertThat(sequential(List.of(1.0, Double.NaN))).isEqualTo(Double.NaN);
      assertThat(sequential(List.of(-0.0, -0.0))).isEqualTo(-0.0);
      assertThat(sequential(List.of(-0.0, 0.0))).isEqualTo(0.0);
      assertThat(sequential(List.of(1.0, -1.0))).isEqualTo(0.0);
      assertThat(sequential(List.of(1.0, -1.0, -0.0))).isEqualTo(0.0);

      final Random random = new Random(3);
      for (final List<Double> values : List.of(List.of(-0.0, -0.0, -0.0), List.of(-0.0, 0.0, -0.0), List.of(2.0, Double.NEGATIVE_INFINITY, 1.0),
          List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.0), List.of(Double.NaN, 1.0, 2.0)))
        for (int attempt = 0; attempt < 50; attempt++)
          assertThat(split(values, random)).as("sum of %s", values).isEqualTo(sequential(values));
    }

    @Test
    void anIntermediateOverflowIsNotTheTotal() {
      // 1e308 + 1e308 OVERFLOWS A double, BUT THE EXACT TOTAL IS BACK IN RANGE
      assertThat(sequential(List.of(1e308, 1e308, -1e308))).isEqualTo(1e308);
      assertThat(sequential(List.of(1e308, 1e308, 0.5))).isEqualTo(Double.POSITIVE_INFINITY);
      final List<Double> values = List.of(1e308, 1e308, -1e308, 1.5, -1e308, Double.MAX_VALUE, -Double.MAX_VALUE);
      final double expected = exact(values);
      assertThat(sequential(values)).isEqualTo(expected);
      final Random random = new Random(5);
      for (int attempt = 0; attempt < 200; attempt++)
        assertThat(split(values, random)).as("sum of %s", values).isEqualTo(expected);
    }

    @Test
    void sqlAndCypherFunctionsMergeTheirPartialsExactly() {
      final List<Double> values = new ArrayList<>();
      final Random random = new Random(11);
      for (int i = 0; i < 5_000; i++)
        values.add(Math.round(random.nextDouble() * 10_000) / 100.0);
      final double expected = exact(values);

      for (int attempt = 0; attempt < 20; attempt++) {
        final int partials = 2 + random.nextInt(6);
        final SQLFunctionSum[] sums = new SQLFunctionSum[partials];
        final SQLFunctionAverage[] avgs = new SQLFunctionAverage[partials];
        final CypherAvgFunction[] cypherAvgs = new CypherAvgFunction[partials];
        for (int p = 0; p < partials; p++) {
          sums[p] = new SQLFunctionSum();
          avgs[p] = new SQLFunctionAverage();
          cypherAvgs[p] = new CypherAvgFunction();
        }
        for (final double value : values) {
          final int p = random.nextInt(partials);
          sums[p].aggregate(null, new Object[] { value }, null);
          avgs[p].aggregate(null, new Object[] { value }, null);
          cypherAvgs[p].execute(new Object[] { value }, null);
        }
        for (int p = 1; p < partials; p++) {
          sums[0].mergePartial(sums[p]);
          avgs[0].mergePartial(avgs[p]);
          cypherAvgs[0].mergePartial(cypherAvgs[p]);
        }
        assertThat(sums[0].getResult()).isEqualTo(expected);
        assertThat(avgs[0].getResult()).isEqualTo(expected / values.size());
        assertThat(cypherAvgs[0].getAggregatedResult()).isEqualTo(expected / values.size());
      }
    }
  }

  /**
   * Regression for #12569: percentile() and median() order their values exactly, not by what they read as a double.
   * <p>
   * The values were sorted by {@code doubleValue()}, which makes 2^53 and 2^53 + 1 equal: the stable sort then kept them in
   * insertion order, and {@code percentile(v, 0)} / {@code percentile(v, 1)} answered the two endpoints swapped whenever the
   * larger value came first. A quantile that lands exactly on one value (the median of an odd count) answered that value
   * rounded to a double, a number no row holds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PercentileExactOrder {
    private static final long TWO_POW_53 = 9007199254740992L;

    @Test
    void endpointsAreTheSmallestAndTheLargestValueInBothInsertionOrders() throws Exception {
      for (final long[] order : new long[][] { { TWO_POW_53 + 1, TWO_POW_53 }, { TWO_POW_53, TWO_POW_53 + 1 } })
        TestHelper.executeInNewDatabase("issue-12569-endpoints", db -> {
          db.command("sql", "CREATE DOCUMENT TYPE Reading BUCKETS 1");
          db.command("sql", "CREATE PROPERTY Reading.v LONG");
          for (final long v : order)
            db.command("sql", "INSERT INTO Reading SET v = ?", v);

          try (final ResultSet rs = db.query("sql",
              "SELECT min(v) AS lo, max(v) AS hi, percentile(v, 0) AS p0, percentile(v, 1) AS p100, percentile(v, 0, 1) AS both FROM Reading")) {
            final Result row = rs.next();
            assertThat(row.<Object>getProperty("lo")).isEqualTo(TWO_POW_53);
            assertThat(row.<Object>getProperty("hi")).isEqualTo(TWO_POW_53 + 1);
            assertThat(row.<Object>getProperty("p0")).isEqualTo(TWO_POW_53);
            assertThat(row.<Object>getProperty("p100")).isEqualTo(TWO_POW_53 + 1);
            assertThat(row.<List<Object>>getProperty("both")).containsExactly(TWO_POW_53, TWO_POW_53 + 1);
          }
        });
    }

    @Test
    void aQuantileOnOneValueAnswersThatValue() throws Exception {
      TestHelper.executeInNewDatabase("issue-12569-median", db -> {
        db.command("sql", "CREATE DOCUMENT TYPE Reading BUCKETS 1");
        db.command("sql", "CREATE PROPERTY Reading.v LONG");
        for (final long v : new long[] { TWO_POW_53 + 2, TWO_POW_53 + 1, TWO_POW_53 })
          db.command("sql", "INSERT INTO Reading SET v = ?", v);

        try (final ResultSet rs = db.query("sql", "SELECT median(v) AS m, percentile(v, 0.25) AS q1, percentile(v, 0.75) AS q3 FROM Reading")) {
          final Result row = rs.next();
          // (n + 1) * q lands on an index: 2, 1 and 3
          assertThat(row.<Object>getProperty("m")).isEqualTo(TWO_POW_53 + 1);
          // 2^53 is a double exactly, so it keeps the double result such a quantile always had
          assertThat(row.<Object>getProperty("q1")).isEqualTo((double) TWO_POW_53);
          assertThat(row.<Object>getProperty("q3")).isEqualTo(TWO_POW_53 + 2);
        }
      });
    }

    @Test
    void anInterpolationBetweenEqualValuesAnswersThatValue() {
      final SQLFunctionMedian median = new SQLFunctionMedian();
      for (final long v : new long[] { TWO_POW_53 + 1, TWO_POW_53 + 1 })
        median.execute(null, null, null, new Object[] { v }, null);
      assertThat(median.getResult()).isEqualTo(TWO_POW_53 + 1);
    }

    @Test
    void mixedNumberClassesAreOrderedByValue() {
      // the DOUBLE 2^53 is below the LONG 2^53 + 1, which reads as the same double
      final SQLFunctionPercentile percentile = new SQLFunctionPercentile();
      for (final Object v : new Object[] { TWO_POW_53 + 1, (double) TWO_POW_53, 7, new BigInteger("9007199254740994") })
        percentile.execute(null, null, null, new Object[] { v, 0, 1 }, null);
      assertThat((List<Object>) percentile.getResult()).containsExactly(7, new BigInteger("9007199254740994"));

      final SQLFunctionPercentile lowest = new SQLFunctionPercentile();
      for (final Object v : new Object[] { TWO_POW_53 + 1, (double) TWO_POW_53 })
        lowest.execute(null, null, null, new Object[] { v, 0, 1 }, null);
      assertThat((List<Object>) lowest.getResult()).containsExactly((double) TWO_POW_53, TWO_POW_53 + 1);
    }

    @Test
    void valuesADoubleHoldsExactlyKeepTheirDoubleResult() {
      // the result of a quantile that lands on one value stays a double when the double is that value (the contract before #12569)
      final SQLFunctionMedian median = new SQLFunctionMedian();
      for (final long v : new long[] { 3L, 1L, 2L })
        median.execute(null, null, null, new Object[] { v }, null);
      assertThat(median.getResult()).isEqualTo(2.0);
    }

    @Test
    void aQuantileOnOneValueNextToAnInfinityIsNotNaN() {
      // lower + 0 * (Infinity - lower) is NaN: a quantile on one value does not interpolate
      final SQLFunctionMedian median = new SQLFunctionMedian();
      for (final double v : new double[] { Double.POSITIVE_INFINITY, 1.0, 2.0 })
        median.execute(null, null, null, new Object[] { v }, null);
      assertThat(median.getResult()).isEqualTo(2.0);

      final SQLFunctionMedian infinite = new SQLFunctionMedian();
      for (final double v : new double[] { Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY })
        infinite.execute(null, null, null, new Object[] { v }, null);
      assertThat(infinite.getResult()).isEqualTo(Double.POSITIVE_INFINITY);
    }
  }
}
