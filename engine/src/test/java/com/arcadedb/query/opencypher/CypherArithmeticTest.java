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
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Arithmetic: division by zero, integer overflow, abs, ceil/floor and long precision.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherArithmeticTest {
  /**
   * Regression test for issue #5163: <b>integer</b> division/modulo by zero must fail the query
   * (like Neo4j, and as required by the OpenCypher TCK) instead of silently returning {@code null}.
   * <p>
   * Note: <b>floating-point</b> division by zero is NOT an error. The OpenCypher TCK (see
   * {@code clauses/return-orderby/ReturnOrderBy1.feature}) requires {@code 0.0 / 0.0} to yield {@code NaN}
   * and {@code x / 0.0} to yield {@code ±Infinity}, matching IEEE 754 and Neo4j. This test also locks in
   * that behavior so a future "fix" does not regress TCK conformance.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5163DivisionByZero extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("U");
      database.transaction(() -> database.newVertex("U").set("id", 1).set("zero", 0).set("zeroD", 0.0).save());
    }

    private void assertFails(final String cypher) {
      assertThatThrownBy(() -> {
        final ResultSet rs = database.query("opencypher", cypher);
        // Force full evaluation (execution is lazy).
        while (rs.hasNext())
          rs.next();
      }).hasMessageContaining("by zero");
    }

    @Test
    void integerDivisionByZeroConstant() {
      assertFails("RETURN 1 / 0 AS r");
    }

    @Test
    void zeroDividedByZeroConstant() {
      assertFails("RETURN 0 / 0 AS r");
    }

    @Test
    void integerModuloByZeroConstant() {
      assertFails("RETURN 1 % 0 AS r");
    }

    @Test
    void integerDivisionByZeroRuntime() {
      // Divisor comes from a property so constant folding cannot pre-compute it.
      assertFails("MATCH (u:U) RETURN 1 / u.zero AS r");
    }

    @Test
    void integerModuloByZeroRuntime() {
      assertFails("MATCH (u:U) RETURN 1 % u.zero AS r");
    }

    // ---- Floating-point division by zero: IEEE 754 semantics, NOT an error (OpenCypher TCK) ----

    @Test
    void floatDivisionByZeroYieldsInfinity() {
      final ResultSet rs = database.query("opencypher", "RETURN 1 / 0.0 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void floatZeroDividedByZeroYieldsNaN() {
      final ResultSet rs = database.query("opencypher", "RETURN 0.0 / 0.0 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isNaN();
    }

    @Test
    void floatModuloByZeroYieldsNaN() {
      final ResultSet rs = database.query("opencypher", "RETURN 1 % 0.0 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isNaN();
    }

    @Test
    void floatDivisionByZeroRuntimeYieldsInfinity() {
      final ResultSet rs = database.query("opencypher", "MATCH (u:U) RETURN 1 / u.zeroD AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void divisionByZeroInWhere() {
      // Must fail, not return the row (silent null propagation bug).
      assertFails("MATCH (u:U) WHERE 1 / 0 IS NULL RETURN u.id");
    }

    @Test
    void divisionByZeroInCase() {
      assertFails("RETURN CASE WHEN 1 / 0 IS NULL THEN 'got_null' ELSE 'ok' END AS r");
    }

    @Test
    void divisionByZeroInWrite() {
      assertThatThrownBy(() -> database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", "CREATE (x:Div {v: 1 / 0}) RETURN x.v");
        while (rs.hasNext())
          rs.next();
      })).hasMessageContaining("by zero");
    }

    @Test
    void validIntegerDivisionStillWorks() {
      final ResultSet rs = database.query("opencypher", "RETURN 1 / 2 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").longValue()).isEqualTo(0L);
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void validFloatDivisionStillWorks() {
      final ResultSet rs = database.query("opencypher", "RETURN 3.0 / 2 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(1.5);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #5164: 64-bit integer arithmetic overflow must fail the query
   * (like Neo4j, the OpenCypher reference implementation) instead of silently wrapping around with
   * two's-complement semantics. Silent wraparound produces mathematically wrong results that look
   * valid and can be persisted to storage.
   * <p>
   * Neo4j raises {@code long overflow} for {@code +}, {@code -} and {@code *} that exceed the
   * {@code long} range, as well as the single division overflow case {@code Long.MIN_VALUE / -1}.
   * Mixed/floating-point arithmetic keeps IEEE 754 semantics (overflow becomes {@code ±Infinity}),
   * which this test also locks in so a future "fix" does not over-reach.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5164IntegerOverflow extends TestHelper {
    private static final long MAX = Long.MAX_VALUE;
    private static final long MIN = Long.MIN_VALUE;

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("U");
      database.transaction(() -> database.newVertex("U")
          .set("id", 1).set("max", MAX).set("min", MIN).set("neg", -1L).set("two", 2L).save());
    }

    private void assertOverflow(final String cypher) {
      assertThatThrownBy(() -> {
        final ResultSet rs = database.query("opencypher", cypher);
        while (rs.hasNext()) // execution is lazy, force full evaluation
          rs.next();
      }).hasMessageContaining("overflow");
    }

    private long asLong(final String cypher) {
      final ResultSet rs = database.query("opencypher", cypher);
      assertThat(rs.hasNext()).isTrue();
      return rs.next().<Number>getProperty("r").longValue();
    }

    // ---- Constant-folded overflow (both operands are literals) ----

    @Test
    void addOverflowConstant() {
      assertOverflow("RETURN 9223372036854775807 + 1 AS r");
    }

    @Test
    void multiplyOverflowConstant() {
      assertOverflow("RETURN 9223372036854775807 * 2 AS r");
    }

    // ---- Runtime overflow (operand comes from a property, no constant folding) ----

    @Test
    void addOverflowRuntime() {
      assertOverflow("MATCH (u:U) RETURN u.max + 1 AS r");
    }

    @Test
    void subtractUnderflowRuntime() {
      // -9223372036854775808 - 1
      assertOverflow("MATCH (u:U) RETURN u.min - 1 AS r");
    }

    @Test
    void multiplyOverflowRuntime() {
      assertOverflow("MATCH (u:U) RETURN u.max * u.two AS r");
    }

    @Test
    void divideOverflowRuntime() {
      // Long.MIN_VALUE / -1 is the only overflowing division.
      assertOverflow("MATCH (u:U) RETURN u.min / u.neg AS r");
    }

    // ---- Overflow must also block a write, never persist the wrapped value ----

    @Test
    void overflowInWriteIsRejected() {
      assertThatThrownBy(() -> database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", "CREATE (x:Overflow {v: 9223372036854775807 + 1}) RETURN x.v");
        while (rs.hasNext())
          rs.next();
      })).hasMessageContaining("overflow");
      // Nothing should have been persisted.
      final ResultSet check = database.query("opencypher", "MATCH (x:Overflow) RETURN count(x) AS r");
      assertThat(check.next().<Number>getProperty("r").longValue()).isEqualTo(0L);
    }

    // ---- Non-overflowing integer arithmetic still works and stays integral ----

    @Test
    void nonOverflowingArithmeticStillWorks() {
      assertThat(asLong("RETURN 9223372036854775806 + 1 AS r")).isEqualTo(MAX);
      assertThat(asLong("RETURN 2 + 3 AS r")).isEqualTo(5L);
      assertThat(asLong("RETURN 1000000 * 1000000 AS r")).isEqualTo(1_000_000_000_000L);
      assertThat(asLong("MATCH (u:U) RETURN u.max - 1 AS r")).isEqualTo(MAX - 1);
    }

    // ---- Floating-point / mixed arithmetic keeps IEEE 754 semantics (Neo4j-compatible) ----

    @Test
    void floatOverflowYieldsInfinityNotError() {
      final ResultSet rs = database.query("opencypher", "RETURN 1.0e308 * 10 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void mixedIntFloatBeyondLongRangeDoesNotError() {
      // 9223372036854775807 * 2.0 uses double arithmetic, no overflow error.
      final ResultSet rs = database.query("opencypher", "RETURN 9223372036854775807 * 2.0 AS r");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isGreaterThan(0.0);
    }
  }

  /**
   * Regression tests for issue #5382: {@code ceil()} and {@code floor()} must return FLOAT
   * (Java Double) even when the result is mathematically integral, matching the openCypher
   * return-type contract implemented by Neo4j. Collapsing whole-number results to Long is
   * semantically observable because it silently switches subsequent divisions to integer
   * division ({@code ceil(2.5)/2} returned 1 instead of 1.5).
   * <p>
   * The same FLOAT contract applies to every math function whose Cypher signature declares
   * a FLOAT return (sqrt, exp, log, log10, trig/hyperbolic functions, degrees, radians,
   * atan2), while {@code abs()} is type-preserving (INTEGER→INTEGER, FLOAT→FLOAT) and
   * {@code sign()} returns INTEGER.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5382CeilFloorFloatType extends TestHelper {

    private Object single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        final Object value = rs.next().getProperty("v");
        assertThat(rs.hasNext()).isFalse();
        return value;
      }
    }

    // ---- reporter's queries ----

    @Test
    void ceilDividedByIntegerUsesFloatDivision() {
      assertThat(single("RETURN ceil(2.5)/2 AS v")).isEqualTo(1.5);
    }

    @Test
    void floorDividedByIntegerUsesFloatDivision() {
      assertThat(single("RETURN floor(3.5)/2 AS v")).isEqualTo(1.5);
    }

    /**
     * Issue #5389 (duplicate of #5382, reported against a Docker image built before the fix
     * landed) listed ceil/floor/cos/exp arithmetic witnesses. Keep them as explicit
     * regressions so a future re-collapse of whole doubles to Long is caught on all four.
     */
    @Test
    void floatContractHoldsInDivisionForAllReportedFunctions() {
      assertThat(single("RETURN ceil(2.1)/2 AS v")).isEqualTo(1.5);
      assertThat(single("RETURN floor(3.5)/2 AS v")).isEqualTo(1.5);
      assertThat(single("RETURN cos(0)/2 AS v")).isEqualTo(0.5);
      assertThat(single("RETURN exp(0)/2 AS v")).isEqualTo(0.5);
      assertThat(single("RETURN ceil(2.1) AS v")).isEqualTo(3.0);
    }

    @Test
    void compositeExpressionKeepsFloatType() {
      // abs(-5) is INTEGER 5, ceil(3.5) is FLOAT 4.0 → 9.0 / 2 = 4.5
      assertThat(single("RETURN (abs(-5) + ceil(3.5)) / 2 AS v")).isEqualTo(4.5);
    }

    // ---- return types of ceil/floor/ceiling ----

    @Test
    void ceilReturnsDouble() {
      assertThat(single("RETURN ceil(2.5) AS v")).isEqualTo(3.0);
      assertThat(single("RETURN ceil(3) AS v")).isEqualTo(3.0);
      assertThat(single("RETURN ceiling(2.5) AS v")).isEqualTo(3.0);
    }

    @Test
    void floorReturnsDouble() {
      assertThat(single("RETURN floor(3.5) AS v")).isEqualTo(3.0);
      assertThat(single("RETURN floor(3) AS v")).isEqualTo(3.0);
    }

    // ---- other FLOAT-contract math functions ----

    @Test
    void sqrtReturnsDouble() {
      assertThat(single("RETURN sqrt(4) AS v")).isEqualTo(2.0);
      assertThat(single("RETURN sqrt(9.0)/2 AS v")).isEqualTo(1.5);
    }

    @Test
    void expLogTrigReturnDouble() {
      assertThat(single("RETURN exp(0) AS v")).isEqualTo(1.0);
      assertThat(single("RETURN log10(100) AS v")).isEqualTo(2.0);
      assertThat(single("RETURN cos(0) AS v")).isEqualTo(1.0);
      assertThat(single("RETURN degrees(0) AS v")).isEqualTo(0.0);
      assertThat(single("RETURN atan2(0, 1) AS v")).isEqualTo(0.0);
    }

    // ---- abs() is type-preserving, sign() returns INTEGER (Neo4j contract) ----

    @Test
    void absPreservesInputType() {
      assertThat(single("RETURN abs(-5) AS v")).isEqualTo(5L);
      assertThat(single("RETURN abs(-5.0) AS v")).isEqualTo(5.0);
      assertThat(single("RETURN abs(-5.5) AS v")).isEqualTo(5.5);
    }

    @Test
    void signStillReturnsInteger() {
      assertThat(single("RETURN sign(-3.5) AS v")).isEqualTo(-1L);
    }

    // ---- large values must not saturate (issue #4391 guard preserved) ----

    @Test
    void largeDoublesStayDoubles() {
      assertThat(single("RETURN floor(1e30) AS v")).isEqualTo(1e30);
      assertThat(single("RETURN ceil(-1e30) AS v")).isEqualTo(-1e30);
    }

    @Test
    void nullInputReturnsNull() {
      assertThat(single("RETURN ceil(null) AS v")).isNull();
      assertThat(single("RETURN floor(null) AS v")).isNull();
      assertThat(single("RETURN abs(null) AS v")).isNull();
    }
  }

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
  @Nested
  class Issue5494AbsOverflow extends TestHelper {

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

  /**
   * Two Cypher lookups answered differently with and without an index (issue #8888): a LONG past 2^53 met a BigDecimal
   * or a Double in double precision in a scan, where 2^53 and 2^53 + 1 are the same number, and an indexed INTEGER
   * looked up by {@code "7.0"} threw NumberFormatException where the scan answered no row.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8888CypherLongPrecision extends TestHelper {
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
    void aLongOrBigIntegerMeetsADoubleByItsBinaryValue() {
      final double twoPow60 = 1152921504606846976.0d;
      final var row = database.query("opencypher",
          "RETURN $big = $d AS eq, $next > $d AS gt, $next < $d AS lt, $d = $big AS eqReversed, $next = $d AS neq",
          Map.of("big", BigInteger.valueOf(1L << 60), "d", twoPow60, "next", (1L << 60) + 1)).next();
      assertThat(row.<Boolean>getProperty("eq")).isTrue();
      assertThat(row.<Boolean>getProperty("eqReversed")).isTrue();
      assertThat(row.<Boolean>getProperty("gt")).isTrue();
      assertThat(row.<Boolean>getProperty("lt")).isFalse();
      assertThat(row.<Boolean>getProperty("neq")).isFalse();
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
}
