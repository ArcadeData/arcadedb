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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7715: an integral member of a time-series request used to be NARROWED with {@code Number.longValue()},
 * so a JSON number carrying a fractional part was truncated and the caller was answered {@code 200} for an
 * interval it never asked for.
 * <p>
 * #7675 caught only the half of that which truncates to zero or below - {@code 0.5} lands on the positivity test
 * - and everything at or above {@code 1.0} was silently rounded down. This pins the refusal at the one place both
 * HTTP time-series endpoints read a number through, so {@code from} and {@code to} are covered by the same change
 * as {@code bucketInterval}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7715FractionalIntegralMemberTest {

  /**
   * The issue's own example: the bucket width a client computed by division.
   */
  @Test
  void refusesAFractionalBucketIntervalRatherThanTruncatingIt() {
    // Parsed from text, the way the body actually arrives: the number reaches the resolver as a lazily parsed
    // primitive rather than as a Double the test handed it, and that is the path the truncation lived on.
    final JSONObject aggregation = new JSONObject(
        "{\"bucketInterval\":1.5,\"requests\":[{\"field\":\"temperature\",\"type\":\"AVG\"}]}");

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireLong(aggregation, "bucketInterval", "aggregation.bucketInterval"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation.bucketInterval' must be a whole number between " + Long.MIN_VALUE + " and "
            + Long.MAX_VALUE + ": received 1.5");
  }

  /**
   * The rest of the family the issue asks to be named: every member of both endpoints that arrives as a number
   * reads through the same helper, so a fractional instant is refused for the same reason a fractional width is.
   */
  @Test
  void refusesAFractionalOptionalMemberTheSameWay() {
    final JSONObject payload = new JSONObject();
    payload.put("from", 1_700_000_000_000.5);
    payload.put("maxDataPoints", 100.25);

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(payload, "from", Long.MIN_VALUE, "from"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'from' must be a whole number between ")
        .hasMessageEndingWith(": received 1.7000000000005E12");

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optInt(payload, "maxDataPoints", 0, "maxDataPoints"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'maxDataPoints' must be a whole number between ");
  }

  /**
   * A numeric STRING is still read as a number - the coercion the class javadoc promises and
   * {@code Issue7340RequiredMemberResolverTest} pins - but a fractional one is the same defect wearing quotes.
   */
  @Test
  void refusesAFractionalNumericStringAndKeepsAcceptingAWholeOne() {
    final JSONObject owner = new JSONObject();
    owner.put("bucketInterval", "1.5");

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation.bucketInterval' must be a whole number between " + Long.MIN_VALUE + " and "
            + Long.MAX_VALUE + ": received '1.5'");

    owner.put("bucketInterval", "5000");
    assertThat(TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
        .isEqualTo(5000L);
  }

  /**
   * A magnitude no long can hold is the same silent substitution seen from the other end: {@code longValue()}
   * saturates or wraps and the caller is answered for a number it did not send.
   */
  @Test
  void refusesAMagnitudeALongCannotHold() {
    final JSONObject owner = new JSONObject();
    owner.put("to", 1.0e20);

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(owner, "to", Long.MAX_VALUE, "to"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'to' must be a whole number between ");

    owner.put("to", "99999999999999999999");
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(owner, "to", Long.MAX_VALUE, "to"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'to' must be a whole number between ");
  }

  /**
   * NaN and the infinities are not numbers a millisecond instant can be, and they are what an arithmetic slip on
   * the client produces - a division by a zero span, most often. They are refused as a wrong TYPE rather than as
   * an out-of-range whole number, because neither has a decimal expansion to report.
   */
  @Test
  void refusesNaNAndTheInfinities() {
    // Written as text: the body parser is lenient, so these three spellings do reach a handler, while
    // JSONObject.put(String, Number) turns them into a JSON null and could not produce the case under test.
    for (final String spelling : new String[] { "NaN", "Infinity", "-Infinity" }) {
      final JSONObject owner = new JSONObject("{\"bucketInterval\":" + spelling + "}");
      assertThatThrownBy(
          () -> TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageStartingWith("'aggregation.bucketInterval' must be a number: received ");
    }
  }

  /**
   * An exponent no long can hold, in a thirteen-byte body. {@link java.math.BigDecimal#longValueExact()} decides
   * whether a value is too large with {@code (precision() - scale) > 19} in {@code int} arithmetic, which
   * OVERFLOWS for {@code 1E2147483647}: the guard passes and the method materialises a 2^31-digit integer.
   * {@code 1E-2147483647} is the mirror image, through {@code setScale(0)}. Both must be refused in constant
   * time (code review on PR #7730).
   * <p>
   * Bounded rather than merely asserted, because the failure mode is an allocation storm and not a wrong answer:
   * a test that only checked the refusal would "pass" by hanging the build first. The bound is measured with
   * {@link StallAwareStopwatch}, NOT with a raw wall clock - a stop-the-world pause late in a full-suite run
   * pauses the guarded code right along with any plain timer, which is the coin flip CLAUDE.md forbids (#6260).
   * It is a TRIPWIRE between a constant-time refusal and materialising hundreds of megabytes, so widening it is
   * free and only narrowing it could break. {@code @Timeout} sits above it as a hang detector, sized so that it
   * can only fire when the thing under test never returns at all.
   */
  @Test
  @Timeout(300)
  void refusesAnExtremeExponentWithoutMaterialisingIt() {
    for (final String spelling : new String[] { "1E2147483647", "1E-2147483647", "-1E2147483647",
        "1E+2147483646", "9E2147483647" }) {
      final JSONObject owner = new JSONObject().put("bucketInterval", spelling);

      // Either refusal is correct and both are constant-time: the JSON layer's own numeric limits turn an
      // extreme exponent away as "must be a number" before BigDecimal sees it, and the scale and precision
      // tests in readLong answer it as "must be a whole number" if it ever gets past them. Which one fires is
      // an implementation detail of a dependency; that it is refused without materialising the value is not.
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(
          () -> TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageStartingWith("'aggregation.bucketInterval' must be a ");
      stopwatch.assertGaveUpWithin(10_000,
          "a constant-time refusal of '" + spelling + "' from materialising the value its exponent names");
    }
  }

  /**
   * The same exponent as a JSON NUMBER rather than a string, which is the other way it arrives from a body the
   * parser read. Both directions are refused, and in constant time, because the value is read as the lexeme the
   * request carried rather than as the double {@code JSONObject.opt} would narrow it to.
   */
  @Test
  @Timeout(300)
  void refusesAnExtremeExponentWrittenAsAJsonNumber() {
    final JSONObject payload = new JSONObject("{\"bucketInterval\":1E2147483647,\"from\":1E-2147483647}");

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireLong(payload, "bucketInterval", "aggregation.bucketInterval"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'aggregation.bucketInterval' must be a ");

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(payload, "from", -1L, "from"))
        .as("10^-2147483647 is refused, not answered as the 0.0 a double would have made of it")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'from' must be a ");

    stopwatch.assertGaveUpWithin(10_000,
        "a constant-time refusal of an extreme exponent from materialising the value it names");
  }

  /**
   * The member is read as the LEXEME the request carried, not as the double {@code JSONObject.opt} narrows it to
   * (CodeRabbit on PR #7730). {@code elementToObject} converts any number whose text holds a {@code '.'} or an
   * exponent with {@code getAsDouble()}, so {@code 9007199254740993.0} would otherwise arrive here already
   * rounded to {@code 9007199254740992.0} and be accepted as a whole number ONE AWAY from the instant the caller
   * wrote - the very substitution this issue is about, arriving by a different door.
   * <p>
   * Reading the lexeme is better than the other way out, refusing every double past the safe-integer range: it
   * refuses nothing that is exact as written. {@code 9007199254740993.0} IS a whole number, and is answered as
   * one.
   */
  @Test
  void readsTheLexemeRatherThanTheDoubleTheParserWouldNarrowItTo() {
    final JSONObject payload = new JSONObject(
        "{\"from\":9007199254740993.0,\"to\":9007199254740993,\"bucketInterval\":1.00000000000000000001}");

    assertThat(TimeSeriesHandlerUtils.optLong(payload, "from", 0L, "from"))
        .as("written with a decimal point, so opt() would have rounded it to ...992").isEqualTo(9007199254740993L);
    assertThat(TimeSeriesHandlerUtils.optLong(payload, "to", 0L, "to")).isEqualTo(9007199254740993L);

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireLong(payload, "bucketInterval", "aggregation.bucketInterval"))
        .as("a fraction a double cannot even hold is still a fraction")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'aggregation.bucketInterval' must be a whole number between ");
  }

  /**
   * The same extreme exponent with a coefficient that HAS trailing zeros, which is a different code path:
   * {@code "100E2147483647"} is {@code (unscaled=100, scale=-2147483647)}, and {@code stripTrailingZeros()}
   * raises the scale to strip a zero - from a scale that is already at the bottom of its range, so it throws a
   * {@code ArithmeticException("Overflow")} of its own. Were it to run, that throw sits outside the resolver's
   * catch and would answer a malformed request with a 500 rather than the 400 the endpoints render an
   * {@code IllegalArgumentException} as (CodeRabbit on PR #7730).
   * <p>
   * What this pins is the REFUSAL, not which guard produces it. The JSON layer's own numeric limits turn an
   * exponent this extreme away before a {@code BigDecimal} carrying such a scale can exist, so today the strip
   * is never reached; the resolver guards it anyway, because its correctness may not rest on a dependency's
   * internal limit. Should that limit ever widen, this case must still be a client error - which is what the
   * assertion says. {@code 1E2147483647} does not exercise the same path: its coefficient has no trailing zero
   * to strip.
   */
  @Test
  @Timeout(300)
  void refusesAnExtremeExponentWhoseCoefficientHasTrailingZeros() {
    for (final String spelling : new String[] { "100E2147483647", "-100E2147483647", "10E2147483647",
        "100E-2147483647" }) {
      final JSONObject owner = new JSONObject().put("from", spelling);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(owner, "from", -1L, "from"))
          .as("'%s' must be refused as a client error, never as an ArithmeticException the mapper reads as 500",
              spelling)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageStartingWith("'from' must be a ");
      stopwatch.assertGaveUpWithin(10_000,
          "a constant-time refusal of '" + spelling + "' from materialising the value its exponent names");
    }
  }

  /**
   * The length cap on a numeric string is defence in depth against a pathological digit run, and nothing more.
   * It must not refuse a value the caller could legitimately mean: a zero-padded {@code 1} is a faithful
   * representation of {@code 1}, however many zeros it carries, and reading it as anything else - including as
   * "not a number" - is the substitution this whole issue is about (CodeRabbit on PR #7730).
   * <p>
   * The refusal, when it does fire, is reported as "must be a number" rather than with a message of its own,
   * because that is #7340's answer for a member that did not arrive as one and there is no reason for this
   * endpoint to have two.
   */
  @Test
  void refusesOnlyAPathologicalNumericStringAndReadsAZeroPaddedOne() {
    final JSONObject padded = new JSONObject();
    padded.put("from", "0".repeat(42) + "1");
    padded.put("to", "-" + "0".repeat(40) + "9223372036854775807");
    padded.put("bucketInterval", "1." + "0".repeat(60));

    assertThat(TimeSeriesHandlerUtils.optLong(padded, "from", -1L, "from"))
        .as("a zero-padded 1 names 1, whatever its length").isEqualTo(1L);
    assertThat(TimeSeriesHandlerUtils.optLong(padded, "to", 0L, "to")).isEqualTo(Long.MIN_VALUE + 1);
    assertThat(TimeSeriesHandlerUtils.requireLong(padded, "bucketInterval", "aggregation.bucketInterval"))
        .as("trailing zeros after the point do not make it fractional").isEqualTo(1L);

    final JSONObject pathological = new JSONObject().put("from", "1".repeat(5000));
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(pathological, "from", 0L, "from"))
        .as("an excessive run of SIGNIFICANT digits is still kept out of the parser")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'from' must be a number: received ");
  }

  /**
   * Zero survives the guards whatever scale it arrives with, including the scale that would make the checks
   * report it as fractional. {@code "from": 0} is an ordinary request.
   */
  @Test
  void keepsAcceptingZeroAtAnyScale() {
    final JSONObject owner = new JSONObject("{\"a\":0,\"b\":0.0,\"c\":\"0E-40\",\"d\":-0.000}");

    assertThat(TimeSeriesHandlerUtils.optLong(owner, "a", -1L, "a")).isZero();
    assertThat(TimeSeriesHandlerUtils.optLong(owner, "b", -1L, "b")).isZero();
    assertThat(TimeSeriesHandlerUtils.optLong(owner, "c", -1L, "c")).isZero();
    assertThat(TimeSeriesHandlerUtils.optLong(owner, "d", -1L, "d")).isZero();
  }

  /**
   * The value the narrowing could never have been caught by a double round-trip: {@code 2^53 + 1} is a whole
   * number a long holds exactly and a double does not, and it is squarely inside the range a millisecond instant
   * lives in once a client works in nanoseconds. Checking the EXACT decimal is what keeps it accepted.
   */
  @Test
  void keepsAcceptingAWholeNumberNoDoubleCanRepresent() {
    final JSONObject owner = new JSONObject();
    owner.put("from", 9007199254740993L);
    owner.put("to", Long.MAX_VALUE);
    owner.put("bucketInterval", 1.0);

    assertThat(TimeSeriesHandlerUtils.optLong(owner, "from", 0L, "from")).isEqualTo(9007199254740993L);
    assertThat(TimeSeriesHandlerUtils.optLong(owner, "to", 0L, "to")).isEqualTo(Long.MAX_VALUE);
    assertThat(TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
        .as("a JSON number written with a zero fractional part IS a whole number").isEqualTo(1L);
  }
}
