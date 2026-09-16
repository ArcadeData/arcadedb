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
import org.junit.jupiter.api.Test;

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
