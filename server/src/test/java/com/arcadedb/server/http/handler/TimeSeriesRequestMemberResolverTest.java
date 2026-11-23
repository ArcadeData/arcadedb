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

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared resolvers both HTTP time-series endpoints read the members of a request through, so that the two answer
 * the same bad request the same way.
 */
class TimeSeriesRequestMemberResolverTest {
  /*
   * Issue #7325: the aggregation function both time-series HTTP endpoints read off the request used to reach
   * {@code AggregationType.valueOf} unguarded. This pins the shared resolver that replaced it - the resolver is what
   * makes the two endpoints answer the same bad request the same way, so its behaviour is tested once here and the
   * endpoint-specific rendering (a 400 body, a Grafana error frame) in the two handler ITs.
   */
  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  @Test
  void resolvesEveryDeclaredAggregationTypeByItsCanonicalName() {
    for (final AggregationType expected : AggregationType.values())
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request(expected.name()), 0)).isEqualTo(expected);
  }

  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  @Test
  void resolvesTheLowerCasedAndPaddedSpellingsAHandWrittenClientSends() {
    for (final AggregationType expected : AggregationType.values()) {
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request(expected.name().toLowerCase(Locale.ENGLISH)), 0))
          .isEqualTo(expected);
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request("  " + expected.name() + "  "), 0))
          .isEqualTo(expected);
    }
  }

  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  @Test
  void refusesAnUnknownNameNamingTheFieldAndEveryAcceptedValue() {
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request("MEDIAN"), 2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregation.requests[2].type")
        .hasMessageContaining("SUM, AVG, MIN, MAX, COUNT")
        .hasMessageContaining("MEDIAN");
  }

  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  /**
   * The accepted values are listed from {@link AggregationType#values()} so a constant added later cannot leave the
   * message advertising a stale set.
   */
  @Test
  void listsEveryDeclaredAggregationTypeInTheRefusal() {
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request("nope"), 0))
        .satisfies(e -> {
          for (final AggregationType type : AggregationType.values())
            assertThat(e).hasMessageContaining(type.name());
        });
  }

  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  @Test
  void refusesAnAbsentNullBlankOrNonStringTypeTheSameWay() {
    final JSONObject absent = new JSONObject();
    absent.put("field", "temperature");

    final JSONObject blank = request("   ");

    final JSONObject nonString = new JSONObject();
    nonString.put("field", "temperature");
    nonString.put("type", 7);

    for (final JSONObject request : new JSONObject[] { absent, blank, nonString })
      assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request, 0))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("aggregation.requests[0].type")
          .hasMessageContaining("is required");
  }

  // Regression: #7325 the aggregation type is resolved and refused by the shared resolver
  /**
   * The refusal echoes what arrived so the caller can see which of its requests was wrong, but a legal-yet-long
   * value must not turn the error body into a copy of the request.
   */
  @Test
  void truncatesAnOverlongEchoedValue() {
    final String overlong = "X".repeat(500);

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request(overlong), 0))
        .satisfies(e -> {
          assertThat(e.getMessage()).contains("...");
          assertThat(e.getMessage().length()).isLessThan(200);
        });
  }

  private static JSONObject request(final String type) {
    final JSONObject request = new JSONObject();
    request.put("field", "temperature");
    request.put("type", type);
    return request;
  }

  /*
   * Issue #7340: the required members of a time-series aggregation request other than {@code type} used to reach
   * {@link JSONObject}'s raising getters unguarded, so an absent or wrongly-typed one was answered as a bare
   * {@code 400 "Invalid JSON payload"} whose specifics production mode conceals in the {@code detail} field.
   * <p>
   * This pins the shared resolver family that replaced those reads - what makes the two endpoints name the same
   * member the same way. The endpoint-specific rendering (a 400 body, a Grafana error frame keyed by refId) is
   * pinned in the two handler ITs.
   */
  // Regression: #7340 an absent or wrongly typed required member is refused by name
  @Test
  void resolvesAMemberThatIsPresentAndOfTheRightType() {
    final JSONObject owner = new JSONObject();
    owner.put("obj", new JSONObject().put("k", "v"));
    owner.put("arr", new JSONArray().put("a"));
    owner.put("str", "hello");
    owner.put("num", 5000L);

    assertThat(TimeSeriesHandlerUtils.requireObject(owner, "obj", "obj").getString("k")).isEqualTo("v");
    assertThat(TimeSeriesHandlerUtils.requireArray(owner, "arr", "arr").length()).isEqualTo(1);
    assertThat(TimeSeriesHandlerUtils.requireString(owner, "str", "str")).isEqualTo("hello");
    assertThat(TimeSeriesHandlerUtils.requireLong(owner, "num", "num")).isEqualTo(5000L);
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * An absent member and an explicitly null one are the same client error: GSON models {@code "x": null} as a
   * present entry, so a presence-only guard lets it through to the converter and back out as a
   * {@code JSONException} (issue #5935's shape).
   */
  @Test
  void refusesAnAbsentOrExplicitlyNullMemberByName() {
    final JSONObject absent = new JSONObject();
    final JSONObject explicitNull = new JSONObject().put("bucketInterval", (Object) null);

    for (final JSONObject owner : new JSONObject[] { absent, explicitNull }) {
      assertThatThrownBy(
          () -> TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("'aggregation.bucketInterval' is required and must be a number");

      assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireArray(owner, "requests", "aggregation.requests"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("'aggregation.requests' is required and must be a JSON array");

      assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireString(owner, "field", "x.field"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("'x.field' is required and must be a string");

      assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireObject(owner, "aggregation", "aggregation"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("'aggregation' is required and must be a JSON object");
    }
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * A container that arrives as the wrong kind is reported BY KIND, never by content: echoing it back would let a
   * multi-megabyte payload into an error body that exists to be read.
   */
  @Test
  void refusesAWronglyTypedMemberNamingItAndDescribingWhatArrivedByKind() {
    final JSONObject owner = new JSONObject();
    owner.put("requests", new JSONObject().put("inner", "x".repeat(5000)));
    owner.put("aggregation", new JSONArray().put("x".repeat(5000)));

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireArray(owner, "requests", "aggregation.requests"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation.requests' must be a JSON array: received a JSON object");

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireObject(owner, "aggregation", "aggregation"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation' must be a JSON object: received a JSON array");
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * A primitive IS echoed, because seeing what arrived is how the caller finds the typo - but truncated, for the
   * same reason the aggregation-name refusal truncates (issue #7325).
   */
  @Test
  void truncatesAnOverlongEchoedPrimitive() {
    final JSONObject owner = new JSONObject().put("from", "x".repeat(5000));

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(owner, "from", Long.MIN_VALUE, "from"))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(e -> {
          assertThat(e.getMessage()).startsWith("'from' must be a number: received ").contains("...");
          assertThat(e.getMessage().length()).isLessThan(300);
        });
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * The helpers delegate to the matching {@link JSONObject} getter instead of re-deciding what is acceptable, so
   * the requests that used to succeed still do. These two coercions are the ones a hand-written client relies on
   * without knowing it.
   */
  @Test
  void keepsAcceptingWhatTheUnderlyingGettersAlwaysAccepted() {
    final JSONObject owner = new JSONObject();
    owner.put("bucketInterval", "5000");
    owner.put("type", 7);

    assertThat(TimeSeriesHandlerUtils.requireLong(owner, "bucketInterval", "aggregation.bucketInterval"))
        .as("a numeric string is still read as a number, as JSONObject.getLong always did").isEqualTo(5000L);
    assertThat(TimeSeriesHandlerUtils.requireString(owner, "type", "type"))
        .as("a JSON number is still rendered as its text, as JSONObject.getString always did").isEqualTo("7");
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * An optional member is only refused when it ARRIVES as something unusable; absent and explicitly null both
   * yield the default, which is what "optional" has to mean for a caller that sends {@code "tags": null}.
   */
  @Test
  void optionalMembersFallBackOnAbsentAndNullButRefuseAWrongType() {
    final JSONObject absent = new JSONObject();
    assertThat(TimeSeriesHandlerUtils.optLong(absent, "from", -1L, "from")).isEqualTo(-1L);
    assertThat(TimeSeriesHandlerUtils.optInt(absent, "maxDataPoints", 0, "maxDataPoints")).isZero();
    assertThat(TimeSeriesHandlerUtils.optString(absent, "refId", "A", "refId")).isEqualTo("A");

    final JSONObject nulls = new JSONObject();
    nulls.put("from", (Object) null);
    nulls.put("refId", (Object) null);
    assertThat(TimeSeriesHandlerUtils.optLong(nulls, "from", -1L, "from")).isEqualTo(-1L);
    assertThat(TimeSeriesHandlerUtils.optString(nulls, "refId", "A", "refId")).isEqualTo("A");

    final JSONObject wrong = new JSONObject();
    wrong.put("from", new JSONArray());
    wrong.put("refId", new JSONObject());
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optLong(wrong, "from", -1L, "from"))
        .hasMessage("'from' must be a number: received a JSON array");
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optString(wrong, "refId", "A", "targets[0].refId"))
        .hasMessage("'targets[0].refId' must be a string: received a JSON object");
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  @Test
  void refusesAnArrayElementOfTheWrongKindByItsIndexedPath() {
    final JSONArray requests = new JSONArray();
    requests.put("AVG");
    requests.put((Object) null);

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireObjectElement(requests, 0, "aggregation.requests[0]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation.requests[0]' must be a JSON object: received 'AVG'");

    assertThatThrownBy(
        () -> TimeSeriesHandlerUtils.requireObjectElement(requests, 1, "aggregation.requests[1]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'aggregation.requests[1]' is required and must be a JSON object");

    final JSONArray fields = new JSONArray();
    fields.put(new JSONObject());
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.requireStringElement(fields, 0, "targets[0].fields[0]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'targets[0].fields[0]' must be a string: received a JSON object");
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * Adversarial pass on this patch: {@code optInt} must not inherit {@code JSONObject.getInt}'s silent
   * {@code Number.intValue()} narrowing, which is the same trap {@code requireIntLimit} exists for on the
   * {@code limit} member - a {@code maxDataPoints} an int cannot hold would WRAP to a different number and the
   * caller would never be told.
   */
  @Test
  void refusesAnIntegerMemberThatWouldHaveToWrapRatherThanNarrowingIt() {
    final JSONObject owner = new JSONObject();
    owner.put("maxDataPoints", 4_294_967_296L);

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.optInt(owner, "maxDataPoints", 0, "maxDataPoints"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'maxDataPoints' must be an integer between " + Integer.MIN_VALUE + " and "
            + Integer.MAX_VALUE + ": received 4294967296");

    owner.put("maxDataPoints", 500);
    assertThat(TimeSeriesHandlerUtils.optInt(owner, "maxDataPoints", 0, "maxDataPoints")).isEqualTo(500);
  }

  // Regression: #7340 an absent or wrongly typed required member is refused by name
  /**
   * The Grafana endpoint nests its requests under a target, so the aggregation-name refusal #7325 introduced has
   * to name the longer path there. The path-free overload the {@code /ts/query} endpoint uses is unchanged, which
   * is why both spellings are pinned here.
   */
  @Test
  void namesTheAggregationTypeByWhicheverPathTheEndpointUses() {
    final JSONObject request = new JSONObject();
    request.put("field", "temperature");
    request.put("type", "MEDIAN");

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request, 0))
        .hasMessageStartingWith("'aggregation.requests[0].type' is required and must be one of ");

    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request,
        "targets[1].aggregation.requests[0].type"))
        .hasMessageStartingWith("'targets[1].aggregation.requests[0].type' is required and must be one of ");
  }

  /*
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
  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
  /**
   * A numeric STRING is still read as a number - the coercion the class javadoc promises and
   * the required-member tests of this class pin - but a fractional one is the same defect wearing quotes.
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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

  // Regression: #7715 an integral member with a fractional part is refused, not truncated
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
