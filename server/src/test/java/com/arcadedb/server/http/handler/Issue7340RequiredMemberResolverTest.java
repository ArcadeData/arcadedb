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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7340: the required members of a time-series aggregation request other than {@code type} used to reach
 * {@link JSONObject}'s raising getters unguarded, so an absent or wrongly-typed one was answered as a bare
 * {@code 400 "Invalid JSON payload"} whose specifics production mode conceals in the {@code detail} field.
 * <p>
 * This pins the shared resolver family that replaced those reads - what makes the two endpoints name the same
 * member the same way. The endpoint-specific rendering (a 400 body, a Grafana error frame keyed by refId) is
 * pinned in the two handler ITs.
 */
class Issue7340RequiredMemberResolverTest {

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
}
