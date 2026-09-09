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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7325: the aggregation function both time-series HTTP endpoints read off the request used to reach
 * {@code AggregationType.valueOf} unguarded. This pins the shared resolver that replaced it - the resolver is what
 * makes the two endpoints answer the same bad request the same way, so its behaviour is tested once here and the
 * endpoint-specific rendering (a 400 body, a Grafana error frame) in the two handler ITs.
 */
class Issue7325AggregationTypeResolverTest {

  @Test
  void resolvesEveryDeclaredAggregationTypeByItsCanonicalName() {
    for (final AggregationType expected : AggregationType.values())
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request(expected.name()), 0)).isEqualTo(expected);
  }

  @Test
  void resolvesTheLowerCasedAndPaddedSpellingsAHandWrittenClientSends() {
    for (final AggregationType expected : AggregationType.values()) {
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request(expected.name().toLowerCase()), 0))
          .isEqualTo(expected);
      assertThat(TimeSeriesHandlerUtils.resolveAggregationType(request("  " + expected.name() + "  "), 0))
          .isEqualTo(expected);
    }
  }

  @Test
  void refusesAnUnknownNameNamingTheFieldAndEveryAcceptedValue() {
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.resolveAggregationType(request("MEDIAN"), 2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("aggregation.requests[2].type")
        .hasMessageContaining("SUM, AVG, MIN, MAX, COUNT")
        .hasMessageContaining("MEDIAN");
  }

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
}
