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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@code start}/{@code end} validation of {@code GET .../prom/api/v1/query_range}
 * (issue #6807). The handler used to hand {@code (long) (Double.parseDouble(v) * 1000)} straight to the
 * evaluator, so {@code start=-9e15}/{@code end=9e15} produced a span that overflowed 64 bits, and a
 * non-numeric value produced a 500 instead of a 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6807PromQLRangeParameterTest {

  @Test
  void parsesPlainAndFractionalEpochSeconds() {
    assertThat(GetPromQLQueryRangeHandler.parseTimestampMs("start", "1700000000")).isEqualTo(1_700_000_000_000L);
    assertThat(GetPromQLQueryRangeHandler.parseTimestampMs("end", "1700000000.5")).isEqualTo(1_700_000_000_500L);
    assertThat(GetPromQLQueryRangeHandler.parseTimestampMs("start", "-1")).isEqualTo(-1_000L);
  }

  @Test
  void rejectsTheOutOfRangeValuesThatOverflowedTheSpan() {
    // The repro from the issue: 9e15 seconds is 9e18 ms, and the span across +/-9e15 exceeds Long.MAX_VALUE.
    assertThatThrownBy(() -> GetPromQLQueryRangeHandler.parseTimestampMs("end", "9e15"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside the supported epoch range");
    assertThatThrownBy(() -> GetPromQLQueryRangeHandler.parseTimestampMs("start", "-9e15"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside the supported epoch range");
  }

  @Test
  void rejectsNonFiniteValues() {
    assertThatThrownBy(() -> GetPromQLQueryRangeHandler.parseTimestampMs("start", "Infinity"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not finite");
    assertThatThrownBy(() -> GetPromQLQueryRangeHandler.parseTimestampMs("end", "NaN"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not finite");
  }

  @Test
  void rejectsNonNumericValuesAsBadRequestRatherThanServerError() {
    assertThatThrownBy(() -> GetPromQLQueryRangeHandler.parseTimestampMs("start", "yesterday"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid start timestamp");
  }
}
