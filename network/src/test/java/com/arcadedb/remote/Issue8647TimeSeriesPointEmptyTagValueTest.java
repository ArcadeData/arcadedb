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
package com.arcadedb.remote;

import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8647: the server refuses a tag with an empty value over both protocols (a malformed line over HTTP,
 * INVALID_ARGUMENT over gRPC). {@link TimeSeriesPoint} is the boundary both remote clients build their request from,
 * so it refuses the value there, where the caller can still see which tag it was, instead of sending it to be dropped.
 */
class Issue8647TimeSeriesPointEmptyTagValueTest {

  @Test
  void anEmptyTagValueIsRefused() {
    assertThatThrownBy(() -> new TimeSeriesPoint("cpu", 1L, Map.of("host", ""), Map.of("value", 1.0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host")
        .hasMessageContaining("empty value");
  }

  @Test
  void aNullTagValueIsStillAnAbsentTag() {
    final Map<String, Object> tags = new HashMap<>();
    tags.put("host", null);

    final TimeSeriesPoint point = new TimeSeriesPoint("cpu", 1L, tags, Map.of("value", 1.0));
    assertThat(point.tags()).containsKey("host");
  }

  @Test
  void aNonEmptyTagValueIsAccepted() {
    final TimeSeriesPoint point = new TimeSeriesPoint("cpu", 1L, Map.of("host", " "), Map.of("value", 1.0));
    assertThat(point.tags()).containsEntry("host", " ");
  }
}
