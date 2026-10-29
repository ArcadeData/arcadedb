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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10908: a LocalDate query parameter goes on the wire as a typed {@code $date} marker, not as a number.
 */
class Issue10908EncodeParametersTest {
  private static final LocalDate DAY = LocalDate.of(2024, 1, 15);

  @Test
  void localDateBecomesAMarker() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("d", DAY);
    in.put("n", 5);
    final Map<String, Object> out = RemoteHttpComponent.encodeParameters(in);
    assertThat(out.get("d")).isEqualTo(Map.of("$date", "2024-01-15"));
    assertThat(out.get("n")).isEqualTo(5);
    assertThat(in.get("d")).isEqualTo(DAY);
  }

  @Test
  void localDatesNestedInACollectionOrMap() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("l", List.of(1, DAY));
    in.put("m", Map.of("k", DAY));
    final Map<String, Object> out = RemoteHttpComponent.encodeParameters(in);
    assertThat(out.get("l")).isEqualTo(List.of(1, Map.of("$date", "2024-01-15")));
    assertThat(out.get("m")).isEqualTo(Map.of("k", Map.of("$date", "2024-01-15")));
  }

  @Test
  void parametersWithoutADateAreReturnedAsIs() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("a", "x");
    in.put("l", List.of(1, 2));
    assertThat(RemoteHttpComponent.encodeParameters(in)).isSameAs(in);
  }
}
