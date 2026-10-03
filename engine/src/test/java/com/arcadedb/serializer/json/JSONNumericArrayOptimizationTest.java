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
package com.arcadedb.serializer.json;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code optimizeNumericArrays} fast path on {@link JSONObject#toMap(boolean)} and
 * {@link JSONArray#toList(boolean)} introduced as a follow-up to issue #3864 to avoid millions
 * of {@code Double} boxing allocations when receiving vector embeddings as JSON HTTP params.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JSONNumericArrayOptimizationTest {

  @Test
  void numericDoubleArrayBecomesPrimitiveDoubleArray() {
    final JSONObject obj = new JSONObject("{\"vector\":[1.5, 2.5, 3.5]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("vector")).isInstanceOf(double[].class);
    final double[] vec = (double[]) map.get("vector");
    assertThat(vec).containsExactly(1.5, 2.5, 3.5);
  }

  @Test
  void numericIntegerArrayBecomesPrimitiveLongArray() {
    final JSONObject obj = new JSONObject("{\"ids\":[1, 2, 3, 4]}");
    final Map<String, Object> map = obj.toMap(true);

    // Issue #4148: integer-only arrays are returned as long[] to preserve int64 precision;
    // downstream Type.convert handles long[] -> int[]/short[]/float[]/double[] when the
    // property type requires it.
    assertThat(map.get("ids")).isInstanceOf(long[].class);
    assertThat((long[]) map.get("ids")).containsExactly(1L, 2L, 3L, 4L);
  }

  @Test
  void mixedNumericAndStringArrayStaysAsList() {
    final JSONObject obj = new JSONObject("{\"mixed\":[1, \"two\", 3.0]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("mixed")).isInstanceOf(List.class);
    final List<?> list = (List<?>) map.get("mixed");
    assertThat((List<Object>) list).containsExactly(1, "two", 3.0);
  }

  @Test
  void emptyArrayStaysAsList() {
    final JSONObject obj = new JSONObject("{\"empty\":[]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("empty")).isInstanceOf(List.class);
    assertThat((List<?>) map.get("empty")).isEmpty();
  }

  @Test
  void nestedNumericArrayInsideObjectArrayIsOptimized() {
    final JSONObject obj = new JSONObject("{\"batch\":[{\"vector\":[1.0, 2.0]}, {\"vector\":[3.0, 4.0]}]}");
    final Map<String, Object> map = obj.toMap(true);

    final List<?> batch = (List<?>) map.get("batch");
    assertThat(batch).hasSize(2);
    final Map<?, ?> first = (Map<?, ?>) batch.get(0);
    assertThat(first.get("vector")).isInstanceOf(double[].class);
    assertThat((double[]) first.get("vector")).containsExactly(1.0, 2.0);
    final Map<?, ?> second = (Map<?, ?>) batch.get(1);
    assertThat(second.get("vector")).isInstanceOf(double[].class);
    assertThat((double[]) second.get("vector")).containsExactly(3.0, 4.0);
  }

  @Test
  void defaultToMapKeepsListOfDoubleForBackwardCompatibility() {
    final JSONObject obj = new JSONObject("{\"vector\":[1.5, 2.5, 3.5]}");
    final Map<String, Object> map = obj.toMap();

    assertThat(map.get("vector")).isInstanceOf(List.class);
    final List<?> list = (List<?>) map.get("vector");
    assertThat(list.get(0)).isInstanceOf(Double.class);
    assertThat((List<Object>) list).containsExactly(1.5, 2.5, 3.5);
  }

  @Test
  void scientificNotationParsedToDouble() {
    final JSONObject obj = new JSONObject("{\"v\":[1e3, 2e-2]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("v")).isInstanceOf(double[].class);
    assertThat((double[]) map.get("v")).containsExactly(1000.0, 0.02);
  }

  @Test
  void mixedDecimalAndIntegerInSameArrayStaysAsList() {
    // A primitive array would turn the integers into floating point values (issue #9003): the list keeps each element as written
    final JSONObject obj = new JSONObject("{\"v\":[1, 2.5, 3]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("v")).isInstanceOf(List.class);
    assertThat(map.get("v")).isEqualTo(List.of(1, 2.5, 3));
  }

  @Test
  void arrayWithNullElementIsNotOptimized() {
    final JSONObject obj = new JSONObject("{\"v\":[1.0, null, 3.0]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("v")).isInstanceOf(List.class);
  }
}
