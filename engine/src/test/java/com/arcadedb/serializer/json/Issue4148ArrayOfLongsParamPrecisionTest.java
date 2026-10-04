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
 * Regression test for issue #4148: HTTP parameter binding loses int64 precision for ARRAY_OF_LONGS.
 * <p>
 * Before the fix, {@link JSONArray#toPrimitiveNumericArrayOrNull()} returned a {@code float[]} for
 * every homogeneous numeric JSON array. For int64 values larger than 2^24, that narrowing collapsed
 * distinct longs onto identical float bits (e.g. both {@code 1000000000000} and
 * {@code 1000000000001} mapped to the same float, then back to {@code 999999995904}).
 * After the fix, integer-only arrays are returned as {@code long[]} so the round trip preserves
 * every bit, while fractional / scientific arrays continue to take the {@code double[]} fast path
 * used by vector embeddings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4148ArrayOfLongsParamPrecisionTest {

  @Test
  void integerOnlyArrayPreservesInt64Precision() {
    // Two consecutive int64 values that differ by 1 must come back distinct.
    final JSONObject obj = new JSONObject("{\"arr\":[1000000000000, 1000000000001, 9999999999999]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("arr")).isInstanceOf(long[].class);
    final long[] arr = (long[]) map.get("arr");
    assertThat(arr).containsExactly(1000000000000L, 1000000000001L, 9999999999999L);
  }

  @Test
  void boundaryInt64ValuesPreserved() {
    final JSONObject obj = new JSONObject("{\"arr\":[" + Long.MAX_VALUE + "," + Long.MIN_VALUE + ",0,-1,1]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("arr")).isInstanceOf(long[].class);
    assertThat((long[]) map.get("arr")).containsExactly(Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L, 1L);
  }

  @Test
  void fractionalArrayKeepsDoubleFastPath() {
    // Vector embeddings: fractional values must continue to take the double[] fast path so we
    // do not regress the issue #3864 follow-up optimization.
    final JSONObject obj = new JSONObject("{\"vector\":[1.5, 2.5, 3.5]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("vector")).isInstanceOf(double[].class);
    assertThat((double[]) map.get("vector")).containsExactly(1.5, 2.5, 3.5);
  }

  @Test
  void scientificNotationKeepsDoubleFastPath() {
    // 1e3 is integer-valued numerically but the textual form has 'e', so we treat it as float.
    // This matches what JSON callers expect: an explicit decimal/exponent means floating point.
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
  void smallIntegerArrayPreservedAsLongArray() {
    // Ordinary integer arrays (e.g. ids) should also come back as long[] now.
    final JSONObject obj = new JSONObject("{\"ids\":[1, 2, 3, 4]}");
    final Map<String, Object> map = obj.toMap(true);

    assertThat(map.get("ids")).isInstanceOf(long[].class);
    assertThat((long[]) map.get("ids")).containsExactly(1L, 2L, 3L, 4L);
  }
}
