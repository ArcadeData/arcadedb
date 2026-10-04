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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.arcadedb.database.DocumentValidator.sameReadonlyValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9023: the content equality the READONLY check uses, branch by branch.
 */
class DocumentValidatorSameReadonlyValueTest {

  @Test
  void nulls() {
    assertThat(sameReadonlyValue(null, null)).isTrue();
    assertThat(sameReadonlyValue(null, 1)).isFalse();
    assertThat(sameReadonlyValue(new byte[0], null)).isFalse();
  }

  @Test
  void primitiveArraysCompareByContentAndComponentType() {
    assertThat(sameReadonlyValue(new byte[] { 1, 2 }, new byte[] { 1, 2 })).isTrue();
    assertThat(sameReadonlyValue(new byte[] { 1, 2 }, new byte[] { 1, 3 })).isFalse();
    assertThat(sameReadonlyValue(new int[] { 1, 2 }, new long[] { 1, 2 })).isFalse();
    // -0.0 and 0.0 stay distinct: a READONLY double must not change sign
    assertThat(sameReadonlyValue(new double[] { 0.0d }, new double[] { -0.0d })).isFalse();
    assertThat(sameReadonlyValue(-0.0d, 0.0d)).isFalse();
  }

  @Test
  void objectArraysCompareElementByElementWithNestedPrimitiveArrays() {
    assertThat(sameReadonlyValue(new Object[] { "a", new byte[] { 1 } }, new Object[] { "a", new byte[] { 1 } })).isTrue();
    assertThat(sameReadonlyValue(new Object[] { "a", new byte[] { 1 } }, new Object[] { "a", new byte[] { 2 } })).isFalse();
    assertThat(sameReadonlyValue(new Object[] { "a" }, new Object[] { "a", "b" })).isFalse();
    assertThat(sameReadonlyValue(new String[] { "a", "b" }, new String[] { "a", "b" })).isTrue();
    assertThat(sameReadonlyValue(new Object[] { null }, new Object[] { null })).isTrue();
  }

  @Test
  void anArrayIsNeverTheSameAsANonArray() {
    assertThat(sameReadonlyValue(new Object[] { 1, 2 }, List.of(1, 2))).isFalse();
    assertThat(sameReadonlyValue(List.of(1, 2), new int[] { 1, 2 })).isFalse();
    assertThat(sameReadonlyValue(new byte[] { 1 }, (byte) 1)).isFalse();
  }

  @Test
  void listsCompareElementByElement() {
    final List<Object> a = new ArrayList<>(List.of("x"));
    a.add(new short[] { 1 });
    final List<Object> b = new ArrayList<>(List.of("x"));
    b.add(new short[] { 1 });
    assertThat(sameReadonlyValue(a, b)).isTrue();
    b.set(1, new short[] { 2 });
    assertThat(sameReadonlyValue(a, b)).isFalse();
    assertThat(sameReadonlyValue(a, List.of("x"))).isFalse();
  }

  @Test
  void mapsCompareKeySetsAndValues() {
    final Map<String, Object> a = new HashMap<>();
    a.put("k", new float[] { 1f });
    a.put("n", null);
    final Map<String, Object> b = new HashMap<>();
    b.put("k", new float[] { 1f });
    b.put("n", null);
    assertThat(sameReadonlyValue(a, b)).isTrue();

    // same size, a different key holding null
    final Map<String, Object> otherKey = new HashMap<>();
    otherKey.put("k", new float[] { 1f });
    otherKey.put("m", null);
    assertThat(sameReadonlyValue(a, otherKey)).isFalse();

    b.put("k", new float[] { 2f });
    assertThat(sameReadonlyValue(a, b)).isFalse();
    b.remove("n");
    assertThat(sameReadonlyValue(a, b)).isFalse();
  }

  @Test
  void otherValuesUseEquals() {
    assertThat(sameReadonlyValue("a", "a")).isTrue();
    assertThat(sameReadonlyValue(1, 1L)).isFalse();
    assertThat(sameReadonlyValue(new RID(3, 4), new RID(3, 4))).isTrue();
  }
}
