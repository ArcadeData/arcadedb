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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntHashSetTest {

  @Test
  void emptySet() {
    final IntHashSet s = new IntHashSet();
    assertThat(s.size()).isZero();
    assertThat(s.isEmpty()).isTrue();
    assertThat(s.contains(42)).isFalse();
  }

  @Test
  void addAndContainsSingle() {
    final IntHashSet s = new IntHashSet();
    assertThat(s.add(42)).isTrue();
    assertThat(s.add(42)).isFalse();
    assertThat(s.contains(42)).isTrue();
    assertThat(s.contains(43)).isFalse();
    assertThat(s.size()).isEqualTo(1);
  }

  @Test
  void resizesAndKeepsAllValues() {
    final IntHashSet s = new IntHashSet(16);
    for (int i = 0; i < 10_000; i++)
      assertThat(s.add(i)).isTrue();
    assertThat(s.size()).isEqualTo(10_000);
    for (int i = 0; i < 10_000; i++)
      assertThat(s.contains(i)).isTrue();
    assertThat(s.contains(10_000)).isFalse();
  }

  @Test
  void clearResetsSize() {
    final IntHashSet s = new IntHashSet();
    for (int i = 0; i < 100; i++)
      s.add(i);
    s.clear();
    assertThat(s.size()).isZero();
    for (int i = 0; i < 100; i++)
      assertThat(s.contains(i)).isFalse();
  }

  @Test
  void rejectsSentinel() {
    final IntHashSet s = new IntHashSet();
    assertThatThrownBy(() -> s.add(Integer.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class);
    // contains of sentinel is safely false
    assertThat(s.contains(Integer.MIN_VALUE)).isFalse();
  }

  @Test
  void handlesNegativeValues() {
    final IntHashSet s = new IntHashSet();
    s.add(-1);
    s.add(Integer.MIN_VALUE + 1);
    s.add(Integer.MAX_VALUE);
    assertThat(s.contains(-1)).isTrue();
    assertThat(s.contains(Integer.MIN_VALUE + 1)).isTrue();
    assertThat(s.contains(Integer.MAX_VALUE)).isTrue();
    assertThat(s.size()).isEqualTo(3);
  }

  @Test
  void forEachVisitsAllValues() {
    final IntHashSet s = new IntHashSet();
    for (int i = 0; i < 100; i++)
      s.add(i * 7);

    final HashSet<Integer> seen = new HashSet<>();
    s.forEach(seen::add);
    assertThat(seen).hasSize(100);
    for (int i = 0; i < 100; i++)
      assertThat(seen).contains(i * 7);
  }

  @Test
  void toArrayReturnsAllValues() {
    final IntHashSet s = new IntHashSet();
    for (int i = 0; i < 50; i++)
      s.add(i);
    final int[] arr = s.toArray();
    assertThat(arr).hasSize(50);
    final HashSet<Integer> seen = new HashSet<>();
    for (final int v : arr)
      seen.add(v);
    assertThat(seen).hasSize(50);
  }

  @Test
  void equivalentToHashSetUnderRandomLoad() {
    // Cross-check against HashSet<Integer> for behavior parity.
    final IntHashSet a = new IntHashSet();
    final HashSet<Integer> b = new HashSet<>();
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < 10_000; i++) {
      final int v = rnd.nextInt(Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
      assertThat(a.add(v)).isEqualTo(b.add(v));
    }
    assertThat(a.size()).isEqualTo(b.size());
    for (final int v : b)
      assertThat(a.contains(v)).isTrue();
  }
}
