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

class LongHashSetTest {

  @Test
  void emptySet() {
    final LongHashSet s = new LongHashSet();
    assertThat(s.size()).isZero();
    assertThat(s.isEmpty()).isTrue();
    assertThat(s.contains(42L)).isFalse();
  }

  @Test
  void addAndContainsSingle() {
    final LongHashSet s = new LongHashSet();
    assertThat(s.add(42L)).isTrue();
    assertThat(s.add(42L)).isFalse();
    assertThat(s.contains(42L)).isTrue();
    assertThat(s.contains(43L)).isFalse();
    assertThat(s.size()).isEqualTo(1);
  }

  @Test
  void resizesAndKeepsAllValues() {
    final LongHashSet s = new LongHashSet(16);
    for (long i = 0; i < 10_000; i++)
      assertThat(s.add(i)).isTrue();
    assertThat(s.size()).isEqualTo(10_000);
    for (long i = 0; i < 10_000; i++)
      assertThat(s.contains(i)).isTrue();
    assertThat(s.contains(10_000L)).isFalse();
  }

  @Test
  void clearResetsSize() {
    final LongHashSet s = new LongHashSet();
    for (long i = 0; i < 100; i++)
      s.add(i);
    s.clear();
    assertThat(s.size()).isZero();
    for (long i = 0; i < 100; i++)
      assertThat(s.contains(i)).isFalse();
  }

  @Test
  void rejectsSentinel() {
    final LongHashSet s = new LongHashSet();
    assertThatThrownBy(() -> s.add(Long.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(s.contains(Long.MIN_VALUE)).isFalse();
  }

  @Test
  void handlesNegativeValuesAndExtremes() {
    final LongHashSet s = new LongHashSet();
    s.add(-1L);
    s.add(Long.MIN_VALUE + 1);
    s.add(Long.MAX_VALUE);
    assertThat(s.contains(-1L)).isTrue();
    assertThat(s.contains(Long.MIN_VALUE + 1)).isTrue();
    assertThat(s.contains(Long.MAX_VALUE)).isTrue();
    assertThat(s.size()).isEqualTo(3);
  }

  @Test
  void forEachVisitsAllValues() {
    final LongHashSet s = new LongHashSet();
    for (long i = 0; i < 100; i++)
      s.add(i * 7);

    final HashSet<Long> seen = new HashSet<>();
    s.forEach(seen::add);
    assertThat(seen).hasSize(100);
    for (long i = 0; i < 100; i++)
      assertThat(seen).contains(i * 7);
  }

  @Test
  void toArrayReturnsAllValues() {
    final LongHashSet s = new LongHashSet();
    for (long i = 0; i < 50; i++)
      s.add(i);
    final long[] arr = s.toArray();
    assertThat(arr).hasSize(50);
    final HashSet<Long> seen = new HashSet<>();
    for (final long v : arr)
      seen.add(v);
    assertThat(seen).hasSize(50);
  }

  @Test
  void equivalentToHashSetUnderRandomLoad() {
    final LongHashSet a = new LongHashSet();
    final HashSet<Long> b = new HashSet<>();
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < 10_000; i++) {
      final long v = rnd.nextLong(Long.MIN_VALUE + 1, Long.MAX_VALUE);
      assertThat(a.add(v)).isEqualTo(b.add(v));
    }
    assertThat(a.size()).isEqualTo(b.size());
    for (final long v : b)
      assertThat(a.contains(v)).isTrue();
  }
}
