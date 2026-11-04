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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tables of a {@link RidHashSet} are allocated on the first add (#10582): an empty set must behave exactly like a filled one that was cleared.
 */
class RidHashSetTest {
  @Test
  void emptySetAnswersWithoutTables() {
    final RidHashSet set = new RidHashSet();
    assertThat(set.isEmpty()).isTrue();
    assertThat(set.size()).isZero();
    assertThat(set.contains(0, 0L)).isFalse();
    assertThat(set.contains(Integer.MIN_VALUE, Long.MIN_VALUE)).isFalse();
    set.clear();
    final List<String> seen = new ArrayList<>();
    set.forEach((b, o) -> seen.add(b + ":" + o));
    assertThat(seen).isEmpty();
  }

  @Test
  void firstAddAllocatesAndGrowsPastInitialCapacity() {
    final RidHashSet set = new RidHashSet(16);
    assertThat(set.add(3, 7L)).isTrue();
    assertThat(set.add(3, 7L)).isFalse();
    assertThat(set.contains(3, 7L)).isTrue();
    assertThat(set.contains(3, 8L)).isFalse();
    for (int i = 0; i < 1000; i++)
      set.add(i % 5, i);
    assertThat(set.size()).isEqualTo(1001);
    for (int i = 0; i < 1000; i++)
      assertThat(set.contains(i % 5, i)).isTrue();
    final int[] count = { 0 };
    set.forEach((b, o) -> count[0]++);
    assertThat(count[0]).isEqualTo(set.size());
    set.clear();
    assertThat(set.isEmpty()).isTrue();
    assertThat(set.contains(3, 7L)).isFalse();
    assertThat(set.add(3, 7L)).isTrue();
  }
}
