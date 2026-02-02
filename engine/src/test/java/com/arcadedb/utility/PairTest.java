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

import java.util.AbstractMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PairTest {

  @Test
  void constructorWithValues() {
    final Pair<String, Integer> pair = new Pair<>("key", 42);

    assertThat(pair.getFirst()).isEqualTo("key");
    assertThat(pair.getSecond()).isEqualTo(42);
  }

  @Test
  void constructorWithMapEntry() {
    final Map.Entry<String, Integer> entry = new AbstractMap.SimpleEntry<>("key", 42);
    final Pair<String, Integer> pair = new Pair<>(entry);

    assertThat(pair.getFirst()).isEqualTo("key");
    assertThat(pair.getSecond()).isEqualTo(42);
  }

  @Test
  void equalsWithSamePair() {
    final Pair<String, Integer> pair1 = new Pair<>("key", 42);
    final Pair<String, Integer> pair2 = new Pair<>("key", 42);

    assertThat(pair1).isEqualTo(pair2);
  }

  @Test
  void equalsWithDifferentFirst() {
    final Pair<String, Integer> pair1 = new Pair<>("key1", 42);
    final Pair<String, Integer> pair2 = new Pair<>("key2", 42);

    assertThat(pair1).isNotEqualTo(pair2);
  }

  @Test
  void equalsWithDifferentSecond() {
    final Pair<String, Integer> pair1 = new Pair<>("key", 42);
    final Pair<String, Integer> pair2 = new Pair<>("key", 43);

    assertThat(pair1).isNotEqualTo(pair2);
  }

  @Test
  void equalsWithNull() {
    final Pair<String, Integer> pair = new Pair<>("key", 42);
    assertThat(pair).isNotEqualTo(null);
  }

  @Test
  void equalsWithSameInstance() {
    final Pair<String, Integer> pair = new Pair<>("key", 42);
    assertThat(pair).isEqualTo(pair);
  }

  @Test
  void equalsWithDifferentType() {
    final Pair<String, Integer> pair = new Pair<>("key", 42);
    assertThat(pair).isNotEqualTo("not a pair");
  }

  @Test
  void hashCodeConsistency() {
    final Pair<String, Integer> pair1 = new Pair<>("key", 42);
    final Pair<String, Integer> pair2 = new Pair<>("key", 42);

    assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode());
  }

  @Test
  void toStringFormat() {
    final Pair<String, Integer> pair = new Pair<>("key", 42);
    final String str = pair.toString();

    assertThat(str).contains("key");
    assertThat(str).contains("42");
    assertThat(str).startsWith("<");
    assertThat(str).endsWith(">");
  }

  @Test
  void compareToWithEqualPairs() {
    final Pair<String, Integer> pair1 = new Pair<>("abc", 42);
    final Pair<String, Integer> pair2 = new Pair<>("abc", 42);

    assertThat(pair1.compareTo(pair2)).isEqualTo(0);
  }

  @Test
  void compareToWithDifferentFirst() {
    final Pair<String, Integer> pair1 = new Pair<>("abc", 42);
    final Pair<String, Integer> pair2 = new Pair<>("def", 42);

    assertThat(pair1.compareTo(pair2)).isLessThan(0);
    assertThat(pair2.compareTo(pair1)).isGreaterThan(0);
  }

  @Test
  void compareToWithDifferentSecond() {
    final Pair<Integer, Integer> pair1 = new Pair<>(1, 10);
    final Pair<Integer, Integer> pair2 = new Pair<>(1, 20);

    assertThat(pair1.compareTo(pair2)).isLessThan(0);
    assertThat(pair2.compareTo(pair1)).isGreaterThan(0);
  }

  @Test
  void compareToFirstTakesPrecedence() {
    final Pair<String, Integer> pair1 = new Pair<>("aaa", 100);
    final Pair<String, Integer> pair2 = new Pair<>("bbb", 1);

    // First value takes precedence
    assertThat(pair1.compareTo(pair2)).isLessThan(0);
  }
}
