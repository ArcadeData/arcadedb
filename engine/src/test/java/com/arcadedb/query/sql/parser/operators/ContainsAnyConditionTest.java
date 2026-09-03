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
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.query.sql.parser.ContainsAnyCondition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli-(at)-arcadedata.com)
 */
class ContainsAnyConditionTest {
  @Test
  void test() {
    final ContainsAnyCondition op = new ContainsAnyCondition();

    assertThat(op.execute(null, null)).isFalse();
    assertThat(op.execute(null, "foo")).isFalse();

    final List<Object> left = new ArrayList<>();
    assertThat(op.execute(left, "foo")).isFalse();
    assertThat(op.execute(left, null)).isFalse();

    left.add("foo");
    left.add("bar");

    assertThat(op.execute(left, "foo")).isTrue();
    assertThat(op.execute(left, "bar")).isTrue();
    assertThat(op.execute(left, "fooz")).isFalse();

    left.add(null);
    assertThat(op.execute(left, null)).isTrue();
  }

  @Test
  void iterable() {
    final Iterable left = new Iterable() {
      private final List<Integer> ls = Arrays.asList(3, 1, 2);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final Iterable right = new Iterable() {
      private final List<Integer> ls = Arrays.asList(2, 3);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final ContainsAnyCondition op = new ContainsAnyCondition();
    assertThat(op.execute(left, right)).isTrue();
  }

  /**
   * Issue #7084: an array right-hand side (the result of split(), a parameter bound to a Java array) must be expanded into
   * its items exactly as a List is, instead of being compared as one opaque object against the collection.
   */
  @Test
  void arrayRightHandSide() {
    final ContainsAnyCondition op = new ContainsAnyCondition();
    final List<Object> left = Arrays.asList("a", "b", "c");

    assertThat(op.execute(left, new String[] { "a", "x" })).isTrue();
    assertThat(op.execute(left, new String[] { "x", "c" })).isTrue();
    assertThat(op.execute(left, new String[] { "x", "y" })).isFalse();
    assertThat(op.execute(left, new String[0])).isFalse();

    final List<Object> numbers = Arrays.asList(1, 2, 3);
    assertThat(op.execute(numbers, new int[] { 9, 2 })).isTrue();
    assertThat(op.execute(numbers, new int[] { 9, 8 })).isFalse();
    // A Java array on the left is expanded too
    assertThat(op.execute(new String[] { "a", "b" }, new String[] { "b" })).isTrue();
    assertThat(op.execute(new String[] { "a", "b" }, Arrays.asList("x", "a"))).isTrue();
  }

  /**
   * A byte[] is the BINARY scalar: it is the value being searched for, not a list of numbers to expand.
   */
  @Test
  void byteArrayRightHandSideStaysScalar() {
    final ContainsAnyCondition op = new ContainsAnyCondition();
    final byte[] binary = new byte[] { 1, 2 };

    assertThat(op.execute(Arrays.asList(1, 2, 3), binary)).isFalse();
    assertThat(op.execute(Arrays.asList("x", binary), binary)).isTrue();
  }

  /**
   * Both left-hand shapes must answer alike: the Collection branch used to compare with strict equals() while the
   * Iterable branch applied its own comparison, so [1,2,3] CONTAINSANY [2L] depended on the left operand's concrete type.
   */
  @Test
  void numericWideningIsIndependentOfLeftShape() {
    final ContainsAnyCondition op = new ContainsAnyCondition();
    final List<Object> collection = Arrays.asList(1, 2, 3);
    final Iterable<Object> iterable = collection::iterator;

    assertThat(op.execute(collection, Arrays.asList(2L))).isTrue();
    assertThat(op.execute(iterable, Arrays.asList(2L))).isTrue();
    assertThat(op.execute(collection, 2L)).isTrue();
    assertThat(op.execute(iterable, 2L)).isTrue();
    assertThat(op.execute(collection, 4L)).isFalse();
    assertThat(op.execute(iterable, 4L)).isFalse();
  }

  /**
   * The Iterable branch iterated the left operand only once, so a right-hand item after the first one could never be
   * found once the left iterator was exhausted.
   */
  @Test
  void iterableLeftIsRescannedForEveryRightItem() {
    final ContainsAnyCondition op = new ContainsAnyCondition();
    final Iterable<Object> left = Arrays.<Object>asList(1, 2)::iterator;

    assertThat(op.execute(left, Arrays.asList(9, 1))).isTrue();
    assertThat(op.execute(left, Arrays.asList(9, 8, 2))).isTrue();
    assertThat(op.execute(left, Arrays.asList(9, 8))).isFalse();
    assertThat(op.execute(Arrays.<Object>asList(1, 2).iterator(), Arrays.asList(9, 2))).isTrue();
  }

  @Test
  void issue1785() {
    final ContainsAnyCondition op = new ContainsAnyCondition();

    final List<Object> nullList = new ArrayList<>();
    nullList.add(null);

    assertThat(op.execute(nullList, nullList)).isTrue();
  }
}
