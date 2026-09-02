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

import com.arcadedb.query.sql.parser.ContainsCondition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
class ContainsConditionTest {
  @Test
  void test() {
    final ContainsCondition op = new ContainsCondition();

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
    final Iterable<?> left = new Iterable<>() {
      private final List<Integer> ls = Arrays.asList(3, 1, 2);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final Iterable<?> right = new Iterable<>() {
      private final List<Integer> ls = Arrays.asList(2, 3);

      @Override
      public Iterator iterator() {
        return ls.iterator();
      }
    };

    final ContainsCondition op = new ContainsCondition();
    assertThat(op.execute(left, right)).isTrue();
  }

  @Test
  void issue1785() {
    final ContainsCondition op = new ContainsCondition();

    final List<Object> nullList = new ArrayList<>();
    nullList.add(null);

    assertThat(op.execute(nullList, nullList)).isTrue();
  }

  /**
   * Regression test for issue #6984: a String[] left-hand side - such as the result of split() - satisfies
   * neither `instanceof Collection` nor `instanceof Iterable` (arrays don't implement Iterable), so it used
   * to fall through to the final `return false` instead of being matched against the scalar/collection on
   * the right.
   */
  @Test
  void issue6984ArrayLeftHandSide() {
    final ContainsCondition op = new ContainsCondition();

    final String[] left = "a b c".split(" ");

    assertThat(op.execute(left, "a")).isTrue();
    assertThat(op.execute(left, "z")).isFalse();

    final int[] primitiveLeft = { 1, 2, 3 };
    assertThat(op.execute(primitiveLeft, 2)).isTrue();
    assertThat(op.execute(primitiveLeft, 5)).isFalse();
  }

  /**
   * Regression test for issue #6995: the same blind spot as #6984, but on the other side of the operator. An array
   * right-hand side - the result of a second split(), a parameter bound to a Java array - satisfies neither
   * {@code instanceof Collection} nor {@code instanceof Iterable}, so it used to skip every branch of the operator
   * and be compared as one opaque object against each left-hand item: the condition was effectively always false.
   * <p>
   * The fix is a normalization, not a change of meaning: an array right-hand side now answers exactly what the
   * equivalent List right-hand side answers, which is the operator's established semantics - a single-item
   * collection is unwrapped to its item (the one-row sub-query case), and a longer one is looked for as one element
   * of the left-hand collection, as {@code SelectStatementExecutionTest.containsCollection} pins down.
   */
  @Test
  void issue6995ArrayRightHandSide() {
    final ContainsCondition op = new ContainsCondition();

    final String[] left = "a b c".split(" ");

    // Single-item array: unwrapped to its item, so the item is looked for among the left-hand values. This is the
    // case that used to be false for an array and true for the identical List.
    assertThat(op.execute(left, "a".split(" "))).isTrue();
    assertThat(op.execute(left, "z".split(" "))).isFalse();
    assertThat(op.execute(new int[] { 1, 2, 3 }, new int[] { 2 })).isTrue();
    assertThat(op.execute(new int[] { 1, 2, 3 }, new int[] { 9 })).isFalse();

    // Longer array: looked for as one element of the left-hand collection, so it matches a nested value.
    assertThat(op.execute(List.of(List.of("a", "b")), "a b".split(" "))).isTrue();
    assertThat(op.execute(List.of(List.of("a", "b")), "a z".split(" "))).isFalse();
  }

  /**
   * The point of the #6995 fix: an array right-hand side must answer whatever the identical List right-hand side
   * answers. Anything else leaves the operator's meaning depending on how the value happened to be produced.
   */
  @Test
  void anArrayRightHandSideAnswersTheSameAsTheEquivalentList() {
    final ContainsCondition op = new ContainsCondition();

    final List<Object> flat = Arrays.asList("a", "b", "c");
    final List<Object> nested = List.of(List.of("a", "b"), List.of("z"));

    for (final Object left : List.of(flat, nested)) {
      for (final List<String> right : List.of(List.of("a"), List.of("z"), List.of("a", "b"), List.of("b", "c"))) {
        final String[] asArray = right.toArray(new String[0]);
        assertThat(op.execute(left, asArray))
            .as("CONTAINS %s must answer the same for the array and for the List", right)
            .isEqualTo(op.execute(left, right));
      }
    }
  }

  /**
   * A byte[] is the BINARY scalar type, not a multi-value: {@code list CONTAINS :binary} must keep looking for that
   * binary value among the list items rather than degenerating into a byte-by-byte test (issue #6995).
   */
  @Test
  void binaryRightHandSideStaysAScalar() {
    final ContainsCondition op = new ContainsCondition();

    final List<Object> left = new ArrayList<>();
    left.add(new byte[] { 1, 2, 3 });

    assertThat(op.execute(left, new byte[] { 1, 2, 3 })).isTrue();
    assertThat(op.execute(left, new byte[] { 1, 2 })).isFalse();
    // Proof it is not being expanded: a single byte of the stored value is not "contained" in the list.
    assertThat(op.execute(left, (byte) 1)).isFalse();
  }
}
