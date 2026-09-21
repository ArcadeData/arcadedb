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
package com.arcadedb.index;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract of the one index-key equality policy, pinned directly rather than only through the three call sites
 * that read it ({@code DocumentIndexer}, {@code TransactionIndexContext.IndexKey} and its {@code ComparableKey}).
 * <p>
 * The point of the class is that array-valued key elements - a {@code BINARY} property's {@code byte[]}, a dense
 * vector's {@code float[]} - are compared by CONTENT, because they do not override {@code Object.equals()} and the
 * shallow {@code Arrays.equals} therefore compares them by identity. That is issue #7109 on one side of the index
 * path and issue #7881 on the other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IndexKeyEqualityTest {

  @Test
  void anArrayValuedElementIsComparedByContent() {
    final Object[] a = { new byte[] { 1, 2, 3 } };
    final Object[] b = { new byte[] { 1, 2, 3 } };

    assertThat(IndexKeyEquality.sameTuple(a, b)).isTrue();
    assertThat(IndexKeyEquality.hashTuple(a)).isEqualTo(IndexKeyEquality.hashTuple(b));
    assertThat(IndexKeyEquality.sameValue(a[0], b[0])).isTrue();
  }

  @Test
  void contentThatDiffersIsStillToldApart() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new byte[] { 1, 2, 3 } },
        new Object[] { new byte[] { 1, 2, 4 } })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new float[] { 1f, 2f } },
        new Object[] { new float[] { 1f, 2f, 3f } })).isFalse();
  }

  @Test
  void everyPrimitiveArrayTypeAnIndexKeyCanHoldIsCoveredNotJustObjectArrays() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new float[] { 1f, 2f } },
        new Object[] { new float[] { 1f, 2f } })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new int[] { 7 } }, new Object[] { new int[] { 7 } })).isTrue();
    // A NESTED Object[] element goes one level deeper still, which is what distinguishes deepEquals from equals.
    assertThat(IndexKeyEquality.sameTuple(new Object[] { new Object[] { new byte[] { 5 } } },
        new Object[] { new Object[] { new byte[] { 5 } } })).isTrue();
  }

  @Test
  void aScalarElementKeepsItsOwnEquality() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a", 1 }, new Object[] { "a", 1 })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a", 1 }, new Object[] { "a", 2 })).isFalse();
    // A List element is content-comparable on its own, which is why ComparableKey.compareTo's List arm never
    // disagreed with the shallow equality the way the array arm did.
    assertThat(IndexKeyEquality.sameTuple(new Object[] { List.of("x", "y") }, new Object[] { List.of("x", "y") }))
        .isTrue();
  }

  @Test
  void aLengthMismatchIsNotTheSameKey() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, new Object[] { "a", "b" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[0], new Object[0])).isTrue();
  }

  @Test
  void aNullTupleEqualsOnlyAnotherNullTupleAndANullElementOnlyAnotherNullElement() {
    assertThat(IndexKeyEquality.sameTuple(null, null)).isTrue();
    assertThat(IndexKeyEquality.sameTuple(null, new Object[] { "a" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, null)).isFalse();
    assertThat(IndexKeyEquality.hashTuple(null)).isZero();

    assertThat(IndexKeyEquality.sameTuple(new Object[] { null }, new Object[] { null })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { null }, new Object[] { "a" })).isFalse();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { "a" }, new Object[] { null })).isFalse();
    assertThat(IndexKeyEquality.sameValue(null, null)).isTrue();
  }

  @Test
  void equalTuplesHashEqually() {
    // The half of the contract a HashMap depends on: two tuples that compare equal must never land in different
    // buckets. Checked on the array-valued case, which is the one the shallow form got wrong.
    for (final Object[][] pair : new Object[][][] {
        { { new byte[] { 1, 2 } }, { new byte[] { 1, 2 } } },
        { { "a", new float[] { 3f } }, { "a", new float[] { 3f } } },
        { { null, new int[] { 4 } }, { null, new int[] { 4 } } } }) {
      assertThat(IndexKeyEquality.sameTuple(pair[0], pair[1])).isTrue();
      assertThat(IndexKeyEquality.hashTuple(pair[0])).isEqualTo(IndexKeyEquality.hashTuple(pair[1]));
    }
  }
}
