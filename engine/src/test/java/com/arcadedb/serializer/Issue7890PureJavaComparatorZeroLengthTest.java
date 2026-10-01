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
package com.arcadedb.serializer;

import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7890: {@code PureJavaComparator.equals(left, right, length)} tested the last byte first
 * with no guard for {@code length == 0}, so it read index -1 and threw {@code ArrayIndexOutOfBoundsException}. That is
 * the #6998 defect one frame below the guard #6998 added to the two-argument {@code BinaryComparator.equalsBytes}: the
 * three-argument {@code equalsBytes} and {@code equalsBinary} forward a zero length straight to the selected
 * comparator, which is the pure-Java one whenever the VarHandle implementation is unavailable.
 */
class Issue7890PureJavaComparatorZeroLengthTest {

  @Test
  void pureJavaComparatorTreatsZeroLengthAsEqual() {
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[0], new byte[0], 0))
        .as("two empty arrays compared over length 0 are equal; the last-byte fast path used to read index -1")
        .isTrue();

    // Length 0 over non-empty arrays compares nothing, so it is equal too - the VarHandle implementation agrees.
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1 }, new byte[] { 2 }, 0)).isTrue();
    assertThat(UnsignedBytesComparator.BEST_COMPARATOR.equals(new byte[] { 1 }, new byte[] { 2 }, 0)).isTrue();

    // Non-zero lengths keep their behavior.
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 2 }, 2)).isTrue();
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 }, 2)).isFalse();
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 2, 2 }, 2)).isFalse();
    assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 }, 1)).isTrue();
  }

  @Test
  void threeArgumentEqualsBytesTreatsZeroLengthAsEqual() {
    assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[0], 0)).isTrue();
  }

  @Test
  void twoEmptyBinaryValuesAreEqual() {
    assertThat(BinaryComparator.equalsBinary(new Binary(new byte[0]), new Binary(new byte[0]))).isTrue();
    assertThat(new Binary(new byte[0]).equals(new Binary(new byte[0]))).isTrue();
    assertThat(BinaryComparator.equals(new Binary(new byte[0]), new Binary(new byte[0]))).isTrue();

    // A size mismatch is still caught before the comparator.
    assertThat(BinaryComparator.equalsBinary(new Binary(new byte[0]), new Binary(new byte[] { 1 }))).isFalse();
  }
}
