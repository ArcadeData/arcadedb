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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for ByteArrayComparator implementations.
 * Tests correctness, edge cases, and consistency across all comparator types.
 */
class ByteArrayComparatorTest {

  @Test
  void bestComparatorIsVarHandle() {
    // Verify that VarHandleComparator is selected as BEST_COMPARATOR on Java 9+
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;
    assertThat(comparator).isInstanceOf(UnsignedBytesComparator.VarHandleComparator.class);
    assertThat(comparator.toString()).contains("VarHandle");
  }

  @Test
  void emptyArrays() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;
    final byte[] empty1 = new byte[0];
    final byte[] empty2 = new byte[0];

    assertThat(comparator.compare(empty1, empty2)).isEqualTo(0);
    assertThat(comparator.equals(empty1, empty2, 0)).isTrue();
  }

  @Test
  void singleByte() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    assertThat(comparator.compare(new byte[]{1}, new byte[]{1})).isEqualTo(0);
    assertThat(comparator.compare(new byte[]{1}, new byte[]{2})).isLessThan(0);
    assertThat(comparator.compare(new byte[]{2}, new byte[]{1})).isGreaterThan(0);
  }

  @Test
  void unsignedComparison() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Test unsigned semantics: 0xFF (255) > 0x7F (127)
    assertThat(comparator.compare(new byte[]{(byte) 0xFF}, new byte[]{0x7F})).isGreaterThan(0);
    assertThat(comparator.compare(new byte[]{0x7F}, new byte[]{(byte) 0xFF})).isLessThan(0);

    // Test unsigned semantics: 0x80 (-128 signed) > 0x7F (127)
    assertThat(comparator.compare(new byte[]{(byte) 0x80}, new byte[]{0x7F})).isGreaterThan(0);
  }

  @Test
  void differentLengths() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Shorter array is less if common prefix is equal
    assertThat(comparator.compare(new byte[]{1, 2}, new byte[]{1, 2, 3})).isLessThan(0);
    assertThat(comparator.compare(new byte[]{1, 2, 3}, new byte[]{1, 2})).isGreaterThan(0);

    // Different at first byte
    assertThat(comparator.compare(new byte[]{2}, new byte[]{1, 2, 3})).isGreaterThan(0);
    assertThat(comparator.compare(new byte[]{1}, new byte[]{2, 3, 4})).isLessThan(0);
  }

  @Test
  void eightByteAlignment() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Test arrays that are exactly 8 bytes (one full stride)
    final byte[] a = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final byte[] b = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final byte[] c = new byte[]{1, 2, 3, 4, 5, 6, 7, 9};

    assertThat(comparator.compare(a, b)).isEqualTo(0);
    assertThat(comparator.compare(a, c)).isLessThan(0);
    assertThat(comparator.compare(c, a)).isGreaterThan(0);
  }

  @Test
  void multipleStrides() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Test arrays larger than 8 bytes (multiple strides)
    final byte[] a = new byte[20];
    final byte[] b = new byte[20];
    final byte[] c = new byte[20];

    for (int i = 0; i < 20; i++) {
      a[i] = (byte) i;
      b[i] = (byte) i;
      c[i] = (byte) i;
    }
    c[15] = 99; // Difference in second stride

    assertThat(comparator.compare(a, b)).isEqualTo(0);
    assertThat(comparator.compare(a, c)).isLessThan(0);
    assertThat(comparator.compare(c, a)).isGreaterThan(0);
  }

  @Test
  void nonAlignedArrays() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Test arrays that are not multiples of 8
    final byte[] a = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    final byte[] b = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    final byte[] c = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 10};

    assertThat(comparator.compare(a, b)).isEqualTo(0);
    assertThat(comparator.compare(a, c)).isLessThan(0);
  }

  @Test
  void allByteValues() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Test all possible byte values (0-255) in unsigned order
    for (int i = 0; i < 256; i++) {
      for (int j = 0; j < 256; j++) {
        final byte[] a = new byte[]{(byte) i};
        final byte[] b = new byte[]{(byte) j};
        final int result = comparator.compare(a, b);

        if (i < j)
          assertThat(result).isLessThan(0);
        else if (i > j)
          assertThat(result).isGreaterThan(0);
        else
          assertThat(result).isEqualTo(0);
      }
    }
  }

  @Test
  void equalsMethod() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    final byte[] a = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final byte[] b = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    final byte[] c = new byte[]{1, 2, 3, 4, 9, 6, 7, 8};

    assertThat(comparator.equals(a, b, 8)).isTrue();
    assertThat(comparator.equals(a, c, 8)).isFalse();

    // Test partial comparison
    assertThat(comparator.equals(a, c, 4)).isTrue(); // First 4 bytes are equal
    assertThat(comparator.equals(a, c, 5)).isFalse(); // 5th byte differs
  }

  @Test
  void reflexivity() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;
    final Random random = new Random(42);

    for (int i = 0; i < 100; i++) {
      final byte[] array = new byte[random.nextInt(50) + 1];
      random.nextBytes(array);

      // x.compare(x) == 0
      assertThat(comparator.compare(array, array)).isEqualTo(0);
    }
  }

  @Test
  void symmetry() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;
    final Random random = new Random(42);

    for (int i = 0; i < 100; i++) {
      final byte[] a = new byte[random.nextInt(50) + 1];
      final byte[] b = new byte[random.nextInt(50) + 1];
      random.nextBytes(a);
      random.nextBytes(b);

      final int ab = comparator.compare(a, b);
      final int ba = comparator.compare(b, a);

      // compare(a,b) == -compare(b,a)
      assertThat(ab).isEqualTo(-ba);
    }
  }

  @Test
  void transitivity() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    final byte[] a = new byte[]{1, 2, 3};
    final byte[] b = new byte[]{2, 3, 4};
    final byte[] c = new byte[]{3, 4, 5};

    // If a < b and b < c, then a < c
    assertThat(comparator.compare(a, b)).isLessThan(0);
    assertThat(comparator.compare(b, c)).isLessThan(0);
    assertThat(comparator.compare(a, c)).isLessThan(0);
  }

  @Test
  void consistencyAcrossImplementations() {
    final ByteArrayComparator unsafe = UnsignedBytesComparator.BEST_COMPARATOR;
    final ByteArrayComparator pureJava = UnsignedBytesComparator.PURE_JAVA_COMPARATOR;
    final Random random = new Random(42);

    // Test that both implementations give same results
    for (int i = 0; i < 100; i++) {
      final byte[] a = new byte[random.nextInt(50) + 1];
      final byte[] b = new byte[random.nextInt(50) + 1];
      random.nextBytes(a);
      random.nextBytes(b);

      final int result1 = unsafe.compare(a, b);
      final int result2 = pureJava.compare(a, b);

      // Both should agree on sign
      assertThat(Integer.signum(result1)).isEqualTo(Integer.signum(result2));

      // Test equals method
      if (a.length > 0 && b.length > 0) {
        final int len = Math.min(a.length, b.length);
        assertThat(unsafe.equals(a, b, len)).isEqualTo(pureJava.equals(a, b, len));
      }
    }
  }

  @Test
  void largeArrays() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;
    final Random random = new Random(42);

    final byte[] large1 = new byte[1000];
    final byte[] large2 = new byte[1000];
    random.nextBytes(large1);
    System.arraycopy(large1, 0, large2, 0, 1000);

    assertThat(comparator.compare(large1, large2)).isEqualTo(0);

    // Change last byte
    large2[999] = (byte) (large2[999] + 1);
    assertThat(comparator.compare(large1, large2)).isLessThan(0);
  }

  @Test
  void differenceInFirstStride() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Difference in first 8 bytes
    final byte[] a = new byte[16];
    final byte[] b = new byte[16];
    for (int i = 0; i < 16; i++) {
      a[i] = 1;
      b[i] = 1;
    }
    b[3] = 2; // Difference in first stride

    assertThat(comparator.compare(a, b)).isLessThan(0);
  }

  @Test
  void differenceInSecondStride() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Difference after first 8 bytes
    final byte[] a = new byte[16];
    final byte[] b = new byte[16];
    for (int i = 0; i < 16; i++) {
      a[i] = 1;
      b[i] = 1;
    }
    b[10] = 2; // Difference in second stride

    assertThat(comparator.compare(a, b)).isLessThan(0);
  }

  @Test
  void differenceInEpilogue() {
    final ByteArrayComparator comparator = UnsignedBytesComparator.BEST_COMPARATOR;

    // Difference in non-stride-aligned bytes
    final byte[] a = new byte[18];
    final byte[] b = new byte[18];
    for (int i = 0; i < 18; i++) {
      a[i] = 1;
      b[i] = 1;
    }
    b[17] = 2; // Difference in epilogue (18 = 8*2 + 2)

    assertThat(comparator.compare(a, b)).isLessThan(0);
  }
}
