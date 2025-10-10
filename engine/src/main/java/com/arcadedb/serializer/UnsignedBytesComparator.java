/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.serializer;

import java.lang.foreign.MemorySegment;

/**
 * This class was inspired by Guava's UnsignedBytes, under Apache 2 license.
 * Updated to use java.lang.foreign.MemorySegment instead of sun.misc.Unsafe
 * according to JEP 471 recommendations.
 *
 * @author Louis Wasserman
 * @author Brian Milch
 * @author Colin Evans
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public final class UnsignedBytesComparator {
  private static final int                 UNSIGNED_MASK        = 0xFF;
  public static final  PureJavaComparator  PURE_JAVA_COMPARATOR = new PureJavaComparator();
  public static final  ByteArrayComparator BEST_COMPARATOR;

  static {
    // Fall back to the pure Java implementation unless we're in a 64-bit JVM
    // Note: We don't need to check the offset alignment as with MemorySegment API
    if (!"64".equals(System.getProperty("sun.arch.data.model"))) {
      BEST_COMPARATOR = PURE_JAVA_COMPARATOR;  // force fallback to PureJavaComparator
    } else {
      BEST_COMPARATOR = new ModernComparator();
    }
  }

  private UnsignedBytesComparator() {
  }

  public static class ModernComparator implements ByteArrayComparator {
    @Override
    public int compare(final byte[] left, final byte[] right) {
      // assumes left and right are non-null
      final MemorySegment leftSegment = MemorySegment.ofArray(left);
      final MemorySegment rightSegment = MemorySegment.ofArray(right);

      final long index = leftSegment.mismatch(rightSegment);
      if (index == -1) {
        return Integer.compare(left.length, right.length);
      }

      // index is either the byte offset which differs or the length of the shorter array
      if (index >= left.length || index >= right.length) {
        return Integer.compare(left.length, right.length);
      }

      return Integer.compare(Byte.toUnsignedInt(left[(int) index]), Byte.toUnsignedInt(right[(int) index]));
    }

    @Override
    public boolean equals(final byte[] left, final byte[] right, final int length) {
      // assumes left and right are non-null and length is non-zero
      if (left.length < length || right.length < length)
        return false;

      final MemorySegment leftSegment = MemorySegment.ofArray(left).asSlice(0, length);
      final MemorySegment rightSegment = MemorySegment.ofArray(right).asSlice(0, length);

      // mismatch is optimized and should be faster than a for loop
      return leftSegment.mismatch(rightSegment) == -1;
    }

    @Override
    public String toString() {
      return "UnsignedBytes.lexicographicalComparator() (java.lang.foreign.MemorySegment version)";
    }
  }

  public static class PureJavaComparator implements ByteArrayComparator {
    @Override
    public int compare(final byte[] left, final byte[] right) {
      final int minLength = Math.min(left.length, right.length);
      for (int i = 0; i < minLength; i++) {
        final int result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0) {
          return result;
        }
      }
      return left.length - right.length;
    }

    @Override
    public boolean equals(final byte[] left, final byte[] right, final int length) {
      // OPTIMIZATION: TEST LAST BYTE FIRST
      int result = UnsignedBytesComparator.compare(left[length - 1], right[length - 1]);
      if (result != 0)
        return false;

      for (int i = 0; i < length - 1; i++) {
        result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0)
          return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "UnsignedBytes.lexicographicalComparator() (pure Java version)";
    }
  }

  public static int compare(final byte a, final byte b) {
    return (a & UNSIGNED_MASK) - (b & UNSIGNED_MASK);
  }

  public static int unsignedLongsCompare(final long a, final long b) {
    final long a2 = a ^ Long.MIN_VALUE;
    final long b2 = b ^ Long.MIN_VALUE;
    return Long.compare(a2, b2);
  }
}
