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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.*;

/**
 * This class was inspired by Guava's UnsignedBytes, under Apache 2 license.
 * <p>
 * As of Java 21+, this implementation uses VarHandle API for safe and efficient byte array comparisons,
 * replacing the deprecated sun.misc.Unsafe API (JEP 471). The VarHandle approach provides equivalent
 * performance through JVM intrinsics while being officially supported and future-proof.
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
    // Prefer VarHandle (Java 9+) as it's officially supported and not deprecated.
    // Falls back to PureJava implementation if VarHandle fails for any reason.
    ByteArrayComparator bestComparator;
    try {
      bestComparator = new VarHandleComparator();
      // Test it works with a simple comparison
      bestComparator.compare(new byte[]{1, 2, 3}, new byte[]{1, 2, 3});
    } catch (final Throwable t) {
      // Fall back to pure Java implementation if VarHandle is not available
      bestComparator = PURE_JAVA_COMPARATOR;
    }
    BEST_COMPARATOR = bestComparator;
  }

  private UnsignedBytesComparator() {
  }

  /**
   * VarHandle-based comparator that replaces sun.misc.Unsafe for Java 21+.
   * Uses MethodHandles.byteArrayViewVarHandle to read 8 bytes at a time,
   * providing equivalent performance to Unsafe through JVM intrinsics.
   */
  public static class VarHandleComparator implements ByteArrayComparator {
    private static final VarHandle LONG_ARRAY_VIEW_HANDLE =
        MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    @Override
    public int compare(final byte[] left, final byte[] right) {
      final int stride = 8;
      final int minLength = Math.min(left.length, right.length);
      final int strideLimit = minLength & -stride;
      int i;

      /*
       * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
       * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      for (i = 0; i < strideLimit; i += stride) {
        final long lw = (long) LONG_ARRAY_VIEW_HANDLE.get(left, i);
        final long rw = (long) LONG_ARRAY_VIEW_HANDLE.get(right, i);
        if (lw != rw) {
          if (BIG_ENDIAN) {
            return unsignedLongsCompare(lw, rw);
          }
          final int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
          return ((int) ((lw >>> n) & UNSIGNED_MASK)) - ((int) ((rw >>> n) & UNSIGNED_MASK));
        }
      }

      // The epilogue to cover the last (minLength % stride) elements.
      for (; i < minLength; i++) {
        final int result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0)
          return result;
      }

      return left.length - right.length;
    }

    @Override
    public boolean equals(final byte[] left, final byte[] right, final int length) {
      final int stride = 8;
      final int strideLimit = length & -stride;
      int i;

      /*
       * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
       * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      for (i = 0; i < strideLimit; i += stride) {
        final long lw = (long) LONG_ARRAY_VIEW_HANDLE.get(left, i);
        final long rw = (long) LONG_ARRAY_VIEW_HANDLE.get(right, i);
        if (lw != rw)
          return false;
      }

      // The epilogue to cover the last (minLength % stride) elements.
      for (; i < length; i++) {
        final int result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0)
          return false;
      }

      return true;
    }

    @Override
    public String toString() {
      return "UnsignedBytes.lexicographicalComparator() (VarHandle version)";
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
