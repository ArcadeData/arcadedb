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

import com.arcadedb.exception.ArcadeDBException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.PrivilegedExceptionAction;
import java.util.Comparator;

/**
 * This class was inspired by Guava's UnsignedBytes, under Apache 2 license.
 *
 * @author Louis Wasserman
 * @author Brian Milch
 * @author Colin Evans
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UnsignedBytesComparator {
  private static final int                UNSIGNED_MASK        = 0xFF;
  private static final Unsafe             theUnsafe;
  private static final int                BYTE_ARRAY_BASE_OFFSET;
  public static final  PureJavaComparator PURE_JAVA_COMPARATOR = new PureJavaComparator();
  public static final  Comparator<byte[]> BEST_COMPARATOR;

  static {
    theUnsafe = getUnsafe();
    BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    // fall back to the safer pure java implementation unless we're in
    // a 64-bit JVM with an 8-byte aligned field offset.
    if (!("64".equals(System.getProperty("sun.arch.data.model")) && (BYTE_ARRAY_BASE_OFFSET % 8) == 0
        // sanity check - this should never fail
        && theUnsafe.arrayIndexScale(byte[].class) == 1)) {
      BEST_COMPARATOR = PURE_JAVA_COMPARATOR;  // force fallback to PureJavaComparator
    } else {
      BEST_COMPARATOR = new UnsafeComparator();
    }
  }

  private UnsignedBytesComparator() {
  }

  public static class UnsafeComparator implements Comparator<byte[]> {
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
        final long lw = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET + (long) i);
        final long rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET + (long) i);
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
    public String toString() {
      return "UnsignedBytes.lexicographicalComparator() (sun.misc.Unsafe version)";
    }
  }

  public static class PureJavaComparator implements Comparator<byte[]> {
    @Override
    public int compare(final byte[] left, final byte[] right) {
      int minLength = Math.min(left.length, right.length);
      for (int i = 0; i < minLength; i++) {
        final int result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0) {
          return result;
        }
      }
      return left.length - right.length;
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

  /**
   * Returns a sun.misc.Unsafe. Suitable for use in a 3rd party package. Replace with a simple
   * call to Unsafe.getUnsafe when integrating into a jdk.
   *
   * @return a sun.misc.Unsafe
   */
  private static sun.misc.Unsafe getUnsafe() {
    try {
      return sun.misc.Unsafe.getUnsafe();
    } catch (SecurityException e) {
      // that's okay; try reflection instead
    }
    try {
      return java.security.AccessController.doPrivileged((PrivilegedExceptionAction<Unsafe>) () -> {
        final Class<Unsafe> k = Unsafe.class;
        for (Field f : k.getDeclaredFields()) {
          f.setAccessible(true);
          final Object x = f.get(null);
          if (k.isInstance(x)) {
            return k.cast(x);
          }
        }
        throw new NoSuchFieldError("the Unsafe");
      });
    } catch (java.security.PrivilegedActionException e) {
      throw new ArcadeDBException("Could not initialize intrinsics", e.getCause());
    }
  }

}
