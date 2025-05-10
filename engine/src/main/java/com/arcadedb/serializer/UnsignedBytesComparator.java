package com.arcadedb.serializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class was inspired by Guava's UnsignedBytes, under Apache 2 license.
 *
 * @author Louis Wasserman
 * @author Brian Milch
 * @author Colin Evans
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UnsignedBytesComparator {
  private static final int                 UNSIGNED_MASK        = 0xFF;
  public static final  PureJavaComparator  PURE_JAVA_COMPARATOR = new PureJavaComparator();
  public static final  ByteArrayComparator BEST_COMPARATOR      = new ByteBufferComparator();

  private UnsignedBytesComparator() {
  }

  public static class ByteBufferComparator implements ByteArrayComparator {
    static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    @Override
    public int compare(final byte[] left, final byte[] right) {
      final int stride = 8;
      final int minLength = Math.min(left.length, right.length);
      final int strideLimit = minLength & -stride;
      int i;

      ByteBuffer leftBuffer = ByteBuffer.wrap(left).order(ByteOrder.nativeOrder());
      ByteBuffer rightBuffer = ByteBuffer.wrap(right).order(ByteOrder.nativeOrder());

      for (i = 0; i < strideLimit; i += stride) {
        final long lw = leftBuffer.getLong(i);
        final long rw = rightBuffer.getLong(i);
        if (lw != rw) {
          if (BIG_ENDIAN) {
            return unsignedLongsCompare(lw, rw);
          }
          final int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
          return ((int) ((lw >>> n) & UNSIGNED_MASK)) - ((int) ((rw >>> n) & UNSIGNED_MASK));
        }
      }

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

      ByteBuffer leftBuffer = ByteBuffer.wrap(left).order(ByteOrder.nativeOrder());
      ByteBuffer rightBuffer = ByteBuffer.wrap(right).order(ByteOrder.nativeOrder());

      for (i = 0; i < strideLimit; i += stride) {
        final long lw = leftBuffer.getLong(i);
        final long rw = rightBuffer.getLong(i);
        if (lw != rw)
          return false;
      }

      for (; i < length; i++) {
        final int result = UnsignedBytesComparator.compare(left[i], right[i]);
        if (result != 0)
          return false;
      }

      return true;
    }

    @Override
    public String toString() {
      return "UnsignedBytes.lexicographicalComparator() (ByteBuffer version)";
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
