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

import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Maps a numeric bound onto the keys of an integral index (BYTE, SHORT, INTEGER, LONG) without changing the answer
 * (issue #9021).
 * <p>
 * An index converts a bound to its key type before it seeks, and {@link Type#convert} truncates a fraction ({@code 12.5}
 * becomes {@code 12}) and wraps or clamps a value out of the key's range ({@code 1e19} becomes {@code Long.MAX_VALUE}). A
 * scan compares the exact values instead, so {@code i = 12.5} found the 12 through the index and nothing without it, and
 * {@code i < 12.5} lost the 12. A bound no key equals is therefore turned into the key that bounds the same set of keys,
 * inclusive: the smallest key above it for a lower bound, the largest key below it for an upper bound, and no key at all
 * for an equality. NaN reads as the greatest number, as the scan orders it.
 */
public final class IntegralKeyBound {
  private IntegralKeyBound() {
  }

  /**
   * True when {@code keyType} is an integral binary key type and {@code bound} is a number no key of that type equals: one with
   * a fraction, NaN, an infinity, or one past the range of the key type. Allocates nothing for an integral bound in range.
   */
  public static boolean isInexact(final byte keyType, final Object bound) {
    if (!(bound instanceof Number number))
      return false;
    final long min, max;
    switch (keyType) {
    case BinaryTypes.TYPE_BYTE -> {
      min = Byte.MIN_VALUE;
      max = Byte.MAX_VALUE;
    }
    case BinaryTypes.TYPE_SHORT -> {
      min = Short.MIN_VALUE;
      max = Short.MAX_VALUE;
    }
    case BinaryTypes.TYPE_INT -> {
      min = Integer.MIN_VALUE;
      max = Integer.MAX_VALUE;
    }
    case BinaryTypes.TYPE_LONG -> {
      min = Long.MIN_VALUE;
      max = Long.MAX_VALUE;
    }
    default -> {
      return false;
    }
    }
    if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
      final long value = number.longValue();
      return value < min || value > max;
    }
    if (!Type.isFinite(number))
      return true;
    final BigDecimal exact = toBigDecimal(number);
    if (exact == null)
      // not a standard number class: leave it to the conversion the index has always applied
      return false;
    if (exact.signum() != 0 && exact.stripTrailingZeros().scale() > 0)
      return true;
    return exact.compareTo(BigDecimal.valueOf(min)) < 0 || exact.compareTo(BigDecimal.valueOf(max)) > 0;
  }

  /**
   * The smallest key of {@code keyType} that is greater than or equal to {@code bound}, or {@code null} when no key is. Called
   * for a bound {@link #isInexact} accepts; a lower bound becomes this key, inclusive whatever the operator was.
   */
  public static Number ceiling(final byte keyType, final Number bound) {
    if (isNaN(bound))
      return null;
    if (isInfinite(bound))
      return bound.doubleValue() > 0 ? null : minOf(keyType);
    final BigDecimal rounded = toBigDecimal(bound).setScale(0, RoundingMode.CEILING);
    if (rounded.compareTo(BigDecimal.valueOf(maxOf(keyType).longValue())) > 0)
      return null;
    if (rounded.compareTo(BigDecimal.valueOf(minOf(keyType).longValue())) < 0)
      return minOf(keyType);
    return box(keyType, rounded.longValue());
  }

  /**
   * The greatest key of {@code keyType} that is lower than or equal to {@code bound}, or {@code null} when no key is. Called for
   * a bound {@link #isInexact} accepts; an upper bound becomes this key, inclusive whatever the operator was.
   */
  public static Number floor(final byte keyType, final Number bound) {
    if (isNaN(bound) || (isInfinite(bound) && bound.doubleValue() > 0))
      return maxOf(keyType);
    if (isInfinite(bound))
      return null;
    final BigDecimal rounded = toBigDecimal(bound).setScale(0, RoundingMode.FLOOR);
    if (rounded.compareTo(BigDecimal.valueOf(minOf(keyType).longValue())) < 0)
      return null;
    if (rounded.compareTo(BigDecimal.valueOf(maxOf(keyType).longValue())) > 0)
      return maxOf(keyType);
    return box(keyType, rounded.longValue());
  }

  /** Maps a {@link Type} to the binary key type {@link #isInexact} takes, or -1 for a type that is not integral. */
  public static byte binaryTypeOf(final Type type) {
    return type == null ? -1 : switch (type) {
      case BYTE, SHORT, INTEGER, LONG -> type.getBinaryType();
      default -> -1;
    };
  }

  private static BigDecimal toBigDecimal(final Number number) {
    if (number instanceof BigDecimal decimal)
      return decimal;
    if (number instanceof BigInteger integer)
      return new BigDecimal(integer);
    if (number instanceof Double || number instanceof Float)
      // the decimal reading the scan compares (BinaryComparator, Type.castComparableNumber)
      return Type.floatingToBigDecimal(number);
    if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
      return BigDecimal.valueOf(number.longValue());
    return null;
  }

  private static boolean isNaN(final Number number) {
    return (number instanceof Double d && d.isNaN()) || (number instanceof Float f && f.isNaN());
  }

  private static boolean isInfinite(final Number number) {
    return (number instanceof Double d && d.isInfinite()) || (number instanceof Float f && f.isInfinite());
  }

  private static Number minOf(final byte keyType) {
    return switch (keyType) {
      case BinaryTypes.TYPE_BYTE -> Byte.MIN_VALUE;
      case BinaryTypes.TYPE_SHORT -> Short.MIN_VALUE;
      case BinaryTypes.TYPE_INT -> Integer.MIN_VALUE;
      default -> Long.MIN_VALUE;
    };
  }

  private static Number maxOf(final byte keyType) {
    return switch (keyType) {
      case BinaryTypes.TYPE_BYTE -> Byte.MAX_VALUE;
      case BinaryTypes.TYPE_SHORT -> Short.MAX_VALUE;
      case BinaryTypes.TYPE_INT -> Integer.MAX_VALUE;
      default -> Long.MAX_VALUE;
    };
  }

  private static Number box(final byte keyType, final long value) {
    return switch (keyType) {
      case BinaryTypes.TYPE_BYTE -> (byte) value;
      case BinaryTypes.TYPE_SHORT -> (short) value;
      case BinaryTypes.TYPE_INT -> (int) value;
      default -> value;
    };
  }
}
