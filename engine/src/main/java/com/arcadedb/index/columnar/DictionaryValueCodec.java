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
package com.arcadedb.index.columnar;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.engine.block.BlockCorruptionException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Type-tagged bytes of one dictionary value. The value is already {@code Type.normalizeForKey()}'d: a {@link String}, a
 * {@link Long}, a {@link BigDecimal}, a {@link Double} (only NaN and the infinities survive normalization as doubles, but any
 * double is accepted), a {@link Boolean} or a {@link RID}. The layout is one type byte followed by the payload of that type.
 */
public final class DictionaryValueCodec {
  static final byte STRING  = 0;
  static final byte LONG    = 1;
  static final byte DECIMAL = 2;
  static final byte DOUBLE  = 3;
  static final byte BOOLEAN = 4;
  static final byte RID     = 5;

  private DictionaryValueCodec() {
  }

  public static boolean isSupported(final Object normalized) {
    return normalized instanceof String || normalized instanceof Long || normalized instanceof BigDecimal || normalized instanceof Double
        || normalized instanceof Boolean || normalized instanceof RID;
  }

  /**
   * Appends the type byte and the payload to {@code out} at its current position.
   *
   * @return the number of bytes written, type byte included
   *
   * @throws IllegalArgumentException when the value is not {@link #isSupported(Object) supported}
   */
  static int encode(final Object value, final Binary out) {
    final int start = out.position();
    out.putByte(typeOf(value));
    switch (value) {
    case String string -> out.putString(string);
    case Long number -> out.putNumber(number);
    case BigDecimal decimal -> {
      out.putNumber(decimal.scale());
      out.putBytes(decimal.unscaledValue().toByteArray());
    }
    case Double number -> out.putLong(Double.doubleToRawLongBits(number));
    case Boolean bool -> out.putByte((byte) (bool ? 1 : 0));
    case RID rid -> {
      out.putInt(rid.getBucketId());
      out.putNumber(rid.getPosition());
    }
    default -> throw new IllegalArgumentException("Unsupported dictionary value type");
    }
    return out.position() - start;
  }

  /**
   * Reads one value from the current position of {@code in}.
   *
   * @throws BlockCorruptionException when the bytes do not describe a value (unknown type byte, truncated or oversized payload)
   */
  static Object decode(final Binary in) {
    try {
      final byte type = in.getByte();
      return switch (type) {
        case STRING -> in.getString();
        case LONG -> in.getNumber();
        case DECIMAL -> {
          final long scale = in.getNumber();
          if (scale < Integer.MIN_VALUE || scale > Integer.MAX_VALUE)
            throw new BlockCorruptionException("Dictionary decimal scale out of range: " + scale);
          yield new BigDecimal(new BigInteger(in.getBytes()), (int) scale);
        }
        case DOUBLE -> Double.longBitsToDouble(in.getLong());
        case BOOLEAN -> in.getByte() != 0;
        case RID -> new RID(in.getInt(), in.getNumber());
        default -> throw new BlockCorruptionException("Unknown dictionary value type " + type);
      };
    } catch (final BlockCorruptionException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw new BlockCorruptionException("Corrupt dictionary value: " + e);
    }
  }

  /**
   * Total order of dictionary values. Classes in order: strings, numbers (LONG, DECIMAL and DOUBLE together), booleans, RIDs.
   * Strings compare with {@link String#compareTo}. Numbers compare by exact numeric value across the three types, with -Infinity
   * first, +Infinity next to last and NaN last; two doubles compare as {@link Double#compare} (-0.0 before 0.0). Numerically equal
   * values of different types (only possible for a non-normalized input, e.g. 0L and -0.0) are ordered by type byte. The order is
   * total and transitive for NORMALIZED values only (what the dictionary holds); for arbitrary mixed input, such as a BigDecimal
   * equal to a Long in value, no such guarantee is given.
   */
  public static int compareValues(final Object a, final Object b) {
    final int classA = classOf(typeOf(a));
    final int classB = classOf(typeOf(b));
    if (classA != classB)
      return Integer.compare(classA, classB);
    return switch (classA) {
      case 0 -> ((String) a).compareTo((String) b);
      case 1 -> compareNumbers(a, b);
      case 2 -> Boolean.compare((Boolean) a, (Boolean) b);
      default -> ((RID) a).compareTo(b);
    };
  }

  private static int classOf(final byte type) {
    return switch (type) {
      case STRING -> 0;
      case LONG, DECIMAL, DOUBLE -> 1;
      case BOOLEAN -> 2;
      default -> 3;
    };
  }

  private static int compareNumbers(final Object a, final Object b) {
    if (a instanceof Long x && b instanceof Long y)
      return Long.compare(x, y);
    if (a instanceof Double x && b instanceof Double y)
      return Double.compare(x, y);
    final int result;
    if (a instanceof Double x)
      result = compareDoubleToExact(x, b);
    else if (b instanceof Double y)
      result = -compareDoubleToExact(y, a);
    else
      result = toDecimal(a).compareTo(toDecimal(b));
    if (result != 0)
      return result;
    return Byte.compare(typeOf(a), typeOf(b));
  }

  /** Exact comparison of a double with a Long or BigDecimal; NaN is above everything, the infinities bound the range. */
  private static int compareDoubleToExact(final double d, final Object exact) {
    if (Double.isNaN(d))
      return 1;
    if (Double.isInfinite(d))
      return d > 0 ? 1 : -1;
    if (exact instanceof Long l) {
      // outside the long range the double decides; inside, truncate and compare the fraction
      if (d >= 0x1p63)
        return 1;
      if (d < -0x1p63)
        return -1;
      final long truncated = (long) d;
      final int c = Long.compare(truncated, l);
      if (c != 0)
        return c;
      return Double.compare(d - truncated, 0d);
    }
    return new BigDecimal(d).compareTo((BigDecimal) exact);
  }

  private static BigDecimal toDecimal(final Object number) {
    return number instanceof Long l ? BigDecimal.valueOf(l) : (BigDecimal) number;
  }

  private static byte typeOf(final Object value) {
    return switch (value) {
      case String ignored -> STRING;
      case Long ignored -> LONG;
      case BigDecimal ignored -> DECIMAL;
      case Double ignored -> DOUBLE;
      case Boolean ignored -> BOOLEAN;
      case RID ignored -> RID;
      case null, default -> throw new IllegalArgumentException("Unsupported dictionary value type");
    };
  }
}
