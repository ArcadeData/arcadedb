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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static com.arcadedb.utility.CollectionUtils.arrayToList;

public class BinaryComparator {
  public int compare(final Object value1, final byte type1, final Object value2, final byte type2) {
    if (value1 == null) {
      if (value2 == null)
        return 0;
      else
        return -1;
    } else if (value2 == null)
      return 1;

    switch (type1) {
    case BinaryTypes.TYPE_INT:
    case BinaryTypes.TYPE_SHORT:
    case BinaryTypes.TYPE_BYTE:
      // Always widen the other operand to its own natural width instead of narrowing it to type1's width:
      // narrowing a wider or floating operand (e.g. a `long` outside `int` range via intValue(), or a fractional
      // `double` via intValue()) can silently truncate it onto the wrong side of value1, which breaks the
      // antisymmetry every comparator must honour (issue #5900).
      return compareNarrowIntegral((Number) value1, type2, value2);

    case BinaryTypes.TYPE_LONG:
      // Same rationale as the INT/SHORT/BYTE case above: a fractional DECIMAL/FLOAT/DOUBLE narrowed via
      // longValue() drops its fraction and can land on the wrong side of value1, the same antisymmetry bug
      // with LONG as type1 instead (#5900 review follow-up).
      return compareWideningLong((Number) value1, type2, value2);

    case BinaryTypes.TYPE_STRING: {
      if (value1 instanceof byte[] bytes1) {
        if (value2 instanceof byte[] bytes)
          return UnsignedBytesComparator.BEST_COMPARATOR.compare(bytes1, bytes);
        else
          return UnsignedBytesComparator.BEST_COMPARATOR.compare(bytes1,
              ((String) value2).getBytes(DatabaseFactory.getDefaultCharset()));
      }

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_BOOLEAN:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATETIME_SECOND:
      case BinaryTypes.TYPE_DATETIME_MICROS:
      case BinaryTypes.TYPE_DATETIME_NANOS:
        // A String value1 against a numeric/boolean/date value2 must agree with the reverse call - delegate to
        // the other side's own comparator and negate, rather than falling through to a lexicographic compareTo()
        // that silently ignores type2 and breaks antisymmetry the same way the narrowing branches did before this
        // fix (e.g. compare("2", STRING, 10, INT) and its reverse both answered "greater"). The DATE/DATETIME
        // branch below already parses a String operand via DateUtils.dateTimeToTimestampInferringStringPrecision(),
        // so this reuses that parsing rather than duplicating it (issue #5947).
        return -compare(value2, type2, value1, type1);

      default:
        // TWO STRINGS ARE ORDERED THE WAY THE PAGES ORDER THEM - BY UNSIGNED UTF-8 BYTES, NOT BY UTF-16 CODE UNITS.
        // String.compareTo() disagrees with the page order for any non-BMP character (a surrogate pair, 0xD800-0xDBFF
        // lead unit, but a 0xF0-0xF4 lead byte) against a BMP character above U+E000, so the range cursor's stop
        // condition, which compares the un-encoded bounds through here, ended a scan before emitting anything
        // (issue #6997)
        return compareStrings((String) value1, value2.toString());
      }
    }

    case BinaryTypes.TYPE_DOUBLE:
    case BinaryTypes.TYPE_FLOAT: {
      // FLOAT widens into double (the reverse direction is what loses precision), so both share this branch
      // instead of FLOAT narrowing the other operand down to float's 24-bit mantissa - the same
      // narrow-instead-of-widen bug as INT/SHORT/BYTE/LONG, just for the floating types (#5900 review
      // follow-up). The widening itself goes through toDouble(), not doubleValue(), so it reads the float's
      // decimal rather than reproducing its rounding error (#7609).
      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_DATETIME_SECOND:
      case BinaryTypes.TYPE_DATETIME_MICROS:
      case BinaryTypes.TYPE_DATETIME_NANOS:
      case BinaryTypes.TYPE_BYTE:
        // Exact for an integral operand past 2^53 where a double is not, and delegating rather than duplicating
        // keeps this direction answering the negation of its reverse - the antisymmetry the narrowing branches
        // used to break (#5900) and the 2^53 band would break again (#7628). temporalAsLong, not a Number cast:
        // a temporal operand is materialised through the configured implementation and need not be a Number.
        return -compareIntegralAgainstFloating(temporalAsLong(value2, type2), (Number) value1, type1);

      case BinaryTypes.TYPE_DECIMAL:
        // The DECIMAL branch of this same method compares exactly; going through double here instead would make
        // this call and its reverse disagree on any decimal carrying more precision than a double holds (#7628).
        return -compare(value2, type2, value1, type1);
      }

      final double v1 = toDouble(value1, type1);
      final double v2;

      switch (type2) {
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = toDouble(value2, type2);
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = (Boolean) value2 ? 1 : 0;
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Double.parseDouble((String) value2);
        break;

      default:
        return -1;
      }

      return Double.compare(v1, v2);
    }

    case BinaryTypes.TYPE_BOOLEAN: {
      final int v1 = (Boolean) value1 ? 1 : 0;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        // Reuse the same widening comparator INT/SHORT/BYTE use as type1: narrowing a wide/floating operand to a
        // `byte` here would be the identical truncation bug this class just eliminated for those three (#5900).
        return compareNarrowIntegral(v1, type2, value2);

      case BinaryTypes.TYPE_BOOLEAN:
        return Integer.compare(v1, (Boolean) value2 ? 1 : 0);

      case BinaryTypes.TYPE_STRING:
        return Integer.compare(v1, Boolean.parseBoolean((String) value2) ? 1 : 0);

      default:
        return -1;
      }
    }
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATETIME_SECOND:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS: {
      // KNOWN GAP, issue #7754: a BOOLEAN value2 reaches here and dateTimeToTimestampInferringStringPrecision has
      // no case for it, so the null it answers NPEs on unboxing - while the reverse direction, BOOLEAN as type1,
      // maps it to 1/0 and answers. Deliberately not patched in passing: whether that comparison should mean
      // anything at all is the actual question, and whichever way it is settled BOTH directions have to implement
      // it, or the comparator stops being antisymmetric (the failure mode of #5900, #5947 and #6997).
      final ChronoUnit higherPrecision = DateUtils.getHigherPrecision(value1, value2);
      final long v1 = DateUtils.dateTimeToTimestampInferringStringPrecision(value1, higherPrecision);
      final long v2 = DateUtils.dateTimeToTimestampInferringStringPrecision(value2, higherPrecision);
      return Long.compare(v1, v2);
    }

    case BinaryTypes.TYPE_BINARY: {
      switch (type2) {
      case BinaryTypes.TYPE_BINARY: {
        // Handle both byte[] and Binary objects
        if (value1 instanceof byte[] bytes1) {
          if (value2 instanceof byte[] bytes2) {
            return UnsignedBytesComparator.BEST_COMPARATOR.compare(bytes1, bytes2);
          } else if (value2 instanceof Binary binary2) {
            return -compareBytes(binary2.getContent(), bytes1);
          }
        } else if (value1 instanceof Binary binary1) {
          if (value2 instanceof byte[] bytes2) {
            return compareBytes(binary1.getContent(), bytes2);
          } else if (value2 instanceof Binary binary2) {
            return binary1.compareTo(binary2);
          }
        }
        throw new IllegalArgumentException(
            "Invalid binary type for comparison: " + value1.getClass() + " and " + value2.getClass());
      }
      }
      throw new UnsupportedOperationException("Comparing binary types");
    }

    case BinaryTypes.TYPE_DECIMAL: {
      switch (type2) {
      case BinaryTypes.TYPE_INT:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Integer) value2));
      case BinaryTypes.TYPE_SHORT:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Short) value2));
      case BinaryTypes.TYPE_LONG:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Long) value2));
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_DATETIME_SECOND:
      case BinaryTypes.TYPE_DATETIME_MICROS:
      case BinaryTypes.TYPE_DATETIME_NANOS:
        // temporalAsLong, not a (Long) cast: a temporal column is materialised through the configured
        // implementation, so this arm threw ClassCastException for a LocalDate/LocalDateTime/Date value - the same
        // bug the integral and floating arms had. The three sub-millisecond types were missing outright and fell
        // through to the unsupported-pair IllegalArgumentException at the bottom of this method (found in review).
        return ((BigDecimal) value1).compareTo(BigDecimal.valueOf(temporalAsLong(value2, type2)));
      case BinaryTypes.TYPE_BYTE:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Byte) value2));
      case BinaryTypes.TYPE_DECIMAL:
        return ((BigDecimal) value1).compareTo((BigDecimal) value2);
      case BinaryTypes.TYPE_FLOAT:
        return ((BigDecimal) value1).compareTo(Type.floatToBigDecimal((Float) value2));
      case BinaryTypes.TYPE_DOUBLE:
        return ((BigDecimal) value1).compareTo(BigDecimal.valueOf((Double) value2));
      case BinaryTypes.TYPE_STRING:
        return ((BigDecimal) value1).compareTo(new BigDecimal((String) value2));
      }
      break;
    }

    case BinaryTypes.TYPE_COMPRESSED_RID:
    case BinaryTypes.TYPE_RID: {
      switch (type2) {
      case BinaryTypes.TYPE_COMPRESSED_RID:
      case BinaryTypes.TYPE_RID:
        return ((Identifiable) value1).getIdentity().compareTo(value2);
      }
    }

    case BinaryTypes.TYPE_LIST: {
      switch (type2) {
      case BinaryTypes.TYPE_LIST:
        final List v1 = value1.getClass().isArray() ? arrayToList(value1) : (List) value1;
        final List v2 = value2.getClass().isArray() ? arrayToList(value2) : (List) value2;

        return CollectionUtils.compare(v1, v2);
      }
      break;
    }

    case BinaryTypes.TYPE_MAP: {
      switch (type2) {
      case BinaryTypes.TYPE_MAP:
        return CollectionUtils.compare((Map) value1, (Map) value2);
      }
      break;
    }

    }

    throw new IllegalArgumentException("Comparison between type " + type1 + " and " + type2 + " not supported");
  }

  /**
   * Shared comparison for a {@code value1} declared as {@code INT}, {@code SHORT} or {@code BYTE} - all of which
   * fit losslessly in a Java {@code int} - against any {@code type2}. The other operand is promoted to whichever
   * width it actually needs (int/long/double) rather than narrowed to {@code value1}'s declared width, so no
   * operand is ever truncated.
   */
  private static int compareNarrowIntegral(final Number value1, final byte type2, final Object value2) {
    switch (type2) {
    case BinaryTypes.TYPE_INT:
    case BinaryTypes.TYPE_SHORT:
    case BinaryTypes.TYPE_BYTE:
      return Integer.compare(value1.intValue(), ((Number) value2).intValue());

    case BinaryTypes.TYPE_BOOLEAN:
      return Integer.compare(value1.intValue(), (Boolean) value2 ? 1 : 0);

    case BinaryTypes.TYPE_LONG:
      return Long.compare(value1.longValue(), ((Number) value2).longValue());

    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME_SECOND:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS:
      return Long.compare(value1.longValue(), temporalAsLong(value2, type2));

    case BinaryTypes.TYPE_DECIMAL:
    case BinaryTypes.TYPE_FLOAT:
    case BinaryTypes.TYPE_DOUBLE:
      return compareIntegralAgainstFloating(value1.longValue(), (Number) value2, type2);

    case BinaryTypes.TYPE_STRING:
      return compareAgainstNumericString(value1, (String) value2);

    default:
      return -1;
    }
  }

  /**
   * The {@code long} a temporal operand compares as.
   * <p>
   * A temporal column is STORED as a long but MATERIALISED through the configured
   * {@code arcadedb.dateTimeImplementation}/{@code dateImplementation}, so a value reaching this comparator can be
   * a {@code LocalDateTime}, {@code Date}, {@code Calendar}, {@code ZonedDateTime} or {@code Instant} just as
   * easily as a {@code Number}. Casting it straight to {@code Number} threw {@code ClassCastException} - for
   * {@code DATE} and {@code DATETIME} that predates this class's current shape, and #7628 extended the same cast
   * to the three sub-millisecond types, turning their wrong answer into a crash instead of fixing it (found
   * in review).
   * <p>
   * The conversion is the one the {@code DATE}/{@code DATETIME} branch of {@link #compare} already applies in the
   * opposite direction, which is why that direction answered instead of throwing - so normalising here is what
   * makes the two agree rather than a second opinion about what a timestamp means.
   *
   * @param value the operand, a {@link Number} or any supported temporal representation
   * @param type  its {@link BinaryTypes} code, which fixes the precision its long is counted in
   */
  private static long temporalAsLong(final Object value, final byte type) {
    // The stored form, and the overwhelmingly common one on an index seek: no conversion, no allocation.
    if (value instanceof Number number)
      return number.longValue();
    // A DATE's long is a count of DAYS, not a timestamp at some sub-day precision, so it converts through its own
    // helper - the one BinarySerializer writes and reads it with. Asking dateTimeToTimestamp* for a DATE raises
    // IllegalArgumentException, which would trade one thrown exception for another.
    if (type == BinaryTypes.TYPE_DATE)
      return DateUtils.dateToEpochDays(value);
    return DateUtils.dateTimeToTimestampInferringStringPrecision(value, DateUtils.getPrecisionFromBinaryType(type));
  }

  /**
   * Widens a numeric operand to {@code double} for comparison. A {@code FLOAT} goes through its decimal form
   * ({@link Type#widenFloat}) rather than {@link Number#doubleValue()}: the primitive widening is exact on the
   * bits and so reproduces the single precision rounding error as a double, which would make this comparator
   * disagree with {@link #equals(Object, Object)} - that one routes through {@link Type#castComparableNumber},
   * which reads the decimal. The two entry points of this class have to answer the same (issue #7609, the same
   * invariant #6997 established for strings).
   *
   * @param value the operand (never {@code null})
   * @param type  its {@link BinaryTypes} code
   *
   * @return the operand as a double
   */
  private static double toDouble(final Object value, final byte type) {
    if (type == BinaryTypes.TYPE_FLOAT)
      return Type.widenFloat((Float) value);
    return ((Number) value).doubleValue();
  }

  /**
   * Shared comparison for a {@code value1} declared as {@code LONG} (or a timestamp type using the same
   * {@code long} representation) against any {@code type2}. {@code INT}/{@code SHORT}/{@code BYTE} widen
   * losslessly into {@code long} so they join the same-width bucket here, unlike in
   * {@link #compareNarrowIntegral}; a {@code DECIMAL}/{@code FLOAT}/{@code DOUBLE} operand is promoted to
   * {@code double} rather than narrowed via {@code longValue()}, which would silently drop its fraction.
   */
  private static int compareWideningLong(final Number value1, final byte type2, final Object value2) {
    switch (type2) {
    case BinaryTypes.TYPE_INT:
    case BinaryTypes.TYPE_SHORT:
    case BinaryTypes.TYPE_BYTE:
    case BinaryTypes.TYPE_LONG:
      return Long.compare(value1.longValue(), ((Number) value2).longValue());

    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME_SECOND:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS:
      return Long.compare(value1.longValue(), temporalAsLong(value2, type2));

    case BinaryTypes.TYPE_BOOLEAN:
      return Long.compare(value1.longValue(), (Boolean) value2 ? 1L : 0L);

    case BinaryTypes.TYPE_DECIMAL:
    case BinaryTypes.TYPE_FLOAT:
    case BinaryTypes.TYPE_DOUBLE:
      return compareIntegralAgainstFloating(value1.longValue(), (Number) value2, type2);

    case BinaryTypes.TYPE_STRING:
      return compareAgainstNumericString(value1, (String) value2);

    default:
      return -1;
    }
  }

  /**
   * Compares an integral operand against a {@code DECIMAL}/{@code FLOAT}/{@code DOUBLE} one. The natural meeting
   * point is {@code double}, but it is only lossless while both operands fit in its 53-bit mantissa: a {@code long}
   * past 2^53 shares its double with the neighbours two or more apart from it, and a {@code DECIMAL} carries
   * arbitrary precision by definition. Either of those routes the pair through {@link BigDecimal} instead, so this
   * comparator keeps answering what the {@code DECIMAL} branch of {@link #compare} and
   * {@link Type#castComparableNumber} answer for the same pair - the three have to agree, or ordering and equality
   * disagree at the same magnitude (issues #7609, #7614, #7628).
   * <p>
   * NaN and the infinities have no decimal form at all and stay in {@code double}, where {@link Double#compare}
   * already orders them totally.
   *
   * @param value1 the integral operand, exact as a {@code long}
   * @param value2 the floating point or decimal operand
   * @param type2  {@code value2}'s {@link BinaryTypes} code
   *
   * @return the sign of {@code value1 - value2}
   */
  private static int compareIntegralAgainstFloating(final long value1, final Number value2, final byte type2) {
    if (type2 == BinaryTypes.TYPE_DECIMAL)
      return BigDecimal.valueOf(value1).compareTo((BigDecimal) value2);

    if (Type.isExactAsDouble(value1) || !Type.isFinite(value2))
      return Double.compare(value1, toDouble(value2, type2));

    return BigDecimal.valueOf(value1).compareTo(Type.floatingToBigDecimal(value2));
  }

  /**
   * Widens to whichever precision the string's own format needs - {@code double} for a fractional/exponent
   * literal or one of the {@code Double} special values, {@code long} otherwise - so an integral string is
   * compared without the precision loss a double round-trip would introduce for large longs, and a fractional
   * string doesn't hard-fail a {@code long} parse. {@code value1} is always integral here: this helper is only
   * reached from {@link #compareNarrowIntegral} and {@link #compareWideningLong}, both of which declare
   * {@code value1} as {@code INT}/{@code SHORT}/{@code BYTE}/{@code LONG}.
   * <p>
   * An integral string outside {@code long}'s range (e.g. 20+ digits, issue #5945) falls back to
   * {@link BigDecimal}, which compares it exactly rather than losing precision through a {@code double}
   * round-trip. The same catch also covers a string that isn't numeric at all (e.g. {@code "abc"}): the
   * {@code BigDecimal} constructor throws its own {@code NumberFormatException} for that case, so behaviour is
   * unchanged there, this method has never guaranteed a result for a non-numeric string.
   */
  private static int compareAgainstNumericString(final Number value1, final String string) {
    switch (string) {
    case "NaN":
    case "+NaN":
    case "-NaN":
    case "Infinity":
    case "+Infinity":
    case "-Infinity":
      return Double.compare(value1.doubleValue(), Double.parseDouble(string));
    }

    if (string.indexOf('.') >= 0 || string.indexOf('e') >= 0 || string.indexOf('E') >= 0)
      return Double.compare(value1.doubleValue(), Double.parseDouble(string));

    try {
      return Long.compare(value1.longValue(), Long.parseLong(string));
    } catch (final NumberFormatException e) {
      return BigDecimal.valueOf(value1.longValue()).compareTo(new BigDecimal(string));
    }
  }

  public int compareBytes(final byte[] buffer1, final Binary buffer2) {
    if (buffer1 == null)
      return -1;
    if (buffer2 == null)
      return 1;

    final long b1Size = buffer1.length;
    final long b2Size = buffer2.getUnsignedNumber();

    final int minSize = (int) Math.min(b1Size, b2Size);

    // Compare bytes UNSIGNED to stay consistent with every other string/byte comparison in the engine
    // (UnsignedBytesComparator, the static compareBytes and compare() for TYPE_STRING). A signed comparison
    // sorts UTF-8 continuation/lead bytes (>= 0x80, negative as a Java byte) before ASCII, which desynchronizes
    // the LSM binary-search seek from the range-cursor stop condition and makes partial-prefix lookups on
    // composite indexes return rows of unrelated keys when the key holds accented/multi-byte characters (#5321).
    for (int i = 0; i < minSize; ++i) {
      final int b1 = buffer1[i] & 0xFF;
      final int b2 = buffer2.getByte() & 0xFF;

      if (b1 > b2)
        return 1;
      else if (b1 < b2)
        return -1;
    }

    return Long.compare(b1Size, b2Size);
  }

  public static boolean equals(final Object a, final Object b) {
    if (a == b)
      return true;
    else if (a == null || b == null)
      return false;
    else if (a instanceof String string && b instanceof String string1)
      return equalsString(string, string1);
    else if (a instanceof byte[] bytes && b instanceof byte[] bytes1)
      return equalsBytes(bytes, bytes1);
    else if (a instanceof Binary binary && b instanceof Binary binary1)
      return equalsBinary(binary, binary1);
    else if (!a.getClass().equals(b.getClass()) &&//
        a instanceof Number number && b instanceof Number number1) {
      final Number[] pair = Type.castComparableNumber(number, number1);
      return pair[0].equals(pair[1]);
    }
    return a.equals(b);
  }

  public static boolean equalsString(final String buffer1, final String buffer2) {
    if (buffer1 == null || buffer2 == null)
      return false;

    if (buffer1.isEmpty() && buffer2.isEmpty())
      return true;

    return equalsBytes(buffer1.getBytes(DatabaseFactory.getDefaultCharset()),
        buffer2.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  public static boolean equalsBytes(final byte[] buffer1, final byte[] buffer2) {
    if (buffer1 == null || buffer2 == null)
      return false;

    if (buffer1.length != buffer2.length)
      return false;

    if (buffer1.length == 0)
      // BOTH EMPTY, SO EQUAL: THE FAST PATH BELOW WOULD READ INDEX -1 (ISSUE #6998)
      return true;

    if (buffer1[buffer1.length - 1] != buffer2[buffer2.length - 1])
      // OPTIMIZATION: CHECK THE LAST BYTE IF IT'S THE SAME FIRST
      return false;

    return equalsBytes(buffer1, buffer2, buffer1.length);
  }

  public static boolean equalsBinary(final Binary buffer1, final Binary buffer2) {
    if (buffer1 == null || buffer2 == null)
      return false;

    if (buffer1.size() != buffer2.size())
      return false;

    return equalsBytes(buffer1.getContent(), buffer2.getContent(), buffer1.size());
  }

  public static boolean equalsBytes(final byte[] buffer1, final byte[] buffer2, final int length) {
    return UnsignedBytesComparator.BEST_COMPARATOR.equals(buffer1, buffer2, length);
  }

  /**
   * Compare 2 values. If strings or byte[] the unsafe native comparator will be used.
   */
  public static int compareTo(final Object a, final Object b) {
    if (a == null && b == null)
      return 0;
    else if (a != null && b == null)
      return 1;
    else if (a == null)
      return -1;
    else if (a instanceof String string && b instanceof String string1)
      return compareStrings(string, string1);
    else if (a instanceof byte[] bytes && b instanceof byte[] bytes1)
      return compareBytes(bytes, bytes1);
    else if (a instanceof Map map && b instanceof Map map1)
      return CollectionUtils.compare(map, map1);
    else if (a instanceof ChronoLocalDate aDate && b instanceof ChronoLocalDate bDate)
      return aDate.compareTo(bDate);
    else if (a instanceof ChronoLocalDateTime<?> aDate && b instanceof ChronoLocalDateTime<?> bDate)
      return aDate.compareTo(bDate);
    else if (DateUtils.isDate(a) || DateUtils.isDate(b))
      return DateUtils.dateTimeToTimestampInferringStringPrecision(a, ChronoUnit.NANOS)
          .compareTo(DateUtils.dateTimeToTimestampInferringStringPrecision(b, ChronoUnit.NANOS));
    return ((Comparable<Object>) a).compareTo(b);
  }

  /**
   * Orders two strings exactly as the unsigned UTF-8 encodings the LSM pages hold would order them, without encoding
   * either: UTF-8 byte order is Unicode code point order, and the only place UTF-16 code unit order departs from it is
   * the surrogate block ({@code 0xD800-0xDFFF}), which UTF-16 places BELOW {@code 0xE000-0xFFFF} while the code points
   * it encodes ({@code U+10000} and above) sort ABOVE every BMP character. The first differing pair of units is
   * therefore remapped so that the two blocks swap places, which is all it takes to sort UTF-16 as UTF-8.
   * <p>
   * This is the one ordering for a String pair, shared by {@link #compare(Object, byte, Object, byte)} and
   * {@link #compareTo(Object, Object)} so the two entry points cannot answer differently again (issue #6997). It also
   * sidesteps the two {@code byte[]} allocations that encoding both operands cost on every comparison, which the index
   * cursor pays once per key while it walks a range. Both sides used to encode with
   * {@link DatabaseFactory#getDefaultCharset()}, which is UTF-8 (issue #6998): the invariant this relies on.
   * <p>
   * The remap is only right for well-formed UTF-16. An isolated surrogate has no code point: the encoder writes the
   * replacement byte {@code '?'} for it, which sorts below almost everything, while the remap would sort it above the
   * whole BMP. So when the first difference sits on or right after a surrogate that is not part of a pair, the pages'
   * own encoding is compared instead - the rare path, and the only one that allocates.
   */
  public static int compareStrings(final String a, final String b) {
    final int aLength = a.length();
    final int bLength = b.length();
    final int length = Math.min(aLength, bLength);
    for (int i = 0; i < length; i++) {
      int ca = a.charAt(i);
      int cb = b.charAt(i);
      if (ca != cb) {
        if (isolatedSurrogateAround(a, i) || isolatedSurrogateAround(b, i))
          return compareBytes(a.getBytes(DatabaseFactory.getDefaultCharset()), b.getBytes(DatabaseFactory.getDefaultCharset()));

        if (ca >= 0xD800 && cb >= 0xD800) {
          // BOTH UNITS ARE IN THE SURROGATE-OR-ABOVE REGION, THE ONLY PLACE THE TWO ORDERS DISAGREE: MOVE THE SURROGATE
          // BLOCK ABOVE THE REST OF THE BMP, WHERE THE 4-BYTE UTF-8 FORM OF WHAT IT ENCODES SORTS
          ca += ca >= 0xE000 ? -0x800 : 0x2000;
          cb += cb >= 0xE000 ? -0x800 : 0x2000;
        }
        return ca - cb;
      }
    }
    return aLength - bLength;
  }

  /**
   * True when the unit at {@code i} is a surrogate without its partner, or when the unit before it is a high surrogate
   * that this unit does not complete: both shapes are encoded as a replacement byte rather than as the code point the
   * UTF-16-as-UTF-8 remap assumes.
   */
  private static boolean isolatedSurrogateAround(final String s, final int i) {
    final char c = s.charAt(i);
    if (Character.isHighSurrogate(c))
      return i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1));
    if (Character.isLowSurrogate(c))
      return i == 0 || !Character.isHighSurrogate(s.charAt(i - 1));
    return i > 0 && Character.isHighSurrogate(s.charAt(i - 1));
  }

  public static int compareBytes(final byte[] buffer1, final byte[] buffer2) {
    return UnsignedBytesComparator.BEST_COMPARATOR.compare(buffer1, buffer2);
  }

  /**
   * Rewrites a value whose SERIALIZED FORM distinguishes values this comparator treats as EQUAL, so that equal
   * values serialize to equal bytes.
   * <p>
   * Every index family eventually compares serialized key bytes rather than values: the LSM index hashes them for
   * its bloom filter, and the HASH index hashes them to route a key and then compares them raw to settle equality.
   * Wherever that happens, a type whose serialized form is STRICTER than the comparator breaks the index's notion
   * of key identity - two spellings of one key land in different places, so a lookup for one cannot find the other
   * and a unique constraint does not see the collision.
   * <p>
   * {@link BigDecimal} is that type, and the only one today. Serialization writes the SCALE followed by the
   * unscaled bytes, while the comparator goes through {@code BigDecimal.compareTo}, which ignores scale: {@code 5}
   * and {@code 5.00} are one key to the comparator and two byte strings to the serializer (issues #7613, #7767).
   * {@code stripTrailingZeros} maps every {@code compareTo}-equal BigDecimal onto one representation, which is the
   * same rule {@link Type#castComparableNumber} applies to a BigDecimal couple and
   * {@link Type#normalizeNumberForKey} applies to a GROUP BY key.
   * <p>
   * Lives here, next to the comparator whose notion of equality it reconciles the bytes with, so a future value
   * type in the same position has ONE place to be handled rather than one per index family.
   *
   * @param value the key component to canonicalize; {@code null} and every other type are returned unchanged
   *
   * @return the canonical representation, or {@code value} itself when it needs no rewriting
   */
  public static Object canonicalizeForByteEquality(final Object value) {
    if (value instanceof BigDecimal decimal) {
      final BigDecimal stripped = decimal.stripTrailingZeros();
      // Equal scales mean equal unscaled values too (same number, same scale), so the bytes already match and the
      // original instance is kept rather than a copy of it.
      if (stripped.scale() != decimal.scale())
        return stripped;
    }
    return value;
  }

  /**
   * The array form of {@link #canonicalizeForByteEquality(Object)}, for a composite key.
   *
   * @return {@code keys} ITSELF when no component needed rewriting - which is every index that has no DECIMAL
   * component, i.e. this costs one instanceof per component and no allocation on the common path - and a rewritten
   * copy otherwise, leaving the caller's array untouched
   */
  public static Object[] canonicalizeForByteEquality(final Object[] keys) {
    Object[] canonical = keys;

    for (int i = 0; i < keys.length; i++) {
      final Object value = canonicalizeForByteEquality(keys[i]);
      if (value != keys[i]) {
        if (canonical == keys)
          canonical = keys.clone();
        canonical[i] = value;
      }
    }

    return canonical;
  }
}
