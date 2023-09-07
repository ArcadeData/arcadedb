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
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.DateUtils;

import java.math.*;
import java.time.temporal.*;
import java.util.*;

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
    case BinaryTypes.TYPE_INT: {
      final int v1 = ((Number) value1).intValue();
      final int v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).intValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = ((Boolean) value2) ? 1 : 0;
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Integer.parseInt((String) value2);
        break;

      default:
        return -1;
      }

      return Integer.compare(v1, v2);
    }

    case BinaryTypes.TYPE_LONG: {
      final long v1 = ((Number) value1).longValue();
      final long v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).longValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = ((Boolean) value2) ? 1 : 0;
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Long.parseLong((String) value2);
        break;

      default:
        return -1;
      }

      return Long.compare(v1, v2);
    }

    case BinaryTypes.TYPE_SHORT: {
      final short v1 = ((Number) value1).shortValue();
      final short v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).shortValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = (short) (((Boolean) value2) ? 1 : 0);
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Short.parseShort((String) value2);
        break;

      default:
        return -1;
      }

      return Short.compare(v1, v2);
    }

    case BinaryTypes.TYPE_STRING: {
      if (value1 instanceof byte[]) {
        if (value2 instanceof byte[])
          return UnsignedBytesComparator.BEST_COMPARATOR.compare((byte[]) value1, (byte[]) value2);
        else
          return UnsignedBytesComparator.BEST_COMPARATOR.compare((byte[]) value1, ((String) value2).getBytes(DatabaseFactory.getDefaultCharset()));
      }

      return ((String) value1).compareTo(value2.toString());
    }

    case BinaryTypes.TYPE_DOUBLE: {
      final double v1 = ((Number) value1).doubleValue();
      final double v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).doubleValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = ((Boolean) value2) ? 1 : 0;
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Double.parseDouble((String) value2);
        break;

      default:
        return -1;
      }

      return Double.compare(v1, v2);
    }

    case BinaryTypes.TYPE_FLOAT: {
      final float v1 = ((Number) value1).floatValue();
      final float v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).floatValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = (float) (((Boolean) value2) ? 1 : 0);
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Float.parseFloat((String) value2);
        break;

      default:
        return -1;
      }

      return Float.compare(v1, v2);
    }

    case BinaryTypes.TYPE_BYTE: {
      final byte v1 = ((Number) value1).byteValue();
      final byte v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).byteValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = (byte) (((Boolean) value2) ? 1 : 0);
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Byte.parseByte((String) value2);
        break;

      default:
        return -1;
      }

      return Byte.compare(v1, v2);
    }

    case BinaryTypes.TYPE_BOOLEAN: {
      final int v1 = ((Boolean) value1) ? 1 : 0;
      final int v2;

      switch (type2) {
      case BinaryTypes.TYPE_INT:
      case BinaryTypes.TYPE_SHORT:
      case BinaryTypes.TYPE_LONG:
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
      case BinaryTypes.TYPE_BYTE:
      case BinaryTypes.TYPE_DECIMAL:
      case BinaryTypes.TYPE_FLOAT:
      case BinaryTypes.TYPE_DOUBLE:
        v2 = ((Number) value2).byteValue();
        break;

      case BinaryTypes.TYPE_BOOLEAN:
        v2 = ((Boolean) value2) ? 1 : 0;
        break;

      case BinaryTypes.TYPE_STRING:
        v2 = Boolean.parseBoolean((String) value2) ? 1 : 0;
        break;

      default:
        return -1;
      }

      return Integer.compare(v1, v2);
    }
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATETIME_SECOND:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS: {
      final ChronoUnit higherPrecision = DateUtils.getHigherPrecision(value1, value2);
      final long v1 = DateUtils.dateTimeToTimestamp(value1, higherPrecision);
      final long v2 = DateUtils.dateTimeToTimestamp(value2, higherPrecision);
      return Long.compare(v1, v2);
    }

    case BinaryTypes.TYPE_BINARY: {
      switch (type2) {
      case BinaryTypes.TYPE_BINARY: {
        return ((Binary) value1).compareTo((Binary) value2);
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
      case BinaryTypes.TYPE_DATETIME:
      case BinaryTypes.TYPE_DATE:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Long) value2));
      case BinaryTypes.TYPE_BYTE:
        return ((BigDecimal) value1).compareTo(new BigDecimal((Byte) value2));
      case BinaryTypes.TYPE_DECIMAL:
        return ((BigDecimal) value1).compareTo((BigDecimal) value2);
      case BinaryTypes.TYPE_FLOAT:
        return ((BigDecimal) value1).compareTo(BigDecimal.valueOf((Float) value2));
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
        return CollectionUtils.compare((List) value1, (List) value2);
      }
      break;
    }

    }

    throw new IllegalArgumentException("Comparison between type " + type1 + " and " + type2 + " not supported");
  }

  public int compareBytes(final byte[] buffer1, final Binary buffer2) {
    if (buffer1 == null)
      return -1;
    if (buffer2 == null)
      return 1;

    final long b1Size = buffer1.length;
    final long b2Size = buffer2.getUnsignedNumber();

    final int minSize = (int) Math.min(b1Size, b2Size);

    for (int i = 0; i < minSize; ++i) {
      final byte b1 = buffer1[i];
      final byte b2 = buffer2.getByte();

      if (b1 > b2)
        return 1;
      else if (b1 < b2)
        return -1;
    }

    return Long.compare(b1Size, b2Size);
  }

  public static boolean equals(final Object a, final Object b) {
    if (a instanceof String && b instanceof String)
      return equalsString((String) a, (String) b);
    else if (a instanceof byte[] && b instanceof byte[])
      return equalsBytes((byte[]) a, (byte[]) b);
    else if (a instanceof Binary && b instanceof Binary)
      return equalsBinary((Binary) a, (Binary) b);
    return a.equals(b);
  }

  public static boolean equalsString(final String buffer1, final String buffer2) {
    if (buffer1 == null || buffer2 == null)
      return false;

    return equalsBytes(buffer1.getBytes(DatabaseFactory.getDefaultCharset()), buffer2.getBytes(DatabaseFactory.getDefaultCharset()));
  }

  public static boolean equalsBytes(final byte[] buffer1, final byte[] buffer2) {
    if (buffer1 == null || buffer2 == null)
      return false;

    if (buffer1.length != buffer2.length)
      return false;

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
    else if (a == null && b != null)
      return -1;
    else if (a instanceof String && b instanceof String)
      return compareBytes(((String) a).getBytes(), ((String) b).getBytes(DatabaseFactory.getDefaultCharset()));
    else if (a instanceof byte[] && b instanceof byte[])
      return compareBytes((byte[]) a, (byte[]) b);
    return ((Comparable<Object>) a).compareTo(b);
  }

  public static int compareBytes(final byte[] buffer1, final byte[] buffer2) {
    return UnsignedBytesComparator.BEST_COMPARATOR.compare(buffer1, buffer2);
  }
}
