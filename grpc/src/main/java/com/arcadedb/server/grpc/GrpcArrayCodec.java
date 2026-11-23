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
package com.arcadedb.server.grpc;

import java.util.List;

/**
 * The {@code logical_type} that keeps a primitive numeric array ({@code ARRAY_OF_SHORTS}, {@code ARRAY_OF_INTEGERS},
 * {@code ARRAY_OF_LONGS}, {@code ARRAY_OF_FLOATS}, {@code ARRAY_OF_DOUBLES}) the same array across the wire (issue #12189). The
 * array travels as a {@code list_value} like any list, tagged with the Java name of its class, and the client decodes a tagged list
 * back to the array, as the embedded API and the HTTP client return it. A peer that ignores {@code logical_type} still reads the
 * list it always read. Shared by the server's encoders and the client's decoder, as {@link GrpcTemporalCodec} is.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class GrpcArrayCodec {
  public static final String SHORT_ARRAY  = "short[]";
  public static final String INT_ARRAY    = "int[]";
  public static final String LONG_ARRAY   = "long[]";
  public static final String FLOAT_ARRAY  = "float[]";
  public static final String DOUBLE_ARRAY = "double[]";

  private GrpcArrayCodec() {
  }

  /** The tag of a primitive numeric array, null for any other value. */
  public static String logicalTypeOf(final Object value) {
    return switch (value) {
      case short[] ignored -> SHORT_ARRAY;
      case int[] ignored -> INT_ARRAY;
      case long[] ignored -> LONG_ARRAY;
      case float[] ignored -> FLOAT_ARRAY;
      case double[] ignored -> DOUBLE_ARRAY;
      case null, default -> null;
    };
  }

  /**
   * The primitive array a list tagged {@code logicalType} stands for, or null when the tag names none of them or an item is not a
   * number (which a tagged list never holds): the caller then keeps the plain list. The items are read straight into the array,
   * with no intermediate list of boxed numbers, since a vector column is read by the million.
   */
  public static Object toArray(final List<GrpcValue> values, final String logicalType) {
    if (logicalType == null || logicalType.isEmpty())
      return null;

    final int size = values.size();
    switch (logicalType) {
    case SHORT_ARRAY: {
      final short[] array = new short[size];
      for (int i = 0; i < size; i++) {
        final GrpcValue item = values.get(i);
        if (!isNumber(item))
          return null;
        array[i] = (short) intOf(item);
      }
      return array;
    }
    case INT_ARRAY: {
      final int[] array = new int[size];
      for (int i = 0; i < size; i++) {
        final GrpcValue item = values.get(i);
        if (!isNumber(item))
          return null;
        array[i] = intOf(item);
      }
      return array;
    }
    case LONG_ARRAY: {
      final long[] array = new long[size];
      for (int i = 0; i < size; i++) {
        final GrpcValue item = values.get(i);
        if (!isNumber(item))
          return null;
        array[i] = longOf(item);
      }
      return array;
    }
    case FLOAT_ARRAY: {
      final float[] array = new float[size];
      for (int i = 0; i < size; i++) {
        final GrpcValue item = values.get(i);
        if (!isNumber(item))
          return null;
        array[i] = floatOf(item);
      }
      return array;
    }
    case DOUBLE_ARRAY: {
      final double[] array = new double[size];
      for (int i = 0; i < size; i++) {
        final GrpcValue item = values.get(i);
        if (!isNumber(item))
          return null;
        array[i] = doubleOf(item);
      }
      return array;
    }
    default:
      return null;
    }
  }

  private static boolean isNumber(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case INT32_VALUE, INT64_VALUE, FLOAT_VALUE, DOUBLE_VALUE -> true;
      default -> false;
    };
  }

  // the conversions below are those of java.lang.Number, for a value isNumber() accepted

  private static int intOf(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case INT32_VALUE -> value.getInt32Value();
      case INT64_VALUE -> (int) value.getInt64Value();
      case FLOAT_VALUE -> (int) value.getFloatValue();
      default -> (int) value.getDoubleValue();
    };
  }

  private static long longOf(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case INT32_VALUE -> value.getInt32Value();
      case INT64_VALUE -> value.getInt64Value();
      case FLOAT_VALUE -> (long) value.getFloatValue();
      default -> (long) value.getDoubleValue();
    };
  }

  private static float floatOf(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case INT32_VALUE -> value.getInt32Value();
      case INT64_VALUE -> value.getInt64Value();
      case FLOAT_VALUE -> value.getFloatValue();
      default -> (float) value.getDoubleValue();
    };
  }

  private static double doubleOf(final GrpcValue value) {
    return switch (value.getKindCase()) {
      case INT32_VALUE -> value.getInt32Value();
      case INT64_VALUE -> value.getInt64Value();
      case FLOAT_VALUE -> value.getFloatValue();
      default -> value.getDoubleValue();
    };
  }
}
