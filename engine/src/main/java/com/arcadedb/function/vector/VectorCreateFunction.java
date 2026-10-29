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
package com.arcadedb.function.vector;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

import java.lang.reflect.Array;
import java.util.List;

/**
 * vector_create(values, dimension, coordinateType) - converts a list of numbers into the primitive array of the coordinate type
 * ({@code INT8} is a {@code byte[]}, {@code INT16} a {@code short[]}, {@code INT32} an {@code int[]}, {@code INTEGER} a
 * {@code long[]}, {@code FLOAT32} a {@code float[]}, {@code FLOAT} a {@code double[]}), a {@code float[]} when none is given (#10917).
 * Used by the Cypher vector() function to create typed vector values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorCreateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector_create";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args[0] == null)
      return null;

    // Null propagation: if dimension is explicitly null, return null
    if (args.length >= 2 && args[1] == null)
      return null;

    final String coordinateType = args.length >= 3 && args[2] != null ? args[2].toString() : "FLOAT32";
    final Object result = switch (coordinateType) {
      case "INT8" -> toBytes(args[0]);
      case "INT16" -> toShorts(args[0]);
      case "INT32" -> toInts(args[0]);
      case "INTEGER" -> toLongs(args[0]);
      case "FLOAT" -> toDoubles(args[0]);
      case "FLOAT32" -> VectorUtils.toFloatArray(args[0]);
      default -> throw new CommandExecutionException("Unsupported vector coordinate type: " + coordinateType);
    };

    // Validate dimension if provided
    if (args.length >= 2 && args[1] != null) {
      // Issue #10850: a whole number or a type error - intValue() read 3.9 as 3 and 4294967299 as 3, so a dimension
      // the caller got wrong could match the vector
      final long expectedDimension = CypherFunctionHelper.requireWholeNumberArgument(args[1], getName(), "dimension");
      final int length = Array.getLength(result);
      if (length != expectedDimension)
        throw new CommandExecutionException("Vector dimension mismatch: expected " + expectedDimension + " but got " + length);
    }

    return result;
  }

  private static byte[] toBytes(final Object values) {
    final Object[] source = elements(values);
    final byte[] result = new byte[source.length];
    for (int i = 0; i < source.length; i++)
      result[i] = (byte) wholeCoordinate(source[i], "INT8", Byte.MIN_VALUE, Byte.MAX_VALUE);
    return result;
  }

  private static short[] toShorts(final Object values) {
    final Object[] source = elements(values);
    final short[] result = new short[source.length];
    for (int i = 0; i < source.length; i++)
      result[i] = (short) wholeCoordinate(source[i], "INT16", Short.MIN_VALUE, Short.MAX_VALUE);
    return result;
  }

  private static int[] toInts(final Object values) {
    final Object[] source = elements(values);
    final int[] result = new int[source.length];
    for (int i = 0; i < source.length; i++)
      result[i] = (int) wholeCoordinate(source[i], "INT32", Integer.MIN_VALUE, Integer.MAX_VALUE);
    return result;
  }

  private static long[] toLongs(final Object values) {
    final Object[] source = elements(values);
    final long[] result = new long[source.length];
    for (int i = 0; i < source.length; i++)
      result[i] = wholeCoordinate(source[i], "INTEGER", Long.MIN_VALUE, Long.MAX_VALUE);
    return result;
  }

  private static double[] toDoubles(final Object values) {
    final Object[] source = elements(values);
    final double[] result = new double[source.length];
    for (int i = 0; i < source.length; i++) {
      if (!(source[i] instanceof Number number))
        throw new CommandExecutionException("Vector coordinate of type FLOAT must be a number, found: " + source[i]);
      result[i] = number.doubleValue();
    }
    return result;
  }

  private static Object[] elements(final Object values) {
    if (values instanceof List<?> list)
      return list.toArray();
    if (values instanceof Object[] array)
      return array;
    if (values != null && values.getClass().isArray()) {
      final int length = Array.getLength(values);
      final Object[] result = new Object[length];
      for (int i = 0; i < length; i++)
        result[i] = Array.get(values, i);
      return result;
    }
    // a string or any other form the float conversion reads
    final float[] floats = VectorUtils.toFloatArray(values);
    final Object[] result = new Object[floats.length];
    for (int i = 0; i < floats.length; i++)
      result[i] = floats[i];
    return result;
  }

  /** A coordinate of an integer type: a whole number inside the range of the type, or an error (never a wrapped value). */
  private static long wholeCoordinate(final Object coordinate, final String type, final long min, final long max) {
    final Long whole = coordinate instanceof Long l ? l : coordinate instanceof Integer i ? (Long) (long) i :
        coordinate instanceof Short sh ? (Long) (long) sh : coordinate instanceof Byte b ? (Long) (long) b : null;
    if (whole != null) {
      if (whole < min || whole > max)
        throw new CommandExecutionException("Vector coordinate " + whole + " is out of the range of " + type);
      return whole;
    }
    if (coordinate instanceof Number number) {
      final double value = number.doubleValue();
      // 2^63 is the double that (double) Long.MAX_VALUE rounds up to, which the long cast would saturate
      if (value != Math.rint(value) || Double.isInfinite(value) || value < min || value > max || value >= 0x1p63)
        throw new CommandExecutionException("Vector coordinate " + value + " is not a whole number of " + type);
      return (long) value;
    }
    throw new CommandExecutionException("Vector coordinate of type " + type + " must be a number, found: " + coordinate);
  }
}
