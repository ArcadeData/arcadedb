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
package com.arcadedb.function.sql.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;

/**
 * Evaluates the absolute value for numeric types.  The argument must be a
 * BigDecimal, BigInteger, Byte, Short, Integer, Long, Double, Float or a
 * Duration, or null.  If null is passed in the result will be null.
 * Otherwise the result will be the mathematical absolute value of the
 * argument passed in and will be of the same type that was passed in.
 * <p>
 * Because the result keeps the argument's type, every fixed-width signed type
 * has exactly one input - its MIN_VALUE - whose magnitude it cannot represent;
 * those fail the query rather than returning a negative "absolute value".
 * Duration has the same single unrepresentable input at Long.MIN_VALUE seconds
 * with no nanos, and is treated the same way.
 *
 * @author Michael MacFadden
 */
public class SQLFunctionAbsoluteValue extends SQLFunctionMathAbstract {
  public static final String NAME = "abs";
  private             Object result;

  public SQLFunctionAbsoluteValue() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable record, final Object currentResult, final Object[] params, final CommandContext context) {
    final Object inputValue = params[0];

    if (inputValue == null) {
      result = null;
    } else if (inputValue instanceof BigDecimal decimal) {
      result = decimal.abs();
    } else if (inputValue instanceof BigInteger integer) {
      result = integer.abs();
    } else if (inputValue instanceof Integer integer) {
      result = (int) absExact(integer, Integer.MIN_VALUE, "integer");
    } else if (inputValue instanceof Long long1) {
      result = absExact(long1, Long.MIN_VALUE, "long");
    } else if (inputValue instanceof Short short1) {
      result = (short) absExact(short1, Short.MIN_VALUE, "short");
    } else if (inputValue instanceof Byte byte1) {
      result = (byte) absExact(byte1, Byte.MIN_VALUE, "byte");
    } else if (inputValue instanceof Double double1) {
      result = Math.abs(double1);
    } else if (inputValue instanceof Float float1) {
      result = Math.abs(float1);
    } else if (inputValue instanceof Duration duration) {
      result = absExact(duration);
    } else {
      throw new IllegalArgumentException("Argument to absolute value must be a number");
    }

    return getResult();
  }

  /**
   * Every fixed-width signed integer type has exactly one value - its MIN_VALUE - whose magnitude it
   * cannot represent. {@code Math.abs()} wraps around and returns that value unchanged, so the caller
   * receives a negative "absolute value" that looks valid and can be persisted. Fail the query
   * instead, using the same wording as the Cypher arithmetic operators.
   * <p>
   * Reported as an {@link ArithmeticErrorException} so the caller's unrepresentable value is a 400 rather than
   * a 500: the query is well-formed and the engine is healthy, and the wire layers single that subclass out from
   * the runtime failures that genuinely are the server's fault. It still extends
   * {@code CommandExecutionException}, so embedded code catching the broader type is unaffected.
   *
   * @param value    the input widened to a long, so one guard serves byte, short, int and long
   * @param minValue the MIN_VALUE of the input's own type, which is the only unrepresentable input
   * @param typeName the input's type as it appears in the error message
   */
  private static long absExact(final long value, final long minValue, final String typeName) {
    if (value == minValue)
      throw new ArithmeticErrorException(typeName + " overflow");
    return Math.abs(value);
  }

  /**
   * A {@code Duration} carries its sign on the whole, not on its components: it is normalized as a possibly
   * negative seconds field plus an always non-negative nanos adjustment, and {@code toSecondsPart()} reports
   * the seconds within the minute rather than the total. Neither part is therefore a usable signal on its own,
   * and taking the two magnitudes independently does not reconstruct the magnitude of the whole.
   * <p>
   * The range is symmetric apart from a single value: negating a duration of exactly {@code Long.MIN_VALUE}
   * seconds and no nanos would need {@code Long.MAX_VALUE + 1} seconds. That is the same unrepresentable
   * magnitude the integral types have at their MIN_VALUE, so it is reported the same way rather than being
   * allowed to escape as a raw {@link ArithmeticException}.
   */
  private static Duration absExact(final Duration duration) {
    if (!duration.isNegative())
      return duration;

    try {
      return duration.negated();
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("duration overflow", e);
    }
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "abs(<number>)";
  }

  @Override
  public Object getResult() {
    return result;
  }
}
