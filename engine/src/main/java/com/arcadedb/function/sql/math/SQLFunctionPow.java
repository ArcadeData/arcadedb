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
import com.arcadedb.utility.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * Math pow() function.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionPow extends SQLFunctionMathAbstract {
  public static final String NAME             = "pow";
  /** The most digits an exact DECIMAL power is computed with; past it the power is rounded to DECIMAL128. */
  static final        int    MAX_EXACT_DIGITS = 1024;
  private             Object result;

  public SQLFunctionPow() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable record, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 2)
      throw new IllegalArgumentException("Syntax error: " + getSyntax());

    result = pow(params[0], params[1]);
    return result;
  }

  /**
   * The value of {@code base} raised to {@code exponent}, which depends on the two values and not on the Java width they
   * arrived in (issue #11047):
   * <ul>
   *   <li>an integral base (BYTE, SHORT, INTEGER, LONG) and a whole exponent that is not negative answer the exact power,
   *       widened as the SQL operators widen it: an INTEGER while it fits, then a LONG, then an exact DECIMAL;</li>
   *   <li>a DECIMAL base (or a {@link BigInteger}) and a whole exponent answer the exact power, rounded to
   *       {@link MathContext#DECIMAL128} past {@link #MAX_EXACT_DIGITS} digits or for a negative exponent, whose power has
   *       no exact decimal form in general;</li>
   *   <li>everything else - a FLOAT or DOUBLE base, a fractional exponent, a negative exponent of an integral base - is
   *       computed in double, as {@code 2 ^ 0.5} is by every SQL dialect.</li>
   * </ul>
   * A negative base is defined; only a power with no real value (a negative base raised to a fractional exponent) answers
   * NULL, as {@code sqrt()} of a negative number does. A power that no type can hold is a {@link ArithmeticErrorException}, the
   * typed client error of {@code 9223372036854775807 + 1}, never a saturated or wrapped number.
   */
  static Object pow(final Object base, final Object exponent) {
    if (base == null || exponent == null)
      return null;
    if (!(base instanceof Number number))
      throw new IllegalArgumentException("pow() requires a numeric <base>, but received " + NumberUtils.describeRejectedNumber(base));
    final Number power = exponentOf(exponent);

    final int whole;
    try {
      whole = NumberUtils.toIntWhole(power);
    } catch (final ArithmeticException notAWholeIntExponent) {
      return doublePow(number, power);
    }

    return switch (number) {
      case Integer i -> integralPow(i, whole, false);
      case Long l -> integralPow(l, whole, true);
      case Short s -> integralPow(s, whole, false);
      case Byte b -> integralPow(b, whole, false);
      case BigInteger integer -> {
        final BigDecimal decimal = decimalPow(new BigDecimal(integer), whole);
        yield decimal.scale() == 0 && whole >= 0 ? decimal.toBigInteger() : decimal;
      }
      case BigDecimal decimal -> decimalPow(decimal, whole);
      default -> doublePow(number, power);
    };
  }

  /**
   * The exact power of an integral base: an INTEGER while it fits (unless the base is a LONG), a LONG while it fits, else an
   * exact DECIMAL. A negative exponent has a fractional power: it is computed in double.
   */
  private static Object integralPow(final long base, final int exponent, final boolean longBase) {
    if (exponent < 0)
      // through the same checks as every double power: 0 ^ -1 has no value and is refused, not answered as infinity
      return doublePow(base, exponent);

    final long power;
    try {
      power = powExact(base, exponent);
    } catch (final ArithmeticException overflow) {
      return decimalPow(BigDecimal.valueOf(base), exponent);
    }
    if (!longBase && power >= Integer.MIN_VALUE && power <= Integer.MAX_VALUE)
      return Math.toIntExact(power);
    return power;
  }

  /**
   * Exponentiation by squaring, failing on the first overflow: the base is squared only while bits of the exponent are left,
   * and every such square divides the final power, so an overflowing square is an overflowing power.
   */
  private static long powExact(long base, int exponent) {
    long power = 1L;
    while (true) {
      if ((exponent & 1) != 0)
        power = Math.multiplyExact(power, base);
      exponent >>>= 1;
      if (exponent == 0)
        return power;
      base = Math.multiplyExact(base, base);
    }
  }

  private static BigDecimal decimalPow(final BigDecimal base, final int exponent) {
    try {
      // the digits of an exact power are about the base's times the exponent: past the bound it is rounded like any decimal
      // arithmetic result, rather than computed with millions of digits on the caller's request
      if (exponent >= 0 && (long) base.precision() * exponent <= MAX_EXACT_DIGITS)
        return base.pow(exponent);
      return base.pow(exponent, MathContext.DECIMAL128);
    } catch (final ArithmeticException e) {
      // zero to a negative power, or a power whose exponent overflows the decimal's scale
      throw new ArithmeticErrorException("pow() has no decimal result for " + base + " ^ " + exponent + ": " + e.getMessage(), e);
    }
  }

  private static Object doublePow(final Number base, final Number exponent) {
    final double b = base.doubleValue();
    final double e = exponent.doubleValue();
    final double power = Math.pow(b, e);
    if (Double.isNaN(power) && !Double.isNaN(b) && !Double.isNaN(e))
      // a negative base raised to a fractional exponent: no real result
      return null;
    if (Double.isInfinite(power) && !(base instanceof Double || base instanceof Float) && !Double.isInfinite(b)
        && !Double.isInfinite(e))
      // an exact base whose power is past every range: refused like an overflow of the operators, not answered as infinity
      throw new ArithmeticErrorException("pow() overflow: " + base + " ^ " + exponent);
    return power;
  }

  private static Number exponentOf(final Object exponent) {
    if (exponent instanceof Number number)
      return number;
    if (exponent instanceof CharSequence text && text.length() <= NumberUtils.MAX_NUMERIC_LITERAL_LENGTH)
      try {
        return new BigDecimal(text.toString().trim());
      } catch (final NumberFormatException e) {
        // FALL THROUGH TO THE TYPED ERROR BELOW
      }
    throw new IllegalArgumentException(
        "pow() requires a numeric <power>, but received " + NumberUtils.describeRejectedNumber(exponent));
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "pow(<number>, <power>)";
  }

  @Override
  public Object getResult() {
    return result;
  }

  /**
   * Pure math over its two arguments (issue #6190).
   */
  @Override
  public boolean isDeterministic() {
    return true;
  }
}
