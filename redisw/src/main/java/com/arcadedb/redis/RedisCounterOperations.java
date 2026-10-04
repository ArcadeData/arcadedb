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
package com.arcadedb.redis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.UnaryOperator;

/**
 * The INCR/INCRBY/INCRBYFLOAT/DECR/DECRBY remapping this module hands to {@code DatabaseInternal.computeGlobalVariable},
 * in the ONE place both the RESP wire path ({@code RedisNetworkExecutor}) and the {@code redis} query language
 * ({@code RedisQueryEngine}) share.
 * <p>
 * It used to be implemented twice, and the copies drifted (#8271): the query language's copy read the increment with
 * {@code Integer.parseInt} instead of a 64-bit parse, so {@code INCRBY k 3000000000} failed where the wire path accepted
 * it; it added with plain {@code Type.increment} instead of a checked {@code Math.addExact}, so a 64-bit overflow wrapped
 * silently instead of being refused; it let INCR/DECR keep incrementing a {@code Double} left behind by INCRBYFLOAT
 * instead of rejecting it the way real Redis does; INCRBYFLOAT on an already-stored float STRING such as {@code "3.3"}
 * was refused instead of accepted; and every refusal read {@code "Key '<k>' is not a number"} instead of real Redis' own
 * "value is not an integer or out of range" / "value is not a valid float". One remapping now backs both surfaces, so
 * they cannot answer the same command differently again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RedisCounterOperations {
  private RedisCounterOperations() {
  }

  /**
   * Parses a 64-bit integer the way real Redis does ({@code string2ll}): the canonical decimal text only, so no leading
   * {@code +}, no leading zeros, no {@code -0}, no surrounding blanks and nothing outside the {@code long} range (#9059).
   * {@code Long.parseLong} accepts all of those, which made INCR succeed on {@code "+10"} and {@code "007"} and rewrite
   * them as {@code 11} and {@code 8}.
   */
  public static long parseInteger(final String text) {
    final int length = text.length();
    int start = 0;
    if (length > 0 && text.charAt(0) == '-')
      start = 1;

    if (length == start || length - start > 19)
      throw new RedisException("value is not an integer or out of range");

    final char first = text.charAt(start);
    if (first < '0' || first > '9' || (first == '0' && length > 1) || (start == 1 && first == '0'))
      throw new RedisException("value is not an integer or out of range");

    for (int i = start + 1; i < length; i++) {
      final char c = text.charAt(i);
      if (c < '0' || c > '9')
        throw new RedisException("value is not an integer or out of range");
    }

    try {
      return Long.parseLong(text);
    } catch (final NumberFormatException e) {
      throw new RedisException("value is not an integer or out of range");
    }
  }

  /**
   * Normalizes a stored Redis value for DECR/DECRBY and the non-decimal path of INCR/INCRBY: a {@code null} key reads
   * as {@code 0}, and a non-{@code Number} stored value (always a {@code String} here) is parsed as a 64-bit integer.
   * Anything that isn't - or can't be parsed as - an integral value, such as a fractional string or a {@code Double}
   * left behind by INCRBYFLOAT, is rejected with real Redis' own "value is not an integer or out of range" rather than
   * silently coercing it or emitting an invalid reply.
   */
  public static Number requireIntegralValue(final Object stored) {
    if (stored == null)
      return 0L;

    Object number = stored;
    if (!(number instanceof Number)) {
      number = parseInteger(number.toString());
    }

    if (!(number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte))
      throw new RedisException("value is not an integer or out of range");

    return (Number) number;
  }

  /**
   * The remapping for INCR/INCRBY: a checked 64-bit addition, refused with "increment or decrement would overflow"
   * rather than wrapping.
   */
  public static UnaryOperator<Object> incrementBy(final long delta) {
    return stored -> {
      final long number = requireIntegralValue(stored).longValue();
      try {
        return Math.addExact(number, delta);
      } catch (final ArithmeticException e) {
        throw new RedisException("increment or decrement would overflow", e);
      }
    };
  }

  /**
   * The remapping for DECR/DECRBY: a checked 64-bit subtraction. Kept as its own subtraction rather than
   * {@code incrementBy(-delta)}: negating {@code Long.MIN_VALUE} overflows and silently wraps back to itself, which
   * would turn a DECR that must be refused into one that runs with the sign flipped.
   */
  public static UnaryOperator<Object> decrementBy(final long delta) {
    return stored -> {
      final long number = requireIntegralValue(stored).longValue();
      try {
        return Math.subtractExact(number, delta);
      } catch (final ArithmeticException e) {
        throw new RedisException("increment or decrement would overflow", e);
      }
    };
  }

  /**
   * The remapping for INCRBYFLOAT: no integral restriction - it accepts any stored value it can read as a decimal number
   * ({@code "3.3"} and an integral {@code "10"} included), refusing only what cannot be parsed as one, with real Redis'
   * own "value is not a valid float". The sum is computed in {@link BigDecimal} and stored as TEXT, the way Redis stores
   * the reply it sends (#9058): {@code 0.1 + 0.2} is {@code 0.3} rather than {@code 0.30000000000000004}, an integral
   * result is {@code 3} rather than {@code 3.0}, and there is no exponent form, so INCR works on an integral result.
   * Like Redis' {@code %.17Lf} the result keeps at most 17 decimals, with trailing zeros removed.
   * <p>
   * NaN/Infinity (as an increment, as the sum, or as a magnitude beyond what Redis' {@code long double} holds) are
   * refused with real Redis' own "increment would produce NaN or Infinity"; a stored non-finite text is "value is not a
   * valid float". The magnitude check also keeps a hostile exponent such as {@code 1e999999999} from allocating a
   * gigantic number.
   */
  public static UnaryOperator<Object> incrementByFloat(final String delta) {
    final BigDecimal increment = parseFloatOperand(delta, "increment would produce NaN or Infinity");

    return stored -> {
      final BigDecimal base;
      if (stored == null)
        base = BigDecimal.ZERO;
      else if (stored instanceof Double || stored instanceof Float) {
        final double d = ((Number) stored).doubleValue();
        if (!Double.isFinite(d))
          throw new RedisException("value is not a valid float");
        base = BigDecimal.valueOf(d);
      } else
        base = parseFloatOperand(stored.toString(), "value is not a valid float");

      final BigDecimal sum = base.add(increment);
      if (isOutOfRange(sum))
        throw new RedisException("increment would produce NaN or Infinity");
      return format(sum);
    };
  }

  private static BigDecimal parseFloatOperand(final String text, final String nonFiniteMessage) {
    final BigDecimal number;
    try {
      number = new BigDecimal(text);
    } catch (final NumberFormatException e) {
      if (isNonFiniteSpelling(text))
        throw new RedisException(nonFiniteMessage);
      throw new RedisException("value is not a valid float");
    }
    if (isOutOfRange(number))
      throw new RedisException(nonFiniteMessage);
    return number;
  }

  private static boolean isNonFiniteSpelling(final String text) {
    final String t = text.startsWith("+") || text.startsWith("-") ? text.substring(1) : text;
    return t.equalsIgnoreCase("nan") || t.equalsIgnoreCase("inf") || t.equalsIgnoreCase("infinity");
  }

  /** Beyond the decimal exponent a {@code long double} holds (about 1e4932). */
  private static boolean isOutOfRange(final BigDecimal number) {
    return number.signum() != 0 && (long) number.precision() - number.scale() > 4932;
  }

  private static String format(final BigDecimal number) {
    final BigDecimal rounded = number.setScale(17, RoundingMode.HALF_EVEN);
    if (rounded.signum() == 0)
      return "0";
    return rounded.stripTrailingZeros().toPlainString();
  }
}
