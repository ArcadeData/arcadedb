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

import com.arcadedb.schema.Type;

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
      try {
        number = Long.parseLong(number.toString());
      } catch (final NumberFormatException e) {
        throw new RedisException("value is not an integer or out of range");
      }
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
   * The remapping for INCRBYFLOAT: no integral restriction - it promotes an integral value to float and accepts any
   * stored value it can read as a float ({@code "3.3"} included), refusing only what cannot be parsed as one, with
   * real Redis' own "value is not a valid float".
   */
  public static UnaryOperator<Object> incrementByFloat(final double delta) {
    return stored -> {
      final Number number;
      if (stored == null)
        number = 0L;
      else if (stored instanceof Number storedNumber)
        number = storedNumber;
      else {
        try {
          number = Double.parseDouble(stored.toString());
        } catch (final NumberFormatException e) {
          throw new RedisException("value is not a valid float");
        }
      }
      return Type.increment(number, delta);
    };
  }
}
