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
package com.arcadedb.function;

/**
 * Canonicalizes numeric values for duplicate-elimination purposes (Cypher {@code UNION},
 * {@code RETURN DISTINCT}, {@code count(DISTINCT ...)}, {@code collect(DISTINCT ...)}) so that
 * finite numeric values which compare equal under Cypher's {@code =} operator (e.g. {@code 1 = 1.0})
 * also collapse to the same key. Without this, duplicate-elimination paths that key a
 * {@link java.util.HashSet} / {@link java.util.HashMap} directly on the boxed value see
 * {@code Integer.valueOf(1)}, {@code Long.valueOf(1)} and {@code Double.valueOf(1.0)} as distinct,
 * because Java's per-type {@code equals()}/{@code hashCode()} never compares across boxed numeric
 * types. See issue #5789.
 * <p>
 * A value that is an exact-integer Java numeric type ({@link Long}, {@link Integer}, {@link Short},
 * {@link Byte}) canonicalizes to its {@code long} value, which is always exact. Any other
 * {@link Number} (typically {@link Double}, {@link Float}, or {@link java.math.BigDecimal})
 * canonicalizes to the same {@code long} value when it represents a finite integer within the range
 * a {@code double} represents integers exactly (|value| &lt;= 2^53), so it lines up with the
 * exact-integer-type canonicalization above; otherwise it canonicalizes to its {@code double} value.
 * Non-numeric values pass through unchanged.
 */
public final class DistinctNumericKey {
  /** 2^53: the largest integer magnitude a double represents exactly. */
  private static final long MAX_EXACT_DOUBLE_INTEGER = 1L << 53;

  private DistinctNumericKey() {
  }

  /**
   * Returns a canonical key for {@code value} suitable for use in a hash-based set/map so that
   * numerically equal finite values, regardless of their boxed numeric type, hash and compare equal.
   * Non-{@link Number} values are returned unchanged.
   */
  public static Object canonicalize(final Object value) {
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
      return ((Number) value).longValue();

    if (value instanceof Number number) {
      final double d = number.doubleValue();
      if (!Double.isNaN(d) && !Double.isInfinite(d) && d == Math.rint(d) && Math.abs(d) <= MAX_EXACT_DOUBLE_INTEGER)
        return (long) d;
      return d;
    }

    return value;
  }
}
