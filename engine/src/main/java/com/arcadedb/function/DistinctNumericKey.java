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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;

import java.util.function.Function;

/**
 * Canonicalizes values for duplicate-elimination purposes (Cypher {@code UNION},
 * {@code RETURN DISTINCT}, {@code count(DISTINCT ...)}, {@code collect(DISTINCT ...)}) so that
 * values which compare equal under Cypher's {@code =} operator also collapse to the same key.
 * <p>
 * <b>Numeric values:</b> finite numeric values which compare equal (e.g. {@code 1 = 1.0}) canonicalize
 * to the same key. Without this, duplicate-elimination paths that key a {@link java.util.HashSet} /
 * {@link java.util.HashMap} directly on the boxed value see {@code Integer.valueOf(1)},
 * {@code Long.valueOf(1)} and {@code Double.valueOf(1.0)} as distinct, because Java's per-type
 * {@code equals()}/{@code hashCode()} never compares across boxed numeric types. See issue #5789.
 * A value that is an exact-integer Java numeric type ({@link Long}, {@link Integer}, {@link Short},
 * {@link Byte}) canonicalizes to its {@code long} value, which is always exact. Any other
 * {@link Number} (typically {@link Double}, {@link Float}, or {@link java.math.BigDecimal})
 * canonicalizes to the same {@code long} value when it represents a finite integer within the range
 * a {@code double} represents integers exactly (|value| &lt;= 2^53), so it lines up with the
 * exact-integer-type canonicalization above; otherwise it canonicalizes to its {@code double} value.
 * <p>
 * <b>Graph elements:</b> a document/vertex/edge canonicalizes to its {@link RID}. Some callers build
 * their duplicate-elimination key by calling {@code toString()} on the canonicalized value (e.g. to
 * concatenate it into a composite string key); {@link com.arcadedb.database.BaseRecord#toString()} is
 * RID-based and stable, but document/vertex/edge implementations that decorate it with the record's
 * deserialized properties are not - a not-yet-loaded property buffer renders as a placeholder (e.g.
 * {@code #1:0[?]}) instead of the actual property values, so two references to the very same record
 * can render two different strings depending on load state alone, even though they represent one
 * value. Canonicalizing to the identity ({@link RID}) up front, whose own {@code toString()} is just
 * the RID text, sidesteps that instability. See issue #6488.
 * <p>
 * Other values pass through unchanged.
 */
public final class DistinctNumericKey {
  /** 2^53: the largest integer magnitude a double represents exactly. */
  private static final long MAX_EXACT_DOUBLE_INTEGER = 1L << 53;

  private DistinctNumericKey() {
  }

  /**
   * Returns a canonical key for {@code value} suitable for use in a hash-based set/map, or to be
   * rendered with {@code toString()} into a composite string key, so that values which are equal
   * under Cypher's {@code =} operator collapse to the same key regardless of their Java
   * representation or in-memory load state. Values that are neither numeric nor a graph element
   * ({@link Identifiable}) are returned unchanged.
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

    if (value instanceof Identifiable identifiable) {
      final RID rid = identifiable.getIdentity();
      if (rid != null)
        return rid;
    }

    return value;
  }

  /**
   * Builds a composite string key from {@code names}, in iteration order, by rendering each name's
   * canonicalized value as {@code name=value|}. Shared by every Cypher duplicate-elimination step
   * (RETURN DISTINCT, WITH DISTINCT, UNION) that dedups on a set of named values, so the
   * {@link #canonicalize(Object)} behavior - including the RID canonicalization above - only needs
   * to be applied in one place. Callers control ordering (and hence key stability across rows) by
   * choosing what {@code names} they pass; this method does not sort or otherwise reorder it.
   */
  public static String buildKey(final Iterable<String> names, final Function<String, Object> valueOf) {
    final StringBuilder keyBuilder = new StringBuilder();
    for (final String name : names)
      keyBuilder.append(name).append('=').append(canonicalize(valueOf.apply(name))).append('|');
    return keyBuilder.toString();
  }
}
