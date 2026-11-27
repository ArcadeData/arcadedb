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
package com.arcadedb.postgres;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The key two rows are compared by when a set operation, DISTINCT, GROUP BY, count(DISTINCT) or a window partition decides whether
 * they are the same row. Numbers compare by value, as in SQL: the literal {@code 1} is a Long and an OID is an Integer, and
 * {@code List.equals} would tell them apart ({@link PostgresCatalogRowSets#numberKey}).
 * <p>
 * The key is {@link Comparable} with an order that agrees with {@link #equals} (two keys are equal exactly when they compare as 0), so
 * a {@link java.util.HashMap} or {@link java.util.HashSet} of keys finds, adds and removes in O(log n) for any set of keys. A plain
 * {@code List} is not Comparable, which leaves the map no order to use. The hash of a numeric value is mixed.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class PostgresCatalogRowKey implements Comparable<PostgresCatalogRowKey> {
  // The order of the kinds of value, which is the first thing two values are compared by
  private static final int NULL     = 0;
  private static final int BOOLEAN  = 1;
  private static final int INTEGRAL = 2;
  private static final int REAL     = 3;
  private static final int DECIMAL  = 4;
  private static final int TEXT     = 5;
  private static final int ARRAY    = 6;
  private static final int OTHER    = 7;

  private final Object[] values;
  private final int      hash;

  private PostgresCatalogRowKey(final Object[] values) {
    this.values = values;
    int h = values.length;
    for (final Object value : values)
      h = 31 * h + hashOf(value);
    this.hash = h;
  }

  /** The key of a row: its values, in order. */
  static PostgresCatalogRowKey of(final Collection<?> values) {
    final Object[] normalized = new Object[values.size()];
    int i = 0;
    for (final Object value : values)
      normalized[i++] = normalize(value);
    return new PostgresCatalogRowKey(normalized);
  }

  /** The key of a row of one value. */
  static PostgresCatalogRowKey ofValue(final Object value) {
    return new PostgresCatalogRowKey(new Object[] { normalize(value) });
  }

  /** A value as the key holds it: a number by its value, an array by the key of its elements, any other value as it is. */
  private static Object normalize(final Object value) {
    if (value == null || value instanceof Long || value instanceof String || value instanceof Boolean)
      return value;
    if (value instanceof Number number)
      return PostgresCatalogRowSets.numberKey(number);
    if (value instanceof List<?> list)
      return of(list);
    return value;
  }

  private static int hashOf(final Object value) {
    if (value == null)
      return 0;
    if (value instanceof Long number)
      return mix(number);
    if (value instanceof Double number)
      return mix(Double.doubleToLongBits(number));
    return value.hashCode();
  }

  /** The finalizer of MurmurHash3: every bit of the value reaches every bit of the result. */
  private static int mix(long value) {
    value ^= value >>> 33;
    value *= 0xff51afd7ed558ccdL;
    value ^= value >>> 33;
    value *= 0xc4ceb9fe1a85ec53L;
    value ^= value >>> 33;
    return (int) value;
  }

  private static int kindOf(final Object value) {
    if (value instanceof Long)
      return INTEGRAL;
    if (value instanceof String)
      return TEXT;
    if (value == null)
      return NULL;
    if (value instanceof Boolean)
      return BOOLEAN;
    if (value instanceof Double)
      return REAL;
    if (value instanceof BigDecimal)
      return DECIMAL;
    if (value instanceof PostgresCatalogRowKey)
      return ARRAY;
    return OTHER;
  }

  private static int compareValues(final Object left, final Object right) {
    final int leftKind = kindOf(left);
    final int rightKind = kindOf(right);
    if (leftKind != rightKind)
      return Integer.compare(leftKind, rightKind);

    return switch (leftKind) {
      case NULL -> 0;
      case BOOLEAN -> Boolean.compare((Boolean) left, (Boolean) right);
      case INTEGRAL -> Long.compare((Long) left, (Long) right);
      case REAL -> Double.compare((Double) left, (Double) right);
      case DECIMAL -> ((BigDecimal) left).compareTo((BigDecimal) right);
      case TEXT -> ((String) left).compareTo((String) right);
      case ARRAY -> ((PostgresCatalogRowKey) left).compareTo((PostgresCatalogRowKey) right);
      default -> compareOthers(left, right);
    };
  }

  /**
   * A value of a class this catalog does not produce today: equal by {@code equals}, otherwise ordered by class, by its own order when
   * it has one, then by hash code and text. Two different values that agree on every one of those are told apart without a real order,
   * which can only cost a lookup its speed and never its answer.
   */
  @SuppressWarnings("unchecked")
  private static int compareOthers(final Object left, final Object right) {
    if (left.equals(right))
      return 0;
    if (left.getClass() != right.getClass()) {
      final int byClass = left.getClass().getName().compareTo(right.getClass().getName());
      if (byClass != 0)
        return byClass;
    } else if (left instanceof Comparable) {
      final int byValue = ((Comparable<Object>) left).compareTo(right);
      if (byValue != 0)
        return byValue;
    }
    final int byHash = Integer.compare(left.hashCode(), right.hashCode());
    if (byHash != 0)
      return byHash;
    final int byText = String.valueOf(left).compareTo(String.valueOf(right));
    return byText != 0 ? byText : 1;
  }

  @Override
  public int compareTo(final PostgresCatalogRowKey other) {
    if (this == other)
      return 0;
    final int common = Math.min(values.length, other.values.length);
    for (int i = 0; i < common; i++) {
      final int comparison = compareValues(values[i], other.values[i]);
      if (comparison != 0)
        return comparison;
    }
    return Integer.compare(values.length, other.values.length);
  }

  @Override
  public boolean equals(final Object other) {
    return this == other || other instanceof PostgresCatalogRowKey key && hash == key.hash && compareTo(key) == 0;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
