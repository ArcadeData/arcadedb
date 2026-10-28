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
package com.arcadedb.query.facet;

import com.arcadedb.index.columnar.DictionaryValueCodec;
import com.arcadedb.schema.Type;

import java.util.Comparator;

/**
 * Total order of facet values, the same as the dictionary's so the order of a single bucket (by rank) and the order of merged or
 * patched values agree: strings, numbers (exact across LONG, DECIMAL and DOUBLE, NaN last), booleans, RIDs; null last. Numbers are
 * first normalized with {@link Type#normalizeNumberForKey}, as the dictionary and GROUP BY keys are, so raw values (a Float, an
 * Integer, {@code -0.0d}, a BigDecimal of any scale) order exactly as their normalized form. A value of any other class sorts after
 * those, by class name, then (two values of one {@link Comparable} class, such as dates and times) by their own order, else by
 * {@code toString}.
 * <p>
 * {@link #compare} normalizes both sides on every call; a caller sorting many values calls {@link #normalize} once per value and
 * {@link #compareNormalized} on the results.
 */
public final class FacetValueComparator implements Comparator<Object> {
  public static final FacetValueComparator INSTANCE = new FacetValueComparator();

  private FacetValueComparator() {
  }

  /** The canonical form of a facet value (null stays null; only numbers change). */
  public static Object normalize(final Object value) {
    // a non-finite Float is kept as a Float by normalizeNumberForKey: as its Double it sorts with the other numbers
    if (value instanceof Float f && !Float.isFinite(f))
      return f.doubleValue();
    return Type.normalizeNumberForKey(value);
  }

  @Override
  public int compare(final Object a, final Object b) {
    return compareNormalized(normalize(a), normalize(b));
  }

  /** Compares two values already passed through {@link #normalize}. */
  public int compareNormalized(final Object x, final Object y) {
    if (x == null || y == null)
      return x == y ? 0 : x == null ? 1 : -1;
    final boolean supportedX = DictionaryValueCodec.isSupported(x);
    final boolean supportedY = DictionaryValueCodec.isSupported(y);
    if (supportedX && supportedY)
      return DictionaryValueCodec.compareValues(x, y);
    if (supportedX != supportedY)
      return supportedX ? -1 : 1;
    final int byClass = x.getClass().getName().compareTo(y.getClass().getName());
    if (byClass != 0)
      return byClass;
    // two values of one comparable class (a Date, a LocalDate, a LocalDateTime, ...) in their own order: time order for temporals
    if (x instanceof Comparable)
      return compareSameClass(x, y);
    return x.toString().compareTo(y.toString());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static int compareSameClass(final Object x, final Object y) {
    return ((Comparable) x).compareTo(y);
  }
}
