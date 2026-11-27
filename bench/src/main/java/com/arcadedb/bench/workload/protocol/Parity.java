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
package com.arcadedb.bench.workload.protocol;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Compares the rows two protocols answered to the same query. Each protocol hands values back in its own Java types (an int
 * over one, a long over another, a float[] here and a list of doubles there), so both sides are normalised first.
 */
public final class Parity {
  private Parity() {
  }

  /**
   * The value with its protocol-specific type removed: any integral number becomes a {@link Long}, any floating number a
   * {@link Double} (a {@link Float}, and a {@link Double} that holds a float value exactly, through the float's shortest decimal
   * form: Bolt widens a stored 0.1f to 0.10000000149011612 where a text protocol says 0.1, and both become 0.1; a double that is
   * no float value, as 0.1 itself, keeps its precision), arrays
   * and collections a {@link List} of normalised elements, maps a {@link TreeMap} of normalised values. Strings and anything else
   * are left as they are.
   */
  public static Object normalise(final Object value) {
    return switch (value) {
      case null -> null;
      case String s -> s;
      case Long l -> l;
      case Integer i -> i.longValue();
      case Short s -> s.longValue();
      case Byte b -> b.longValue();
      case Double d -> (double) (float) d.doubleValue() == d ? shortestFloat(d.floatValue()) : d;
      case Float f -> shortestFloat(f);
      case BigDecimal d -> d.doubleValue();
      case BigInteger i -> i.bitLength() < 64 ? (Object) i.longValue() : i;
      case Number n -> n.longValue(); // AtomicInteger, AtomicLong, LongAdder: the other JDK numbers are integral
      case Map<?, ?> map -> {
        final TreeMap<String, Object> sorted = new TreeMap<>();
        for (final Map.Entry<?, ?> e : map.entrySet())
          sorted.put(String.valueOf(e.getKey()), normalise(e.getValue()));
        yield sorted;
      }
      case Collection<?> collection -> {
        final List<Object> list = new ArrayList<>(collection.size());
        for (final Object element : collection)
          list.add(normalise(element));
        yield list;
      }
      default -> {
        if (!value.getClass().isArray())
          yield value;
        final int length = Array.getLength(value);
        final List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
          list.add(normalise(Array.get(value, i)));
        yield list;
      }
    };
  }

  private static Double shortestFloat(final float f) {
    return Double.parseDouble(Float.toString(f));
  }

  /**
   * Null when both answers hold the same rows after {@link #normalise(Object)}, else a one-line description of the first
   * difference: the row index, the column and both values. With {@code ordered} false the rows are compared as multisets (both
   * sides sorted by their normalised text first, so the row index of a difference is the index in that order).
   */
  public static String diff(final List<Map<String, Object>> expected, final List<Map<String, Object>> got, final boolean ordered) {
    final List<TreeMap<String, Object>> left = normaliseRows(expected, ordered);
    final List<TreeMap<String, Object>> right = normaliseRows(got, ordered);
    final String order = ordered ? "" : " (rows sorted)";

    final int common = Math.min(left.size(), right.size());
    for (int i = 0; i < common; i++) {
      final TreeMap<String, Object> l = left.get(i);
      final TreeMap<String, Object> r = right.get(i);
      final TreeSet<String> columns = new TreeSet<>(l.keySet());
      columns.addAll(r.keySet());
      for (final String column : columns) {
        if (!r.containsKey(column))
          return "row " + i + order + ": column '" + column + "' missing, expected " + l.get(column);
        if (!l.containsKey(column))
          return "row " + i + order + ": column '" + column + "' unexpected, got " + r.get(column);
        if (!Objects.equals(l.get(column), r.get(column)))
          return "row " + i + order + ", column '" + column + "': expected " + describe(l.get(column)) + ", got " + describe(
              r.get(column));
      }
    }
    if (left.size() > common)
      return "row " + common + order + ": expected " + left.get(common) + ", got no row (" + left.size() + " rows expected, "
          + right.size() + " got)";
    if (right.size() > common)
      return "row " + common + order + ": expected no row, got " + right.get(common) + " (" + left.size() + " rows expected, "
          + right.size() + " got)";
    return null;
  }

  private static List<TreeMap<String, Object>> normaliseRows(final List<Map<String, Object>> rows, final boolean ordered) {
    final List<TreeMap<String, Object>> result = new ArrayList<>(rows.size());
    for (final Map<String, Object> row : rows) {
      @SuppressWarnings("unchecked")
      final TreeMap<String, Object> normalised = (TreeMap<String, Object>) normalise(row);
      result.add(normalised);
    }
    if (!ordered)
      result.sort(Comparator.comparing(Parity::canonical));
    return result;
  }

  /** The text a row is sorted by: its values with their types, so 1 and "1" do not sort as equals by accident. */
  private static String canonical(final Object value) {
    return switch (value) {
      case null -> "null";
      case Map<?, ?> map -> {
        final StringBuilder b = new StringBuilder("{");
        for (final Map.Entry<?, ?> e : map.entrySet())
          b.append(e.getKey()).append('=').append(canonical(e.getValue())).append(',');
        yield b.append('}').toString();
      }
      case List<?> list -> {
        final StringBuilder b = new StringBuilder("[");
        for (final Object e : list)
          b.append(canonical(e)).append(',');
        yield b.append(']').toString();
      }
      default -> value.getClass().getSimpleName() + ':' + value;
    };
  }

  private static String describe(final Object value) {
    return value == null ? "null" : value + " (" + value.getClass().getSimpleName() + ")";
  }
}
