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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a catalog query does to the values of its rows once they are computed (issue #10228): combines the arms
 * of a set operation, tells two rows apart for DISTINCT, GROUP BY and count(DISTINCT), orders values, and computes
 * an aggregate over a group. Nothing here knows where the rows came from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCatalogRowSets {
  private PostgresCatalogRowSets() {
  }

  /**
   * Combines the rows of a set operation's arms, in SQL's precedence: INTERSECT binds tighter than UNION and
   * EXCEPT, which associate to the left.
   */
  static List<List<Object>> combine(final List<String> operators, final List<Boolean> all,
      final List<List<List<Object>>> arms) {
    final List<List<List<Object>>> terms = new ArrayList<>();
    final List<Integer> termOperators = new ArrayList<>();
    List<List<Object>> current = arms.get(0);
    for (int i = 0; i < operators.size(); i++) {
      if ("INTERSECT".equals(operators.get(i)))
        current = combine("INTERSECT", all.get(i), current, arms.get(i + 1));
      else {
        terms.add(current);
        termOperators.add(i);
        current = arms.get(i + 1);
      }
    }
    terms.add(current);

    List<List<Object>> combined = terms.get(0);
    for (int t = 0; t < termOperators.size(); t++) {
      final int i = termOperators.get(t);
      combined = combine(operators.get(i), all.get(i), combined, terms.get(t + 1));
    }
    return combined;
  }

  /**
   * The value of an aggregate over the values a group of rows gave it, NULLs already dropped (array_agg keeps
   * them) and DISTINCT already applied: the same answer PostgreSQL gives over the same values, NULL over none.
   *
   * @param separators string_agg's delimiter of each value's row, null for every other aggregate
   */
  static Object aggregateOf(final String name, final List<Object> values, final List<Object> separators) {
    return switch (name) {
      case "count" -> (long) values.size();
      case "sum", "avg" -> sum(name, values);
      case "min", "max" -> {
        Object extreme = null;
        for (final Object value : values)
          if (extreme == null || ("min".equals(name) ? compareValues(value, extreme) < 0 :
              compareValues(value, extreme) > 0))
            extreme = value;
        yield extreme;
      }
      case "bool_and", "every", "bool_or" -> {
        if (values.isEmpty())
          yield null;
        final boolean and = !"bool_or".equals(name);
        for (final Object value : values)
          if (PostgresCatalogExpression.isTrue(value) != and)
            yield !and;
        yield and;
      }
      case "string_agg" -> {
        if (values.isEmpty())
          yield null;
        final StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
          if (i > 0 && separators.get(i) != null)
            buffer.append(PostgresCatalogExpression.asString(separators.get(i)));
          buffer.append(PostgresCatalogExpression.asString(values.get(i)));
        }
        yield buffer.toString();
      }
      case "array_agg" -> values.isEmpty() ? null : values;
      default -> PostgresCatalogExpression.UNKNOWN;
    };
  }

  /** {@code sum} and {@code avg}: NULL over no value, integral when every value is, as in PostgreSQL's sum. */
  private static Object sum(final String name, final List<Object> values) {
    if (values.isEmpty())
      return null;
    long integral = 0;
    double real = 0;
    boolean allIntegral = true;
    for (final Object value : values) {
      if (!(value instanceof Number number))
        return PostgresCatalogExpression.UNKNOWN;
      if (number instanceof Double || number instanceof Float || number instanceof BigDecimal)
        allIntegral = false;
      else
        integral += number.longValue();
      real += number.doubleValue();
    }
    if ("avg".equals(name))
      return real / values.size();
    return allIntegral ? (Object) integral : (Object) real;
  }

  /** One set operation over two row lists, with SQL's duplicate rules: without ALL the result has no duplicate. */
  private static List<List<Object>> combine(final String operator, final boolean all, final List<List<Object>> left,
      final List<List<Object>> right) {
    final List<List<Object>> result = new ArrayList<>(left.size() + right.size());

    if ("UNION".equals(operator)) {
      if (all) {
        result.addAll(left);
        result.addAll(right);
        return result;
      }
      final Set<List<Object>> seen = new HashSet<>();
      for (final List<List<Object>> side : List.of(left, right))
        for (final List<Object> row : side)
          if (seen.add(distinctKey(row)))
            result.add(row);
      return result;
    }

    final Map<List<Object>, Integer> inRight = new HashMap<>();
    for (final List<Object> row : right)
      inRight.merge(distinctKey(row), 1, Integer::sum);

    final Set<List<Object>> emitted = new HashSet<>();
    final boolean intersect = "INTERSECT".equals(operator);
    for (final List<Object> row : left) {
      final List<Object> key = distinctKey(row);
      final int count = inRight.getOrDefault(key, 0);
      if (all) {
        // The ALL forms are bag operations: each row of the right side matches one row of the left at most.
        if (count > 0)
          inRight.put(key, count - 1);
        if (intersect == (count > 0))
          result.add(row);
      } else if (intersect == (count > 0) && emitted.add(key))
        result.add(row);
    }
    return result;
  }

  /**
   * The key two rows are compared by when a set operation, DISTINCT, GROUP BY or count(DISTINCT) decides whether
   * they are the same row. Numbers compare by value, as in SQL: the literal {@code 1} is a Long and an OID is an
   * Integer, and {@code List.equals} would tell them apart.
   */
  static List<Object> distinctKey(final List<Object> values) {
    List<Object> key = null;
    for (int i = 0; i < values.size(); i++) {
      final Object value = values.get(i);
      if (!(value instanceof Number number) || value instanceof Long)
        continue;
      if (key == null)
        key = new ArrayList<>(values);
      key.set(i, numberKey(number));
    }
    return key == null ? values : key;
  }

  /**
   * An integral value as a Long, whatever type carries it; a BigDecimal or BigInteger outside long's range by its
   * exact decimal value, so two that differ below a double's precision stay apart; any other number as a Double.
   */
  static Object numberKey(final Number number) {
    if (number instanceof Integer || number instanceof Short || number instanceof Byte)
      return number.longValue();
    if (number instanceof BigDecimal || number instanceof BigInteger) {
      final BigDecimal exact = (number instanceof BigDecimal decimal ? decimal : new BigDecimal((BigInteger) number))
          .stripTrailingZeros();
      if (exact.scale() <= 0)
        try {
          return exact.longValueExact();
        } catch (final ArithmeticException e) {
          // beyond long: the decimal value itself is the key
        }
      return exact;
    }
    final double d = number.doubleValue();
    return d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 9.0E15 ? (Object) (long) d : (Object) d;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  static int compareValues(final Object left, final Object right) {
    if (left == null && right == null)
      return 0;
    // PostgreSQL sorts nulls last by default in ascending order.
    if (left == null)
      return 1;
    if (right == null)
      return -1;
    if (left instanceof Number l && right instanceof Number r)
      return Double.compare(l.doubleValue(), r.doubleValue());
    if (left instanceof Comparable && left.getClass() == right.getClass())
      return ((Comparable) left).compareTo(right);
    return String.valueOf(left).compareTo(String.valueOf(right));
  }
}
