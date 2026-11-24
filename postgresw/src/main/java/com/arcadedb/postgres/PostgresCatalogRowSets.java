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
 * of a set operation (telling two rows apart by {@link PostgresCatalogRowKey}), orders values, and computes
 * an aggregate over a group. Nothing here knows where the rows came from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresCatalogRowSets {
  private PostgresCatalogRowSets() {
  }

  /**
   * Combines the rows of a set operation's arms, in SQL's precedence: INTERSECT binds tighter than UNION and
   * EXCEPT, which associate to the left. The arms are left as they were.
   * <p>
   * The work of every step is charged to {@code budget} before the step runs, by the rows it reads: a chain that costs more than the
   * statement may stops there. A run of UNION or UNION ALL steps grows one list, and a run of UNION steps keeps one set of the rows
   * it holds, so such a chain costs the rows of its arms and not the square of their number.
   *
   * @param budget the work budget of the statement
   */
  static List<List<Object>> combine(final List<String> operators, final List<Boolean> all,
      final List<List<List<Object>>> arms, final PostgresCatalogJoin.Budget budget) {
    final List<List<List<Object>>> terms = new ArrayList<>();
    final List<Integer> termOperators = new ArrayList<>();
    List<List<Object>> current = arms.get(0);
    for (int i = 0; i < operators.size(); i++) {
      if ("INTERSECT".equals(operators.get(i)))
        current = intersectOrExcept("INTERSECT", all.get(i), current, arms.get(i + 1), budget);
      else {
        terms.add(current);
        termOperators.add(i);
        current = arms.get(i + 1);
      }
    }
    terms.add(current);

    List<List<Object>> combined = terms.get(0);
    // whether combined is a list of this method's own, which a step may grow, and the keys of its rows when it has no duplicate
    boolean owned = false;
    Set<PostgresCatalogRowKey> keys = null;
    for (int t = 0; t < termOperators.size(); t++) {
      final int i = termOperators.get(t);
      final List<List<Object>> right = terms.get(t + 1);

      if (!"UNION".equals(operators.get(i))) {
        combined = intersectOrExcept(operators.get(i), all.get(i), combined, right, budget);
        owned = true;
        keys = null;
      } else if (all.get(i)) {
        if (owned)
          budget.charge(right.size());
        else {
          charge(budget, combined.size(), right.size());
          final List<List<Object>> grown = new ArrayList<>(combined.size() + right.size());
          grown.addAll(combined);
          combined = grown;
          owned = true;
        }
        combined.addAll(right);
        if (!right.isEmpty())
          keys = null;
      } else {
        if (keys == null) {
          // the rows so far are kept once each, and their keys are what the next steps compare with
          charge(budget, combined.size(), right.size());
          final Set<PostgresCatalogRowKey> seen = new HashSet<>();
          final List<List<Object>> distinct = new ArrayList<>(combined.size() + right.size());
          for (final List<Object> row : combined)
            if (seen.add(PostgresCatalogRowKey.of(row)))
              distinct.add(row);
          combined = distinct;
          owned = true;
          keys = seen;
        } else
          budget.charge(right.size());
        for (final List<Object> row : right)
          if (keys.add(PostgresCatalogRowKey.of(row)))
            combined.add(row);
      }
    }
    return combined;
  }

  private static void charge(final PostgresCatalogJoin.Budget budget, final int left, final int right) {
    budget.charge((int) Math.min((long) left + right, Integer.MAX_VALUE));
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

  /**
   * An INTERSECT or an EXCEPT over two row lists, with SQL's duplicate rules: without ALL the result has no duplicate. It reads every
   * row of both lists, which is charged before it does.
   */
  private static List<List<Object>> intersectOrExcept(final String operator, final boolean all, final List<List<Object>> left,
      final List<List<Object>> right, final PostgresCatalogJoin.Budget budget) {
    charge(budget, left.size(), right.size());
    final List<List<Object>> result = new ArrayList<>();

    final Map<PostgresCatalogRowKey, Integer> inRight = new HashMap<>();
    for (final List<Object> row : right)
      inRight.merge(PostgresCatalogRowKey.of(row), 1, Integer::sum);

    final Set<PostgresCatalogRowKey> emitted = new HashSet<>();
    final boolean intersect = "INTERSECT".equals(operator);
    for (final List<Object> row : left) {
      final PostgresCatalogRowKey key = PostgresCatalogRowKey.of(row);
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
