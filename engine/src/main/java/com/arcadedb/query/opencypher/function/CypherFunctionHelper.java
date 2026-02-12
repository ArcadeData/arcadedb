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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class with shared static helper methods for Cypher functions.
 */
public final class CypherFunctionHelper {

  private CypherFunctionHelper() {
    // utility class
  }

  /**
   * Returns the Cypher type order rank for mixed-type comparison.
   * Order: MAP(0) < NODE(1) < RELATIONSHIP(2) < LIST(3) < PATH(4) < STRING(5) < BOOLEAN(6) < NUMBER(7) < NaN(8) < NULL(9)
   */
  public static int cypherTypeRank(final Object value) {
    if (value == null)
      return 9;
    if (value instanceof Number) {
      final double d = ((Number) value).doubleValue();
      return Double.isNaN(d) ? 8 : 7;
    }
    if (value instanceof Boolean)
      return 6;
    if (value instanceof String)
      return 5;
    if (value instanceof List)
      return 3;
    if (value instanceof Vertex)
      return 1;
    if (value instanceof Edge)
      return 2;
    if (value instanceof Map)
      return 0;
    return 4; // path or other
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static int cypherCompare(final Object a, final Object b) {
    if (a == null && b == null)
      return 0;
    if (a == null)
      return 1;
    if (b == null)
      return -1;
    final int rankA = cypherTypeRank(a);
    final int rankB = cypherTypeRank(b);
    if (rankA != rankB)
      return Integer.compare(rankA, rankB);
    // Same type category - compare within type
    if (a instanceof Number && b instanceof Number)
      return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
    if (a instanceof String && b instanceof String)
      return ((String) a).compareTo((String) b);
    if (a instanceof Boolean && b instanceof Boolean)
      return Boolean.compare((Boolean) a, (Boolean) b);
    if (a instanceof List && b instanceof List) {
      final List<?> la = (List<?>) a;
      final List<?> lb = (List<?>) b;
      for (int i = 0; i < Math.min(la.size(), lb.size()); i++) {
        final int cmp = cypherCompare(la.get(i), lb.get(i));
        if (cmp != 0)
          return cmp;
      }
      return Integer.compare(la.size(), lb.size());
    }
    if (a instanceof Comparable && b instanceof Comparable) {
      try {
        return ((Comparable) a).compareTo(b);
      } catch (final ClassCastException e) {
        return 0;
      }
    }
    return 0;
  }

  /**
   * Get or initialize statement time for temporal constructors.
   * In Cypher, temporal functions like date(), localtime(), etc. should return the same
   * frozen time throughout the entire query execution to ensure consistent results.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> getStatementTime(final CommandContext context) {
    Map<String, Object> statementTime = (Map<String, Object>) context.getVariable("$statementTime");
    if (statementTime == null) {
      // First call - freeze the current time
      statementTime = new HashMap<>();
      statementTime.put("date", CypherDate.now());
      statementTime.put("localtime", CypherLocalTime.now());
      statementTime.put("time", CypherTime.now());
      statementTime.put("localdatetime", CypherLocalDateTime.now());
      statementTime.put("datetime", CypherDateTime.now());
      context.setVariable("$statementTime", statementTime);
    }
    return statementTime;
  }

  /**
   * Wrap a java.time value from ArcadeDB storage into the corresponding CypherTemporalValue.
   */
  public static CypherTemporalValue wrapTemporal(final Object val) {
    if (val instanceof CypherTemporalValue)
      return (CypherTemporalValue) val;
    if (val instanceof LocalDate)
      return new CypherDate((LocalDate) val);
    if (val instanceof LocalDateTime)
      return new CypherLocalDateTime((LocalDateTime) val);
    if (val instanceof java.time.ZonedDateTime)
      return new CypherDateTime((java.time.ZonedDateTime) val);
    throw new CommandExecutionException("Expected temporal value but got: " + (val == null ? "null" : val.getClass().getSimpleName()));
  }

  /**
   * Apply a map of adjustments to a truncated date.
   * The map can contain: year, month, day, dayOfWeek, ordinalDay, dayOfQuarter.
   */
  public static LocalDate applyDateMap(LocalDate date, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return date;
    // Optimized: single map lookup instead of containsKey() + get()
    Object value = map.get("year");
    if (value != null)
      date = date.withYear(((Number) value).intValue());
    value = map.get("month");
    if (value != null)
      date = date.withMonth(((Number) value).intValue());
    value = map.get("day");
    if (value != null)
      date = date.withDayOfMonth(((Number) value).intValue());
    value = map.get("dayOfWeek");
    if (value != null)
      date = date.with(java.time.temporal.WeekFields.ISO.dayOfWeek(), ((Number) value).longValue());
    return date;
  }

  /**
   * Apply a map of adjustments to a truncated time.
   */
  public static LocalTime applyTimeMap(LocalTime time, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return time;
    // Optimized: single map lookup instead of containsKey() + get()
    Object value = map.get("hour");
    if (value != null)
      time = time.withHour(((Number) value).intValue());
    value = map.get("minute");
    if (value != null)
      time = time.withMinute(((Number) value).intValue());
    value = map.get("second");
    if (value != null)
      time = time.withSecond(((Number) value).intValue());
    time = time.withNano(TemporalUtil.computeNanos(map, time.getNano()));
    return time;
  }

  /**
   * Apply a map of adjustments to a truncated local datetime.
   */
  public static LocalDateTime applyDateTimeMap(LocalDateTime dt, final Map<String, Object> map) {
    if (map == null || map.isEmpty())
      return dt;
    final LocalDate date = applyDateMap(dt.toLocalDate(), map);
    final LocalTime time = applyTimeMap(dt.toLocalTime(), map);
    return LocalDateTime.of(date, time);
  }

  /**
   * Convert a List or array argument to a float array for vector operations.
   */
  public static float[] toFloatArray(final Object value) {
    if (value instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        if (list.get(i) instanceof Number)
          result[i] = ((Number) list.get(i)).floatValue();
        else
          throw new CommandExecutionException("Vector elements must be numeric");
      }
      return result;
    }
    if (value instanceof float[])
      return (float[]) value;
    if (value instanceof double[]) {
      final double[] d = (double[]) value;
      final float[] result = new float[d.length];
      for (int i = 0; i < d.length; i++)
        result[i] = (float) d[i];
      return result;
    }
    throw new CommandExecutionException("Vector argument must be a list of numbers");
  }
}
