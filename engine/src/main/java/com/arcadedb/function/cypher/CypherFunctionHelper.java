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
package com.arcadedb.function.cypher;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
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
import com.arcadedb.query.sql.executor.MultiValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
   * Returns the Cypher name of the runtime type of a value: INTEGER, FLOAT, STRING, BOOLEAN, LIST&lt;ANY&gt;, MAP,
   * NODE, RELATIONSHIP or NULL. Used to phrase type errors (and by valueType()) with the vocabulary of the
   * language instead of Java class names.
   */
  public static String cypherTypeName(final Object value) {
    return switch (value) {
      case null -> "NULL";
      case Long ignored -> "INTEGER";
      case Integer ignored -> "INTEGER";
      case Short ignored -> "INTEGER";
      case Byte ignored -> "INTEGER";
      case Double ignored -> "FLOAT";
      case Float ignored -> "FLOAT";
      case String ignored -> "STRING";
      case Boolean ignored -> "BOOLEAN";
      case Vertex ignored -> "NODE";
      case Edge ignored -> "RELATIONSHIP";
      case List ignored -> "LIST<ANY>";
      case Map ignored -> "MAP";
      default -> value.getClass().isArray() ? "LIST<ANY>" : value.getClass().getSimpleName().toUpperCase();
    };
  }

  /**
   * Resolves the single argument of a LIST-typed Cypher function (head(), last(), tail(), ...) to a List.
   * <p>
   * Cypher declares those functions as {@code f(list :: LIST<ANY>)}, so anything that is not a list is a
   * client-facing type error in Neo4j and Memgraph: answering {@code null} instead would make a wrong query
   * look like a successful one with no value (issue #5476). {@code null} is the one exception, because Cypher
   * null semantics propagate it through every function.
   *
   * @return the argument as a List, or {@code null} when the argument itself is {@code null}
   *
   * @throws CommandSemanticException when the argument is neither {@code null} nor a list
   */
  public static List<Object> requireListArgument(final Object value, final String functionName) {
    if (value == null)
      return null;

    final List<Object> list = asListOrNull(value);
    if (list != null)
      return list;

    throw typeMismatch(functionName, "a LIST<ANY>", value);
  }

  /**
   * Returns the value as a List when Cypher considers it a LIST, or {@code null} when it is anything else, so the caller can
   * decide whether that is a type error (head(), last(), tail()) or just another accepted shape (size(), which also takes
   * STRING and MAP).
   * <p>
   * A MAP is not a LIST here: iterating a map yields nothing meaningful for a list function. Functions that do accept maps
   * handle them before calling this method.
   */
  public static List<Object> asListOrNull(final Object value) {
    if (value == null || value instanceof Map)
      return null;

    // Accept List/Collection/array (incl. primitive arrays from numeric-array parameters, issue #4284).
    final List<Object> list = MultiValue.getMultiValueAsList(value);
    if (list != null)
      return list;

    // Lazily-produced sequences are lists too, once materialized. Identifiable is excluded on purpose: a
    // record is a NODE/RELATIONSHIP even when it happens to be iterable.
    if (!(value instanceof Identifiable)) {
      if (value instanceof Iterable<?> iterable)
        return materialize(iterable.iterator());
      if (value instanceof Iterator<?> iterator)
        return materialize(iterator);
    }

    return null;
  }

  /**
   * Builds the error raised when a function is handed an argument outside its input domain, e.g. {@code size(42)}
   * (issue #5477) or {@code head(42)} (issue #5476). Answering {@code null} instead would be indistinguishable from legal
   * Cypher null propagation, so a wrong query would look like a successful one. A {@link CommandSemanticException} makes the
   * HTTP layer report 400 Bad Request with this message rather than a 500.
   *
   * @param functionName  function name without parentheses, e.g. {@code "size"}
   * @param expectedTypes the input domain, phrased for the message, e.g. {@code "a STRING, a LIST<ANY> or a MAP"}
   * @param value         the offending argument (never {@code null}: null propagation is not a type error)
   */
  public static CommandSemanticException typeMismatch(final String functionName, final String expectedTypes,
      final Object value) {
    return new CommandSemanticException(
        "Type mismatch: " + functionName + "() expects " + expectedTypes + " argument but got " + cypherTypeName(value));
  }

  private static List<Object> materialize(final Iterator<?> iterator) {
    final List<Object> list = new ArrayList<>();
    while (iterator.hasNext())
      list.add(iterator.next());
    return list;
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
    if (val instanceof ZonedDateTime)
      return new CypherDateTime((ZonedDateTime) val);
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
      date = date.with(WeekFields.ISO.dayOfWeek(), ((Number) value).longValue());
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

}
