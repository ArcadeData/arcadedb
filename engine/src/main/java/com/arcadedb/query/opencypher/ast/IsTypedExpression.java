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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;

/**
 * GQL value-type predicate {@code <expr> IS [NOT] TYPED <type>} (issue #3365 section 3.3).
 * Also surfaced as the shorthand {@code <expr> :: <type>}.
 *
 * <p>Per ISO/IEC 39075:2024, a value conforms to a declared type when its actual value-type
 * is a subtype of the declared type, with NULL conforming to any nullable declared type and
 * to no NOT NULL declared type. The numeric subtype hierarchy is
 * {@code INT8 < INT16 < INT32 < INT64} for signed integers and {@code FLOAT32 < FLOAT64}
 * for floats; widening conversions are accepted but narrowing ones are not. The Java runtime
 * type of the value carries the precision (Byte/Short/Integer/Long, Float/Double).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IsTypedExpression implements BooleanExpression {
  private final Expression valueExpression;
  private final String     normalizedTypeName;
  private final String     listElementTypeName;
  private final boolean    requiresNonNull;
  private final boolean    isNot;

  public IsTypedExpression(final Expression valueExpression, final String normalizedTypeName,
      final String listElementTypeName, final boolean requiresNonNull, final boolean isNot) {
    this.valueExpression = valueExpression;
    this.normalizedTypeName = normalizedTypeName;
    this.listElementTypeName = listElementTypeName;
    this.requiresNonNull = requiresNonNull;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = valueExpression.evaluate(result, context);
    final boolean conforms = conforms(value);
    return isNot != conforms;
  }

  private boolean conforms(final Object value) {
    if (value == null)
      return !requiresNonNull && !"NOTHING".equals(normalizedTypeName);

    return switch (normalizedTypeName) {
      case "ANY", "ANY VALUE", "PROPERTY VALUE" -> true;
      case "BOOL", "BOOLEAN" -> value instanceof Boolean;
      // GQL signed-integer subtype hierarchy: a Java Byte is also INT16/INT32/INT64/INTEGER,
      // but a Java Long is not INT8. INT (without suffix) aliases INT32 per Cypher 25.
      case "INT8", "INTEGER8" -> value instanceof Byte;
      case "INT16", "INTEGER16" -> value instanceof Byte || value instanceof Short;
      case "INT", "INT32", "INTEGER32" ->
          value instanceof Byte || value instanceof Short || value instanceof Integer;
      case "INT64", "INTEGER64", "INTEGER", "SIGNED INTEGER" ->
          value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
      // GQL float subtype hierarchy: Java Float is FLOAT32 (and therefore also FLOAT64/FLOAT),
      // Java Double is FLOAT64/FLOAT but not FLOAT32.
      case "FLOAT32" -> value instanceof Float;
      case "FLOAT64", "FLOAT" -> value instanceof Float || value instanceof Double;
      case "VARCHAR", "STRING" -> value instanceof String;
      case "DATE" -> value instanceof LocalDate;
      case "LOCAL DATETIME", "LOCAL TIMESTAMP" -> value instanceof LocalDateTime;
      case "ZONED DATETIME", "ZONED TIMESTAMP" -> value instanceof ZonedDateTime;
      case "DURATION" -> value instanceof Duration;
      case "POINT" -> value.getClass().getSimpleName().toLowerCase().contains("point");
      case "MAP" -> value instanceof Map;
      case "LIST", "ARRAY" -> isListMatch(value);
      case "NODE", "VERTEX", "ANY NODE", "ANY VERTEX" -> value instanceof Vertex;
      case "RELATIONSHIP", "EDGE", "ANY RELATIONSHIP", "ANY EDGE" -> value instanceof Edge;
      case "ELEMENT" -> value instanceof Document;
      case "PATH", "PATHS" -> value instanceof Collection || value.getClass().isArray();
      case "NULL", "NOTHING" -> false;
      default -> false;
    };
  }

  private boolean isListMatch(final Object value) {
    if (!(value instanceof Collection<?>) && !value.getClass().isArray())
      return false;
    if (listElementTypeName == null)
      return true;
    final Iterable<?> elements;
    if (value instanceof Collection<?> coll)
      elements = coll;
    else if (value instanceof Object[] arr)
      elements = java.util.Arrays.asList(arr);
    else
      return false;
    final IsTypedExpression elementCheck = new IsTypedExpression(null, listElementTypeName, null, false, false);
    for (final Object e : elements)
      if (!elementCheck.conforms(e))
        return false;
    return true;
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    sb.append(valueExpression.getText());
    sb.append(isNot ? " IS NOT TYPED " : " IS TYPED ");
    sb.append(normalizedTypeName);
    if (listElementTypeName != null)
      sb.append("<").append(listElementTypeName).append(">");
    if (requiresNonNull)
      sb.append(" NOT NULL");
    return sb.toString();
  }

  public Expression getValueExpression() {
    return valueExpression;
  }

  public String getNormalizedTypeName() {
    return normalizedTypeName;
  }

  public String getListElementTypeName() {
    return listElementTypeName;
  }

  public boolean requiresNonNull() {
    return requiresNonNull;
  }

  public boolean isNot() {
    return isNot;
  }
}
