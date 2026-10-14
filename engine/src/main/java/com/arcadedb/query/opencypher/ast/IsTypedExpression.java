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
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Arrays;
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
  private final Expression     valueExpression;
  private final CypherTypeName type;
  private final boolean        isNot;

  public IsTypedExpression(final Expression valueExpression, final CypherTypeName type, final boolean isNot) {
    this.valueExpression = valueExpression;
    this.type = type;
    this.isNot = isNot;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = valueExpression.evaluate(result, context);
    final boolean conforms = conforms(value, type.name(), type.nonNull(), type.elementName(), type.elementNonNull());
    return isNot != conforms;
  }

  /** Whether the value conforms to the type, as {@code value IS :: type} answers it. */
  public static boolean conforms(final Object value, final CypherTypeName type) {
    return conforms(value, type.name(), type.nonNull(), type.elementName(), type.elementNonNull());
  }

  /**
   * Whether the value conforms to the type. The temporal types, DURATION and POINT are answered by the same classification
   * {@code valueType()} reports ({@link CypherFunctionHelper#cypherTypeName}), so the two cannot disagree: they used to test
   * the {@code java.time} classes, which no Cypher temporal value is, and answered false for {@code date() IS :: DATE} (#9709).
   */
  private static boolean conforms(final Object value, final String name, final boolean nonNull, final String elementName,
      final boolean elementNonNull) {
    if (value == null)
      return !nonNull && !"NOTHING".equals(name);

    return switch (name) {
      case "ANY" -> true;
      case "BOOLEAN" -> value instanceof Boolean;
      // GQL signed-integer subtype hierarchy: a Java Byte is also INT16/INT32/INTEGER,
      // but a Java Long is not INT8. INT (without suffix) aliases INT32 per Cypher 25.
      case "INT8" -> value instanceof Byte;
      case "INT16" -> value instanceof Byte || value instanceof Short;
      case "INT32" -> value instanceof Byte || value instanceof Short || value instanceof Integer;
      case "INTEGER" -> value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
      // GQL float subtype hierarchy: Java Float is FLOAT32 (and therefore also FLOAT),
      // Java Double is FLOAT but not FLOAT32.
      case "FLOAT32" -> value instanceof Float;
      case "FLOAT" -> value instanceof Float || value instanceof Double;
      case "STRING" -> value instanceof String;
      case "DATE", "LOCAL DATETIME", "ZONED DATETIME", "LOCAL TIME", "ZONED TIME", "DURATION", "POINT" ->
          name.equals(CypherFunctionHelper.cypherTypeName(value));
      // a point is a map at runtime, but it is a POINT and not a MAP to the type system
      case "MAP" -> value instanceof Map && !"POINT".equals(CypherFunctionHelper.cypherTypeName(value));
      case CypherTypeName.LIST -> isListMatch(value, elementName, elementNonNull);
      case "NODE" -> value instanceof Vertex;
      case "RELATIONSHIP" -> value instanceof Edge;
      case "ELEMENT" -> value instanceof Document;
      // a path is a value of its own, not any list of elements (issue #9940)
      case "PATH" -> CypherFunctionHelper.isPathValue(value);
      default -> false;
    };
  }

  private static boolean isListMatch(final Object value, final String elementName, final boolean elementNonNull) {
    // a path is a PATH, not a LIST, even when it is carried by a list (issue #9940)
    if (CypherFunctionHelper.isPathValue(value))
      return false;
    if (!(value instanceof Collection<?>) && !value.getClass().isArray())
      return false;
    if (elementName == null)
      return true;
    final Iterable<?> elements;
    if (value instanceof Collection<?> coll)
      elements = coll;
    else if (value instanceof Object[] arr)
      elements = Arrays.asList(arr);
    else
      return false;
    for (final Object e : elements)
      if (!conforms(e, elementName, elementNonNull, null, false))
        return false;
    return true;
  }

  @Override
  public String getText() {
    return valueExpression.getText() + (isNot ? " IS NOT TYPED " : " IS TYPED ") + type;
  }

  public Expression getValueExpression() {
    return valueExpression;
  }

  public CypherTypeName getType() {
    return type;
  }

  public boolean isNot() {
    return isNot;
  }
}
