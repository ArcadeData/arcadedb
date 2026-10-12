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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.math.BigInteger;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves and applies the inline property map of a graph pattern - the {@code {k: v}} in
 * {@code (n:Person {name: $name})} or {@code -[:TRANSFER {transactionId: row.id}]->}.
 * <p>
 * A declared value reaches the matcher in one of three shapes, depending on which parser built the
 * pattern and on what was written: a plain literal, a {@link CypherASTBuilder.ParameterReference}
 * (how {@code CypherASTBuilder} encodes {@code $param}) or a whole {@link Expression}
 * ({@code CypherExpressionBuilder} keeps every non-literal as one, including {@code $param}, and
 * both parsers keep dynamic values such as {@code row.id} or {@code toUpper(x)} that way). Only the
 * first shape can be compared as-is; the other two have to be resolved against the current row and
 * the query parameters first.
 * <p>
 * Every pattern evaluator used to carry its own copy of that resolution, and the copies disagreed
 * on which shapes they knew about, so the same filter silently matched nothing depending on where
 * it was written: a {@code $param} inside a pattern comprehension, or a variable reference inside a
 * relationship property map, matched no record at all while the very same filter written as a
 * literal matched (issue #5501). An inline map is a filter, so a value it cannot resolve produces
 * an empty result rather than an error - which is exactly why the divergence stayed invisible.
 * Keeping one implementation keeps the spellings honest with each other.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class InlineProperties {

  /** Stand-in row for the pattern evaluators that have no input row; a parameter still resolves against it. */
  private static final Result NO_ROW = new ResultInternal(Collections.emptyMap());

  private InlineProperties() {
  }

  /**
   * Returns true if the record satisfies every entry of the inline property map. A null or empty map
   * declares no constraint and matches anything.
   *
   * @param row the bindings visible where the pattern is evaluated, used to resolve dynamic values such as
   *            {@code row.id}; may be null when the pattern is evaluated without an input row
   */
  public static boolean matches(final Document record, final Map<String, Object> properties, final Result row,
      final CommandContext context) {
    if (properties == null || properties.isEmpty())
      return true;

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      if (!matchesProperty(record, entry.getKey(), resolve(entry.getValue(), row, context)))
        return false;
    }
    return true;
  }

  /**
   * Returns true if the property {@code key} of the record satisfies an already resolved expected value: the comparison of
   * {@link #matchesResolvedValue(Object, Object)}, plus what only the record can tell about the stored value:
   * <ul>
   * <li>its declaration, which defines the text form of the property's values in both directions: a property declared
   * {@code STRING} stores the text of whatever is written into it ({@code '1'} for {@code 1}), and any other declaration
   * reads a text written into it as its own type ({@code 5} for {@code '5'} in an INTEGER). The same pattern has to find
   * that value again, or a repeated MERGE duplicates the node (issues #9695, #9794), see
   * {@link #declaredValue(Document, String, Object)};</li>
   * <li>a DATE that {@code arcadedb.dateImplementation} read back as an instant still compares as a date (issue #9647);</li>
   * <li>the ISO text a release before the native temporal types stored a temporal as is that temporal, as a property read
   * already restores it (issue #9806).</li>
   * </ul>
   * Every pattern path (node, relationship, variable-length, pattern predicate and comprehension, MERGE,
   * {@code merge.relationship}) compares through here, so none of them can miss one of the three.
   */
  public static boolean matchesProperty(final Document record, final String key, final Object expected) {
    Object actual = record.get(key);
    if (expected instanceof CypherTemporalValue) {
      // Only looked up against a temporal, so any other entry pays nothing
      if (actual instanceof String text)
        actual = TemporalUtil.restoreLegacyText(record, key, text);
      if (matchesResolvedValue(TemporalUtil.asDeclaredDate(record, key, actual), expected))
        return true;
    } else if (matchesResolvedValue(actual, expected))
      return true;
    // Only a text met by a non-text pays for the schema lookup
    return isTextAgainstNonText(actual, expected) && matchesResolvedValue(actual, declaredValue(record, key, expected));
  }

  /**
   * The value a write of {@code operand} stores in the property {@code key} of the record when the declaration converts it
   * through its text form (the conversion of CREATE, MERGE and SET: the Cypher value's storage form, then the schema
   * conversion to the declared type): a non-text operand into a property declared {@code STRING}, or a text into a property
   * declared with any other scalar type. Null otherwise, or when the declaration cannot store the operand. Comparing a
   * stored value with this, instead of with the operand itself, is what lets a pattern match the value it would store.
   * <p>
   * Only the text form is converted: two values of the same kind keep the Cypher typing of {@code =}, so a fraction equals
   * no integer although a write truncates it, as every index path already decides it does. A schemaless property keeps the
   * Cypher typing too, under which {@code '1' = 1} is false.
   */
  public static Object declaredValue(final Document record, final String key, final Object operand) {
    if (operand == null || operand instanceof Map)
      return null;
    final Property property = declaredProperty(record, key);
    return property != null ? declaredValue(property, record.getDatabase(), operand) : null;
  }

  /**
   * Whether exactly one of the two values is a text: the only mismatch a declaration can turn into a match (see
   * {@link #declaredValue(Document, String, Object)}), and so the gate in front of its schema lookup. A scan comparing
   * every record against one value pays that lookup only on such a mismatch.
   */
  public static boolean isTextAgainstNonText(final Object actual, final Object operand) {
    return actual != null && operand != null && (actual instanceof String) != (operand instanceof String) && !(operand instanceof Map);
  }

  /**
   * The record a variable binding stands for: the record itself, or the one a RID points to (a binding procedures and
   * persisted node links leave lazy, see {@code PropertyAccessExpression}); null for any other value.
   */
  public static Document boundRecord(final Object bound) {
    if (bound instanceof Document record)
      return record;
    if (bound instanceof RID rid) {
      try {
        return rid.getRecord() instanceof Document record ? record : null;
      } catch (final RecordNotFoundException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * The property {@code key} of the record's type (or of a supertype) when it is declared with a scalar type, whose values
   * have a text form a write converts to or from; otherwise null.
   */
  public static Property declaredProperty(final Document record, final String key) {
    final DocumentType type = record.getType();
    if (type == null)
      return null;
    final Property property = type.getPolymorphicPropertyIfExists(key);
    return property != null && isScalarDeclaration(property.getType()) ? property : null;
  }

  /**
   * {@link #declaredValue(Document, String, Object)} for a caller that holds the property instead of a record, such as an
   * index lookup building its key.
   */
  public static Object declaredValue(final Property property, final Database database, final Object operand) {
    if (property == null || operand == null || operand instanceof Map || !isScalarDeclaration(property.getType()))
      return null;
    // the text form, in one direction or the other: a text into a STRING, or a non-text into anything else, is not converted
    if ((property.getType() == Type.STRING) == (operand instanceof String))
      return null;
    // NaN equals nothing, not even the 'NaN' it was stored as
    if ((operand instanceof Double d && d.isNaN()) || (operand instanceof Float f && f.isNaN()))
      return null;
    // a text of a shape the declaration never reads is refused here rather than by the conversion's exception, which a scan
    // comparing every record against the same text would otherwise pay once per record
    if (operand instanceof String text && !mayBeReadAs(property.getType(), text))
      return null;
    // null for a value the declaration cannot store: the write refuses it, so no record holds what it would have stored. With a
    // property every refusal of Type.convert is an IllegalArgumentException, the one convertOrNull answers null for
    final Object stored = Type.convertOrNull(database, TemporalUtil.toCoreJavaType(operand),
        property.getType().getJavaImplementation(database), property);
    return stored instanceof Double d && d.isNaN() || stored instanceof Float f && f.isNaN() ? null : stored;
  }

  /**
   * Whether {@code text} has a shape {@code Type.convert} can read as {@code type}: an integral type reads an optional sign
   * and digits ({@code Integer.parseInt} and siblings, so {@code '5.7'} or {@code ' 5'} is refused, as by the write) with no
   * more significant digits than its range holds, a BOOLEAN reads {@code true} or {@code false} in any case, a FLOAT, DOUBLE
   * or DECIMAL a text starting the way a number does, and a temporal a text holding a digit, which every date, time and
   * duration has whatever format the database declares. A shape this lets through can still be refused by the conversion.
   */
  private static boolean mayBeReadAs(final Type type, final String text) {
    final int length = text.length();
    switch (type) {
    case BYTE, SHORT, INTEGER, LONG -> {
      int start = length > 0 && (text.charAt(0) == '-' || text.charAt(0) == '+') ? 1 : 0;
      if (start == length)
        return false;
      for (int i = start; i < length; i++)
        if (text.charAt(i) < '0' || text.charAt(i) > '9')
          return false;
      // leading zeros are no digits of the value: '007' is an INTEGER, '99999999999' never is
      while (start < length - 1 && text.charAt(start) == '0')
        start++;
      final int maxDigits = switch (type) {
        case BYTE -> 3;
        case SHORT -> 5;
        case INTEGER -> 10;
        default -> 19;
      };
      return length - start <= maxDigits;
    }
    case BOOLEAN -> {
      return "true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text);
    }
    case FLOAT, DOUBLE, DECIMAL -> {
      final String trimmed = text.trim();
      if (trimmed.isEmpty())
        return false;
      final int start = trimmed.charAt(0) == '-' || trimmed.charAt(0) == '+' ? 1 : 0;
      if (start == trimmed.length())
        return false;
      final char first = trimmed.charAt(start);
      // a letter only starts NaN or Infinity, the two words Double.parseDouble reads
      return first >= '0' && first <= '9' || first == '.' || trimmed.startsWith("NaN", start) || trimmed.startsWith("Infinity",
          start);
    }
    case DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS, OFFSET_TIME, LOCAL_TIME, ZONED_DATETIME, DURATION -> {
      for (int i = 0; i < length; i++)
        if (text.charAt(i) >= '0' && text.charAt(i) <= '9')
          return true;
      return false;
    }
    default -> {
      return true;
    }
    }
  }

  /**
   * The declared types whose values have a text form a write converts to or from. A collection, an embedded document, a
   * link or a binary is compared as it is.
   */
  private static boolean isScalarDeclaration(final Type type) {
    return switch (type) {
      case STRING, BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS,
           DATETIME_NANOS, OFFSET_TIME, LOCAL_TIME, ZONED_DATETIME, DURATION -> true;
      default -> false;
    };
  }

  /**
   * Compares a stored property value against an already resolved expected value, for the traversal
   * components that resolve the whole map once per row instead of once per candidate edge.
   * <p>
   * An inline entry means {@code n.k = v}, so it follows the comparison semantics of {@code =}: a null on
   * either side never matches, not even against another null. Numbers compare by value across types, so a
   * stored {@code Integer} matches an inline literal parsed as {@code Long} and a stored {@code Float}
   * matches a {@code Double} parameter (issue #5146).
   * <p>
   * Prefer {@link #matchesProperty(Document, String, Object)} when the record is at hand: only that one knows the
   * declared type of the property.
   */
  public static boolean matchesResolvedValue(final Object actual, final Object expected) {
    if (actual == null || expected == null)
      return false;
    if (actual.equals(expected))
      return true;
    if (expected instanceof CypherTemporalValue wanted)
      return temporalEqual(actual, wanted);
    return actual instanceof Number a && expected instanceof Number b && numbersEqual(a, b);
  }

  /**
   * A value that went through a {@code WITH}, an {@code UNWIND} or a {@code datetime()} call is a Cypher temporal
   * wrapper, which a stored {@code java.time} value never {@code equals()}: compare them the way {@code =} does, a
   * naive datetime against a zoned one by instant (issue #8921).
   */
  private static boolean temporalEqual(final Object actual, final CypherTemporalValue wanted) {
    final Object stored = TemporalUtil.fromCoreJavaType(actual);
    if (!(stored instanceof CypherTemporalValue have))
      return false;
    if (have instanceof CypherLocalDateTime local && wanted instanceof CypherDateTime zoned)
      return local.getValue().toInstant(ZoneOffset.UTC).equals(zoned.getValue().toInstant());
    if (have instanceof CypherDateTime zoned && wanted instanceof CypherLocalDateTime local)
      return zoned.getValue().toInstant().equals(local.getValue().toInstant(ZoneOffset.UTC));
    // Different temporal types never equal: decided here rather than by the IllegalArgumentException compareTo throws,
    // because this runs once per candidate record
    return have.getClass() == wanted.getClass() && have.compareTo(wanted) == 0;
  }

  /**
   * Resolves every value of an inline property map at once, for the traversers that filter thousands of
   * candidate edges against the same map: resolving per row instead of per edge keeps the row-dependent
   * work out of the traversal loop.
   * <p>
   * A value that resolves to null - an unbound parameter, a row-dependent expression evaluated without a
   * row - is kept in the returned map, so the entry goes on constraining the traversal and matches nothing,
   * exactly as it does on the non-traversal paths.
   *
   * @return the resolved map, the original one when it holds nothing to resolve, or null when there is no
   * constraint at all (which lets the traversers stay on their unconstrained fast path)
   */
  public static Map<String, Object> resolveAll(final Map<String, Object> properties, final Result row,
      final CommandContext context) {
    if (properties == null || properties.isEmpty())
      return null;

    Map<String, Object> resolved = null;
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Object declared = entry.getValue();
      final Object value = resolve(declared, row, context);
      if (value == declared) {
        if (resolved != null)
          resolved.put(entry.getKey(), value);
        continue;
      }
      if (resolved == null)
        resolved = new HashMap<>(properties);
      resolved.put(entry.getKey(), value);
    }
    return resolved != null ? resolved : properties;
  }

  /**
   * Resolves the value declared for an inline property to the value the pattern actually filters on:
   * evaluates an expression against the current row, then resolves a parameter reference against the
   * query parameters.
   * <p>
   * An expression can itself yield a parameter reference (a bare {@code $param} kept as a
   * {@link com.arcadedb.query.opencypher.ast.ParameterExpression} resolves to the bound value, but the
   * legacy encodings do not), so the parameter step runs after the expression step and not instead of it.
   * An unbound parameter resolves to null, which makes the entry match nothing - the same outcome as
   * comparing against a null property.
   *
   * @return the resolved value, or null when it is a parameter the caller never bound
   */
  public static Object resolve(final Object declared, final Result row, final CommandContext context) {
    Object value = declared;

    if (value instanceof Expression expression)
      value = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, row != null ? row : NO_ROW, context);

    if (value instanceof CypherASTBuilder.ParameterReference parameter)
      return parameter(parameter.getName(), context, null);

    if (value instanceof String text && text.length() > 1 && text.charAt(0) == '$')
      // Legacy encoding of a parameter as the raw text "$name". Left untouched when unbound, because
      // unlike the other encodings it is indistinguishable from a string that genuinely starts with '$'.
      return parameter(text.substring(1), context, text);

    return value;
  }

  private static Object parameter(final String name, final CommandContext context, final Object whenUnbound) {
    final Map<String, Object> parameters = context != null ? context.getInputParameters() : null;
    if (parameters == null)
      return whenUnbound;
    final Object value = parameters.get(name);
    return value != null ? value : whenUnbound;
  }

  /**
   * Compares two numbers by value. Integral types compare as longs so the full 64-bit range survives;
   * anything else compares as a double, which keeps a stored {@code Float} 1.5 apart from a {@code Double}
   * 1.9 - truncating both to long would have declared them equal.
   */
  private static boolean numbersEqual(final Number a, final Number b) {
    if (isIntegral(a) && isIntegral(b))
      return a.longValue() == b.longValue();
    return ComparisonExpression.floatAwareEquals(a, b);
  }

  private static boolean isIntegral(final Number number) {
    return number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte
        || number instanceof BigInteger;
  }
}
