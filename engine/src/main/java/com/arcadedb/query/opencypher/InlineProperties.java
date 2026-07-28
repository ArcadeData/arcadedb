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

import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.math.BigInteger;
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
      if (!matchesValue(record.get(entry.getKey()), entry.getValue(), row, context))
        return false;
    }
    return true;
  }

  /**
   * Returns true if a stored property value satisfies the value declared for it in the pattern.
   */
  public static boolean matchesValue(final Object actual, final Object declared, final Result row,
      final CommandContext context) {
    return matchesResolvedValue(actual, resolve(declared, row, context));
  }

  /**
   * Compares a stored property value against an already resolved expected value, for the traversal
   * components that resolve the whole map once per row instead of once per candidate edge.
   * <p>
   * An inline entry means {@code n.k = v}, so it follows the comparison semantics of {@code =}: a null on
   * either side never matches, not even against another null. Numbers compare by value across types, so a
   * stored {@code Integer} matches an inline literal parsed as {@code Long} and a stored {@code Float}
   * matches a {@code Double} parameter (issue #5146).
   */
  public static boolean matchesResolvedValue(final Object actual, final Object expected) {
    if (actual == null || expected == null)
      return false;
    if (actual.equals(expected))
      return true;
    return actual instanceof Number a && expected instanceof Number b && numbersEqual(a, b);
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
    return a.doubleValue() == b.doubleValue();
  }

  private static boolean isIntegral(final Number number) {
    return number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte
        || number instanceof BigInteger;
  }
}
