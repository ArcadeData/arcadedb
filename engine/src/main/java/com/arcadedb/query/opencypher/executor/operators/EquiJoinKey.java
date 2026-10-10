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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;

import java.util.Arrays;

/**
 * One {@code left = right} conjunct of a WHERE that relates two independently matched parts of a pattern: each side
 * reads the variables of one part only, so each can be evaluated on that part's row alone, and the two parts can be
 * joined on the values instead of crossed and filtered (issue #8584).
 * <p>
 * A join only narrows the pairs the WHERE is then evaluated on - the WHERE still decides every pair it lets through -
 * so it must never lose a pair the comparison would accept, and may keep a few it rejects. {@link #canonicalKey} is
 * built to that rule over the comparison of {@link com.arcadedb.query.opencypher.ast.ComparisonExpression}: values it
 * calls equal get the same key, and a value whose equality the key cannot follow (a temporal, a list, a map, a
 * string spelling a RID, which compares equal to a number) is {@link #UNHASHABLE}: its row is paired with every row
 * of the other side, as a product would.
 *
 * @param left         the side that reads the left (probe, outer) part
 * @param right        the side that reads the right (build, inner) part
 * @param viaEvaluator whether the comparison evaluates its sides with the expression evaluator, which it does when
 *                     either side is a function call: the key has to see the values the comparison sees
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record EquiJoinKey(Expression left, Expression right, boolean viaEvaluator) {
  /** The key of a value no value equals: null (null = x is null) and NaN (NaN = x is false). */
  public static final Object NO_MATCH   = new Object();
  /** The key of a value whose equality a hash key cannot follow. */
  public static final Object UNHASHABLE = new Object();

  public static EquiJoinKey of(final Expression left, final Expression right) {
    return new EquiJoinKey(left, right, left instanceof FunctionCallExpression || right instanceof FunctionCallExpression);
  }

  public Object evaluateLeft(final Result row, final CommandContext context) {
    return evaluate(left, row, context);
  }

  public Object evaluateRight(final Result row, final CommandContext context) {
    return evaluate(right, row, context);
  }

  public String getText() {
    return left.getText() + " = " + right.getText();
  }

  private Object evaluate(final Expression expression, final Result row, final CommandContext context) {
    return viaEvaluator ?
        OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, row, context) :
        expression.evaluate(row, context);
  }

  /**
   * The hash key of the values the keys evaluate to on one side: {@link #NO_MATCH}, {@link #UNHASHABLE}, the
   * canonical value of a single key, or the list of the canonical values of a composite one.
   */
  public static Object canonicalKey(final EquiJoinKey[] keys, final boolean leftSide, final Result row,
      final CommandContext context) {
    if (keys.length == 1)
      return canonicalValue(leftSide ? keys[0].evaluateLeft(row, context) : keys[0].evaluateRight(row, context));

    final Object[] values = new Object[keys.length];
    boolean unhashable = false;
    for (int i = 0; i < keys.length; i++) {
      final Object value = canonicalValue(leftSide ? keys[i].evaluateLeft(row, context) : keys[i].evaluateRight(row, context));
      // One side of an AND that cannot be true makes the whole WHERE fail, whatever the others are
      if (value == NO_MATCH)
        return NO_MATCH;
      if (value == UNHASHABLE)
        unhashable = true;
      values[i] = value;
    }
    return unhashable ? UNHASHABLE : Arrays.asList(values);
  }

  /**
   * The value two values the Cypher {@code =} calls equal share. Numbers compare as doubles, a Float through its
   * decimal form, and minus zero equals zero; graph elements compare by identity.
   */
  public static Object canonicalValue(final Object value) {
    if (value == null)
      return NO_MATCH;
    if (value instanceof String string) {
      // A string spelling a RID equals the number id() encodes it to
      if (RID.is(string))
        return UNHASHABLE;
      // A property declared STRING holds the text of a number or a boolean written into it, which = finds again with that
      // number or boolean (issue #9695): such a text shares their key. Two texts that differ but spell the same number
      // ('5', '5.0') share it too, which only lets through a pair the comparison above the join then rejects
      return textKey(string);
    }
    if (value instanceof Number number) {
      final double d = number instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
      if (Double.isNaN(d))
        return NO_MATCH;
      return d == 0.0 ? 0.0 : d;
    }
    if (value instanceof Boolean)
      return value;
    if (value instanceof Identifiable identifiable) {
      final RID rid = identifiable.getIdentity();
      return rid != null ? rid : UNHASHABLE;
    }
    return UNHASHABLE;
  }

  /**
   * The key of a text: the key of the number or boolean it spells, or the text itself. A text is only parsed when its
   * characters are all those of a number's text, so an ordinary text (a zip code with a dash, an id with letters) costs a
   * character scan and never a thrown exception, and an integer's digits are read as a long without one either.
   */
  private static Object textKey(final String text) {
    final int length = text.length();
    if (length == 0)
      return text;
    final char first = text.charAt(0);
    if (first == 't' && text.equals("true") || first == 'f' && text.equals("false"))
      return Boolean.valueOf(text);
    if (text.equals("Infinity") || text.equals("-Infinity"))
      return Double.parseDouble(text);

    final int start = first == '-' ? 1 : 0;
    if (start == length)
      return text;
    boolean integral = true;
    for (int i = start; i < length; i++) {
      final char c = text.charAt(i);
      if (c >= '0' && c <= '9')
        continue;
      if (c != '.' && c != 'E' && c != '-')
        return text; // not the text of a number: no parse at all
      integral = false;
    }

    if (integral) {
      // Up to 18 digits always fit a long; the key of a number is its double, like canonicalValue's
      if (length - start <= 18) {
        long value = 0;
        for (int i = start; i < length; i++)
          value = value * 10 + (text.charAt(i) - '0');
        final double d = start == 1 ? -(double) value : (double) value;
        return d == 0.0 ? 0.0 : d;
      }
    }
    try {
      final double d = Double.parseDouble(text);
      return d == 0.0 ? 0.0 : d;
    } catch (final NumberFormatException e) {
      return text; // the characters of a number in no number's order ("1-2", "1..2")
    }
  }
}
