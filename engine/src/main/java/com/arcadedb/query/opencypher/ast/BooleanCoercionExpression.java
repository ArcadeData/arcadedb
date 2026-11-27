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

import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;

import java.lang.reflect.Array;
import java.util.Collection;

/**
 * Adapter that wraps a generic {@link Expression} so it can be used as a
 * {@link BooleanExpression}. Used when the WHERE clause body evaluates to a
 * boolean-typed value but is not a comparison/IS NULL/IN/pattern/EXISTS form -
 * for example a bare boolean literal ({@code WHERE true}, {@code WHERE false}),
 * a parameter, or a property access on a boolean-typed property.
 * <p>
 * {@link #toPredicate(Object)} is the one place a value in a predicate position becomes a truth value, for WHERE, the
 * logical operators, CASE WHEN and the WHERE of a list predicate or comprehension alike (issue #11760).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BooleanCoercionExpression implements BooleanExpression {
  /** The kind {@link #nonBooleanKindOf(Expression)} answers for a value known to be a list when the statement is parsed. */
  public static final String LIST_KIND = "List";

  private final Expression expression;

  public BooleanCoercionExpression(final Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    return Boolean.TRUE.equals(toPredicate(expression.evaluate(result, context)));
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    return toPredicate(expression.evaluate(result, context));
  }

  /**
   * The kind of an operand whose value is known not to be a BOOLEAN before the statement runs, or {@code null} when the operand
   * may be a predicate. Known: a non-Boolean, non-null literal, a list or map literal, an arithmetic expression other than
   * {@code +} (a number, a temporal or a duration, never a list), and a {@code +} with a list literal on either side (a list). A
   * {@code +} of operands whose types are known only at run time may be a list concatenation ({@code n.l + n.m}), which a
   * predicate takes by whether it is empty, so it is left to the run: the same line Neo4j 2026.09 draws. The parse-time refusal
   * of an operand under AND/OR/XOR/NOT reads this, in RETURN and WITH as in WHERE (issue #11760).
   */
  public static String nonBooleanKindOf(final Expression operand) {
    return switch (operand) {
      case LiteralExpression literal -> literal.getValue() == null || literal.getValue() instanceof Boolean ?
          null :
          literal.getValue().getClass().getSimpleName();
      case ListExpression ignored -> LIST_KIND;
      case MapExpression ignored -> "Map";
      case ArithmeticExpression arithmetic -> {
        if (arithmetic.getOperator() != ArithmeticExpression.Operator.ADD)
          yield "Number";
        yield arithmetic.getLeft() instanceof ListExpression || arithmetic.getRight() instanceof ListExpression ? LIST_KIND : null;
      }
      case null, default -> null;
    };
  }

  /** {@link #nonBooleanKindOf(Expression)} of the value this predicate adapts, {@code null} for any other predicate form. */
  public static String nonBooleanKindOf(final BooleanExpression operand) {
    return operand instanceof BooleanCoercionExpression coercion ? nonBooleanKindOf(coercion.expression) : null;
  }

  /**
   * The truth value of a value used as a predicate, as Neo4j's runtime takes it: a BOOLEAN is itself, null is unknown
   * ({@code null}), a LIST is true when it is not empty (the coercion Cypher kept for a list read from a property), and
   * any other value is a type error. Before issue #11760 the logical operators took every non-null value as true and a
   * WHERE took every non-Boolean as false, so the same integer was true under OR and false under WHERE.
   */
  public static Boolean toPredicate(final Object value) {
    if (value instanceof Boolean bool)
      return bool;
    if (value == null)
      return null;
    if (value instanceof Collection<?> list)
      return !list.isEmpty();
    // a byte[] is BINARY, one opaque value everywhere in openCypher, not a list
    if (MultiValue.isSequenceArray(value))
      return Array.getLength(value) > 0;
    throw new TypeMismatchException(
        "TypeError: Type mismatch: expected Boolean but was " + CypherFunctionHelper.cypherTypeName(value));
  }

  @Override
  public String getText() {
    return expression.getText();
  }

  public Expression getExpression() {
    return expression;
  }
}
