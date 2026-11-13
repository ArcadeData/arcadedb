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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.ChainedPropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.ListIndexExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.ParameterExpression;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.Collections;
import java.util.Map;

/**
 * The values an index seek can be built from before any row exists: the value side of {@code n.key = <value>}, of an
 * {@code IN} element, of an inline property map entry. One definition, shared by every planner that turns such a predicate into a
 * seek and by the operators that resolve the seek's key at run time, so a value one of them accepts is never one another cannot
 * evaluate (issues #11480, #11481).
 * <p>
 * A value qualifies when it reads no variable and gives the same answer for every row of the statement: a literal, a parameter, a
 * field of a parameter ({@code $d.u}, {@code $d['u']}, {@code $ids[0]}), and any deterministic function, arithmetic, list or map of
 * those ({@code toLower($name)}, {@code date($day)}). It does not when it reads a variable ({@code other.key}: only a row binds it),
 * reads the graph (a pattern, a subquery: the statement may be writing what it reads), calls a function that answers differently on
 * each call ({@code rand()}) or aggregates.
 * <p>
 * Two degrees: {@link #isRowIndependent} is every such value, which the relationship seek takes (it unwraps the Cypher temporal
 * values itself); {@link #isParameterDerived} is the part of it the node seeks take, what the caller passed: a literal, a parameter
 * and a part of one. Before this class each site kept its own list, and the node seek accepted only a literal or a bare parameter,
 * so {@code MATCH (n:Entity {uuid: $d.u})} scanned the whole label where {@code {uuid: $u}} sought the index (#11480).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RowIndependentValues {
  /** The row a row-independent value is evaluated against: it reads nothing from it, a parameter resolves from the context. */
  private static final Result NO_ROW = new ResultInternal(Collections.emptyMap());

  private RowIndependentValues() {
  }

  /**
   * Whether a NODE index seek can take {@code value} as a key, resolved by {@link #resolve} once per execution: a plain value (an
   * already-evaluated literal), or a {@link #isParameterDerived parameter-derived} expression.
   */
  public static boolean isSeekable(final Object value) {
    // A value that is not an expression is a literal already evaluated, or a parameter reference: both resolve without a row
    return !(value instanceof Expression expression) || isParameterDerived(expression);
  }

  /**
   * Whether {@code expression} is a literal, a parameter, or a part of a parameter: a field ({@code $d.u}, {@code $d.edge.source}),
   * a key ({@code $d['u']}) or an element ({@code $ids[1]}) of one, with a literal or a parameter as the key (issue #11480). Such a
   * value is one the caller passed, of the same kinds a bare parameter takes, so a node seek built from it compares the stored key
   * exactly as a seek built from a flat parameter does.
   * <p>
   * Narrower than {@link #isRowIndependent} on purpose: a computed value such as {@code date('2024-01-01')} or {@code toString(1)}
   * is row-independent too, but it reaches an index as a Cypher value (a temporal wrapper, a coerced text), whose key conversion
   * does not decide what Cypher's equality decides for every temporal implementation and declared type (issues #8572, #9647,
   * #9794). The node seeks keep those as the filter they always were.
   */
  public static boolean isParameterDerived(final Expression expression) {
    if (expression instanceof LiteralExpression || expression instanceof ParameterExpression)
      return true;
    if (expression instanceof ChainedPropertyAccessExpression chained)
      return isParameterDerived(chained.getBaseExpression()) && !(chained.getBaseExpression() instanceof LiteralExpression);
    if (expression instanceof ListIndexExpression index)
      return isParameterDerived(index.getListExpression()) && !(index.getListExpression() instanceof LiteralExpression)
          && (index.getIndexExpression() instanceof LiteralExpression || index.getIndexExpression() instanceof ParameterExpression);
    return false;
  }

  /** Whether {@code expression} can be evaluated once, without a row, and stand for its value on every row of the statement. */
  public static boolean isRowIndependent(final Expression expression) {
    if (expression == null)
      return false;
    // The two shapes nearly every seek is written with, answered without a walk
    if (expression instanceof LiteralExpression || expression instanceof ParameterExpression)
      return true;

    final CypherReferencedVariables referenced = CypherReferencedVariables.of(expression);
    if (!referenced.isComplete() || !referenced.getNames().isEmpty())
      return false;
    return isDeterministic(expression);
  }

  /**
   * Whether two evaluations of {@code expression} against the same row give the same value: it calls no aggregation and no
   * non-deterministic function such as {@code rand()}, and reads no pattern or nested statement, whose answer depends on the
   * graph at the moment it is evaluated. A value evaluated once for a seek and once more by a filter must be one.
   */
  public static boolean isDeterministic(final Expression expression) {
    final boolean[] dependent = new boolean[1];
    CypherExpressionWalker.walk(expression, new CypherExpressionWalker.Visitor() {
      @Override
      public void visit(final Expression node) {
        if (node instanceof FunctionCallExpression call
            && (call.isAggregation() || !ExpressionOptimizer.isDeterministicFunction(call.getFunctionName())))
          dependent[0] = true;
      }

      @Override
      public void visitPattern(final PathPattern pattern) {
        // A pattern reads the graph, which the statement itself may be changing between two evaluations
        dependent[0] = true;
      }

      @Override
      public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement statement) {
        dependent[0] = true;
        return null;
      }
    });
    return !dependent[0];
  }

  /**
   * The key value a seek built from {@code value} looks up in this execution: a parameter from the command's parameters, a literal
   * as written, any other row-independent expression evaluated against no row. A plain value is returned as it is. Null when a
   * parameter is not given, which a seek reads as "no key".
   */
  public static Object resolve(final Object value, final CommandContext context) {
    if (value instanceof ParameterExpression parameter) {
      final Map<String, Object> parameters = context.getInputParameters();
      return parameters != null ? parameters.get(parameter.getParameterName()) : null;
    }
    if (value instanceof CypherASTBuilder.ParameterReference parameter) {
      final Map<String, Object> parameters = context.getInputParameters();
      return parameters != null ? parameters.get(parameter.getName()) : null;
    }
    if (value instanceof LiteralExpression literal)
      return literal.getValue();
    if (value instanceof Expression expression)
      return OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, NO_ROW, context);
    return value;
  }
}
