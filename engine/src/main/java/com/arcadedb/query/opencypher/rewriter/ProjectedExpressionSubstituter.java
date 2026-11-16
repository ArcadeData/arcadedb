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
package com.arcadedb.query.opencypher.rewriter;

import com.arcadedb.query.opencypher.ast.BooleanCoercionExpression;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.IsNullExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.RegexExpression;
import com.arcadedb.query.opencypher.ast.StringMatchExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Replaces, anywhere inside an expression, references that a collapsing projection has resolved:
 * a subexpression that repeats a projected expression becomes a reference to that projection's
 * output column, and a property read of a projected variable is re-pointed at the column the
 * variable was projected as (issue #5286).
 * <p>
 * This is what lets an ORDER BY that only <em>contains</em> a projected expression keep working
 * after the rows collapse:
 * <pre>
 *   RETURN n.age AS a, count(*) AS c ORDER BY n.age + 1   -- sorts on a + 1
 *   RETURN n AS node, count(*) AS c ORDER BY n.age        -- sorts on node.age
 * </pre>
 * Both are well defined, because the substituted part is a grouping key and therefore constant
 * within each group. Neo4j resolves them the same way.
 * <p>
 * Subexpression identity is decided on {@link Expression#getText()}, which the relevant node types
 * synthesize from their children rather than storing the source slice, so it is a canonical rendering
 * on both sides and insensitive to how the query was spaced.
 * <p>
 * Reach is bounded by what {@link ExpressionRewriter} traverses: arithmetic, comparison and logical
 * spines, plus the function-call arguments handled below. An expression whose interesting part sits
 * under a node type the base rewriter does not descend into is simply left alone; the caller's scope
 * check then reports it rather than sorting on a value that is no longer well defined.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ProjectedExpressionSubstituter extends ExpressionRewriter {
  private final Map<String, String> outputNameByExpressionText;
  private final Map<String, String> outputNameByVariable;

  /**
   * @param outputNameByExpressionText canonical text of each projected expression, to the output column it is projected as
   * @param outputNameByVariable       source variable of each bare-variable projection, to the output column it is projected as
   */
  public ProjectedExpressionSubstituter(final Map<String, String> outputNameByExpressionText,
      final Map<String, String> outputNameByVariable) {
    this.outputNameByExpressionText = outputNameByExpressionText;
    this.outputNameByVariable = outputNameByVariable;
  }

  @Override
  public Object rewrite(final Object expression) {
    // A node that repeats a projected expression becomes a plain reference to its output column.
    // BooleanExpression nodes are excluded: the base rewriter casts its children back to the
    // interface it found them under, and a VariableExpression is not a BooleanExpression.
    if (expression instanceof Expression && !(expression instanceof BooleanExpression)) {
      final String outputName = outputNameByExpressionText.get(((Expression) expression).getText());
      if (outputName != null)
        return new VariableExpression(outputName);
    }

    // A predicate that repeats a projected predicate reads its column, coerced back to the predicate it stands for:
    // WITH n.b = true AS flag, count(*) AS c WHERE n.b = true (issue #12245). A BooleanExpression sits only where a
    // BooleanExpression is expected, so the coercion keeps the type the parent casts it to
    if (expression instanceof BooleanExpression booleanExpression) {
      final String outputName = outputNameByExpressionText.get(booleanExpression.getText());
      if (outputName != null)
        return new BooleanCoercionExpression(new VariableExpression(outputName));
    }

    // A property read of a projected variable follows that variable to the column it landed in
    if (expression instanceof PropertyAccessExpression) {
      final PropertyAccessExpression propertyAccess = (PropertyAccessExpression) expression;
      final String outputName = outputNameByVariable.get(propertyAccess.getVariableName());
      if (outputName != null && !outputName.equals(propertyAccess.getVariableName()))
        return new PropertyAccessExpression(outputName, propertyAccess.getPropertyName());
    }

    return super.rewrite(expression);
  }

  /**
   * The predicates below hold their operands as expressions the base rewriter does not descend into. A WHERE of a
   * collapsing WITH is built of them ({@code WHERE n.b IS NOT NULL}, issue #12245), so the substitution has to reach
   * their operands too.
   */
  @Override
  protected BooleanExpression visitBooleanCoercion(final BooleanCoercionExpression expr) {
    final Expression operand = rewriteOperand(expr.getExpression());
    return operand == expr.getExpression() ? expr : new BooleanCoercionExpression(operand);
  }

  @Override
  protected BooleanExpression visitIsNull(final IsNullExpression expr) {
    final Expression operand = rewriteOperand(expr.getExpression());
    return operand == expr.getExpression() ? expr : new IsNullExpression(operand, expr.isNot());
  }

  @Override
  protected BooleanExpression visitStringMatch(final StringMatchExpression expr) {
    final Expression operand = rewriteOperand(expr.getExpression());
    final Expression pattern = rewriteOperand(expr.getPattern());
    if (operand == expr.getExpression() && pattern == expr.getPattern())
      return expr;
    return new StringMatchExpression(operand, pattern, expr.getMatchType());
  }

  @Override
  protected BooleanExpression visitRegex(final RegexExpression expr) {
    final Expression operand = rewriteOperand(expr.getExpression());
    final Expression pattern = rewriteOperand(expr.getPattern());
    if (operand == expr.getExpression() && pattern == expr.getPattern())
      return expr;
    return new RegexExpression(operand, pattern);
  }

  @Override
  protected BooleanExpression visitIn(final InExpression expr) {
    final Expression operand = rewriteOperand(expr.getExpression());
    final List<Expression> list = expr.getList();
    List<Expression> rewrittenList = null;
    for (int i = 0; i < list.size(); i++) {
      final Expression element = list.get(i);
      final Expression newElement = rewriteOperand(element);
      if (newElement != element) {
        if (rewrittenList == null)
          rewrittenList = new ArrayList<>(list);
        rewrittenList.set(i, newElement);
      }
    }
    if (operand == expr.getExpression() && rewrittenList == null)
      return expr;
    return new InExpression(operand, rewrittenList != null ? rewrittenList : list);
  }

  @Override
  protected Expression visitBooleanWrapper(final BooleanWrapperExpression expr) {
    final Object inner = rewrite(expr.getBooleanExpression());
    if (inner == expr.getBooleanExpression() || !(inner instanceof BooleanExpression))
      return expr;
    return new BooleanWrapperExpression((BooleanExpression) inner);
  }

  /**
   * Rewrites the elements of a list literal: {@code WHERE true IN [n.b]} holds the grouping key inside the list.
   */
  @Override
  protected Expression visitList(final ListExpression expr) {
    final List<Expression> elements = expr.getElements();
    List<Expression> rewritten = null;
    for (int i = 0; i < elements.size(); i++) {
      final Expression element = elements.get(i);
      final Expression newElement = rewriteOperand(element);
      if (newElement != element) {
        if (rewritten == null)
          rewritten = new ArrayList<>(elements);
        rewritten.set(i, newElement);
      }
    }
    if (rewritten == null)
      return expr;
    final StringBuilder text = new StringBuilder("[");
    for (int i = 0; i < rewritten.size(); i++) {
      if (i > 0)
        text.append(", ");
      text.append(rewritten.get(i).getText());
    }
    return new ListExpression(rewritten, text.append(']').toString());
  }

  private Expression rewriteOperand(final Expression operand) {
    if (operand == null)
      return null;
    final Object rewritten = rewrite(operand);
    return rewritten instanceof Expression ? (Expression) rewritten : operand;
  }

  /**
   * Rewrites the arguments of a function call. The base implementation predates
   * {@link FunctionCallExpression#getArguments()} and leaves them untouched, which would strand a
   * projected expression used inside a call such as {@code ORDER BY toUpper(n.name)}.
   */
  @Override
  protected Expression visitFunctionCall(final FunctionCallExpression expr) {
    final List<Expression> arguments = expr.getArguments();
    if (arguments == null || arguments.isEmpty())
      return expr;

    List<Expression> rewritten = null;
    for (int i = 0; i < arguments.size(); i++) {
      final Expression argument = arguments.get(i);
      final Object newArgument = rewrite(argument);
      if (newArgument != argument && newArgument instanceof Expression) {
        if (rewritten == null)
          rewritten = new ArrayList<>(arguments);
        rewritten.set(i, (Expression) newArgument);
      }
    }

    if (rewritten == null)
      return expr;

    return new FunctionCallExpression(expr.getOriginalFunctionName(), rewritten, expr.isDistinct());
  }
}
