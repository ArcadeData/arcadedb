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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a WHERE clause in a Cypher query.
 * Contains filter expressions to apply to matched patterns.
 */
public class WhereClause {
  private final BooleanExpression condition;

  // Legacy constructor for backward compatibility (deprecated)
  private final String conditionString;

  public WhereClause(final BooleanExpression condition) {
    this.condition = condition;
    this.conditionString = condition != null ? condition.getText() : null;
  }

  // Legacy constructor - kept for backward compatibility
  public WhereClause(final String condition) {
    this.conditionString = condition;
    this.condition = null;
  }

  public BooleanExpression getConditionExpression() {
    return condition;
  }

  // Legacy method - kept for backward compatibility
  public String getCondition() {
    return conditionString;
  }

  /**
   * Collects all variable names referenced in a BooleanExpression tree.
   * Variables are found in PropertyAccessExpression (n.name) and VariableExpression (n).
   */
  public static Set<String> collectVariables(final BooleanExpression expr) {
    final Set<String> vars = new HashSet<>();
    collectVariablesRecursive(expr, vars);
    return vars;
  }

  /**
   * Extracts the sub-expression from a BooleanExpression that only references
   * the given set of available variables. For AND expressions, individual conjuncts
   * are checked independently. Returns null if no predicates can be extracted.
   *
   * @param expr              the full boolean expression
   * @param availableVariables the variables available at the current execution point
   * @return the extractable sub-expression, or null if none qualifies
   */
  public static BooleanExpression extractForVariables(final BooleanExpression expr,
      final Set<String> availableVariables) {
    if (expr == null)
      return null;

    // For AND: split into conjuncts and collect those referencing only available variables
    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression leftExtracted = extractForVariables(logical.getLeft(), availableVariables);
      final BooleanExpression rightExtracted = extractForVariables(logical.getRight(), availableVariables);
      if (leftExtracted != null && rightExtracted != null)
        return new LogicalExpression(LogicalExpression.Operator.AND, leftExtracted, rightExtracted);
      if (leftExtracted != null)
        return leftExtracted;
      return rightExtracted;
    }

    // For any other expression: check if all referenced variables are available
    final Set<String> vars = collectVariables(expr);
    if (!vars.isEmpty() && availableVariables.containsAll(vars))
      return expr;
    return null;
  }

  /**
   * Returns the residual expression after removing the extracted predicates.
   * This is the complement of extractForVariables: it returns predicates that
   * could NOT be pushed down.
   *
   * @param expr               the full boolean expression
   * @param availableVariables the variables available at the current execution point
   * @return the residual sub-expression, or null if everything was extracted
   */
  public static BooleanExpression residualForVariables(final BooleanExpression expr,
      final Set<String> availableVariables) {
    if (expr == null)
      return null;

    // For AND: split into conjuncts, keep those NOT referencing only available variables
    if (expr instanceof LogicalExpression logical && logical.getOperator() == LogicalExpression.Operator.AND) {
      final BooleanExpression leftResidual = residualForVariables(logical.getLeft(), availableVariables);
      final BooleanExpression rightResidual = residualForVariables(logical.getRight(), availableVariables);
      if (leftResidual != null && rightResidual != null)
        return new LogicalExpression(LogicalExpression.Operator.AND, leftResidual, rightResidual);
      if (leftResidual != null)
        return leftResidual;
      return rightResidual;
    }

    // For any other expression: keep it if it references variables NOT in availableVariables
    final Set<String> vars = collectVariables(expr);
    if (vars.isEmpty() || !availableVariables.containsAll(vars))
      return expr;
    return null;
  }

  private static void collectVariablesRecursive(final BooleanExpression expr, final Set<String> vars) {
    if (expr == null)
      return;

    if (expr instanceof ComparisonExpression comp) {
      collectExpressionVariables(comp.getLeft(), vars);
      collectExpressionVariables(comp.getRight(), vars);
    } else if (expr instanceof LogicalExpression logical) {
      collectVariablesRecursive(logical.getLeft(), vars);
      collectVariablesRecursive(logical.getRight(), vars);
    } else if (expr instanceof IsNullExpression isNull) {
      collectExpressionVariables(isNull.getExpression(), vars);
    } else if (expr instanceof InExpression) {
      // InExpression has an expression field - collect from its text as fallback
      collectFromText(expr.getText(), vars);
    } else if (expr instanceof StringMatchExpression) {
      collectFromText(expr.getText(), vars);
    } else if (expr instanceof RegexExpression) {
      collectFromText(expr.getText(), vars);
    } else if (expr instanceof LabelCheckExpression) {
      collectFromText(expr.getText(), vars);
    } else if (expr instanceof PatternPredicateExpression) {
      // Pattern predicates reference multiple variables, collect from text
      collectFromText(expr.getText(), vars);
    }
  }

  private static void collectExpressionVariables(final Expression expr, final Set<String> vars) {
    if (expr instanceof PropertyAccessExpression propAccess)
      vars.add(propAccess.getVariableName());
    else if (expr instanceof VariableExpression varExpr)
      vars.add(varExpr.getVariableName());
    else if (expr instanceof FunctionCallExpression) {
      // Function calls may reference variables in their arguments - collect from text
      collectFromText(expr.getText(), vars);
    }
  }

  /**
   * Fallback: extracts variable names from expression text by finding
   * patterns like "varName.property" or "varName:" (label check).
   * This is a conservative approximation used for expression types without
   * direct accessor methods for their sub-expressions.
   */
  private static void collectFromText(final String text, final Set<String> vars) {
    if (text == null)
      return;
    // Match variable.property patterns
    int i = 0;
    while (i < text.length()) {
      if (Character.isLetter(text.charAt(i)) || text.charAt(i) == '_') {
        final int start = i;
        while (i < text.length() && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_'))
          i++;
        if (i < text.length() && text.charAt(i) == '.') {
          vars.add(text.substring(start, i));
        }
      } else
        i++;
    }
  }
}
