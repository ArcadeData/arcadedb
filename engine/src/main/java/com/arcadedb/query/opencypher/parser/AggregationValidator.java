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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The aggregation phase of the semantic validation: no aggregation nested in another, none in a WHERE or where a
 * clause cannot group, no non-deterministic argument, and no projection mixing an aggregation with a value that is
 * not one of its grouping keys.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AggregationValidator {
  private AggregationValidator() {
  }

  static void validateAggregations(final CypherStatement statement) {
    // Check for nested aggregations in RETURN
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        checkNestedAggregation(item.getExpression(), false);

    // Check for nested aggregations in WITH
    for (final WithClause withClause : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : withClause.getItems())
        checkNestedAggregation(item.getExpression(), false);

    // Check for aggregation in WHERE
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null) {
      for (final ClauseEntry entry : clauses) {
        if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause())
            checkAggregationInWhere(matchClause.getWhereClause());
          if (matchClause.hasPathPatterns())
            for (final PathPattern path : matchClause.getPathPatterns())
              checkAggregationInGroupWhere(path);
        } else if (entry.getType() == ClauseEntry.ClauseType.WITH) {
          final WithClause withClause = entry.getTypedClause();
          if (withClause.getWhereClause() != null)
            checkAggregationInWhere(withClause.getWhereClause());
        }
      }
    }
    if (statement.getWhereClause() != null)
      checkAggregationInWhere(statement.getWhereClause());

    // Check for aggregation in ORDER BY after non-aggregating RETURN
    if (statement.getReturnClause() != null && statement.getOrderByClause() != null
        && !statement.getReturnClause().hasAggregations())
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems())
        checkAggregationInOrderBy(item.getExpressionAST());

    // Check for aggregation in ORDER BY after non-aggregating WITH
    for (final WithClause withClause : statement.getWithClauses())
      if (withClause.getOrderByClause() != null && !withClause.hasAggregations())
        for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems())
          checkAggregationInOrderBy(item.getExpressionAST());

    // Check for ambiguous aggregation expressions in RETURN items
    if (statement.getReturnClause() != null)
      checkAmbiguousAggregation(statement.getReturnClause().getReturnItems());

    // Check for ambiguous aggregation in WITH items
    for (final WithClause withClause : statement.getWithClauses())
      checkAmbiguousAggregation(withClause.getItems());

    // Check for ambiguous aggregation in ORDER BY for aggregating RETURN
    if (statement.getReturnClause() != null && statement.getReturnClause().hasAggregations()
        && statement.getOrderByClause() != null) {
      final Set<String> groupingVars = collectGroupingVariables(statement.getReturnClause().getReturnItems());
      for (final OrderByClause.OrderByItem item : statement.getOrderByClause().getItems()) {
        final Expression expr = item.getExpressionAST();
        if (expr != null && expr.containsAggregation())
          checkMixedAggregation(expr, groupingVars);
      }
    }

    // Check for ambiguous aggregation in ORDER BY for aggregating WITH
    for (final WithClause withClause : statement.getWithClauses()) {
      if (withClause.hasAggregations() && withClause.getOrderByClause() != null) {
        final Set<String> groupingVars = collectGroupingVariables(withClause.getItems());
        // Collect projected names and expression texts for non-projected aggregation check
        final Set<String> projectedNames = new HashSet<>();
        for (final ReturnClause.ReturnItem projItem : withClause.getItems()) {
          projectedNames.add(projItem.getOutputName());
          if (projItem.getAlias() != null)
            projectedNames.add(projItem.getAlias());
          // Also add the expression text so ORDER BY sum(x) matches WITH sum(x) AS s
          if (projItem.getExpression() != null) {
            final String exprText = projItem.getExpression().getText();
            if (exprText != null)
              projectedNames.add(exprText);
          }
        }
        for (final OrderByClause.OrderByItem item : withClause.getOrderByClause().getItems()) {
          final Expression expr = item.getExpressionAST();
          if (expr == null)
            continue;
          // Check for non-projected aggregation function in ORDER BY
          if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation()) {
            final String exprText = expr.getText();
            if (exprText != null && !projectedNames.contains(exprText))
              throw new CommandSemanticException("UndefinedVariable: Aggregation in ORDER BY is not projected");
          }
          if (expr.containsAggregation())
            checkMixedAggregation(expr, groupingVars);
        }
      }
    }
  }

  private static void checkNestedAggregation(final Expression expr, final boolean insideAggregation) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation()) {
        if (insideAggregation)
          throw new CommandParsingException("NestedAggregation: Nested aggregation functions are not allowed");
        // Check for non-deterministic functions inside aggregation (e.g., count(rand()))
        for (final Expression arg : func.getArguments())
          checkNonConstantInAggregation(arg);
        for (final Expression arg : func.getArguments())
          checkNestedAggregation(arg, true);
        return;
      }
      for (final Expression arg : func.getArguments())
        checkNestedAggregation(arg, insideAggregation);
    } else if (expr instanceof ArithmeticExpression) {
      checkNestedAggregation(((ArithmeticExpression) expr).getLeft(), insideAggregation);
      checkNestedAggregation(((ArithmeticExpression) expr).getRight(), insideAggregation);
    } else if (expr instanceof TernaryLogicalExpression) {
      checkNestedAggregation(((TernaryLogicalExpression) expr).getLeft(), insideAggregation);
      if (((TernaryLogicalExpression) expr).getRight() != null)
        checkNestedAggregation(((TernaryLogicalExpression) expr).getRight(), insideAggregation);
    } else if (expr instanceof BooleanWrapperExpression) {
      final BooleanExpression boolExpr = ((BooleanWrapperExpression) expr).getBooleanExpression();
      checkNestedAggregationInBoolean(boolExpr, insideAggregation);
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkNestedAggregation(elem, insideAggregation);
    } else if (expr instanceof ListComprehensionExpression) {
      final ListComprehensionExpression lce = (ListComprehensionExpression) expr;
      checkNestedAggregation(lce.getListExpression(), insideAggregation);
      if (lce.getMapExpression() != null)
        checkAggregationInListComprehension(lce.getMapExpression());
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkNestedAggregation(caseExpr.getCaseExpression(), insideAggregation);
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkNestedAggregation(alt.getWhenExpression(), insideAggregation);
        checkNestedAggregation(alt.getThenExpression(), insideAggregation);
      }
      if (caseExpr.getElseExpression() != null)
        checkNestedAggregation(caseExpr.getElseExpression(), insideAggregation);
    }
  }

  private static void checkNestedAggregationInBoolean(final BooleanExpression boolExpr, final boolean insideAggregation) {
    if (boolExpr instanceof ComparisonExpression) {
      checkNestedAggregation(((ComparisonExpression) boolExpr).getLeft(), insideAggregation);
      checkNestedAggregation(((ComparisonExpression) boolExpr).getRight(), insideAggregation);
    } else if (boolExpr instanceof LogicalExpression) {
      checkNestedAggregationInBoolean(((LogicalExpression) boolExpr).getLeft(), insideAggregation);
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkNestedAggregationInBoolean(((LogicalExpression) boolExpr).getRight(), insideAggregation);
    }
  }

  private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = Set.of("rand", "randomuuid");

  private static void checkNonConstantInAggregation(final Expression expr) {
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (NON_DETERMINISTIC_FUNCTIONS.contains(func.getFunctionName().toLowerCase(Locale.ROOT)))
        throw new CommandParsingException("NonConstantExpression: Non-constant expression is not allowed inside aggregation: " + func.getFunctionName());
    }
  }

  private static void checkAggregationInListComprehension(final Expression expr) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
      throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in list comprehensions");
    if (expr instanceof ArithmeticExpression) {
      checkAggregationInListComprehension(((ArithmeticExpression) expr).getLeft());
      checkAggregationInListComprehension(((ArithmeticExpression) expr).getRight());
    }
  }

  private static void checkAggregationInWhere(final WhereClause whereClause) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return;
    checkAggregationInBooleanExpression(whereClause.getConditionExpression());
  }

  /** The inner {@code WHERE} of a quantified path pattern filters rows like the one of the clause: no aggregate (issue #10559). */
  private static void checkAggregationInGroupWhere(final PathPattern path) {
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel instanceof QuantifiedPathPattern group) {
        checkAggregationInBooleanExpression(group.getInnerWhere());
        checkAggregationInGroupWhere(group.getInnerPattern());
      }
  }

  private static void checkAggregationInBooleanExpression(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return;

    if (boolExpr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) boolExpr;
      checkAggregationInBooleanExpression(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkAggregationInBooleanExpression(logExpr.getRight());
    } else if (boolExpr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) boolExpr;
      checkAggregationInExpression(comp.getLeft());
      checkAggregationInExpression(comp.getRight());
    } else if (boolExpr instanceof InExpression) {
      final InExpression inExpr = (InExpression) boolExpr;
      checkAggregationInExpression(inExpr.getExpression());
      for (final Expression elem : inExpr.getList())
        checkAggregationInExpression(elem);
    } else if (boolExpr instanceof IsNullExpression isNull) {
      // count(x) IS NOT NULL is as much an aggregate in a WHERE as count(x) > 0 (issue #10559)
      checkAggregationInExpression(isNull.getExpression());
    } else if (boolExpr instanceof RegexExpression regex) {
      checkAggregationInExpression(regex.getExpression());
      checkAggregationInExpression(regex.getPattern());
    } else if (boolExpr instanceof StringMatchExpression match) {
      checkAggregationInExpression(match.getExpression());
      checkAggregationInExpression(match.getPattern());
    } else if (boolExpr instanceof IsNormalizedExpression normalized) {
      checkAggregationInExpression(normalized.getValueExpression());
    } else if (boolExpr instanceof IsTypedExpression typed) {
      checkAggregationInExpression(typed.getValueExpression());
    } else if (boolExpr instanceof BooleanCoercionExpression coercion) {
      checkAggregationInExpression(coercion.getExpression());
    }
  }

  private static void checkAggregationInOrderBy(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in ORDER BY after RETURN");
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkAggregationInOrderBy(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkAggregationInOrderBy(((ArithmeticExpression) expr).getLeft());
      checkAggregationInOrderBy(((ArithmeticExpression) expr).getRight());
    }
  }

  /**
   * Check for ambiguous aggregation: expressions that mix aggregation calls with
   * non-aggregated sub-expressions. The rules are:
   * - Simple variable/property references matching grouping keys are OK
   * - Literals and parameters are OK
   * - Complex expressions (arithmetic, etc.) containing variables are AMBIGUOUS
   * - Variable/property references NOT matching grouping keys are AMBIGUOUS
   */
  private static void checkAmbiguousAggregation(final List<ReturnClause.ReturnItem> items) {
    final Set<String> groupingVars = collectGroupingVariables(items);
    for (final ReturnClause.ReturnItem item : items) {
      final Expression expr = item.getExpression();
      if (!expr.containsAggregation())
        continue;
      // Pure aggregation call is fine
      if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
        continue;
      // Mixed expression — check non-aggregated parts
      checkMixedAggregation(expr, groupingVars);
    }
  }

  /**
   * Recursively checks a mixed aggregation expression for ambiguous non-aggregated parts.
   */
  private static void checkMixedAggregation(final Expression expr, final Set<String> groupingVars) {
    if (expr == null)
      return;
    if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation())
      return; // Stop at aggregation boundary

    if (expr instanceof ArithmeticExpression) {
      final Expression left = ((ArithmeticExpression) expr).getLeft();
      final Expression right = ((ArithmeticExpression) expr).getRight();
      final boolean leftAgg = left != null && left.containsAggregation();
      final boolean rightAgg = right != null && right.containsAggregation();

      if (leftAgg && !rightAgg) {
        validateNonAggPart(right, groupingVars);
        checkMixedAggregation(left, groupingVars);
      } else if (!leftAgg && rightAgg) {
        validateNonAggPart(left, groupingVars);
        checkMixedAggregation(right, groupingVars);
      } else {
        checkMixedAggregation(left, groupingVars);
        checkMixedAggregation(right, groupingVars);
      }
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkMixedAggregation(arg, groupingVars);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkMixedAggregationInBoolean(((BooleanWrapperExpression) expr).getBooleanExpression(), groupingVars);
    } else if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      checkMixedAggregation(comp.getLeft(), groupingVars);
      checkMixedAggregation(comp.getRight(), groupingVars);
    }
  }

  private static void checkMixedAggregationInBoolean(final BooleanExpression boolExpr, final Set<String> groupingVars) {
    if (boolExpr instanceof ComparisonExpression) {
      checkMixedAggregation(((ComparisonExpression) boolExpr).getLeft(), groupingVars);
      checkMixedAggregation(((ComparisonExpression) boolExpr).getRight(), groupingVars);
    } else if (boolExpr instanceof LogicalExpression) {
      checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getLeft(), groupingVars);
      if (((LogicalExpression) boolExpr).getRight() != null)
        checkMixedAggregationInBoolean(((LogicalExpression) boolExpr).getRight(), groupingVars);
    }
  }

  /**
   * Validates that a non-aggregated part in a mixed expression is not ambiguous.
   */
  private static void validateNonAggPart(final Expression expr, final Set<String> groupingVars) {
    if (expr == null)
      return;
    // Simple variable reference — OK if it's a grouping key
    if (expr instanceof VariableExpression) {
      if (!groupingVars.contains(((VariableExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Simple property access — OK if the variable is a grouping key
    if (expr instanceof PropertyAccessExpression) {
      if (!groupingVars.contains(((PropertyAccessExpression) expr).getVariableName()))
        throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
      return;
    }
    // Literals and stars are always OK
    if (expr instanceof LiteralExpression || expr instanceof StarExpression)
      return;
    // Complex expressions containing variables are ambiguous
    if (hasVariableRefOutsideAggregation(expr))
      throw new CommandParsingException("AmbiguousAggregationExpression: Ambiguous aggregation expression");
  }

  private static boolean hasVariableRefOutsideAggregation(final Expression expr) {
    if (expr == null)
      return false;
    if (expr instanceof VariableExpression || expr instanceof PropertyAccessExpression)
      return true;
    if (expr instanceof LiteralExpression || expr instanceof StarExpression)
      return false;
    if (expr instanceof FunctionCallExpression) {
      final FunctionCallExpression func = (FunctionCallExpression) expr;
      if (func.isAggregation())
        return false; // Variables inside aggregation arguments are OK
      for (final Expression arg : func.getArguments())
        if (hasVariableRefOutsideAggregation(arg))
          return true;
      return false;
    }
    if (expr instanceof ArithmeticExpression)
      return hasVariableRefOutsideAggregation(((ArithmeticExpression) expr).getLeft())
          || hasVariableRefOutsideAggregation(((ArithmeticExpression) expr).getRight());
    if (expr instanceof BooleanWrapperExpression)
      return hasBooleanVarRefOutsideAgg(((BooleanWrapperExpression) expr).getBooleanExpression());
    if (expr instanceof ComparisonExpressionWrapper) {
      final ComparisonExpression comp = ((ComparisonExpressionWrapper) expr).getComparison();
      return hasVariableRefOutsideAggregation(comp.getLeft())
          || hasVariableRefOutsideAggregation(comp.getRight());
    }
    return false;
  }

  private static boolean hasBooleanVarRefOutsideAgg(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return false;
    if (boolExpr instanceof ComparisonExpression)
      return hasVariableRefOutsideAggregation(((ComparisonExpression) boolExpr).getLeft())
          || hasVariableRefOutsideAggregation(((ComparisonExpression) boolExpr).getRight());
    if (boolExpr instanceof LogicalExpression) {
      if (hasBooleanVarRefOutsideAgg(((LogicalExpression) boolExpr).getLeft()))
        return true;
      return ((LogicalExpression) boolExpr).getRight() != null
          && hasBooleanVarRefOutsideAgg(((LogicalExpression) boolExpr).getRight());
    }
    return false;
  }

  private static Set<String> collectGroupingVariables(final List<ReturnClause.ReturnItem> items) {
    final Set<String> vars = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items)
      if (!item.getExpression().containsAggregation()) {
        collectVariableNamesFromExpression(item.getExpression(), vars);
        // Also include the alias as a valid grouping reference
        if (item.getAlias() != null)
          vars.add(item.getAlias());
      }
    return vars;
  }

  static void collectVariableNamesFromExpression(final Expression expr, final Set<String> vars) {
    if (expr instanceof VariableExpression)
      vars.add(((VariableExpression) expr).getVariableName());
    else if (expr instanceof PropertyAccessExpression)
      vars.add(((PropertyAccessExpression) expr).getVariableName());
    else if (expr instanceof ArithmeticExpression) {
      collectVariableNamesFromExpression(((ArithmeticExpression) expr).getLeft(), vars);
      collectVariableNamesFromExpression(((ArithmeticExpression) expr).getRight(), vars);
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        collectVariableNamesFromExpression(arg, vars);
    }
  }

  private static void collectVariableRefsOutsideAggregation(final Expression expr, final Set<String> vars) {
    if (expr == null)
      return;
    if (expr instanceof VariableExpression)
      vars.add(((VariableExpression) expr).getVariableName());
    else if (expr instanceof PropertyAccessExpression)
      vars.add(((PropertyAccessExpression) expr).getVariableName());
    else if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        return; // Stop at aggregation boundary
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        collectVariableRefsOutsideAggregation(arg, vars);
    } else if (expr instanceof ArithmeticExpression) {
      collectVariableRefsOutsideAggregation(((ArithmeticExpression) expr).getLeft(), vars);
      collectVariableRefsOutsideAggregation(((ArithmeticExpression) expr).getRight(), vars);
    }
  }

  private static void checkAggregationInExpression(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof FunctionCallExpression) {
      if (((FunctionCallExpression) expr).isAggregation())
        throw new CommandParsingException("InvalidAggregation: Aggregation functions are not allowed in WHERE");
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkAggregationInExpression(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkAggregationInExpression(((ArithmeticExpression) expr).getLeft());
      checkAggregationInExpression(((ArithmeticExpression) expr).getRight());
    }
  }
}
