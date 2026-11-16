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
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The phase of the semantic validation that checks what a statement projects: a RETURN * with something to return,
 * SKIP and LIMIT values that read no variable, column names that are unique, and expressions that carry the alias
 * the clause requires.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ProjectionValidator {
  private ProjectionValidator() {
  }

  static void validateReturnStar(final CypherStatement statement) {
    if (statement.getReturnClause() == null)
      return;
    for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
      if (item.isStar() && !statementDeclaresAnyVariable(statement))
        throw new CommandParsingException("NoVariablesInScope: RETURN * is not allowed when there are no variables in scope");
  }

  /**
   * Whether this statement's own clauses put at least one name into scope - the pool {@code RETURN *} draws from.
   * Deliberately not the same question as "what is in scope here", which for an {@code EXISTS { } / COUNT { } /
   * COLLECT { }} body also includes every variable the enclosing row seeds it with ({@link VariableScopeValidator#checkSubqueryExpressionScope}):
   * a body that does nothing but {@code RETURN *} must still be rejected, exactly as a standalone {@code RETURN *}
   * is, and is (see {@code CypherSubqueryBodyIssue5656Test#returnStarWithNothingInScopeIsRejectedInsideABodyAsOutsideOne}).
   * <p>
   * Before issue #6135 this only looked at the statement's {@code MATCH} patterns and {@code UNWIND} variables, so a
   * name put into scope any other way - a {@code WITH} projection, a {@code CALL ... YIELD} output, or a variable a
   * {@code CALL { }} subquery returns - was invisible to it, and {@code RETURN *} was rejected even though the
   * executor would have projected the name correctly had it been allowed to run.
   */
  private static boolean statementDeclaresAnyVariable(final CypherStatement statement) {
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses == null || clauses.isEmpty()) {
      // Some builder paths leave the ordered list empty; the per-kind getters still answer (mirrors buildVarTypes).
      for (final MatchClause matchClause : statement.getMatchClauses())
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            if (pathDeclaresVariable(path))
              return true;
      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          if (pathDeclaresVariable(path))
            return true;
      return statement.getMergeClause() != null && pathDeclaresVariable(statement.getMergeClause().getPathPattern());
    }

    for (final ClauseEntry entry : clauses) {
      switch (entry.getType()) {
      case MATCH -> {
        final MatchClause matchClause = entry.getTypedClause();
        if (matchClause.hasPathPatterns())
          for (final PathPattern path : matchClause.getPathPatterns())
            if (pathDeclaresVariable(path))
              return true;
      }
      case CREATE -> {
        final CreateClause createClause = entry.getTypedClause();
        if (createClause != null && !createClause.isEmpty())
          for (final PathPattern path : createClause.getPathPatterns())
            if (pathDeclaresVariable(path))
              return true;
      }
      case MERGE -> {
        final MergeClause mergeClause = entry.getTypedClause();
        if (mergeClause != null && pathDeclaresVariable(mergeClause.getPathPattern()))
          return true;
      }
      case WITH -> {
        final WithClause withClause = entry.getTypedClause();
        for (final ReturnClause.ReturnItem item : withClause.getItems())
          if (!item.isStar() && item.getOutputName() != null)
            return true;
      }
      case UNWIND -> {
        if (((UnwindClause) entry.getTypedClause()).getVariable() != null)
          return true;
      }
      case LOAD_CSV -> {
        if (((LoadCSVClause) entry.getTypedClause()).getVariable() != null)
          return true;
      }
      case CALL -> {
        final CallClause callClause = entry.getTypedClause();
        if (callClause.hasYield()) {
          if (callClause.isYieldAll()) {
            final List<String> yieldAllFields = CypherSemanticValidator.resolveYieldAllFieldNames(callClause);
            if (yieldAllFields != null && !yieldAllFields.isEmpty())
              return true;
          } else {
            for (final CallClause.YieldItem item : callClause.getYieldItems())
              if (item.getOutputName() != null)
                return true;
          }
        }
      }
      case SUBQUERY -> {
        // What a CALL { } subquery returns becomes an ordinary variable of the enclosing scope, exactly like WITH.
        final SubqueryClause subqueryClause = entry.getTypedClause();
        if (subqueryClause.getInnerStatement() != null) {
          final ReturnClause innerReturn = subqueryClause.getInnerStatement().getReturnClause();
          if (innerReturn != null)
            for (final ReturnClause.ReturnItem item : innerReturn.getReturnItems())
              if (item.getOutputName() != null)
                return true;
        }
        if (subqueryClause.getReportStatusVariable() != null)
          return true;
      }
      default -> {
        // No binding of its own.
      }
      }
    }
    return false;
  }

  private static boolean pathDeclaresVariable(final PathPattern path) {
    if (path == null)
      return false;
    if (path.hasPathVariable())
      return true;
    for (final NodePattern node : path.getNodes())
      if (node.getVariable() != null && !node.getVariable().isEmpty())
        return true;
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.getVariable() != null && !rel.getVariable().isEmpty())
        return true;
    return false;
  }

  // ==============================
  // Phase 6: SKIP/LIMIT Validation
  // ==============================

  static void validateSkipLimit(final CypherStatement statement) {
    validateSkipLimitExpr(statement.getSkip(), "SKIP");
    validateSkipLimitExpr(statement.getLimit(), "LIMIT");

    // Check WITH clauses
    for (final WithClause withClause : statement.getWithClauses()) {
      validateSkipLimitExpr(withClause.getSkip(), "SKIP");
      validateSkipLimitExpr(withClause.getLimit(), "LIMIT");
    }
  }

  private static void validateSkipLimitExpr(final Expression expr, final String clauseName) {
    if (expr == null)
      return;
    // Check for negative and floating-point literal values
    if (expr instanceof LiteralExpression) {
      final Object val = ((LiteralExpression) expr).getValue();
      if (val instanceof Number) {
        if (val instanceof Float || val instanceof Double) {
          final double d = ((Number) val).doubleValue();
          if (d != Math.floor(d) || Double.isInfinite(d))
            throw new CommandParsingException("InvalidArgumentType: " + clauseName + " value must be an integer, got: Float(" + d + ")");
        }
        if (((Number) val).intValue() < 0)
          throw new CommandParsingException("NegativeIntegerArgument: " + clauseName + " value cannot be negative: " + val);
      }
    }
    // Check that SKIP/LIMIT expressions don't reference query variables (NonConstantExpression)
    if (containsVariableReference(expr))
      throw new CommandParsingException("NonConstantExpression: " + clauseName + " expression must not reference variables");
  }

  private static boolean containsVariableReference(final Expression expr) {
    if (expr instanceof VariableExpression)
      return !"*".equals(((VariableExpression) expr).getVariableName());
    if (expr instanceof PropertyAccessExpression)
      return true;
    if (expr instanceof ArithmeticExpression) {
      return containsVariableReference(((ArithmeticExpression) expr).getLeft())
          || containsVariableReference(((ArithmeticExpression) expr).getRight());
    }
    if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        if (containsVariableReference(arg))
          return true;
    }
    return false;
  }

  // ==========================================
  // Phase 7: Column Name Conflict
  // ==========================================

  static void validateColumnNames(final CypherStatement statement) {
    // Check RETURN clause for duplicate aliases
    if (statement.getReturnClause() != null)
      checkDuplicateAliases(statement.getReturnClause().getReturnItems());

    // Check WITH clauses for duplicate aliases
    for (final WithClause withClause : statement.getWithClauses())
      checkDuplicateAliases(withClause.getItems());
  }

  /**
   * The {@code *} of {@code WITH *} / {@code RETURN *} projects no column of its own and so cannot collide with one;
   * it is recognised by {@link ReturnClause.ReturnItem#isStar()} rather than by its name, because a projection can
   * legitimately be aliased to the literal name {@code *} with backticks, and two of those DO collide (issue #6334).
   */
  private static void checkDuplicateAliases(final List<ReturnClause.ReturnItem> items) {
    final Set<String> seen = new HashSet<>();
    for (final ReturnClause.ReturnItem item : items) {
      if (item.isStar())
        continue;
      String name = item.getAlias();
      if (name == null && item.getExpression() instanceof VariableExpression)
        name = ((VariableExpression) item.getExpression()).getVariableName();
      if (name != null && !seen.add(name))
        throw new CommandParsingException("ColumnNameConflict: Column name '" + name + "' is defined more than once");
    }
  }

  // ==========================================
  // Phase 8: Expression Alias Validation
  // ==========================================

  static void validateExpressionAliases(final CypherStatement statement) {
    // In WITH, non-variable expressions (aggregations, function calls, arithmetic, etc.)
    // must have an alias. Simple variable references don't need one.
    for (final WithClause withClause : statement.getWithClauses()) {
      // Only enforce if WITH has aggregations (mixed aggregation/non-aggregation context)
      if (withClause.hasAggregations()) {
        for (final ReturnClause.ReturnItem item : withClause.getItems()) {
          final Expression expr = item.getExpression();
          if (expr instanceof FunctionCallExpression && ((FunctionCallExpression) expr).isAggregation()) {
            if (item.getAlias() == null)
              throw new CommandParsingException("NoExpressionAlias: Expression in WITH must be aliased (use AS)");
          }
        }
      }
    }
  }
}
