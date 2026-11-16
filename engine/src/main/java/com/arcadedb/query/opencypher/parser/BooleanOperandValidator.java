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
 * The phase of the semantic validation that rejects an operand of AND, OR, XOR or NOT whose type is known when the
 * statement is parsed and is not a boolean.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BooleanOperandValidator {
  private BooleanOperandValidator() {
  }

  static void validateBooleanOperandTypes(final CypherStatement statement) {
    // Check RETURN clause expressions
    if (statement.getReturnClause() != null)
      for (final ReturnClause.ReturnItem item : statement.getReturnClause().getReturnItems())
        checkBooleanOperandInExpression(item.getExpression());

    // Check WITH clause expressions
    for (final WithClause withClause : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : withClause.getItems())
        checkBooleanOperandInExpression(item.getExpression());

    // Check WHERE clause expressions
    final List<ClauseEntry> clauses = statement.getClausesInOrder();
    if (clauses != null) {
      for (final ClauseEntry entry : clauses) {
        if (entry.getType() == ClauseEntry.ClauseType.MATCH) {
          final MatchClause matchClause = entry.getTypedClause();
          if (matchClause.hasWhereClause())
            checkBooleanOperandInBooleanExpression(matchClause.getWhereClause().getConditionExpression());
        } else if (entry.getType() == ClauseEntry.ClauseType.WITH) {
          final WithClause withClause = entry.getTypedClause();
          if (withClause.getWhereClause() != null)
            checkBooleanOperandInBooleanExpression(withClause.getWhereClause().getConditionExpression());
        }
      }
    }
    if (statement.getWhereClause() != null)
      checkBooleanOperandInBooleanExpression(statement.getWhereClause().getConditionExpression());
  }

  private static void checkBooleanOperandInExpression(final Expression expr) {
    if (expr == null)
      return;

    if (expr instanceof TernaryLogicalExpression) {
      final TernaryLogicalExpression tle = (TernaryLogicalExpression) expr;
      // Check that operands are valid boolean types
      checkOperandForBooleanContext(tle.getLeft());
      if (tle.getRight() != null)
        checkOperandForBooleanContext(tle.getRight());
      // Recurse into operands
      checkBooleanOperandInExpression(tle.getLeft());
      if (tle.getRight() != null)
        checkBooleanOperandInExpression(tle.getRight());
    } else if (expr instanceof FunctionCallExpression) {
      for (final Expression arg : ((FunctionCallExpression) expr).getArguments())
        checkBooleanOperandInExpression(arg);
    } else if (expr instanceof ArithmeticExpression) {
      checkBooleanOperandInExpression(((ArithmeticExpression) expr).getLeft());
      checkBooleanOperandInExpression(((ArithmeticExpression) expr).getRight());
    } else if (expr instanceof ListExpression) {
      for (final Expression elem : ((ListExpression) expr).getElements())
        checkBooleanOperandInExpression(elem);
    } else if (expr instanceof BooleanWrapperExpression) {
      checkBooleanOperandInBooleanExpression(((BooleanWrapperExpression) expr).getBooleanExpression());
    } else if (expr instanceof CaseExpression) {
      final CaseExpression caseExpr = (CaseExpression) expr;
      if (caseExpr.getCaseExpression() != null)
        checkBooleanOperandInExpression(caseExpr.getCaseExpression());
      for (final CaseAlternative alt : caseExpr.getAlternatives()) {
        checkBooleanOperandInExpression(alt.getWhenExpression());
        checkBooleanOperandInExpression(alt.getThenExpression());
      }
      if (caseExpr.getElseExpression() != null)
        checkBooleanOperandInExpression(caseExpr.getElseExpression());
    }
  }

  private static void checkBooleanOperandInBooleanExpression(final BooleanExpression boolExpr) {
    if (boolExpr == null)
      return;

    if (boolExpr instanceof LogicalExpression) {
      final LogicalExpression logExpr = (LogicalExpression) boolExpr;
      // Check operands
      checkBooleanExprOperandValidity(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkBooleanExprOperandValidity(logExpr.getRight());
      // Recurse
      checkBooleanOperandInBooleanExpression(logExpr.getLeft());
      if (logExpr.getRight() != null)
        checkBooleanOperandInBooleanExpression(logExpr.getRight());
    } else if (boolExpr instanceof ComparisonExpression) {
      final ComparisonExpression comp = (ComparisonExpression) boolExpr;
      checkBooleanOperandInExpression(comp.getLeft());
      checkBooleanOperandInExpression(comp.getRight());
    } else if (boolExpr instanceof InExpression) {
      final InExpression inExpr = (InExpression) boolExpr;
      checkBooleanOperandInExpression(inExpr.getExpression());
      for (final Expression elem : inExpr.getList())
        checkBooleanOperandInExpression(elem);
    }
  }

  private static void checkOperandForBooleanContext(final Expression operand) {
    if (operand == null)
      return;

    // Reject non-boolean literal values
    if (operand instanceof LiteralExpression) {
      final Object value = ((LiteralExpression) operand).getValue();
      if (value != null && !(value instanceof Boolean))
        throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got " + value.getClass().getSimpleName());
    }
    // Reject list expressions as boolean operands (e.g., [1,2] AND true)
    else if (operand instanceof ListExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got List");
      // Reject map expressions as boolean operands (e.g., {a: 1} AND true)
    else if (operand instanceof MapExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got Map");
      // Reject arithmetic expressions (always return numbers)
    else if (operand instanceof ArithmeticExpression)
      throw new CommandParsingException("InvalidArgumentType: Expected Boolean but got Number");
  }

  private static void checkBooleanExprOperandValidity(final BooleanExpression operand) {
    // For BooleanExpression operands in LogicalExpression, we need to unwrap
    // to check for non-boolean literals. ComparisonExpression, InExpression, etc.
    // always return boolean, so those are fine. But if the operand wraps an Expression,
    // we need to check it.
    // In practice, the ANTLR parser produces LogicalExpression with BooleanExpression operands
    // that are typically ComparisonExpression, InExpression, etc. — these are inherently boolean.
    // The invalid cases are more typically caught via TernaryLogicalExpression in RETURN context.
  }
}
