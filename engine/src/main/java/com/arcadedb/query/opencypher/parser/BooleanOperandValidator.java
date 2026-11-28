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
 * statement is parsed and is not a boolean, and a whole predicate whose type is known to be a list
 * ({@link #refuseListPredicate(Expression)}).
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

  /**
   * The operand check of a WHERE (or FILTER) condition, run on the condition as parsed, BEFORE the rewriters simplify it: the
   * boolean simplifier folds {@code true OR x} into {@code true} and {@code x AND false} into {@code false}, at any depth, so
   * checked after it {@code WHERE true OR (1 AND false)} lost the integer it had to refuse (issue #11760). The validation of the
   * whole statement checks the simplified condition again, which finds nothing new.
   */
  static void validateConditionOperands(final BooleanExpression condition) {
    checkBooleanOperandInBooleanExpression(condition);
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

  /**
   * Refuses, when the statement is parsed, a predicate that is known to be a list: a list literal, or a {@code +} with a list
   * literal on either side. The value in a predicate position (the condition of a WHERE or FILTER, the inline WHERE of a
   * pattern, the WHEN of a generic CASE, the WHERE of a list predicate or of a comprehension) is taken at run time by
   * {@link BooleanCoercionExpression#toPredicate(Object)}, which reads a list as "not empty": the coercion a list <b>read from
   * a property</b> gets, as in Neo4j, since its type is not known before the statement runs. A list whose type IS known was
   * taken the same way there, and refused as an operand of AND/OR/XOR/NOT by the check below: {@code WHERE [1, 2]} matched the
   * rows and {@code WHERE [1, 2] AND true} failed (issue #12975). Neo4j refuses both ("Coercion of list to boolean is not
   * allowed").
   * <p>
   * It reads {@link BooleanCoercionExpression#nonBooleanKindOf(Expression)}, the rule of the operand check, so the two cannot
   * disagree on what a list is. The other kinds that rule knows (a number, a string, a map) are not refused here: the run
   * refuses them with a type error in every position, so they never had two answers.
   */
  static void refuseListPredicate(final Expression predicate) {
    refuseList(BooleanCoercionExpression.nonBooleanKindOf(predicate));
  }

  /** {@link #refuseListPredicate(Expression)} for a predicate parsed as a {@link BooleanExpression}: the value it adapts. */
  static void refuseListPredicate(final BooleanExpression predicate) {
    refuseList(BooleanCoercionExpression.nonBooleanKindOf(predicate));
  }

  private static void refuseList(final String kind) {
    if (BooleanCoercionExpression.LIST_KIND.equals(kind))
      throw notABoolean(kind);
  }

  /** The one refusal of a value known not to be a BOOLEAN where a predicate is expected. */
  private static CommandParsingException notABoolean(final String kind) {
    return new CommandParsingException("InvalidArgumentType: Expected Boolean but got " + kind);
  }

  /** Refuses an operand known not to be a BOOLEAN: a non-Boolean literal, a list or map literal, an arithmetic expression. */
  private static void checkOperandForBooleanContext(final Expression operand) {
    final String kind = BooleanCoercionExpression.nonBooleanKindOf(operand);
    if (kind != null)
      throw notABoolean(kind);
  }

  /**
   * An operand of AND, OR, XOR or NOT in a WHERE: comparisons, IN, IS NULL and the other predicate forms are Boolean by
   * construction, so the only operand to look into is a value adapted to a predicate, which is held to the rule the RETURN
   * operands follow. It used to check nothing, so {@code WHERE false AND 1} ran (the short-circuit never reached the integer)
   * while {@code WHERE 1 AND false} failed: the answer depended on the operand order (issue #11760). Neo4j refuses both before
   * running anything.
   */
  private static void checkBooleanExprOperandValidity(final BooleanExpression operand) {
    final String kind = BooleanCoercionExpression.nonBooleanKindOf(operand);
    if (kind != null)
      throw notABoolean(kind);
  }
}
