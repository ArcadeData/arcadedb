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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;

/**
 * Expression representing a CASE expression.
 * <p>
 * Two forms are supported:
 * <p>
 * 1. Simple form:
 * CASE
 * WHEN condition1 THEN result1
 * WHEN condition2 THEN result2
 * ELSE defaultResult
 * END
 * <p>
 * 2. Extended form:
 * CASE expression
 * WHEN value1 THEN result1
 * WHEN value2 THEN result2
 * ELSE defaultResult
 * END
 */
public class CaseExpression implements Expression {
  private final Expression caseExpression; // null for simple form
  private final List<CaseAlternative> alternatives;
  private final Expression elseExpression; // null if no ELSE clause
  // Extended form only: reuse the '=' comparison so a WHEN value matches under Cypher equality
  // (numeric type folding, temporal, list, RID interop) instead of strict Object.equals(). Built once
  // per CASE AST node, not per evaluated row.
  private final ComparisonExpression whenEquality;

  /**
   * Constructor for simple CASE form (no case expression).
   */
  public CaseExpression(final List<CaseAlternative> alternatives, final Expression elseExpression) {
    this(null, alternatives, elseExpression);
  }

  /**
   * Constructor for extended CASE form (with case expression).
   */
  public CaseExpression(final Expression caseExpression, final List<CaseAlternative> alternatives,
                        final Expression elseExpression) {
    this.caseExpression = caseExpression;
    this.alternatives = alternatives;
    this.elseExpression = elseExpression;
    this.whenEquality = caseExpression != null
        ? new ComparisonExpression(null, ComparisonExpression.Operator.EQUALS, null)
        : null;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    return evaluateWith(expr -> expr.evaluate(result, context));
  }

  /**
   * Functional interface used to evaluate a single sub-expression of the CASE.
   * Lets callers plug in an override-aware evaluator (e.g. one that resolves pre-computed
   * aggregation results) while sharing the branch-selection and equality semantics below.
   */
  @FunctionalInterface
  public interface SubExpressionEvaluator {
    Object evaluate(Expression expression);
  }

  /**
   * Evaluate the CASE, delegating every sub-expression evaluation to the supplied evaluator.
   * The branch-selection logic and Cypher '=' equality semantics live here so both the plain
   * {@link #evaluate(Result, CommandContext)} path and the aggregation-override path share them.
   */
  public Object evaluateWith(final SubExpressionEvaluator subEvaluator) {
    // Extended form: CASE expr WHEN value THEN result
    if (caseExpression != null) {
      final Object caseValue = subEvaluator.evaluate(caseExpression);

      for (final CaseAlternative alternative : alternatives) {
        final Object whenValue = subEvaluator.evaluate(alternative.getWhenExpression());

        // Check equality using Cypher '=' semantics (numeric folding, temporal, list, RID interop)
        if (Boolean.TRUE.equals(whenEquality.evaluateWithValues(caseValue, whenValue))) {
          return subEvaluator.evaluate(alternative.getThenExpression());
        }
      }
    }
    // Simple form: CASE WHEN condition THEN result
    else {
      for (final CaseAlternative alternative : alternatives) {
        final Object whenResult = subEvaluator.evaluate(alternative.getWhenExpression());

        // Check if condition is true
        if (isTrue(whenResult)) {
          return subEvaluator.evaluate(alternative.getThenExpression());
        }
      }
    }

    // No match found - use ELSE or return null
    if (elseExpression != null) {
      return subEvaluator.evaluate(elseExpression);
    }

    return null;
  }

  @Override
  public boolean isAggregation() {
    // CASE is aggregation if any of its expressions are aggregations
    if (caseExpression != null && caseExpression.isAggregation()) {
      return true;
    }

    for (final CaseAlternative alternative : alternatives) {
      if (alternative.getWhenExpression().isAggregation()
          || alternative.getThenExpression().isAggregation()) {
        return true;
      }
    }

    if (elseExpression != null && elseExpression.isAggregation()) {
      return true;
    }

    return false;
  }

  @Override
  public boolean containsAggregation() {
    // CASE contains aggregation if any of its expressions contain aggregations
    if (caseExpression != null && caseExpression.containsAggregation()) {
      return true;
    }

    for (final CaseAlternative alternative : alternatives) {
      if (alternative.getWhenExpression().containsAggregation()
          || alternative.getThenExpression().containsAggregation()) {
        return true;
      }
    }

    if (elseExpression != null && elseExpression.containsAggregation()) {
      return true;
    }

    return false;
  }

  @Override
  public String getText() {
    // Reconstruct a properly spaced representation instead of returning the raw ANTLR text, whose
    // getText() concatenates tokens with no whitespace (e.g. "CASEWHENrISNOTNULLTHEN1ELSE0END").
    // The glued form hides variable references from downstream word-boundary analysis such as
    // CypherExecutionPlan.isEdgeVariableReferenced, which then wrongly dropped an OPTIONAL MATCH edge
    // variable used only inside a CASE (issue #5137).
    final StringBuilder sb = new StringBuilder("CASE");
    if (caseExpression != null)
      sb.append(' ').append(caseExpression.getText());
    for (final CaseAlternative alternative : alternatives)
      sb.append(" WHEN ").append(alternative.getWhenExpression().getText())
          .append(" THEN ").append(alternative.getThenExpression().getText());
    if (elseExpression != null)
      sb.append(" ELSE ").append(elseExpression.getText());
    sb.append(" END");
    return sb.toString();
  }

  public Expression getCaseExpression() {
    return caseExpression;
  }

  public List<CaseAlternative> getAlternatives() {
    return alternatives;
  }

  public Expression getElseExpression() {
    return elseExpression;
  }

  /**
   * Check if a value is considered true.
   */
  private boolean isTrue(final Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    // Non-null, non-false values are considered true in Cypher
    return true;
  }
}
