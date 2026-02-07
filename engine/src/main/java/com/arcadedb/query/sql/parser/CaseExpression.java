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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
import java.util.Map;

/**
 * Expression representing a SQL CASE expression.
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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseExpression extends MathExpression {
  private final Expression caseExpression; // null for simple form
  private final List<CaseAlternative> alternatives;
  private final Expression elseExpression; // null if no ELSE clause

  /**
   * Constructor for simple CASE form (no case expression).
   */
  public CaseExpression(final List<CaseAlternative> alternatives, final Expression elseExpression) {
    super(-1);
    this.caseExpression = null;
    this.alternatives = alternatives;
    this.elseExpression = elseExpression;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    return evaluateCase(currentRecord, context);
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    return evaluateCase(currentRecord, context);
  }

  /**
   * Constructor for extended CASE form (with case expression).
   */
  public CaseExpression(final Expression caseExpression, final List<CaseAlternative> alternatives,
                        final Expression elseExpression) {
    super(-1);
    this.caseExpression = caseExpression;
    this.alternatives = alternatives;
    this.elseExpression = elseExpression;
  }

  /**
   * Evaluate CASE expression with an Identifiable record.
   */
  private Object evaluateCase(final Identifiable currentRecord, final CommandContext context) {
    // Extended form: CASE expr WHEN value THEN result
    if (caseExpression != null) {
      final Object caseValue = caseExpression.execute(currentRecord, context);

      for (final CaseAlternative alternative : alternatives) {
        final Object whenValue = alternative.getWhenExpression().execute(currentRecord, context);

        // Check equality
        if (valuesEqual(caseValue, whenValue))
          return alternative.getThenExpression().execute(currentRecord, context);
      }
    }
    // Simple form: CASE WHEN condition THEN result
    else {
      for (final CaseAlternative alternative : alternatives) {
        final Boolean whenResult = alternative.getWhenCondition().evaluateExpression(currentRecord, context);

        // Check if condition is true
        if (isTrue(whenResult))
          return alternative.getThenExpression().execute(currentRecord, context);
      }
    }

    // No match found - use ELSE or return null
    if (elseExpression != null)
      return elseExpression.execute(currentRecord, context);

    return null;
  }

  /**
   * Evaluate CASE expression with a Result record.
   */
  private Object evaluateCase(final Result currentRecord, final CommandContext context) {
    // Extended form: CASE expr WHEN value THEN result
    if (caseExpression != null) {
      final Object caseValue = caseExpression.execute(currentRecord, context);

      for (final CaseAlternative alternative : alternatives) {
        final Object whenValue = alternative.getWhenExpression().execute(currentRecord, context);

        // Check equality
        if (valuesEqual(caseValue, whenValue))
          return alternative.getThenExpression().execute(currentRecord, context);
      }
    }
    // Simple form: CASE WHEN condition THEN result
    else {
      for (final CaseAlternative alternative : alternatives) {
        final Boolean whenResult = alternative.getWhenCondition().evaluateExpression(currentRecord, context);

        // Check if condition is true
        if (isTrue(whenResult))
          return alternative.getThenExpression().execute(currentRecord, context);
      }
    }

    // No match found - use ELSE or return null
    if (elseExpression != null)
      return elseExpression.execute(currentRecord, context);

    return null;
  }

  @Override
  public boolean isAggregate(final CommandContext ctx) {
    // CASE is aggregate if any of its expressions are aggregates
    if (caseExpression != null && caseExpression.isAggregate(ctx))
      return true;

    for (final CaseAlternative alternative : alternatives) {
      if (alternative.isSimpleForm()) {
        // Simple form uses WhereClause - check if it contains aggregates
        // For simplicity, assume WHERE clauses don't contain aggregates in CASE
      } else {
        if (alternative.getWhenExpression().isAggregate(ctx))
          return true;
      }
      if (alternative.getThenExpression().isAggregate(ctx))
        return true;
    }

    return elseExpression != null && elseExpression.isAggregate(ctx);
  }

  /**
   * Check if two values are equal, handling nulls properly.
   */
  private boolean valuesEqual(final Object a, final Object b) {
    if (a == null && b == null)
      return true;
    if (a == null || b == null)
      return false;
    return a.equals(b);
  }

  /**
   * Check if a value is considered true.
   */
  private boolean isTrue(final Object value) {
    if (value == null)
      return false;
    if (value instanceof Boolean)
      return (Boolean) value;
    // Non-null, non-false values are considered true
    return true;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CASE");
    if (caseExpression != null) {
      builder.append(" ");
      caseExpression.toString(params, builder);
    }
    for (final CaseAlternative alt : alternatives) {
      builder.append(" WHEN ");
      if (alt.isSimpleForm())
        alt.getWhenCondition().toString(params, builder);
      else
        alt.getWhenExpression().toString(params, builder);
      builder.append(" THEN ");
      alt.getThenExpression().toString(params, builder);
    }
    if (elseExpression != null) {
      builder.append(" ELSE ");
      elseExpression.toString(params, builder);
    }
    builder.append(" END");
  }

  @Override
  public CaseExpression copy() {
    if (caseExpression != null) {
      // Extended form
      return new CaseExpression(
          caseExpression.copy(),
          alternatives, // CaseAlternative doesn't need deep copy as it's immutable
          elseExpression != null ? elseExpression.copy() : null
      );
    } else {
      // Simple form
      return new CaseExpression(
          alternatives,
          elseExpression != null ? elseExpression.copy() : null
      );
    }
  }
}
