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
  private final String text;

  /**
   * Constructor for simple CASE form (no case expression).
   */
  public CaseExpression(final List<CaseAlternative> alternatives, final Expression elseExpression, final String text) {
    this(null, alternatives, elseExpression, text);
  }

  /**
   * Constructor for extended CASE form (with case expression).
   */
  public CaseExpression(final Expression caseExpression, final List<CaseAlternative> alternatives,
                        final Expression elseExpression, final String text) {
    this.caseExpression = caseExpression;
    this.alternatives = alternatives;
    this.elseExpression = elseExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Extended form: CASE expr WHEN value THEN result
    if (caseExpression != null) {
      final Object caseValue = caseExpression.evaluate(result, context);

      for (final CaseAlternative alternative : alternatives) {
        final Object whenValue = alternative.getWhenExpression().evaluate(result, context);

        // Check equality
        if (valuesEqual(caseValue, whenValue)) {
          return alternative.getThenExpression().evaluate(result, context);
        }
      }
    }
    // Simple form: CASE WHEN condition THEN result
    else {
      for (final CaseAlternative alternative : alternatives) {
        final Object whenResult = alternative.getWhenExpression().evaluate(result, context);

        // Check if condition is true
        if (isTrue(whenResult)) {
          return alternative.getThenExpression().evaluate(result, context);
        }
      }
    }

    // No match found - use ELSE or return null
    if (elseExpression != null) {
      return elseExpression.evaluate(result, context);
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
    return text;
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
   * Check if two values are equal, handling nulls properly.
   */
  private boolean valuesEqual(final Object a, final Object b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    return a.equals(b);
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
