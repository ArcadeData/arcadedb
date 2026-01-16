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

import java.util.*;

/**
 * Represents a SQL CASE expression.
 * <p>
 * Two forms are supported:
 * <p>
 * 1. Searched CASE (no case expression):
 * CASE
 *   WHEN condition1 THEN result1
 *   WHEN condition2 THEN result2
 *   ELSE defaultResult
 * END
 * <p>
 * 2. Simple CASE (with case expression):
 * CASE expression
 *   WHEN value1 THEN result1
 *   WHEN value2 THEN result2
 *   ELSE defaultResult
 * END
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseExpression extends MathExpression {
  protected Expression                 caseExpression;  // null for searched CASE
  protected final List<CaseWhenClause> whenClauses = new ArrayList<>();
  protected Expression                 elseExpression;  // null if no ELSE

  public CaseExpression(final int id) {
    super(id);
  }

  @Override
  public Object execute(final Identifiable currentRecord, final CommandContext context) {
    // Simple CASE: compare case expression to each WHEN value
    if (caseExpression != null) {
      final Object caseValue = caseExpression.execute(currentRecord, context);
      for (final CaseWhenClause whenClause : whenClauses) {
        final Object whenValue = whenClause.getWhenExpression().execute(currentRecord, context);
        if (valuesEqual(caseValue, whenValue))
          return whenClause.getThenExpression().execute(currentRecord, context);
      }
    }
    // Searched CASE: evaluate each WHEN condition as boolean
    else {
      for (final CaseWhenClause whenClause : whenClauses) {
        final Object whenResult = whenClause.getWhenCondition().evaluate(currentRecord, context);
        if (isTrue(whenResult))
          return whenClause.getThenExpression().execute(currentRecord, context);
      }
    }

    // No match - return ELSE or null
    if (elseExpression != null)
      return elseExpression.execute(currentRecord, context);
    return null;
  }

  @Override
  public Object execute(final Result currentRecord, final CommandContext context) {
    // Simple CASE: compare case expression to each WHEN value
    if (caseExpression != null) {
      final Object caseValue = caseExpression.execute(currentRecord, context);
      for (final CaseWhenClause whenClause : whenClauses) {
        final Object whenValue = whenClause.getWhenExpression().execute(currentRecord, context);
        if (valuesEqual(caseValue, whenValue))
          return whenClause.getThenExpression().execute(currentRecord, context);
      }
    }
    // Searched CASE: evaluate each WHEN condition as boolean
    else {
      for (final CaseWhenClause whenClause : whenClauses) {
        final Object whenResult = whenClause.getWhenCondition().evaluate(currentRecord, context);
        if (isTrue(whenResult))
          return whenClause.getThenExpression().execute(currentRecord, context);
      }
    }

    // No match - return ELSE or null
    if (elseExpression != null)
      return elseExpression.execute(currentRecord, context);
    return null;
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
    if (value instanceof Number)
      return ((Number) value).intValue() > 0;
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
    for (final CaseWhenClause whenClause : whenClauses) {
      builder.append(" ");
      whenClause.toString(params, builder);
    }
    if (elseExpression != null) {
      builder.append(" ELSE ");
      elseExpression.toString(params, builder);
    }
    builder.append(" END");
  }

  @Override
  public boolean isAggregate(final CommandContext context) {
    if (caseExpression != null && caseExpression.isAggregate(context))
      return true;
    for (final CaseWhenClause whenClause : whenClauses) {
      if (whenClause.isAggregate(context))
        return true;
    }
    if (elseExpression != null && elseExpression.isAggregate(context))
      return true;
    return false;
  }

  @Override
  public boolean isEarlyCalculated(final CommandContext context) {
    if (caseExpression != null && !caseExpression.isEarlyCalculated(context))
      return false;
    for (final CaseWhenClause whenClause : whenClauses) {
      if (!whenClause.isEarlyCalculated(context))
        return false;
    }
    if (elseExpression != null && !elseExpression.isEarlyCalculated(context))
      return false;
    return true;
  }

  @Override
  public CaseExpression copy() {
    final CaseExpression result = new CaseExpression(-1);
    result.caseExpression = caseExpression == null ? null : caseExpression.copy();
    for (final CaseWhenClause whenClause : whenClauses)
      result.whenClauses.add(whenClause.copy());
    result.elseExpression = elseExpression == null ? null : elseExpression.copy();
    return result;
  }

  @Override
  public void extractSubQueries(final SubQueryCollector collector) {
    if (caseExpression != null)
      caseExpression.extractSubQueries(collector);
    for (final CaseWhenClause whenClause : whenClauses)
      whenClause.extractSubQueries(collector);
    if (elseExpression != null)
      elseExpression.extractSubQueries(collector);
  }

  @Override
  public void extractSubQueries(final Identifier letAlias, final SubQueryCollector collector) {
    if (caseExpression != null)
      caseExpression.extractSubQueries(collector);
    for (final CaseWhenClause whenClause : whenClauses)
      whenClause.extractSubQueries(collector);
    if (elseExpression != null)
      elseExpression.extractSubQueries(collector);
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { caseExpression, whenClauses, elseExpression };
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    final List<SimpleNode> elements = new ArrayList<>();
    if (caseExpression != null)
      elements.add(caseExpression);
    for (final CaseWhenClause whenClause : whenClauses) {
      if (whenClause.getWhenCondition() != null)
        elements.add(whenClause.getWhenCondition());
      else if (whenClause.getWhenExpression() != null)
        elements.add(whenClause.getWhenExpression());
      if (whenClause.getThenExpression() != null)
        elements.add(whenClause.getThenExpression());
    }
    if (elseExpression != null)
      elements.add(elseExpression);
    return elements.toArray(new SimpleNode[0]);
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    final List<String> result = new ArrayList<>();
    if (caseExpression != null) {
      final List<String> x = caseExpression.getMatchPatternInvolvedAliases();
      if (x != null)
        result.addAll(x);
    }
    for (final CaseWhenClause whenClause : whenClauses) {
      if (whenClause.getWhenCondition() != null) {
        final List<String> x = whenClause.getWhenCondition().getMatchPatternInvolvedAliases();
        if (x != null)
          result.addAll(x);
      } else if (whenClause.getWhenExpression() != null) {
        final List<String> x = whenClause.getWhenExpression().getMatchPatternInvolvedAliases();
        if (x != null)
          result.addAll(x);
      }
      if (whenClause.getThenExpression() != null) {
        final List<String> x = whenClause.getThenExpression().getMatchPatternInvolvedAliases();
        if (x != null)
          result.addAll(x);
      }
    }
    if (elseExpression != null) {
      final List<String> x = elseExpression.getMatchPatternInvolvedAliases();
      if (x != null)
        result.addAll(x);
    }
    if (result.isEmpty())
      return null;
    return result;
  }

  @Override
  public boolean refersToParent() {
    if (caseExpression != null && caseExpression.refersToParent())
      return true;
    for (final CaseWhenClause whenClause : whenClauses) {
      if (whenClause.getWhenCondition() != null && whenClause.getWhenCondition().refersToParent())
        return true;
      if (whenClause.getWhenExpression() != null && whenClause.getWhenExpression().refersToParent())
        return true;
      if (whenClause.getThenExpression() != null && whenClause.getThenExpression().refersToParent())
        return true;
    }
    if (elseExpression != null && elseExpression.refersToParent())
      return true;
    return false;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final CaseExpression that = (CaseExpression) o;
    return Objects.equals(caseExpression, that.caseExpression) && Objects.equals(whenClauses, that.whenClauses) && Objects.equals(
        elseExpression, that.elseExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(caseExpression, whenClauses, elseExpression);
  }

  // Getters
  public Expression getCaseExpression() {
    return caseExpression;
  }

  public List<CaseWhenClause> getWhenClauses() {
    return whenClauses;
  }

  public Expression getElseExpression() {
    return elseExpression;
  }
}
