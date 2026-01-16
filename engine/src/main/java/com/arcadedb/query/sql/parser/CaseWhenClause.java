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

import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a single WHEN...THEN clause in a CASE expression.
 * <p>
 * For searched CASE (CASE WHEN condition THEN result END):
 * - whenCondition contains the boolean condition (OrBlock)
 * <p>
 * For simple CASE (CASE expr WHEN value THEN result END):
 * - whenExpression contains the value to compare against
 * <p>
 * Example: WHEN age < 18 THEN 'minor'
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseWhenClause extends SimpleNode {
  protected OrBlock    whenCondition;   // For searched CASE (boolean condition)
  protected Expression whenExpression;  // For simple CASE (value to compare)
  protected Expression thenExpression;

  public CaseWhenClause(final int id) {
    super(id);
  }

  public boolean isAggregate(final CommandContext context) {
    // OrBlock (whenCondition) doesn't support isAggregate - conditions typically don't contain aggregates
    if (whenExpression != null && whenExpression.isAggregate(context))
      return true;
    if (thenExpression != null && thenExpression.isAggregate(context))
      return true;
    return false;
  }

  public boolean isEarlyCalculated(final CommandContext context) {
    // OrBlock (whenCondition) doesn't support isEarlyCalculated - return false (conservative)
    if (whenCondition != null)
      return false;
    if (whenExpression != null && !whenExpression.isEarlyCalculated(context))
      return false;
    if (thenExpression != null && !thenExpression.isEarlyCalculated(context))
      return false;
    return true;
  }

  public CaseWhenClause copy() {
    final CaseWhenClause result = new CaseWhenClause(-1);
    result.whenCondition = whenCondition == null ? null : whenCondition.copy();
    result.whenExpression = whenExpression == null ? null : whenExpression.copy();
    result.thenExpression = thenExpression == null ? null : thenExpression.copy();
    return result;
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (whenCondition != null)
      whenCondition.extractSubQueries(collector);
    if (whenExpression != null)
      whenExpression.extractSubQueries(collector);
    if (thenExpression != null)
      thenExpression.extractSubQueries(collector);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("WHEN ");
    if (whenCondition != null)
      whenCondition.toString(params, builder);
    else if (whenExpression != null)
      whenExpression.toString(params, builder);
    builder.append(" THEN ");
    if (thenExpression != null)
      thenExpression.toString(params, builder);
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { whenCondition, whenExpression, thenExpression };
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    if (whenCondition != null)
      return new SimpleNode[] { whenCondition, thenExpression };
    return new SimpleNode[] { whenExpression, thenExpression };
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final CaseWhenClause that = (CaseWhenClause) o;
    return Objects.equals(whenCondition, that.whenCondition) &&
           Objects.equals(whenExpression, that.whenExpression) &&
           Objects.equals(thenExpression, that.thenExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(whenCondition, whenExpression, thenExpression);
  }

  /**
   * Returns the WHEN condition for searched CASE expressions.
   */
  public OrBlock getWhenCondition() {
    return whenCondition;
  }

  /**
   * Returns the WHEN expression for simple CASE expressions.
   */
  public Expression getWhenExpression() {
    return whenExpression;
  }

  public Expression getThenExpression() {
    return thenExpression;
  }
}
