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

/**
 * String matching expression for WHERE clauses.
 * Supports STARTS WITH, ENDS WITH, and CONTAINS operators.
 * Example: n.name STARTS WITH 'A', n.email ENDS WITH '@example.com', n.name CONTAINS 'li'
 */
public class StringMatchExpression implements BooleanExpression {
  public enum MatchType {
    STARTS_WITH,
    ENDS_WITH,
    CONTAINS
  }

  private final Expression expression;
  private final Expression pattern;
  private final MatchType matchType;

  public StringMatchExpression(final Expression expression, final Expression pattern, final MatchType matchType) {
    this.expression = expression;
    this.pattern = pattern;
    this.matchType = matchType;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    final Object patternObj = pattern.evaluate(result, context);

    // Null or non-string operands return null (three-valued logic)
    if (value == null || patternObj == null)
      return null;
    if (!(value instanceof String) || !(patternObj instanceof String))
      return null;

    final String valueStr = (String) value;
    final String patternStr = (String) patternObj;

    return switch (matchType) {
      case STARTS_WITH -> valueStr.startsWith(patternStr);
      case ENDS_WITH -> valueStr.endsWith(patternStr);
      case CONTAINS -> valueStr.contains(patternStr);
    };
  }

  @Override
  public String getText() {
    final String op;
    switch (matchType) {
      case STARTS_WITH:
        op = " STARTS WITH ";
        break;
      case ENDS_WITH:
        op = " ENDS WITH ";
        break;
      case CONTAINS:
        op = " CONTAINS ";
        break;
      default:
        op = " UNKNOWN ";
    }
    return expression.getText() + op + pattern.getText();
  }

  public Expression getExpression() {
    return expression;
  }

  public Expression getPattern() {
    return pattern;
  }

  public MatchType getMatchType() {
    return matchType;
  }
}
