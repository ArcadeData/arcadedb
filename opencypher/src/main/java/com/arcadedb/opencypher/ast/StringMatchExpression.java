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
package com.arcadedb.opencypher.ast;

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
    final Object value = expression.evaluate(result, context);
    if (value == null) {
      return false;
    }

    final Object patternObj = pattern.evaluate(result, context);
    if (patternObj == null) {
      return false;
    }

    final String valueStr = value.toString();
    final String patternStr = patternObj.toString();

    switch (matchType) {
      case STARTS_WITH:
        return valueStr.startsWith(patternStr);
      case ENDS_WITH:
        return valueStr.endsWith(patternStr);
      case CONTAINS:
        return valueStr.contains(patternStr);
      default:
        return false;
    }
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
