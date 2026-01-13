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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Regular expression matching expression for WHERE clauses.
 * Uses the =~ operator.
 * Example: n.name =~ '.*Smith', n.email =~ '.*@example\\.com'
 */
public class RegexExpression implements BooleanExpression {
  private final Expression expression;
  private final Expression pattern;
  private Pattern compiledPattern;

  public RegexExpression(final Expression expression, final Expression pattern) {
    this.expression = expression;
    this.pattern = pattern;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    if (value == null) {
      return false;
    }

    // Get pattern string
    final Object patternObj = pattern.evaluate(result, context);
    if (patternObj == null) {
      return false;
    }

    final String patternStr = patternObj.toString();

    // Compile pattern if not already compiled or if pattern changed
    if (compiledPattern == null || !compiledPattern.pattern().equals(patternStr)) {
      try {
        compiledPattern = Pattern.compile(patternStr);
      } catch (final PatternSyntaxException e) {
        // Invalid regex pattern
        return false;
      }
    }

    // Match against value
    final String valueStr = value.toString();
    return compiledPattern.matcher(valueStr).matches();
  }

  @Override
  public String getText() {
    return expression.getText() + " =~ " + pattern.getText();
  }

  public Expression getExpression() {
    return expression;
  }

  public Expression getPattern() {
    return pattern;
  }
}
