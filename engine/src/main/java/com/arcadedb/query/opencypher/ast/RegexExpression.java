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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.TimeBoundRegex;

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
  // One deadline for the lifetime of this AST node, i.e. every row a WHERE ... =~ clause scans - not
  // recomputed per row, and not tied to compiledPattern's per-row recompilation (a row-varying pattern string
  // must not also reset the time budget). See MatchesCondition.matches() for the equivalent SQL-side rationale.
  private Long    deadline;

  public RegexExpression(final Expression expression, final Expression pattern) {
    this.expression = expression;
    this.pattern = pattern;
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    // Filtering semantics: a null (unknown) result excludes the row, same as false.
    return Boolean.TRUE.equals(evaluateTernary(result, context));
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value = expression.evaluate(result, context);
    // Cypher three-valued logic: a null operand makes the match unknown (null), not false.
    if (value == null)
      return null;

    final Object patternObj = pattern.evaluate(result, context);
    if (patternObj == null)
      return null;

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

    // Match against value. GlobalConfiguration.getValueAsLong(Database) resolves context.getDatabase()'s
    // per-database override (falling back to the compiled-in default if a database is ever not bound to the
    // context - RegexExpression is, in practice, only ever constructed by the openCypher parser and evaluated
    // with one already bound). See MatchesCondition.matches() for why context.getConfiguration() would silently
    // ignore a per-database override here.
    final String valueStr = value.toString();
    if (deadline == null)
      deadline = TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(context.getDatabase()));
    return TimeBoundRegex.matchesUntil(compiledPattern, valueStr, deadline);
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
