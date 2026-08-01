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
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

/**
 * Expression representing EXISTS predicate.
 * Examples:
 * - EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.name = 'Alice' }
 * - EXISTS { (n)-[:KNOWS]->() }
 * <p>
 * Returns true if the pattern/subquery has at least one match, false otherwise.
 * <p>
 * <b>A body that fails is not a body that does not match.</b> Both were once answered with {@code false}: the Cypher
 * contract here is a boolean, so any exception was logged at {@code FINE} and absorbed. That made a
 * {@code WHERE NOT EXISTS { ... } CREATE ...} de-duplicating guard degrade into an unconditional {@code CREATE} the
 * moment its body threw, and it kept a retryable {@code ConcurrentModificationException} from ever reaching the
 * server's auto-retry. The failure now propagates (issue #5656).
 */
public class ExistsExpression implements Expression {
  private final String          subquery;
  private final String          text;
  private final CypherStatement parsedSubquery;

  /**
   * Used by the runtime paths that synthesize an existential subquery from a pattern already held as an AST
   * ({@link PatternPredicateExpression}). Such an expression is built while the query runs, never handed to the
   * parse-time validation, so it carries no parsed body.
   */
  public ExistsExpression(final String subquery, final String text) {
    this(subquery, text, null);
  }

  public ExistsExpression(final String subquery, final String text, final CypherStatement parsedSubquery) {
    this.subquery = subquery;
    this.text = text;
    this.parsedSubquery = parsedSubquery;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    if (CorrelatedSubqueryRunner.canRun(parsedSubquery)) {
      try (final ResultSet resultSet = CorrelatedSubqueryRunner.run(parsedSubquery, result, context)) {
        return resultSet.hasNext();
      }
    }

    final Map<String, Object> params = CorrelatedSubqueryRewriter.newParams(context);
    final String modifiedSubquery = CorrelatedSubqueryRewriter.correlate(subquery, result, "__exists_", params,
        ExistsExpression::wrapNonMatchBody);
    try (final ResultSet resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params)) {
      return resultSet.hasNext();
    }
  }

  /**
   * Builds the correlated query when the body does not start with MATCH.
   * <p>
   * A body that starts with another clause keyword (RETURN, WITH, ...) only needs the extra patterns
   * prepended as their own MATCH; injecting a WHERE there would short-circuit the body's own clauses.
   * A bare pattern body becomes the predicate of the injected MATCH.
   */
  private static String wrapNonMatchBody(final String patterns, final String body) {
    if (CorrelatedSubqueryRewriter.startsWithClauseKeyword(body))
      return "MATCH " + patterns + " " + body;
    return "MATCH " + patterns + " WHERE " + body;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public String getSubquery() {
    return subquery;
  }

  /**
   * The body as an AST, or {@code null} when this expression was synthesized at runtime rather than parsed, or when
   * the statement builder declined the body (the best-effort build of issue #5626).
   * <p>
   * This is what the parse-time checks walk (#5626) and, since #5656, what actually executes: the outer row is handed
   * to it as a seed row rather than spliced into its text. {@link #getSubquery()} is the text that body was written
   * as, and is used only on the fallback path, when there is no AST to run.
   */
  public CypherStatement getParsedSubquery() {
    return parsedSubquery;
  }
}
