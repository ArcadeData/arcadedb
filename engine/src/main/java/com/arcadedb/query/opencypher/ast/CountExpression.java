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
 * Expression representing a {@code COUNT { ... }} pattern / subquery expression.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code COUNT { (p)-[:OWNS]->(:Dog) }}</li>
 *   <li>{@code COUNT { MATCH (n)-[:KNOWS]->(f) WHERE f.age > 18 }}</li>
 * </ul>
 * Runs the inner pattern or subquery once per outer row, with the outer row handed to the body as a seed row, and
 * returns the number of matches as a long.
 * <p>
 * <b>A body that fails is not a body that matches nothing.</b> Both were once answered with {@code 0}, because the
 * Cypher contract here is a number; the failure now propagates (issue #5656).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CountExpression implements Expression {
  private final String          subquery;
  private final String          text;
  private final CypherStatement parsedSubquery;

  public CountExpression(final String subquery, final String text, final CypherStatement parsedSubquery) {
    this.subquery = subquery;
    this.text = text;
    this.parsedSubquery = parsedSubquery;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // The number is asked for directly rather than counted off the rows: a body whose row count is its match count -
    // the shape a bare pattern is normalised into - is then answered by a count push-down instead of a scan (#5715).
    if (CorrelatedSubqueryRunner.canRun(parsedSubquery))
      return CorrelatedSubqueryRunner.countRows(parsedSubquery, result, context);

    final Map<String, Object> params = CorrelatedSubqueryRewriter.newParams(context);
    final String modifiedSubquery = CorrelatedSubqueryRewriter.correlate(subquery, result, "__count_", params,
        CountExpression::wrapNonMatchBody);
    try (final ResultSet resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params)) {
      return countRows(resultSet);
    }
  }

  private static long countRows(final ResultSet resultSet) {
    long count = 0L;
    while (resultSet.hasNext()) {
      resultSet.next();
      count++;
    }
    return count;
  }

  /**
   * Builds the correlated query when the body does not start with MATCH.
   * <p>
   * A body that opens with another clause keyword only needs the extra patterns prepended as their own
   * MATCH; comma-splicing them into the body's first clause instead produced the unparseable
   * {@code MATCH (n), UNWIND n.tags AS t RETURN t} and the failure was absorbed as a count of zero.
   * A bare pattern body is joined to the injected patterns as a further pattern of the same MATCH.
   */
  private static String wrapNonMatchBody(final String patterns, final String body) {
    if (CorrelatedSubqueryRewriter.startsWithClauseKeyword(body))
      return "MATCH " + patterns + " " + body;
    // Defensive mirror of EXISTS: a bare pattern is normalized into "MATCH ... RETURN 1" when the
    // expression is built, so the correlation takes the leading-MATCH path and never arrives here.
    // Kept so the wrapper stays correct if that normalization ever moves.
    return "MATCH " + patterns + ", " + body + " RETURN 1";
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
   * The body as an AST, or {@code null} when the statement builder declined it (the best-effort build of issue #5626).
   * <p>
   * This is what the parse-time checks walk (#5626) and, since #5656, what actually executes: the outer row is handed
   * to it as a seed row rather than spliced into its text. {@link #getSubquery()} is the text that body was written
   * as, and is used only on the fallback path, when there is no AST to run.
   */
  public CypherStatement getParsedSubquery() {
    return parsedSubquery;
  }
}
