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

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Map;
import java.util.logging.Level;

/**
 * Expression representing EXISTS predicate.
 * Examples:
 * - EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.name = 'Alice' }
 * - EXISTS { (n)-[:KNOWS]->() }
 * <p>
 * Returns true if the pattern/subquery has at least one match, false otherwise.
 */
public class ExistsExpression implements Expression {
  private final String subquery;
  private final String text;

  public ExistsExpression(final String subquery, final String text) {
    this.subquery = subquery;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Map<String, Object> params = CorrelatedSubqueryRewriter.newParams(context);
    final String modifiedSubquery = CorrelatedSubqueryRewriter.correlate(subquery, result, "__exists_", params,
        ExistsExpression::wrapNonMatchBody);
    try (final var resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params)) {
      return resultSet.hasNext();
    } catch (final Exception e) {
      // An existential subquery that cannot run is not the same as one that does not match, but the
      // Cypher contract here is a boolean, so the failure is absorbed. Trace it: a silent false is
      // exactly how the corrupted-subquery bug of issue #5464 stayed invisible for so long.
      LogManager.instance().log(ExistsExpression.class, Level.FINE, "Error on evaluating EXISTS subquery '%s'", e,
          modifiedSubquery);
      return false;
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
    final String upper = body.toUpperCase();
    if (upper.startsWith("RETURN") || upper.startsWith("WITH") || upper.startsWith("UNWIND")
        || upper.startsWith("CALL") || upper.startsWith("OPTIONAL"))
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
}
