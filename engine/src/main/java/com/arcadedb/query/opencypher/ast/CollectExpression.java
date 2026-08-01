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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expression representing COLLECT { ... } subquery.
 * Example:
 * - COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name }
 * <p>
 * Runs the inner query once per outer row, with the outer row handed to the body
 * as a seed row, and returns the values produced by the inner RETURN as a list.
 * The list contains a single scalar per row when the RETURN projects one item,
 * otherwise a list per row.
 * <p>
 * <b>A body that fails is not a body that matches nothing.</b> Both were once answered with an empty list, because
 * the Cypher contract here is a list; the failure now propagates (issue #5656).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollectExpression implements Expression {
  private final String          subquery;
  private final String          text;
  private final CypherStatement parsedSubquery;

  public CollectExpression(final String subquery, final String text, final CypherStatement parsedSubquery) {
    this.subquery = subquery;
    this.text = text;
    this.parsedSubquery = parsedSubquery;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    if (CorrelatedSubqueryRunner.canRun(parsedSubquery)) {
      try (final ResultSet resultSet = CorrelatedSubqueryRunner.run(parsedSubquery, result, context)) {
        return collectRows(resultSet);
      }
    }

    final Map<String, Object> params = CorrelatedSubqueryRewriter.newParams(context);
    final String modifiedSubquery = CorrelatedSubqueryRewriter.correlate(subquery, result, "__collect_", params,
        (patterns, body) -> "MATCH " + patterns + " WITH * " + body);
    try (final ResultSet resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params)) {
      return collectRows(resultSet);
    }
  }

  /**
   * One entry per row: the projected value when the body returns a single item, the list of them otherwise.
   * <p>
   * A row with <i>no</i> visible column contributes {@code null} - the value of a projection that projected nothing -
   * rather than an empty list, which would read as "collected a row holding no values" and is not the same statement.
   * The grammar gives {@code COLLECT} one alternative, {@code COLLECT LCURLY queryWithLocalDefinitions RCURLY}, so the
   * body always carries a RETURN and the case is not reachable from a parsed query; it is written out because the
   * alternative was to leave the {@code else} branch quietly answering it.
   */
  private static List<Object> collectRows(final ResultSet resultSet) {
    final List<Object> collected = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      final Set<String> propertyNames = row.getPropertyNames();
      // Filter out internal (space-prefixed) names
      final List<String> visibleNames = new ArrayList<>(propertyNames.size());
      for (final String n : propertyNames) {
        if (!n.startsWith(" "))
          visibleNames.add(n);
      }
      if (visibleNames.isEmpty()) {
        collected.add(null);
      } else if (visibleNames.size() == 1) {
        collected.add(row.getProperty(visibleNames.get(0)));
      } else {
        final List<Object> rowValues = new ArrayList<>(visibleNames.size());
        for (final String n : visibleNames)
          rowValues.add(row.getProperty(n));
        collected.add(rowValues);
      }
    }
    return collected;
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
