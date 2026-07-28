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
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Expression representing COLLECT { ... } subquery.
 * Example:
 * - COLLECT { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name }
 * <p>
 * Runs the inner query once per outer row, with correlated variables bound via
 * parameters, and returns the values produced by the inner RETURN as a list.
 * The list contains a single scalar per row when the RETURN projects one item,
 * otherwise a list per row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CollectExpression implements Expression {
  private final String subquery;
  private final String text;

  public CollectExpression(final String subquery, final String text) {
    this.subquery = subquery;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Map<String, Object> params = CorrelatedSubqueryRewriter.newParams(context);
    final String modifiedSubquery = CorrelatedSubqueryRewriter.correlate(subquery, result, "__collect_", params,
        (patterns, body) -> "MATCH " + patterns + " WITH * " + body);

    final List<Object> collected = new ArrayList<>();
    try (final ResultSet resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final Set<String> propertyNames = row.getPropertyNames();
        // Filter out internal (space-prefixed) names
        final List<String> visibleNames = new ArrayList<>(propertyNames.size());
        for (final String n : propertyNames) {
          if (!n.startsWith(" "))
            visibleNames.add(n);
        }
        if (visibleNames.size() == 1) {
          collected.add(row.getProperty(visibleNames.get(0)));
        } else {
          final List<Object> rowValues = new ArrayList<>(visibleNames.size());
          for (final String n : visibleNames)
            rowValues.add(row.getProperty(n));
          collected.add(rowValues);
        }
      }
    } catch (final Exception e) {
      // The Cypher contract here is a list, so a subquery that cannot run is absorbed as empty.
      // Trace it: a silent empty list is how the corrupted-subquery bug of issue #5464 stayed invisible.
      LogManager.instance().log(CollectExpression.class, Level.FINE, "Error on evaluating COLLECT subquery '%s'", e,
          modifiedSubquery);
      return new ArrayList<>();
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
}
