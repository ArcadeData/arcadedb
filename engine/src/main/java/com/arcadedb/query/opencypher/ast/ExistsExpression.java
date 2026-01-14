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
    // Execute the subquery with access to variables from the outer query
    // Variables from the outer query need to be bound to specific nodes/edges in the EXISTS pattern

    try {
      // Build parameter map and modify subquery to bind variables
      final java.util.Map<String, Object> params = new java.util.HashMap<>();
      String modifiedSubquery = subquery;

      if (result != null) {
        final java.util.List<String> whereConditions = new java.util.ArrayList<>();

        // Extract variables from the result
        for (final String propertyName : result.getPropertyNames()) {
          final Object value = result.getProperty(propertyName);
          params.put(propertyName, value);

          // If it's a vertex or edge, add a WHERE condition to bind it using id()
          if (value instanceof com.arcadedb.database.Identifiable) {
            // Use id() function to compare by RID: id(varName) = 'rid'
            final String rid = ((com.arcadedb.database.Identifiable) value).getIdentity().toString();
            whereConditions.add("id(" + propertyName + ") = '" + rid + "'");
          }
        }

        // Inject WHERE conditions into the subquery
        if (!whereConditions.isEmpty()) {
          final String conditionsStr = String.join(" AND ", whereConditions);

          // Normalize the subquery by adding spaces around WHERE keyword if missing
          // This handles cases like "WHEREc.name" -> "WHERE c.name"
          modifiedSubquery = modifiedSubquery.replaceAll("(?i)WHERE(?=[A-Za-z])", "WHERE ");

          final String whereClause = " WHERE " + conditionsStr;

          // Find where to inject the WHERE clause
          // If subquery already has WHERE, append with AND
          // Otherwise, add before RETURN
          if (modifiedSubquery.matches("(?i).*\\bWHERE\\b.*")) {
            // WHERE clause exists, add conditions with AND
            // Insert right after the WHERE keyword
            modifiedSubquery = modifiedSubquery.replaceFirst("(?i)\\bWHERE\\s+",
                java.util.regex.Matcher.quoteReplacement("WHERE " + conditionsStr + " AND "));
          } else if (modifiedSubquery.matches("(?i).*\\bRETURN\\b.*")) {
            modifiedSubquery = modifiedSubquery.replaceFirst("(?i)\\bRETURN\\b",
                java.util.regex.Matcher.quoteReplacement(whereClause + " RETURN"));
          } else {
            // No RETURN clause, append WHERE at the end
            modifiedSubquery = modifiedSubquery + whereClause;
          }
        }
      }

      // Execute the modified subquery with parameters
      final var resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params);
      final boolean exists = resultSet.hasNext();
      resultSet.close();
      return exists;
    } catch (final Exception e) {
      // If the subquery fails to parse/execute, return false
      return false;
    }
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
