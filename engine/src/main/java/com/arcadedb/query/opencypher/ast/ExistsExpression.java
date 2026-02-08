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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

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
    try {
      final Map<String, Object> params = new HashMap<>();
      String modifiedSubquery = subquery;

      if (result != null) {
        final List<String> whereConditions = new ArrayList<>();

        for (final String propertyName : result.getPropertyNames()) {
          final Object value = result.getProperty(propertyName);
          params.put(propertyName, value);

          if (value instanceof Identifiable) {
            final String rid = ((Identifiable) value).getIdentity().toString();
            final String paramName = "__exists_" + propertyName;
            params.put(paramName, rid);
            whereConditions.add("id(" + propertyName + ") = $" + paramName);
          }
        }

        if (!whereConditions.isEmpty()) {
          final String conditionsStr = String.join(" AND ", whereConditions);
          modifiedSubquery = injectWhereConditions(modifiedSubquery, conditionsStr);
        }
      }

      final var resultSet = context.getDatabase().query("opencypher", modifiedSubquery, params);
      final boolean exists = resultSet.hasNext();
      resultSet.close();
      return exists;
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Inject WHERE conditions after the first MATCH clause's pattern, before any subsequent clause.
   * Handles subqueries like:
   * - MATCH (n)-->() RETURN true
   * - MATCH (n)-->(m) WITH n, count(*) AS c WHERE c = 3 RETURN true
   * - MATCH (n) WHERE n.prop > 5 RETURN true
   */
  private static String injectWhereConditions(final String query, final String conditions) {
    // Find the first clause keyword after the initial MATCH pattern
    final java.util.regex.Pattern clausePattern = java.util.regex.Pattern.compile(
        "\\b(WITH|RETURN|ORDER\\s+BY|SKIP|LIMIT|UNION)\\b",
        java.util.regex.Pattern.CASE_INSENSITIVE);

    // Skip past the initial "MATCH " keyword
    final String upper = query.toUpperCase().trim();
    final int matchKeywordEnd = upper.startsWith("MATCH") ? 5 : 0;

    final java.util.regex.Matcher clauseMatcher = clausePattern.matcher(query);
    if (clauseMatcher.find(matchKeywordEnd)) {
      final int clauseStart = clauseMatcher.start();
      final String beforeClause = query.substring(0, clauseStart);

      // Check if there's a WHERE in the MATCH clause (before the first WITH/RETURN)
      final int wherePos = beforeClause.toUpperCase().lastIndexOf("WHERE");
      if (wherePos >= 0) {
        // Append to existing MATCH WHERE: insert conditions after "WHERE "
        int insertPos = wherePos + 5;
        while (insertPos < beforeClause.length() && Character.isWhitespace(beforeClause.charAt(insertPos)))
          insertPos++;
        return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
      }
      // Insert new WHERE clause before the next clause keyword
      return query.substring(0, clauseStart) + "WHERE " + conditions + " " + query.substring(clauseStart);
    }

    // No subsequent clause found — check for existing WHERE
    final int wherePos = query.toUpperCase().indexOf("WHERE");
    if (wherePos >= 0) {
      int insertPos = wherePos + 5;
      while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
        insertPos++;
      return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
    }

    // Append WHERE at end
    return query + " WHERE " + conditions;
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
