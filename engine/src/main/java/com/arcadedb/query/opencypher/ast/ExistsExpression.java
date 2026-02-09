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
        final List<String> matchPatterns = new ArrayList<>();

        for (final String propertyName : result.getPropertyNames()) {
          // Skip internal variables (space-prefixed)
          if (propertyName.startsWith(" "))
            continue;
          final Object value = result.getProperty(propertyName);
          params.put(propertyName, value);

          if (value instanceof Identifiable) {
            final String rid = ((Identifiable) value).getIdentity().toString();
            final String paramName = "__exists_" + propertyName;
            params.put(paramName, rid);

            // Check if this variable appears in the subquery
            if (variableUsedInSubquery(modifiedSubquery, propertyName)) {
              whereConditions.add("id(" + propertyName + ") = $" + paramName);
              // Add as extra MATCH pattern so the variable is in scope
              matchPatterns.add("(" + propertyName + ")");
            }
          }
        }

        if (!matchPatterns.isEmpty())
          modifiedSubquery = injectMatchPatterns(modifiedSubquery, matchPatterns);
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
   * Checks if a variable name is used anywhere in the subquery text.
   */
  private static boolean variableUsedInSubquery(final String subquery, final String varName) {
    final int idx = subquery.indexOf(varName);
    if (idx < 0)
      return false;
    // Verify it's a word boundary (not part of a longer identifier)
    if (idx > 0 && Character.isLetterOrDigit(subquery.charAt(idx - 1)))
      return false;
    final int end = idx + varName.length();
    if (end < subquery.length() && Character.isLetterOrDigit(subquery.charAt(end)))
      return false;
    return true;
  }

  /**
   * Injects additional MATCH patterns into the subquery.
   * If the subquery starts with MATCH, adds patterns as comma-separated items.
   * If it's a simple pattern (no MATCH keyword), wraps it with MATCH.
   */
  private static String injectMatchPatterns(final String subquery, final List<String> patterns) {
    final String trimmed = subquery.trim();
    final String upper = trimmed.toUpperCase();

    if (upper.startsWith("MATCH")) {
      // Find the end of "MATCH " and insert patterns with commas
      int pos = 5;
      while (pos < trimmed.length() && Character.isWhitespace(trimmed.charAt(pos)))
        pos++;
      return trimmed.substring(0, pos) + String.join(", ", patterns) + ", " + trimmed.substring(pos);
    }

    // Simple pattern subquery (no MATCH keyword) — add patterns after wrapping
    return "MATCH " + String.join(", ", patterns) + " WHERE " + trimmed;
  }

  /**
   * Inject WHERE conditions after the first MATCH clause's pattern, before any subsequent clause.
   * Handles subqueries like:
   * - MATCH (n)-->() RETURN true
   * - MATCH (n)-->(m) WITH n, count(*) AS c WHERE c = 3 RETURN true
   * - MATCH (n) WHERE n.prop > 5 RETURN true
   * Respects brace nesting (e.g., nested exists { ... } blocks).
   */
  private static String injectWhereConditions(final String query, final String conditions) {
    final String upper = query.toUpperCase().trim();
    final int matchKeywordEnd = upper.startsWith("MATCH") ? 5 : 0;

    // Find the first top-level clause keyword (WHERE, WITH, RETURN, etc.) at brace depth 0
    int clauseStart = -1;
    int topWherePos = -1;
    int braceDepth = 0;

    for (int i = matchKeywordEnd; i < query.length(); i++) {
      final char c = query.charAt(i);
      if (c == '{') {
        braceDepth++;
        continue;
      }
      if (c == '}') {
        braceDepth--;
        continue;
      }
      if (braceDepth > 0)
        continue;

      // Only check keywords at the top level (braceDepth == 0)
      if (matchesKeywordAt(upper, i, "WHERE") && topWherePos < 0)
        topWherePos = i;
      else if (clauseStart < 0 && (matchesKeywordAt(upper, i, "WITH") || matchesKeywordAt(upper, i, "RETURN")
          || matchesKeywordAt(upper, i, "ORDER") || matchesKeywordAt(upper, i, "SKIP")
          || matchesKeywordAt(upper, i, "LIMIT") || matchesKeywordAt(upper, i, "UNION")))
        clauseStart = i;

      // Stop once we find a non-WHERE clause keyword
      if (clauseStart >= 0)
        break;
    }

    if (clauseStart >= 0) {
      if (topWherePos >= 0 && topWherePos < clauseStart) {
        // Existing WHERE before the clause — append to it
        int insertPos = topWherePos + 5;
        while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
          insertPos++;
        return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
      }
      // Insert new WHERE before the clause keyword
      return query.substring(0, clauseStart) + "WHERE " + conditions + " " + query.substring(clauseStart);
    }

    // No subsequent clause — check for top-level WHERE
    if (topWherePos >= 0) {
      int insertPos = topWherePos + 5;
      while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
        insertPos++;
      return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
    }

    // Append WHERE at end
    return query + " WHERE " + conditions;
  }

  /**
   * Checks if the uppercase query string has a keyword at the given position,
   * ensuring it's a word boundary (not part of a longer identifier).
   */
  private static boolean matchesKeywordAt(final String upper, final int pos, final String keyword) {
    if (pos + keyword.length() > upper.length())
      return false;
    if (!upper.startsWith(keyword, pos))
      return false;
    // Check word boundary before
    if (pos > 0 && Character.isLetterOrDigit(upper.charAt(pos - 1)))
      return false;
    // Check word boundary after
    final int end = pos + keyword.length();
    if (end < upper.length() && Character.isLetterOrDigit(upper.charAt(end)))
      return false;
    return true;
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
