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
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    try {
      final Map<String, Object> params = new HashMap<>();
      String modifiedSubquery = subquery;

      if (result != null) {
        final List<String> whereConditions = new ArrayList<>();
        final List<String> matchPatterns = new ArrayList<>();
        final List<String> withItems = new ArrayList<>();

        for (final String propertyName : result.getPropertyNames()) {
          // Skip internal variables (space-prefixed)
          if (propertyName.startsWith(" "))
            continue;
          final Object value = result.getProperty(propertyName);
          params.put(propertyName, value);

          if (value instanceof Identifiable) {
            final String rid = ((Identifiable) value).getIdentity().toString();
            final String paramName = "__collect_" + propertyName;
            params.put(paramName, rid);

            if (variableUsedInSubquery(modifiedSubquery, propertyName)) {
              whereConditions.add("id(" + propertyName + ") = $" + paramName);
              matchPatterns.add("(" + propertyName + ")");
            }
          } else if (value != null && variableUsedInSubquery(modifiedSubquery, propertyName)) {
            final String paramName = "__collect_" + propertyName;
            params.put(paramName, value);
            withItems.add("$" + paramName + " AS " + propertyName);
          }
        }

        if (!matchPatterns.isEmpty())
          modifiedSubquery = injectMatchPatterns(modifiedSubquery, matchPatterns);
        if (!whereConditions.isEmpty()) {
          final String conditionsStr = String.join(" AND ", whereConditions);
          modifiedSubquery = injectWhereConditions(modifiedSubquery, conditionsStr);
        }
        if (!withItems.isEmpty())
          modifiedSubquery = "WITH " + String.join(", ", withItems) + " " + modifiedSubquery;
      }

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
      }
      return collected;
    } catch (final Exception e) {
      return new ArrayList<>();
    }
  }

  /**
   * Checks if a variable name is used anywhere in the subquery text. Scans every occurrence and
   * accepts the variable as long as at least one is a whole-word match, so e.g. {@code p} is
   * detected in {@code WHERE p2.age > p.age} (where {@code p} appears first inside {@code p2}).
   */
  private static boolean variableUsedInSubquery(final String subquery, final String varName) {
    int fromIndex = 0;
    final int len = varName.length();
    while (true) {
      final int idx = subquery.indexOf(varName, fromIndex);
      if (idx < 0)
        return false;
      final boolean leftOk = idx == 0 || !isCypherIdentifierChar(subquery.charAt(idx - 1));
      final int end = idx + len;
      final boolean rightOk = end >= subquery.length() || !isCypherIdentifierChar(subquery.charAt(end));
      if (leftOk && rightOk)
        return true;
      fromIndex = idx + 1;
    }
  }

  private static boolean isCypherIdentifierChar(final char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Injects additional MATCH patterns into the subquery so correlated variables are in scope.
   */
  private static String injectMatchPatterns(final String subquery, final List<String> patterns) {
    final String trimmed = subquery.trim();
    final String upper = trimmed.toUpperCase();

    if (upper.startsWith("MATCH")) {
      int pos = 5;
      while (pos < trimmed.length() && Character.isWhitespace(trimmed.charAt(pos)))
        pos++;
      return trimmed.substring(0, pos) + String.join(", ", patterns) + ", " + trimmed.substring(pos);
    }

    return "MATCH " + String.join(", ", patterns) + " WITH * " + trimmed;
  }

  /**
   * Inject WHERE conditions after the first MATCH clause's pattern, before any subsequent clause.
   * Mirrors the logic in ExistsExpression so correlated variables are filtered to the outer row.
   */
  private static String injectWhereConditions(final String query, final String conditions) {
    final String upper = query.toUpperCase().trim();
    final int matchKeywordEnd = upper.startsWith("MATCH") ? 5 : 0;

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

      if (matchesKeywordAt(upper, i, "WHERE") && topWherePos < 0)
        topWherePos = i;
      else if (clauseStart < 0 && (matchesKeywordAt(upper, i, "WITH") || matchesKeywordAt(upper, i, "RETURN")
          || matchesKeywordAt(upper, i, "ORDER") || matchesKeywordAt(upper, i, "SKIP")
          || matchesKeywordAt(upper, i, "LIMIT") || matchesKeywordAt(upper, i, "UNION")))
        clauseStart = i;

      if (clauseStart >= 0)
        break;
    }

    if (clauseStart >= 0) {
      if (topWherePos >= 0 && topWherePos < clauseStart) {
        int insertPos = topWherePos + 5;
        while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
          insertPos++;
        return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
      }
      return query.substring(0, clauseStart) + "WHERE " + conditions + " " + query.substring(clauseStart);
    }

    if (topWherePos >= 0) {
      int insertPos = topWherePos + 5;
      while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
        insertPos++;
      return query.substring(0, insertPos) + conditions + " AND " + query.substring(insertPos);
    }

    return query + " WHERE " + conditions;
  }

  private static boolean matchesKeywordAt(final String upper, final int pos, final String keyword) {
    if (pos + keyword.length() > upper.length())
      return false;
    if (!upper.startsWith(keyword, pos))
      return false;
    if (pos > 0 && Character.isLetterOrDigit(upper.charAt(pos - 1)))
      return false;
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
