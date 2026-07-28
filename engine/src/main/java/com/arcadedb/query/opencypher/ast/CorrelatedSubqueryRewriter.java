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
import java.util.function.BinaryOperator;

/**
 * Shared text rewriting used by the three subquery expressions - {@code EXISTS { }},
 * {@code COUNT { }} and {@code COLLECT { }} - to correlate an inner subquery with the outer row.
 * <p>
 * Each of them runs its body as a standalone Cypher query, once per outer row, so every outer
 * variable the body mentions has to be re-bound: graph entities are pinned by RID through an
 * injected {@code id(v) = $param} condition, scalars are brought into scope through a leading
 * {@code WITH $param AS v}.
 * <p>
 * The three used to carry three copies of this logic, and the copies drifted: only one of them
 * parenthesized an existing WHERE body before ANDing the correlation onto it (issues #4995/#5165),
 * and all three mistook an inline pattern predicate - {@code -[r:E WHERE r.tag = 'ok']->} or
 * {@code (b:A WHERE b.v = 2)} - for the clause-level WHERE, injecting the correlation inside the
 * pattern and corrupting the subquery (issue #5464). Keeping one implementation keeps them honest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CorrelatedSubqueryRewriter {

  private CorrelatedSubqueryRewriter() {
  }

  /**
   * Builds the parameter map for a correlated subquery, pre-seeded with the outer query's own parameters.
   * <p>
   * The body runs as a standalone statement, so every {@code $param} it mentions is resolved against THIS
   * map and not against the outer query's. Starting from an empty map left all of them unbound, and because
   * an unbound parameter evaluates to null rather than raising, the subquery simply matched nothing and the
   * three callers absorbed that into their neutral value - {@code EXISTS} false, {@code COUNT} 0,
   * {@code COLLECT} empty. Silent and load-bearing: a de-duplicating
   * {@code WHERE NOT EXISTS { MATCH (a)-[:E {id: $id}]->(b) } CREATE ...} guard degrades into an
   * unconditional CREATE.
   * <p>
   * Seeded FIRST so that the bindings {@link #correlate} adds still win: an outer row variable has to keep
   * shadowing a parameter of the same name, as it did before.
   *
   * @param context the outer command context, may be null when the expression is evaluated standalone
   */
  public static Map<String, Object> newParams(final CommandContext context) {
    final Map<String, Object> outerParams = context != null ? context.getInputParameters() : null;
    return outerParams == null || outerParams.isEmpty() ? new HashMap<>() : new HashMap<>(outerParams);
  }

  /**
   * Rewrites a subquery body so that it is correlated to the given outer row.
   *
   * @param subquery        the subquery body, as written inside the braces (already normalized to
   *                        carry a RETURN)
   * @param row             the outer row, or null when the expression is evaluated standalone
   * @param paramPrefix     prefix for the generated parameter names, unique per expression kind
   * @param params          receives the generated parameters; the caller passes it to the query
   * @param nonMatchWrapper builds the correlated query when the body does not start with MATCH; it
   *                        receives the comma-joined extra patterns and the trimmed body
   *
   * @return the rewritten subquery, or the original one when the row binds nothing it references
   */
  public static String correlate(final String subquery, final Result row, final String paramPrefix,
      final Map<String, Object> params, final BinaryOperator<String> nonMatchWrapper) {
    if (row == null)
      return subquery;

    final List<String> whereConditions = new ArrayList<>();
    final List<String> matchPatterns = new ArrayList<>();
    final List<String> withItems = new ArrayList<>();

    for (final String propertyName : row.getPropertyNames()) {
      // Skip internal variables (space-prefixed)
      if (propertyName.startsWith(" "))
        continue;
      final Object value = row.getProperty(propertyName);
      params.put(propertyName, value);

      if (value instanceof Identifiable) {
        final String paramName = paramPrefix + propertyName;
        params.put(paramName, ((Identifiable) value).getIdentity().toString());

        // Check if this variable appears in the subquery
        if (variableUsedInSubquery(subquery, propertyName)) {
          whereConditions.add("id(" + propertyName + ") = $" + paramName);
          // Add as extra MATCH pattern so the variable is in scope
          matchPatterns.add("(" + propertyName + ")");
        }
      } else if (value != null && variableUsedInSubquery(subquery, propertyName)) {
        // Scalar outer variable referenced in subquery: bring into scope via WITH.
        final String paramName = paramPrefix + propertyName;
        params.put(paramName, value);
        withItems.add("$" + paramName + " AS " + propertyName);
      }
    }

    String rewritten = subquery;
    if (!matchPatterns.isEmpty())
      rewritten = injectMatchPatterns(rewritten, matchPatterns, nonMatchWrapper);
    if (!whereConditions.isEmpty())
      rewritten = injectWhereConditions(rewritten, String.join(" AND ", whereConditions));
    if (!withItems.isEmpty())
      rewritten = "WITH " + String.join(", ", withItems) + " " + rewritten;
    return rewritten;
  }

  /**
   * Checks if a variable name is used anywhere in the subquery text. Scans every occurrence and
   * accepts the variable as long as at least one is a whole-word match, so e.g. {@code p} is
   * detected in {@code WHERE p2.age > p.age} (where {@code p} appears first inside {@code p2}).
   */
  public static boolean variableUsedInSubquery(final String subquery, final String varName) {
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

  /**
   * Injects the extra MATCH patterns that bring the outer-bound variables into scope. A body that
   * already starts with MATCH gets them as leading comma-separated items; anything else is handed to
   * the caller-supplied wrapper, because the three expression kinds need different shapes there.
   */
  private static String injectMatchPatterns(final String subquery, final List<String> patterns,
      final BinaryOperator<String> nonMatchWrapper) {
    final String trimmed = subquery.trim();
    final String joined = String.join(", ", patterns);

    if (trimmed.toUpperCase().startsWith("MATCH")) {
      // Find the end of "MATCH " and insert patterns with commas
      int pos = 5;
      while (pos < trimmed.length() && Character.isWhitespace(trimmed.charAt(pos)))
        pos++;
      return trimmed.substring(0, pos) + joined + ", " + trimmed.substring(pos);
    }

    return nonMatchWrapper.apply(joined, trimmed);
  }

  /**
   * Injects the correlation conditions after the first MATCH clause's pattern, before any subsequent
   * clause. Handles subqueries like:
   * <ul>
   *   <li>{@code MATCH (n)-->() RETURN true}</li>
   *   <li>{@code MATCH (n)-->(m) WITH n, count(*) AS c WHERE c = 3 RETURN true}</li>
   *   <li>{@code MATCH (n) WHERE n.prop > 5 RETURN true}</li>
   * </ul>
   * Only keywords that sit outside every bracketing construct and outside string literals count:
   * braces (nested subquery blocks, property maps), parentheses (node patterns) and square brackets
   * (relationship patterns, list literals). Without the parenthesis/bracket guard an inline pattern
   * predicate such as {@code -[r:E WHERE r.tag = 'ok']->} was mistaken for the clause-level WHERE
   * and the correlation was injected inside the pattern, corrupting the subquery (issue #5464).
   */
  public static String injectWhereConditions(final String query, final String conditions) {
    // NOTE: upper must not be trimmed - every index below addresses both strings interchangeably
    final String upper = query.toUpperCase();
    int scanStart = 0;
    while (scanStart < upper.length() && Character.isWhitespace(upper.charAt(scanStart)))
      scanStart++;
    final int matchKeywordEnd = upper.startsWith("MATCH", scanStart) ? scanStart + 5 : scanStart;

    // Find the first top-level clause keyword (WHERE, WITH, RETURN, ...) outside any nesting
    int clauseStart = -1;
    int topWherePos = -1;
    int depth = 0;
    char quote = 0;

    for (int i = matchKeywordEnd; i < query.length(); i++) {
      final char c = query.charAt(i);

      if (quote != 0) {
        // Inside a string literal: only its (non-escaped) closing quote matters
        if (c == '\\')
          i++;
        else if (c == quote)
          quote = 0;
        continue;
      }
      if (c == '\'' || c == '"' || c == '`') {
        quote = c;
        continue;
      }
      if (c == '{' || c == '(' || c == '[') {
        depth++;
        continue;
      }
      if (c == '}' || c == ')' || c == ']') {
        depth--;
        continue;
      }
      if (depth > 0)
        continue;

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
      if (topWherePos >= 0 && topWherePos < clauseStart)
        return wrapExistingWhere(query, topWherePos, clauseStart, conditions);
      // Insert new WHERE before the clause keyword
      return query.substring(0, clauseStart) + "WHERE " + conditions + " " + query.substring(clauseStart);
    }

    // No subsequent clause - check for top-level WHERE
    if (topWherePos >= 0)
      return wrapExistingWhere(query, topWherePos, query.length(), conditions);

    // Append WHERE at end
    return query + " WHERE " + conditions;
  }

  /**
   * Prepends the correlation {@code conditions} to an existing WHERE predicate, wrapping the
   * original predicate in parentheses so operator precedence is preserved. Without the parentheses
   * an injected {@code AND} would bind tighter than an inner {@code OR}
   * (e.g. {@code cond AND a OR b} -> {@code (cond AND a) OR b}), decoupling the OR branch from the
   * correlated outer row (issues #4995 and #5165).
   */
  private static String wrapExistingWhere(final String query, final int topWherePos, final int whereBodyEnd,
      final String conditions) {
    int insertPos = topWherePos + 5;
    while (insertPos < query.length() && Character.isWhitespace(query.charAt(insertPos)))
      insertPos++;
    final String body = query.substring(insertPos, whereBodyEnd).trim();
    final String tail = whereBodyEnd < query.length() ? " " + query.substring(whereBodyEnd) : "";
    return query.substring(0, insertPos) + conditions + " AND (" + body + ")" + tail;
  }

  /**
   * Checks if the uppercase query string has a keyword at the given position, ensuring it is a word
   * boundary (not part of a longer identifier or Cypher token). Underscore is treated as an
   * identifier character, and ':', '.', '$' are treated as non-boundary token prefixes, so that
   * patterns like [:WORKS_WITH], n.with, or $with do not falsely match a keyword fragment.
   */
  private static boolean matchesKeywordAt(final String upper, final int pos, final String keyword) {
    if (pos + keyword.length() > upper.length())
      return false;
    if (!upper.startsWith(keyword, pos))
      return false;
    // Check word boundary before - reject if preceded by an identifier char or a Cypher token
    // prefix (: for labels/types, . for property access, $ for parameters)
    if (pos > 0) {
      final char before = upper.charAt(pos - 1);
      if (isCypherIdentifierChar(before) || before == ':' || before == '.' || before == '$')
        return false;
    }
    // Check word boundary after
    final int end = pos + keyword.length();
    if (end < upper.length()) {
      final char after = upper.charAt(end);
      if (isCypherIdentifierChar(after) || after == ':' || after == '.' || after == '$')
        return false;
    }
    return true;
  }

  private static boolean isCypherIdentifierChar(final char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }
}
