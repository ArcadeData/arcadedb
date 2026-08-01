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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

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
 * <p>
 * <b>This is now the fallback, not the path.</b> A body that has an AST is run from it, with the outer row handed in
 * as a seed row and nothing rewritten - see {@link CorrelatedSubqueryRunner}, which is where the list above stops
 * growing. What still arrives here is a body the statement builder declined (the best-effort build of issue #5626)
 * and the existential subquery {@link PatternPredicateExpression} synthesizes at runtime from a pattern it holds.
 * Fixing a correlation bug means asking first whether the shape can reach this class at all.
 * <p>
 * One thing it cannot do, and could not be taught to: correlate a <b>relationship</b> variable. An outer binding is
 * pinned by an injected {@code MATCH (v)}, which is node syntax, so a relationship-valued outer variable produced a
 * body that did not parse. That never surfaced while a failed body was absorbed into the expression's neutral value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CorrelatedSubqueryRewriter {
  /**
   * Keywords that can open a Cypher subquery body. Anything else is a bare pattern, and only a bare
   * pattern may be wrapped into a synthesized {@code MATCH}. {@code OPTIONAL} covers
   * {@code OPTIONAL MATCH}, {@code LOAD} covers {@code LOAD CSV} and {@code DETACH} covers
   * {@code DETACH DELETE}.
   */
  private static final String[] CLAUSE_KEYWORDS = { "MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "CALL", "FOREACH",
      "LOAD", "USE", "FINISH", "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "INSERT" };

  /**
   * Keywords that close the pattern of the MATCH a correlation condition is injected into: every
   * clause keyword, plus the sub-clauses that can trail a projection. {@code WHERE} is deliberately
   * absent - it is the one keyword the injection merges into rather than stops at.
   */
  private static final String[] CLAUSE_BOUNDARY_KEYWORDS = Stream.concat(Arrays.stream(CLAUSE_KEYWORDS),
      Stream.of("ORDER", "SKIP", "LIMIT", "UNION")).toArray(String[]::new);

  /**
   * Clauses that write, and so may not appear in the read-only subquery expressions. {@code DELETE}
   * also covers {@code DETACH DELETE}. {@code INSERT} is here because this engine treats it as a
   * synonym of {@code CREATE} - {@code ClauseDispatcher.handleInsert} builds a {@code CreateClause} -
   * so leaving it out let a body that is plainly a write reach the executor, where it produced the
   * expression's neutral value instead of an error.
   */
  private static final String[] UPDATE_CLAUSE_KEYWORDS = { "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "INSERT" };

  /**
   * First letters of each keyword array, so a per-character scan rejects a position with one array
   * read instead of walking every keyword. Derived, never hand-maintained: the whole point of this
   * class is that hand-maintained copies of the keyword list drift.
   */
  private static final boolean[] BOUNDARY_KEYWORD_FIRST_CHAR = firstCharTable(CLAUSE_BOUNDARY_KEYWORDS);

  private static final boolean[] UPDATE_KEYWORD_FIRST_CHAR = firstCharTable(UPDATE_CLAUSE_KEYWORDS);

  private CorrelatedSubqueryRewriter() {
  }

  private static boolean[] firstCharTable(final String[] keywords) {
    final boolean[] table = new boolean[128];
    for (final String keyword : keywords)
      table[keyword.charAt(0)] = true;
    return table;
  }

  /**
   * Tells whether a subquery body opens with a clause keyword rather than with a bare pattern.
   * <p>
   * Callers use this to decide whether the body still needs a synthesized {@code MATCH} in front of
   * it. The list has to stay complete: an unlisted keyword makes its clause look like a pattern, and
   * the resulting {@code MATCH UNWIND [1, 2] AS y RETURN y RETURN 1} does not parse. Because the
   * three subquery expressions absorb a failed body into their neutral value, that misclassification
   * surfaces only as a silently wrong {@code COUNT} of 0 or {@code EXISTS} of false (issue #5461).
   */
  public static boolean startsWithClauseKeyword(final String body) {
    return matchesAnyKeywordAt(body.trim().toUpperCase(Locale.ROOT), 0, CLAUSE_KEYWORDS, BOUNDARY_KEYWORD_FIRST_CHAR);
  }

  /**
   * Tells whether a subquery body contains a write clause, which the three read-only subquery
   * expressions must reject.
   * <p>
   * Only a keyword written as a clause counts: the scan skips string literals and backtick-quoted
   * identifiers, and requires a word boundary. The guard used to be a bare
   * {@code toUpperCase().contains("SET ")}, which could not tell a clause from ordinary user data, so
   * a read-only {@code WHERE n.name = 'SET x'} was rejected as an update (issue #5541).
   * <p>
   * Nesting is deliberately not tracked: a write nested inside a bracketed construct - {@code CALL {
   * ... CREATE ... }} - is still a write, and must still be rejected.
   * <p>
   * Comments are not skipped, so an update keyword inside one is still read as a clause. That blind
   * spot predates this scan and is not widened by it.
   */
  public static boolean containsUpdateClause(final String body) {
    final String upper = body.toUpperCase(Locale.ROOT);
    char quote = 0;

    for (int i = 0; i < upper.length(); i++) {
      final char c = upper.charAt(i);

      if (quote != 0) {
        // Inside a string literal or quoted identifier: only its closing quote matters. Backslash
        // escapes apply to string literals only - a backtick-quoted identifier escapes a literal
        // backtick by doubling it, which this toggle handles on its own (close then reopen).
        if (c == '\\' && quote != '`')
          i++;
        else if (c == quote)
          quote = 0;
        continue;
      }
      if (c == '\'' || c == '"' || c == '`') {
        quote = c;
        continue;
      }
      final int keywordLength = matchedKeywordLengthAt(upper, i, UPDATE_CLAUSE_KEYWORDS, UPDATE_KEYWORD_FIRST_CHAR);
      if (keywordLength > 0 && opensAClauseAt(upper, i + keywordLength))
        return true;
    }
    return false;
  }

  /**
   * Tells whether the keyword just matched is followed by something a write clause can actually take,
   * rather than being an identifier that happens to be spelled like one.
   * <p>
   * None of these keywords is reserved in this grammar - {@code RETURN 1 AS set} and
   * {@code RETURN 1 AS insert} both parse - so a scan that accepts any word boundary flags an alias
   * as a write. A clause is followed by its pattern or target, so only whitespace or an immediately
   * following {@code (} qualifies: that keeps {@code SET n.x = 1}, {@code DELETE n} and the
   * space-less {@code CREATE(n)}, while rejecting an alias at the end of the body or before a comma.
   * <p>
   * A colon disqualifies it again, because the keyword is then a map key: {@link #matchesKeywordAt}
   * already rejects the glued {@code {set: 1}}, and this catches {@code {set : 1}}.
   * <p>
   * What this cannot separate is an alias that is itself followed by a clause, as in
   * {@code WITH 1 AS set RETURN set} - lexically identical to {@code SET n.x = 1}. Telling those
   * apart needs clause position from the parse tree rather than a text scan.
   */
  private static boolean opensAClauseAt(final String upper, final int afterKeyword) {
    if (afterKeyword >= upper.length())
      return false;
    if (upper.charAt(afterKeyword) == '(')
      return true;
    if (!Character.isWhitespace(upper.charAt(afterKeyword)))
      return false;

    int pos = afterKeyword;
    while (pos < upper.length() && Character.isWhitespace(upper.charAt(pos)))
      pos++;
    return pos < upper.length() && upper.charAt(pos) != ':';
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

    if (trimmed.toUpperCase(Locale.ROOT).startsWith("MATCH")) {
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
    final String upper = query.toUpperCase(Locale.ROOT);
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
      else if (clauseStart < 0 && matchesAnyKeywordAt(upper, i, CLAUSE_BOUNDARY_KEYWORDS, BOUNDARY_KEYWORD_FIRST_CHAR))
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

  /**
   * {@code firstChars} must be a table built from a superset of {@code keywords} - see
   * {@link #firstCharTable} - so that it can never reject a position one of them would have matched.
   * {@link #CLAUSE_BOUNDARY_KEYWORDS} is such a superset of {@link #CLAUSE_KEYWORDS}.
   */
  private static boolean matchesAnyKeywordAt(final String upper, final int pos, final String[] keywords,
      final boolean[] firstChars) {
    return matchedKeywordLengthAt(upper, pos, keywords, firstChars) > 0;
  }

  /**
   * Length of the keyword matching at {@code pos}, or -1 when none does. Callers that only need a
   * yes/no answer use {@link #matchesAnyKeywordAt}; the length is for those that have to look at what
   * follows the keyword.
   */
  private static int matchedKeywordLengthAt(final String upper, final int pos, final String[] keywords,
      final boolean[] firstChars) {
    if (pos >= upper.length())
      return -1;
    final char first = upper.charAt(pos);
    if (first < firstChars.length && !firstChars[first])
      return -1;
    for (final String keyword : keywords)
      if (matchesKeywordAt(upper, pos, keyword))
        return keyword.length();
    return -1;
  }

  private static boolean isCypherIdentifierChar(final char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }
}
