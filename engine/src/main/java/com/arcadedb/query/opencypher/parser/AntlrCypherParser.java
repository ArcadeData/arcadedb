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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cypher parser using ANTLR4.
 * Phase 1: Simple regex-based parser for basic MATCH queries.
 * TODO: Integrate full ANTLR4-generated parser from Cypher25Lexer/Parser grammars.
 */
public class AntlrCypherParser {
  private static final Pattern MATCH_PATTERN = Pattern.compile("MATCH\\s+(.+?)(?:WHERE|RETURN|SET|DELETE|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CREATE_PATTERN = Pattern.compile("CREATE\\s+(.+?)(?:SET|RETURN|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern MERGE_PATTERN = Pattern.compile("MERGE\\s+(.+?)(?:SET|RETURN|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SET_PATTERN = Pattern.compile("SET\\s+(.+?)(?:RETURN|DELETE|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DELETE_PATTERN = Pattern.compile("(DETACH\\s+)?DELETE\\s+(.+?)(?:RETURN|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern WHERE_PATTERN = Pattern.compile("WHERE\\s+(.+?)(?:RETURN|SET|DELETE|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern RETURN_PATTERN = Pattern.compile("RETURN\\s+(.+?)(?:ORDER BY|SKIP|LIMIT|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ORDER_BY_PATTERN = Pattern.compile("ORDER BY\\s+(.+?)(?:SKIP|LIMIT|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SKIP_PATTERN = Pattern.compile("SKIP\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern LIMIT_PATTERN = Pattern.compile("LIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern HAS_CREATE_PATTERN = Pattern.compile("\\bCREATE\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern HAS_MERGE_PATTERN = Pattern.compile("\\bMERGE\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern HAS_DELETE_PATTERN = Pattern.compile("\\bDELETE\\b", Pattern.CASE_INSENSITIVE);

  private final DatabaseInternal database;

  public AntlrCypherParser(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Parses a Cypher query string into a CypherStatement AST.
   * Phase 1: Basic regex parsing for MATCH/WHERE/RETURN.
   * TODO: Replace with full ANTLR4 parser integration.
   *
   * @param query the Cypher query string
   * @return parsed CypherStatement
   * @throws CommandParsingException if query cannot be parsed
   */
  public CypherStatement parse(final String query) {
    if (query == null || query.trim().isEmpty())
      throw new CommandParsingException("Query cannot be empty");

    final String trimmedQuery = query.trim();

    try {
      // Extract MATCH clause
      final List<MatchClause> matchClauses = new ArrayList<>();
      final Matcher matchMatcher = MATCH_PATTERN.matcher(trimmedQuery);
      if (matchMatcher.find()) {
        final String pattern = matchMatcher.group(1).trim();
        // Phase 2+: Parse pattern into PathPattern objects
        final List<PathPattern> pathPatterns = PatternParser.parsePathPatterns(pattern);
        matchClauses.add(new MatchClause(pathPatterns, false));
      }

      // Extract CREATE clause
      CreateClause createClause = null;
      final Matcher createMatcher = CREATE_PATTERN.matcher(trimmedQuery);
      if (createMatcher.find()) {
        final String pattern = createMatcher.group(1).trim();
        final List<PathPattern> pathPatterns = PatternParser.parsePathPatterns(pattern);
        createClause = new CreateClause(pathPatterns);
      }

      // Extract MERGE clause
      MergeClause mergeClause = null;
      final Matcher mergeMatcher = MERGE_PATTERN.matcher(trimmedQuery);
      if (mergeMatcher.find()) {
        final String pattern = mergeMatcher.group(1).trim();
        final List<PathPattern> pathPatterns = PatternParser.parsePathPatterns(pattern);
        if (!pathPatterns.isEmpty()) {
          mergeClause = new MergeClause(pathPatterns.get(0));
        }
      }

      // Extract SET clause
      SetClause setClause = null;
      final Matcher setMatcher = SET_PATTERN.matcher(trimmedQuery);
      if (setMatcher.find()) {
        final String setStr = setMatcher.group(1).trim();
        setClause = parseSet(setStr);
      }

      // Extract DELETE clause
      DeleteClause deleteClause = null;
      final Matcher deleteMatcher = DELETE_PATTERN.matcher(trimmedQuery);
      if (deleteMatcher.find()) {
        final boolean detach = deleteMatcher.group(1) != null;
        final String variables = deleteMatcher.group(2).trim();
        final List<String> variableList = Arrays.asList(variables.split("\\s*,\\s*"));
        deleteClause = new DeleteClause(variableList, detach);
      }

      // Extract WHERE clause
      WhereClause whereClause = null;
      final Matcher whereMatcher = WHERE_PATTERN.matcher(trimmedQuery);
      if (whereMatcher.find()) {
        final String condition = whereMatcher.group(1).trim();
        whereClause = new WhereClause(condition);
      }

      // Extract RETURN clause
      ReturnClause returnClause = null;
      final Matcher returnMatcher = RETURN_PATTERN.matcher(trimmedQuery);
      if (returnMatcher.find()) {
        final String items = returnMatcher.group(1).trim();
        final List<String> itemList = Arrays.asList(items.split("\\s*,\\s*"));
        returnClause = new ReturnClause(itemList);
      }

      // Extract ORDER BY clause
      OrderByClause orderByClause = null;
      final Matcher orderByMatcher = ORDER_BY_PATTERN.matcher(trimmedQuery);
      if (orderByMatcher.find()) {
        final String orderByStr = orderByMatcher.group(1).trim();
        orderByClause = parseOrderBy(orderByStr);
      }

      // Extract SKIP clause
      Integer skip = null;
      final Matcher skipMatcher = SKIP_PATTERN.matcher(trimmedQuery);
      if (skipMatcher.find()) {
        skip = Integer.parseInt(skipMatcher.group(1));
      }

      // Extract LIMIT clause
      Integer limit = null;
      final Matcher limitMatcher = LIMIT_PATTERN.matcher(trimmedQuery);
      if (limitMatcher.find()) {
        limit = Integer.parseInt(limitMatcher.group(1));
      }

      // Check for write operations
      final boolean hasCreate = HAS_CREATE_PATTERN.matcher(trimmedQuery).find();
      final boolean hasMerge = HAS_MERGE_PATTERN.matcher(trimmedQuery).find();
      final boolean hasDelete = HAS_DELETE_PATTERN.matcher(trimmedQuery).find();

      return new SimpleCypherStatement(query, matchClauses, whereClause, returnClause, orderByClause, skip, limit,
          createClause, setClause, deleteClause, mergeClause, null, null, hasCreate, hasMerge, hasDelete);

    } catch (final Exception e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    }
  }

  /**
   * Parses ORDER BY string into OrderByClause.
   * Example: "n.name ASC, n.age DESC" or "n.name, n.age DESC"
   *
   * @param orderByStr ORDER BY string
   * @return parsed OrderByClause
   */
  private OrderByClause parseOrderBy(final String orderByStr) {
    final List<OrderByClause.OrderByItem> items = new ArrayList<>();
    final String[] parts = orderByStr.split(",");

    for (final String part : parts) {
      final String trimmed = part.trim();
      boolean ascending = true;
      String expression = trimmed;

      // Check for explicit ASC/DESC
      if (trimmed.toUpperCase().endsWith(" DESC")) {
        ascending = false;
        expression = trimmed.substring(0, trimmed.length() - 5).trim();
      } else if (trimmed.toUpperCase().endsWith(" ASC")) {
        ascending = true;
        expression = trimmed.substring(0, trimmed.length() - 4).trim();
      }

      items.add(new OrderByClause.OrderByItem(expression, ascending));
    }

    return new OrderByClause(items);
  }

  /**
   * Parses SET string into SetClause.
   * Example: "n.name = 'Alice', n.age = 30"
   *
   * @param setStr SET string
   * @return parsed SetClause
   */
  private SetClause parseSet(final String setStr) {
    final List<SetClause.SetItem> items = new ArrayList<>();
    final String[] parts = setStr.split(",");

    for (final String part : parts) {
      final String trimmed = part.trim();
      final String[] assignParts = trimmed.split("=", 2);
      if (assignParts.length == 2) {
        final String left = assignParts[0].trim();
        final String right = assignParts[1].trim();

        // Parse left side: variable.property
        if (left.contains(".")) {
          final String[] propParts = left.split("\\.", 2);
          final String variable = propParts[0].trim();
          final String property = propParts[1].trim();
          items.add(new SetClause.SetItem(variable, property, right));
        }
      }
    }

    return new SetClause(items);
  }
}
