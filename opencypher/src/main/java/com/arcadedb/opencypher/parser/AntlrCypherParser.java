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
package com.arcadedb.opencypher.parser;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.opencypher.ast.*;

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
  private static final Pattern MATCH_PATTERN = Pattern.compile("MATCH\\s+(.+?)(?:WHERE|RETURN|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern WHERE_PATTERN = Pattern.compile("WHERE\\s+(.+?)(?:RETURN|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern RETURN_PATTERN = Pattern.compile("RETURN\\s+(.+?)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern CREATE_PATTERN = Pattern.compile("\\bCREATE\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern MERGE_PATTERN = Pattern.compile("\\bMERGE\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern DELETE_PATTERN = Pattern.compile("\\bDELETE\\b", Pattern.CASE_INSENSITIVE);

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
        matchClauses.add(new MatchClause(pattern, false));
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

      // Check for write operations
      final boolean hasCreate = CREATE_PATTERN.matcher(trimmedQuery).find();
      final boolean hasMerge = MERGE_PATTERN.matcher(trimmedQuery).find();
      final boolean hasDelete = DELETE_PATTERN.matcher(trimmedQuery).find();

      return new SimpleCypherStatement(query, matchClauses, whereClause, returnClause, hasCreate, hasMerge, hasDelete);

    } catch (final Exception e) {
      throw new CommandParsingException("Failed to parse Cypher query: " + query, e);
    }
  }
}
