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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.utility.LRUCache;

import java.util.Collections;
import java.util.Map;

/**
 * LRU cache for parsed OpenCypher statements. Caches the AST (Abstract Syntax Tree) to avoid
 * expensive ANTLR parsing on every query execution.
 * <p>
 * This cache provides significant performance improvements for repeated queries by eliminating
 * the parsing overhead (5-20ms per query). Thread-safe implementation using synchronized access.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherStatementCache {
  private final Map<String, CypherStatement> cache;
  private final Cypher25AntlrParser          parser;

  /**
   * Creates a new statement cache.
   *
   * @param size     maximum number of statements to cache (LRU eviction when exceeded)
   */
  public CypherStatementCache(final int size) {
    this.parser = new Cypher25AntlrParser();
    // Use LRUCache wrapped in synchronizedMap for thread-safety
    this.cache = Collections.synchronizedMap(new LRUCache<>(size));
  }

  /**
   * Gets a parsed statement from cache, or parses it if not cached.
   *
   * @param query the OpenCypher query string
   * @return the parsed CypherStatement (either from cache or freshly parsed)
   * @throws CommandParsingException if the query is invalid
   */
  public CypherStatement get(final String query) {
    // Strip trailing semicolons - Neo4j clients (e.g., Neo4j Desktop) commonly append them
    final String normalizedQuery = query.endsWith(";") ? query.substring(0, query.length() - 1).trim() : query;

    CypherStatement statement = cache.get(normalizedQuery);
    if (statement == null) {
      statement = parse(normalizedQuery);
      cache.put(normalizedQuery, statement);
    }
    return statement;
  }

  /**
   * Parses an OpenCypher query using ANTLR4 parser.
   *
   * @param query the OpenCypher query string
   * @return the parsed CypherStatement AST
   * @throws CommandParsingException if parsing fails
   */
  private CypherStatement parse(final String query) throws CommandParsingException {
    try {
      return parser.parse(query);
    } catch (final Exception e) {
      throw new CommandParsingException("Error parsing OpenCypher query: " + query, e);
    }
  }

  /**
   * Checks if a statement is in the cache.
   *
   * @param query the query string
   * @return true if the statement is cached
   */
  public boolean contains(final String query) {
    return cache.containsKey(query);
  }

  /**
   * Clears all cached statements.
   * Should be called when schema changes invalidate cached ASTs.
   */
  public void clear() {
    cache.clear();
  }

  /**
   * Returns the current cache size.
   *
   * @return number of cached statements
   */
  public int size() {
    return cache.size();
  }
}
