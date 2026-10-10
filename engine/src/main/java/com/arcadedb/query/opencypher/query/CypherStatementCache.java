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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser.ParsedQuery;
import com.arcadedb.query.opencypher.parser.CypherLiteralParameterizer;

/**
 * Scan-resistant LRU cache (a burst of one-off query texts does not evict the statements hit repeatedly, issue #8286) for parsed
 * OpenCypher statements. Caches the AST (Abstract Syntax Tree) to avoid
 * expensive ANTLR parsing on every query execution.
 * <p>
 * This cache provides significant performance improvements for repeated queries by eliminating
 * the parsing overhead (5-20ms per query). Thread-safe implementation using synchronized access.
 * <p>
 * {@link #getParameterized(String)} also lets texts that differ only in their literal values share one entry, by extracting
 * those literals into generated parameters (issue #8307, see {@link CypherLiteralParameterizer}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherStatementCache {
  private final Database                   database;
  private final CypherLiteralParameterizer cache;

  /**
   * Creates a new statement cache.
   *
   * @param database the database the cached statements belong to, handed to the parser so the {@code SCOPE.DATABASE}
   *                 parse limits are read from its own configuration and {@code ALTER DATABASE} reaches them
   *                 (issue #7922)
   * @param size     maximum number of statements to cache (LRU eviction when exceeded)
   */
  public CypherStatementCache(final Database database, final int size) {
    this.database = database;
    this.cache = new CypherLiteralParameterizer(new Cypher25AntlrParser(database), size);
  }

  /**
   * Gets a parsed statement from cache, or parses it if not cached.
   *
   * @param query the OpenCypher query string
   * @return the parsed CypherStatement (either from cache or freshly parsed)
   * @throws CommandParsingException if the query is invalid
   */
  public CypherStatement get(final String query) {
    return getParsed(query).statement();
  }

  /**
   * Gets a parsed query - its AST plus the parameter names it references - from cache, or parses it if not
   * cached. Callers that need both take this in one lookup; the parameter names are cached with the AST, so
   * checking that a caller bound them all never re-scans the query text. The literals stay in the statement.
   *
   * @param query the OpenCypher query string
   * @return the parsed query (either from cache or freshly parsed)
   * @throws CommandParsingException if the query is invalid
   */
  public ParsedQuery getParsed(final String query) {
    return cache.lookupAsWritten(normalize(query)).statement();
  }

  /**
   * Gets a parsed query as written - its literals stay in the statement - in the shape {@link #getParameterized(String)}
   * returns, so a caller can take either path through the same code.
   *
   * @param query the OpenCypher query string
   * @return the parsed query, keyed by its own text, with no extracted value
   * @throws CommandParsingException if the query is invalid
   */
  public Lookup<ParsedQuery> getAsWritten(final String query) {
    return cache.lookupAsWritten(normalize(query));
  }

  /**
   * Gets a parsed query whose literals may have been extracted into generated parameters, so every text that differs only in
   * its literal values shares one cached statement (issue #8307). The caller binds {@link Lookup#parameters()} with its own
   * parameters ({@link Lookup#mergeParameters(java.util.Map)}) and keys any plan it caches on {@link Lookup#cacheKey()}.
   * Extraction is skipped when {@link GlobalConfiguration#QUERY_LITERAL_PARAMETERIZATION} is off for the database.
   *
   * @param query the OpenCypher query string
   * @return the parsed query, its cache key and the extracted values
   * @throws CommandParsingException if the query is invalid
   */
  public Lookup<ParsedQuery> getParameterized(final String query) {
    return cache.lookup(normalize(query),
        database == null || database.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION));
  }

  // Strip trailing semicolons - Neo4j clients (e.g., Neo4j Desktop) commonly append them
  private static String normalize(final String query) {
    return query.endsWith(";") ? query.substring(0, query.length() - 1).trim() : query;
  }

  /**
   * Returns whether a query is idempotent (read-only), using the cached statement.
   * This avoids creating an AnalyzedQuery wrapper object on each call. Takes the same parameterized lookup as the
   * execution that typically follows, so both share one cache entry.
   *
   * @param query the OpenCypher query string
   * @return true if the query is read-only
   * @throws CommandParsingException if the query is invalid
   */
  public boolean isIdempotent(final String query) {
    return getParameterized(query).statement().statement().isReadOnly();
  }

  /**
   * Checks if a statement is in the cache.
   *
   * @param query the query string, or the cache key of a parameterized lookup
   * @return true if the statement is cached
   */
  public boolean contains(final String query) {
    return cache.contains(query);
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
