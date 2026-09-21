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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: db.index.fulltext.queryNodes(indexName, queryString, options = {})
 * <p>
 * Searches a BM25 full-text index (ArcadeDB's native {@code FULL_TEXT} index type) and returns matching nodes ranked
 * by relevance. This provides compatibility with Neo4j's full-text search API, letting text search and graph pattern
 * matching live in the same Cypher statement instead of requiring a separate SQL {@code SEARCH_INDEX()} query.
 * </p>
 * <p>
 * The index is identified in ArcadeDB's {@code Type[property]} format, exactly as returned by the schema and as
 * accepted by the SQL {@code SEARCH_INDEX()} function - there is no separate "index name" concept to reconcile.
 * </p>
 * <p>
 * The trailing {@code options} map is optional and carries {@code skip} and {@code limit}; see
 * {@link FullTextQueryOptions} for the keys, and for why the ones ArcadeDB cannot honour are refused by name
 * instead of ignored (issue #8103).
 * </p>
 * <p>
 * Example (Neo4j-compatible):
 * <pre>
 * CALL db.index.fulltext.queryNodes('Article[content]', 'java AND database')
 * YIELD node, score
 * RETURN node.title AS title, score
 * ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see DbIndexFulltextQueryRelationships
 */
public class DbIndexFulltextQueryNodes implements CypherProcedure {
  public static final String NAME = "db.index.fulltext.querynodes";
  /** {@link #NAME} is lower-cased for registry lookup; error messages quote the call the way the caller wrote it. */
  private static final String DISPLAY_NAME = "db.index.fulltext.queryNodes";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  /**
   * Three, not two: Neo4j declares {@code options = {}} as a third parameter and ArcadeDB now implements it, so the
   * full-arity call is one this procedure accepts (issue #8103). It stays optional - {@link #getMinArgs()} is
   * unchanged - and an omitted map means no skip and no limit, exactly what the two-argument form has always done.
   */
  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Searches a full-text index and returns matching nodes ranked by BM25 relevance score (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String indexName = args[0].toString();
    final String queryText = args[1].toString();
    final FullTextQueryOptions options = FullTextQueryOptions.parse(DISPLAY_NAME, args);

    final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(context.getDatabase(), indexName);

    final DocumentType type = context.getDatabase().getSchema().getType(typeIndex.getTypeName());
    if (type instanceof EdgeType)
      throw new CommandSQLParsingException(
          DISPLAY_NAME + "(): index '" + indexName + "' is a full-text index on relationship type '"
              + type.getName() + "', use db.index.fulltext.queryRelationships() instead");

    // Asked for no rows: the index name and the query string have still been validated above, but there is nothing
    // left for the search itself to contribute.
    if (options.returnsNothing())
      return Stream.empty();

    final Map<RID, Float> matches = FullTextSearch.search(typeIndex, queryText, options.searchLimit());

    return options.page(matches, "node", rid -> rid.asDocument(true)).stream();
  }
}
