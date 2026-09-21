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
 * Neo4j-compatible procedure: db.index.fulltext.queryRelationships(indexName, queryString, options = {})
 * <p>
 * Searches a BM25 full-text index declared on an edge type and returns matching relationships ranked by relevance.
 * The relationship counterpart of {@link DbIndexFulltextQueryNodes}, mirroring Neo4j's
 * {@code db.index.fulltext.queryRelationships()}.
 * </p>
 * <p>
 * The trailing {@code options} map is optional and carries {@code skip} and {@code limit}, exactly as on
 * {@link DbIndexFulltextQueryNodes}; see {@link FullTextQueryOptions} (issue #8103).
 * </p>
 * <p>
 * Example (Neo4j-compatible):
 * <pre>
 * CALL db.index.fulltext.queryRelationships('Cites[note]', 'java')
 * YIELD relationship, score
 * RETURN relationship.note AS note, score
 * ORDER BY score DESC
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see DbIndexFulltextQueryNodes
 */
public class DbIndexFulltextQueryRelationships implements CypherProcedure {
  public static final String NAME = "db.index.fulltext.queryrelationships";
  /** {@link #NAME} is lower-cased for registry lookup; error messages quote the call the way the caller wrote it. */
  private static final String DISPLAY_NAME = "db.index.fulltext.queryRelationships";

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
    return "Searches a full-text index and returns matching relationships ranked by BM25 relevance score (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("relationship", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String indexName = args[0].toString();
    final String queryText = args[1].toString();
    final FullTextQueryOptions options = FullTextQueryOptions.parse(DISPLAY_NAME, args);

    final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(context.getDatabase(), indexName);

    final DocumentType type = context.getDatabase().getSchema().getType(typeIndex.getTypeName());
    if (!(type instanceof EdgeType))
      throw new CommandSQLParsingException(
          DISPLAY_NAME + "(): index '" + indexName + "' is a full-text index on node type '"
              + type.getName() + "', use db.index.fulltext.queryNodes() instead");

    // Asked for no rows: the index name and the query string have still been validated above, but there is nothing
    // left for the search itself to contribute.
    if (options.returnsNothing())
      return Stream.empty();

    final Map<RID, Float> matches = FullTextSearch.search(typeIndex, queryText, options.searchLimit());

    return options.page(matches, "relationship", rid -> rid.asEdge(true)).stream();
  }
}
