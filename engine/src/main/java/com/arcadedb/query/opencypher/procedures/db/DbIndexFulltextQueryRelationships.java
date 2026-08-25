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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: db.index.fulltext.queryRelationships(indexName, queryString)
 * <p>
 * Searches a BM25 full-text index declared on an edge type and returns matching relationships ranked by relevance.
 * The relationship counterpart of {@link DbIndexFulltextQueryNodes}, mirroring Neo4j's
 * {@code db.index.fulltext.queryRelationships()}.
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

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
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

    final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(context.getDatabase(), indexName);

    final DocumentType type = context.getDatabase().getSchema().getType(typeIndex.getTypeName());
    if (!(type instanceof EdgeType))
      throw new CommandSQLParsingException(
          "db.index.fulltext.queryRelationships(): index '" + indexName + "' is a full-text index on node type '"
              + type.getName() + "', use db.index.fulltext.queryNodes() instead");

    final Map<RID, Float> matches = FullTextSearch.search(typeIndex, queryText, -1);

    final List<Map.Entry<RID, Float>> sorted = new ArrayList<>(matches.entrySet());
    sorted.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));

    final List<Result> results = new ArrayList<>(sorted.size());
    for (final Map.Entry<RID, Float> entry : sorted) {
      try {
        final Edge relationship = entry.getKey().asEdge(true);
        final ResultInternal r = new ResultInternal();
        r.setProperty("relationship", relationship);
        r.setProperty("score", entry.getValue());
        results.add(r);
      } catch (final RecordNotFoundException e) {
        // Stale posting: the record was deleted since the index was last updated, skip it (same handling as the
        // SEARCH_INDEX() SQL function's searchFromTarget()).
      }
    }

    return results.stream();
  }
}
