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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.List;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: {@code db.index.vector.queryRelationships(indexName, k, vector[, options])}
 * <p>
 * Performs an approximate nearest neighbor search with a vector index over a relationship type and yields the
 * relationships it finds, nearest first, each with its similarity score.
 * <p>
 * The index is named as {@code 'Type[property]'} (for example {@code 'RELATES_TO[fact_embedding]'}) or by its schema name.
 * <p>
 * The search is the one {@link DbIndexVectorQueryNodes} runs: the same index resolution, the same {@code where} and
 * {@code filter} pre-filters (applied by the index while it walks, not after its top k) and the same score, so the two
 * procedures agree on a score and a {@code min_score} means the same thing to either. Only what is yielded differs - a
 * relationship rather than a node - and an index over a vertex type is refused, the way
 * {@link DbIndexFulltextQueryRelationships} refuses a full-text index over one.
 */
public class DbIndexVectorQueryRelationships implements CypherProcedure {
  public static final String NAME = "db.index.vector.queryrelationships";
  /** {@link #NAME} is lower-cased for registry lookup; error messages quote the call the way the caller wrote it. */
  private static final String DISPLAY_NAME = "db.index.vector.queryRelationships";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Performs approximate nearest neighbor vector search using an HNSW index over a relationship type "
        + "(Neo4j-compatible), optionally pre-filtered by an options map {where: <map or condition>, filter: [<record or rid>, ...]}";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("relationship", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String indexName = args[0].toString();

    // An index over a vertex type is refused before the search runs: the caller would otherwise be handed nodes where it
    // asked for relationships
    for (final LSMVectorIndex index : DbIndexVectorQueryNodes.resolveVectorIndexes(indexName, context)) {
      final DocumentType type = context.getDatabase().getSchema().getType(index.getTypeName());
      if (!(type instanceof EdgeType))
        throw new CommandSQLParsingException(
            DISPLAY_NAME + "(): index '" + indexName + "' is a vector index on node type '" + type.getName()
                + "', use db.index.vector.queryNodes() instead");
    }

    return DbIndexVectorQueryNodes.search(indexName, args[1], args[2], args.length > 3 ? args[3] : null, null, context,
        DISPLAY_NAME + "()").stream().map(DbIndexVectorQueryRelationships::asRelationshipRow);
  }

  /** The row the search answered, with its record yielded as a relationship. */
  private static Result asRelationshipRow(final Result row) {
    final ResultInternal out = new ResultInternal();
    out.setProperty("relationship", ((Identifiable) row.getProperty("node")).getIdentity().asEdge(true));
    out.setProperty("score", row.getProperty("score"));
    return out;
  }
}
