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

import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.db.VectorSearchClauseProcedure;

import java.util.ArrayList;
import java.util.List;

/**
 * The Cypher 25 {@code SEARCH} subclause of a {@code MATCH} (issue #10776), run as a {@code CALL} placed in front of the
 * {@code MATCH} it belongs to:
 * <pre>
 * MATCH (d:Doc) SEARCH d IN (VECTOR INDEX idx FOR $q WHERE d.lang = 'en' LIMIT 10) SCORE AS s
 * </pre>
 * runs as
 * <pre>
 * CALL &lt;vector search of idx for $q, k = 10, filtered by d.lang = 'en'&gt; YIELD node AS d, score AS s
 * MATCH (d:Doc)
 * </pre>
 * so the {@code MATCH} reads {@code d} already bound to one of the k nearest records and keeps the rows its pattern and
 * {@code WHERE} still match, which is how Neo4j applies them: after the search. The query vector and k are evaluated per
 * input row, so they can read the variables bound by the clauses before the {@code MATCH}.
 * <p>
 * The clause carries its own {@link VectorSearchClauseProcedure}, which holds the {@code WHERE} of the search: it is not
 * in the procedure registry, so no {@code CALL} by name reaches it.
 */
public class SearchCallClause extends CallClause {
  private final VectorSearchClauseProcedure procedure;
  private final String                      variable;
  private final String                      scoreVariable;
  private final Expression                  indexName;
  private final Expression                  queryVector;
  private final Expression                  limit;

  public SearchCallClause(final String variable, final Expression indexName, final Expression queryVector,
      final WhereClause where, final Expression limit, final String scoreVariable) {
    super(VectorSearchClauseProcedure.NAME, List.of(indexName, queryVector, limit), yieldItems(variable, scoreVariable), null,
        false);
    this.procedure = new VectorSearchClauseProcedure(variable, where != null ? where.getConditionExpression() : null);
    this.variable = variable;
    this.scoreVariable = scoreVariable;
    this.indexName = indexName;
    this.queryVector = queryVector;
    this.limit = limit;
  }

  private static List<YieldItem> yieldItems(final String variable, final String scoreVariable) {
    final List<YieldItem> items = new ArrayList<>(2);
    items.add(new YieldItem("node", variable));
    if (scoreVariable != null)
      items.add(new YieldItem("score", scoreVariable));
    return items;
  }

  @Override
  public CypherProcedure resolveProcedure() {
    return procedure;
  }

  public String getVariable() {
    return variable;
  }

  public String getScoreVariable() {
    return scoreVariable;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SEARCH ").append(variable).append(" IN (VECTOR INDEX ")
        .append(indexName.getText()).append(" FOR ").append(queryVector.getText());
    if (procedure.getFilter() != null)
      sb.append(" WHERE ").append(procedure.getFilter().getText());
    sb.append(" LIMIT ").append(limit.getText()).append(')');
    if (scoreVariable != null)
      sb.append(" SCORE AS ").append(scoreVariable);
    return sb.toString();
  }
}
