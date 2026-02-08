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

import java.util.ArrayList;
import java.util.List;

/**
 * AST node representing a UNION query that combines multiple subqueries.
 * <p>
 * Cypher UNION syntax:
 * - UNION: Combines results and removes duplicates
 * - UNION ALL: Combines results without removing duplicates
 * - UNION DISTINCT: Same as UNION (explicit duplicate removal)
 * <p>
 * Example:
 * <pre>
 * MATCH (n:Person) RETURN n.name AS name
 * UNION
 * MATCH (n:Company) RETURN n.name AS name
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UnionStatement implements CypherStatement {
  private final List<CypherStatement> queries;
  private final List<Boolean>         unionAllFlags; // true = UNION ALL, false = UNION (DISTINCT)

  /**
   * Creates a new UnionStatement.
   *
   * @param queries       the list of queries to combine (must have at least 2)
   * @param unionAllFlags flags indicating whether each union is ALL (no dedup) or not
   *                      The list has size = queries.size() - 1
   *                      unionAllFlags[i] corresponds to the union between queries[i] and queries[i+1]
   */
  public UnionStatement(final List<CypherStatement> queries, final List<Boolean> unionAllFlags) {
    if (queries == null || queries.size() < 2)
      throw new IllegalArgumentException("UNION requires at least 2 queries");
    if (unionAllFlags == null || unionAllFlags.size() != queries.size() - 1)
      throw new IllegalArgumentException("unionAllFlags must have size = queries.size() - 1");

    this.queries = new ArrayList<>(queries);
    this.unionAllFlags = new ArrayList<>(unionAllFlags);
  }

  /**
   * Returns all queries in the UNION.
   */
  public List<CypherStatement> getQueries() {
    return queries;
  }

  /**
   * Returns the UNION ALL flags.
   * unionAllFlags[i] is true if the union between queries[i] and queries[i+1] is UNION ALL.
   */
  public List<Boolean> getUnionAllFlags() {
    return unionAllFlags;
  }

  /**
   * Returns true if all unions are UNION ALL (no deduplication needed).
   */
  public boolean isAllUnionAll() {
    for (final Boolean flag : unionAllFlags)
      if (!flag)
        return false;
    return true;
  }

  /**
   * Returns the first query (used for determining column structure).
   */
  public CypherStatement getFirstQuery() {
    return queries.get(0);
  }

  // Delegate CypherStatement methods to the first query
  // UNION inherits read-only status from all subqueries

  @Override
  public boolean isReadOnly() {
    for (final CypherStatement query : queries)
      if (!query.isReadOnly())
        return false;
    return true;
  }

  @Override
  public List<MatchClause> getMatchClauses() {
    return queries.getFirst().getMatchClauses();
  }

  @Override
  public WhereClause getWhereClause() {
    return queries.getFirst().getWhereClause();
  }

  @Override
  public ReturnClause getReturnClause() {
    return queries.getFirst().getReturnClause();
  }

  @Override
  public boolean hasCreate() {
    for (final CypherStatement query : queries)
      if (query.hasCreate())
        return true;
    return false;
  }

  @Override
  public boolean hasMerge() {
    for (final CypherStatement query : queries)
      if (query.hasMerge())
        return true;
    return false;
  }

  @Override
  public boolean hasDelete() {
    for (final CypherStatement query : queries)
      if (query.hasDelete())
        return true;
    return false;
  }

  @Override
  public OrderByClause getOrderByClause() {
    // For UNION, ORDER BY applies to the final result
    // Currently we don't support ORDER BY after UNION in the grammar
    return null;
  }

  @Override
  public Expression getSkip() {
    return null;
  }

  @Override
  public Expression getLimit() {
    return null;
  }

  @Override
  public CreateClause getCreateClause() {
    return queries.getFirst().getCreateClause();
  }

  @Override
  public SetClause getSetClause() {
    return queries.getFirst().getSetClause();
  }

  @Override
  public DeleteClause getDeleteClause() {
    return queries.getFirst().getDeleteClause();
  }

  @Override
  public MergeClause getMergeClause() {
    return queries.getFirst().getMergeClause();
  }

  @Override
  public List<UnwindClause> getUnwindClauses() {
    return queries.getFirst().getUnwindClauses();
  }

  @Override
  public List<WithClause> getWithClauses() {
    return queries.getFirst().getWithClauses();
  }

  @Override
  public List<ClauseEntry> getClausesInOrder() {
    return queries.getFirst().getClausesInOrder();
  }
}
