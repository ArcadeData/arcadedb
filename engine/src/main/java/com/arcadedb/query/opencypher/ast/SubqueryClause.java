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

import java.util.List;

/**
 * AST node representing a CALL subquery clause.
 * <p>
 * Cypher syntax:
 * <pre>
 * CALL { WITH x RETURN x * 10 AS y }
 * CALL (x) { RETURN x * 10 AS y }
 * OPTIONAL CALL { ... }
 * CALL { WITH row CREATE (n:Person {name: row.name}) } IN TRANSACTIONS OF 1000 ROWS
 * </pre>
 * <p>
 * A CALL subquery executes an inner query for each input row.
 * Variables are imported from the outer scope via the initial WITH clause
 * inside the subquery body. The inner RETURN clause makes computed values
 * available to the outer query.
 * <p>
 * When IN TRANSACTIONS is specified, the subquery commits in batches
 * rather than accumulating all changes in a single transaction.
 */
public class SubqueryClause {
  private static final int DEFAULT_BATCH_SIZE = 1000;

  private final CypherStatement innerStatement;
  private final List<String> scopeVariables; // for CALL (x, y) { ... } syntax, null if not specified
  private final boolean optional;
  private final boolean inTransactions;
  private final Expression batchSize; // null = default (1000)

  public SubqueryClause(final CypherStatement innerStatement, final List<String> scopeVariables,
                         final boolean optional) {
    this(innerStatement, scopeVariables, optional, false, null);
  }

  public SubqueryClause(final CypherStatement innerStatement, final List<String> scopeVariables,
                         final boolean optional, final boolean inTransactions, final Expression batchSize) {
    this.innerStatement = innerStatement;
    this.scopeVariables = scopeVariables;
    this.optional = optional;
    this.inTransactions = inTransactions;
    this.batchSize = batchSize;
  }

  public CypherStatement getInnerStatement() {
    return innerStatement;
  }

  public List<String> getScopeVariables() {
    return scopeVariables;
  }

  public boolean isOptional() {
    return optional;
  }

  public boolean isInTransactions() {
    return inTransactions;
  }

  public Expression getBatchSize() {
    return batchSize;
  }

  public int getDefaultBatchSize() {
    return DEFAULT_BATCH_SIZE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (optional)
      sb.append("OPTIONAL ");
    sb.append("CALL ");
    if (scopeVariables != null) {
      sb.append("(");
      sb.append(String.join(", ", scopeVariables));
      sb.append(") ");
    }
    sb.append("{ ... }");
    if (inTransactions) {
      sb.append(" IN TRANSACTIONS");
      if (batchSize != null)
        sb.append(" OF ").append(batchSize.getText()).append(" ROWS");
    }
    return sb.toString();
  }
}
