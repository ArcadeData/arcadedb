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
 * Represents a FOREACH clause in a Cypher query.
 * FOREACH iterates over a list and executes write clauses for each element.
 * The FOREACH clause does not produce new output rows; it passes through input rows unchanged
 * while performing side effects (CREATE, SET, DELETE, MERGE).
 * <p>
 * Syntax: FOREACH (variable IN expression | clause+)
 * <p>
 * Example: FOREACH (i IN [1, 2, 3] | CREATE (:Node {id: i}))
 */
public class ForeachClause {
  private final String variable;
  private final Expression listExpression;
  private final List<ClauseEntry> innerClauses;

  public ForeachClause(final String variable, final Expression listExpression, final List<ClauseEntry> innerClauses) {
    this.variable = variable;
    this.listExpression = listExpression;
    this.innerClauses = innerClauses;
  }

  public String getVariable() {
    return variable;
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public List<ClauseEntry> getInnerClauses() {
    return innerClauses;
  }

  /**
   * True when this FOREACH's body contains a DELETE clause, directly or nested inside another
   * FOREACH. DELETE is the only inner clause type that physically removes a record - CREATE/SET/MERGE/
   * REMOVE mutate or add, never remove - so it is the only one that can make a later row of the outer
   * MATCH feeding this FOREACH dereference an already-deleted entity (issue #6491, same hazard as a
   * bare {@code DETACH DELETE} following a disconnected-pattern MATCH, just running through FOREACH's
   * own row-by-row loop instead of {@code DeleteStep}'s).
   *
   * @return true when a DELETE clause appears anywhere in this FOREACH's body
   */
  public boolean containsDelete() {
    for (final ClauseEntry entry : innerClauses) {
      if (entry.getType() == ClauseEntry.ClauseType.DELETE)
        return true;
      if (entry.getType() == ClauseEntry.ClauseType.FOREACH) {
        final ForeachClause nested = entry.getTypedClause();
        if (nested.containsDelete())
          return true;
      }
    }
    return false;
  }
}
