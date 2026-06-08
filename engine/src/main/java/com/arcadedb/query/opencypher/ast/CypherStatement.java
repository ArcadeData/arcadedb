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
 * Base interface for all Cypher AST statements.
 * Represents a complete Cypher query or command.
 */
public interface CypherStatement {
  /**
   * Returns true if this statement is read-only (no writes).
   *
   * @return true if read-only, false if contains write operations
   */
  boolean isReadOnly();

  /**
   * Returns the list of MATCH clauses in this statement.
   * Defaults to an empty list for non-query (control/admin/DDL/session) statements.
   *
   * @return list of MATCH clauses (may be empty)
   */
  default List<MatchClause> getMatchClauses() {
    return List.of();
  }

  /**
   * Returns the WHERE clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return WHERE clause or null
   */
  default WhereClause getWhereClause() {
    return null;
  }

  /**
   * Returns the RETURN clause.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return RETURN clause (required in most queries)
   */
  default ReturnClause getReturnClause() {
    return null;
  }

  /**
   * Returns true if this statement contains CREATE operations.
   * Defaults to false for non-query (control/admin/DDL/session) statements.
   *
   * @return true if contains CREATE
   */
  default boolean hasCreate() {
    return false;
  }

  /**
   * Returns true if this statement contains MERGE operations.
   * Defaults to false for non-query (control/admin/DDL/session) statements.
   *
   * @return true if contains MERGE
   */
  default boolean hasMerge() {
    return false;
  }

  /**
   * Returns true if this statement contains DELETE operations.
   * Defaults to false for non-query (control/admin/DDL/session) statements.
   *
   * @return true if contains DELETE
   */
  default boolean hasDelete() {
    return false;
  }

  /**
   * Returns the ORDER BY clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return ORDER BY clause or null
   */
  default OrderByClause getOrderByClause() {
    return null;
  }

  /**
   * Returns the SKIP value if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return SKIP value or null
   */
  default Expression getSkip() {
    return null;
  }

  /**
   * Returns the LIMIT value if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return LIMIT value or null
   */
  default Expression getLimit() {
    return null;
  }

  /**
   * Returns the CREATE clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return CREATE clause or null
   */
  default CreateClause getCreateClause() {
    return null;
  }

  /**
   * Returns the SET clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return SET clause or null
   */
  default SetClause getSetClause() {
    return null;
  }

  /**
   * Returns the DELETE clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return DELETE clause or null
   */
  default DeleteClause getDeleteClause() {
    return null;
  }

  /**
   * Returns the MERGE clause if present.
   * Defaults to null for non-query (control/admin/DDL/session) statements.
   *
   * @return MERGE clause or null
   */
  default MergeClause getMergeClause() {
    return null;
  }

  /**
   * Returns the UNWIND clauses if present.
   * Defaults to an empty list for non-query (control/admin/DDL/session) statements.
   *
   * @return list of UNWIND clauses (may be empty)
   */
  default List<UnwindClause> getUnwindClauses() {
    return List.of();
  }

  /**
   * Returns the WITH clauses if present.
   * WITH clauses enable query chaining by projecting and transforming results.
   * Defaults to an empty list for non-query (control/admin/DDL/session) statements.
   *
   * @return list of WITH clauses (may be empty)
   */
  default List<WithClause> getWithClauses() {
    return List.of();
  }

  /**
   * Returns all clauses in the order they appear in the query.
   * This is essential for queries like UNWIND...MATCH where clause order matters.
   *
   * @return list of clause entries in order (may be empty)
   */
  default List<ClauseEntry> getClausesInOrder() {
    return List.of();
  }

  /**
   * Returns the CALL clauses if present.
   * CALL clauses invoke procedures/functions.
   *
   * @return list of CALL clauses (may be empty)
   */
  default List<CallClause> getCallClauses() {
    return List.of();
  }

  /**
   * Returns the REMOVE clauses if present.
   * REMOVE clauses remove properties from nodes/edges or labels from nodes.
   *
   * @return list of REMOVE clauses (may be empty)
   */
  default List<RemoveClause> getRemoveClauses() {
    return List.of();
  }

  /**
   * Returns true if any MATCH clause contains a variable-length path pattern.
   * Pre-computed at parse time to avoid scanning on every execution.
   */
  default boolean hasVariableLengthPath() {
    return false;
  }

  /**
   * Returns true if UNWIND appears before MATCH in clause order.
   * Pre-computed at parse time to avoid scanning on every execution.
   */
  default boolean hasUnwindBeforeMatch() {
    return false;
  }

  /**
   * Returns true if WITH appears before MATCH in clause order.
   * Pre-computed at parse time to avoid scanning on every execution.
   */
  default boolean hasWithBeforeMatch() {
    return false;
  }

  /**
   * Returns true if the query contains a CALL subquery clause.
   * Pre-computed at parse time to avoid scanning on every execution.
   */
  default boolean hasSubquery() {
    return false;
  }

  /**
   * Returns true if any write clause (CREATE, MERGE, SET, DELETE, REMOVE, FOREACH)
   * appears before a MATCH in clause order. Used to disable the optimizer fast path
   * for those queries, because that path executes MATCH first via the physical plan
   * and chains writes afterwards, breaking same-statement write-then-read visibility.
   * Pre-computed at parse time to avoid scanning on every execution.
   */
  default boolean hasWriteBeforeMatch() {
    return false;
  }

  /**
   * Returns true when the query terminates with a GQL FINISH clause (issue #3365 section 1.3).
   * A query with FINISH never returns rows, even when MATCH would otherwise produce results.
   */
  default boolean hasFinishClause() {
    return false;
  }

  /**
   * Returns true when this is a server/session control statement (transaction control such as
   * {@code START TRANSACTION}/{@code COMMIT}/{@code ROLLBACK}, or session management such as
   * {@code SESSION SET}/{@code RESET}/{@code CLOSE}).
   * <p>
   * These statements sit on two deliberately decoupled axes, which this single predicate names so the
   * split no longer needs parallel comments at each call site (issue #4505):
   * <ul>
   *   <li>{@link #isReadOnly()} returns {@code false}, so they are <em>not</em> idempotent: HA routes them
   *       to the leader and Bolt sends them through {@code command()} (the only path that dispatches them).</li>
   *   <li>They read and write no data of their own, so the MCP permission axis
   *       ({@code OpenCypherQueryEngine.getOperationTypes()}) gates them as least-privilege
   *       {@code OperationType.READ}; the work they wrap is gated separately by its own operation types.</li>
   * </ul>
   *
   * @return true for transaction-control / session-management statements, false otherwise
   */
  default boolean isServerControlStatement() {
    return false;
  }
}
