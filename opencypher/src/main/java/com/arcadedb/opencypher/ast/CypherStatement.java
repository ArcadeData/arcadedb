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
package com.arcadedb.opencypher.ast;

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
   *
   * @return list of MATCH clauses (may be empty)
   */
  List<MatchClause> getMatchClauses();

  /**
   * Returns the WHERE clause if present.
   *
   * @return WHERE clause or null
   */
  WhereClause getWhereClause();

  /**
   * Returns the RETURN clause.
   *
   * @return RETURN clause (required in most queries)
   */
  ReturnClause getReturnClause();

  /**
   * Returns true if this statement contains CREATE operations.
   *
   * @return true if contains CREATE
   */
  boolean hasCreate();

  /**
   * Returns true if this statement contains MERGE operations.
   *
   * @return true if contains MERGE
   */
  boolean hasMerge();

  /**
   * Returns true if this statement contains DELETE operations.
   *
   * @return true if contains DELETE
   */
  boolean hasDelete();

  /**
   * Returns the ORDER BY clause if present.
   *
   * @return ORDER BY clause or null
   */
  OrderByClause getOrderByClause();

  /**
   * Returns the SKIP value if present.
   *
   * @return SKIP value or null
   */
  Integer getSkip();

  /**
   * Returns the LIMIT value if present.
   *
   * @return LIMIT value or null
   */
  Integer getLimit();

  /**
   * Returns the CREATE clause if present.
   *
   * @return CREATE clause or null
   */
  CreateClause getCreateClause();

  /**
   * Returns the SET clause if present.
   *
   * @return SET clause or null
   */
  SetClause getSetClause();

  /**
   * Returns the DELETE clause if present.
   *
   * @return DELETE clause or null
   */
  DeleteClause getDeleteClause();

  /**
   * Returns the MERGE clause if present.
   *
   * @return MERGE clause or null
   */
  MergeClause getMergeClause();

  /**
   * Returns the UNWIND clause if present.
   *
   * @return UNWIND clause or null
   */
  UnwindClause getUnwindClause();
}
