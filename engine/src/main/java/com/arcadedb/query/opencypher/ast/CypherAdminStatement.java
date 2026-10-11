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
 * AST node for Cypher admin statements: user management, executed directly against the security manager, and
 * {@code SHOW TRANSACTIONS} / {@code TERMINATE TRANSACTIONS} (issue #9689), executed against the server's registry of
 * running statements. Both bypass the normal query execution pipeline.
 */
public class CypherAdminStatement implements CypherStatement {

  public enum Kind {
    SHOW_USERS, SHOW_CURRENT_USER, CREATE_USER, DROP_USER, ALTER_USER, SHOW_TRANSACTIONS, TERMINATE_TRANSACTIONS
  }

  private final Kind kind;
  private final String userName;
  private final String password;
  private final boolean ifNotExists;
  private final boolean ifExists;
  // SHOW/TERMINATE TRANSACTIONS: the ids named as string literals, or the expression that yields them (a parameter, a
  // list, a column of the SHOW a TERMINATE is composed with); neither for a SHOW of every transaction
  private final List<String>         transactionIds;
  private final Expression           transactionIdsExpression;
  // SHOW TRANSACTIONS ... TERMINATE TRANSACTIONS ...: the TERMINATE that consumes the rows of the SHOW
  private final CypherAdminStatement composedTerminate;

  public CypherAdminStatement(final Kind kind, final String userName, final String password,
      final boolean ifNotExists, final boolean ifExists) {
    this.kind = kind;
    this.userName = userName;
    this.password = password;
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
    this.transactionIds = null;
    this.transactionIdsExpression = null;
    this.composedTerminate = null;
  }

  /** {@code SHOW TRANSACTIONS} or {@code TERMINATE TRANSACTIONS} (issue #9689). */
  public CypherAdminStatement(final Kind kind, final List<String> transactionIds, final Expression transactionIdsExpression,
      final CypherAdminStatement composedTerminate) {
    this.kind = kind;
    this.userName = null;
    this.password = null;
    this.ifNotExists = false;
    this.ifExists = false;
    this.transactionIds = transactionIds;
    this.transactionIdsExpression = transactionIdsExpression;
    this.composedTerminate = composedTerminate;
  }

  public Kind getKind() {
    return kind;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public List<String> getTransactionIds() {
    return transactionIds;
  }

  public Expression getTransactionIdsExpression() {
    return transactionIdsExpression;
  }

  public CypherAdminStatement getComposedTerminate() {
    return composedTerminate;
  }

  public boolean isTransactionsCommand() {
    return kind == Kind.SHOW_TRANSACTIONS || kind == Kind.TERMINATE_TRANSACTIONS;
  }

  /**
   * True for {@code SHOW/TERMINATE TRANSACTIONS}: they read and write no data, and each server lists and stops its own
   * statements, so an HA replica runs them itself rather than forwarding them to the leader - the same rule the HTTP
   * {@code list queries} / {@code terminate query} commands follow.
   */
  @Override
  public boolean isReadOnly() {
    return isTransactionsCommand();
  }

  // All structural query accessors (getMatchClauses, getReturnClause, hasCreate, ...) inherit the
  // empty/neutral defaults from CypherStatement: an admin (user management) statement carries no clauses.
}
