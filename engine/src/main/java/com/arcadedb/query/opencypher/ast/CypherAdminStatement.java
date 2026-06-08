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

/**
 * AST node for Cypher admin statements (user management commands).
 * These bypass the normal query execution pipeline and are executed directly against the security manager.
 */
public class CypherAdminStatement implements CypherStatement {

  public enum Kind {
    SHOW_USERS, SHOW_CURRENT_USER, CREATE_USER, DROP_USER, ALTER_USER
  }

  private final Kind kind;
  private final String userName;
  private final String password;
  private final boolean ifNotExists;
  private final boolean ifExists;

  public CypherAdminStatement(final Kind kind, final String userName, final String password,
      final boolean ifNotExists, final boolean ifExists) {
    this.kind = kind;
    this.userName = userName;
    this.password = password;
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
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

  @Override
  public boolean isReadOnly() {
    return false;
  }

  // All structural query accessors (getMatchClauses, getReturnClause, hasCreate, ...) inherit the
  // empty/neutral defaults from CypherStatement: an admin (user management) statement carries no clauses.
}
