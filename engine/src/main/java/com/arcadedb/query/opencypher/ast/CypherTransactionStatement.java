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
 * AST node for ISO/IEC 39075 (GQL) transaction control statements: {@code START TRANSACTION},
 * {@code COMMIT} and {@code ROLLBACK}. These bypass the normal query execution pipeline and are
 * executed directly against the database transaction API (mirroring the SQL BEGIN/COMMIT/ROLLBACK
 * statements). Issue #4141 section 2.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherTransactionStatement implements CypherStatement {

  public enum Kind {
    BEGIN, COMMIT, ROLLBACK
  }

  private final Kind   kind;
  private final String isolationLevel;

  public CypherTransactionStatement(final Kind kind) {
    this(kind, null);
  }

  public CypherTransactionStatement(final Kind kind, final String isolationLevel) {
    this.kind = kind;
    this.isolationLevel = isolationLevel;
  }

  public Kind getKind() {
    return kind;
  }

  /**
   * Returns the requested transaction isolation level for {@code START TRANSACTION ISOLATION <level>}
   * (ArcadeDB extension), or {@code null} when none was specified (engine default applies).
   */
  public String getIsolationLevel() {
    return isolationLevel;
  }

  @Override
  public boolean isReadOnly() {
    // Intentionally false: transaction control is not idempotent (a repeated START TRANSACTION nests) and
    // must establish its write context on the leader, so HA must not route it to a follower. The decoupled
    // READ permission gating lives on the other axis - see isServerControlStatement().
    return false;
  }

  @Override
  public boolean isServerControlStatement() {
    return true;
  }

  // All structural query accessors (getMatchClauses, getReturnClause, hasCreate, ...) inherit the
  // empty/neutral defaults from CypherStatement: a transaction control statement carries no clauses.
}
