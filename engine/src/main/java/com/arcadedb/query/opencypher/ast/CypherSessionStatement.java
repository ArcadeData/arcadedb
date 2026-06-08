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
 * AST node for ISO/IEC 39075 (GQL) Session Management statements: {@code SESSION SET $name = value},
 * {@code SESSION RESET} and {@code SESSION CLOSE}.
 * <p>
 * These operate on the {@link com.arcadedb.query.QuerySession} bound to the current thread (a server
 * session); they are executed directly, bypassing the planner pipeline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherSessionStatement implements CypherStatement {

  public enum Kind {
    SET, RESET, CLOSE
  }

  private final Kind       kind;
  private final String     parameterName;
  private final Expression valueExpression;

  public CypherSessionStatement(final Kind kind) {
    this(kind, null, null);
  }

  public CypherSessionStatement(final Kind kind, final String parameterName, final Expression valueExpression) {
    this.kind = kind;
    this.parameterName = parameterName;
    this.valueExpression = valueExpression;
  }

  public Kind getKind() {
    return kind;
  }

  /** The session-parameter name for {@code SESSION SET} (without the leading {@code $}); null otherwise. */
  public String getParameterName() {
    return parameterName;
  }

  /** The value expression for {@code SESSION SET}; null otherwise. */
  public Expression getValueExpression() {
    return valueExpression;
  }

  /**
   * Returns {@code false}: SESSION statements are not idempotent, must go through {@code command()} (the only
   * path that dispatches them) and must not be routed to a follower. The decoupled READ permission gating
   * lives on the other axis - see {@link #isServerControlStatement()}.
   */
  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public boolean isServerControlStatement() {
    return true;
  }

  // All structural query accessors (getMatchClauses, getReturnClause, hasCreate, ...) inherit the
  // empty/neutral defaults from CypherStatement: a session management statement carries no clauses.
}
