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
 * AST node for ISO/IEC 39075 (GQL) Session Management statements: {@code SESSION SET $name = value},
 * {@code SESSION RESET} and {@code SESSION CLOSE}. Issue #4141 section 2.
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

  @Override
  public boolean isReadOnly() {
    // Intentionally false even though session management reads/writes no query data. isReadOnly() drives
    // isIdempotent(), which HA uses to route a command and which Bolt's isWriteQuery() uses to pick
    // command() vs query() - SESSION statements must go through command() (the only path that dispatches
    // them) and must not be routed to a follower. The MCP permission axis classifies them separately as
    // READ in OpenCypherQueryEngine.getOperationTypes(); that is a different concern from this one.
    return false;
  }

  @Override
  public List<MatchClause> getMatchClauses() {
    return List.of();
  }

  @Override
  public WhereClause getWhereClause() {
    return null;
  }

  @Override
  public ReturnClause getReturnClause() {
    return null;
  }

  @Override
  public boolean hasCreate() {
    return false;
  }

  @Override
  public boolean hasMerge() {
    return false;
  }

  @Override
  public boolean hasDelete() {
    return false;
  }

  @Override
  public OrderByClause getOrderByClause() {
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
    return null;
  }

  @Override
  public SetClause getSetClause() {
    return null;
  }

  @Override
  public DeleteClause getDeleteClause() {
    return null;
  }

  @Override
  public MergeClause getMergeClause() {
    return null;
  }

  @Override
  public List<UnwindClause> getUnwindClauses() {
    return List.of();
  }

  @Override
  public List<WithClause> getWithClauses() {
    return List.of();
  }
}
