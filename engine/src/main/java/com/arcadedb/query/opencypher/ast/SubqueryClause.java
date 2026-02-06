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
 * </pre>
 * <p>
 * A CALL subquery executes an inner query for each input row.
 * Variables are imported from the outer scope via the initial WITH clause
 * inside the subquery body. The inner RETURN clause makes computed values
 * available to the outer query.
 */
public class SubqueryClause {
  private final CypherStatement innerStatement;
  private final List<String> scopeVariables; // for CALL (x, y) { ... } syntax, null if not specified
  private final boolean optional;

  public SubqueryClause(final CypherStatement innerStatement, final List<String> scopeVariables,
                         final boolean optional) {
    this.innerStatement = innerStatement;
    this.scopeVariables = scopeVariables;
    this.optional = optional;
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
    return sb.toString();
  }
}
