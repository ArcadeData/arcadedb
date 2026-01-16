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

import java.util.ArrayList;
import java.util.List;

/**
 * AST node representing a CALL clause for invoking procedures/functions.
 * <p>
 * Cypher syntax:
 * <pre>
 * CALL procedureName(arg1, arg2, ...)
 * CALL procedureName() YIELD field1, field2 AS alias2
 * OPTIONAL CALL procedureName()
 * </pre>
 * <p>
 * In ArcadeDB, CALL is mapped to SQL functions available in the DefaultSQLFunctionFactory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CallClause {
  private final String procedureName;       // Full procedure name (e.g., "db.labels", "arcadedb.vectorNeighbors")
  private final List<Expression> arguments; // Arguments passed to the procedure
  private final List<YieldItem> yieldItems; // YIELD items (null means yield all, empty means YIELD *)
  private final WhereClause yieldWhere;     // Optional WHERE clause after YIELD
  private final boolean optional;           // OPTIONAL CALL

  /**
   * Represents a YIELD item: fieldName AS alias
   */
  public static class YieldItem {
    private final String fieldName;
    private final String alias; // null if no alias

    public YieldItem(final String fieldName, final String alias) {
      this.fieldName = fieldName;
      this.alias = alias;
    }

    public String getFieldName() {
      return fieldName;
    }

    public String getAlias() {
      return alias;
    }

    public String getOutputName() {
      return alias != null ? alias : fieldName;
    }
  }

  public CallClause(final String procedureName, final List<Expression> arguments,
                    final List<YieldItem> yieldItems, final WhereClause yieldWhere, final boolean optional) {
    this.procedureName = procedureName;
    this.arguments = arguments != null ? new ArrayList<>(arguments) : new ArrayList<>();
    this.yieldItems = yieldItems; // null = no YIELD, empty list = YIELD *
    this.yieldWhere = yieldWhere;
    this.optional = optional;
  }

  public String getProcedureName() {
    return procedureName;
  }

  /**
   * Returns just the function name without namespace.
   * E.g., for "db.labels" returns "labels".
   */
  public String getSimpleName() {
    final int lastDot = procedureName.lastIndexOf('.');
    return lastDot >= 0 ? procedureName.substring(lastDot + 1) : procedureName;
  }

  /**
   * Returns the namespace part of the procedure name.
   * E.g., for "db.labels" returns "db".
   */
  public String getNamespace() {
    final int lastDot = procedureName.lastIndexOf('.');
    return lastDot >= 0 ? procedureName.substring(0, lastDot) : "";
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  public List<YieldItem> getYieldItems() {
    return yieldItems;
  }

  public boolean hasYield() {
    return yieldItems != null;
  }

  public boolean isYieldAll() {
    return yieldItems != null && yieldItems.isEmpty();
  }

  public WhereClause getYieldWhere() {
    return yieldWhere;
  }

  public boolean isOptional() {
    return optional;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (optional)
      sb.append("OPTIONAL ");
    sb.append("CALL ").append(procedureName);
    sb.append("(");
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(arguments.get(i).getText());
    }
    sb.append(")");
    if (yieldItems != null) {
      sb.append(" YIELD ");
      if (yieldItems.isEmpty()) {
        sb.append("*");
      } else {
        for (int i = 0; i < yieldItems.size(); i++) {
          if (i > 0)
            sb.append(", ");
          sb.append(yieldItems.get(i).getFieldName());
          if (yieldItems.get(i).getAlias() != null)
            sb.append(" AS ").append(yieldItems.get(i).getAlias());
        }
      }
    }
    return sb.toString();
  }
}
