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
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;

/**
 * Decides whether an inline sub-query - {@code (SELECT max(x) FROM U)} written as an operand of a comparison, of arithmetic,
 * of a function call or as a projection item - answers the same for every row of the enclosing statement, so that the
 * planner can lift it into a LET that runs once instead of once per row (issue #12714).
 * <p>
 * The answer is deliberately conservative: when in doubt the sub-query stays inline and runs per row, exactly as before.
 * A sub-query is hoistable only when, over the WHOLE parsed tree beneath it (nested sub-queries included):
 * <ul>
 * <li>every statement is a {@code SELECT} that names its own target. A {@code SELECT} without a FROM, or one whose FROM is a
 * function call or a parameter, can reach the outer row through the implicit {@code $current} a function falls back to;</li>
 * <li>no identifier starts with {@code $}: {@code $parent}, {@code $current}, {@code $depth}, a LET variable of the outer
 * statement or a script variable could all differ from one row to the next, and none of them is worth proving constant;</li>
 * <li>{@link CorrelatedSubQueryCache#isCacheable(Statement)} holds: nothing in it writes, and every function and method
 * call is a built-in that answers the same within one execution (no {@code sysdate()}, {@code uuid()},
 * {@code randomInt()}, {@code eval()}, user-defined function...).</li>
 * </ul>
 * Input parameters ({@code ?}, {@code :name}) are constant within one command, so they do not disqualify a sub-query.
 */
public final class UncorrelatedSubQuery {
  private UncorrelatedSubQuery() {
  }

  public static boolean isHoistable(final Statement statement) {
    if (!(statement instanceof SelectStatement))
      return false;
    if (!CorrelatedSubQueryCache.isCacheable(statement))
      return false;
    return SqlAstInspector.allNodesMatch(statement, UncorrelatedSubQuery::isNodeUncorrelated);
  }

  private static boolean isNodeUncorrelated(final SimpleNode node) {
    if (node instanceof Statement)
      return node instanceof SelectStatement select && hasOwnTarget(select.getTarget());

    if (node instanceof Identifier identifier) {
      final String name = identifier.getStringValue();
      return name == null || !name.startsWith("$");
    }

    return true;
  }

  /** A FROM that names a type, a bucket, an index, a schema entry, RIDs or another statement (itself checked as a node). */
  private static boolean hasOwnTarget(final FromClause target) {
    final FromItem item = target == null ? null : target.getItem();
    if (item == null)
      return false;
    return item.getIdentifier() != null || item.getStatement() != null || item.getBucket() != null || item.getBucketList() != null
        || item.getIndex() != null || item.getSchema() != null || (item.getRids() != null && !item.getRids().isEmpty());
  }
}
