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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Whether the rows a write clause receives were selected by a predicate on record content (issue #11808): a
 * {@code WHERE}, or a property map in a pattern.
 * <p>
 * Such a predicate was evaluated on the content the transaction read. A write clause that finds the record changed by a
 * concurrent commit since that read reloads the newest content; the values it assigns are evaluated again on it, but the
 * predicate is not, so the write would land on a record the statement's own {@code MATCH ... WHERE} rejects - a
 * compare-and-set that two callers can both win. The statement context carries the answer so the write clause can refuse
 * that case with the retryable conflict instead.
 */
public final class RowGuards {
  private static final String GUARDED_ROWS = "guardedRows";

  private RowGuards() {
  }

  /**
   * Records on {@code context} whether {@code statement} selects the rows it writes with a predicate on record content.
   */
  public static void register(final CommandContext context, final CypherStatement statement) {
    if (!statement.isReadOnly() && hasRowPredicate(statement))
      context.setVariable(GUARDED_ROWS, Boolean.TRUE);
  }

  /**
   * Whether the statement running in {@code context} selects the rows it writes with a predicate on record content.
   */
  public static boolean guardsRows(final CommandContext context) {
    return Boolean.TRUE.equals(context.getVariable(GUARDED_ROWS));
  }

  static boolean hasRowPredicate(final CypherStatement statement) {
    if (statement.getWhereClause() != null)
      return true;
    for (final MatchClause match : statement.getMatchClauses()) {
      if (match.hasWhereClause())
        return true;
      if (match.hasPathPatterns())
        for (final PathPattern path : match.getPathPatterns())
          if (hasPatternPredicate(path))
            return true;
    }
    for (final WithClause with : statement.getWithClauses())
      if (with.getWhereClause() != null)
        return true;
    return false;
  }

  private static boolean hasPatternPredicate(final PathPattern path) {
    for (final NodePattern node : path.getNodes())
      if (node.hasProperties() || node.getPropertiesParameterName() != null || node.hasWhereExpression())
        return true;
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.hasProperties() || rel.getPropertiesParameterName() != null || rel.hasWhereExpression())
        return true;
    return false;
  }
}
