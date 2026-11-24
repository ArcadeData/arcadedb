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

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;

/**
 * The predicates that selected the record an UPDATE or a DELETE writes when its source is a SELECT that makes one row of each
 * record it reads ({@code UPDATE (SELECT @rid, claimed FROM R WHERE k = :k) SET claimed = :me WHERE claimed IS NULL}): the WHERE of
 * the SELECT, which reads the record, and the WHERE of the statement, which reads the row the projection made of it (issue #12593).
 * <p>
 * A row that carries the {@code @rid} of the record and not the record holds what the projection read, and the statement loads the
 * record only when it writes it, so a commit that landed in between is built upon unseen: two concurrent claims of that shape both
 * claimed the record. Both predicates are evaluated again on the record as the write finds it, the row made again from it, and a
 * record they no longer select is not written, as the WHERE of {@code UPDATE R ... WHERE} is evaluated again on a record that changed
 * since its read (issue #11818).
 * <p>
 * Only a SELECT whose rows can be made again from one record qualifies: one that reads records (a type, a bucket, RIDs, an index),
 * with no GROUP BY, aggregation, UNWIND, LET or expand, and whose projection reads no enclosing query ({@code $parent}). Any other
 * source keeps its rows as they are.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class SubquerySourceSelection {
  private final WhereClause sourceWhere;
  private final Projection  projection;
  private final WhereClause where;

  private SubquerySourceSelection(final WhereClause sourceWhere, final Projection projection, final WhereClause where) {
    this.sourceWhere = sourceWhere;
    this.projection = projection;
    this.where = where;
  }

  /**
   * @param target the target of the UPDATE or DELETE
   * @param where  the WHERE of the statement, or null
   *
   * @return the predicates to evaluate again, or null when the target is no SELECT whose rows can be made again from a record, or
   * when neither the SELECT nor the statement has a WHERE
   */
  static SubquerySourceSelection of(final FromClause target, final WhereClause where, final CommandContext context) {
    final FromItem item = target != null ? target.getItem() : null;
    if (item == null || !(item.getStatement() instanceof SelectStatement select))
      return null;
    if (select.getGroupBy() != null || select.getLetClause() != null || select.getUnwind() != null)
      return null;
    final FromItem source = select.getTarget() != null ? select.getTarget().getItem() : null;
    if (source == null || source.getStatement() != null || source.getFunctionCall() != null || source.getInputParam() != null
        || source.getResultSet() != null)
      return null;
    final Projection projection = select.getProjection();
    if (projection != null) {
      if (projection.isExpand() || projection.refersToParent())
        return null;
      for (final ProjectionItem projectionItem : projection.getItems())
        if (projectionItem.isAggregate(context))
          return null;
    }
    if (select.getWhereClause() == null && where == null)
      return null;
    return new SubquerySourceSelection(select.getWhereClause() != null ? select.getWhereClause().copy() : null,
        projection != null ? projection.copy() : null, where != null ? where.copy() : null);
  }

  /** Whether {@code record}, as the write finds it, still yields a row of the source that the statement selects. */
  boolean selects(final Document record, final CommandContext context) {
    final ResultInternal recordRow = new ResultInternal(record);
    // $current is restored: the record is not the row the statement is at until it is written
    final Object oldCurrent = context.getVariable("current");
    try {
      context.setVariable("current", recordRow);
      if (sourceWhere != null && !Boolean.TRUE.equals(sourceWhere.matchesFilters(recordRow, context)))
        return false;
      if (where == null)
        return true;
      final Result row = projection != null ? projection.calculateSingle(context, recordRow) : recordRow;
      context.setVariable("current", row);
      return Boolean.TRUE.equals(where.matchesFilters(row, context));
    } finally {
      context.setVariable("current", oldCurrent);
    }
  }
}
