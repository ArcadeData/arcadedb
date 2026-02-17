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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;

/**
 * Determines if a defining query is "simple" (eligible for per-record incremental refresh).
 * Simple queries have: single type in FROM, no GROUP BY, no aggregate functions,
 * no subqueries, no TRAVERSE, no JOINs.
 */
public class MaterializedViewQueryClassifier {

  public static boolean isSimple(final String sql, final Database database) {
    try {
      final Statement parsed = ((DatabaseInternal) database).getStatementCache().get(sql);
      if (!(parsed instanceof SelectStatement select))
        return false;

      // GROUP BY present
      if (select.getGroupBy() != null)
        return false;

      // Check for aggregate functions in projection
      final Projection projection = select.getProjection();
      if (projection != null && projection.getItems() != null) {
        final BasicCommandContext ctx = new BasicCommandContext();
        ctx.setDatabase((DatabaseInternal) database);
        for (final ProjectionItem item : projection.getItems())
          if (item.isAggregate(ctx))
            return false;
      }

      // Check FROM clause: must be a simple type identifier (no subquery, no function call)
      if (select.getTarget() != null && select.getTarget().getItem() != null) {
        final FromItem item = select.getTarget().getItem();
        // Subquery in FROM
        if (item.getStatement() != null)
          return false;
        // Function call in FROM
        if (item.getFunctionCall() != null)
          return false;
        // No type identifier (e.g., bucket or index source)
        if (item.getIdentifier() == null)
          return false;
      }

      return true;
    } catch (final Exception e) {
      // If parsing fails, treat as complex query to be safe
      return false;
    }
  }
}
