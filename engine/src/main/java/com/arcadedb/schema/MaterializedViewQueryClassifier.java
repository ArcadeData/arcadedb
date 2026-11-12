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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;

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

  /**
   * Determines if each source record maps to at most one view row, independent of every other record: a simple single-type
   * SELECT with no ORDER BY, SKIP, LIMIT, LET, UNWIND, FACETS, DISTINCT, EXPAND or subquery, and no output column named like the
   * reserved {@code _sourceRID} column. Such a view can be refreshed per changed source record.
   */
  public static boolean isPerRecord(final String sql, final Database database) {
    try {
      if (!isSimple(sql, database))
        return false;

      final SelectStatement select = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
      if (select.getOrderBy() != null || select.getLimit() != null || select.getSkip() != null || select.getLetClause() != null
          || select.getUnwind() != null || select.getFacets() != null)
        return false;

      final Projection projection = select.getProjection();
      // No projection (SELECT FROM X) returns the whole record, as * does
      boolean wholeRecord = projection == null || projection.getItems() == null || projection.getItems().isEmpty();
      if (projection != null) {
        if (projection.isDistinct())
          return false;
        if (projection.getItems() != null)
          for (final ProjectionItem item : projection.getItems()) {
            if (item.isExpand())
              return false;
            if (item.isAll()) {
              wholeRecord = true;
              continue;
            }
            // The OUTPUT name, alias or not (SELECT _sourceRID FROM X names its column too): the derived query adds a column of
            // that name, and the two would collide
            if (MaterializedViewPerRecordRefresher.SOURCE_RID.equalsIgnoreCase(item.getProjectionAliasAsString()))
              return false;
          }
      }

      final List<String> sourceTypes = MaterializedViewSources.of(database, sql).typeNames();
      if (sourceTypes.size() != 1)
        return false;

      // A whole record carries every property its type declares, so the same collision comes from the schema
      if (wholeRecord && declaresSourceRid(database.getSchema(), sourceTypes.getFirst()))
        return false;

      // a subquery over the same type passes the type count, so count the select statements of the whole tree
      return countSelects(new SQLAntlrParser(database).parseWithTree(sql).tree()) <= 1;
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Whether the type, a supertype or a subtype (whose records {@code SELECT FROM} the type returns too) declares a property named
   * like the reserved column, in any case. Read at DDL time only. A schemaless record can still carry one undeclared: no static
   * reading of the schema can see that.
   */
  private static boolean declaresSourceRid(final Schema schema, final String typeName) {
    if (!schema.existsType(typeName))
      return false;
    final DocumentType type = schema.getType(typeName);
    for (final String name : type.getPolymorphicPropertyNames())
      if (MaterializedViewPerRecordRefresher.SOURCE_RID.equalsIgnoreCase(name))
        return true;
    for (final DocumentType subType : type.getSubTypes())
      if (declaresSourceRid(schema, subType.getName()))
        return true;
    return false;
  }

  private static int countSelects(final ParseTree node) {
    int count = node instanceof SQLParser.SelectStatementContext ? 1 : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += countSelects(node.getChild(i));
    return count;
  }
}
