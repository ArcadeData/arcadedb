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
package com.arcadedb.function.sql.text;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.executor.IndexedFunctionPushdown;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.function.sql.SQLFunctionAbstract;

import java.util.List;

/**
 * SQL function to search a full-text index.
 * <p>
 * Usage: SEARCH_INDEX('indexName', 'query')
 * Example: SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java database')
 * <p>
 * Implements {@link IndexableSQLFunction} so the query planner can use index-based fetching
 * instead of a full type scan when SEARCH_INDEX is used in a WHERE clause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionSearchIndex extends SQLFunctionAbstract implements IndexableSQLFunction {
  public static final String NAME  = "fulltext.searchIndex";
  public static final String ALIAS = "search_index";

  public SQLFunctionSearchIndex() {
    super(NAME);
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX() requires 2 parameters: indexName and query");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_INDEX() parameters cannot be null");

    return search(iParams[0].toString(), iParams[1].toString(), iContext).evaluate(iCurrentRecord, iContext);
  }

  // ---- IndexableSQLFunction implementation ----

  @Override
  public boolean shouldExecuteAfterSearch(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Must still execute per-row after the indexed fetch to set $score in the context
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Only `= true` asks for the matches (see IndexableSQLFunction.selectsMatches)
    if (oExpressions == null || oExpressions.length < 2 || !IndexableSQLFunction.selectsMatches(operator, right))
      return false;

    // Verify the named index exists and is a full-text type index
    final String indexName = resolveStringParam(oExpressions[0], context);
    if (indexName == null)
      return false;

    final Index index = context.getDatabase().getSchema().getIndexByName(indexName);
    return index instanceof TypeIndex;
  }

  @Override
  public boolean canExecuteInline(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Can always fall back to full scan with per-row evaluation
    return true;
  }

  @Override
  public long estimate(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    return -1;
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions) {
    return searchFromTarget(target, operator, rightValue, context, oExpressions, IndexedFunctionPushdown.NONE);
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions, final int limit) {
    return searchFromTarget(target, operator, rightValue, context, oExpressions, IndexedFunctionPushdown.ofLimit(limit));
  }

  /**
   * Answers the matching records lazily, best first or in RID order, deciding the {@code @rid} conditions of the block on
   * the matches before any record is loaded (issues #9895 and #9909): see {@link FullTextFunctionSearch#search}.
   */
  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions,
      final IndexedFunctionPushdown pushdown) {
    if (oExpressions == null || oExpressions.length < 2)
      return List.of();

    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null)
      return List.of();

    return search(indexName, queryString, context).search(target, context, pushdown);
  }

  @Override
  public boolean supportsRIDOrder() {
    return true;
  }

  @Override
  public boolean answersByScore() {
    return true;
  }

  @Override
  public Object getScoringExplain(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 2)
      return null;
    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null)
      return null;

    final Index index = context.getDatabase().getSchema().getIndexByName(indexName);
    if (!(index instanceof final TypeIndex typeIndex))
      return null;

    return FullTextSearch.explainScoring(typeIndex, queryString);
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * The search of the named full-text index, which is validated here.
   *
   * @throws SchemaException          if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  private static FullTextFunctionSearch search(final String indexName, final String queryString, final CommandContext context) {
    return new FullTextFunctionSearch(FullTextSearch.resolveFullTextIndex(context.getDatabase(), indexName), queryString, null);
  }

  /**
   * Resolves a function expression parameter to a string value.
   */
  static String resolveStringParam(final Expression expr, final CommandContext context) {
    if (expr == null)
      return null;
    final Object value = expr.execute((Identifiable) null, context);
    return value != null ? value.toString() : null;
  }
}
