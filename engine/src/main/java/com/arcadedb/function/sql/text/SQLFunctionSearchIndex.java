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
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionArity;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.executor.IndexedFunctionPushdown;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.schema.MultiTypeIndexes;
import com.arcadedb.schema.Schema;

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

  // The key of the opaque statement cache that holds the searches built so far (see SearchMemo); the other keys of the function all
  // carry a ':' and a name after their prefix, so no index name can make one equal to it
  private static final String SEARCHES_KEY = "search_index_searches";

  public SQLFunctionSearchIndex() {
    super(NAME);
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  /** A third argument used to be ignored without a word: it is refused instead (issue #11069). */
  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw FunctionArity.mismatch(NAME, FunctionArity.describe(2, 2), iParams.length);

    if (iParams[0] == null || iParams[1] == null)
      throw new InvalidArgumentException("SEARCH_INDEX() parameters cannot be null");

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
    if (readsRecordAttribute(oExpressions[0], context))
      return false;
    final String indexName = resolveStringParam(oExpressions[0], context);
    if (indexName == null)
      return false;

    final Schema schema = context.getDatabase().getSchema();
    // a logical index over several types (issue #11014) is searched through its members; a name that is an index is not a logical
    // one, so the walk of the types is paid only by a name that is not
    final boolean exists = schema.existsIndex(indexName);
    if (!exists && MultiTypeIndexes.find(schema, indexName) != null)
      return true;

    // A name built from the record (`@type + '[name]'`) resolves, with no record, to a name that is no index: the call is
    // evaluated per record, where the name is the real one, instead of being refused here for a name nobody wrote (issue #12795)
    if (!exists && !oExpressions[0].isEarlyCalculated(context))
      return false;

    final Index index = schema.getIndexByName(indexName);
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
    checkArity(oExpressions);

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
  public boolean continuesPastLimit() {
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

    final List<TypeIndex> logical = MultiTypeIndexes.resolve(context.getDatabase().getSchema(), indexName);
    if (logical != null && !logical.isEmpty())
      return search(indexName, queryString, context).explainScoring(context);

    final Index index = context.getDatabase().getSchema().getIndexByName(indexName);
    if (!(index instanceof final TypeIndex typeIndex))
      return null;

    return FullTextSearch.explainScoring(typeIndex, queryString, null, WorkGuard.forCommandDeadline(context));
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * The search of the named full-text index, or of the per-type indexes of the logical index of that name (issue #11014), which
   * are validated here.
   *
   * @throws SchemaException          if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  private static FullTextFunctionSearch search(final String indexName, final String queryString, final CommandContext context) {
    // The function runs once per row and arm: a search is a function of the index name and the query alone, and holds no state of
    // its own (what a statement computes is cached in the context, under keys the search builds), so the statement keeps the ones it
    // has built rather than building the object and its keys again on every row
    final SearchMemo memo = (SearchMemo) context.getCachedValue(SEARCHES_KEY);
    if (memo != null) {
      final FullTextFunctionSearch known = memo.find(indexName, queryString);
      if (known != null)
        return known;
    }

    // Resolved once per statement: the function runs once per row, and the name of a logical index is found by walking the types
    final String membersKey = "search_index_members:" + indexName;
    @SuppressWarnings("unchecked")
    List<TypeIndex> indexes = (List<TypeIndex>) context.getCachedValue(membersKey);
    if (indexes == null) {
      indexes = FullTextSearch.resolveFullTextIndexes(context.getDatabase(), indexName);
      context.setCachedValue(membersKey, indexes);
    }
    final FullTextFunctionSearch search = new FullTextFunctionSearch(indexes, indexName, queryString, null);
    final SearchMemo extended = SearchMemo.with(memo, indexName, queryString, search);
    if (extended != memo)
      context.setCachedValue(SEARCHES_KEY, extended);
    return search;
  }

  /**
   * The searches a statement has built, by index name and query. Immutable, so that a context that reads it through its parent never
   * shares a write with it: a statement that adds a search stores a larger copy in its own context. Holds at most
   * {@link #CAPACITY} searches (a condition has a handful of arms); a search past that is built again on each row, as without the memo.
   */
  private static final class SearchMemo {
    private static final int CAPACITY = 8;

    private final String[]                 names;
    private final String[]                 queries;
    private final FullTextFunctionSearch[] searches;

    private SearchMemo(final String[] names, final String[] queries, final FullTextFunctionSearch[] searches) {
      this.names = names;
      this.queries = queries;
      this.searches = searches;
    }

    /** {@code memo}, which may be null, with one more search; {@code memo} itself when it is full. */
    static SearchMemo with(final SearchMemo memo, final String name, final String query, final FullTextFunctionSearch search) {
      final int size = memo == null ? 0 : memo.searches.length;
      if (size == CAPACITY)
        return memo;
      final String[] names = new String[size + 1];
      final String[] queries = new String[size + 1];
      final FullTextFunctionSearch[] searches = new FullTextFunctionSearch[size + 1];
      if (size > 0) {
        System.arraycopy(memo.names, 0, names, 0, size);
        System.arraycopy(memo.queries, 0, queries, 0, size);
        System.arraycopy(memo.searches, 0, searches, 0, size);
      }
      names[size] = name;
      queries[size] = query;
      searches[size] = search;
      return new SearchMemo(names, queries, searches);
    }

    FullTextFunctionSearch find(final String name, final String query) {
      for (int i = 0; i < searches.length; i++)
        if (names[i].equals(name) && queries[i].equals(query))
          return searches[i];
      return null;
    }
  }

  /**
   * Resolves a function expression parameter to a string value with no current record, or null when it has no value there.
   */
  static String resolveStringParam(final Expression expr, final CommandContext context) {
    final Object value = resolveParam(expr, context);
    return value != null ? value.toString() : null;
  }

  /**
   * Resolves a function expression parameter with no current record, for the indexed fetch and for the planner that decides on
   * it. What reads the record ({@code @type}, a property) is null there. No expression is turned away up front: a value known
   * only when the statement runs and that is not read from the record, such as {@code $parent.$current.term} in a sub-query,
   * resolves here and is answered by the index.
   */
  static Object resolveParam(final Expression expr, final CommandContext context) {
    return expr != null ? expr.execute((Identifiable) null, context) : null;
  }

  /**
   * Whether the argument that names what to search (the index, the fields) reads a record attribute. With no record the
   * attribute is null, and an expression can turn that null into the name of another index
   * ({@code ifnull(@type, 'Other') + '[name]'}): the indexed fetch would then answer from an index the records do not name.
   * Such a call is not served by the index and is evaluated per record. An argument that reads no attribute costs one more
   * evaluation of it, once per plan.
   */
  static boolean readsRecordAttribute(final Expression expr, final CommandContext context) {
    if (expr == null)
      return false;
    context.setCachedValue(SuffixIdentifier.RECORD_ATTRIBUTE_NEEDS_RECORD, SuffixIdentifier.IN_EVERY_READ);
    try {
      expr.execute((Identifiable) null, context);
      return false;
    } catch (final CommandExecutionException e) {
      // the attribute was refused, or the argument fails for a reason the per-record evaluation reports
      return true;
    } finally {
      context.setCachedValue(SuffixIdentifier.RECORD_ATTRIBUTE_NEEDS_RECORD, null);
    }
  }
}
