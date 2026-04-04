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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextQueryExecutor;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.schema.Schema;

import java.util.*;

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
  public static final String NAME = "search_index";

  public SQLFunctionSearchIndex() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX() requires 2 parameters: indexName and query");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_INDEX() parameters cannot be null");

    final String indexName = iParams[0].toString();
    final String queryString = iParams[1].toString();

    // Cache key for this specific search
    final String cacheKey = "search_index:" + indexName + ":" + queryString;

    // Try to get cached results (Map of RID -> score)
    @SuppressWarnings("unchecked")
    Map<RID, Integer> allResults = (Map<RID, Integer>) iContext.getVariable(cacheKey);

    if (allResults == null) {
      // First execution - perform the actual search
      allResults = performSearch(indexName, queryString, iContext.getDatabase());

      // Cache the results
      iContext.setVariable(cacheKey, allResults);
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      final RID rid = iCurrentRecord.getIdentity();
      final boolean matches = allResults.containsKey(rid);

      if (matches) {
        // Store the score for this record in the context variable $score
        // This allows $score projection to work in SELECT
        final int recordScore = allResults.get(rid);
        iContext.setVariable("$score", (float) recordScore);
      } else {
        // Clear the score for non-matching records
        iContext.setVariable("$score", 0f);
      }

      return matches;
    }

    // Return cursor with all results (used when called without a current record)
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, Integer> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] { queryString }, entry.getKey(), entry.getValue()));
    }
    entries.sort((a, b) -> Integer.compare(b.score, a.score));

    return new TempIndexCursor(entries);
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
    if (oExpressions == null || oExpressions.length < 2)
      return false;

    // Verify the named index exists and is a full-text type index
    final String indexName = resolveStringParam(oExpressions[0], context);
    if (indexName == null)
      return false;

    final Schema schema = context.getDatabase().getSchema();
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
    if (oExpressions == null || oExpressions.length < 2)
      return List.of();

    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null)
      return List.of();

    // Perform the search and cache results
    final String cacheKey = "search_index:" + indexName + ":" + queryString;
    @SuppressWarnings("unchecked")
    Map<RID, Integer> allResults = (Map<RID, Integer>) context.getVariable(cacheKey);

    if (allResults == null) {
      allResults = performSearch(indexName, queryString, context.getDatabase());
      context.setVariable(cacheKey, allResults);
    }

    // Sort RIDs by score descending for optimal ORDER BY $score performance
    final List<Map.Entry<RID, Integer>> sorted = new ArrayList<>(allResults.entrySet());
    sorted.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

    // Fetch records by RID
    final DatabaseInternal db = (DatabaseInternal) context.getDatabase();
    final List<Record> records = new ArrayList<>(sorted.size());
    for (final Map.Entry<RID, Integer> entry : sorted) {
      try {
        final Record record = db.lookupByRID(entry.getKey(), true);
        if (record != null)
          records.add(record);
      } catch (final RecordNotFoundException e) {
        // Record was deleted since the index was last updated, skip it
      }
    }
    return records;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * Performs the full-text search across all bucket indexes and returns a map of RID to score.
   */
  private Map<RID, Integer> performSearch(final String indexName, final String queryString, final Database database) {
    final Map<RID, Integer> allResults = new HashMap<>();

    final Index index = database.getSchema().getIndexByName(indexName);

    if (index == null)
      throw new CommandExecutionException("Index '" + indexName + "' not found");

    if (!(index instanceof final TypeIndex typeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    // Get the underlying full-text index
    final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    // Execute search across all bucket indexes
    for (final Index bucketIndex : bucketIndexes) {
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex) {
        final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);
        final IndexCursor cursor = executor.search(queryString, -1);

        while (cursor.hasNext()) {
          final Identifiable match = cursor.next();
          final int score = cursor.getScore();
          allResults.merge(match.getIdentity(), score, Integer::sum);
        }
      }
    }

    return allResults;
  }

  /**
   * Resolves a function expression parameter to a string value.
   */
  private static String resolveStringParam(final Expression expr, final CommandContext context) {
    if (expr == null)
      return null;
    final Object value = expr.execute((Identifiable) null, context);
    return value != null ? value.toString() : null;
  }
}
