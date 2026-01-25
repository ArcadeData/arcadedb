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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextQueryExecutor;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

import java.util.*;

/**
 * SQL function to search a full-text index.
 *
 * Usage: SEARCH_INDEX('indexName', 'query')
 * Example: SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java database')
 */
public class SQLFunctionSearchIndex extends SQLFunctionAbstract {
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
      allResults = new HashMap<>();

      final Database database = iContext.getDatabase();
      final Index index = database.getSchema().getIndexByName(indexName);

      if (index == null)
        throw new CommandExecutionException("Index '" + indexName + "' not found");

      if (!(index instanceof TypeIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

      final TypeIndex typeIndex = (TypeIndex) index;

      // Get the underlying full-text index
      final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
      if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

      // Execute search across all bucket indexes using QueryExecutor
      for (final Index bucketIndex : bucketIndexes) {
        if (bucketIndex instanceof LSMTreeFullTextIndex) {
          final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) bucketIndex;
          final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);
          final IndexCursor cursor = executor.search(queryString, -1);

          while (cursor.hasNext()) {
            final Identifiable match = cursor.next();
            final int score = cursor.getScore();
            allResults.merge(match.getIdentity(), score, Integer::sum);
          }
        }
      }

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

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }
}
