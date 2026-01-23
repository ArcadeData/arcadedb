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
import com.arcadedb.index.lsm.FullTextQueryExecutor;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * SQL function to search full-text indexes by field names.
 * Automatically finds the appropriate full-text index for the given fields.
 *
 * Usage: SEARCH_FIELDS(['field1', 'field2'], 'query')
 * Example: SELECT FROM Article WHERE SEARCH_FIELDS(['title', 'content'], 'java')
 */
public class SQLFunctionSearchFields extends SQLFunctionAbstract {
  public static final String NAME = "search_fields";

  public SQLFunctionSearchFields() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_FIELDS() requires 2 parameters: fields array and query");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_FIELDS() parameters cannot be null");

    // Parse field names from first parameter
    final List<String> fieldNames = new ArrayList<>();
    final Object fieldsParam = iParams[0];

    if (fieldsParam instanceof Collection) {
      for (final Object f : (Collection<?>) fieldsParam) {
        fieldNames.add(f.toString());
      }
    } else if (fieldsParam.getClass().isArray()) {
      for (final Object f : (Object[]) fieldsParam) {
        fieldNames.add(f.toString());
      }
    } else {
      fieldNames.add(fieldsParam.toString());
    }

    final String queryString = iParams[1].toString();
    final Database database = iContext.getDatabase();

    // Find the full-text index that covers these fields
    // First, determine the type from the current record or query context
    String typeName = null;
    if (iCurrentRecord != null) {
      typeName = iCurrentRecord.asDocument().getTypeName();
    }

    if (typeName == null)
      throw new CommandExecutionException("SEARCH_FIELDS() requires a type context (use in WHERE clause with FROM)");

    // Cache key for this specific search
    final String cacheKey = "search_fields:" + typeName + ":" + fieldNames + ":" + queryString;

    // Try to get cached results
    @SuppressWarnings("unchecked")
    Map<RID, Integer> allResults = (Map<RID, Integer>) iContext.getVariable(cacheKey);

    if (allResults == null) {
      allResults = new HashMap<>();

      final DocumentType type = database.getSchema().getType(typeName);

      // Find full-text index matching the fields
      TypeIndex matchingIndex = null;
      for (final TypeIndex typeIndex : type.getAllIndexes(true)) {
        if (typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT) {
          final List<String> indexFields = typeIndex.getPropertyNames();
          if (indexFields.containsAll(fieldNames)) {
            matchingIndex = typeIndex;
            break;
          }
        }
      }

      if (matchingIndex == null)
        throw new CommandExecutionException("No full-text index found for fields: " + fieldNames);

      // Execute search using the found index
      for (final Index bucketIndex : matchingIndex.getIndexesOnBuckets()) {
        if (bucketIndex instanceof LSMTreeFullTextIndex) {
          final FullTextQueryExecutor executor = new FullTextQueryExecutor((LSMTreeFullTextIndex) bucketIndex);
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
      return allResults.containsKey(iCurrentRecord.getIdentity());
    }

    // Return cursor with all results
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, Integer> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] { queryString }, entry.getKey(), entry.getValue()));
    }
    entries.sort((a, b) -> Integer.compare(b.score, a.score));

    return new TempIndexCursor(entries);
  }

  @Override
  public String getSyntax() {
    return "SEARCH_FIELDS(<fields-array>, <query>)";
  }
}
