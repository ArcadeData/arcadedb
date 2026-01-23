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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
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

    final String indexName = iParams[0].toString();
    final String queryString = iParams[1].toString();

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

    // Execute search across all bucket indexes
    final Set<RID> matchingRids = new HashSet<>();

    for (final Index bucketIndex : bucketIndexes) {
      if (bucketIndex instanceof LSMTreeFullTextIndex) {
        final IndexCursor cursor = bucketIndex.get(new Object[] { queryString });
        while (cursor.hasNext()) {
          matchingRids.add(cursor.next().getIdentity());
        }
      }
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      return matchingRids.contains(iCurrentRecord.getIdentity());
    }

    // If no current record context, this shouldn't happen in a WHERE clause
    // but return false as a safe default
    return false;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }
}
