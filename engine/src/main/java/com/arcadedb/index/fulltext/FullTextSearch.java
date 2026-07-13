/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared entry point for full-text search (BM25 or CLASSIC similarity) over a type's full-text index. Used by the
 * SEARCH_INDEX SQL function and by callers that need scored results without going through SQL.
 */
public class FullTextSearch {

  private FullTextSearch() {
  }

  /**
   * Resolves a full-text index by name.
   *
   * @throws com.arcadedb.exception.SchemaException if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  public static TypeIndex resolveFullTextIndex(final Database database, final String indexName) {
    final IndexInternal[] bucketIndexes = resolveFullTextBuckets(database, indexName);
    return bucketIndexes[0].getTypeIndex();
  }

  /**
   * Resolves the named index, validates it is a full-text type index, and returns the single snapshot of its bucket
   * sub-indexes so callers validate and iterate over the exact same array.
   *
   * @throws com.arcadedb.exception.SchemaException if no index carries that name
   * @throws CommandExecutionException              if the index exists but is not a full-text type index
   */
  private static IndexInternal[] resolveFullTextBuckets(final Database database, final String indexName) {
    final Index index = database.getSchema().getIndexByName(indexName);

    if (!(index instanceof final TypeIndex typeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    return bucketIndexes;
  }

  /**
   * Searches every bucket index behind the named full-text index and returns the matching RIDs with their scores.
   */
  public static Map<RID, Float> search(final Database database, final String indexName, final String queryText) {
    final Map<RID, Float> allResults = new HashMap<>();

    for (final IndexInternal bucketIndex : resolveFullTextBuckets(database, indexName)) {
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex) {
        final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);
        final IndexCursor cursor = executor.search(queryText, -1);

        while (cursor.hasNext()) {
          final Identifiable match = cursor.next();
          final float score = cursor.getFloatScore();
          // Float::sum across buckets: a given RID lives in exactly one bucket, so a RID is produced by at most one bucket index
          // and the merge is effectively an insert (no real summing). For CLASSIC the additive semantics would also be correct;
          // for BM25 the per-bucket scoping relies on this one-bucket-per-RID invariant - if it ever broke, scores would be
          // double-counted here rather than failing loudly.
          allResults.merge(match.getIdentity(), score, Float::sum);
        }
      }
    }

    return allResults;
  }

  /**
   * Returns the similarity the index scores with: {@link FullTextIndexMetadata#SIMILARITY_BM25} or
   * {@link FullTextIndexMetadata#SIMILARITY_CLASSIC}. Indexes persisted before BM25 support load as CLASSIC.
   */
  public static String getSimilarity(final TypeIndex typeIndex) {
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex)
        return ftIndex.isBM25() ? FullTextIndexMetadata.SIMILARITY_BM25 : FullTextIndexMetadata.SIMILARITY_CLASSIC;

    return FullTextIndexMetadata.SIMILARITY_CLASSIC;
  }

  /**
   * Returns the sorted names of every full-text index in the database.
   */
  public static List<String> listFullTextIndexes(final Database database) {
    final List<String> names = new ArrayList<>();

    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof final TypeIndex typeIndex && typeIndex.getType() == Schema.INDEX_TYPE.FULL_TEXT)
        names.add(typeIndex.getName());

    Collections.sort(names);
    return names;
  }
}
