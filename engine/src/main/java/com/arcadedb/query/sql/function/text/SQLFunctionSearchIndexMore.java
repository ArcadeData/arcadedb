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
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.fulltext.MoreLikeThisConfig;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * SQL function to search for documents similar to source documents using More Like This.
 *
 * Usage: SEARCH_INDEX_MORE('indexName', [sourceRIDs], {metadata})
 * Example: SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0])
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionSearchIndexMore extends SQLFunctionAbstract {
  public static final String NAME = "search_index_more";

  public SQLFunctionSearchIndexMore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    // Validate parameters
    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX_MORE() requires at least 2 parameters: indexName and sourceRIDs");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_INDEX_MORE() parameters cannot be null");

    final String indexName = iParams[0].toString();

    // Convert sourceRIDs to Set<RID>
    final Set<RID> sourceRids = parseSourceRIDs(iParams[1]);

    // Parse optional metadata
    final MoreLikeThisConfig config;
    if (iParams.length >= 3 && iParams[2] != null) {
      final JSONObject metadata = new JSONObject(iParams[2].toString());
      config = MoreLikeThisConfig.fromJSON(metadata);
    } else {
      config = new MoreLikeThisConfig();
    }

    // Validate source RID count
    if (sourceRids.isEmpty())
      throw new CommandExecutionException("SEARCH_INDEX_MORE requires at least one source RID");

    if (sourceRids.size() > config.getMaxSourceDocs())
      throw new CommandExecutionException("Source RIDs (" + sourceRids.size() + ") exceeds maxSourceDocs limit (" + config.getMaxSourceDocs() + ")");

    // Cache key - use hashCode to avoid special characters that might be interpreted as property paths
    final String cacheKey = "search_index_more:" + indexName + ":" + sourceRids + ":" + config.hashCode();

    // Try to get cached results
    @SuppressWarnings("unchecked")
    Map<RID, float[]> allResults = (Map<RID, float[]>) iContext.getVariable(cacheKey);

    if (allResults == null) {
      allResults = new HashMap<>();

      final Database database = iContext.getDatabase();
      final Index index = database.getSchema().getIndexByName(indexName);

      if (index == null)
        throw new CommandExecutionException("Index '" + indexName + "' not found");

      if (!(index instanceof TypeIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

      final TypeIndex typeIndex = (TypeIndex) index;
      final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();

      if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
        throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

      // Validate source RIDs exist
      for (final RID rid : sourceRids) {
        if (!database.existsRecord(rid))
          throw new CommandExecutionException("Record " + rid + " not found");
      }

      // Execute MLT search across all bucket indexes
      float maxScore = 0f;

      for (final Index bucketIndex : bucketIndexes) {
        if (bucketIndex instanceof LSMTreeFullTextIndex) {
          final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) bucketIndex;
          final IndexCursor cursor = ftIndex.searchMoreLikeThis(sourceRids, config);

          while (cursor.hasNext()) {
            final Identifiable match = cursor.next();
            final float score = (float) cursor.getScore();

            allResults.compute(match.getIdentity(), (k, v) -> {
              if (v == null) return new float[] { score, 0f };
              v[0] += score;
              return v;
            });

            if (score > maxScore)
              maxScore = score;
          }
        }
      }

      // Normalize scores to 0.0-1.0 for $similarity
      if (maxScore > 0f) {
        for (final float[] scoreAndSim : allResults.values()) {
          scoreAndSim[1] = scoreAndSim[0] / maxScore;
        }
      }

      iContext.setVariable(cacheKey, allResults);
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      final RID rid = iCurrentRecord.getIdentity();
      final boolean matches = allResults.containsKey(rid);

      if (matches) {
        final float[] scoreAndSim = allResults.get(rid);
        iContext.setVariable("$score", scoreAndSim[0]);
        iContext.setVariable("$similarity", scoreAndSim[1]);
      } else {
        iContext.setVariable("$score", 0f);
        iContext.setVariable("$similarity", 0f);
      }

      return matches;
    }

    // Return cursor with all results
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, float[]> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] {}, entry.getKey(), (int) entry.getValue()[0]));
    }
    entries.sort((a, b) -> Integer.compare(b.score, a.score));

    return new TempIndexCursor(entries);
  }

  private Set<RID> parseSourceRIDs(final Object sourceRIDsParam) {
    final Set<RID> result = new HashSet<>();

    if (sourceRIDsParam instanceof Collection) {
      for (final Object item : (Collection<?>) sourceRIDsParam) {
        if (item instanceof RID) {
          result.add((RID) item);
        } else if (item instanceof Identifiable) {
          result.add(((Identifiable) item).getIdentity());
        } else {
          result.add(new RID(item.toString()));
        }
      }
    } else if (sourceRIDsParam instanceof RID) {
      result.add((RID) sourceRIDsParam);
    } else {
      throw new CommandExecutionException("sourceRIDs must be a Collection of RIDs");
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX_MORE(<index-name>, <source-rids>, [<metadata>])";
  }
}
