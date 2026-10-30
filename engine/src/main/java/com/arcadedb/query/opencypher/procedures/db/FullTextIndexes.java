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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.opencypher.query.CypherMultiTypeIndexes;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The full-text index a procedure's name argument designates: one {@code Type[property]} index, or the per-type indexes a
 * {@code CREATE FULLTEXT INDEX ... FOR (n:A|B)} spans (issue #10914).
 * <p>
 * The answers of the per-type indexes are merged by score. BM25 statistics (document frequency, average length) are those of
 * each per-type index, so a score is relative to the index of its own type: records of different types are ranked on a common
 * scale, but not exactly the one a single index over all of them would give.
 */
final class FullTextIndexes {
  private FullTextIndexes() {
  }

  /** The per-type full-text indexes behind the name, at least one. */
  static List<TypeIndex> resolve(final Database database, final String indexName) {
    final List<TypeIndex> logical = CypherMultiTypeIndexes.resolve(database.getSchema(), indexName);
    if (logical == null)
      return List.of(FullTextSearch.resolveFullTextIndex(database, indexName));
    if (logical.isEmpty())
      throw new CommandExecutionException("Index '" + indexName + "' has no full-text index left");
    for (final TypeIndex typeIndex : logical)
      FullTextSearch.resolveFullTextIndex(database, typeIndex.getName());
    return logical;
  }

  /** Searches every index and merges the matches: a record belongs to one type, so no two indexes match the same RID. */
  static Map<RID, Float> search(final List<TypeIndex> typeIndexes, final String queryText, final int limit, final WorkGuard guard) {
    if (typeIndexes.size() == 1)
      return FullTextSearch.search(typeIndexes.getFirst(), queryText, limit, guard);
    final Map<RID, Float> merged = new HashMap<>();
    for (final TypeIndex typeIndex : typeIndexes)
      merged.putAll(FullTextSearch.search(typeIndex, queryText, limit, guard));
    return merged;
  }
}
