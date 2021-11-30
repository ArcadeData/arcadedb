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
 */
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.*;
import java.util.logging.*;

public class DatabaseChecker {
  private final Database    database;
  private       int         verboseLevel = 1;
  private       Set<Object> buckets      = Collections.emptySet();
  private       Set<String> types        = Collections.emptySet();

  public DatabaseChecker(final Database database) {
    this.database = database;
  }

  public Map<String, Object> check() {
    final Map<String, Object> result = new LinkedHashMap<>();

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Starting checking database '%s'...", null, database.getName());

    long autofix = 0;
    long warnings = 0;
    long errors = 0;

    long pageSize = 0;
    long totalPages = 0;
    long totalRecords = 0;
    long totalVertices = 0;
    long totalEdges = 0;
    long totalActiveRecords = 0;
    long totalPlaceholderRecords = 0;
    long totalSurrogateRecords = 0;
    long totalDeletedRecords = 0;
    long totalMaxOffset = 0;

    for (Bucket b : database.getSchema().getBuckets()) {
      if (buckets != null && !buckets.isEmpty())
        if (!buckets.contains(b.name))
          continue;

      if (types != null && !types.isEmpty()) {
        final DocumentType type = database.getSchema().getTypeByBucketId(b.id);
        if (type == null || !types.contains(type.getName()))
          continue;
      }

      final Map<String, Long> stats = b.check(verboseLevel);

      pageSize += stats.get("pageSize");
      totalPages += stats.get("totalPages");
      totalRecords += stats.get("totalRecords");
      totalVertices += stats.containsKey("totalVertices") ? stats.get("totalVertices") : 0L;
      totalEdges += stats.containsKey("totalEdges") ? stats.get("totalEdges") : 0L;
      totalActiveRecords += stats.get("totalActiveRecords");
      totalPlaceholderRecords += stats.get("totalPlaceholderRecords");
      totalSurrogateRecords += stats.get("totalSurrogateRecords");
      totalDeletedRecords += stats.get("totalDeletedRecords");
      totalMaxOffset += stats.get("totalMaxOffset");
    }

    final float avgPageUsed = totalPages > 0 ? ((float) totalMaxOffset) / totalPages * 100F / pageSize : 0;

    result.put("warnings", warnings);
    result.put("autofix", autofix);
    result.put("errors", errors);

    result.put("pageSize", pageSize);
    result.put("totalPages", totalPages);
    result.put("totalRecords", totalRecords);
    result.put("totalVertices", totalVertices);
    result.put("totalEdges", totalEdges);
    result.put("totalActiveRecords", totalActiveRecords);
    result.put("totalPlaceholderRecords", totalPlaceholderRecords);
    result.put("totalSurrogateRecords", pageSize);
    result.put("totalDeletedRecords", totalDeletedRecords);
    result.put("totalMaxOffset", totalMaxOffset);
    result.put("avgPageUsed", avgPageUsed);

    if (verboseLevel > 0) {
      LogManager.instance()
          .log(this, Level.INFO, "Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%", null, totalRecords,
              totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords, avgPageUsed);

      LogManager.instance().log(this, Level.INFO, "Completed checking of database '%s':", null, database.getName());
      LogManager.instance().log(this, Level.INFO, "- warning %d", null, warnings);
      LogManager.instance().log(this, Level.INFO, "- auto-fix %d", null, autofix);
      LogManager.instance().log(this, Level.INFO, "- errors %d", null, errors);
    }

    return result;
  }

  public DatabaseChecker setVerboseLevel(final int verboseLevel) {
    this.verboseLevel = verboseLevel;
    return this;
  }

  public DatabaseChecker setBuckets(final Set<Object> buckets) {
    this.buckets = buckets;
    return this;
  }

  public DatabaseChecker setTypes(final Set<String> types) {
    this.types = types;
    return this;
  }
}
