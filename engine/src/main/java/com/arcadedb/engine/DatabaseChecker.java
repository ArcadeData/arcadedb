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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.*;
import java.util.logging.*;

public class DatabaseChecker {
  private final DatabaseInternal database;
  private       int              verboseLevel = 1;
  private       boolean          fix          = false;
  private       Set<Object>      buckets      = Collections.emptySet();
  private       Set<String>      types        = Collections.emptySet();

  public DatabaseChecker(final Database database) {
    this.database = (DatabaseInternal) database;
  }

  public Map<String, Object> check() {
    final Map<String, Object> result = new LinkedHashMap<>();

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Integrity check of database '%s' started", null, database.getName());

    final LinkedHashSet<String> warnings = new LinkedHashSet<>();
    long autoFix = 0;
    long errors = 0;

    long pageSize = 0;
    long totalPages = 0;
    long totalAllocatedRecords = 0;
    long totalActiveRecords = 0;
    long totalAllocatedVertices = 0;
    long totalActiveVertices = 0;
    long totalAllocatedEdges = 0;
    long totalActiveEdges = 0;
    long totalPlaceholderRecords = 0;
    long totalSurrogateRecords = 0;
    long totalDeletedRecords = 0;
    long totalMaxOffset = 0;
    long missingReferenceBack = 0;

    final Set<RID> corruptedRecords = new HashSet<>();

    long edgesToRemove = 0L;
    long invalidLinks = 0L;

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking edges...", null);

    for (DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof EdgeType) {
        final Map<String, Object> stats = database.getGraphEngine().checkEdges(type.getName(), fix, verboseLevel);

        autoFix += (Long) stats.get("autoFix");
        edgesToRemove += (Long) stats.get("edgesToRemove");
        invalidLinks += (Long) stats.get("invalidLinks");
        missingReferenceBack += (Long) stats.get("missingReferenceBack");
        corruptedRecords.addAll((Collection<RID>) stats.get("corruptedRecords"));
        warnings.addAll((Collection<String>) stats.get("warnings"));
      }
    }

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking vertices...", null);

    for (DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof VertexType) {
        final Map<String, Object> stats = database.getGraphEngine().checkVertices(type.getName(), fix, verboseLevel);

        autoFix += (Long) stats.get("autoFix");
        edgesToRemove += (Long) stats.get("edgesToRemove");
        invalidLinks += (Long) stats.get("invalidLinks");
        corruptedRecords.addAll((Collection<RID>) stats.get("corruptedRecords"));
        warnings.addAll((Collection<String>) stats.get("warnings"));
      }
    }

    result.put("edgesToRemove", edgesToRemove);
    result.put("invalidLinks", invalidLinks);
    result.put("missingReferenceBack", missingReferenceBack);

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking buckets...", null);

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
      totalAllocatedRecords += stats.get("totalAllocatedRecords");
      totalActiveRecords += stats.get("totalActiveRecords");
      totalAllocatedVertices += stats.containsKey("totalAllocatedVertices") ? stats.get("totalAllocatedVertices") : 0L;
      totalActiveVertices += stats.containsKey("totalActiveVertices") ? stats.get("totalActiveVertices") : 0L;
      totalAllocatedEdges += stats.containsKey("totalAllocatedEdges") ? stats.get("totalAllocatedEdges") : 0L;
      totalActiveEdges += stats.containsKey("totalActiveEdges") ? stats.get("totalActiveEdges") : 0L;
      totalPlaceholderRecords += stats.get("totalPlaceholderRecords");
      totalSurrogateRecords += stats.get("totalSurrogateRecords");
      totalDeletedRecords += stats.get("totalDeletedRecords");
      totalMaxOffset += stats.get("totalMaxOffset");

      autoFix += stats.get("autoFix");
      errors += stats.get("errors");
    }

    final float avgPageUsed = totalPages > 0 ? ((float) totalMaxOffset) / totalPages * 100F / pageSize : 0;

    result.put("warnings", warnings);
    result.put("autoFix", autoFix);
    result.put("errors", errors);

    result.put("pageSize", pageSize);
    result.put("totalPages", totalPages);
    result.put("totalAllocatedRecords", totalAllocatedRecords);
    result.put("totalActiveRecords", totalActiveRecords);
    result.put("totalAllocatedVertices", totalAllocatedVertices);
    result.put("totalActiveVertices", totalActiveVertices);
    result.put("totalAllocatedEdges", totalAllocatedEdges);
    result.put("totalActiveEdges", totalActiveEdges);
    result.put("totalPlaceholderRecords", totalPlaceholderRecords);
    result.put("totalSurrogateRecords", totalSurrogateRecords);
    result.put("totalDeletedRecords", totalDeletedRecords);
    result.put("totalMaxOffset", totalMaxOffset);
    result.put("avgPageUsed", avgPageUsed);

    if (verboseLevel > 0) {
      LogManager.instance()
          .log(this, Level.INFO, "Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%", null, totalAllocatedRecords,
              totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords, avgPageUsed);

      LogManager.instance().log(this, Level.INFO, "Completed checking of database '%s':", null, database.getName());
      LogManager.instance().log(this, Level.INFO, "- warning %d", null, warnings);
      LogManager.instance().log(this, Level.INFO, "- auto-fix %d", null, autoFix);
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

  public DatabaseChecker setFix(final boolean fix) {
    this.fix = fix;
    return this;
  }
}
