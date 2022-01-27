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
package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.VertexType;
import org.json.JSONObject;

import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class DatabaseChecker {
  private final DatabaseInternal    database;
  private       int                 verboseLevel = 1;
  private       boolean             fix          = false;
  private       Set<Object>         buckets      = Collections.emptySet();
  private       Set<String>         types        = Collections.emptySet();
  private final Map<String, Object> result       = new HashMap<>();

  public DatabaseChecker(final Database database) {
    this.database = (DatabaseInternal) database;
  }

  public Map<String, Object> check() {
    result.clear();

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Integrity check of database '%s' started", null, database.getName());

    result.put("autoFix", 0L);
    result.put("invalidLinks", 0L);
    result.put("warnings", new LinkedHashSet<>());
    result.put("deletedRecords", new LinkedHashSet<>());
    result.put("corruptedRecords", new LinkedHashSet<>());

    checkEdges();

    checkVertices();

    checkBuckets(result);

    final Set<Integer> affectedBuckets = new HashSet<>();
    for (RID rid : (Collection<RID>) result.get("corruptedRecords"))
      affectedBuckets.add(rid.getBucketId());

    final Set<Index> affectedIndexes = new HashSet<>();
    for (Index index : database.getSchema().getIndexes())
      if (affectedBuckets.contains(index.getAssociatedBucketId()))
        affectedIndexes.add(index);

    final Set<String> rebuildIndexes = affectedIndexes.stream().map(x -> x.getName()).collect(Collectors.toSet());
    result.put("rebuiltIndexes", rebuildIndexes);

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Rebuilding indexes %s...", null, rebuildIndexes);

    if (fix)
      for (Index idx : affectedIndexes) {
        final String bucketName = database.getSchema().getBucketById(idx.getAssociatedBucketId()).getName();
        final EmbeddedSchema.INDEX_TYPE indexType = idx.getType();
        final boolean unique = idx.isUnique();
        final List<String> propNames = idx.getPropertyNames();
        final String typeName = idx.getTypeName();
        final int pageSize = idx.getPageSize();
        final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = idx.getNullStrategy();

        database.getSchema().dropIndex(idx.getName());
        database.getSchema()
            .createBucketIndex(indexType, unique, typeName, bucketName, propNames.toArray(new String[propNames.size()]), pageSize, nullStrategy, null);
      }

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Result:\n%s", null, new JSONObject(result).toString(2));

    return result;
  }

  private void checkEdges() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking edges...");

    for (DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof EdgeType) {
        final Map<String, Object> stats = database.getGraphEngine().checkEdges(type.getName(), fix, verboseLevel);

        updateStats(stats);

        ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
        ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      }
    }
  }

  private void checkVertices() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking vertices...");

    for (DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof VertexType) {
        final Map<String, Object> stats = database.getGraphEngine().checkVertices(type.getName(), fix, verboseLevel);

        updateStats(stats);

        ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
        ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      }
    }
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

  private void checkBuckets(Map<String, Object> result) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking buckets...");

    result.put("pageSize", 0L);
    result.put("totalPages", 0L);
    result.put("totalAllocatedRecords", 0L);
    result.put("totalActiveRecords", 0L);
    result.put("totalPlaceholderRecords", 0L);
    result.put("totalSurrogateRecords", 0L);
    result.put("totalDeletedRecords", 0L);
    result.put("totalMaxOffset", 0L);
    result.put("totalAllocatedVertices", 0L);
    result.put("totalActiveVertices", 0L);
    result.put("totalAllocatedEdges", 0L);
    result.put("totalActiveEdges", 0L);

    for (Bucket bucket : database.getSchema().getBuckets()) {
      if (buckets != null && !buckets.isEmpty())
        if (!buckets.contains(bucket.name))
          continue;

      if (types != null && !types.isEmpty()) {
        final DocumentType type = database.getSchema().getTypeByBucketId(bucket.id);
        if (type == null || !types.contains(type.getName()))
          continue;
      }

      if (fix)
        database.begin();

      final Map<String, Object> stats = bucket.check(verboseLevel, fix);

      if (fix)
        database.commit();

      updateStats(stats);

      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("deletedRecords")).addAll((Collection<RID>) stats.get("deletedRecords"));
    }

    result.put("avgPageUsed", (Long) result.get("totalPages") > 0 ?
        ((float) (Long) result.get("totalMaxOffset")) / (Long) result.get("totalPages") * 100F / (Long) result.get("pageSize") :
        0F);
  }

  private void updateStats(final Map<String, Object> stats) {
    for (Map.Entry<String, Object> entry : stats.entrySet()) {
      final Object value = entry.getValue();
      if (value instanceof Long) {
        Long current = (Long) result.get(entry.getKey());
        if (current == null)
          current = 0L;
        result.put(entry.getKey(), current + (Long) value);
      }
    }
  }
}
