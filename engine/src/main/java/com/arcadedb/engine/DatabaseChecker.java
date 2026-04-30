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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.GraphDatabaseChecker;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LongHashSet;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

public class DatabaseChecker {
  private final DatabaseInternal    database;
  private       int                 verboseLevel = 1;
  private       boolean             fix          = false;
  private       boolean             compress     = false;
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
    result.put("deletedRecordsAfterFix", new LinkedHashSet<>());
    result.put("corruptedRecords", new LinkedHashSet<>());

    checkEdges();

    checkVertices();

    checkDocuments();

    checkBuckets(result);

    checkExternalProperties();

    final Set<Integer> affectedBuckets = new HashSet<>();
    for (final RID rid : (Collection<RID>) result.get("corruptedRecords"))
      if (rid != null)
        affectedBuckets.add(rid.getBucketId());

    final Set<Index> affectedIndexes = new HashSet<>();
    for (final Index index : database.getSchema().getIndexes())
      if (affectedBuckets.contains(index.getAssociatedBucketId()))
        affectedIndexes.add(index);

    final Set<String> rebuildIndexes = affectedIndexes.stream().map(x -> x.getName()).collect(Collectors.toSet());
    result.put("rebuiltIndexes", rebuildIndexes);

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Rebuilding indexes %s...", null, rebuildIndexes);

    if (fix)
      for (final Index idx : affectedIndexes) {
        final String bucketName = database.getSchema().getBucketById(idx.getAssociatedBucketId()).getName();
        final Schema.INDEX_TYPE indexType = idx.getType();
        final boolean unique = idx.isUnique();
        final List<String> propNames = idx.getPropertyNames();
        final String typeName = idx.getTypeName();
        final int pageSize = ((IndexInternal) idx).getPageSize();
        final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = idx.getNullStrategy();

        database.getSchema().dropIndex(idx.getName());

        database.getSchema().buildBucketIndex(typeName, bucketName, propNames.toArray(new String[propNames.size()]))
            .withType(indexType).withUnique(unique).withPageSize(pageSize).withNullStrategy(nullStrategy).create();
      }

    if (compress)
      compress();

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Result:\n%s", null, new JSONObject(result).toString(2));

    return result;
  }

  private void checkDocuments() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking documents...");

    final List<String> warnings = new ArrayList<>();
    final Set<RID> corruptedRecords = new LinkedHashSet<>();

    for (final DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (!(type instanceof LocalVertexType) && !(type instanceof LocalEdgeType)) {
        database.begin();
        try {

          // CHECK RECORD IS OF THE RIGHT TYPE
          for (final Bucket b : type.getBuckets(false)) {
            b.scan((rid, view) -> {
              try {
                final Record record = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
                record.asDocument(true);
              } catch (Exception e) {
                warnings.add("vertex " + rid + " cannot be loaded, removing it");
                corruptedRecords.add(rid);
              }
              return true;
            }, null);
          }

        } finally {
          database.commit();
        }

        ((LinkedHashSet<String>) result.get("warnings")).addAll(warnings);
        ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll(corruptedRecords);
      }
    }
  }

  public void compress() {
    if (database.isTransactionActive())
      database.rollback();

    int pageTxBatch = 10;
    int pageBatch = 0;

    for (final Bucket b : database.getSchema().getBuckets()) {
      final LocalBucket bucket = (LocalBucket) b;

      database.begin();

      final int pages = bucket.getTotalPages();
      for (int i = 0; i < pages; i++) {
        try {
          final MutablePage page = database.getTransaction()
              .getPageToModify(new PageId(database, bucket.getFileId(), i), bucket.getPageSize(), false);

          bucket.compressPage(page, true);

          ++pageBatch;

          if (pageBatch >= pageTxBatch) {
            database.commit();
            database.begin();
            pageBatch = 0;
          }

        } catch (IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on loading page %d of bucket %s", e, i, bucket.getName());
        }
      }

      database.commit();
    }
  }

  private void checkEdges() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking edges...");

    for (final DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof LocalEdgeType) {
        final Map<String, Object> stats = new GraphDatabaseChecker(database).checkEdges(type.getName(), fix, verboseLevel);

        updateStats(stats);

        ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
        ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      }
    }
  }

  private void checkVertices() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking vertices...");

    for (final DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty())
        if (type == null || !types.contains(type.getName()))
          continue;

      if (type instanceof LocalVertexType) {
        final Map<String, Object> stats = new GraphDatabaseChecker(database).checkVertices(type.getName(), fix, verboseLevel);

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

  public DatabaseChecker setCompress(final boolean compress) {
    this.compress = compress;
    return this;
  }

  /** Detects (and on FIX deletes) external-property records that are no longer referenced by any primary record. */
  private void checkExternalProperties() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking external-property buckets...");

    final List<String> warnings = new ArrayList<>();
    final Set<RID> orphanedExternalRecords = new LinkedHashSet<>();
    long fixedCount = 0L;

    // For every external bucket, build the set of positions actually referenced from primary records of its
    // owning type. Orphan = an external record whose position is NOT in that set.
    //
    // We use LongHashSet (open-addressing primitive long set) instead of HashSet<Long> to skip Long-boxing on
    // every add/contains and shrink the per-entry footprint from ~48 B (Long box + HashMap.Node + array slot)
    // to ~8 B + load-factor slack. On a database with 10M referenced positions this is the difference between
    // ~50 MB and ~10 MB of CHECK DATABASE working set. A future streaming variant could sort both bucket scans
    // by position and walk them in lockstep, dropping the heap footprint to O(1) at the cost of a sort.
    final Map<Integer, LongHashSet>   referencedByExtBucketId = new HashMap<>();
    final Map<Integer, LocalBucket>   extBucketsToCheck       = new HashMap<>();

    for (final DocumentType type : database.getSchema().getTypes()) {
      if (!(type instanceof LocalDocumentType ldt) || !ldt.hasExternalProperties())
        continue;
      if (types != null && !types.isEmpty() && !types.contains(type.getName()))
        continue;

      for (final Bucket primaryBucket : type.getBuckets(false)) {
        final Integer extBucketId = ldt.getExternalBucketIdFor(primaryBucket.getFileId());
        if (extBucketId == null)
          continue;
        final LocalBucket extBucket = (LocalBucket) database.getSchema().getBucketById(extBucketId);
        extBucketsToCheck.put(extBucketId, extBucket);
        final LongHashSet referenced = referencedByExtBucketId.computeIfAbsent(extBucketId, k -> new LongHashSet());

        primaryBucket.scan((rid, view) -> {
          try {
            final Document record = (Document) database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
            for (final RID extRid : database.getSerializer().findExistingExternalRids(database, record).values())
              referenced.add(extRid.getPosition());
          } catch (final Exception e) {
            warnings.add("primary record " + rid + " could not be parsed for external pointer scan: " + e.getMessage());
          }
          return true;
        }, null);
      }
    }

    for (final Map.Entry<Integer, LocalBucket> entry : extBucketsToCheck.entrySet()) {
      final LocalBucket extBucket = entry.getValue();
      final LongHashSet referenced = referencedByExtBucketId.get(entry.getKey());

      final List<RID> orphans = new ArrayList<>();
      extBucket.scan((rid, view) -> {
        if (!referenced.contains(rid.getPosition()))
          orphans.add(rid);
        return true;
      }, null);

      orphanedExternalRecords.addAll(orphans);

      if (fix && !orphans.isEmpty()) {
        final boolean startedNewTx = !database.isTransactionActive();
        if (startedNewTx)
          database.begin();
        // All-or-nothing per bucket: any failure rolls back the whole batch (or, if a caller-supplied tx is
        // already open, throws so the caller can decide). We never commit a partially-cleaned bucket.
        boolean anyFailure = false;
        long localFixed = 0L;
        for (final RID orphan : orphans) {
          try {
            extBucket.deleteRecord(orphan);
            // Mirror the accounting in LocalDatabase.cascadeDeleteExternalValues so count() stays consistent.
            database.getTransaction().updateBucketRecordDelta(extBucket.getFileId(), -1);
            localFixed++;
          } catch (final Exception e) {
            warnings.add("could not delete orphan external record " + orphan + ": " + e.getMessage());
            anyFailure = true;
            break;
          }
        }
        if (startedNewTx) {
          if (anyFailure)
            database.rollback();
          else {
            database.commit();
            fixedCount += localFixed;
          }
        } else if (anyFailure) {
          throw new com.arcadedb.exception.DatabaseOperationException(
              "Failed to delete orphan external records in bucket '" + extBucket.getName()
                  + "'; aborting CHECK DATABASE FIX so the caller's transaction is not silently committed in a partial state");
        } else {
          fixedCount += localFixed;
        }
      }
    }

    result.put("orphanedExternalRecords", (long) orphanedExternalRecords.size());
    result.put("orphanedExternalRecordsFixed", fixedCount);
    // BinarySerializer.findExistingExternalRids() catches parse failures and returns an empty map, which
    // means the caller skips orphan-cleanup for that record. Surfacing the JVM-cumulative count here lets the
    // operator notice corruption-driven leak rates climbing without having to grep WARN logs. The counter is
    // process-static, so re-runs of CHECK report the running total since startup.
    result.put("externalRidScanFailuresCumulative",
        com.arcadedb.serializer.BinarySerializer.getExternalRidScanFailures());
    ((LinkedHashSet<String>) result.get("warnings")).addAll(warnings);
    if (fix)
      ((LinkedHashSet<RID>) result.get("deletedRecordsAfterFix")).addAll(orphanedExternalRecords);
  }

  private void checkBuckets(final Map<String, Object> result) {
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
    result.put("totalAllocatedDocuments", 0L);
    result.put("totalActiveDocuments", 0L);
    result.put("totalAllocatedVertices", 0L);
    result.put("totalActiveVertices", 0L);
    result.put("totalAllocatedEdges", 0L);
    result.put("totalActiveEdges", 0L);

    for (final Bucket b : database.getSchema().getBuckets()) {
      final LocalBucket bucket = (LocalBucket) b;
      if (buckets != null && !buckets.isEmpty())
        if (!buckets.contains(bucket.componentName))
          continue;

      if (types != null && !types.isEmpty()) {
        final DocumentType type = database.getSchema().getTypeByBucketId(bucket.fileId);
        if (type == null || !types.contains(type.getName()))
          continue;
      }

      final boolean startedNewTx = !database.isTransactionActive();
      if (startedNewTx && fix)
        database.begin();

      final Map<String, Object> stats = bucket.check(verboseLevel, fix);

      if (startedNewTx && fix)
        database.commit();

      updateStats(stats);

      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("deletedRecordsAfterFix")).addAll((Collection<RID>) stats.get("deletedRecordsAfterFix"));
    }

    result.put("avgPageUsed", (Long) result.get("totalPages") > 0 ?
        ((float) (Long) result.get("totalMaxOffset")) / (Long) result.get("totalPages") * 100F / (Long) result.get("pageSize") :
        0F);
  }

  private void updateStats(final Map<String, Object> stats) {
    for (final Map.Entry<String, Object> entry : stats.entrySet()) {
      final Object value = entry.getValue();
      if (value instanceof Long long1) {
        Long current = (Long) result.get(entry.getKey());
        if (current == null)
          current = 0L;
        result.put(entry.getKey(), current + long1);
      }
    }
  }
}
