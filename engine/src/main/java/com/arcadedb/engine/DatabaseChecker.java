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
import com.arcadedb.exception.DatabaseOperationException;
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
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.ProgressCallback;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class DatabaseChecker {
  private final DatabaseInternal    database;
  private       int                 verboseLevel = 1;
  private       boolean             fix          = false;
  private       boolean             compress     = false;
  private       Set<Object>         buckets      = Collections.emptySet();
  private       Set<String>         types        = Collections.emptySet();
  private       int                 maxWarnings  = 100_000;
  private final Map<String, Object> result       = new HashMap<>();

  // Distinct missing/unloadable targets (a record an edge points to) aggregated across the edge and vertex scans,
  // with a per-target reference count and a sample error. A single missing supernode can be referenced by millions
  // of edges; this collapses that fan-out so the operator sees "vertex #28:1 ..., referenced by N edge(s)" instead
  // of scrolling N raw warning lines. Kept out of the serialized result as RID-keyed maps; the readable summary is
  // emitted as topMissingReferences/distinctMissingReferences at the end of check().
  private final Map<RID, Long>      missingReferences      = new HashMap<>();
  private final Map<RID, String>    missingReferenceErrors = new HashMap<>();

  // Progress reporting (issue #5372): the step plan is computed upfront in check() so every emission carries a
  // stable stepIndex/totalSteps pair; per-record emissions are throttled to integer-percentage changes.
  private ProgressCallback progressCallback;
  private int              currentStep;
  private int              totalSteps;
  private String           currentStepName = "";
  private long             stepDone;
  private long             stepTotal;
  private int              lastReportedPct;

  public DatabaseChecker(final Database database) {
    this.database = (DatabaseInternal) database;
  }

  public Map<String, Object> check() {
    result.clear();
    missingReferences.clear();
    missingReferenceErrors.clear();

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Integrity check of database '%s' started", null, database.getName());

    result.put("autoFix", 0L);
    result.put("invalidLinks", 0L);
    result.put("warnings", new LinkedHashSet<>());
    result.put("deletedRecordsAfterFix", new LinkedHashSet<>());
    result.put("corruptedRecords", new LinkedHashSet<>());
    result.put("corruptedIndexes", new LinkedHashSet<>());
    result.put("totalWarnings", 0L);
    result.put("totalCorruptedRecords", 0L);
    result.put("distinctMissingReferences", 0L);
    result.put("topMissingReferences", new ArrayList<String>());

    // COMPUTE THE STEP PLAN UPFRONT so every progress emission carries a stable stepIndex/totalSteps pair.
    final List<DocumentType> edgeTypes = new ArrayList<>();
    final List<DocumentType> vertexTypes = new ArrayList<>();
    final List<DocumentType> documentTypes = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty() && (type == null || !types.contains(type.getName())))
        continue;
      if (type instanceof LocalEdgeType)
        edgeTypes.add(type);
      else if (type instanceof LocalVertexType)
        vertexTypes.add(type);
      else
        documentTypes.add(type);
    }

    // The orphaned-segment reclaim (issue #5375) requires walking EVERY vertex to build the reachable set, so
    // it only runs on a full-scope fix: under a type/bucket filter the unwalked vertices' segments would be
    // misclassified as orphans.
    final boolean reclaimOrphanedSegments =
        fix && (types == null || types.isEmpty()) && (buckets == null || buckets.isEmpty());

    currentStep = 0;
    totalSteps = edgeTypes.size() + vertexTypes.size() + documentTypes.size() // per-type checks
        + 3 // buckets + external properties + indexes
        + (reclaimOrphanedSegments ? 1 : 0)
        + (fix ? 1 : 0) // rebuild affected indexes
        + (compress ? 1 : 0);

    checkEdges(edgeTypes);

    checkVertices(vertexTypes);

    if (reclaimOrphanedSegments) {
      // AFTER checkVertices: the chain rebuilds have already re-attached every recoverable segment, so what
      // remains unreachable in the edge-list buckets is genuine garbage.
      ++currentStep;
      final Map<String, Object> stats = new GraphDatabaseChecker(database)
          .setProgress(progressCallback, "Reclaiming orphaned edge segments", currentStep, totalSteps)
          .reclaimOrphanedEdgeSegments(verboseLevel, maxWarnings);
      updateStats(stats);
      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
    }

    checkDocuments(documentTypes);

    checkBuckets(result);

    checkExternalProperties();

    final Set<Index> corruptMetadataIndexes = checkIndexes();

    final Set<Integer> affectedBuckets = new HashSet<>();
    for (final RID rid : (Collection<RID>) result.get("corruptedRecords"))
      if (rid != null)
        affectedBuckets.add(rid.getBucketId());

    final Set<Index> affectedIndexes = new HashSet<>();
    for (final Index index : database.getSchema().getIndexes())
      if (affectedBuckets.contains(index.getAssociatedBucketId()))
        affectedIndexes.add(index);

    // Indexes whose own metadata is corrupt (e.g. a damaged hash index metadata page) must be rebuilt even when
    // no record corruption pointed at their bucket.
    affectedIndexes.addAll(corruptMetadataIndexes);

    final Set<String> rebuildIndexes = affectedIndexes.stream().map(x -> x.getName()).collect(Collectors.toSet());
    result.put("rebuiltIndexes", rebuildIndexes);

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Rebuilding indexes %s...", null, rebuildIndexes);

    if (fix)
      stepBegin("Rebuilding indexes", affectedIndexes.size());

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

        stepTick();
      }

    if (fix)
      stepComplete();

    if (compress)
      compress();

    result.put("distinctMissingReferences", (long) missingReferences.size());
    result.put("topMissingReferences", formatTopMissingReferences());

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Result:\n%s", null, new JSONObject(result).toString(2));

    return result;
  }

  /** Merges one scan's per-target dangling-reference counts into the cross-scan accumulator. */
  private void mergeMissingReferences(final Map<RID, Long> refs, final Map<RID, String> errors) {
    if (refs == null)
      return;
    for (final Map.Entry<RID, Long> e : refs.entrySet()) {
      missingReferences.merge(e.getKey(), e.getValue(), Long::sum);
      if (errors != null)
        missingReferenceErrors.putIfAbsent(e.getKey(), errors.get(e.getKey()));
    }
  }

  /** Top dangling targets by reference count, formatted for humans and capped so the summary stays readable. */
  private List<String> formatTopMissingReferences() {
    final int limit = 100;
    return missingReferences.entrySet().stream()
        .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
        .limit(limit)
        .map(e -> e.getKey() + " could not be loaded (error: " + missingReferenceErrors.getOrDefault(e.getKey(), "?")
            + "), referenced by " + e.getValue() + " edge(s)")
        .collect(Collectors.toList());
  }

  private void checkDocuments(final List<DocumentType> documentTypes) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking documents...");

    final List<String> warnings = new ArrayList<>();
    final Set<RID> corruptedRecords = new LinkedHashSet<>();

    for (final DocumentType type : documentTypes) {
      stepBegin("Checking documents '" + type.getName() + "'", database.countType(type.getName(), false));

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
            stepTick();
            return true;
          }, null);
        }

      } finally {
        database.commit();
      }

      stepComplete();

      ((LinkedHashSet<String>) result.get("warnings")).addAll(warnings);
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll(corruptedRecords);
    }
  }

  public void compress() {
    if (database.isTransactionActive())
      database.rollback();

    long totalPagesToCompress = 0;
    for (final Bucket b : database.getSchema().getBuckets())
      totalPagesToCompress += ((LocalBucket) b).getTotalPages();
    stepBegin("Compressing buckets", totalPagesToCompress);

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
        stepTick();
      }

      database.commit();
    }

    stepComplete();
  }

  private void checkEdges(final List<DocumentType> edgeTypes) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking edges...");

    for (final DocumentType type : edgeTypes) {
      ++currentStep;

      final int currentWarnings = ((LinkedHashSet<String>) result.get("warnings")).size();
      final int currentCorrupted = ((LinkedHashSet<RID>) result.get("corruptedRecords")).size();
      final Map<String, Object> stats = new GraphDatabaseChecker(database)
          .setProgress(progressCallback, "Checking edges '" + type.getName() + "'", currentStep, totalSteps)
          .checkEdges(type.getName(), fix, verboseLevel,
              Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));

      updateStats(stats);

      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      mergeMissingReferences((Map<RID, Long>) stats.get("missingReferences"),
          (Map<RID, String>) stats.get("missingReferenceErrors"));
    }
  }

  private void checkVertices(final List<DocumentType> vertexTypes) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking vertices...");

    for (final DocumentType type : vertexTypes) {
      ++currentStep;

      final int currentWarnings = ((LinkedHashSet<String>) result.get("warnings")).size();
      final int currentCorrupted = ((LinkedHashSet<RID>) result.get("corruptedRecords")).size();
      final Map<String, Object> stats = new GraphDatabaseChecker(database)
          .setProgress(progressCallback, "Checking vertices '" + type.getName() + "'", currentStep, totalSteps)
          .checkVertices(type.getName(), fix, verboseLevel,
              Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));

      updateStats(stats);

      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      mergeMissingReferences((Map<RID, Long>) stats.get("missingReferences"),
          (Map<RID, String>) stats.get("missingReferenceErrors"));
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

  public DatabaseChecker setMaxWarnings(final int maxWarnings) {
    this.maxWarnings = maxWarnings;
    return this;
  }

  /**
   * Installs a receiver for step-by-step progress of the whole check (issue #5372). The step plan is computed
   * upfront in {@link #check()}; emissions are throttled to integer-percentage changes so the callback is
   * never on the hot path.
   */
  public DatabaseChecker setProgressCallback(final ProgressCallback progressCallback) {
    this.progressCallback = progressCallback;
    return this;
  }

  /** Starts a new step of the plan and emits it immediately so pollers see the transition right away. */
  private void stepBegin(final String name, final long total) {
    ++currentStep;
    currentStepName = name;
    stepDone = 0;
    stepTotal = total;
    lastReportedPct = -1;
    if (progressCallback != null)
      progressCallback.onProgress(currentStepName, currentStep, totalSteps, 0, total);
  }

  /** One unit of work done in the current step; emits only when the integer percentage changes. */
  private void stepTick() {
    if (progressCallback == null)
      return;
    ++stepDone;
    if (stepTotal > 0 && stepDone > stepTotal)
      stepDone = stepTotal; // COUNT DRIFT (concurrent writes, placeholders): clamp, never report over 100%
    final int pct = stepTotal > 0 ? (int) (stepDone * 100 / stepTotal) : (int) (stepDone >>> 13);
    if (pct != lastReportedPct) {
      lastReportedPct = pct;
      progressCallback.onProgress(currentStepName, currentStep, totalSteps, stepDone, stepTotal);
    }
  }

  /** Emits the current step as finished (done == total). */
  private void stepComplete() {
    if (progressCallback == null)
      return;
    if (stepTotal > 0)
      stepDone = stepTotal;
    progressCallback.onProgress(currentStepName, currentStep, totalSteps, stepDone, stepTotal > 0 ? stepTotal : stepDone);
  }

  /** Detects (and on FIX deletes) external-property records that are no longer referenced by any primary record. */
  private void checkExternalProperties() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking external-property buckets...");

    stepBegin("Checking external properties", -1);

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
          throw new DatabaseOperationException(
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
        BinarySerializer.getExternalRidScanFailures());
    ((LinkedHashSet<String>) result.get("warnings")).addAll(warnings);
    if (fix)
      ((LinkedHashSet<RID>) result.get("deletedRecordsAfterFix")).addAll(orphanedExternalRecords);

    stepComplete();
  }

  /**
   * Checks the structural integrity of each index's own metadata (independent of record content) and returns the
   * set of indexes found corrupt. Catches damage like a hash index metadata page with an invalid key type (issue
   * #352) proactively, instead of letting it surface as a cryptic failure during a query. On FIX, the returned
   * indexes are rebuilt by the caller together with the record-corruption-affected ones.
   */
  private Set<Index> checkIndexes() {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking index metadata...");

    stepBegin("Checking indexes", database.getSchema().getIndexes().length);

    final List<String> warnings = new ArrayList<>();
    final Set<String>  corruptedIndexNames = new LinkedHashSet<>();
    final Set<Index>   corruptedIndexes    = new HashSet<>();

    for (final Index index : database.getSchema().getIndexes()) {
      stepTick();
      if (types != null && !types.isEmpty()) {
        final String typeName = index.getTypeName();
        if (typeName == null || !types.contains(typeName))
          continue;
      }

      final List<String> problems;
      try {
        problems = ((IndexInternal) index).checkIntegrity();
      } catch (final Exception e) {
        warnings.add("index '" + index.getName() + "': integrity check failed: " + e.getMessage());
        corruptedIndexNames.add(index.getName());
        corruptedIndexes.add(index);
        continue;
      }

      if (!problems.isEmpty()) {
        corruptedIndexNames.add(index.getName());
        corruptedIndexes.add(index);
        for (final String problem : problems)
          warnings.add("index '" + index.getName() + "': " + problem);
      }
    }

    ((LinkedHashSet<String>) result.get("corruptedIndexes")).addAll(corruptedIndexNames);
    ((LinkedHashSet<String>) result.get("warnings")).addAll(warnings);

    stepComplete();

    return corruptedIndexes;
  }

  private void checkBuckets(final Map<String, Object> result) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking buckets...");

    stepBegin("Checking buckets", database.getSchema().getBuckets().size());

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
      stepTick();

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

    stepComplete();
  }

  /**
   * Accumulates every Long entry of the sub-check stats into the global result. This is the single place totals
   * like totalWarnings/totalCorruptedRecords are summed, so callers must NOT add them again or they double-count.
   */
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
