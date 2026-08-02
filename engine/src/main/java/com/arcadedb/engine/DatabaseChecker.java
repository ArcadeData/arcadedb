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
import com.arcadedb.exception.RecordNotFoundException;
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
import com.arcadedb.utility.CollectionUtils;
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
  /**
   * #5680: {@code CHECK DATABASE RECORD} scope - visit only these records instead of scanning whole types. The
   * cheap repair for "one vertex's edge chain is broken", which the strict {@code GraphEngine.deleteVertex}
   * points operators at: its cost is the listed records plus, in fix mode, the edge scan a rebuild needs - ONE
   * per distinct vertex TYPE named, since {@code reconnectEdges} runs inside each per-type {@code checkVertices}
   * call. Naming ten vertices of one type costs one sweep; naming one vertex of each of three types costs three.
   */
  private       Set<RID>            records      = Collections.emptySet();
  /** Shared with {@code CheckDatabaseStatement}, which diagnoses the same conflict before resolving the RIDs. */
  public static final String RECORD_SCOPE_CONFLICT_ERROR =
      "CHECK DATABASE RECORD cannot be combined with TYPE or BUCKET: RECORD already names the exact records to "
          + "check. Drop the TYPE/BUCKET clause, or run the two checks separately";
  /** #5680: entries {@link #setRecords} discarded because they did not resolve; reported, never silent. */
  private       int                 droppedRecords = 0;
  /**
   * The ONLY retention bound this class has: it caps both the {@code warnings} messages ({@link #addWarning}) and
   * the {@code corruptedRecords} set ({@link #addCorrupted}).
   * <p>
   * #5764, so a future reader does not "fix" the apparent inconsistency: {@code GraphDatabaseChecker} takes a
   * SEPARATE {@code maxCorrupted} parameter for the same set, which looks like two policies on one run. It is one -
   * {@link #checkEdges}, {@link #checkVertices} and {@link #checkScopedRecords} all pass this field's REMAINING
   * budget into that parameter, so both names resolve to this setting. Aligning the two would mean adding a knob,
   * not removing a divergence.
   * <p>
   * The cap is not purely cosmetic, which is why it is worth stating: the RETAINED {@code corruptedRecords} are
   * what {@link #check()} turns into {@code affectedBuckets}, so a record dropped past the cap does not get the
   * indexes on its bucket rebuilt under {@code FIX}. Only reachable on a run finding more than 100k corrupt
   * records, where a bounded repair is the point.
   */
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
    // it only runs on a full-scope fix: under a type/bucket/record filter the unwalked vertices' segments would
    // be misclassified as orphans.
    final boolean recordScope = !records.isEmpty();
    if (recordScope && ((types != null && !types.isEmpty()) || (buckets != null && !buckets.isEmpty())))
      // #5680: RECORD is the narrowest scope there is, so combining it with TYPE/BUCKET can only mean the caller
      // expected something this does not do. Silently letting RECORD win would run a check the caller did not ask
      // for; an intersection would be a third semantics nobody asked for either. Refuse and say so.
      throw new IllegalArgumentException(RECORD_SCOPE_CONFLICT_ERROR);

    final boolean reclaimOrphanedSegments = fix && !recordScope //
        && (types == null || types.isEmpty()) && (buckets == null || buckets.isEmpty());

    final Set<Index> corruptMetadataIndexes;

    if (recordScope) {
      // #5680: RECORD scope - visit the listed records only. The database-wide passes (buckets, external
      // properties, indexes) are deliberately skipped: they cannot be narrowed to a record, and paying for them
      // would defeat the point of naming one. Everything a listed record is checked and repaired for is
      // identical to the type-wide run, including the edge-list rebuild.
      // NOTE the one cost a record scope does NOT bound: if a listed record turns out to be CORRUPTED, the shared
      // tail below drops and rebuilds every index on its bucket, which is a full bucket scan. That matches the
      // type-wide semantics (a corrupted record's index entries must go) and only fires on genuine corruption -
      // an edge-list rebuild alone deletes no record and triggers none of it - but it means "scoped" bounds the
      // CHECK, not necessarily the FIX.
      final Map<DocumentType, List<RID>> scoped = groupRecordsByType();

      currentStep = 0;
      totalSteps = scoped.size() + (fix ? 1 : 0) + (compress ? 1 : 0);

      if (droppedRecords > 0)
        // No count, for the same reason the refusal above quotes none.
        addWarning("one or more of the records given did not resolve to a RID and were not checked");

      if (compress)
        // COMPRESS is not scoped and cannot be: it works on buckets, not records. Legal and meaningful ("check
        // this record, then compress the database"), so it is not refused - but naming a record sets an
        // expectation of a bounded run, and this is the one clause that breaks it. Say so rather than surprise.
        addWarning(
            "COMPRESS is not limited by the RECORD scope: the whole database will be compressed after the check");

      checkScopedRecords(scoped);

      corruptMetadataIndexes = Collections.emptySet();

    } else {
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

      corruptMetadataIndexes = checkIndexes();
    }

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
        // getPageSizeForNewFile(), not getPageSize(): the index is dropped just below and rebuilt through the creation
        // path. Carrying over a page size that path refuses would make FIX destroy the very index the check flagged -
        // and an unaddressable HASH page size is exactly one of the things checkIntegrity() now reports (#5713).
        final int pageSize = ((IndexInternal) idx).getPageSizeForNewFile();
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

  /**
   * The type-wide document check. #5764 settled the three ways it used to disagree with its RECORD-scoped twin
   * {@link #checkScopedDocuments} over the SAME finding, all of them in the reporting rather than in the check:
   * <ul>
   *   <li>it added to the {@code warnings}/{@code corruptedRecords} sets without touching
   *   {@code totalWarnings}/{@code totalCorruptedRecords}, so a corrupt document reported different numbers
   *   depending on which path found it - and, since the totals are what a capped run reports, reported zero once
   *   the sets filled up;</li>
   *   <li>it called a document a "vertex", and said it was "removing it" when nothing here removes anything -
   *   {@code corruptedRecords} only drives the index rebuild at the end of {@link #check()};</li>
   *   <li>it kept no cap at all, so a type whose whole bucket is unreadable retained one message per record.</li>
   * </ul>
   * Both paths now go through {@link #addWarning}/{@link #addCorrupted}. The accumulators are also per-type now:
   * they used to be declared outside the loop and re-added to the result after every type, which the sets absorbed
   * but paid for in re-insertions proportional to the square of the type count.
   */
  private void checkDocuments(final List<DocumentType> documentTypes) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking documents...");

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
              addWarning("document " + rid + " cannot be loaded");
              addCorrupted(rid);
            }
            stepTick();
            return true;
          }, null);
        }

      } finally {
        database.commit();
      }

      stepComplete();
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

  /**
   * #5680: the RECORD-scoped records grouped by the type that owns their bucket, in insertion order (so the step
   * sequence a caller sees is the order the RIDs were given). A RID whose bucket belongs to no type is grouped
   * under a null key and reported as such by {@link #checkScopedRecords(Map)}.
   */
  private Map<DocumentType, List<RID>> groupRecordsByType() {
    final Map<DocumentType, List<RID>> byType = new LinkedHashMap<>();
    for (final RID rid : records)
      byType.computeIfAbsent(database.getSchema().getTypeByBucketId(rid.getBucketId()), k -> new ArrayList<>()).add(rid);
    return byType;
  }

  /**
   * #5680: runs the per-type check over the RECORD scope only. Each group is dispatched to the same worker the
   * type-wide run uses, so a listed vertex gets the identical connectivity check and, in fix mode, the identical
   * rebuild of its edge list from the surviving edge records.
   */
  private void checkScopedRecords(final Map<DocumentType, List<RID>> scoped) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking %d record(s)...", null, records.size());

    for (final Map.Entry<DocumentType, List<RID>> entry : scoped.entrySet()) {
      final DocumentType type = entry.getKey();
      final List<RID> rids = entry.getValue();

      // NOTE on currentStep: each arm below owns its own bump, deliberately. The graph arms pass a FIXED value
      // into the nested GraphDatabaseChecker, so they bump it here; the no-type and document arms go through
      // stepBegin, which bumps it itself. Bumping once up front for every arm double-counted those groups and
      // reported "step 3 of 2" as soon as the RID list contained a plain document (totalSteps budgets one step
      // per group).

      if (type == null) {
        // stepBegin (which owns the currentStep bump) rather than a bare ++currentStep: this arm otherwise
        // advanced the step counter without ever naming the step, so a progress poller saw it go by blank.
        stepBegin("Checking records of no known type", rids.size());
        // The bucket belongs to no type (an internal file, or a RID the caller invented). Capped like every other
        // warning source: a caller passing thousands of bogus RIDs must not blow past maxWarnings.
        for (final RID rid : rids) {
          addWarning("record " + rid + " does not belong to any type");
          stepTick();
        }
        stepComplete();
        continue;
      }

      if (!(type instanceof LocalEdgeType) && !(type instanceof LocalVertexType)) {
        checkScopedDocuments(type, rids);
        continue;
      }

      ++currentStep;

      final int currentWarnings = ((LinkedHashSet<String>) result.get("warnings")).size();
      final int currentCorrupted = ((LinkedHashSet<RID>) result.get("corruptedRecords")).size();
      final GraphDatabaseChecker graphChecker = new GraphDatabaseChecker(database)
          .setProgress(progressCallback, "Checking records of '" + type.getName() + "'", currentStep, totalSteps);

      final Map<String, Object> stats;
      if (type instanceof LocalEdgeType)
        stats = graphChecker.checkEdges(type.getName(), rids, fix, verboseLevel,
            Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));
      else
        stats = graphChecker.checkVertices(type.getName(), rids, fix, verboseLevel,
            Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));

      updateStats(stats);
      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      mergeMissingReferences((Map<RID, Long>) stats.get("missingReferences"),
          (Map<RID, String>) stats.get("missingReferenceErrors"));
    }
  }

  /**
   * Records a warning raised by this class itself (as opposed to one merged in from a nested checker), under the same
   * {@link CollectionUtils#addBounded} rule as {@link #addCorrupted}, and - like {@code GraphDatabaseChecker}'s twin -
   * LOGGED when the cap meant it could not be retained, so a capped run does not lose the message silently. Both
   * honour {@code verboseLevel == 0} as "the caller asked for no logging"; before #5773 only this one did, so the
   * same dropped message was audible from one arm and silent from the other. The retained set and
   * {@code totalWarnings} still report how many were dropped either way.
   * <p>
   * #5764: shared with the type-wide {@link #checkDocuments}, which used to bypass the totals and the cap.
   * <p>
   * #5773: de-duplicated, where it used to count occurrences. The retained {@code warnings} is a {@code Set}, so two
   * findings that render to the same message have always collapsed to one line; the total counted both, which made it
   * exceed the retained size on a run nowhere near its cap. Both sides now answer "distinct messages".
   * <p>
   * The one residual: a message produced identically by two DIFFERENT sub-checks (each {@code GraphDatabaseChecker}
   * keeps its own set, and {@link #updateStats} sums their totals while the sets are union-ed here) is still
   * retained once and counted twice. Reachable only for the few messages that carry no RID - "reconnected N
   * outgoing edges" for two types that reconnected the same N - and closing it would mean threading a dropped-count
   * out of every sub-check instead of a total, which buys nothing an operator reads.
   */
  private void addWarning(final String warning) {
    final LinkedHashSet<String> warnings = (LinkedHashSet<String>) result.get("warnings");
    final CollectionUtils.BoundedAdd outcome = CollectionUtils.addBounded(warnings, maxWarnings, warning);
    if (!outcome.isFirstSighting())
      return;
    if (outcome == CollectionUtils.BoundedAdd.DROPPED && verboseLevel > 0)
      LogManager.instance().log(this, Level.WARNING, "- " + warning);
    result.put("totalWarnings", (Long) result.get("totalWarnings") + 1);
  }

  /**
   * Flags a record as corrupted under {@link CollectionUtils#addBounded}, which documents the retain-and-de-duplicate
   * rule itself: exact while under the cap, degrading past it only for RIDs the cap refused to retain. Neither caller
   * of THIS one reaches the degraded case today - the type-wide bucket scan visits each RID once, and the RECORD scope
   * is a {@link Set}, so a duplicate cannot survive as far as {@link #groupRecordsByType()}.
   * <p>
   * #5773: there used to be four hand-written copies of that rule across this class and
   * {@link GraphDatabaseChecker}, and two of them disagreed past the cap - the graph one incremented unconditionally
   * there, so a RID still present in its retained set was counted again. That case IS reachable (its
   * {@code checkEdges} flags one RID twice for an edge whose endpoints are both gone). Extracting the rule is what
   * keeps them from drifting apart again; do not re-inline it.
   * <p>
   * The cap is {@code maxWarnings} because that is the only bound this class has; the {@code maxCorrupted} the
   * graph arms take is a separate knob {@link GraphDatabaseChecker} owns, and it is passed the remaining budget
   * from this same field by {@link #checkScopedRecords}. Two names, one setting.
   */
  private void addCorrupted(final RID rid) {
    if (CollectionUtils.addBounded((LinkedHashSet<RID>) result.get("corruptedRecords"), maxWarnings, rid)
        .isFirstSighting())
      result.put("totalCorruptedRecords", (Long) result.get("totalCorruptedRecords") + 1);
  }

  /**
   * The document arm of the RECORD scope: the same "does it load as a document" check {@link #checkDocuments} does,
   * over the RIDs named instead of over every bucket of the type. Since #5764 the two report a finding identically
   * - same message, same totals, same cap - so a corrupt document reads the same whichever path found it.
   */
  private void checkScopedDocuments(final DocumentType type, final List<RID> rids) {
    stepBegin("Checking records of '" + type.getName() + "'", rids.size());

    database.begin();
    try {
      for (final RID rid : rids) {
        try {
          database.lookupByRID(rid, true).asDocument(true);
        } catch (final RecordNotFoundException e) {
          // Not corruption: flagging it would put this bucket into affectedBuckets and have FIX drop and rebuild
          // every index on it - see the same guard in GraphDatabaseChecker's scoped arms.
          addWarning("document " + rid + " does not exist");
        } catch (final Exception e) {
          // NOT "removing it": corruptedRecords only drives the index rebuild, nothing deletes the record here.
          addWarning("document " + rid + " cannot be loaded");
          addCorrupted(rid);
        }
        stepTick();
      }
    } finally {
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

  /**
   * Restricts the check to these records (#5680). A null or empty set means "no record scope" - the check runs
   * over the whole database, or over whatever {@link #setTypes}/{@link #setBuckets} narrowed it to.
   * <p>
   * A set that is NOT empty but contains only nulls is REFUSED rather than treated as "no scope". Silently
   * dropping them would widen a call that explicitly named records into a full-database scan, which on a large
   * database is enormously more expensive than what was asked for and is the one outcome such a caller cannot
   * have wanted. The guard lives here rather than only in the SQL layer because this is the public entry point,
   * and it is the last place that can still tell "named records, none usable" from "named no records" - once the
   * nulls are dropped the two are indistinguishable.
   */
  public DatabaseChecker setRecords(final Set<RID> records) {
    if (records == null || records.isEmpty()) {
      this.records = Collections.emptySet();
      // Reset too: this is public API and can be called more than once, so a count left over from an earlier
      // call must not survive into a run that has no record scope at all.
      this.droppedRecords = 0;
      return this;
    }
    // LinkedHashSet: the grouping below (and so the reported step order) follows the order the RIDs were given.
    // Nulls are dropped rather than trusted away: Rid.toRecordId() answers null for a non-literal RID form, which
    // groupRecordsByType would meet as an NPE on getBucketId().
    final LinkedHashSet<RID> copy = new LinkedHashSet<>(records.size());
    for (final RID rid : records)
      if (rid != null)
        copy.add(rid);

    // A PARTIAL drop narrows the scope rather than widening it, so it is not refused - but it is not swallowed
    // either: a caller who mistyped one of several RIDs would otherwise get a clean report for a check that
    // quietly skipped it. Surfaced as a warning by check().
    this.droppedRecords = records.size() - copy.size();

    if (copy.isEmpty())
      // Deliberately no count: a Set collapses every null-resolving RID into one element, so any number quoted
      // here would be wrong for a multi-RID statement.
      throw new IllegalArgumentException("CHECK DATABASE RECORD: none of the records given resolves to a RID");

    this.records = copy;
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
