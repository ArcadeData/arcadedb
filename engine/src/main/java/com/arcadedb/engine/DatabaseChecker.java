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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesIntegrity;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.GraphDatabaseChecker;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalTimeSeriesType;
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
  // Matches RebuildIndexStatement.MAX_ATTEMPTS: the lock-contention retry bound for the index-rebuild file lock.
  private static final int          LOCK_MAX_ATTEMPTS = 5;
  // Matches RebuildIndexStatement.POST_DROP_ATTEMPT_MULTIPLIER (#6040): once an attempt's dropIndex() has actually
  // committed, exhausting the retry budget leaves the index missing rather than merely unrebuilt, so that state
  // gets a larger budget than a plain lock-acquisition timeout, where nothing was touched and giving up simply
  // leaves the index exactly as it was.
  private static final int          LOCK_POST_DROP_ATTEMPT_MULTIPLIER = 4;
  /**
   * How many unreferenced files the single log line about them names (issue #6143). A node that leaked files can
   * hold any number of them and an unbounded log line helps nobody; the full list is always in the
   * {@code unreferencedFiles} result key.
   */
  private static final int          LOGGED_UNREFERENCED_FILES = 20;
  private final DatabaseInternal    database;
  private       int                 verboseLevel = 1;
  private       boolean             fix          = false;
  private       boolean             compress     = false;
  /** #6360: see {@link #setDeep(boolean)}. */
  private       boolean             deep         = false;
  /** #6090: see {@link #setDeleteOrphanEdgeRecords(boolean)} for why this is not part of {@link #fix}. */
  private       boolean             deleteOrphanEdgeRecords = false;
  /** #6189: see {@link #setReclaimUnreferencedFiles(boolean)} for why this is not part of {@link #fix} either. */
  private       boolean             reclaimUnreferencedFiles = false;
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
    // Issue #6136 (3): the breakdown behind autoFix, seeded so the three keys are always present rather than
    // appearing only on the runs that happened to perform that kind of repair.
    //
    // autoFix stays the ACTION count it has always been - removedRecords + prunedDanglingEntries - because
    // existing readers and existing tests depend on it, and because "how many defects were there" is not a
    // question the arms can answer jointly: one edge that is both listed in an adjacency chain and corrupt as a
    // record is one defect, two repairs, two writes to two distinct pages. reconnectedEdges is NOT in the sum, for
    // the same backward-compatibility reason - a rebuilt chain has never contributed to autoFix and making it do
    // so would change every existing number. Read the three together to learn what a run did.
    //
    // "removedRecords" rather than the "deletedRecords" the issue proposed, deliberately: the result already
    // carries "totalDeletedRecords", which counts records found in the DELETED state by the bucket scan and has
    // nothing to do with repair, and a key differing from it by one word would read as its non-total sibling. This
    // one is the count of "deletedRecordsAfterFix", the list of what this run actually took out.
    result.put("removedRecords", 0L);
    result.put("prunedDanglingEntries", 0L);
    result.put("reconnectedEdges", 0L);
    result.put("invalidLinks", 0L);
    // Issue #6090: the ORPHAN EDGE RECORD findings, seeded so a clean run publishes zeros rather than omitting the
    // keys - "does this database hold any?" must be answerable from the result of every run, not only from one that
    // happened to find some. unreachableEdgeRecords is the orphan count (an edge record no adjacency list
    // references, so no traversal reaches it though countType counts it); the two edgesMissing* keys are the
    // per-side breakdown, IN only for a bidirectional edge type. missingReferenceBack is left exactly as it was.
    //
    // THE THREE ARE NOT DISJOINT, which matters to anything rendering them as a summary (Studio, an HTTP client):
    // an orphaned BIDIRECTIONAL edge is one record that increments all three, and an orphaned unidirectional one
    // increments unreachableEdgeRecords and edgesMissingOutReference. They answer "how many edges have this
    // defect", each independently, not "which bucket does each defective edge fall into", so summing them
    // double-counts. unreachableEdgeRecords is the one to show as the orphan total; the other two are the
    // per-direction detail behind it plus the half-linked edges that are still reachable from one side.

    result.put("unreachableEdgeRecords", 0L);
    result.put("edgesMissingOutReference", 0L);
    result.put("edgesMissingInReference", 0L);
    result.put("unreachableEdgeRecordsFound", new LinkedHashSet<RID>());
    // Issue #6062: what the back-reference probes cost, seeded so a run always answers "why was this slow" rather
    // than answering it only when a graph pass happened to run. adjacencyProbes is how many times the check asked an
    // endpoint whether its adjacency list references an edge - structurally two per edge, per pass;
    // adjacencyProbeListWalks is how many walks of a list answering them took, which used to be one per probe and
    // now grows with the number of DISTINCT lists; adjacencyEntriesScanned is the entries those walks visited, the
    // number that was quadratic in a super-node's degree. adjacencyProbeListWalks at or above adjacencyProbes on a
    // graph with hubs means the cache is not engaging - the budget may be below the hub's degree, see
    // arcadedb.checkDatabaseAdjacencyCacheEntries.
    result.put("adjacencyProbes", 0L);
    result.put("adjacencyProbeListWalks", 0L);
    result.put("adjacencyEntriesScanned", 0L);
    result.put("warnings", new LinkedHashSet<>());
    // Issue #6143: files this node holds that no schema component claims. Report-only, always present so a clean
    // run says "none" rather than saying nothing, and empty under a RECORD scope, which cannot answer the question.
    result.put("unreferencedFiles", new LinkedHashSet<String>());
    // Issue #6189: which of the above this run actually deleted, seeded empty for the same reason - a caller must
    // be able to read "none" rather than "nothing was asked". Stays empty unless BOTH fix and
    // reclaimUnreferencedFiles are set; see checkUnreferencedFiles().
    result.put("reclaimedUnreferencedFiles", new LinkedHashSet<String>());
    result.put("deletedRecordsAfterFix", new LinkedHashSet<>());
    result.put("corruptedRecords", new LinkedHashSet<>());
    result.put("corruptedIndexes", new LinkedHashSet<>());
    // Issue #6340: the TimeSeries pass, seeded like every other family so a clean run publishes zeros rather than
    // omitting the keys - "did this run look at the TimeSeries files at all?" has to be answerable from the result
    // of every run, and a database with no TimeSeries type answers it with zeros rather than with silence.
    result.put("corruptedTimeSeries", new LinkedHashSet<String>());
    // #6360: one line per DERIVED counter or header FIX rewrote, and a count of them. Deliberately not folded into
    // autoFix - see checkTimeSeries.
    result.put("timeSeriesRepairs", new LinkedHashSet<String>());
    result.put("repairedTimeSeries", 0L);
    result.put("totalTimeSeriesTypes", 0L);
    result.put("totalTimeSeriesShards", 0L);
    result.put("totalTimeSeriesSamples", 0L);
    result.put("totalTimeSeriesSealedBlocks", 0L);
    result.put("totalWarnings", 0L);
    result.put("totalCorruptedRecords", 0L);
    result.put("distinctMissingReferences", 0L);
    result.put("topMissingReferences", new ArrayList<String>());
    // Issue #6128 (5): says WHICH copy of a replicated database this answer describes. The statement is classified
    // non-idempotent, so a follower forwards it to the leader and the check only ever reads the leader's pages;
    // repairs then reach the other nodes as WAL deltas over the page ranges they touched. Neither direction covers a
    // follower whose own copy diverged. Reported as a field rather than left to the documentation because the
    // dangerous reading of a clean result - "the cluster is fine" - is one an operator makes from the output.
    result.put("checkedNodeScope", database.isReplicated() ? "this node only (replicated database)" : "whole database");

    if (database.isReplicated())
      addWarning("this database is replicated and this check inspected only the node that ran it: a clean result "
          + "does not certify the other nodes, and a repair reaches them as replicated changes rather than by each "
          + "node repairing itself. Progress is published per node too, so poll it on the node running the check");

    // COMPUTE THE STEP PLAN UPFRONT so every progress emission carries a stable stepIndex/totalSteps pair.
    final List<DocumentType> edgeTypes = new ArrayList<>();
    final List<DocumentType> vertexTypes = new ArrayList<>();
    final List<DocumentType> documentTypes = new ArrayList<>();
    // Issue #6340: a TimeSeries type is a LocalDocumentType and used to land in documentTypes, where the per-type
    // pass found it had no record buckets and did nothing - which is the whole reason CHECK DATABASE had no
    // TimeSeries coverage. Its data is not in record buckets: it is in the .tstb/.tstd components the schema
    // registers as files and in the .ts.sealed files outside the paginated layer, so it gets its own pass.
    final List<LocalTimeSeriesType> timeSeriesTypes = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes()) {
      if (types != null && !types.isEmpty() && (type == null || !types.contains(type.getName())))
        continue;
      if (type instanceof LocalEdgeType)
        edgeTypes.add(type);
      else if (type instanceof LocalVertexType)
        vertexTypes.add(type);
      else if (type instanceof LocalTimeSeriesType tsType)
        timeSeriesTypes.add(tsType);
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
          // #6340: ONE step for all TimeSeries types, and only when the database has any. A step per type would
          // change the plan of every database that has none, which is what every existing progress expectation
          // was written against; a step that is not there costs nobody anything.
          + (timeSeriesTypes.isEmpty() ? 0 : 1)
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

      checkTimeSeries(timeSeriesTypes);

      checkBuckets(result);

      checkExternalProperties();

      corruptMetadataIndexes = checkIndexes();

      checkUnreferencedFiles();
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

    // A MANUAL index is reported but never rebuilt. It is bound to no type and no bucket, so there is no record to
    // regenerate its entries from - they are the only copy - and the drop-and-recreate below would destroy them. It
    // cannot even be attempted: with no associated bucket the very first line of the loop resolved bucket id -1 and
    // raised "Bucket with id '-1' was not found", aborting the whole FIX before ANY index was repaired (issue #5780).
    // The finding still reaches the operator through corruptedIndexes/warnings, which is all this pass can honestly
    // do about it.
    //
    // Filtered here rather than inside the `if (fix)` loop below so that rebuiltIndexes, which is reported in BOTH
    // modes, does not name an index no run would ever rebuild. The message is therefore phrased for both: it states
    // what this check does not cover, not what a repair attempt failed to do.
    affectedIndexes.removeIf(index -> {
      if (index.isAutomatic())
        return false;
      addWarning("index '" + index.getName() + "': it is a manual index. Its entries are not derived from any record, "
          + "so this check does not rebuild it - drop it and repopulate it to clear this finding");
      return true;
    });

    final Set<String> rebuildIndexes = affectedIndexes.stream().map(x -> x.getName()).collect(Collectors.toSet());
    result.put("rebuiltIndexes", rebuildIndexes);

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Rebuilding indexes %s...", null, rebuildIndexes);

    if (fix)
      stepBegin("Rebuilding indexes", affectedIndexes.size());

    if (fix)
      for (final Index idx : affectedIndexes) {
        // bucketName/indexType/unique/propNames/typeName/pageSize/nullStrategy are all read once here, before the
        // retry loop below, rather than recomputed per attempt the way RebuildIndexStatement.buildIndex() does.
        // That is safe: every one of them is a plain, idempotent read off idx/schema state fixed before the first
        // drop, with no mutable accumulator behind it - unlike rebuildMetadata just below, which for FULL_TEXT
        // wraps a live BM25 counter and for that reason IS recomputed per attempt.
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

        // Same defect class fixed in RebuildIndexStatement.buildIndex() for issue #5791/#4732: capture the owning
        // TypeIndex's name BEFORE the drop below, otherwise a single-bucket type (the common default) loses its
        // explicitly-named TypeIndex wrapper. Safe to read once, outside the retry loop below: it is a plain name
        // read off idx, unaffected by how many attempts the rebuild takes.
        final TypeIndex ownerTypeIndex = ((IndexInternal) idx).getTypeIndex();

        // Same file-locking coverage AND retry-on-contention as RebuildIndexStatement.buildIndex(): holding the
        // lock across both the drop and the create prevents a concurrent writer from observing the index
        // gone-then-back, and retrying survives tryLockFiles's LockTimeoutException (a NeedRetryException) on a
        // busy database.
        //
        // reconstructForRebuild(...) is called PER ATTEMPT, inside the loop, matching RebuildIndexStatement -
        // NOT hoisted above it like ownerTypeIndex. For FULL_TEXT it returns a fresh FullTextIndexMetadata with
        // its BM25 corpus counters zeroed; buildBucketIndex().create() mutates that exact instance in place as
        // it indexes each record. A single shared instance reused across attempts would still carry attempt N's
        // partial counts into attempt N+1's create() call, inflating totalDocs/sumDocLength (and so skewing BM25
        // relevance scoring) on any FULL_TEXT index whose rebuild needed more than one attempt.
        //
        // #6040: dropIndex() itself is safe to repeat (LocalSchema.dropIndexInternal no-ops once the name is
        // already gone), but a NeedRetryException raised deep inside create()'s bucket scan - AFTER this same
        // attempt's drop already committed - left every subsequent attempt retrying to recover from a
        // self-inflicted "index currently gone" state, not merely waiting out lock contention, while being
        // charged against the very same LOCK_MAX_ATTEMPTS budget as a lock timeout that touched nothing at all.
        // indexDropped (sticky once true) tracks that distinction; once set, the exhaustion check below applies
        // the larger LOCK_POST_DROP_ATTEMPT_MULTIPLIER budget instead of giving up at LOCK_MAX_ATTEMPTS.
        boolean rebuilt = false;
        boolean indexDropped = false;
        RuntimeException lastFailure = null;
        boolean lastFailureRetryable = true;
        int attemptsUsed = 0;
        for (int attempt = 1; ; attempt++) {
          attemptsUsed = attempt;
          // Effectively-final per iteration: set to true by the callback the instant its dropIndex() commits.
          final boolean[] droppedThisAttempt = new boolean[1];
          try {
            final IndexMetadata rebuildMetadata = IndexMetadata.reconstructForRebuild((IndexInternal) idx, typeName,
                propNames.toArray(new String[0]));
            database.executeLockingFiles(((IndexInternal) idx).getFileIds(), () -> {
              database.getSchema().dropIndex(idx.getName());
              droppedThisAttempt[0] = true;

              // Was missing entirely before #6040 (default maxAttempts=1, effectively no inner retry at all), unlike
              // RebuildIndexStatement's identically-shaped call. This lets create()'s own bucket-scan contention
              // resolve WITHOUT re-entering this outer loop, while the outer file lock from executeLockingFiles is
              // still held - so a single outer attempt can now take noticeably longer than before (up to
              // LOCK_MAX_ATTEMPTS internal retries with their own backoff) before it either succeeds or falls
              // through to the outer catch below.
              database.getSchema().buildBucketIndex(typeName, bucketName, propNames.toArray(new String[propNames.size()]))
                  .withType(indexType).withUnique(unique).withPageSize(pageSize).withNullStrategy(nullStrategy)
                  .withMetadata(rebuildMetadata)
                  .withIndexName(ownerTypeIndex != null ? ownerTypeIndex.getName() : null)
                  .withMaxAttempts(LOCK_MAX_ATTEMPTS)
                  .create();
              return null;
            });
            rebuilt = true;
            break;
          } catch (final NeedRetryException e) {
            lastFailure = e;
            lastFailureRetryable = true;
            if (droppedThisAttempt[0])
              indexDropped = true;

            final int budget = indexDropped ? LOCK_MAX_ATTEMPTS * LOCK_POST_DROP_ATTEMPT_MULTIPLIER : LOCK_MAX_ATTEMPTS;
            if (attempt >= budget)
              break;
            try {
              Thread.sleep(200 + 200L * attempt);
            } catch (final InterruptedException ie) {
              // An interrupt is a request to stop, not contention to ride out: honor it immediately rather than
              // trying the remaining indexes.
              Thread.currentThread().interrupt();
              throw e;
            }
          } catch (final RuntimeException e) {
            // NOT retryable, so it gets no attempts: a DuplicatedKeyException from a unique index whose bucket
            // already holds a violating record, a ReplicatedEntryTooLargeException on an HA leader whose rebuilt
            // index exceeds arcadedb.ha.appendBufferSize, an IndexException from a record the build cannot read.
            // Retrying reproduces it exactly.
            //
            // Caught at all - rather than left to unwind - for the reason the NeedRetryException arm above already
            // states and which applies with MORE force here: this loop runs after checkDocuments/checkVertices/
            // checkEdges have already found and repaired things, and none of that may be thrown away because ONE
            // index could not be rebuilt. Before this arm existed, any such failure escaped check() with that
            // attempt's dropIndex() already committed, so the single most destructive outcome (index gone, nothing
            // reported, remaining indexes never even attempted) was also the one with no handling at all.
            //
            // RuntimeException, not a narrower list: the set of ways a rebuild can fail is open (every index type
            // has its own, and the HA wrapper adds more), and the alternative to catching an unanticipated one is
            // destroying the run. Nothing is swallowed - the exception's own message is quoted in the warning
            // below and the index is struck from rebuiltIndexes.
            lastFailure = e;
            lastFailureRetryable = false;
            if (droppedThisAttempt[0])
              indexDropped = true;
            break;
          }
        }

        // Isolated per index rather than letting the failure unwind out of check(): this loop repairs potentially
        // many indexes plus whatever checkDocuments/checkVertices/checkEdges already found and fixed, and none of
        // that should be discarded because ONE index could not be rebuilt - whether it ran out of retry attempts
        // or failed outright. Report it (this class's "reported, never silent" convention - see the manual-index
        // warning above) and correct the rebuiltIndexes claim instead of pretending the rebuild happened.
        //
        // The warning's phrasing follows indexDropped (#6040) and retryability - see rebuildFailureStatus - and
        // quotes lastFailure.getMessage() so the operator can see the underlying cause in every case.
        if (!rebuilt) {
          rebuildIndexes.remove(idx.getName());
          // Named by the OWNING TypeIndex where there is one, because that is the name the operator created and
          // the only one they can act on: idx.getName() is the internal per-bucket sub-index ("Doc_0_<nanos>"),
          // which nobody typed and which makes the follow-up query below return nothing. The sub-index name is
          // still quoted when the two differ, since a multi-bucket type has several and this says which failed.
          final String reportedName = ownerTypeIndex != null ? ownerTypeIndex.getName() : idx.getName();
          final String bucketSubIndex = reportedName.equals(idx.getName()) ? "" : " (bucket sub-index '" + idx.getName() + "')";
          addWarning("index '" + reportedName + "'" + bucketSubIndex + " " + (lastFailureRetryable ?
              "did not finish rebuilding after " + attemptsUsed + " attempts" :
              "could not be rebuilt") + " (" + rebuildFailureStatus(indexDropped, lastFailureRetryable)
              + ", error: " + lastFailure.getMessage() + "). Verify with `SELECT FROM schema:indexes WHERE name = '"
              + reportedName + "'` and run CHECK DATABASE FIX again if it is missing");
        }

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

  /**
   * What state a failed index rebuild left behind, which is the only part of the warning an operator has to act
   * on: an index that is MISSING needs recreating, one that is unchanged needs nothing.
   * <p>
   * Both dimensions matter and neither implies the other. {@code indexDropped} says whether this attempt's
   * {@code dropIndex()} had already committed when it failed - #6040's distinction, and the difference between
   * "gone" and "merely unrebuilt". Retryability says whether it was worth attempting again, which is what decides
   * between the two ways of not being dropped: exhausting the lock-acquisition budget, or failing outright before
   * the drop.
   */
  private static String rebuildFailureStatus(final boolean indexDropped, final boolean retryable) {
    if (indexDropped)
      return "the index was dropped but the rebuild could not complete; it is now missing";
    return retryable ?
        "the index lock could not be acquired; the index itself is unchanged" :
        "the rebuild failed before the index was dropped; the index itself is unchanged";
  }

  /**
   * Unions one edge scan's ORPHAN EDGE RECORD RIDs into the cross-type list (issue #6090). Merged by hand rather
   * than through {@link #updateStats}, which only folds {@code Long} values - the same reason
   * {@code corruptedRecords} is merged here. The count beside it stays exact even when this set hit its cap.
   */
  private void mergeUnreachableEdgeRecords(final Map<String, Object> stats) {
    final Collection<RID> unreachable = (Collection<RID>) stats.get("unreachableEdgeRecordsFound");
    if (unreachable != null)
      ((LinkedHashSet<RID>) result.get("unreachableEdgeRecordsFound")).addAll(unreachable);
  }

  /**
   * Merges the records a sub-check actually deleted into {@code deletedRecordsAfterFix}.
   * <p>
   * Absent before, which made the field answer a different question depending on which pass did the removing: the
   * bucket-wide {@code LocalBucket.check} listed what it deleted, while the vertex and edge arms deleted silently
   * and reported only an {@code autoFix} count. The distinction was invisible while a broken-chain record was
   * always removed by the bucket pass; once the arms could remove it first, the same repair stopped listing the
   * same record. Reported by whichever pass performs it, or the field cannot be read at all.
   */
  private void mergeDeletedRecords(final Map<String, Object> stats) {
    final Collection<RID> deleted = (Collection<RID>) stats.get("deletedRecordsAfterFix");
    if (deleted != null)
      ((LinkedHashSet<RID>) result.get("deletedRecordsAfterFix")).addAll(deleted);
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
   *   <li>it called a document a "vertex", and claimed to be "removing it" while removing nothing;</li>
   *   <li>it kept no cap at all, so a type whose whole bucket is unreadable retained one message per record.</li>
   * </ul>
   * Both paths now go through {@link #addWarning}/{@link #addCorrupted}. The accumulators are also per-type now:
   * they used to be declared outside the loop and re-added to the result after every type, which the sets absorbed
   * but paid for in re-insertions proportional to the square of the type count.
   * <p>
   * The second of those was settled the OTHER way round afterwards, so do not restore the old wording from the
   * bullet above: under {@code FIX} this arm now really does remove the record, via
   * {@link #deleteCorruptedRecords}, matching the vertex and edge arms. Flagging without removing was not merely a
   * reporting quirk - {@code corruptedRecords} drives the index rebuild at the end of {@link #check()}, and that
   * rebuild rescans the bucket, so the record left behind destroyed the very index the flag asked to repair.
   */
  private void checkDocuments(final List<DocumentType> documentTypes) {
    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking documents...");

    for (final DocumentType type : documentTypes) {
      stepBegin("Checking documents '" + type.getName() + "'", database.countType(type.getName(), false));

      // Collected during the scan and deleted after it: LocalBucket.scan is walking the very pages the delete
      // would rewrite, so removing a record from inside the callback would mutate the structure being iterated.
      final List<RID> toDelete = new ArrayList<>();

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
              if (fix)
                toDelete.add(rid);
            }
            stepTick();
            return true;
          }, null);
        }

        deleteCorruptedRecords(toDelete);

      } finally {
        database.commit();
      }

      stepComplete();
    }
  }

  /**
   * Validates the storage of every TimeSeries type in scope (issue #6340).
   * <p>
   * <b>Why this pass exists.</b> Before it, {@code DatabaseChecker} contained no reference to TimeSeries at all -
   * not to {@code .tstb}, not to {@code .tstd}, not to {@code .ts.sealed}. The checker walks record buckets and
   * indexes; a TimeSeries type has neither, since {@code LocalTimeSeriesType} registers its shards with the schema
   * as FILES rather than as a type's record buckets and its compacted data does not go through the paginated layer
   * at all. So a type falling into the document arm had nothing to scan, and every one of the three on-disk
   * formats TimeSeries owns - the mutable bucket, the sealed store, the tag dictionary, the last two with their
   * own magics, headers and CRCs - was outside the reach of the only tool whose job is to find damage in them.
   * That was not an abstract gap: #6314 fixed a bug that wrote real rows to disk at the wrong stride, and nothing
   * in the engine could detect a file left in that state, because the header a mismatched session also wrote
   * counts rows the pages no longer hold.
   * <p>
   * <b>{@code FIX} repairs derived bookkeeping and never a sample</b> (issue #6360 item 1, which asked exactly
   * this). Three things here are DERIVED - the mutable bucket's page-0 counters, the sealed store's header block
   * count and global timestamp bounds, and the tail of an interrupted sealed append - and every one of them is
   * recomputable from, or invisible to, the data it describes. Rewriting them discards nothing, and leaving them
   * wrong is not cosmetic: {@code TimeSeriesSealedStore.loadDirectory} reads the global bounds straight out of the
   * header instead of recomputing them, so a query pruned against a wrong bound silently misses data the file
   * holds, and {@code appendBlock} writes at the END of the sealed file, so a tail nothing can read makes every
   * block appended after it unreadable too. Even there the repair is narrow: only a tail that STARTS with a block
   * magic is dropped, because that one is incomplete by its own evidence, while one that does not could be a
   * complete block whose magic took a bit flip.
   * <p>
   * Everything else is reported and left alone, and that is the answer rather than a deferral. A record bucket can
   * be repaired because its records are self-describing and its indexes are derivable from them; a sealed block is
   * append-only columnar data and the ONLY copy of the samples in it, so "repair" there means choosing which
   * samples to throw away. Under HA a sealed store is derived and a node can rebuild one by recompacting from its
   * replicated mutable pages - which makes discarding one recoverable, not automatic. That is an operator's
   * decision with the operator's knowledge of the cluster behind it, and a checker that made it for them would be
   * deleting data to make its own report come out clean.
   * <p>
   * <b>Scoped by TYPE and by nothing else.</b> The type filter has already been applied by the caller's
   * classification, so naming a type checks that type's files and nothing else, exactly as it does for a document
   * type. A BUCKET scope does not narrow this pass and cannot: a TimeSeries shard is not a bucket, so there is
   * nothing for a bucket name to select - the same reason {@link #checkIndexes} is not narrowed by BUCKET either.
   * A RECORD scope never reaches here, since that branch skips every database-wide pass.
   * <p>
   * ONE step for all types rather than one per type, so a database with no TimeSeries type keeps the step plan it
   * had. The per-type progress is the tick.
   */
  private void checkTimeSeries(final List<LocalTimeSeriesType> timeSeriesTypes) {
    if (timeSeriesTypes.isEmpty())
      return;

    if (verboseLevel > 0)
      LogManager.instance().log(this, Level.INFO, "Checking TimeSeries types...");

    stepBegin("Checking TimeSeries", timeSeriesTypes.size());

    final TimeSeriesIntegrity.Options options = TimeSeriesIntegrity.Options.of(deep, fix);
    final Set<String> corrupted = new LinkedHashSet<>();
    long totalShards = 0;
    long totalSamples = 0;
    long totalSealedBlocks = 0;
    long totalRepairs = 0;

    for (final LocalTimeSeriesType type : timeSeriesTypes) {
      stepTick();

      final TimeSeriesEngine engine = type.getEngine();
      if (engine == null) {
        // The type is in the schema but its engine never initialised, so its files were never opened. Reported
        // rather than skipped: a type nothing can read is exactly the kind of thing this check is asked about.
        // The reason (when known, issue #6356) names the file that failed to load, so an operator does not have
        // to go hunting through the server log for it.
        final String reason = type.getEngineUnavailableReason();
        addWarning("timeseries '" + type.getName() + "': the storage engine is not initialised, so none of its "
            + "files could be read" + (reason != null ? ": " + reason : ""));
        corrupted.add(type.getName());
        continue;
      }

      try {
        // ONE walk over the type's shards, not three: the report carries the totals this pass publishes alongside
        // the findings, and the totals are read inside the SAME lock window each shard's mutable-half check
        // already runs in - so the numbers describe one instant, not a second and third walk over the same
        // shards. The sealed-half findings do not share that window (issue #6406 item 1: they are fanned out
        // across the type's own pool, after every shard's mutable half has already run) - see
        // TimeSeriesEngine.checkIntegrity's own javadoc for what that trades away and why it is still safe.
        final TimeSeriesEngine.IntegrityReport report = engine.checkIntegrity(options);

        totalShards += report.shards();
        totalSamples += report.samples();
        totalSealedBlocks += report.sealedBlocks();

        if (!report.problems().isEmpty()) {
          corrupted.add(type.getName());
          for (final String problem : report.problems())
            addWarning("timeseries '" + type.getName() + "': " + problem);
        }

        // Kept out of autoFix, which is the RECORD action count (#6136) and has never included anything that is
        // not a removed record or a pruned index entry. A header rewritten from the pages it describes is neither.
        totalRepairs += report.repairs().size();
        for (final String repair : report.repairs())
          ((LinkedHashSet<String>) result.get("timeSeriesRepairs")).add("timeseries '" + type.getName() + "': " + repair);
      } catch (final Exception e) {
        // Same shape as the index arm: a check that cannot complete is itself a finding, never a reason to
        // abandon the rest of the run.
        addWarning("timeseries '" + type.getName() + "': integrity check failed: " + e.getMessage());
        corrupted.add(type.getName());
      }
    }

    ((LinkedHashSet<String>) result.get("corruptedTimeSeries")).addAll(corrupted);
    result.put("totalTimeSeriesTypes", (long) timeSeriesTypes.size());
    result.put("totalTimeSeriesShards", totalShards);
    result.put("totalTimeSeriesSamples", totalSamples);
    result.put("totalTimeSeriesSealedBlocks", totalSealedBlocks);
    result.put("repairedTimeSeries", totalRepairs);

    stepComplete();
  }

  /**
   * Removes the records this pass flagged corrupted, counting them into {@code autoFix} and {@code removedRecords}.
   * <p>
   * Counts a repair only once the delete and its counter delta have both succeeded, and counts nothing for a record
   * that was already gone. {@code GraphDatabaseChecker}'s vertex and edge arms used to differ here - they counted
   * BEFORE attempting the delete, so a record they then failed to remove was still reported as fixed - and #6128
   * aligned them on this one: {@code autoFix} is what an operator reads to decide whether the run did anything, and
   * a number that includes failed deletes cannot answer that.
   * <p>
   * This is not merely tidiness, and it is not optional. A corrupted record's RID goes into
   * {@code corruptedRecords}, which is what {@link #check()} turns into {@code affectedBuckets}, and every index on
   * an affected bucket is then dropped and rebuilt. The rebuild's own bucket scan re-reads every record in it, so a
   * corrupt record LEFT IN PLACE is met a second time by {@code LSMTreeIndex.build}, whose error callback
   * propagates rather than logging (deliberately - a swallowed failure would leave the index silently incomplete).
   * That exception escaped {@link #check()} with the drop already committed, which destroyed the index and
   * abandoned the rest of the run. The document arms used to flag without deleting and were the only way to reach
   * that state; {@link #rebuildFailureStatus} now contains the damage if a rebuild fails for some other reason,
   * but not creating the state in the first place is the actual fix.
   * <p>
   * Deletes through the bucket rather than {@code database.deleteRecord}: the record cannot be materialised - that
   * is what "corrupted" means here - so there is nothing to hand the record-level API, and its index entries are
   * about to be regenerated from scratch by the rebuild anyway. Same call the graph arms use.
   */
  private void deleteCorruptedRecords(final List<RID> toDelete) {
    for (final RID rid : toDelete) {
      try {
        final Bucket bucket = database.getSchema().getBucketById(rid.getBucketId());
        // See GraphDatabaseChecker.deleteCorruptedRecord: escalates to a force delete for the one failure force
        // exists to clear, a structurally broken chunk chain, which a plain delete reports as a retry signal.
        if (bucket instanceof LocalBucket localBucket)
          localBucket.deleteCorruptedRecord(rid);
        else
          bucket.deleteRecord(rid);
        // Mirror the accounting in LocalDatabase.cascadeDeleteExternalValues (and in checkExternalProperties just
        // below) so count() stays consistent: LocalBucket.deleteRecord does NOT touch cachedRecordCount, and
        // count(*) reads that counter rather than scanning. Without this the type keeps reporting the deleted
        // record until something else recomputes the counter - which a full-scope run happens to do in
        // checkBuckets, but the RECORD scope deliberately skips, so the same repair would report two different
        // counts depending on the scope it was asked for.
        database.getTransaction().updateBucketRecordDelta(rid.getBucketId(), -1);
        result.put("autoFix", (Long) result.get("autoFix") + 1);
        // The per-kind arm of the same repair (issue #6136, item 3): this arm only ever deletes, so its whole
        // contribution to autoFix is a deleted record.
        result.put("removedRecords", (Long) result.get("removedRecords") + 1);
        // Same reason as mergeDeletedRecords: a removal nobody lists cannot be audited.
        ((LinkedHashSet<RID>) result.get("deletedRecordsAfterFix")).add(rid);
      } catch (final RecordNotFoundException e) {
        // ALREADY GONE
      } catch (final Exception e) {
        addWarning("document " + rid + " cannot be removed (error: " + e.getMessage() + ")");
      }
    }
  }

  public void compress() {
    if (database.isTransactionActive())
      database.rollback();

    long totalPagesToCompress = 0;
    for (final Bucket b : database.getSchema().getBuckets())
      totalPagesToCompress += ((LocalBucket) b).getTotalPages();
    stepBegin("Compressing buckets", totalPagesToCompress);

    // Issue #6128 (4): the hardcoded 10 made COMPRESS unusable on a replicated database. Every commit is a Raft
    // consensus round trip, so ten pages per transaction means one round trip per ten pages - millions of them on a
    // database of any size, which is not a slow operation but one that does not finish. Reusing the repair budget
    // rather than adding a second knob: it answers the same question ("how many pages may one CHECK DATABASE
    // transaction accumulate"), and at its 256 default this is a 25x reduction in round trips while staying well
    // under the appendBufferSize the entry has to fit in. Guarded so a 0 (batching disabled) does not turn into a
    // single transaction over every page in the database, which is the one outcome nobody wants here.
    final int budget = database.getConfiguration()
        .getValueAsInteger(GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES);
    final int pageTxBatch = budget > 0 ? budget : 10;
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
        stats = graphChecker.checkEdges(type.getName(), rids, fix, deleteOrphanEdgeRecords, verboseLevel,
            Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));
      else
        stats = graphChecker.checkVertices(type.getName(), rids, fix, verboseLevel,
            Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));

      updateStats(stats);
      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      mergeUnreachableEdgeRecords(stats);
      mergeDeletedRecords(stats);
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

    final List<RID> toDelete = new ArrayList<>();

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
          addWarning("document " + rid + " cannot be loaded");
          addCorrupted(rid);
          if (fix)
            toDelete.add(rid);
        }
        stepTick();
      }

      // Same removal as the type-wide arm, for the same reason: this scope also feeds affectedBuckets, so a
      // corrupt record left here would be met again by the rebuild that its own flagging triggers.
      deleteCorruptedRecords(toDelete);

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
          .checkEdges(type.getName(), null, fix, deleteOrphanEdgeRecords, verboseLevel,
              Math.max(0, maxWarnings - currentWarnings), Math.max(0, maxWarnings - currentCorrupted));

      updateStats(stats);

      ((LinkedHashSet<String>) result.get("warnings")).addAll((Collection<String>) stats.get("warnings"));
      ((LinkedHashSet<RID>) result.get("corruptedRecords")).addAll((Collection<RID>) stats.get("corruptedRecords"));
      mergeUnreachableEdgeRecords(stats);
      mergeDeletedRecords(stats);
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
      mergeDeletedRecords(stats);
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

  /**
   * Opt-in for the checks that have to DECODE the data rather than reconcile what describes it - {@code CHECK
   * DATABASE ... DEEP} (issue #6360).
   * <p>
   * Today only the TimeSeries pass has a tier above its default, and what that tier adds is not more of the same:
   * the default already reads every byte of every sealed store to verify its per-block CRC32s, which proves the
   * bytes are the bytes that were written and proves nothing about whether they mean what the block claims. DEEP
   * decompresses each block and reconciles it against the three claims read paths answer queries from without ever
   * looking at the values - sorted timestamps, per-column min/max/sum, and the distinct tag values block pruning
   * skips on. Each of those, when wrong, produces a wrong ANSWER rather than an error.
   * <p>
   * It is a clause of its own rather than the default because it is the one part of the check whose cost is
   * decompression of the whole dataset rather than a sequential read of it, and because everything it can find is
   * a bug in a writer rather than damage a disk did - the class of thing an operator goes looking for, not the
   * class they run a scheduled check for.
   */
  public DatabaseChecker setDeep(final boolean deep) {
    this.deep = deep;
    return this;
  }

  /**
   * Opt-in for reclaiming ORPHAN EDGE RECORDS (issue #6090): edge records no vertex's adjacency list references.
   * Only meaningful together with {@link #setFix(boolean)}, which performs the removal.
   * <p>
   * SEPARATE FROM {@code FIX} ON PURPOSE, and the reason is a data-loss path rather than a taste for options. The
   * detection cannot distinguish "this record is garbage a failed load left behind" from "this vertex lost its
   * head-chunk pointer, so its perfectly good edges look unreferenced" - a null head chunk reads exactly like a
   * vertex with no edges. In the second case the edge records are the ONLY surviving description of that
   * adjacency and the thing {@code RESTORE VERTEX} rebuilds it from, so a repair that deletes them by default
   * would turn a recoverable database into an unrecoverable one. Reporting is therefore always on; removing is
   * always asked for.
   */
  public DatabaseChecker setDeleteOrphanEdgeRecords(final boolean deleteOrphanEdgeRecords) {
    this.deleteOrphanEdgeRecords = deleteOrphanEdgeRecords;
    return this;
  }

  /**
   * Opt-in for reclaiming unreferenced files (issue #6189): the operator-triggered alternative to the automatic,
   * inferred reclaim that issue #6189 explicitly did NOT take (see that issue for the full argument against it).
   * Only meaningful together with {@link #setFix(boolean)}, which performs the removal.
   * <p>
   * Deletes exactly the files {@link UnreferencedFiles#scan} already proves under
   * {@link UnreferencedFiles.Kind#NO_SCHEMA_COMPONENT} - nothing in the schema names them, so a raw file delete
   * can never leave a dangling schema reference. The other two shapes {@code UnreferencedFiles} can prove,
   * {@link UnreferencedFiles.Kind#UNOWNED_BUCKET} and {@link UnreferencedFiles.Kind#UNOWNED_INDEX}, are registered
   * schema components; reclaiming those safely means unregistering the component, not only deleting its file, and
   * stays a manual {@code DROP BUCKET} / index-tooling operation the finding's reason already names.
   * <p>
   * SEPARATE FROM {@code FIX} ON PURPOSE, for the same reason {@link #setDeleteOrphanEdgeRecords} is: reporting a
   * finding is always safe, deleting is a step an operator has to ask for explicitly, on the node they chose to run
   * this on. This runs against whatever {@link DatabaseInternal} this checker was constructed with - it does not
   * itself target a specific node of a cluster.
   */
  public DatabaseChecker setReclaimUnreferencedFiles(final boolean reclaimUnreferencedFiles) {
    this.reclaimUnreferencedFiles = reclaimUnreferencedFiles;
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

  /**
   * Reports the paginated files this node holds that no schema component claims (issue #6143), and - only when both
   * {@link #fix} and {@link #reclaimUnreferencedFiles} were asked for - deletes the ones it can prove safe to delete
   * (issue #6189).
   * <p>
   * The finding is always logged (verbosity permitting) and recorded in the {@code unreferencedFiles} result key
   * BEFORE any deletion runs, so an operator who reads the log or the result sees what was found before what was
   * removed - "the finding printed first" is a code-order guarantee here, not merely a documentation claim.
   * <p>
   * REPORTED AS ITS OWN RESULT KEY, NOT AS A WARNING, and the distinction is not cosmetic. A warning here means the
   * data is suspect; an unreferenced file is not a defect in the data at all - nothing is corrupt, nothing is lost,
   * and the state is one a supported operation produces (a bucket created with CREATE BUCKET and not yet given to a
   * type is exactly this shape, and so is the file left by an index construction that refused its own arguments).
   * Folding it into {@code warnings} would also redefine what a clean database is for every caller that treats an
   * empty warning list as the definition, {@code TestHelper.checkDatabaseIntegrity} among them.
   * <p>
   * The principal producer is a replicated schema session that shipped instalments and then lost leadership: it can
   * no longer submit the compensating removal, so the files its instalments created stay on the other nodes with
   * nothing referencing them. Only the node that ran the session logs anything about it, and this check runs on the
   * LEADER for a replicated database, so a clean result here does not certify the followers - the same caveat
   * {@code checkedNodeScope} already carries for the rest of the run. Each node also publishes its own count as the
   * {@code arcadedb.ha.schema.unreferenced_files} gauge, which is how an operator finds the node that holds them.
   * <p>
   * NOT LIMITED BY THE TYPE/BUCKET SCOPE, unlike everything else in this branch, and it cannot be: "no schema
   * component claims this file" is a property of the whole schema, not of a type, so narrowing it would mean
   * answering a different question - and the answer would be wrong, since a type that does not claim the file says
   * nothing about whether another one does. A scoped run therefore reports findings from outside its scope, which
   * the warning says. Same shape as the COMPRESS caveat above, and the reason both are stated rather than left to
   * surprise someone.
   * <p>
   * The SCAN always runs, and only the log line honours {@code verboseLevel}: the result key has to be truthful on
   * every run, because a caller reading {@code unreferencedFiles} as empty must be able to conclude there are none
   * rather than that the run was quiet. It is in-memory with no I/O, so running it unconditionally costs nothing
   * worth gating.
   * <p>
   * No progress step: it reads in-memory registries with no I/O, so it is over before a poller could observe it,
   * and giving it one would change the step plan every existing progress test asserts.
   */
  private void checkUnreferencedFiles() {
    final List<UnreferencedFiles.UnreferencedFile> unreferenced = UnreferencedFiles.scan(database);
    if (unreferenced.isEmpty())
      return;

    final LinkedHashSet<String> reported = (LinkedHashSet<String>) result.get("unreferencedFiles");
    for (final UnreferencedFiles.UnreferencedFile file : unreferenced)
      reported.add(file.toString());

    final boolean reclaiming = fix && reclaimUnreferencedFiles;

    if (verboseLevel >= 1) {
      // The log line names at most this many. The full list is always in the result key, which is a collection a
      // caller can read; embedding all of it here would put an unbounded line in the log.
      final StringBuilder names = new StringBuilder();
      for (int i = 0; i < unreferenced.size() && i < LOGGED_UNREFERENCED_FILES; i++) {
        if (i > 0)
          names.append(", ");
        names.append(unreferenced.get(i).fileName());
      }
      if (unreferenced.size() > LOGGED_UNREFERENCED_FILES)
        names.append(" and ").append(unreferenced.size() - LOGGED_UNREFERENCED_FILES)
            .append(" more (see the 'unreferencedFiles' result)");

      LogManager.instance().log(this, Level.INFO,
          "%d file(s) on this node are referenced by no schema component (this pass covers the whole database, "
              + "whatever TYPE or BUCKET scope was asked for: a file nothing claims is a property of the schema, "
              + "not of a type): %s. They are inert - no query, index or replication path reads a file the schema "
              + "does not reference - so this costs disk only. %s", null, unreferenced.size(), names,
          reclaiming ?
              "CHECK DATABASE FIX RECLAIM UNREFERENCED FILES was asked for, so the ones this node can prove safe to "
                  + "delete are being reclaimed now" :
              "This check does not remove them; re-run as CHECK DATABASE FIX RECLAIM UNREFERENCED FILES to reclaim "
                  + "the ones it can prove safe to delete, or remove them by hand with the server stopped, after a "
                  + "backup");
    }

    // Logged/recorded above FIRST, deletion (if any) only after: an operator reading the log or the result sees
    // what was found before what was removed.
    if (reclaiming)
      reclaimUnreferencedFiles(unreferenced);
  }

  /**
   * Deletes the {@link UnreferencedFiles.Kind#NO_SCHEMA_COMPONENT} findings from {@code unreferenced} - the only
   * shape a raw file delete can never leave a dangling schema reference behind, because nothing in the schema
   * names it (issue #6189). {@code UNOWNED_BUCKET} and {@code UNOWNED_INDEX} findings are left untouched; see
   * {@link #setReclaimUnreferencedFiles(boolean)} for why.
   * <p>
   * One file's failure does not abort the rest: {@link FileManager#dropFile} can fail independently per file, and a
   * caller reclaiming ten files wants the nine that worked, not none of them because the tenth's I/O failed.
   * <p>
   * {@code unreferenced} is a snapshot {@link UnreferencedFiles#scan} took with no lock, on a database that stays
   * open and writable for the whole of this method: the exact shape this reclaims, "no schema component was ever
   * built for it", is also what a legitimate, still-in-progress instalment sequence looks like before it publishes
   * (see the class doc). So EVERY file is re-verified right here, immediately before its own {@code dropFile} call,
   * rather than trusted from the snapshot - a schema reload landing between the scan and this file's turn in the
   * loop (which the I/O of dropping earlier files in the list only gives more time to) would otherwise mean
   * deleting a file that is, by now, backing a live component. The re-check is one map lookup, so paying it per
   * file costs nothing worth avoiding. It cannot close the window all the way to zero - a reload landing in the
   * gap between this check and the {@code dropFile} two lines below is still possible - only a lock shared with
   * the schema-attach path could do that, which is a separate, larger change this method does not make.
   * <p>
   * Package-private rather than private: the seam a test needs to drive this exact race deterministically, by
   * handing it a finding whose fileId has since gained a real component, instead of only documenting the residual
   * window in a comment (see {@code Issue6189ReclaimUnreferencedFilesTest}).
   * <p>
   * The "is it still gone from the schema" check and the "actually drop it" step are two separate calls rather
   * than one, and cannot be otherwise: unregistering a schema component and removing a file are different
   * subsystems with no shared lock to make them atomic together, which is exactly why the window between them is
   * the one residual this method documents rather than closes. The "is it already gone from the FILE MANAGER"
   * check does NOT have that excuse - both the check and the drop are one subsystem - so it is not a separate call
   * at all: {@link FileManager#dropFile} reports whether it found anything to remove, atomically with removing it
   * (issue #6189 review, second round), which is what {@code reclaimed} below trusts instead of a same-effect but
   * racy {@code existsFile} call made before it.
   */
  void reclaimUnreferencedFiles(final List<UnreferencedFiles.UnreferencedFile> unreferenced) {
    final LinkedHashSet<String> reclaimed = (LinkedHashSet<String>) result.get("reclaimedUnreferencedFiles");
    for (final UnreferencedFiles.UnreferencedFile file : unreferenced) {
      if (file.kind() != UnreferencedFiles.Kind.NO_SCHEMA_COMPONENT)
        continue;

      if (database.getSchema().getEmbedded().getFileByIdIfExists(file.fileId()) != null) {
        // A schema component now claims this file id - it stopped being reclaimable sometime after the scan. Skip
        // it rather than delete out from under whatever just attached it; the next CHECK DATABASE run reports its
        // current state.
        addWarning("skipped reclaiming " + file + ": a schema component was attached to it after it was found, so "
            + "it is no longer safe to delete");
        continue;
      }

      try {
        if (!database.getFileManager().dropFile(file.fileId()))
          // Already gone by the time this file's turn came up - e.g. a second RECLAIM run in flight at once, or the
          // file was dropped some other way. dropFile() reports this atomically with checking, so - unlike the
          // schema race above - there is no window left to warn about: whatever removed it already achieved the
          // end state this call wanted, so this is a silent skip, not a warning.
          continue;

        reclaimed.add(file.toString());
        if (verboseLevel >= 1)
          LogManager.instance().log(this, Level.INFO, "reclaimed unreferenced file %s", null, file);
      } catch (final IOException e) {
        addWarning("failed to reclaim unreferenced file " + file + ": " + e.getMessage());
      }
    }
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

      // #6320: the transaction the repairs are made in belongs to check() itself now - it batches its commits, so it
      // cannot be handed one that might be somebody else's. It used to be begun here, and only when no transaction was
      // already open, which is the one case where batching would have had to be off: through HTTP a command always
      // arrives with one open, so the bucket repairs of a production CHECK DATABASE FIX would never have been bounded.
      final Map<String, Object> stats = bucket.check(verboseLevel, fix);

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
