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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.logging.Level;

/**
 * Computes the facets of a query over its {@link MatchSet} from the columnar projection (spec 7). The snapshot of every bucket of the
 * match set the user may read is acquired once, up front, so every facet of the query reads the same state (D20); every snapshot this
 * executor acquired is closed before it returns, on every path. A bucket the user may not read is not counted at all.
 * <p>
 * Per facet the buckets are grouped by declaring type (the type of the COLUMNAR index of the property, one per hierarchy, so a
 * subtype's buckets join its supertype's); each group is counted by one {@link FacetCollector} over the group's
 * {@link CrossBucketOrdinalMap}, and the collectors of several groups are merged by value.
 * <p>
 * Fallback scan (spec 7.8, D14): a bucket the projection cannot answer for a facet (no projection, a projection closed or dropped
 * meanwhile, an unusable snapshot, a column the snapshot does not publish with the schema's current encoding, a segment view without
 * the column holding a match, no COLUMNAR index on the property, an expression facet, a type whose records fire read events, see
 * {@link FacetFallbackScanner#firesReadEvents}: the scan reads them as any read does, so a record a trigger hides counts nowhere and a
 * record a listener rewrites counts as rewritten) is counted from its matched records by
 * {@link FacetFallbackScanner}, by collectors of their own (one per declaring type of the column, one for raw values) merged with the
 * others by value. The answer stays exact and the facet's source is {@code scan}. A query that scans takes one permit of
 * {@link ColumnarIndexManager#fallbackScans()} without waiting; refused, every facet that needed a scan is {@code unavailable} with the
 * reason {@code fallback scans capped}, and the others are counted as usual. Granted, the query counts once in
 * {@code query.fallback.scans} under the reason of its first scanned facet and bucket ({@link ColumnarStats#FALLBACK_SCAN_REASONS}),
 * and each scanned facet and bucket is logged at FINE with its reason.
 * <p>
 * Patching (spec 7.3, D10, D11): the matches a snapshot cannot answer for, and those the caller's open transaction wrote, are read
 * from their records by {@link FacetPatcher}, once per record for every facet (a match no facet counts from the projection is not read).
 * Above {@code max(arcadedb.columnar.patchBudget, arcadedb.columnar.patchBudgetRatio x matches)} positions in all, the matches being
 * those of the buckets patched, nothing is patched and the answer is not exact, except for the buckets the caller's transaction wrote:
 * they are scanned for every facet. The fallback scan and the patch loop check the query's deadline and termination every few records.
 * <p>
 * Memory (D13): besides the counters, every bitmap the query derives from the match set is charged to {@link FacetMemoryBudget} before
 * it is allocated: each bucket's patch set, and per bucket the matches a facet counts in the segments (a FILTER facet's own matches,
 * the matches minus the patch set). Those are made for one bucket and one facet at a time and dropped once the facet counted the
 * bucket (a histogram keeps them until it finishes); a refused charge makes that facet unavailable ({@code memory budget}), or, for a
 * patch set, the answer not exact.
 * <p>
 * Waiting for counting memory is bounded per collector (one {@code arcadedb.columnar.queryBudgetWait} window, see
 * {@link CollectorReservations}), and a facet that spans N declaring types has N collectors: it can wait up to N windows in all.
 * Every reservation that waited, even one refused afterwards, is counted in {@code query.budget.waits}.
 */
public final class FacetExecutor {
  /** The {@code source} of a facet a bucket of which was read from its records. */
  static final String SCAN                  = "scan";
  static final String FALLBACK_SCANS_CAPPED = "fallback scans capped";

  /** By facet and bucket: the projection answers the facet in the bucket; any other value is {@link #scanReason}. */
  private static final byte     ANSWERS      = 0;
  /** The FINE log text of each {@link ColumnarStats#FALLBACK_SCAN_REASONS} reason, by reason index. */
  private static final String[] REASON_TEXTS = { "expression facet", "no projection", "no COLUMNAR index",
      "column not published (create or schema window)", "column encoding changed", "segment without the column", "read events",
      "own writes over patch budget" };

  private FacetExecutor() {
  }

  /** {@link #execute(DatabaseInternal, MatchSet, List, CommandContext, DocumentType)} without the type the query selects from. */
  public static FacetResults execute(final DatabaseInternal db, final MatchSet matches, final List<FacetSpec> facets,
      final CommandContext ctx) {
    return execute(db, matches, facets, ctx, null);
  }

  /**
   * @param matches the matched records, owned by the caller; {@link MatchSet#setFacetFilterIndex} was called with one entry per facet
   * @param ctx     the context of the query: its deadline and termination bound the records read
   * @param target  the type the query selects from, null when unknown. A facet that counts no bucket (an empty match set) still
   *                converts its literals (BOUNDS, range bounds, INCLUDE) with the column of the property's declaring type in it, exactly as
   *                a facet that counts one does; without it, or without a COLUMNAR index on the property there, a number is taken as is
   *                and a date as epoch milliseconds
   */
  public static FacetResults execute(final DatabaseInternal db, final MatchSet matches, final List<FacetSpec> facets,
      final CommandContext ctx, final DocumentType target) {
    final ColumnarIndexManager manager = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager();
    if (matches.budgetWaits() > 0)
      manager.getStats().queryBudgetWaits(matches.budgetWaits());

    final LinkedHashMap<String, FacetResult> results = new LinkedHashMap<>();
    final LinkedHashMap<String, FacetExecutionStats> stats = new LinkedHashMap<>();
    if (matches.isOverBudget()) {
      for (final FacetSpec spec : facets) {
        // refused before any count: the spec is still validated, an unsupported kind is an error and not a budget answer
        collectorFor(spec, null, null, 0).release();
        results.put(spec.alias(), FacetResult.unavailable(kindName(spec), TermsCollector.MEMORY_BUDGET));
      }
      return new FacetResults(true, 0, 0, results);
    }

    final int[] bucketIds = readableBuckets(db, matches.bucketIds());
    final ColumnarSnapshot[] snapshots = new ColumnarSnapshot[bucketIds.length];
    final boolean[] acquired = new boolean[bucketIds.length];
    final CollectorReservations patchCharges = new CollectorReservations(GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong());
    Throwable failure = null;
    try {
      final long now = System.currentTimeMillis();
      long lagMs = 0;
      for (int i = 0; i < bucketIds.length; i++) {
        snapshots[i] = matches.snapshot(bucketIds[i]);
        if (snapshots[i] == null) {
          snapshots[i] = acquire(manager, bucketIds[i]);
          acquired[i] = snapshots[i] != null;
        }
        if (snapshots[i] != null && snapshots[i].oldestPendingEpochStartMs() > 0)
          lagMs = Math.max(lagMs, lagOf(now, snapshots[i].oldestPendingEpochStartMs()));
      }

      // a bucket whose records fire read events answers no facet from the projection (final review B-I1), decided for this execution
      final boolean[] readEvents = new boolean[bucketIds.length];
      for (int i = 0; i < bucketIds.length; i++)
        readEvents[i] = FacetFallbackScanner.firesReadEvents(db, db.getSchema().getTypeByBucketId(bucketIds[i]));

      // by facet, the buckets the projection answers (ANSWERS) and why the others are read from their records; by bucket, whether it
      // answers any facet (only those are patched)
      final byte[][] reasons = new byte[facets.size()][bucketIds.length];
      final boolean[] answered = new boolean[bucketIds.length];
      for (int f = 0; f < reasons.length; f++)
        for (int i = 0; i < bucketIds.length; i++) {
          reasons[f][i] = readEvents[i] ? scanReason(ColumnarStats.FALLBACK_READ_EVENTS) :
              projectionReason(db, matches, f, facets.get(f), bucketIds[i], snapshots[i]);
          answered[i] |= reasons[f][i] == ANSWERS;
        }

      final PositionBitmap[] patches = new PositionBitmap[bucketIds.length];
      final boolean[] ownScanned = new boolean[bucketIds.length];
      final boolean exact = planPatches(db, manager, matches, bucketIds, snapshots, reasons, answered, patches, ownScanned, patchCharges);

      // a bucket the caller's transaction wrote over the patch budget is read from its records for every facet
      for (int i = 0; i < bucketIds.length; i++)
        if (ownScanned[i])
          for (final byte[] facetReasons : reasons)
            if (facetReasons[i] == ANSWERS)
              facetReasons[i] = scanReason(ColumnarStats.FALLBACK_OWN_WRITES);

      final long patched = count(db, manager, matches, facets, ctx, target, bucketIds, snapshots, patches, reasons, results, stats);
      if (patched > 0)
        manager.getStats().queryPatched(patched);
      final FacetResults facetResults = new FacetResults(exact, patched, lagMs, results);
      facetResults.setExecutionStats(stats);
      return facetResults;
    } catch (final RuntimeException | Error e) {
      failure = e;
      throw e;
    } finally {
      if (patchCharges.waits() > 0)
        manager.getStats().queryBudgetWaits(patchCharges.waits());
      patchCharges.release();
      closeAcquired(snapshots, acquired, failure);
    }
  }

  /** Age of a pending epoch, in ms: at least 1, an epoch that is pending is never 0 ms old in meaning, even read in its own millisecond (spec 8.3). */
  static long lagOf(final long nowMs, final long oldestPendingEpochStartMs) {
    return Math.max(1L, nowMs - oldestPendingEpochStartMs);
  }

  /**
   * Fills the patch set of each bucket the projection answers a facet in (D11, plus the records the caller's open transaction wrote),
   * restricted to the matches a facet the projection answers there counts (final review A-M1). The sizes are counted first, allocating
   * nothing; when their sum exceeds {@code max(arcadedb.columnar.patchBudget, arcadedb.columnar.patchBudgetRatio x matches)}, the
   * matches being those of the buckets that need a patch set, nothing is patched (D10): a bucket the caller's transaction wrote is then
   * scanned for every facet instead ({@code scanned}, no patch set), any other is counted from the projection alone. Under the budget
   * each patch set is charged to the memory budget before it is made (with the bitmap of the transaction's writes it holds); a refused
   * charge drops every patch set the same way, except that it is no patch overflow.
   *
   * @param reasons  by facet and bucket, {@link #ANSWERS} when the projection answers the facet in the bucket
   * @param answered by bucket, whether the projection answers any facet in it; a bucket every facet scans needs no patch set
   *
   * @return false when a bucket that needed patching is counted from the projection alone (the answer is not exact)
   */
  private static boolean planPatches(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int[] bucketIds, final ColumnarSnapshot[] snapshots, final byte[][] reasons, final boolean[] answered,
      final PositionBitmap[] patches, final boolean[] scanned, final CollectorReservations charges) {
    final PositionBitmap[] own = FacetPatcher.ownWrites(db, matches, bucketIds);
    final long[] sizes = new long[bucketIds.length];
    final LongPredicate[] counted = new LongPredicate[bucketIds.length];
    final boolean[] answering = new boolean[reasons.length];
    long total = 0;
    long patchedMatches = 0;
    for (int i = 0; i < bucketIds.length; i++) {
      if (snapshots[i] == null || !answered[i])
        continue;
      for (int f = 0; f < answering.length; f++)
        answering[f] = reasons[f][i] == ANSWERS;
      counted[i] = matches.countedBy(bucketIds[i], answering);
      sizes[i] = FacetPatcher.patchSetSize(matches, snapshots[i], bucketIds[i], counted[i], own != null ? own[i] : null);
      total += sizes[i];
      patchedMatches += matches.forBucket(bucketIds[i]).cardinality();
    }
    final long budget = Math.max(GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValueAsLong(),
        (long) (GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValueAsDouble() * patchedMatches));
    boolean refused = false;
    if (total <= budget)
      for (int i = 0; i < bucketIds.length; i++) {
        if (snapshots[i] == null || !answered[i])
          continue;
        if (sizes[i] == 0) {
          // caught up (the usual case): nothing to charge or read
          patches[i] = new PositionBitmap(snapshots[i].pendingPositions().maxRecordsInPage());
          continue;
        }
        final long ownBytes = own != null && own[i] != null ? FacetPatcher.patchSetBytes(matches, bucketIds[i], own[i].cardinality()) : 0;
        if (charges.reserve(FacetPatcher.patchSetBytes(matches, bucketIds[i], sizes[i]) + ownBytes, true) == null) {
          refused = true;
          break;
        }
        patches[i] = FacetPatcher.patchSet(matches, snapshots[i], bucketIds[i], counted[i], own != null ? own[i] : null);
      }
    if (total <= budget && !refused)
      return true;

    if (!refused)
      manager.getStats().queryPatchOverflow();
    boolean exact = true;
    for (int i = 0; i < bucketIds.length; i++) {
      if (snapshots[i] == null || !answered[i])
        continue;
      if (own != null && own[i] != null)
        // the caller's own uncommitted writes are not lag: the committed projection value would be a wrong answer
        scanned[i] = true;
      else if (sizes[i] > 0)
        exact = false;
      patches[i] = null;
    }
    charges.release();
    return exact;
  }

  /**
   * {@link #ANSWERS} when the bucket's snapshot holds the facet's property, encoded as the schema's definition encodes it now
   * ({@link ColumnDefinition#sameEncoding}), for every match of the facet it holds live; else the {@link #scanReason} the bucket is read
   * from its records for. A column redefined after the snapshot was taken (retyped, or dropped and created again) is not decoded with
   * the new definition: the bucket is read from its records.
   */
  private static byte projectionReason(final DatabaseInternal db, final MatchSet matches, final int facetIndex, final FacetSpec spec,
      final int bucketId, final ColumnarSnapshot snapshot) {
    final String property = spec.property();
    if (property == null)
      return scanReason(ColumnarStats.FALLBACK_EXPRESSION);
    // the schema first: a property without a COLUMNAR index scans whatever state the projection is in
    final ColumnDefinition current = FacetFallbackScanner.column(db.getSchema().getTypeByBucketId(bucketId), property);
    if (current == null)
      return scanReason(ColumnarStats.FALLBACK_NO_INDEX);
    if (snapshot == null)
      return scanReason(ColumnarStats.FALLBACK_NO_PROJECTION);
    final ColumnDefinition published = snapshot.publishedColumn(property);
    if (published == null)
      return scanReason(ColumnarStats.FALLBACK_NOT_PUBLISHED);
    if (!current.sameEncoding(published))
      return scanReason(ColumnarStats.FALLBACK_ENCODING);
    final MatchSet.FacetMatches facetMatches = matches.facetMatches(facetIndex, bucketId);
    for (final ColumnarSegmentView segment : snapshot.segments())
      if (segment.column(property) == null && facetMatches != null && facetMatches.andCardinality(segment.live()) > 0)
        return scanReason(ColumnarStats.FALLBACK_SEGMENT_COLUMN);
    return ANSWERS;
  }

  /** The by-facet-and-bucket code of a {@link ColumnarStats#FALLBACK_SCAN_REASONS} reason; never {@link #ANSWERS}. */
  private static byte scanReason(final int fallbackReason) {
    return (byte) (fallbackReason + 1);
  }

  /**
   * Counts every facet: bucket by bucket, each facet's collector counts the segments (the facet's matches minus the patch set), then the
   * bucket's records are read once: the patch set for the facets the projection answers, and the matches for the facets scanned in it
   * (a bucket with both reads each record once for all of them).
   * <p>
   * The fallback scan permit (D14) is taken here, once the facets are planned and only when a facet that is still available will
   * scan; it is not waited for, and refused, every facet with a scanned bucket is {@code unavailable / fallback scans capped}. Granted,
   * the query counts once under the reason of its first scanned facet and bucket, and every scanned facet and bucket is logged at FINE.
   *
   * @param reasons by facet and bucket, {@link #ANSWERS} or why the facet is scanned in the bucket; {@link #plan} sets the reason of a
   *                bucket it scans for a column found missing or redefined meanwhile
   *
   * @return the records read to patch the projection (not those read for scanned facets)
   */
  private static long count(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final List<FacetSpec> facets, final CommandContext ctx, final DocumentType target, final int[] bucketIds,
      final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches, final byte[][] reasons,
      final LinkedHashMap<String, FacetResult> results, final LinkedHashMap<String, FacetExecutionStats> stats) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(ctx);
    final FacetPlan[] plans = new FacetPlan[facets.size()];
    final DerivedMatches derived = new DerivedMatches(matches, facets);
    boolean scanPermit = false;
    try {
      boolean wantsScan = false;
      for (int f = 0; f < plans.length; f++) {
        plans[f] = plan(db, manager, matches, f, withConvertedCursor(db, target, bucketIds, facets.get(f)), bucketIds, snapshots,
            reasons[f]);
        plans[f].deadline(ctx != null ? ctx.getCommandDeadline() : Long.MAX_VALUE);
        wantsScan |= plans[f].scanned && !plans[f].unavailable;
      }
      if (wantsScan) {
        scanPermit = manager.fallbackScans().tryAcquire();
        if (scanPermit)
          countScans(db, manager, plans, bucketIds, reasons);
        else
          for (final FacetPlan plan : plans)
            plan.capped = plan.scanned;
      }

      long patched = 0;
      final FacetCollector[] patching = new FacetCollector[plans.length];
      final List<FacetCollector> patchingView = Arrays.asList(patching);
      // by facet, the column its patching collector decodes in the bucket
      final ColumnDefinition[] patchingColumns = new ColumnDefinition[plans.length];
      final FacetCollector[] scanning = new FacetCollector[plans.length];
      final FacetFallbackScanner.Source[] sources = new FacetFallbackScanner.Source[plans.length];
      final List<FacetFallbackScanner.Source> sourcesView = Arrays.asList(sources);
      final FacetFallbackScanner.ScanSink sink = (f, position, value) -> scanning[f].collectPatched(position, value);
      for (int i = 0; i < bucketIds.length; i++) {
        boolean anyPatching = false;
        boolean anyScanning = false;
        for (int f = 0; f < plans.length; f++) {
          scanning[f] = plans[f].scanBucket(i, bucketIds[i]);
          sources[f] = scanning[f] != null ? plans[f].scanSourceOf[i] : null;
          patching[f] = null;
          if (scanning[f] == null && plans[f].counts(i)) {
            final PositionBitmap segmentMatches = derived.of(f, bucketIds[i], patches[i]);
            if (segmentMatches == null)
              plans[f].refuse();
            else
              patching[f] = plans[f].countBucket(i, snapshots[i], segmentMatches);
            derived.facetDone();
          }
          patchingColumns[f] = patching[f] != null ? plans[f].columnOf[i] : null;
          anyScanning |= scanning[f] != null;
          anyPatching |= patching[f] != null;
        }
        derived.bucketDone();
        final boolean patchHere = anyPatching && patches[i] != null && !patches[i].isEmpty();
        if (anyScanning) {
          final FacetPatcher.Patching patcher = patchHere ?
              new FacetPatcher.Patching(db, matches, snapshots[i], facets, patchingView, patchingColumns) : null;
          FacetFallbackScanner.scan(db, matches, bucketIds[i], sourcesView, ctx, sink, patchHere ? patches[i] : null, patcher, guard);
          if (patcher != null)
            patched += patcher.read();
        } else if (patchHere)
          patched += FacetPatcher.countPatched(db, matches, snapshots[i], patches[i], facets, patchingView, patchingColumns, guard);
      }

      for (final FacetPlan plan : plans) {
        final FacetResult result = plan.finish(db, manager, target);
        // a STATS result carries the type too: its min and max are encoded as a row's value of the property
        final Type valueType = plan.spec instanceof TermsFacetSpec || plan.spec instanceof HierarchyFacetSpec || plan.spec instanceof StatsFacetSpec ?
            declaredType(db, target, bucketIds, plan.spec.property()) : null;
        results.put(plan.spec.alias(), valueType != null ? asRowValues(result, valueType) : result);
        stats.put(plan.spec.alias(), plan.stats);
      }
      return patched;
    } finally {
      if (scanPermit)
        manager.fallbackScans().release();
      for (final FacetPlan plan : plans)
        if (plan != null)
          plan.release();
      derived.release(manager);
    }
  }

  /**
   * Counts the query in {@code query.fallback.scans} under the reason of its first scanned facet and bucket, and logs every scanned
   * facet and bucket at FINE with its reason (the text is built only when FINE is on).
   */
  private static void countScans(final DatabaseInternal db, final ColumnarIndexManager manager, final FacetPlan[] plans,
      final int[] bucketIds, final byte[][] reasons) {
    final boolean fine = LogManager.instance().isLoggable(FacetExecutor.class, Level.FINE);
    boolean counted = false;
    for (int f = 0; f < plans.length; f++) {
      if (!plans[f].scanned || plans[f].unavailable)
        continue;
      for (int i = 0; i < bucketIds.length; i++) {
        if (plans[f].scanSourceOf[i] == null)
          continue;
        final int reason = reasons[f][i] - 1;
        if (!counted) {
          manager.getStats().queryFallbackScan(reason);
          counted = true;
          if (!fine)
            return;
        }
        final DocumentType type = db.getSchema().getTypeByBucketId(bucketIds[i]);
        LogManager.instance().log(FacetExecutor.class, Level.FINE, "FACETS facet '%s' of bucket '%s' (type '%s') answered by a record scan: %s",
            plans[f].spec.alias(), db.getSchema().getBucketById(bucketIds[i]).getName(), type != null ? type.getName() : null,
            reasonText(manager, bucketIds[i], reason));
      }
    }
  }

  /** The FINE text of a reason; the one of a bucket without a usable projection says why, read only here. */
  private static String reasonText(final ColumnarIndexManager manager, final int bucketId, final int reason) {
    if (reason != ColumnarStats.FALLBACK_NO_PROJECTION)
      return REASON_TEXTS[reason];
    final ColumnarProjection projection = manager.projection(bucketId);
    if (projection == null)
      return REASON_TEXTS[reason];
    final String unusable = projection.unusableReason();
    // no reason recorded: the projection closed or was dropped after the query looked, or it is usable again since
    return unusable != null ? "projection unusable (" + unusable + ")" : "projection closed, dropped or replaced while the query read it";
  }

  /**
   * The bitmaps a facet's collector counts the segments of a bucket with, derived from the match set and charged to the memory budget
   * before they are made: a FILTER facet's own matches, and the matches minus the bucket's patch set. A facet without FILTER in a bucket
   * without patch set counts the match set's own bitmap (nothing made). The matches minus the patch set of the facets without FILTER
   * are made once per bucket and shared by them; a FILTER facet's bitmap is dropped (its charge released) as soon as the facet counted
   * the bucket. A histogram keeps the bitmaps it counted until it finishes: those stay charged until the query ends.
   */
  private static final class DerivedMatches {
    private final MatchSet              matches;
    /** By facet: whether its collector keeps the bitmaps (a histogram). */
    private final boolean[]             keeps;
    /** Whether a facet without FILTER keeps the bitmaps: the shared one is then charged until the query ends. */
    private final boolean               sharedKept;
    private final CollectorReservations kept;
    private final CollectorReservations bucket;
    private final CollectorReservations facet;
    private       PositionBitmap        shared;

    private DerivedMatches(final MatchSet matches, final List<FacetSpec> facets) {
      this.matches = matches;
      this.keeps = new boolean[facets.size()];
      boolean sharedKept = false;
      for (int f = 0; f < keeps.length; f++) {
        keeps[f] = facets.get(f) instanceof HistogramFacetSpec;
        sharedKept |= keeps[f] && matches.filterIndexOf(f) < 0;
      }
      this.sharedKept = sharedKept;
      final long waitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
      this.kept = new CollectorReservations(waitMs);
      this.bucket = new CollectorReservations(waitMs);
      this.facet = new CollectorReservations(waitMs);
    }

    /** The bitmap the facet counts the bucket's segments with; null when the memory budget refused it. */
    private PositionBitmap of(final int facetIndex, final int bucketId, final PositionBitmap patch) {
      final boolean patched = patch != null && !patch.isEmpty();
      final boolean filtered = matches.filterIndexOf(facetIndex) >= 0;
      if (!filtered && !patched)
        return matches.forFacet(facetIndex, bucketId);
      if (!filtered && shared != null)
        return shared;
      final CollectorReservations charges = filtered ? keeps[facetIndex] ? kept : facet : sharedKept ? kept : bucket;
      if (charges.reserve(matches.derivedBytes(facetIndex, bucketId, patch), true) == null)
        return null;
      // without FILTER the match set's own bitmap: a copy-on-write snapshot, so the andNot below clones only the patched pages
      final PositionBitmap derived = filtered ? matches.forFacet(facetIndex, bucketId) : matches.forFacet(facetIndex, bucketId).snapshot();
      if (patched)
        derived.andNot(patch);
      if (!filtered)
        shared = derived;
      return derived;
    }

    /** The facet counted the bucket: a FILTER facet's bitmap is dropped, unless its histogram keeps it. */
    private void facetDone() {
      facet.release();
    }

    /** Every facet counted the bucket: the shared bitmap is dropped, unless a histogram keeps it. */
    private void bucketDone() {
      shared = null;
      bucket.release();
    }

    private void release(final ColumnarIndexManager manager) {
      final int waits = kept.waits() + bucket.waits() + facet.waits();
      if (waits > 0)
        manager.getStats().queryBudgetWaits(waits);
      kept.release();
      bucket.release();
      facet.release();
    }
  }

  /**
   * The collector of a facet for one declaring type.
   *
   * @param type       the declaring type, null when no bucket is counted (the collector then only answers its INCLUDE values)
   * @param projection the projection of one bucket of the type; no collector of this plan reads it, those of plan 5 do
   * @param matchCount the records the facet counts in the type, to size the counter
   *
   * @throws CommandExecutionException for a range or histogram over a column it cannot bucket (a string column, a bound that is not a
   *                                   value of the column)
   */
  public static FacetCollector collectorFor(final FacetSpec spec, final DocumentType type, final ColumnarProjection projection,
      final long matchCount) {
    if (type == null)
      return collectorFor(spec, null, matchCount, null, null, null);
    return columnCollector(spec, type, column(type, spec.property()), matchCount);
  }

  /** The collector of a facet for one declaring type, counting {@code column} (the definition the counted segments were written with). */
  private static FacetCollector columnCollector(final FacetSpec spec, final DocumentType type, final ColumnDefinition column,
      final long matchCount) {
    final DatabaseInternal db = (DatabaseInternal) type.getSchema().getEmbedded().getDatabase();
    return collectorFor(spec, column, matchCount, db,
        ColumnConversion.of(((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().extractor(type.getName()), column), type.getName());
  }

  /**
   * @param column        the column counted (a real one, or the synthetic column of the raw values a fallback scan reads), null for none
   * @param extractor     converts literals (and raw scanned values) into the column's LONG encoding
   * @param declaringType the type of the COLUMNAR index of the property in the counted buckets (whose METADATA holds a HIERARCHY
   *                      facet's hierarchy), null when unknown or when they have none
   */
  private static FacetCollector collectorFor(final FacetSpec spec, final ColumnDefinition column, final long matchCount,
      final DatabaseInternal db, final ColumnConversion extractor, final String declaringType) {
    final long budgetWaitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    return switch (spec) {
      case TermsFacetSpec terms -> new TermsCollector(terms, column, matchCount, budgetWaitMs, db, extractor);
      case RangeFacetSpec range -> new RangeCollector(range, column, budgetWaitMs, db, extractor);
      case HistogramFacetSpec histogram -> new HistogramCollector(histogram, column, budgetWaitMs, db, extractor);
      case HierarchyFacetSpec hierarchy -> new HierarchyFacetCollector(hierarchy, column, matchCount, db, declaringType, budgetWaitMs);
      case DistinctFacetSpec distinct -> new DistinctFacetCollector(distinct, column, matchCount, db, declaringType, budgetWaitMs);
      case StatsFacetSpec stats -> new StatsFacetCollector(stats, column, matchCount, db, budgetWaitMs);
    };
  }

  /**
   * Groups the facet's buckets by declaring type and prepares one collector per group: the buckets the projection answers, then the
   * scanned buckets, by how they are read ({@link FacetFallbackScanner#sourceOf}, decided here once per bucket and handed to the scan):
   * one collector per column, one per kind of raw read. The declaring type's column definition is read once here: a bucket whose
   * snapshot publishes the column with another encoding (redefined since {@link #projectionReason} looked) is scanned too, so the
   * collector decodes every segment it counts with the definition it was written with.
   *
   * @param reasons by bucket index, {@link #ANSWERS} or why the facet is scanned in the bucket; a bucket found without a usable column
   *                here is scanned too, and its reason set
   */
  private static FacetPlan plan(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int facetIndex, final FacetSpec spec, final int[] bucketIds, final ColumnarSnapshot[] snapshots, final byte[] reasons) {
    final FacetPlan plan = new FacetPlan(spec, bucketIds.length);
    final String property = spec.property();
    // the buckets of each declaring type, in bucket order; the scanned buckets by column or kind of raw read
    final LinkedHashMap<String, List<Integer>> groups = new LinkedHashMap<>();
    final LinkedHashMap<String, List<Integer>> scanGroups = new LinkedHashMap<>();
    final Map<String, DocumentType> declaringTypes = new LinkedHashMap<>();
    final Map<String, ColumnDefinition> declaredColumns = new LinkedHashMap<>();
    // a date column or declared date property in any bucket: the buckets of types that do not declare the property read it as dates
    boolean dates = false;
    for (int i = 0; i < bucketIds.length; i++) {
      final DocumentType type = db.getSchema().getTypeByBucketId(bucketIds[i]);
      // an index dropped since projectionReason looked: the bucket is scanned as well
      final TypeIndex index = type == null || property == null ? null : type.getPolymorphicColumnarIndex(property);
      ColumnDefinition declared = null;
      if (index != null && reasons[i] == ANSWERS) {
        final String declaring = index.getTypeName();
        declaringTypes.computeIfAbsent(declaring, k -> db.getSchema().getType(declaring));
        declared = declaredColumns.computeIfAbsent(declaring, k -> FacetFallbackScanner.column(declaringTypes.get(declaring), property));
        if (declared != null && !declared.sameEncoding(snapshots[i].publishedColumn(property))) {
          declared = null;
          reasons[i] = scanReason(ColumnarStats.FALLBACK_ENCODING);
        }
      }
      if (declared == null) {
        if (reasons[i] == ANSWERS)
          reasons[i] = scanReason(ColumnarStats.FALLBACK_NO_INDEX);
        final FacetFallbackScanner.Source source = FacetFallbackScanner.sourceOf(db, type, spec);
        plan.scanSourceOf[i] = source;
        dates |= source.read() == FacetFallbackScanner.Read.EPOCH_MS || source.column() != null && source.column().propertyType()
            .isDateOrDateTime();
        continue;
      }
      groups.computeIfAbsent(index.getTypeName(), k -> new ArrayList<>()).add(i);
      dates |= declared.propertyType().isDateOrDateTime();
    }
    for (int i = 0; i < bucketIds.length; i++) {
      FacetFallbackScanner.Source source = plan.scanSourceOf[i];
      if (source == null)
        continue;
      if (dates && FacetFallbackScanner.isUndeclared(db.getSchema().getTypeByBucketId(bucketIds[i]), source))
        plan.scanSourceOf[i] = source = FacetFallbackScanner.asDates(db, source);
      String key = source.read() == FacetFallbackScanner.Read.COLUMN ? "column " + source.column().indexFileId() : source.read().name();
      if (source.read() == FacetFallbackScanner.Read.EPOCH || spec instanceof StatsFacetSpec && source.column() != null)
        // one collector per declared type: date epoch counts are of different units, and STATS returns values of the declared type
        key += " " + source.column().propertyType();
      if (spec instanceof HierarchyFacetSpec)
        // one hierarchy per declaring type: the scanned buckets of each are counted over its own view; the buckets of a type without
        // the index have none, and their collector reports which type that is
        key += " of " + hierarchyGroupOf(db.getSchema().getTypeByBucketId(bucketIds[i]), property);
      scanGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
    }

    try {
      for (final Map.Entry<String, List<Integer>> group : groups.entrySet())
        if (!plan.addGroup(db, manager, matches, facetIndex, group.getKey(), declaringTypes.get(group.getKey()),
            declaredColumns.get(group.getKey()), group.getValue(), bucketIds, snapshots)) {
          plan.unavailable = true;
          return plan;
        }
      for (final List<Integer> members : scanGroups.values())
        plan.addScanGroup(db, matches, facetIndex, members, bucketIds);
      return plan;
    } catch (final RuntimeException | Error e) {
      plan.release();
      throw e;
    }
  }

  /** One facet being counted: a collector and an ordinal map per declaring type, and per bucket what its segments are counted with. */
  private static final class FacetPlan {
    private final FacetSpec                   spec;
    private final FacetExecutionStats         stats      = new FacetExecutionStats();
    private final List<FacetCollector>        collectors = new ArrayList<>(1);
    private final List<CrossBucketOrdinalMap> maps       = new ArrayList<>(1);
    /** By bucket index: the collector of the bucket's declaring type, null for a bucket the facet does not count. */
    private final FacetCollector[]            collectorOf;
    private final CrossBucketOrdinalMap[]     mapOf;
    /** By bucket index: the column {@link #collectorOf} decodes, which a patched record of the bucket is read with too. */
    private final ColumnDefinition[]          columnOf;
    /** By bucket index: the collector of a bucket the facet is scanned in, null for the others. */
    private final FacetCollector[]            scanCollectorOf;
    /** By bucket index: how a bucket the facet is scanned in is read, null for the others. */
    private final FacetFallbackScanner.Source[] scanSourceOf;
    /** The memory budget refused an ordinal map or a bitmap of the facet's matches: nothing is counted. */
    private       boolean                     unavailable;
    /** The facet needed a fallback scan and the permit was refused: nothing is counted. */
    private       boolean                     capped;
    /** A bucket of the facet was read from its records instead of the projection. */
    private       boolean                     scanned;

    private FacetPlan(final FacetSpec spec, final int buckets) {
      this.spec = spec;
      this.collectorOf = new FacetCollector[buckets];
      this.mapOf = new CrossBucketOrdinalMap[buckets];
      this.columnOf = new ColumnDefinition[buckets];
      this.scanCollectorOf = new FacetCollector[buckets];
      this.scanSourceOf = new FacetFallbackScanner.Source[buckets];
    }

    /**
     * Prepares scanned buckets read the same way (one column, or one kind of raw read): one collector for the column of their
     * {@link FacetFallbackScanner.Source}, sized for the facet's matches in them.
     */
    private void addScanGroup(final DatabaseInternal db, final MatchSet matches, final int facetIndex, final List<Integer> members,
        final int[] bucketIds) {
      long matchCount = 0;
      for (final int i : members) {
        final MatchSet.FacetMatches facetMatches = matches.facetMatches(facetIndex, bucketIds[i]);
        if (facetMatches != null)
          matchCount += facetMatches.cardinality();
      }
      final FacetFallbackScanner.Source source = scanSourceOf[members.getFirst()];
      final DocumentType bucketType = db.getSchema().getTypeByBucketId(bucketIds[members.getFirst()]);
      final FacetCollector collector = spec instanceof HierarchyFacetSpec hierarchy ?
          new HierarchyFacetCollector(hierarchy, source.column(), matchCount, db, declaringTypeName(bucketType, spec.property()),
              bucketType != null ? bucketType.getName() : null, GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong()) :
          collectorFor(spec, source.column(), matchCount, db, source.conversion(), null);
      collectors.add(collector);
      for (final int i : members)
        scanCollectorOf[i] = collector;
      scanned = true;
    }

    /**
     * Begins a scanned bucket on its collector.
     *
     * @return the collector, which then takes the bucket's scanned records; null when the facet is not scanned in the bucket
     */
    private FacetCollector scanBucket(final int i, final int bucketId) {
      final FacetCollector collector = unavailable || capped ? null : scanCollectorOf[i];
      if (collector != null)
        // no snapshot: every value arrives through collectPatched
        collector.beginBucket(bucketId, null, null);
      return collector;
    }

    /**
     * Prepares the buckets of one declaring type: the collector is sized for every record it counts, from segments and from records.
     *
     * @param column the declaring type's column, which every member's snapshot publishes with the same encoding
     *
     * @return false when the memory budget refused the private ordinal array of an older dictionary generation
     */
    private boolean addGroup(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches, final int facetIndex,
        final String declaring, final DocumentType declaringType, final ColumnDefinition column, final List<Integer> members,
        final int[] bucketIds, final ColumnarSnapshot[] snapshots) {
      final ColumnarSnapshot[] groupSnapshots = new ColumnarSnapshot[members.size()];
      long matchCount = 0;
      for (int j = 0; j < groupSnapshots.length; j++) {
        final int i = members.get(j);
        groupSnapshots[j] = snapshots[i];
        // from the segments or from the patched records, every match of the facet is counted once
        matchCount += matches.facetMatches(facetIndex, bucketIds[i]).cardinality();
      }

      CrossBucketOrdinalMap map = null;
      if (column.kind() == ColumnKind.ORDINAL || column.kind() == ColumnKind.ORDINAL_LIST) {
        map = CrossBucketOrdinalMap.of(db, declaring, spec.property(), groupSnapshots);
        if (map == null)
          return false;
        maps.add(map);
      }

      final FacetCollector collector = columnCollector(spec, declaringType, column, matchCount);
      collectors.add(collector);
      for (final int i : members) {
        collectorOf[i] = collector;
        mapOf[i] = map;
        columnOf[i] = column;
      }
      return true;
    }

    /** The deadline of the statement: a HIERARCHY collector waits for its view, and builds it, until then at most (final review A-M4). */
    private void deadline(final long deadlineMs) {
      for (final FacetCollector collector : collectors)
        if (collector instanceof HierarchyFacetCollector hierarchy)
          hierarchy.deadline(deadlineMs);
    }

    /** Whether the facet counts the bucket from its snapshot (it is available and the bucket is of one of its declaring types). */
    private boolean counts(final int i) {
      return !unavailable && !capped && collectorOf[i] != null;
    }

    /** The memory budget refused a bitmap of the facet's matches: the facet is unavailable and its counters are given back now. */
    private void refuse() {
      unavailable = true;
      release();
    }

    /**
     * Begins the bucket on its collector and counts its segment views.
     *
     * @param segmentMatches the facet's matches in the bucket minus the patch set: what the segments count
     *
     * @return the collector, which then takes the bucket's patched records
     */
    private FacetCollector countBucket(final int i, final ColumnarSnapshot snapshot, final PositionBitmap segmentMatches) {
      final FacetCollector collector = collectorOf[i];
      collector.beginBucket(snapshot.bucketId(), snapshot, mapOf[i]);
      for (final ColumnarSegmentView segment : snapshot.segments())
        // a view without the column holds no match of the facet live, or the bucket would be scanned (projectionReason)
        if (segment.column(spec.property()) != null)
          collector.collect(segment, segmentMatches);
      return collector;
    }

    /**
     * Merges the collectors of the declaring types and ranks the facet.
     *
     * @param target the type the query selects from, null when unknown: a facet that counts no bucket converts its literals with the
     *               column of the property's declaring type in it
     */
    private FacetResult finish(final DatabaseInternal db, final ColumnarIndexManager manager, final DocumentType target) {
      // before any early return and before the merge: a collector that waited and then ended capped or unavailable still waited
      int waits = 0;
      for (final FacetCollector collector : collectors)
        waits += collector.budgetWaits();
      if (waits > 0)
        manager.getStats().queryBudgetWaits(waits);
      if (capped) {
        stats.source(FacetResult.UNAVAILABLE);
        return FacetResult.unavailable(kindName(spec), FALLBACK_SCANS_CAPPED);
      }
      if (unavailable) {
        stats.source(FacetResult.UNAVAILABLE);
        return FacetResult.unavailable(kindName(spec), TermsCollector.MEMORY_BUDGET);
      }
      if (collectors.isEmpty())
        collectors.add(collectorFor(spec, declaringType(db, target, spec.property()), null, 0));

      final FacetCollector first = collectors.getFirst();
      for (int i = 1; i < collectors.size(); i++)
        first.merge(collectors.get(i));
      FacetResult result = first.finish();
      final FacetExecutionStats collected = first.stats();
      if (collected != null) {
        stats.add(collected);
        stats.source(collected.source());
      }
      if (scanned && !FacetResult.UNAVAILABLE.equals(result.source())) {
        result = result.withSource(SCAN);
        stats.source(SCAN);
      }
      return result;
    }

    private void release() {
      for (final FacetCollector collector : collectors)
        collector.release();
      for (final CrossBucketOrdinalMap map : maps)
        map.close();
    }
  }

  /** The scan group of a HIERARCHY facet's bucket of {@code type}: its declaring type, else the type itself (it has no hierarchy). */
  private static String hierarchyGroupOf(final DocumentType type, final String property) {
    final String declaring = declaringTypeName(type, property);
    return declaring != null ? declaring : "unindexed " + (type != null ? type.getName() : null);
  }

  /** The name of the type declaring the property's COLUMNAR index that {@code type} answers through, null when it has none. */
  private static String declaringTypeName(final DocumentType type, final String property) {
    final TypeIndex index = type == null || property == null ? null : type.getPolymorphicColumnarIndex(property);
    return index == null ? null : index.getTypeName();
  }

  /**
   * The declaring type of the property's COLUMNAR index in {@code target} (the type a match of {@code target} would be counted with),
   * null when there is no target, no plain property or no such index.
   */
  private static DocumentType declaringType(final DatabaseInternal db, final DocumentType target, final String property) {
    if (target == null || property == null)
      return null;
    final TypeIndex index = target.getPolymorphicColumnarIndex(property);
    return index == null ? null : db.getSchema().getType(index.getTypeName());
  }

  /**
   * A terms facet with its AFTER cursor converted once with the property's schema type in the queried type ({@link #declaredType}, the
   * type its buckets are returned as), so every collector, those of buckets read from their records included (they know no column),
   * compares it with values of that type: {@code AFTER '2021-01-01 00:00:00'} on a DATETIME, or a cursor a client echoes back as the
   * formatted string of a returned one. A number on a numeric property stays exact. A cursor that is not a value of the type is refused
   * here, once, whatever path counts the facet ({@link FacetBounds#cursorConverted}). Any other facet as it is.
   */
  private static FacetSpec withConvertedCursor(final DatabaseInternal db, final DocumentType target, final int[] bucketIds,
      final FacetSpec spec) {
    if (!(spec instanceof TermsFacetSpec terms) || terms.after() == null)
      return spec;
    final Object converted = FacetBounds.cursorConverted(terms.alias(), terms.property(), db,
        declaredType(db, target, bucketIds, terms.property()), terms.after());
    return converted == terms.after() ? spec : new TermsFacetSpec(terms.alias(), terms.property(), terms.expression(), terms.filter(),
        terms.top(), terms.minCount(), terms.order(), terms.include(), terms.missingBucket(), converted);
  }

  /**
   * The result with its terms (and its cursor) as the values a row of the property holds ({@link FacetResults#asRowValue}): the
   * collectors count and rank normalized keys (a Long for every integral type, an exact decimal for a FLOAT or DOUBLE), a caller gets
   * what GROUP BY gets, an Integer for an INTEGER property, a Double for a DOUBLE one (final fix batch 2, round 2). The transports then
   * encode these values as they encode a row's.
   */
  private static FacetResult asRowValues(final FacetResult result, final Type valueType) {
    List<FacetBucket> buckets = null;
    for (int i = 0; i < result.buckets().size(); i++) {
      final FacetBucket bucket = result.buckets().get(i);
      if (bucket.key() != null || bucket.isMissing() || bucket.value() == null)
        continue;
      final Object rowValue = FacetResults.asRowValue(bucket.value(), valueType);
      if (rowValue == bucket.value())
        continue;
      if (buckets == null)
        buckets = new ArrayList<>(result.buckets());
      final FacetBucket converted = FacetBucket.term(rowValue, bucket.count());
      final JSONObject attributes = bucket.attributes();
      buckets.set(i, attributes.isEmpty() ? converted : converted.withAttributes(attributes));
    }
    FacetResult converted = buckets != null ? result.withBuckets(buckets) : result;
    if (converted.hasAfter() && converted.after() != null)
      converted = converted.withAfter(FacetResults.asRowValue(converted.after(), valueType));
    return converted.withValueType(valueType);
  }

  /**
   * The schema type of the property in the type the query selects from (else in the type of the first matched bucket): the type of a
   * terms facet's values, which a transport encodes as it encodes a row's value of the property. Null for an expression facet or a
   * property no such type declares.
   */
  private static Type declaredType(final DatabaseInternal db, final DocumentType target, final int[] bucketIds, final String property) {
    if (property == null)
      return null;
    final DocumentType type = target != null ? target : bucketIds.length > 0 ? db.getSchema().getTypeByBucketId(bucketIds[0]) : null;
    final Property declared = type != null ? type.getPolymorphicPropertyIfExists(property) : null;
    if (declared == null)
      return null;
    // a list is counted by item: the terms are values of its items' type, when it declares one; any other container is unknown
    return switch (declared.getType()) {
      case LIST -> declared.getOfType() != null ? Type.getTypeByName(declared.getOfType()) : null;
      case BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL, STRING, DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS,
           DATETIME_NANOS -> declared.getType();
      default -> null;
    };
  }

  private static ColumnDefinition column(final DocumentType type, final String property) {
    final ColumnDefinition column = FacetFallbackScanner.column(type, property);
    if (column == null)
      throw new IllegalStateException("Type " + type.getName() + " has no COLUMNAR index on '" + property + "'");
    return column;
  }

  /** The buckets of the match set the current user may read records of; the others are not counted. */
  private static int[] readableBuckets(final DatabaseInternal db, final int[] bucketIds) {
    final int[] readable = new int[bucketIds.length];
    int n = 0;
    for (final int bucketId : bucketIds)
      if (SecurityHelper.canAccessFile(db, bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
        readable[n++] = bucketId;
    return n == readable.length ? readable : Arrays.copyOf(readable, n);
  }

  /** The usable snapshot of the bucket's projection; null when it has none, it was closed or dropped meanwhile, or it is unusable. */
  private static ColumnarSnapshot acquire(final ColumnarIndexManager manager, final int bucketId) {
    final ColumnarProjection projection = manager.projection(bucketId);
    if (projection == null)
      return null;
    final ColumnarSnapshot snapshot;
    try {
      snapshot = projection.acquire();
    } catch (final IllegalStateException e) {
      return null;
    }
    if (snapshot.isUsable())
      return snapshot;
    snapshot.close();
    return null;
  }

  /**
   * Closes the snapshots this executor acquired (not those the match set owns), each in its own guard. A close failure is thrown, or,
   * when {@code inFlight} (the failure the execution already ends with) is set, added to it as suppressed so that one is not lost.
   */
  static void closeAcquired(final ColumnarSnapshot[] snapshots, final boolean[] acquired, final Throwable inFlight) {
    RuntimeException failure = null;
    for (int i = 0; i < snapshots.length; i++) {
      if (!acquired[i])
        continue;
      try {
        snapshots[i].close();
      } catch (final RuntimeException e) {
        if (inFlight != null)
          inFlight.addSuppressed(e);
        else if (failure == null)
          failure = e;
        else
          failure.addSuppressed(e);
      }
    }
    if (failure != null)
      throw failure;
  }

  /** The {@code type} member of the facet's result. */
  static String kindName(final FacetSpec spec) {
    return switch (spec) {
      case TermsFacetSpec t -> "terms";
      case RangeFacetSpec r -> "range";
      case HistogramFacetSpec h -> "histogram";
      case HierarchyFacetSpec h -> "hierarchy";
      case DistinctFacetSpec d -> "distinct";
      case StatsFacetSpec s -> "stats";
    };
  }
}
