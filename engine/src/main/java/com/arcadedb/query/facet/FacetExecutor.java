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
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 * meanwhile, an unusable snapshot, a column not published by the snapshot, a segment view without the column holding a match, no
 * COLUMNAR index on the property, an expression facet) is counted from its matched records by {@link FacetFallbackScanner}, by
 * collectors of their own (one per declaring type of the column, one for raw values) merged with the others by value. The answer stays
 * exact and the facet's source is {@code scan}. A query that scans takes one permit of {@link ColumnarIndexManager#fallbackScans()}
 * without waiting; refused, every facet that needed a scan is {@code unavailable} with the reason {@code fallback scans capped}, and
 * the others are counted as usual.
 * <p>
 * Patching (spec 7.3, D10, D11): the matches a snapshot cannot answer for, and those the caller's open transaction wrote, are read
 * from their records by {@link FacetPatcher}, once per record for every facet. Above {@code arcadedb.columnar.patchBudget} positions
 * in all nothing is patched and the answer is not exact, except for the buckets the caller's transaction wrote: they are scanned for
 * every facet.
 */
public final class FacetExecutor {
  /** The {@code source} of a facet a bucket of which was read from its records. */
  static final String SCAN                  = "scan";
  static final String FALLBACK_SCANS_CAPPED = "fallback scans capped";

  private FacetExecutor() {
  }

  /** {@link #execute(DatabaseInternal, MatchSet, List, CommandContext, DocumentType)} without the type the query selects from. */
  public static FacetResults execute(final DatabaseInternal db, final MatchSet matches, final List<FacetSpec> facets,
      final CommandContext ctx) {
    return execute(db, matches, facets, ctx, null);
  }

  /**
   * @param matches the matched records, owned by the caller; {@link MatchSet#setFacetFilterIndex} was called with one entry per facet
   * @param ctx     the context of the query
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
          lagMs = Math.max(lagMs, now - snapshots[i].oldestPendingEpochStartMs());
      }

      final PositionBitmap[] patches = new PositionBitmap[bucketIds.length];
      final boolean[] ownScanned = new boolean[bucketIds.length];
      final boolean exact = planPatches(db, manager, matches, bucketIds, snapshots, patches, ownScanned);

      // by facet, the buckets read from their records (null: none)
      final boolean[][] scans = new boolean[facets.size()][];
      for (int f = 0; f < scans.length; f++)
        scans[f] = scans(db, matches, f, facets.get(f), bucketIds, snapshots, ownScanned);

      final long patched = count(db, manager, matches, facets, ctx, target, bucketIds, snapshots, patches, scans, results, stats);
      if (patched > 0)
        manager.getStats().queryPatched(patched);
      final FacetResults facetResults = new FacetResults(exact, patched, lagMs, results);
      facetResults.setExecutionStats(stats);
      return facetResults;
    } finally {
      closeAcquired(snapshots, acquired);
    }
  }

  /**
   * Fills each bucket's patch set (D11, plus the records the caller's open transaction wrote). When their sum exceeds
   * {@code arcadedb.columnar.patchBudget} nothing is patched (D10): a bucket the caller's transaction wrote is then scanned for every
   * facet instead ({@code scanned}, no patch set), any other is counted from the projection alone.
   *
   * @return false when a bucket that needed patching is counted from the projection alone (the answer is not exact)
   */
  private static boolean planPatches(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int[] bucketIds, final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches, final boolean[] scanned) {
    final PositionBitmap[] own = FacetPatcher.ownWrites(db, matches, bucketIds);
    long total = 0;
    for (int i = 0; i < bucketIds.length; i++) {
      if (snapshots[i] == null)
        continue;
      patches[i] = FacetPatcher.patchSet(matches, snapshots[i], bucketIds[i]);
      if (own != null && own[i] != null)
        patches[i].or(own[i]);
      total += patches[i].cardinality();
    }
    if (total <= GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValueAsLong())
      return true;

    manager.getStats().queryPatchOverflow();
    boolean exact = true;
    for (int i = 0; i < bucketIds.length; i++) {
      if (patches[i] == null)
        continue;
      if (own != null && own[i] != null)
        // the caller's own uncommitted writes are not lag: the committed projection value would be a wrong answer
        scanned[i] = true;
      else if (!patches[i].isEmpty())
        exact = false;
      patches[i] = null;
    }
    return exact;
  }

  /**
   * The buckets the facet is counted in by the fallback scan: those the projection cannot answer for it (see the class comment), and
   * those {@code ownScanned} (the caller's transaction wrote them, over the patch budget).
   *
   * @return by bucket index, null when the projection answers every bucket
   */
  private static boolean[] scans(final DatabaseInternal db, final MatchSet matches, final int facetIndex, final FacetSpec spec,
      final int[] bucketIds, final ColumnarSnapshot[] snapshots, final boolean[] ownScanned) {
    boolean[] scans = null;
    for (int i = 0; i < bucketIds.length; i++)
      if (ownScanned[i] || !projectionAnswers(db, matches, facetIndex, spec, bucketIds[i], snapshots[i])) {
        if (scans == null)
          scans = new boolean[bucketIds.length];
        scans[i] = true;
      }
    return scans;
  }

  /** Whether the bucket's snapshot holds the facet's property for every match of the facet it holds live. */
  private static boolean projectionAnswers(final DatabaseInternal db, final MatchSet matches, final int facetIndex, final FacetSpec spec,
      final int bucketId, final ColumnarSnapshot snapshot) {
    final String property = spec.property();
    if (property == null || snapshot == null || !snapshot.publishes(property))
      return false;
    if (FacetFallbackScanner.column(db.getSchema().getTypeByBucketId(bucketId), property) == null)
      return false;
    final PositionBitmap facetMatches = matches.forFacet(facetIndex, bucketId);
    for (final ColumnarSegmentView segment : snapshot.segments())
      if (segment.column(property) == null && facetMatches != null && facetMatches.andCardinality(segment.live()) > 0)
        return false;
    return true;
  }

  /**
   * Counts every facet: bucket by bucket, each facet's collector counts the segments (the facet's matches minus the patch set), then the
   * bucket's records are read once: the patch set for the facets the projection answers, and the matches for the facets scanned in it
   * (a bucket with both reads each record once for all of them).
   * <p>
   * The fallback scan permit (D14) is taken here, once the facets are planned and only when a facet that is still available will
   * scan; it is not waited for, and refused, every facet with a scanned bucket is {@code unavailable / fallback scans capped}.
   *
   * @param scans by facet, the buckets it is scanned in (null: none)
   *
   * @return the records read to patch the projection (not those read for scanned facets)
   */
  private static long count(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final List<FacetSpec> facets, final CommandContext ctx, final DocumentType target, final int[] bucketIds,
      final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches, final boolean[][] scans,
      final LinkedHashMap<String, FacetResult> results, final LinkedHashMap<String, FacetExecutionStats> stats) {
    final FacetPlan[] plans = new FacetPlan[facets.size()];
    boolean scanPermit = false;
    try {
      boolean wantsScan = false;
      for (int f = 0; f < plans.length; f++) {
        plans[f] = plan(db, manager, matches, f, facets.get(f), bucketIds, snapshots, patches, scans[f]);
        wantsScan |= plans[f].scanned && !plans[f].unavailable;
      }
      if (wantsScan) {
        scanPermit = manager.fallbackScans().tryAcquire();
        if (scanPermit)
          manager.getStats().queryFallbackScan();
        else
          for (final FacetPlan plan : plans)
            plan.capped = plan.scanned;
      }

      long patched = 0;
      final FacetCollector[] patching = new FacetCollector[plans.length];
      final List<FacetCollector> patchingView = Arrays.asList(patching);
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
          patching[f] = scanning[f] != null ? null : plans[f].countBucket(i, snapshots[i]);
          anyScanning |= scanning[f] != null;
          anyPatching |= patching[f] != null;
        }
        final boolean patchHere = anyPatching && patches[i] != null && !patches[i].isEmpty();
        if (anyScanning) {
          final FacetPatcher.Patching patcher = patchHere ? new FacetPatcher.Patching(db, matches, snapshots[i], facets, patchingView) : null;
          FacetFallbackScanner.scan(db, matches, bucketIds[i], sourcesView, ctx, sink, patchHere ? patches[i] : null, patcher);
          if (patcher != null)
            patched += patcher.read();
        } else if (patchHere)
          patched += FacetPatcher.countPatched(db, matches, snapshots[i], patches[i], facets, patchingView);
      }

      for (final FacetPlan plan : plans) {
        results.put(plan.spec.alias(), plan.finish(db, manager, target));
        stats.put(plan.spec.alias(), plan.stats);
      }
      return patched;
    } finally {
      if (scanPermit)
        manager.fallbackScans().release();
      for (final FacetPlan plan : plans)
        if (plan != null)
          plan.release();
    }
  }

  /**
   * The collector of a facet for one declaring type.
   *
   * @param type       the declaring type, null when no bucket is counted (the collector then only answers its INCLUDE values)
   * @param projection the projection of one bucket of the type; no collector of this plan reads it, those of plan 5 do
   * @param matchCount the records the facet counts in the type, to size the counter
   *
   * @throws CommandExecutionException for a kind of facet no collector computes yet, and for a range or histogram over a column it cannot
   *                                   bucket (a string column, a bound that is not a value of the column)
   */
  public static FacetCollector collectorFor(final FacetSpec spec, final DocumentType type, final ColumnarProjection projection,
      final long matchCount) {
    if (type == null)
      return collectorFor(spec, null, matchCount, null, null);
    final DatabaseInternal db = (DatabaseInternal) type.getSchema().getEmbedded().getDatabase();
    return collectorFor(spec, column(type, spec.property()), matchCount, db,
        ColumnConversion.of(((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().extractor(type.getName())));
  }

  /**
   * @param column    the column counted (a real one, or the synthetic column of the raw values a fallback scan reads), null for none
   * @param extractor converts literals (and raw scanned values) into the column's LONG encoding
   */
  private static FacetCollector collectorFor(final FacetSpec spec, final ColumnDefinition column, final long matchCount,
      final DatabaseInternal db, final ColumnConversion extractor) {
    final long budgetWaitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    return switch (spec) {
      case TermsFacetSpec terms -> new TermsCollector(terms, column, matchCount, budgetWaitMs, db, extractor);
      case RangeFacetSpec range -> new RangeCollector(range, column, budgetWaitMs, db, extractor);
      case HistogramFacetSpec histogram -> new HistogramCollector(histogram, column, budgetWaitMs, db, extractor);
      default -> throw new CommandExecutionException("FACETS " + spec.alias() + ": " + kindKeyword(spec) + " facets are not supported");
    };
  }

  /**
   * Groups the facet's buckets by declaring type and prepares one collector per group: the buckets the projection answers, then the
   * scanned buckets, by how they are read ({@link FacetFallbackScanner#sourceOf}, decided here once per bucket and handed to the scan):
   * one collector per column, one per kind of raw read.
   *
   * @param scans by bucket index, the buckets the facet is scanned in; null when none
   */
  private static FacetPlan plan(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int facetIndex, final FacetSpec spec, final int[] bucketIds, final ColumnarSnapshot[] snapshots,
      final PositionBitmap[] patches, final boolean[] scans) {
    final FacetPlan plan = new FacetPlan(spec, bucketIds.length);
    final String property = spec.property();
    // the buckets of each declaring type, in bucket order; the scanned buckets by column or kind of raw read
    final LinkedHashMap<String, List<Integer>> groups = new LinkedHashMap<>();
    final LinkedHashMap<String, List<Integer>> scanGroups = new LinkedHashMap<>();
    final Map<String, DocumentType> declaringTypes = new LinkedHashMap<>();
    // a date column or declared date property in any bucket: the buckets of types that do not declare the property read it as dates
    boolean dates = false;
    for (int i = 0; i < bucketIds.length; i++) {
      final DocumentType type = db.getSchema().getTypeByBucketId(bucketIds[i]);
      // an index dropped since projectionAnswers looked: the bucket is scanned as well
      final TypeIndex index = type == null || property == null ? null : type.getPolymorphicColumnarIndex(property);
      if (scans != null && scans[i] || index == null) {
        final FacetFallbackScanner.Source source = FacetFallbackScanner.sourceOf(db, type, spec);
        plan.scanSourceOf[i] = source;
        dates |= source.read() == FacetFallbackScanner.Read.EPOCH_MS || source.column() != null && source.column().propertyType()
            .isDateOrDateTime();
        continue;
      }
      final String declaring = index.getTypeName();
      declaringTypes.putIfAbsent(declaring, db.getSchema().getType(declaring));
      groups.computeIfAbsent(declaring, k -> new ArrayList<>()).add(i);
      dates |= column(declaringTypes.get(declaring), property).propertyType().isDateOrDateTime();
    }
    for (int i = 0; i < bucketIds.length; i++) {
      FacetFallbackScanner.Source source = plan.scanSourceOf[i];
      if (source == null)
        continue;
      if (dates && FacetFallbackScanner.isUndeclared(db.getSchema().getTypeByBucketId(bucketIds[i]), source))
        plan.scanSourceOf[i] = source = FacetFallbackScanner.asDates(db, source);
      final String key = source.read() == FacetFallbackScanner.Read.COLUMN ? "column " + source.column().indexFileId() : source.read().name();
      scanGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
    }

    try {
      for (final Map.Entry<String, List<Integer>> group : groups.entrySet())
        if (!plan.addGroup(db, manager, matches, facetIndex, group.getKey(), declaringTypes.get(group.getKey()), group.getValue(),
            bucketIds, snapshots, patches)) {
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
    /** By bucket index: the facet's matches minus the patch set, what the segments count. */
    private final PositionBitmap[]            segmentMatches;
    /** By bucket index: the collector of a bucket the facet is scanned in, null for the others. */
    private final FacetCollector[]            scanCollectorOf;
    /** By bucket index: how a bucket the facet is scanned in is read, null for the others. */
    private final FacetFallbackScanner.Source[] scanSourceOf;
    /** The memory budget refused an ordinal map: nothing is counted. */
    private       boolean                     unavailable;
    /** The facet needed a fallback scan and the permit was refused: nothing is counted. */
    private       boolean                     capped;
    /** A bucket of the facet was read from its records instead of the projection. */
    private       boolean                     scanned;

    private FacetPlan(final FacetSpec spec, final int buckets) {
      this.spec = spec;
      this.collectorOf = new FacetCollector[buckets];
      this.mapOf = new CrossBucketOrdinalMap[buckets];
      this.segmentMatches = new PositionBitmap[buckets];
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
        final PositionBitmap facetMatches = matches.forFacet(facetIndex, bucketIds[i]);
        if (facetMatches != null)
          matchCount += facetMatches.cardinality();
      }
      final FacetFallbackScanner.Source source = scanSourceOf[members.getFirst()];
      final FacetCollector collector = collectorFor(spec, source.column(), matchCount, db, source.conversion());
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
     * @return false when the memory budget refused the private ordinal array of an older dictionary generation
     */
    private boolean addGroup(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches, final int facetIndex,
        final String declaring, final DocumentType declaringType, final List<Integer> members, final int[] bucketIds,
        final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches) {
      final ColumnarSnapshot[] groupSnapshots = new ColumnarSnapshot[members.size()];
      long matchCount = 0;
      for (int j = 0; j < groupSnapshots.length; j++) {
        final int i = members.get(j);
        groupSnapshots[j] = snapshots[i];
        // shared with the match set for a facet without FILTER: copied before it is written
        PositionBitmap facetMatches = matches.forFacet(facetIndex, bucketIds[i]);
        final PositionBitmap patch = patches[i];
        if (patch != null && !patch.isEmpty()) {
          // the facet's patched records: counted from the records, not from the segments
          matchCount += facetMatches.andCardinality(patch);
          if (matches.sharesFacetMatches(facetIndex))
            facetMatches = facetMatches.copy();
          facetMatches.andNot(patch);
        }
        segmentMatches[i] = facetMatches;
        matchCount += facetMatches.cardinality();
      }

      final ColumnKind kind = column(declaringType, spec.property()).kind();
      CrossBucketOrdinalMap map = null;
      if (kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST) {
        map = CrossBucketOrdinalMap.of(db, declaring, spec.property(), groupSnapshots);
        if (map == null)
          return false;
        maps.add(map);
      }

      final FacetCollector collector = collectorFor(spec, declaringType, manager.projection(bucketIds[members.getFirst()]), matchCount);
      collectors.add(collector);
      for (final int i : members) {
        collectorOf[i] = collector;
        mapOf[i] = map;
      }
      return true;
    }

    /**
     * Begins the bucket on its collector and counts its segment views.
     *
     * @return the collector, which then takes the bucket's patched records; null when the facet does not count the bucket
     */
    private FacetCollector countBucket(final int i, final ColumnarSnapshot snapshot) {
      final FacetCollector collector = unavailable || capped ? null : collectorOf[i];
      if (collector == null)
        return null;
      collector.beginBucket(snapshot.bucketId(), snapshot, mapOf[i]);
      for (final ColumnarSegmentView segment : snapshot.segments())
        // a view without the column holds no match of the facet live, or the bucket would be scanned (projectionAnswers)
        if (segment.column(spec.property()) != null)
          collector.collect(segment, segmentMatches[i]);
      return collector;
    }

    /**
     * Merges the collectors of the declaring types and ranks the facet.
     *
     * @param target the type the query selects from, null when unknown: a facet that counts no bucket converts its literals with the
     *               column of the property's declaring type in it
     */
    private FacetResult finish(final DatabaseInternal db, final ColumnarIndexManager manager, final DocumentType target) {
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
      if (first.budgetWaits() > 0)
        manager.getStats().queryBudgetWaits(first.budgetWaits());
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

  /** Closes the snapshots this executor acquired (not those the match set owns), each in its own guard. */
  private static void closeAcquired(final ColumnarSnapshot[] snapshots, final boolean[] acquired) {
    RuntimeException failure = null;
    for (int i = 0; i < snapshots.length; i++) {
      if (!acquired[i])
        continue;
      try {
        snapshots[i].close();
      } catch (final RuntimeException e) {
        if (failure == null)
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

  /** The keyword of the facet's kind, as the FACETS clause writes it. */
  private static String kindKeyword(final FacetSpec spec) {
    return switch (spec) {
      case TermsFacetSpec t -> "TERMS";
      case RangeFacetSpec r -> "RANGES";
      case HistogramFacetSpec h -> "HISTOGRAM";
      case HierarchyFacetSpec h -> "HIERARCHY";
      case DistinctFacetSpec d -> "DISTINCT";
      case StatsFacetSpec s -> "STATS";
    };
  }
}
