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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
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
 * {@link CrossBucketOrdinalMap}, and the collectors of several groups are merged by value. A bucket the projection cannot answer for
 * a facet (no projection, a projection closed or dropped meanwhile, an unusable snapshot, a column not published by the snapshot, a
 * segment view without the column, no COLUMNAR index on the property, an expression facet) needs the fallback scan, which is not
 * implemented yet: the facet then fails with an {@link IllegalStateException}.
 * <p>
 * Patching (spec 7.3, D10, D11): the matches a snapshot cannot answer for, and those the caller's open transaction wrote, are read
 * from their records by {@link FacetPatcher}, once per record for every facet. Above {@code arcadedb.columnar.patchBudget} positions
 * in all nothing is patched and the answer is not exact, except for the buckets the caller's transaction wrote: they are read from
 * their records entirely (source {@code scan}, one fallback scan permit for the query, D14).
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
    boolean scanPermit = false;
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
      final boolean[] scanned = new boolean[bucketIds.length];
      final boolean exact = planPatches(db, manager, matches, bucketIds, snapshots, patches, scanned);
      if (anyOf(scanned)) {
        // the buckets the caller's transaction wrote are read from their records: one fallback scan permit for the query (D14)
        if (!manager.fallbackScans().tryAcquire()) {
          for (final FacetSpec spec : facets) {
            collectorFor(spec, null, null, 0).release();
            results.put(spec.alias(), FacetResult.unavailable(kindName(spec), FALLBACK_SCANS_CAPPED));
            final FacetExecutionStats facetStats = new FacetExecutionStats();
            facetStats.source(FacetResult.UNAVAILABLE);
            stats.put(spec.alias(), facetStats);
          }
          final FacetResults refused = new FacetResults(exact, 0, lagMs, results);
          refused.setExecutionStats(stats);
          return refused;
        }
        scanPermit = true;
        manager.getStats().queryFallbackScan();
      }

      final long patched = count(db, manager, matches, facets, target, bucketIds, snapshots, patches, scanned, results, stats);
      if (patched > 0)
        manager.getStats().queryPatched(patched);
      final FacetResults facetResults = new FacetResults(exact, patched, lagMs, results);
      facetResults.setExecutionStats(stats);
      return facetResults;
    } finally {
      if (scanPermit)
        manager.fallbackScans().release();
      closeAcquired(snapshots, acquired);
    }
  }

  /**
   * Fills each bucket's patch set (D11, plus the records the caller's open transaction wrote). When their sum exceeds
   * {@code arcadedb.columnar.patchBudget} nothing is patched (D10): a bucket the caller's transaction wrote is then read from its
   * records instead (its patch set becomes every match of it, {@code scanned}), any other is counted from the projection alone.
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
      if (own != null && own[i] != null) {
        // the caller's own uncommitted writes are not lag: the committed projection value would be a wrong answer
        // every match of the bucket (the written ones among them); shared with the match set, nothing writes a patch set from here on
        patches[i] = matches.forBucket(bucketIds[i]);
        scanned[i] = true;
      } else {
        if (!patches[i].isEmpty())
          exact = false;
        patches[i] = null;
      }
    }
    return exact;
  }

  /**
   * Counts every facet: bucket by bucket, each facet's collector counts the segments (the facet's matches minus the patch set), then the
   * patch set is read once for all of them.
   *
   * @return the records read to patch the projection (not those of scanned buckets)
   */
  private static long count(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final List<FacetSpec> facets, final DocumentType target, final int[] bucketIds, final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches,
      final boolean[] scanned, final LinkedHashMap<String, FacetResult> results, final LinkedHashMap<String, FacetExecutionStats> stats) {
    final FacetPlan[] plans = new FacetPlan[facets.size()];
    try {
      for (int f = 0; f < plans.length; f++)
        plans[f] = plan(db, manager, matches, f, facets.get(f), bucketIds, snapshots, patches, scanned);

      long patched = 0;
      final FacetCollector[] patching = new FacetCollector[plans.length];
      final List<FacetCollector> patchingView = Arrays.asList(patching);
      for (int i = 0; i < bucketIds.length; i++) {
        boolean anyCollector = false;
        for (int f = 0; f < plans.length; f++) {
          patching[f] = plans[f].countBucket(i, snapshots[i]);
          anyCollector |= patching[f] != null;
        }
        if (anyCollector && patches[i] != null && !patches[i].isEmpty()) {
          final long read = FacetPatcher.countPatched(db, matches, snapshots[i], patches[i], facets, patchingView);
          if (!scanned[i])
            patched += read;
        }
      }

      for (final FacetPlan plan : plans) {
        results.put(plan.spec.alias(), plan.finish(db, manager, target));
        stats.put(plan.spec.alias(), plan.stats);
      }
      return patched;
    } finally {
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
    final long budgetWaitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    DatabaseInternal db = null;
    ColumnarValueExtractor extractor = null;
    ColumnDefinition column = null;
    if (type != null) {
      db = (DatabaseInternal) type.getSchema().getEmbedded().getDatabase();
      extractor = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().extractor(type.getName());
      column = column(type, spec.property());
    }
    return switch (spec) {
      case TermsFacetSpec terms -> new TermsCollector(terms, column, matchCount, budgetWaitMs, db, extractor);
      case RangeFacetSpec range -> new RangeCollector(range, column, budgetWaitMs, db, extractor);
      case HistogramFacetSpec histogram -> new HistogramCollector(histogram, column, budgetWaitMs, db, extractor);
      default -> throw new CommandExecutionException("FACETS " + spec.alias() + ": " + kindKeyword(spec) + " facets are not supported");
    };
  }

  /**
   * Groups the facet's buckets by declaring type and prepares one collector per group.
   *
   * @throws IllegalStateException for a bucket the projection cannot answer (the fallback scan is not available yet)
   */
  private static FacetPlan plan(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int facetIndex, final FacetSpec spec, final int[] bucketIds, final ColumnarSnapshot[] snapshots,
      final PositionBitmap[] patches, final boolean[] scanned) {
    final String property = spec.property();
    // the buckets of each declaring type, in bucket order
    final LinkedHashMap<String, List<Integer>> groups = new LinkedHashMap<>();
    final Map<String, DocumentType> declaringTypes = new LinkedHashMap<>();
    for (int i = 0; i < bucketIds.length; i++) {
      final int bucketId = bucketIds[i];
      if (property == null)
        throw deferred(spec, bucketId, "an expression facet is computed from the records");
      if (snapshots[i] == null)
        throw deferred(spec, bucketId, "the bucket has no usable columnar projection");
      final DocumentType type = db.getSchema().getTypeByBucketId(bucketId);
      final TypeIndex index = type == null ? null : type.getPolymorphicColumnarIndex(property);
      if (index == null)
        throw deferred(spec, bucketId, "no COLUMNAR index on '" + property + "'");
      if (!snapshots[i].publishes(property))
        throw deferred(spec, bucketId, "the column '" + property + "' is not published yet");
      final String declaring = index.getTypeName();
      declaringTypes.putIfAbsent(declaring, db.getSchema().getType(declaring));
      groups.computeIfAbsent(declaring, k -> new ArrayList<>()).add(i);
    }

    final FacetPlan plan = new FacetPlan(spec, bucketIds.length);
    try {
      for (final Map.Entry<String, List<Integer>> group : groups.entrySet())
        if (!plan.addGroup(db, manager, matches, facetIndex, group.getKey(), declaringTypes.get(group.getKey()), group.getValue(),
            bucketIds, snapshots, patches, scanned)) {
          plan.unavailable = true;
          break;
        }
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
    /** The memory budget refused an ordinal map: nothing is counted. */
    private       boolean                     unavailable;
    /** A bucket of the facet was read from its records instead of the projection. */
    private       boolean                     scanned;

    private FacetPlan(final FacetSpec spec, final int buckets) {
      this.spec = spec;
      this.collectorOf = new FacetCollector[buckets];
      this.mapOf = new CrossBucketOrdinalMap[buckets];
      this.segmentMatches = new PositionBitmap[buckets];
    }

    /**
     * Prepares the buckets of one declaring type: the collector is sized for every record it counts, from segments and from records.
     *
     * @return false when the memory budget refused the private ordinal array of an older dictionary generation
     */
    private boolean addGroup(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches, final int facetIndex,
        final String declaring, final DocumentType declaringType, final List<Integer> members, final int[] bucketIds,
        final ColumnarSnapshot[] snapshots, final PositionBitmap[] patches, final boolean[] scannedBuckets) {
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
        scanned |= scannedBuckets[i];
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
      final FacetCollector collector = unavailable ? null : collectorOf[i];
      if (collector == null)
        return null;
      collector.beginBucket(snapshot.bucketId(), snapshot, mapOf[i]);
      for (final ColumnarSegmentView segment : snapshot.segments()) {
        if (segment.column(spec.property()) == null) {
          if (segmentMatches[i].andCardinality(segment.live()) > 0)
            throw deferred(spec, snapshot.bucketId(), "a segment view has no column '" + spec.property() + "'");
          continue;
        }
        collector.collect(segment, segmentMatches[i]);
      }
      return collector;
    }

    /**
     * Merges the collectors of the declaring types and ranks the facet.
     *
     * @param target the type the query selects from, null when unknown: a facet that counts no bucket converts its literals with the
     *               column of the property's declaring type in it
     */
    private FacetResult finish(final DatabaseInternal db, final ColumnarIndexManager manager, final DocumentType target) {
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
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar)
          return columnar.columnDefinition();
    throw new IllegalStateException("Type " + type.getName() + " has no COLUMNAR index on '" + property + "'");
  }

  private static boolean anyOf(final boolean[] flags) {
    for (final boolean flag : flags)
      if (flag)
        return true;
    return false;
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

  /** A bucket the projection cannot answer: until the fallback scan exists, the facet fails. */
  private static IllegalStateException deferred(final FacetSpec spec, final int bucketId, final String why) {
    return new IllegalStateException(
        "FACETS " + spec.alias() + ": bucket " + bucketId + " needs the fallback scan (" + why + "), which is not available yet");
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
