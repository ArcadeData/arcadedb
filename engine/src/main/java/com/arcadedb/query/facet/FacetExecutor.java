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
 */
public final class FacetExecutor {
  private FacetExecutor() {
  }

  /**
   * @param matches the matched records, owned by the caller; {@link MatchSet#setFacetFilterIndex} was called with one entry per facet
   * @param ctx     the context of the query
   */
  public static FacetResults execute(final DatabaseInternal db, final MatchSet matches, final List<FacetSpec> facets,
      final CommandContext ctx) {
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

      for (int f = 0; f < facets.size(); f++) {
        final FacetSpec spec = facets.get(f);
        final FacetExecutionStats facetStats = new FacetExecutionStats();
        results.put(spec.alias(), facet(db, manager, matches, f, spec, bucketIds, snapshots, facetStats));
        stats.put(spec.alias(), facetStats);
      }
      final FacetResults facetResults = new FacetResults(true, 0, lagMs, results);
      facetResults.setExecutionStats(stats);
      return facetResults;
    } finally {
      closeAcquired(snapshots, acquired);
    }
  }

  /**
   * The collector of a facet for one declaring type.
   *
   * @param type       the declaring type, null when no bucket is counted (the collector then only answers its INCLUDE values)
   * @param projection the projection of one bucket of the type; no collector of this plan reads it, those of plan 5 do
   * @param matchCount the records the facet counts in the type, to size the counter
   *
   * @throws CommandExecutionException for a kind of facet no collector computes yet
   */
  public static FacetCollector collectorFor(final FacetSpec spec, final DocumentType type, final ColumnarProjection projection,
      final long matchCount) {
    if (spec instanceof TermsFacetSpec terms) {
      if (type == null)
        return new TermsCollector(terms, null, matchCount, GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong(), null, null);
      final DatabaseInternal db = (DatabaseInternal) type.getSchema().getEmbedded().getDatabase();
      return new TermsCollector(terms, column(type, terms.property()), matchCount, GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong(),
          db, ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().extractor(type.getName()));
    }
    throw new CommandExecutionException("FACETS " + spec.alias() + ": " + kindKeyword(spec) + " facets are not supported");
  }

  private static FacetResult facet(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int facetIndex, final FacetSpec spec, final int[] bucketIds, final ColumnarSnapshot[] snapshots,
      final FacetExecutionStats facetStats) {
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

    final List<FacetCollector> collectors = new ArrayList<>(Math.max(1, groups.size()));
    final List<CrossBucketOrdinalMap> maps = new ArrayList<>(groups.size());
    try {
      for (final Map.Entry<String, List<Integer>> group : groups.entrySet()) {
        final FacetCollector collector = countGroup(db, manager, matches, facetIndex, spec, group.getKey(),
            declaringTypes.get(group.getKey()), group.getValue(), bucketIds, snapshots, maps);
        if (collector == null) {
          facetStats.source(FacetResult.UNAVAILABLE);
          return FacetResult.unavailable(kindName(spec), TermsCollector.MEMORY_BUDGET);
        }
        collectors.add(collector);
      }
      if (collectors.isEmpty())
        collectors.add(collectorFor(spec, null, null, 0));

      final FacetCollector first = collectors.getFirst();
      for (int i = 1; i < collectors.size(); i++)
        first.merge(collectors.get(i));
      final FacetResult result = first.finish();
      if (first instanceof TermsCollector terms) {
        facetStats.add(terms.stats());
        facetStats.source(terms.stats().source());
        if (terms.budgetWaits() > 0)
          manager.getStats().queryBudgetWaits(terms.budgetWaits());
      }
      return result;
    } finally {
      for (final FacetCollector collector : collectors)
        collector.release();
      for (final CrossBucketOrdinalMap map : maps)
        map.close();
    }
  }

  /**
   * Counts the buckets of one declaring type.
   *
   * @return the collector, or null when the memory budget refused the private ordinal array of an older dictionary generation
   */
  private static FacetCollector countGroup(final DatabaseInternal db, final ColumnarIndexManager manager, final MatchSet matches,
      final int facetIndex, final FacetSpec spec, final String declaring, final DocumentType declaringType, final List<Integer> members,
      final int[] bucketIds, final ColumnarSnapshot[] snapshots, final List<CrossBucketOrdinalMap> maps) {
    final String property = spec.property();
    final ColumnarSnapshot[] groupSnapshots = new ColumnarSnapshot[members.size()];
    final PositionBitmap[] facetMatches = new PositionBitmap[members.size()];
    long matchCount = 0;
    for (int j = 0; j < groupSnapshots.length; j++) {
      final int i = members.get(j);
      groupSnapshots[j] = snapshots[i];
      // shared with the match set for a facet without FILTER: read only, nothing here writes it
      facetMatches[j] = matches.forFacet(facetIndex, bucketIds[i]);
      matchCount += facetMatches[j].cardinality();
    }

    final ColumnKind kind = column(declaringType, property).kind();
    CrossBucketOrdinalMap map = null;
    if (kind == ColumnKind.ORDINAL || kind == ColumnKind.ORDINAL_LIST) {
      map = CrossBucketOrdinalMap.of(db, declaring, property, groupSnapshots);
      if (map == null)
        return null;
      maps.add(map);
    }

    final FacetCollector collector = collectorFor(spec, declaringType, manager.projection(bucketIds[members.getFirst()]), matchCount);
    try {
      for (int j = 0; j < groupSnapshots.length; j++) {
        final ColumnarSnapshot snapshot = groupSnapshots[j];
        collector.beginBucket(snapshot.bucketId(), snapshot, map);
        for (final ColumnarSegmentView segment : snapshot.segments()) {
          if (segment.column(property) == null) {
            if (facetMatches[j].andCardinality(segment.live()) > 0)
              throw deferred(spec, snapshot.bucketId(), "a segment view has no column '" + property + "'");
            continue;
          }
          collector.collect(segment, facetMatches[j]);
        }
      }
      return collector;
    } catch (final RuntimeException | Error e) {
      collector.release();
      throw e;
    }
  }

  private static ColumnDefinition column(final DocumentType type, final String property) {
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar)
          return columnar.columnDefinition();
    throw new IllegalStateException("Type " + type.getName() + " has no COLUMNAR index on '" + property + "'");
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
