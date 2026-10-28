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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.HierarchyManager;
import com.arcadedb.index.columnar.HierarchyView;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

/**
 * Counts a HIERARCHY facet for one declaring type (spec 7.6): every matched document counts once in each node of the ancestor closure
 * of its values ({@link AncestorExpander}) over the {@link HierarchyView} of the type's COLUMNAR index, so two values in one subtree
 * count their shared ancestors once and a cycle terminates. A value no vertex of the hierarchy has is an orphan: its own root bucket,
 * with {@code "orphan": true}. A matched live row without a value counts as missing, a dead row not at all (the segment's ordinals are
 * visited for live rows only).
 * <p>
 * <b>View and security.</b> The view is taken once, at the first {@link #beginBucket}, from the type's {@link HierarchyManager} (a
 * private one when the caller's transaction changed the tag graph). The shared view is built with no user, so this is the only gate:
 * the current user must be allowed to read the records of every file the hierarchy watches ({@link HierarchyManager#watchedFileIds}),
 * checked before the view is asked for and again with the file set the view was built from; a refusal answers the facet
 * {@code unavailable} with the reason {@value #NOT_READABLE}. A view that cannot be had (a source dropped, the manager closed, even
 * during the build, the caller nested too deep) answers {@code unavailable} with the manager's reason: the query never fails for it.
 * <p>
 * <b>Ordinals.</b> A dictionary ordinal is translated to its merged ordinal ({@link CrossBucketOrdinalMap}), then to its node by a memo
 * of the query indexed by merged ordinal, filled on demand by {@link HierarchyView#nodeOfValue} (the view holds no ordinal: values
 * added to a dictionary after the view was built map by value). An orphan is counted by merged ordinal, a value with no ordinal (a
 * patched record's new value, every value of a bucket the fallback scan reads) by value.
 * <p>
 * <b>Memory.</b> The node counter (a dense array without waiting, else a hash map waiting one {@code queryBudgetWait} window, topped up
 * as it grows), the expander's bitset and stack, the memo, and the orphan counts are reserved from {@link FacetMemoryBudget} before they
 * are allocated, through the collector's own {@link CollectorReservations}; any refusal makes the facet {@code unavailable}
 * ({@code memory budget}) and releases everything.
 * <p>
 * Result: buckets ranked by count, then value, after the expansion; {@code TOP} and {@code MIN COUNT} apply to the nodes. Each bucket
 * carries {@code parent} (the value of its first parent in CSR order, null for a root), {@code depth} ({@link HierarchyView#depth}).
 * {@code otherCount} is the sum of the counts of the nodes not returned, so it can exceed the matches; {@code missing} the matches
 * without a value. The collectors of several declaring types are merged by node value, a node's attributes taken from the first
 * collector whose view has it. Not thread safe: driven by one query thread.
 */
public final class HierarchyFacetCollector implements FacetCollector, OrdinalConsumer {
  static final         String KIND               = "hierarchy";
  static final         String NOT_READABLE       = "hierarchy source not readable";
  private static final String PARENT             = "parent";
  private static final String DEPTH              = "depth";
  private static final String ORPHAN             = "orphan";
  /** A memo entry of an orphan merged ordinal; 0 is not resolved yet, a node is stored as {@code node + 1}. */
  private static final int    ORPHAN_ENTRY       = -1;
  /** The distinct orphan ordinals the orphan counter is first reserved for. */
  private static final int    ORPHAN_EXPECTED    = 16;
  /** The heap one entry of the merge by value holds: map node, table share and count array (the key is held by a view or a map). */
  private static final long   MERGE_ENTRY_BYTES  = 32 + 16 + 24;

  private final HierarchyFacetSpec            spec;
  private final ColumnKind                    kind;
  private final long                          matchCount;
  private final DatabaseInternal              database;
  private final String                        declaringType;
  /** The type a missing hierarchy is reported on: the declaring type, else the type of the counted buckets. */
  private final String                        countedType;
  /** The view of a hand-driven collector (tests), null to take it from the declaring type's hierarchy. */
  private final HierarchyView                 fixedView;
  private final CollectorReservations         budget;
  private final FacetExecutionStats           stats  = new FacetExecutionStats();
  private final List<HierarchyFacetCollector> others = new ArrayList<>();

  private boolean       viewTaken;
  private HierarchyView view;
  /** Why the facet is unavailable, null while it is not. */
  private String        unavailable;
  private boolean       finished;
  private long          missing;

  // by node
  private FacetCounter     nodes;
  /** The node counter when it is a hash map that can outgrow its reservation, null for a dense one. */
  private GrowingCounter   growingNodes;
  private AncestorExpander expander;
  private int[]            starts = new int[8];
  private int              startCount;

  // by merged ordinal
  private CrossBucketOrdinalMap map;
  private int[]                 memo;
  private GrowingCounter        orphanOrdinals;

  // by value: orphans without an ordinal, charged in steps of TermsCollector.FOREIGN_STEP_BYTES
  private Map<Object, long[]> orphanValues;
  private long                orphanValueBytes;
  private long                orphanValueReserved;

  // the bucket being counted
  private ColumnarSnapshot snapshot;
  private int[]            toMerged;
  // the segment being counted: positions arrive in ascending order, so a change of position is a new document
  private long             lastPosition;
  private long             positions;

  /**
   * @param column        the column of the property in the declaring type, null when no projected bucket is counted (validation only,
   *                      an empty match set, or the raw values of a fallback scan)
   * @param matchCount    the records the facet counts in this declaring type, to size the node counter
   * @param db            the database of the hierarchy; null only for a collector that counts nothing (validation only)
   * @param declaringType the type declaring the COLUMNAR index with the hierarchy; null when the counted buckets have none (the facet is
   *                      then unavailable as soon as a bucket is counted)
   * @param budgetWaitMs  the longest this collector waits for the budget, all its reservations together (one deadline)
   */
  HierarchyFacetCollector(final HierarchyFacetSpec spec, final ColumnDefinition column, final long matchCount, final DatabaseInternal db,
      final String declaringType, final long budgetWaitMs) {
    this(spec, column, matchCount, db, declaringType, declaringType, null, budgetWaitMs);
  }

  /**
   * @param countedType the type of the counted buckets, named by the reason of a facet unavailable for want of a hierarchy when
   *                    {@code declaringType} is null (buckets of a type without a COLUMNAR index on the property)
   */
  HierarchyFacetCollector(final HierarchyFacetSpec spec, final ColumnDefinition column, final long matchCount, final DatabaseInternal db,
      final String declaringType, final String countedType, final long budgetWaitMs) {
    this(spec, column, matchCount, db, declaringType, countedType, null, budgetWaitMs);
  }

  private HierarchyFacetCollector(final HierarchyFacetSpec spec, final ColumnDefinition column, final long matchCount,
      final DatabaseInternal db, final String declaringType, final String countedType, final HierarchyView fixedView,
      final long budgetWaitMs) {
    this.spec = spec;
    this.kind = column == null ? null : column.kind();
    this.matchCount = matchCount;
    this.database = db;
    this.declaringType = declaringType;
    this.countedType = declaringType != null ? declaringType : countedType;
    this.fixedView = fixedView;
    this.budget = new CollectorReservations(budgetWaitMs);
  }

  /** A collector counting over a given view, with no hierarchy manager and no access check (tests drive it by hand). */
  static HierarchyFacetCollector forView(final HierarchyFacetSpec spec, final ColumnDefinition column, final long matchCount,
      final HierarchyView view, final long budgetWaitMs) {
    return new HierarchyFacetCollector(spec, column, matchCount, null, null, null, view, budgetWaitMs);
  }

  @Override
  public void beginBucket(final int bucketId, final ColumnarSnapshot snapshot, final CrossBucketOrdinalMap map) {
    if (unavailable != null)
      return;
    if (!viewTaken) {
      takeView();
      if (unavailable != null)
        return;
    }
    this.snapshot = snapshot;
    if (snapshot == null) {
      // a bucket the fallback scan reads: every value arrives by value through collectPatched
      toMerged = null;
      return;
    }
    if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": HIERARCHY counts a dictionary column, '" + spec.property() + "' is " + kind);
    if (map == null)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": the dictionary column '" + spec.property() + "' needs an ordinal map");
    if (this.map == null) {
      this.map = map;
      final FacetMemoryBudget.Reservation granted = budget.reserve(16L + (long) Integer.BYTES * map.size(), true);
      if (granted == null) {
        refuse();
        return;
      }
      stats.reserved(granted.bytes());
      memo = new int[map.size()];
    } else if (this.map != map)
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": every bucket of a declaring type is counted with the same ordinal map");
    toMerged = map.bucketArray(bucketId);
  }

  @Override
  public void collect(final ColumnarSegmentView segment, final PositionBitmap matches) {
    if (unavailable != null)
      return;
    final ColumnReader column = segment.column(spec.property());
    if (column == null)
      throw new IllegalStateException("FACETS " + spec.alias() + ": a segment view without the column '" + spec.property() + "'");
    final long matchedLive = matches.andCardinality(segment.live());
    if (matchedLive == 0) {
      stats.segmentSkipped();
      return;
    }
    stats.segmentScanned();
    lastPosition = -1;
    positions = 0;
    startCount = 0;
    // live rows only: the matches are not ANDed with live by the caller
    column.forEachOrdinal(matches, this);
    expandDocument();
    missing += matchedLive - positions;
  }

  /** {@link OrdinalConsumer}: one ordinal of a matched live row, ascending positions. */
  @Override
  public void accept(final long position, final int ordinal) {
    if (unavailable != null)
      return;
    if (position != lastPosition) {
      expandDocument();
      lastPosition = position;
      positions++;
    }
    startOrdinal(ordinal);
  }

  @Override
  public void collectPatched(final long position, final PatchedValue value) {
    if (unavailable != null)
      return;
    if (value.isMissing()) {
      missing++;
      return;
    }
    startCount = 0;
    for (int i = 0; i < value.ordinalCount() && unavailable == null; i++)
      startOrdinal(value.ordinalAt(i));
    for (int i = 0; i < value.unknownCount() && unavailable == null; i++)
      startValue(value.unknown(i));
    expandDocument();
  }

  @Override
  public void merge(final FacetCollector other) {
    if (!(other instanceof HierarchyFacetCollector collector))
      throw new IllegalArgumentException("FACETS " + spec.alias() + ": cannot merge a " + other.getClass().getSimpleName() + " into hierarchy");
    others.add(collector);
    stats.add(collector.stats);
  }

  @Override
  public FacetResult finish() {
    if (finished)
      throw new IllegalStateException("FACETS " + spec.alias() + ": already finished");
    try {
      final String reason = unavailableReason();
      if (reason != null) {
        stats.source(FacetResult.UNAVAILABLE);
        return FacetResult.unavailable(KIND, reason);
      }
      stats.source("index");
      long missingTotal = 0;
      for (final HierarchyFacetCollector collector : all()) {
        stats.counterBytes(collector.counterBytes());
        missingTotal += collector.missing;
      }
      if (others.isEmpty() && !hasOrphans())
        return rankNodes();
      // the merge map holds one entry per distinct value of every collector: charged before it is built
      long entries = 0;
      for (final HierarchyFacetCollector collector : all())
        entries += collector.distinctValues();
      if (budget.reserve(entries * MERGE_ENTRY_BYTES, true) == null) {
        stats.source(FacetResult.UNAVAILABLE);
        return FacetResult.unavailable(KIND, TermsCollector.MEMORY_BUDGET);
      }
      return rankValues(missingTotal);
    } finally {
      finished = true;
      release();
      for (final HierarchyFacetCollector collector : others)
        collector.release();
    }
  }

  @Override
  public void release() {
    budget.release();
  }

  /** The statistics of the facet, the other declaring types merged in included. */
  @Override
  public FacetExecutionStats stats() {
    return stats;
  }

  @Override
  public int budgetWaits() {
    int waits = budget.waits();
    for (final HierarchyFacetCollector collector : others)
      waits += collector.budget.waits();
    return waits;
  }

  // ------------------------------------------------------------------------------------------------------------------- view

  /** Takes the view the collector counts over, after the access check; on any failure the facet is unavailable with the reason. */
  private void takeView() {
    viewTaken = true;
    if (fixedView != null)
      view = fixedView;
    else {
      final HierarchyManager manager =
          declaringType == null ? null : ((LocalDatabase) database.getEmbedded()).getColumnarIndexManager().hierarchy(declaringType, spec.property());
      if (manager == null) {
        unavailable = "no hierarchy on " + countedType + "." + spec.property();
        return;
      }
      // the shared view is built with no user: this check is what keeps the tag paths from a user who may not read them
      if (!readable(manager)) {
        unavailable = NOT_READABLE;
        return;
      }
      HierarchyView taken;
      try {
        taken = manager.view();
      } catch (final CancellationException e) {
        // the manager closed during the build (the database closing, the index dropped)
        taken = null;
      }
      if (taken == null) {
        final String reason = manager.unavailableReason();
        unavailable = reason != null ? reason : "the hierarchy of " + declaringType + "." + spec.property() + " is not available";
        return;
      }
      // view() recomputes the watched files when the schema moved: the files the view was built from are checked as well
      if (!readable(manager)) {
        unavailable = NOT_READABLE;
        return;
      }
      view = taken;
    }
    allocateNodeCounter();
  }

  /** Whether the current user may read the records of every file the hierarchy watches. */
  private boolean readable(final HierarchyManager manager) {
    for (final int fileId : manager.watchedFileIds())
      if (!SecurityHelper.canAccessFile(database, fileId, SecurityDatabaseUser.ACCESS.READ_RECORD))
        return false;
    return true;
  }

  // ------------------------------------------------------------------------------------------------------------------- counting

  private void allocateNodeCounter() {
    final int size = view.nodeCount();
    // the expander's visited bitset and node stack
    final FacetMemoryBudget.Reservation expansion = budget.reserve(32L + (long) Integer.BYTES * size + Long.BYTES * ((size + 63L) >>> 6),
        true);
    if (expansion == null) {
      refuse();
      return;
    }
    stats.reserved(expansion.bytes());
    if (FacetCounter.prefersDense(matchCount, size)) {
      // dense is tried without waiting: refused, a hash map of the nodes reached is smaller
      final FacetMemoryBudget.Reservation dense = budget.reserve(DenseFacetCounter.bytesFor(size), false);
      if (dense != null) {
        stats.counter(FacetExecutionStats.CountingMode.DENSE, dense.bytes(), size);
        nodes = new DenseFacetCounter(size);
        expander = new AncestorExpander(size);
        return;
      }
    }
    final int expected = (int) Math.min(size, Math.max(1L, matchCount));
    final FacetMemoryBudget.Reservation hash = budget.reserve(HashFacetCounter.bytesFor(expected), true);
    if (hash == null) {
      refuse();
      return;
    }
    stats.counter(FacetExecutionStats.CountingMode.HASH, hash.bytes(), size);
    final HashFacetCounter created = new HashFacetCounter(expected);
    nodes = created;
    // a document reaches several nodes: the map can hold up to every node of the view
    if (expected < size)
      growingNodes = new GrowingCounter(created, expected, size, hash.bytes());
    expander = new AncestorExpander(size);
  }

  /** One bucket ordinal of the document being counted: its node joins the document's start nodes, an orphan is counted at once. */
  private void startOrdinal(final int ordinal) {
    // only a projected bucket delivers ordinals (a scanned one delivers values): its snapshot is set, and a null toMerged is the identity
    // map of a declaring type read through one bucket, whose bucket ordinals are the merged ordinals
    assert snapshot != null && map != null : "FACETS " + spec.alias() + ": an ordinal of a bucket begun without a snapshot";
    final int merged = toMerged == null ? ordinal : ordinal < toMerged.length ? toMerged[ordinal] : -1;
    if (memo == null || merged < 0 || merged >= memo.length) {
      // a value added to the bucket's dictionary after the map was taken (a patched record): by value
      startValue(snapshot.dictionary(spec.property()).valueOf(ordinal));
      return;
    }
    int entry = memo[merged];
    if (entry == 0) {
      final int node = view.nodeOfValue(map.value(merged));
      entry = node >= 0 ? node + 1 : ORPHAN_ENTRY;
      memo[merged] = entry;
    }
    if (entry > 0)
      addStart(entry - 1);
    else
      countOrphanOrdinal(merged);
  }

  /** A value of the document being counted that has no merged ordinal: its node joins the start nodes, else it is an orphan. */
  private void startValue(final Object value) {
    final int node = view.nodeOfValue(value);
    if (node >= 0)
      addStart(node);
    else
      addOrphanValue(value);
  }

  private void addStart(final int node) {
    if (startCount == starts.length)
      starts = Arrays.copyOf(starts, startCount * 2);
    starts[startCount++] = node;
  }

  /** Counts the document being counted once in every node of the closure of its start nodes. */
  private void expandDocument() {
    if (startCount == 0 || unavailable != null)
      return;
    final int size = expander.expand(view.parentOffsets(), view.parents(), view.nodeCount(), starts, startCount);
    startCount = 0;
    final int[] closure = expander.nodes();
    if (growingNodes == null)
      for (int i = 0; i < size; i++)
        nodes.increment(closure[i]);
    else
      for (int i = 0; i < size; i++)
        if (!growingNodes.increment(closure[i]))
          return;
    stats.nodeVisits(size);
  }

  private void countOrphanOrdinal(final int merged) {
    if (orphanOrdinals == null) {
      final int expected = Math.min(ORPHAN_EXPECTED, map.size());
      final FacetMemoryBudget.Reservation granted = budget.reserve(HashFacetCounter.bytesFor(expected), true);
      if (granted == null) {
        refuse();
        return;
      }
      stats.reserved(granted.bytes());
      orphanOrdinals = new GrowingCounter(new HashFacetCounter(expected), expected, map.size(), granted.bytes());
    }
    orphanOrdinals.increment(merged);
  }

  /**
   * Counts an orphan by value. Every new key is charged before it is added ({@link TermsCollector#foreignEntryBytes}), in steps of at
   * least {@link TermsCollector#FOREIGN_STEP_BYTES}; a refusal makes the facet unavailable.
   */
  private void addOrphanValue(final Object value) {
    final Object key = FacetValueComparator.normalize(value);
    if (orphanValues == null)
      orphanValues = new HashMap<>();
    final long[] count = orphanValues.get(key);
    if (count != null) {
      count[0]++;
      return;
    }
    orphanValueBytes += TermsCollector.foreignEntryBytes(key);
    if (orphanValueBytes > orphanValueReserved) {
      final FacetMemoryBudget.Reservation more = budget.reserve(Math.max(TermsCollector.FOREIGN_STEP_BYTES,
          orphanValueBytes - orphanValueReserved), true);
      if (more == null) {
        refuse();
        return;
      }
      orphanValueReserved += more.bytes();
      stats.reserved(more.bytes());
    }
    orphanValues.put(key, new long[] { 1L });
  }

  /** The memory budget refused a reservation: the facet is unavailable and everything is given back now. */
  private void refuse() {
    unavailable = TermsCollector.MEMORY_BUDGET;
    // the document being counted is dropped with the counters: its buffered start nodes are never expanded
    startCount = 0;
    nodes = null;
    growingNodes = null;
    orphanOrdinals = null;
    orphanValues = null;
    memo = null;
    expander = null;
    release();
  }

  /** A hash counter whose reservation is topped up before it outgrows it, up to {@code bound} distinct keys. */
  private final class GrowingCounter {
    private final HashFacetCounter counter;
    private final int              bound;
    private       int              reservedDistinct;
    private       long             reservedBytes;

    private GrowingCounter(final HashFacetCounter counter, final int reservedDistinct, final int bound, final long reservedBytes) {
      this.counter = counter;
      this.reservedDistinct = reservedDistinct;
      this.bound = bound;
      this.reservedBytes = reservedBytes;
    }

    /** @return false when the budget refused to cover one more key: the facet is then unavailable */
    private boolean increment(final int key) {
      if (counter.size() >= reservedDistinct && reservedDistinct < bound) {
        final int distinct = (int) Math.min(bound, Math.max(reservedDistinct * 2L, reservedDistinct + 1L));
        final long needed = counter.bytesAt(distinct);
        if (needed > reservedBytes) {
          final FacetMemoryBudget.Reservation more = budget.reserve(needed - reservedBytes, true);
          if (more == null) {
            refuse();
            return false;
          }
          reservedBytes += more.bytes();
          stats.toppedUp(more.bytes());
        }
        reservedDistinct = distinct;
      }
      counter.increment(key);
      return true;
    }
  }

  // ------------------------------------------------------------------------------------------------------------------- ranking

  private String unavailableReason() {
    for (final HierarchyFacetCollector collector : all())
      if (collector.unavailable != null)
        return collector.unavailable;
    return null;
  }

  private List<HierarchyFacetCollector> all() {
    final List<HierarchyFacetCollector> all = new ArrayList<>(others.size() + 1);
    all.add(this);
    all.addAll(others);
    return all;
  }

  private boolean hasOrphans() {
    return orphanOrdinals != null && orphanOrdinals.counter.size() > 0 || orphanValues != null && !orphanValues.isEmpty();
  }

  /** The heap the counters held when the facet was ranked. */
  private long counterBytes() {
    long bytes = nodes != null ? nodes.estimatedBytes() : 0;
    if (orphanOrdinals != null)
      bytes += orphanOrdinals.counter.estimatedBytes();
    return bytes + orphanValueBytes;
  }

  /** An upper bound of the distinct values this collector counted: what it adds to the merge map. */
  private long distinctValues() {
    long distinct = orphanValues == null ? 0 : orphanValues.size();
    if (orphanOrdinals != null)
      distinct += orphanOrdinals.counter.size();
    if (nodes != null) {
      final long[] nonZero = new long[1];
      nodes.forEachNonZero((node, count) -> nonZero[0]++);
      distinct += nonZero[0];
    }
    return distinct;
  }

  /** The fast path, one declaring type and no orphan: ranks the nodes and builds the buckets of the returned ones only. */
  private FacetResult rankNodes() {
    final List<FacetBucket> buckets = new ArrayList<>();
    if (nodes == null)
      return FacetResult.of(KIND, "index", buckets, 0, missing);
    final long[] total = new long[1];
    nodes.forEachNonZero((node, count) -> total[0] += count);
    final HierarchyView counted = view;
    final FacetValueComparator values = FacetValueComparator.INSTANCE;
    long returned = 0;
    for (final int node : FacetRanker.topOrdinals(nodes, spec.top(), spec.minCount(), FacetOrder.COUNT_DESC,
        (a, b) -> values.compareNormalized(counted.value(a), counted.value(b)))) {
      final long count = nodes.get(node);
      buckets.add(nodeBucket(counted, node, count));
      returned += count;
    }
    return FacetResult.of(KIND, "index", buckets, total[0] - returned, missing);
  }

  /** The general path: counts by value (several declaring types, orphans). */
  private FacetResult rankValues(final long missingTotal) {
    final Map<Object, long[]> counts = new HashMap<>();
    for (final HierarchyFacetCollector collector : all())
      collector.addValuesTo(counts);
    long total = 0;
    for (final long[] count : counts.values())
      total += count[0];
    final List<FacetBucket> buckets = new ArrayList<>();
    long returned = 0;
    for (final Map.Entry<Object, Long> entry : FacetRanker.topValues(counts, spec.top(), spec.minCount(), FacetOrder.COUNT_DESC)) {
      buckets.add(valueBucket(entry.getKey(), entry.getValue()));
      returned += entry.getValue();
    }
    return FacetResult.of(KIND, "index", buckets, total - returned, missingTotal);
  }

  private void addValuesTo(final Map<Object, long[]> counts) {
    if (nodes != null)
      nodes.forEachNonZero((node, count) -> counts.computeIfAbsent(view.value(node), k -> new long[1])[0] += count);
    if (orphanOrdinals != null)
      orphanOrdinals.counter.forEachNonZero(
          (merged, count) -> counts.computeIfAbsent(FacetValueComparator.normalize(map.value(merged)), k -> new long[1])[0] += count);
    if (orphanValues != null)
      for (final Map.Entry<Object, long[]> entry : orphanValues.entrySet())
        counts.computeIfAbsent(entry.getKey(), k -> new long[1])[0] += entry.getValue()[0];
  }

  /** The bucket of a value: a node of the first collector whose view has it, else an orphan root. */
  private FacetBucket valueBucket(final Object value, final long count) {
    for (final HierarchyFacetCollector collector : all())
      if (collector.view != null) {
        final int node = collector.view.nodeOfValue(value);
        if (node >= 0)
          return nodeBucket(collector.view, node, count);
      }
    final JSONObject attributes = new JSONObject();
    attributes.put(PARENT, (Object) null);
    attributes.put(DEPTH, 0);
    attributes.put(ORPHAN, true);
    return FacetBucket.term(value, count).withAttributes(attributes);
  }

  private static FacetBucket nodeBucket(final HierarchyView view, final int node, final long count) {
    final int first = view.parentOffsets()[node];
    final Object parent = first < view.parentOffsets()[node + 1] ? view.value(view.parents()[first]) : null;
    final JSONObject attributes = new JSONObject();
    attributes.put(PARENT, parent);
    attributes.put(DEPTH, view.depth(node));
    if (view.orphan(node))
      attributes.put(ORPHAN, true);
    return FacetBucket.term(view.value(node), count).withAttributes(attributes);
  }
}
