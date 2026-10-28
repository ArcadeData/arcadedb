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
package com.arcadedb.index.columnar;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * The engine-side counters and gauge sources of the columnar projections of one database. Pure {@link LongAdder} / {@link AtomicLong}
 * cells keyed by type (and property or reason): no Micrometer here, the server binder reads these cells and registers the meters. A
 * cell is created on its first write; every later write to the same key is a lookup and an add, without allocation.
 */
public final class ColumnarStats {
  /** Reports one existing cell: {@code kind} is the meter name without the {@code arcadedb.columnar.} prefix; unused parts are null. */
  @FunctionalInterface
  public interface StatsKeyConsumer {
    void accept(String kind, String type, String property, String reason);
  }

  private static final String NONE = "";

  /** Count and total of a duration. */
  private static final class Duration {
    final LongAdder count = new LongAdder();
    final LongAdder nanos = new LongAdder();
  }

  /** Cells of one meter, keyed by type and a second key (property, reason, or none). */
  private static final class Table<T> {
    private final String                                        kind;
    private final boolean                                       secondIsReason;
    private final Supplier<T>                                   factory;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, T>> cells = new ConcurrentHashMap<>();

    Table(final String kind, final boolean secondIsReason, final Supplier<T> factory) {
      this.kind = kind;
      this.secondIsReason = secondIsReason;
      this.factory = factory;
    }

    T cell(final String type, final String second) {
      ConcurrentHashMap<String, T> byType = cells.get(type);
      if (byType == null)
        byType = cells.computeIfAbsent(type, k -> new ConcurrentHashMap<>());
      T cell = byType.get(second);
      if (cell == null)
        cell = byType.computeIfAbsent(second, k -> factory.get());
      return cell;
    }

    /** The cell, or null when nothing was ever written under the key (a read never creates a cell). */
    T existing(final String type, final String second) {
      final ConcurrentHashMap<String, T> byType = cells.get(type);
      return byType == null ? null : byType.get(second);
    }

    void forEachKey(final StatsKeyConsumer consumer) {
      cells.forEach((type, byType) -> byType.forEach((second, cell) -> {
        final boolean none = second.isEmpty();
        consumer.accept(kind, type, none || secondIsReason ? null : second, none || !secondIsReason ? null : second);
      }));
    }
  }

  private static Table<LongAdder> counter(final String kind, final boolean secondIsReason) {
    return new Table<>(kind, secondIsReason, LongAdder::new);
  }

  private static Table<AtomicLong> gauge(final String kind) {
    return new Table<>(kind, false, AtomicLong::new);
  }

  private final Table<LongAdder> rebuilds                = counter("rebuilds", true);
  private final Table<LongAdder> conversionFailures      = counter("conversion.failures", false);
  private final Table<LongAdder> coercedValues           = counter("conversion.coerced", false);
  private final Table<LongAdder> applyPages              = counter("apply.pages", false);
  private final Table<LongAdder> placeholdersResolved    = counter("apply.placeholders.resolved", false);
  private final Table<LongAdder> flushes                 = counter("flush.count", false);
  private final Table<LongAdder> merges                  = counter("merge.count", false);
  private final Table<LongAdder> recordsReread           = counter("apply.records.reread", false);
  private final Table<LongAdder> unreadable              = counter("apply.unreadable", false);
  private final Table<LongAdder> rebuildPagesReused      = counter("rebuild.pages.reused", false);
  private final Table<LongAdder> dictionaryCompactions   = counter("dictionary.compactions", false);
  private final Table<LongAdder> ordinalsReclaimed       = counter("dictionary.ordinals.reclaimed", false);
  private final Table<Duration>  flushDurations          = new Table<>("flush.duration", false, Duration::new);
  private final Table<Duration>  mergeDurations          = new Table<>("merge.duration", false, Duration::new);
  private final Table<AtomicLong> overlayRows            = gauge("overlay.rows");
  private final Table<AtomicLong> segments               = gauge("segments");
  private final Table<AtomicLong> openFiles              = gauge("files.open");
  private final Table<?>[]       tables                 = { rebuilds, conversionFailures, coercedValues, applyPages, placeholdersResolved, flushes,
      merges, recordsReread, unreadable, rebuildPagesReused, dictionaryCompactions, ordinalsReclaimed, flushDurations, mergeDurations,
      overlayRows, segments, openFiles };

  private final LongAdder rebuildsTotal = new LongAdder();
  private final LongAdder applyPasses   = new LongAdder();
  private final LongAdder recoveryPages = new LongAdder();
  // FACETS queries of the database (plan 4): database-wide, not keyed by type
  private final LongAdder queryPatched        = new LongAdder();
  private final LongAdder queryBudgetWaits    = new LongAdder();
  private final LongAdder queryPatchOverflows = new LongAdder();
  private final LongAdder queryFallbackScans  = new LongAdder();
  private final LongAdder queryFilterFallbacks = new LongAdder();
  // GROUP BY queries answered by a ColumnarGroupByStep (plan 5): database-wide, not keyed by type
  private final LongAdder groupByAccelerated = new LongAdder();
  private final LongAdder groupByFallbacks   = new LongAdder();

  // ---- writers

  public void rebuild(final String type, final String reason) {
    rebuilds.cell(type, reason).increment();
    rebuildsTotal.increment();
  }

  /** One value of a projected record the column kind cannot hold. Counted by the apply path only, never by read-only extraction. */
  public void conversionFailure(final String type, final String property) {
    conversionFailures.cell(type, property).increment();
  }

  /**
   * One value of a projected record held in the column as the declared type although the record stores another one (written before the
   * property was typed: a {@code 5} in a STRING property). Counted by the apply and rebuild paths only, each time they extract it, so it
   * only grows: a GROUP BY answered from the column would return the declared type where the normal plan returns the raw value.
   */
  public void coercedValue(final String type, final String property) {
    coercedValues.cell(type, property).increment();
  }

  /** One per apply pass that ran (any projection). */
  public void applyPass() {
    applyPasses.increment();
  }

  public void applyPages(final String type, final long n) {
    applyPages.cell(type, NONE).add(n);
  }

  public void placeholdersResolved(final String type, final long n) {
    placeholdersResolved.cell(type, NONE).add(n);
  }

  public void flush(final String type) {
    flushes.cell(type, NONE).increment();
  }

  public void merge(final String type) {
    merges.cell(type, NONE).increment();
  }

  public void recoveryPages(final long n) {
    recoveryPages.add(n);
  }

  /** Records a FACETS query read from the record itself to correct the projection. */
  public void queryPatched(final long n) {
    queryPatched.add(n);
  }

  /** Memory budget reservations of FACETS queries of this database that had to wait, whether granted late or finally refused. */
  public void queryBudgetWaits(final long n) {
    queryBudgetWaits.add(n);
  }

  /**
   * One FACETS query whose patch set exceeded the patch budget. It means "patch budget exceeded", not "answer inexact": a bucket the
   * caller's own open transaction wrote is then scanned instead, and that answer stays exact.
   */
  public void queryPatchOverflow() {
    queryPatchOverflows.increment();
  }

  /** One FACETS query that scanned records because the projection could not answer a bucket. */
  public void queryFallbackScan() {
    queryFallbackScans.increment();
  }

  /** One INDEX facet FILTER evaluator that answered from the rows instead of its RID set (index not ready, collation, key type, budget). */
  public void queryFilterFallback() {
    queryFilterFallbacks.increment();
  }

  /** One execution of an eligible GROUP BY answered from the projection. */
  public void groupByAccelerated() {
    groupByAccelerated.increment();
  }

  /** One execution of an eligible GROUP BY that ran its normal aggregation chain instead (switched off, or the projection declined). */
  public void groupByFallbacks() {
    groupByFallbacks.increment();
  }

  /** Every slot re-read by an apply pass: the numerator of the re-read amplification. */
  public void recordsReread(final String type, final long n) {
    recordsReread.cell(type, NONE).add(n);
  }

  public void flushDuration(final String type, final long nanos) {
    final Duration duration = flushDurations.cell(type, NONE);
    duration.count.increment();
    duration.nanos.add(nanos);
  }

  public void mergeDuration(final String type, final long nanos) {
    final Duration duration = mergeDurations.cell(type, NONE);
    duration.count.increment();
    duration.nanos.add(nanos);
  }

  public void unreadable(final String type) {
    unreadable.cell(type, NONE).increment();
  }

  public void rebuildPagesReused(final String type, final long n) {
    rebuildPagesReused.cell(type, NONE).add(n);
  }

  /** One dictionary compaction that reclaimed {@code reclaimed} ordinals. */
  public void dictionaryCompaction(final String type, final String property, final long reclaimed) {
    dictionaryCompactions.cell(type, property).increment();
    ordinalsReclaimed.cell(type, property).add(reclaimed);
  }

  // ---- gauge sources, set by the owner of the value

  public void setOverlayRows(final String type, final long rows) {
    overlayRows.cell(type, NONE).set(rows);
  }

  /** Test only: projections report through {@link #addSegments}, whose sums a set would overwrite. */
  void setSegments(final String type, final int n) {
    segments.cell(type, NONE).set(n);
  }

  /** Adds {@code delta} to the type's segment count: each bucket projection reports its own change, so the gauge is the type's sum. */
  public void addSegments(final String type, final int delta) {
    segments.cell(type, NONE).addAndGet(delta);
  }

  /** Adds {@code delta} to the type's open segment file count: each bucket projection reports its own change, so the gauge is the type's sum. */
  public void addOpenFiles(final String type, final int delta) {
    openFiles.cell(type, NONE).addAndGet(delta);
  }

  public void setOpenFiles(final String type, final int n) {
    openFiles.cell(type, NONE).set(n);
  }

  // ---- readers

  private static long sum(final LongAdder cell) {
    return cell == null ? 0 : cell.sum();
  }

  private static long value(final AtomicLong cell) {
    return cell == null ? 0 : cell.get();
  }

  public long rebuilds(final String type, final String reason) {
    return sum(rebuilds.existing(type, reason));
  }

  public long rebuildsTotal() {
    return rebuildsTotal.sum();
  }

  public long conversionFailures(final String type, final String property) {
    return sum(conversionFailures.existing(type, property));
  }

  public long coercedValues(final String type, final String property) {
    return sum(coercedValues.existing(type, property));
  }

  public long applyPasses() {
    return applyPasses.sum();
  }

  public long applyPagesCount(final String type) {
    return sum(applyPages.existing(type, NONE));
  }

  public long placeholdersResolvedCount(final String type) {
    return sum(placeholdersResolved.existing(type, NONE));
  }

  public long flushCount(final String type) {
    return sum(flushes.existing(type, NONE));
  }

  public long mergeCount(final String type) {
    return sum(merges.existing(type, NONE));
  }

  public long recoveryPagesCount() {
    return recoveryPages.sum();
  }

  public long queryPatchedCount() {
    return queryPatched.sum();
  }

  public long queryBudgetWaitsCount() {
    return queryBudgetWaits.sum();
  }

  public long queryPatchOverflowsCount() {
    return queryPatchOverflows.sum();
  }

  public long queryFallbackScansCount() {
    return queryFallbackScans.sum();
  }

  public long queryFilterFallbacksCount() {
    return queryFilterFallbacks.sum();
  }

  public long groupByAcceleratedCount() {
    return groupByAccelerated.sum();
  }

  public long groupByFallbacksCount() {
    return groupByFallbacks.sum();
  }

  public long recordsRereadCount(final String type) {
    return sum(recordsReread.existing(type, NONE));
  }

  public long unreadableCount(final String type) {
    return sum(unreadable.existing(type, NONE));
  }

  public long rebuildPagesReusedCount(final String type) {
    return sum(rebuildPagesReused.existing(type, NONE));
  }

  public long flushDurationCount(final String type) {
    final Duration duration = flushDurations.existing(type, NONE);
    return duration == null ? 0 : duration.count.sum();
  }

  public long flushDurationTotalNanos(final String type) {
    final Duration duration = flushDurations.existing(type, NONE);
    return duration == null ? 0 : duration.nanos.sum();
  }

  public long mergeDurationCount(final String type) {
    final Duration duration = mergeDurations.existing(type, NONE);
    return duration == null ? 0 : duration.count.sum();
  }

  public long mergeDurationTotalNanos(final String type) {
    final Duration duration = mergeDurations.existing(type, NONE);
    return duration == null ? 0 : duration.nanos.sum();
  }

  public long dictionaryCompactions(final String type, final String property) {
    return sum(dictionaryCompactions.existing(type, property));
  }

  public long dictionaryOrdinalsReclaimed(final String type, final String property) {
    return sum(ordinalsReclaimed.existing(type, property));
  }

  public long overlayRows(final String type) {
    return value(overlayRows.existing(type, NONE));
  }

  public int segments(final String type) {
    return (int) value(segments.existing(type, NONE));
  }

  public int openFiles(final String type) {
    return (int) value(openFiles.existing(type, NONE));
  }

  /** Whether any cell was ever written; {@code recovery.pages}, a database-wide counter, is not a cell. */
  public boolean hasCells() {
    for (final Table<?> table : tables)
      if (!table.cells.isEmpty())
        return true;
    return false;
  }

  /**
   * Reports every cell that exists, once, for the binder to discover what to register. The global {@code recovery.pages} cell is
   * always reported (type null).
   */
  public void forEachKey(final StatsKeyConsumer consumer) {
    rebuilds.forEachKey(consumer);
    conversionFailures.forEachKey(consumer);
    coercedValues.forEachKey(consumer);
    applyPages.forEachKey(consumer);
    placeholdersResolved.forEachKey(consumer);
    flushes.forEachKey(consumer);
    merges.forEachKey(consumer);
    recordsReread.forEachKey(consumer);
    unreadable.forEachKey(consumer);
    rebuildPagesReused.forEachKey(consumer);
    dictionaryCompactions.forEachKey(consumer);
    ordinalsReclaimed.forEachKey(consumer);
    flushDurations.forEachKey(consumer);
    mergeDurations.forEachKey(consumer);
    overlayRows.forEachKey(consumer);
    segments.forEachKey(consumer);
    openFiles.forEachKey(consumer);
    consumer.accept("recovery.pages", null, null, null);
  }
}
