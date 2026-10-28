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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LocalSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Owner of the columnar projections of one database: one {@link ColumnarProjection} per bucket that has a {@link ColumnarIndex}, the
 * routing of dirty page marks to their trackers, the scheduling of apply passes and of flush and merge, and the lifecycle (open with
 * the schema, close, kill, drop). One instance per {@link LocalDatabase}, created before the schema loads.
 * <p>
 * <b>Registration.</b> {@link #register} records an index; its projection is opened once the index is bound to its bucket and the
 * schema publishes it: at once for an index bound outside a schema load, by {@link #openRegistered()} for one the DDL binds after the
 * factory registered it ({@link ColumnarIndex#build}), and by {@link #schemaLoadEnded} for the indexes a schema load registered (the
 * load stages its components, so a bucket or type lookup made during it answers the previous generation). Registration is
 * idempotent per index file id: a full schema reload registers a new {@link ColumnarIndex} per file id without unregistering the
 * superseded one, and the existing projection is kept (V-B4: a new column of a projected bucket is added with
 * {@link ColumnarProjection#addColumn}). Opening never throws to the registering caller (a schema load has published its generation by
 * then): a bucket that fails is logged, reported unusable and retried by later passes with a backoff, and the other buckets open.
 * <p>
 * <b>Marks.</b> {@link #markPages} is on the commit path: one volatile read and one array read for a file with no projection, an atomic
 * OR per page for a projected one; a database with no projection and no watcher is not even called ({@link #hasMarkTargets()}). A
 * mark on an external-property bucket goes to the owning projection's external tracker (R7). A file
 * with no projection may be watched ({@link #watchFiles}): its marks bump the watcher's generation synchronously and the watcher runs
 * later on {@link ColumnarApplyPool}, coalesced.
 * <p>
 * <b>Scheduling (P53).</b> {@link #startApplying()} registers one tick with {@link ColumnarApplyPool}. The tick takes no lock: it reads
 * the trackers' pending clocks (volatile) and CASes {@link #passScheduled} to submit at most one pass of this database at a time,
 * when a tracker has pending pages, a watcher is pending, or a flush interval went by since the last pass. A pass applies the pending
 * pages of each projection ({@link ColumnarApplier}), runs the pending watchers, and queues on {@link ColumnarMaintenancePool} the
 * flush, then merge, of each projection that needs one, at most one job per projection. A busy maintenance slot is no failure; a
 * failed flush or merge delays the next attempt of that projection exponentially ({@link #MAINTENANCE_BACKOFF_BASE_MS}, doubled per
 * consecutive failure up to {@link #MAINTENANCE_BACKOFF_MAX_MS}); a merge failing {@link #CORRUPT_MERGES_BEFORE_REBUILD} times in a
 * row with a {@link BlockCorruptionException} marks the projection {@code corrupt}, which requests its rebuild.
 * <p>
 * READ_ONLY databases (R16): no tick, no flush, no file written or deleted.
 */
public final class ColumnarIndexManager {
  /** Delay after the first failed flush or merge of a projection; doubled per consecutive failure. */
  static final long MAINTENANCE_BACKOFF_BASE_MS   = 1_000L;
  /** Longest delay between two attempts of a failing flush or merge. */
  static final long MAINTENANCE_BACKOFF_MAX_MS    = 60_000L;
  /** Merges of a projection failing in a row with a {@link BlockCorruptionException} after which it is marked {@code corrupt}. */
  static final int  CORRUPT_MERGES_BEFORE_REBUILD = 3;
  /** Bound of the drain of {@link #close()}, of the wait of {@link #suspendApply()} for a running pass, and of {@link #flushNow()}. */
  static final long DRAIN_TIMEOUT_MS              = 30_000L;
  /** Reached before the projection of a bucket is opened, or given a new column, by the registration. */
  static final String OPEN_PROJECTION = "manager.openProjection";

  /** Permits of {@link #fallbackScans()}; plan 4 reads them from {@code arcadedb.columnar.maxFallbackScans}. */
  private static final int    FALLBACK_SCANS  = 2;
  private static final long   LOG_THROTTLE_MS = 60_000L;
  private static final long[] NO_SEGMENTS     = new long[0];

  /** The projection a mark of each file id goes to, and whether that file is the projection's external-property bucket. */
  private record Routes(ColumnarProjection[] byFileId, boolean[] externalFileId) {
    static final Routes EMPTY = new Routes(new ColumnarProjection[0], new boolean[0]);
  }

  /** One projection with its maintenance state. */
  private static final class Slot {
    final ColumnarProjection projection;
    /** Set while a maintenance job of the projection is queued or running: the hand-over of the plain fields below. */
    final AtomicBoolean      maintenanceQueued = new AtomicBoolean();
    /** Jobs inside their body on this slot (a scheduled job past its start, a {@link #flushNow()}): what a close stops and waits for. */
    final AtomicInteger      running           = new AtomicInteger();
    volatile long            nextAttemptMs;
    volatile long            backoffMs;
    volatile boolean         mergeWanted;
    int                      corruptMerges;
    // written by close() only
    boolean                  cleanlyFlushed;
    long                     noWalEpochAtClose;

    Slot(final ColumnarProjection projection) {
      this.projection = projection;
    }
  }

  /** One {@link #watchFiles} registration. */
  private static final class Watcher {
    final Runnable      onChange;
    final AtomicLong    generation = new AtomicLong();
    final AtomicBoolean pending    = new AtomicBoolean();
    int[]               fileIds    = new int[0];

    Watcher(final Runnable onChange) {
      this.onChange = onChange;
    }
  }

  private final LocalDatabase database;
  private final boolean       readOnly;
  private final ColumnarStats stats        = new ColumnarStats();
  private final Semaphore     fallbackScans = new Semaphore(FALLBACK_SCANS);
  private final AtomicLong    reconcileRequests = new AtomicLong();
  private final Runnable      tick         = this::tick;

  private final ReentrantLock                         registryLock = new ReentrantLock();
  // guarded by registryLock
  private final Map<Integer, ColumnarIndex>           indexes      = new HashMap<>();
  private final Map<Integer, ColumnarIndex>           pendingLoad  = new LinkedHashMap<>();
  private final Map<Integer, ColumnarIndex>           pendingBind  = new LinkedHashMap<>();
  private final Map<Integer, Slot>                    slotsByBucket = new HashMap<>();
  /** Columns whose projection could not be opened (or given the column), by bucket: retried by the passes, with a backoff. */
  private final Map<Integer, List<ColumnDefinition>>  failedOpens  = new LinkedHashMap<>();
  private long                                        openRetryBackoffMs;
  private boolean                                     closed;
  // published copies, replaced whole under registryLock
  private volatile Slot[]                             slots        = new Slot[0];
  private volatile Routes                             routes       = Routes.EMPTY;
  private volatile ColumnarIndex[]                    registered   = new ColumnarIndex[0];
  /** The buckets of {@link #failedOpens}, published for the lock-free readers. */
  private volatile Set<Integer>                       failedBuckets = Set.of();
  private volatile long                               nextOpenRetryMs;
  /** Replaced by a new empty map after {@link #registered} on every change, so an extractor built from a stale list is dropped. */
  private volatile Map<String, ColumnarValueExtractor> extractors  = new ConcurrentHashMap<>();

  private final Object       watchLock        = new Object();
  // replaced whole under watchLock
  private volatile Watcher[]   watchers         = new Watcher[0];
  private volatile Watcher[][] watchersByFileId = new Watcher[0][];
  private volatile boolean     watchersPending;

  /** {@link #slots} or {@link #watchers} is not empty: see {@link #hasMarkTargets()}; written by {@link #refreshMarkTargets()}. */
  private volatile boolean     markTargets;

  /** Called with the file id by every mark, before it is routed (V-M16); null outside tests. */
  static volatile IntConsumer onMarkForTesting;

  private final AtomicBoolean passScheduled       = new AtomicBoolean();
  private final AtomicInteger suspensions         = new AtomicInteger();
  /** Maintenance jobs queued or running: a test reads it; a close never waits on a merely queued job (the pool is JVM-wide). */
  private final AtomicInteger maintenanceInFlight = new AtomicInteger();
  /** Maintenance bodies entered and not ended, see {@link Slot#running}. */
  private final AtomicInteger maintenanceRunning  = new AtomicInteger();
  private volatile boolean    applying;
  private volatile boolean    closing;
  private volatile boolean    killed;
  private volatile long       lastPassMs          = System.currentTimeMillis();
  private volatile long       lastFailureLogMs;
  private final AtomicLong    passesSubmitted     = new AtomicLong();
  /** The clock of the maintenance schedule (backoffs); a test may freeze it. */
  private volatile LongSupplier clockMs           = System::currentTimeMillis;
  /** How long a clean close lets the running pass and flush jobs end, {@link #DRAIN_TIMEOUT_MS} outside tests. */
  private volatile long       drainTimeoutMs      = DRAIN_TIMEOUT_MS;

  public ColumnarIndexManager(final LocalDatabase database) {
    this.database = database;
    this.readOnly = database.getMode() == ComponentFile.MODE.READ_ONLY;
  }

  /** The database this manager belongs to. */
  public LocalDatabase getDatabase() {
    return database;
  }

  // ---------------------------------------------------------------------------------------------------------------- registration

  /**
   * Called when an index is created or loaded. Idempotent per index file id (the latest instance is kept). The projection of its bucket
   * is opened, or given the new column, at once when the index is bound to its bucket and no schema load runs on this thread; later
   * otherwise, see the class comment.
   */
  public void register(final ColumnarIndex index) {
    registryLock.lock();
    try {
      if (closed)
        return;
      if (database.getSchema() instanceof LocalSchema schema && schema.isLoadingOnCurrentThread())
        pendingLoad.put(index.getFileId(), index);
      else if (!isBound(index))
        pendingBind.put(index.getFileId(), index);
      else
        open(List.of(index));
    } finally {
      registryLock.unlock();
    }
  }

  /** Opens the projections of the registered indexes that are bound to their bucket by now (DDL, after the schema bound the index). */
  public void openRegistered() {
    registryLock.lock();
    try {
      if (closed || pendingBind.isEmpty())
        return;
      final List<ColumnarIndex> bound = new ArrayList<>();
      for (final ColumnarIndex index : pendingBind.values())
        if (isBound(index))
          bound.add(index);
      for (final ColumnarIndex index : bound)
        pendingBind.remove(index.getFileId());
      open(bound);
    } finally {
      registryLock.unlock();
    }
  }

  /**
   * Called by the schema at the end of a load, once its components are published (or the load gave up): opens the projections of the
   * indexes the load registered, or forgets them when the load did not publish.
   */
  public void schemaLoadEnded(final boolean published) {
    registryLock.lock();
    try {
      if (closed || pendingLoad.isEmpty())
        return;
      final List<ColumnarIndex> loaded = new ArrayList<>(pendingLoad.values());
      pendingLoad.clear();
      if (published)
        open(loaded);
    } finally {
      registryLock.unlock();
    }
  }

  /**
   * Called when an index is dropped. The projection of its bucket is dropped (its directory deleted) when no other registered index
   * projects that bucket; otherwise the column stays until the reconciler removes it.
   */
  public void unregister(final ColumnarIndex index) {
    registryLock.lock();
    try {
      final int fileId = index.getFileId();
      pendingLoad.remove(fileId);
      pendingBind.remove(fileId);
      if (indexes.remove(fileId) == null)
        return;
      final int bucketId = index.getAssociatedBucketId();
      final List<ColumnDefinition> failed = failedOpens.get(bucketId);
      if (failed != null) {
        failed.removeIf(column -> column.indexFileId() == fileId);
        if (failed.isEmpty())
          failedOpens.remove(bucketId);
      }
      publishRegistry();
      for (final ColumnarIndex other : indexes.values())
        if (other.getAssociatedBucketId() == bucketId)
          return;
      final Slot slot = slotsByBucket.remove(bucketId);
      if (slot == null)
        return;
      publishRegistry();
      slot.projection.drop();
    } finally {
      registryLock.unlock();
    }
  }

  /** Whether the projection of {@code index} holds a valid state to answer facets from; false while it has none. */
  public boolean isUsable(final ColumnarIndex index) {
    final int bucketId = index.getAssociatedBucketId();
    final ColumnarProjection projection = projection(bucketId);
    return projection != null && projection.isUsable() && !failedBuckets.contains(bucketId);
  }

  /** Asks for the projections to be reconciled with the COLUMNAR indexes of the schema (columns added or dropped). */
  public void requestReconcile() {
    reconcileRequests.incrementAndGet();
  }

  /** The number of {@link #requestReconcile()} calls so far. */
  long reconcileRequests() {
    return reconcileRequests.get();
  }

  /** The schema has set the bucket, type and property of the index (the factory registers it before). */
  private static boolean isBound(final ColumnarIndex index) {
    final IndexMetadata metadata = index.getMetadata();
    return metadata.associatedBucketId >= 0 && metadata.typeName != null && metadata.propertyNames != null
        && !metadata.propertyNames.isEmpty();
  }

  /** Under {@link #registryLock}: records the indexes and opens, or adds the column to, the projection of each bucket. */
  private void open(final List<ColumnarIndex> list) {
    if (list.isEmpty())
      return;
    final Map<Integer, List<ColumnDefinition>> columnsByBucket = new LinkedHashMap<>();
    for (final ColumnarIndex index : list) {
      indexes.put(index.getFileId(), index);
      columnsByBucket.computeIfAbsent(index.getAssociatedBucketId(), k -> new ArrayList<>()).add(index.columnDefinition());
    }
    boolean failed = false;
    for (final Map.Entry<Integer, List<ColumnDefinition>> entry : columnsByBucket.entrySet())
      failed |= !openBucket(entry.getKey(), entry.getValue());
    if (failed && openRetryBackoffMs == 0)
      scheduleOpenRetry();
    publishRegistry();
  }

  /**
   * Under {@link #registryLock}: opens the bucket's projection with the columns, or adds them to the open one. Never throws: a bucket
   * that fails (a write error publishing an empty projection, a bucket the schema cannot resolve, ...) is recorded in
   * {@link #failedOpens}, reported unusable and retried by a later pass, while the other buckets go on.
   *
   * @return false when the bucket failed
   */
  private boolean openBucket(final int bucketId, final List<ColumnDefinition> columns) {
    try {
      ColumnarTestHooks.reach(OPEN_PROJECTION);
      final Slot slot = slotsByBucket.get(bucketId);
      if (slot != null)
        for (final ColumnDefinition column : columns)
          slot.projection.addColumn(column);
      else {
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId);
        slotsByBucket.put(bucketId, new Slot(ColumnarProjection.open(database, bucket, columns.toArray(new ColumnDefinition[0]), stats)));
      }
      return true;
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING, "Cannot open the columnar projection of bucket %d of database '%s', retried later", e,
          bucketId, database.getName());
      final List<ColumnDefinition> failed = failedOpens.computeIfAbsent(bucketId, k -> new ArrayList<>());
      for (final ColumnDefinition column : columns)
        if (failed.stream().noneMatch(f -> f.indexFileId() == column.indexFileId()))
          failed.add(column);
      return false;
    }
  }

  /** Under {@link #registryLock}: the next retry of the failed opens, the delay doubling per failed retry like a failed flush. */
  private void scheduleOpenRetry() {
    openRetryBackoffMs = openRetryBackoffMs == 0 ? MAINTENANCE_BACKOFF_BASE_MS : Math.min(openRetryBackoffMs * 2, MAINTENANCE_BACKOFF_MAX_MS);
    nextOpenRetryMs = clockMs.getAsLong() + openRetryBackoffMs;
  }

  /** A retry of a failed open is due: lock free, so the tick can test it. */
  private boolean isOpenRetryDue() {
    return !failedBuckets.isEmpty() && clockMs.getAsLong() >= nextOpenRetryMs;
  }

  /** A pass: retries the failed opens once their backoff elapsed. */
  private void retryFailedOpens() {
    if (!isOpenRetryDue())
      return;
    final List<ColumnarProjection> reopened = new ArrayList<>();
    registryLock.lock();
    try {
      if (closed || failedOpens.isEmpty())
        return;
      final Map<Integer, List<ColumnDefinition>> retry = new LinkedHashMap<>(failedOpens);
      failedOpens.clear();
      boolean failed = false;
      for (final Map.Entry<Integer, List<ColumnDefinition>> entry : retry.entrySet()) {
        final boolean hadProjection = slotsByBucket.containsKey(entry.getKey());
        if (!openBucket(entry.getKey(), entry.getValue()))
          failed = true;
        else if (!hadProjection && !readOnly) {
          // the marks of the commits made while the bucket had no projection were dropped: the files it loaded miss them, so it
          // is served only once rebuilt. MISSING: the projection lacks rows, nothing failed a check. Unusable BEFORE the routes
          // publish it, the rebuild requested AFTER, so the rebuild scan starts once the marks reach the projection (Task 16: the
          // rebuild must keep the rows applied after this point). READ_ONLY: no mark can be dropped, no rebuild would run
          final ColumnarProjection projection = slotsByBucket.get(entry.getKey()).projection;
          projection.markUnusableWithoutRequest(ColumnarProjection.MISSING);
          reopened.add(projection);
        }
      }
      if (failed)
        scheduleOpenRetry();
      else
        openRetryBackoffMs = 0;
      publishRegistry();
    } finally {
      registryLock.unlock();
    }
    for (final ColumnarProjection projection : reopened)
      projection.requestRebuild(ColumnarProjection.MISSING);
  }

  /** Under {@link #registryLock}: publishes the indexes, the slots and the routes of the current projections. */
  private void publishRegistry() {
    registered = indexes.values().toArray(new ColumnarIndex[0]);
    failedBuckets = Set.copyOf(failedOpens.keySet());
    extractors = new ConcurrentHashMap<>();
    final Slot[] next = slotsByBucket.values().toArray(new Slot[0]);
    int length = 0;
    for (final Slot slot : next)
      length = Math.max(length, Math.max(slot.projection.bucketId(), slot.projection.externalFileId()) + 1);
    final ColumnarProjection[] byFileId = new ColumnarProjection[length];
    final boolean[] external = new boolean[length];
    for (final Slot slot : next) {
      byFileId[slot.projection.bucketId()] = slot.projection;
      final int externalId = slot.projection.externalFileId();
      if (externalId >= 0) {
        byFileId[externalId] = slot.projection;
        external[externalId] = true;
      }
    }
    slots = next;
    routes = new Routes(byFileId, external);
    refreshMarkTargets();
  }

  /**
   * After {@link #routes} or {@link #watchersByFileId} changed. One monitor for both publishers, so the last refresh reads both
   * latest values whatever the interleaving of a registry and a watcher change.
   */
  private synchronized void refreshMarkTargets() {
    markTargets = routes.byFileId.length > 0 || watchersByFileId.length > 0;
  }

  // ------------------------------------------------------------------------------------------------------------------- lookups

  /** The projection of the bucket, null when the bucket has no COLUMNAR index (or is an external-property bucket). */
  public ColumnarProjection projection(final int bucketId) {
    final Routes current = routes;
    if (bucketId < 0 || bucketId >= current.byFileId.length || current.externalFileId[bucketId])
      return null;
    return current.byFileId[bucketId];
  }

  /**
   * The extractor of the type's columns (its own COLUMNAR indexes and those of its super types), null when the type has none. One
   * extractor serves every bucket of the type: the dictionary of a record is the one of its bucket's projection (R5). It refuses, with
   * an {@link IllegalArgumentException}, a record with no identity and a record of a bucket with no projection: a caller resolves the
   * projection of the record's bucket first and skips the record when there is none. Takes no lock (an apply pass may call it).
   */
  public ColumnarValueExtractor extractor(final String typeName) {
    // the map first, the indexes after: both are replaced in the other order, so a cached extractor is never older than its map
    final Map<String, ColumnarValueExtractor> cache = extractors;
    final ColumnarValueExtractor cached = cache.get(typeName);
    if (cached != null)
      return cached;
    final DocumentType type = database.getSchema().getTypeOrNull(typeName);
    if (type == null)
      return null;
    final Map<String, ColumnDefinition> byProperty = new LinkedHashMap<>();
    for (final ColumnarIndex index : registered)
      if (type.instanceOf(index.getTypeName()))
        byProperty.putIfAbsent(index.columnDefinition().property(), index.columnDefinition());
    if (byProperty.isEmpty())
      return null;
    final ColumnarValueExtractor created = new ColumnarValueExtractor(typeName, byProperty.values().toArray(new ColumnDefinition[0]),
        this::dictionaryOf, stats);
    final ColumnarValueExtractor raced = cache.putIfAbsent(typeName, created);
    return raced != null ? raced : created;
  }

  private GlobalDictionary dictionaryOf(final int bucketId, final String property) {
    final ColumnarProjection projection = projection(bucketId);
    if (projection == null)
      throw new IllegalArgumentException("Bucket " + bucketId + " has no columnar projection, its records cannot be extracted");
    final GlobalDictionary dictionary = projection.dictionary(property);
    if (dictionary == null)
      throw new IllegalArgumentException("The columnar projection of bucket " + bucketId + " has no dictionary of '" + property + "' yet");
    return dictionary;
  }

  public ColumnarStats getStats() {
    return stats;
  }

  /** Conversion failures counted for the property of the type, see {@link ColumnarStats#conversionFailures}. */
  public long conversionFailures(final String type, final String property) {
    return stats.conversionFailures(type, property);
  }

  /** The permits of the queries of this database that scan records because no projection can answer them; one instance per database. */
  public Semaphore fallbackScans() {
    return fallbackScans;
  }

  // ---------------------------------------------------------------------------------------------------------------------- marks

  /**
   * True when a mark can reach something: a projection or a watched file. The commit path, Raft apply and WAL replay read it first,
   * so a database with no COLUMNAR index and no watcher pays this one volatile read per commit and nothing else.
   */
  public boolean hasMarkTargets() {
    return markTargets;
  }

  /** The file is a projected bucket or the external-property bucket of one: its marks are what a crash must not lose. Lock free. */
  public boolean isProjected(final int fileId) {
    final Routes current = routes;
    return fileId >= 0 && fileId < current.byFileId.length && current.byFileId[fileId] != null;
  }

  /** Packs a page for {@link #markPackedPages}: sorting the keys groups them by file id, then by page number. */
  public static long packPage(final int fileId, final int pageNumber) {
    return ((long) fileId << 32) | (pageNumber & 0xFFFFFFFFL);
  }

  /** True when one of the first {@code count} {@link #packPage packed} keys, sorted, is on a {@link #isProjected projected} file. */
  public boolean projectsAnyPackedPage(final long[] sortedKeys, final int count) {
    int previous = -1;
    for (int i = 0; i < count; i++) {
      final int fileId = (int) (sortedKeys[i] >>> 32);
      if (fileId != previous && isProjected(fileId))
        return true;
      previous = fileId;
    }
    return false;
  }

  /**
   * Marks the first {@code count} {@link #packPage packed} keys, sorted, one {@link #markPages} (or {@link #markPagesWithoutWAL}) call
   * per file. {@code buffer} receives the page numbers of one file at a time, so it holds {@code count} ints at least: nothing is
   * allocated. Every file is marked even when the no-WAL marker of one cannot be written; the first such failure is thrown after.
   *
   * @throws UncheckedIOException with {@code withoutWAL}, when the {@code NOWAL} marker of a projection cannot be written
   */
  public void markPackedPages(final long[] sortedKeys, final int count, final int[] buffer, final boolean withoutWAL) {
    UncheckedIOException failure = null;
    for (int i = 0; i < count; ) {
      final int fileId = (int) (sortedKeys[i] >>> 32);
      int pages = 0;
      while (i < count && (int) (sortedKeys[i] >>> 32) == fileId)
        buffer[pages++] = (int) sortedKeys[i++];
      if (!withoutWAL)
        markPages(fileId, buffer, pages);
      else
        try {
          markPagesWithoutWAL(fileId, buffer, pages);
        } catch (final UncheckedIOException e) {
          if (failure == null)
            failure = e;
        }
    }
    if (failure != null)
      throw failure;
  }

  /** Marks the pages of the file dirty (commit, Raft apply, WAL replay). No lock; nothing allocated. */
  public void markPages(final int fileId, final int[] pageNumbers, final int count) {
    mark(fileId, pageNumbers, count);
  }

  /**
   * {@link #markPages} for a commit that wrote no WAL (R8, spec 6.5): the pages are marked, then the no-WAL epoch of the projection is
   * advanced (in this order, so a pass cannot drain the epoch before the mark lands), then, under the projection's
   * {@code noWalMarkerLock} and only when no marker is current, {@code <bucketDir>/NOWAL} is written synchronously before this returns
   * (R25: one fsynced write per projection until a clean close deletes the marker; a flush never does). The marker is the record that
   * the bucket pages of a no-WAL commit may reach the disk after the projection's rows: an unclean open finding it rebuilds.
   *
   * @throws UncheckedIOException when the marker cannot be written; the projection is then marked unusable, which requests its rebuild,
   *                              and the next no-WAL mark tries the marker again
   */
  public void markPagesWithoutWAL(final int fileId, final int[] pageNumbers, final int count) {
    final ColumnarProjection projection = mark(fileId, pageNumbers, count);
    if (projection == null)
      return;
    final long epoch = projection.bumpNoWalEpoch();
    synchronized (projection.noWalMarkerLock) {
      if (projection.noWalMarkerEpoch != -1)
        return;
      final Path marker = projection.bucketDir().resolve(ColumnarStore.NOWAL_MARKER);
      try {
        ColumnarStore.writeAtomically(marker, new byte[0]);
        projection.noWalMarkerEpoch = epoch;
      } catch (final IOException e) {
        // a projection dropped meanwhile has no directory and nothing left to protect
        if (!Files.isDirectory(projection.bucketDir()))
          return;
        LogManager.instance().log(this, Level.SEVERE, "Cannot write the NOWAL marker of the columnar projection of bucket '%s'", e,
            projection.bucketName());
        projection.markUnusable(ColumnarProjection.MISSING);
        throw new UncheckedIOException("Cannot write the NOWAL marker of the columnar projection of bucket '" + projection.bucketName()
            + "'", e);
      }
    }
  }

  /** Marks into the tracker the file id routes to and returns its projection; notifies the watchers of a file with no projection. */
  private ColumnarProjection mark(final int fileId, final int[] pageNumbers, final int count) {
    final IntConsumer hook = onMarkForTesting;
    if (hook != null)
      hook.accept(fileId);
    final Routes current = routes;
    if (fileId >= 0 && fileId < current.byFileId.length) {
      final ColumnarProjection projection = current.byFileId[fileId];
      if (projection != null) {
        (current.externalFileId[fileId] ? projection.externalTracker() : projection.tracker()).mark(pageNumbers, count);
        return projection;
      }
    }
    final Watcher[][] byFile = watchersByFileId;
    if (fileId >= 0 && fileId < byFile.length && byFile[fileId] != null) {
      for (final Watcher watcher : byFile[fileId]) {
        watcher.generation.incrementAndGet();
        watcher.pending.set(true);
      }
      watchersPending = true;
    }
    return null;
  }

  /**
   * Plan 5: runs {@code onChange} on {@link ColumnarApplyPool} after marks on any of the files, which must have no projection (a
   * projected file's marks go to its tracker). Coalesced: one run per pass whatever the number of marks. Calling it again with the
   * same {@code onChange} adds files.
   */
  public void watchFiles(final int[] fileIds, final Runnable onChange) {
    synchronized (watchLock) {
      Watcher watcher = find(onChange);
      if (watcher == null) {
        watcher = new Watcher(onChange);
        final Watcher[] next = Arrays.copyOf(watchers, watchers.length + 1);
        next[watchers.length] = watcher;
        watchers = next;
      }
      final int[] merged = Arrays.copyOf(watcher.fileIds, watcher.fileIds.length + fileIds.length);
      System.arraycopy(fileIds, 0, merged, watcher.fileIds.length, fileIds.length);
      watcher.fileIds = Arrays.stream(merged).filter(id -> id >= 0).distinct().toArray();
      publishWatchers();
    }
  }

  /** Removes the watcher of {@link #watchFiles}; it does not run again once a pass running it ends. */
  public void unwatchFiles(final Runnable onChange) {
    synchronized (watchLock) {
      final Watcher watcher = find(onChange);
      if (watcher == null)
        return;
      final Watcher[] next = new Watcher[watchers.length - 1];
      int i = 0;
      for (final Watcher w : watchers)
        if (w != watcher)
          next[i++] = w;
      watchers = next;
      publishWatchers();
    }
  }

  /**
   * Plan 5: the number of marks on the watched files of {@code onChange} so far, incremented on the marking thread before
   * {@link #markPages} returns; 0 for an unknown watcher.
   */
  public long watchGeneration(final Runnable onChange) {
    final Watcher watcher = find(onChange);
    return watcher == null ? 0 : watcher.generation.get();
  }

  private Watcher find(final Runnable onChange) {
    for (final Watcher watcher : watchers)
      if (watcher.onChange == onChange)
        return watcher;
    return null;
  }

  /** Under {@link #watchLock}. */
  private void publishWatchers() {
    int length = 0;
    for (final Watcher watcher : watchers)
      for (final int fileId : watcher.fileIds)
        length = Math.max(length, fileId + 1);
    final Watcher[][] byFile = new Watcher[length][];
    for (final Watcher watcher : watchers)
      for (final int fileId : watcher.fileIds) {
        final Watcher[] current = byFile[fileId];
        if (current == null)
          byFile[fileId] = new Watcher[] { watcher };
        else {
          final Watcher[] grown = Arrays.copyOf(current, current.length + 1);
          grown[current.length] = watcher;
          byFile[fileId] = grown;
        }
      }
    watchersByFileId = byFile;
    refreshMarkTargets();
  }

  // ----------------------------------------------------------------------------------------------------------------- scheduling

  /** After the WAL replay: registers the tick of this database with {@link ColumnarApplyPool}. Never on a READ_ONLY database. */
  public void startApplying() {
    if (readOnly || closing || applying)
      return;
    ColumnarApplyPool.getInstance().register(tick, () -> database.getConfiguration().getValueAsLong(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL));
    applying = true;
  }

  /** The tick is registered with {@link ColumnarApplyPool}. */
  boolean isApplyingForTesting() {
    return applying;
  }

  /** The tick this manager registers, to call it from a test thread. */
  Runnable tickForTesting() {
    return tick;
  }

  /** No lock: volatile reads, then a CAS, then a submit. */
  private void tick() {
    // a database with no COLUMNAR index pays these reads only, never a configuration read or a pass
    if (slots.length == 0 && !watchersPending && failedBuckets.isEmpty() || closing || suspensions.get() > 0)
      return;
    if (!watchersPending && !hasPendingApply() && !isOpenRetryDue()
        && System.currentTimeMillis() - lastPassMs < database.getConfiguration().getValueAsLong(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL))
      return;
    if (passScheduled.compareAndSet(false, true))
      try {
        ColumnarApplyPool.getInstance().getExecutorService().execute(this::pass);
        passesSubmitted.incrementAndGet();
      } catch (final RejectedExecutionException e) {
        passScheduled.set(false);
      }
  }

  private boolean hasPendingApply() {
    for (final Slot slot : slots)
      if (hasPendingApply(slot.projection))
        return true;
    return false;
  }

  /** Pages in an epoch or in flight; lock free. */
  private static boolean hasPendingApply(final ColumnarProjection projection) {
    final DirtyPageTracker external = projection.externalTracker();
    return projection.tracker().oldestPendingEpochStartMs() != 0 || external != null && external.oldestPendingEpochStartMs() != 0;
  }

  /** One pass of this database, on {@link ColumnarApplyPool}. */
  private void pass() {
    try {
      if (closing || suspensions.get() > 0)
        return;
      lastPassMs = System.currentTimeMillis();
      for (final Slot slot : slots) {
        if (closing || suspensions.get() > 0)
          return;
        if (hasPendingApply(slot.projection))
          apply(slot.projection);
      }
      runWatchers();
      retryFailedOpens();
      scheduleMaintenance();
    } catch (final Throwable e) {
      logFailure("Columnar apply pass of database '%s' failed", e);
    } finally {
      passScheduled.set(false);
    }
  }

  private void apply(final ColumnarProjection projection) {
    try {
      ColumnarApplier.runPass(this, projection);
    } catch (final RuntimeException e) {
      // the pages stay in flight and are offered again by the next pass
      logFailure("Columnar apply pass of database '%s' failed on bucket " + projection.bucketName(), e);
    }
  }

  private void runWatchers() {
    if (!watchersPending)
      return;
    watchersPending = false;
    for (final Watcher watcher : watchers)
      if (watcher.pending.getAndSet(false))
        try {
          watcher.onChange.run();
        } catch (final RuntimeException e) {
          logFailure("Columnar file watcher of database '%s' failed", e);
        }
  }

  /** Queues the flush and merge of every projection that needs one and is not waiting for a backoff; at most one job per projection. */
  private void scheduleMaintenance() {
    if (readOnly)
      return;
    final long now = clockMs.getAsLong();
    final long intervalMs = database.getConfiguration().getValueAsLong(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL);
    final int maxRows = database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS);
    for (final Slot slot : slots) {
      final ColumnarProjection projection = slot.projection;
      if (closing || now < slot.nextAttemptMs || projection.manifest() == null || projection.isClosedOrDropped())
        continue;
      if (!slot.mergeWanted && !ColumnarFlusher.shouldFlush(projection, now, intervalMs, maxRows))
        continue;
      if (!slot.maintenanceQueued.compareAndSet(false, true))
        continue;
      maintenanceInFlight.incrementAndGet();
      try {
        ColumnarMaintenancePool.getInstance().getExecutorService().execute(() -> {
          try {
            runMaintenanceBody(slot, () -> maintain(slot));
          } finally {
            slot.maintenanceQueued.set(false);
            maintenanceInFlight.decrementAndGet();
          }
        });
      } catch (final RejectedExecutionException e) {
        slot.maintenanceQueued.set(false);
        maintenanceInFlight.decrementAndGet();
      }
    }
  }

  /**
   * Runs a maintenance body counted as running. Registered BEFORE {@link #closing} is read, as a close sets {@link #closing} before it
   * reads the running counts: a body that misses the close is seen running by it, and a job that only waited in the queue exits here
   * without touching the projection.
   */
  private void runMaintenanceBody(final Slot slot, final Runnable body) {
    slot.running.incrementAndGet();
    maintenanceRunning.incrementAndGet();
    try {
      if (!closing)
        body.run();
    } finally {
      slot.running.decrementAndGet();
      maintenanceRunning.decrementAndGet();
    }
  }

  /** Flush, then merge when the merge policy selects segments. Runs on {@link ColumnarMaintenancePool}, or a test thread. */
  private void maintain(final Slot slot) {
    final ColumnarProjection projection = slot.projection;
    if (!projection.canWrite() || projection.isClosedOrDropped())
      return;
    boolean merging = false;
    try {
      ColumnarFlusher.flush(projection, database.getLastTransactionId());
      final long[] selected = selectMerge(projection);
      slot.mergeWanted = selected.length > 0;
      if (selected.length > 0) {
        merging = true;
        ColumnarFlusher.merge(projection, selected);
        merging = false;
        slot.corruptMerges = 0;
        slot.mergeWanted = selectMerge(projection).length > 0;
      }
      slot.backoffMs = 0;
      slot.nextAttemptMs = 0;
    } catch (final ColumnarFlusher.MaintenanceBusyException e) {
      // another flush, merge or compaction of the projection runs: not a failure, the next pass retries
    } catch (final BlockCorruptionException e) {
      if (merging && ++slot.corruptMerges >= CORRUPT_MERGES_BEFORE_REBUILD) {
        slot.corruptMerges = 0;
        slot.mergeWanted = false;
        failed(slot, e);
        projection.markUnusable(ColumnarProjection.CORRUPT);
      } else
        failed(slot, e);
    } catch (final IOException | RuntimeException e) {
      if (merging)
        slot.corruptMerges = 0;
      failed(slot, e);
    }
  }

  /** The segments the next merge rewrites, none while the projection has no valid state to merge. */
  private long[] selectMerge(final ColumnarProjection projection) {
    final ColumnarManifest manifest = projection.manifest();
    if (manifest == null || !projection.isUsable())
      return NO_SEGMENTS;
    return MergePolicy.select(manifest.segments(),
        database.getConfiguration().getValueAsFloat(GlobalConfiguration.COLUMNAR_MERGE_DEAD_RATIO),
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS));
  }

  private void failed(final Slot slot, final Exception e) {
    final long backoff = slot.backoffMs == 0 ? MAINTENANCE_BACKOFF_BASE_MS : Math.min(slot.backoffMs * 2, MAINTENANCE_BACKOFF_MAX_MS);
    slot.backoffMs = backoff;
    slot.nextAttemptMs = clockMs.getAsLong() + backoff;
    logFailure("Columnar flush or merge of database '%s' failed on bucket " + slot.projection.bucketName() + ", retried in " + backoff
        + " ms", e);
  }

  /** Runs the flush and merge job of the bucket's projection on the calling thread, whatever its schedule. */
  void maintainForTesting(final int bucketId) {
    maintain(slot(bucketId));
  }

  /** Runs the maintenance scheduling of a pass on the calling thread: the jobs it queues run on {@link ColumnarMaintenancePool}. */
  void scheduleMaintenanceForTesting() {
    scheduleMaintenance();
  }

  /** No maintenance job of this database is queued or running. */
  boolean isMaintenanceIdleForTesting() {
    return maintenanceInFlight.get() == 0;
  }

  /** Freezes, or moves, the clock of the maintenance schedule. */
  void setClockForTesting(final LongSupplier clock) {
    this.clockMs = clock;
  }

  /** Shortens the time a clean close lets a running job end before abandoning it. */
  void setDrainTimeoutForTesting(final long ms) {
    this.drainTimeoutMs = ms;
  }

  /** Passes the tick submitted so far. */
  long passesSubmittedForTesting() {
    return passesSubmitted.get();
  }

  /** The delay before the next attempt of a failing flush or merge of the bucket's projection, 0 after a success. */
  long maintenanceBackoffMsForTesting(final int bucketId) {
    return slot(bucketId).backoffMs;
  }

  private Slot slot(final int bucketId) {
    registryLock.lock();
    try {
      return slotsByBucket.get(bucketId);
    } finally {
      registryLock.unlock();
    }
  }

  private void logFailure(final String message, final Throwable e) {
    final long now = System.currentTimeMillis();
    if (now - lastFailureLogMs < LOG_THROTTLE_MS)
      return;
    lastFailureLogMs = now;
    LogManager.instance().log(this, Level.SEVERE, message, e, database.getName());
  }

  /**
   * Holds every apply pass of this database until the returned handle is closed (idempotent); a pass running when this is called is
   * waited for. Marks keep accumulating, so the lag is observable. Holds may nest.
   *
   * @throws IllegalStateException when the running pass does not end within {@link #DRAIN_TIMEOUT_MS} (nothing is held then)
   */
  public AutoCloseable suspendApply() {
    suspensions.incrementAndGet();
    if (!awaitNoPass(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DRAIN_TIMEOUT_MS))) {
      suspensions.decrementAndGet();
      LogManager.instance().log(this, Level.WARNING, "The running columnar apply pass of database '%s' did not end within %d ms", null,
          database.getName(), DRAIN_TIMEOUT_MS);
      throw new IllegalStateException("The running columnar apply pass of database '" + database.getName() + "' did not end within "
          + DRAIN_TIMEOUT_MS + " ms");
    }
    final AtomicBoolean released = new AtomicBoolean();
    return () -> {
      if (released.compareAndSet(false, true))
        suspensions.decrementAndGet();
    };
  }

  /**
   * Waits until every projection is usable and no page is pending (in an epoch or in flight): every mark that returned before the call
   * is applied and its overlay published.
   *
   * @return false when that is not reached within {@code timeoutMs}
   */
  public boolean awaitCaughtUp(final long timeoutMs) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (!isCaughtUp()) {
      if (System.nanoTime() - deadline >= 0)
        return false;
      Thread.sleep(5);
    }
    return true;
  }

  private boolean isCaughtUp() {
    if (!failedBuckets.isEmpty())
      return false;
    for (final Slot slot : slots)
      if (!slot.projection.isUsable() || hasPendingApply(slot.projection))
        return false;
    return true;
  }

  /**
   * Flushes every projection now on the calling thread, even with an empty overlay (V-M16), waiting for a running flush, merge or
   * compaction of a projection to end first (for {@link #DRAIN_TIMEOUT_MS} at most). Nothing on a READ_ONLY database.
   *
   * @throws UncheckedIOException                      when a flush fails
   * @throws ColumnarFlusher.MaintenanceBusyException when another job still holds a projection's slot at the deadline
   */
  public void flushNow() throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DRAIN_TIMEOUT_MS);
    for (final Slot slot : slots) {
      // counted as running, so a close stops it and waits for it
      slot.running.incrementAndGet();
      maintenanceRunning.incrementAndGet();
      try {
        for (;;)
          try {
            ColumnarFlusher.flush(slot.projection, database.getLastTransactionId());
            break;
          } catch (final ColumnarFlusher.MaintenanceBusyException e) {
            if (System.nanoTime() - deadline >= 0)
              throw e;
            Thread.sleep(1);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
      } finally {
        slot.running.decrementAndGet();
        maintenanceRunning.decrementAndGet();
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------------ lifecycle

  /**
   * Clean close: unregisters the tick (no projection lock held), waits for the running pass and maintenance jobs, applies what is still
   * pending, flushes and publishes every projection (all bounded by {@link #DRAIN_TIMEOUT_MS} together), then closes them. A flush or
   * merge still running at the bound is stopped ({@link ColumnarProjection#stopPublishing()}: it aborts at its next step and publishes
   * nothing) and waited for, so no job writes once this returns; its projection is then closed without a close flush. After a
   * {@link #kill()}, or on a READ_ONLY database, nothing is written: the projections are only closed. Idempotent.
   */
  public void close() {
    shutdown(true);
  }

  /**
   * The close of a database being dropped: as {@link #close()} with no apply and no flush, its files are about to be deleted. A running
   * flush or merge is stopped at once and waited for, so nothing writes in the directory the drop deletes.
   */
  public void closeForDrop() {
    shutdown(false);
  }

  /** Crash simulation: unregisters the tick and stops scheduling; no I/O, no wait. A later {@link #close()} only closes. */
  public void kill() {
    killed = true;
    closing = true;
    stopApplying();
  }

  private void stopApplying() {
    if (applying) {
      ColumnarApplyPool.getInstance().unregister(tick);
      applying = false;
    }
  }

  private void shutdown(final boolean flush) {
    final Slot[] toClose;
    registryLock.lock();
    try {
      if (closed)
        return;
      closed = true;
      toClose = slots;
    } finally {
      registryLock.unlock();
    }
    closing = true;
    stopApplying();
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(drainTimeoutMs);
    awaitNoPass(deadline);
    final boolean flushing = flush && !killed && !readOnly;
    // a clean close lets a running flush end (its rows are what the close persists); a drop or a killed database does not wait
    // only RUNNING jobs are waited for: a job queued on the JVM-wide pool, maybe behind another database's long merge, exits without
    // work when it starts (closing is set)
    if (flushing)
      while (maintenanceRunning.get() > 0 && System.nanoTime() - deadline < 0)
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));

    // a job still running aborts at its next step and publishes nothing; it is waited for, so once this returns no job of this
    // database writes in its directories (a drop deletes them, a reopen in this JVM writes them)
    for (final Slot slot : toClose)
      if (slot.running.get() > 0) {
        slot.projection.stopPublishing();
        if (flushing)
          LogManager.instance().log(this, Level.WARNING, "The columnar projection of bucket '%s' of database '%s' is closed without its "
              + "close flush: its flush or merge was still running after %d ms and was abandoned", null, slot.projection.bucketName(),
              database.getName(), drainTimeoutMs);
      }
    awaitMaintenanceEnded();

    // the only closer of the projections: every running job has ended
    for (final Slot slot : toClose) {
      if (flushing)
        flushOnClose(slot, deadline);
      slot.projection.close();
    }
  }

  /** Waits for every running maintenance body of this database to end, logging while one does not; it aborts at its next step by now. */
  private void awaitMaintenanceEnded() {
    long nextLog = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DRAIN_TIMEOUT_MS);
    while (maintenanceRunning.get() > 0) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      if (System.nanoTime() - nextLog >= 0) {
        nextLog = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DRAIN_TIMEOUT_MS);
        LogManager.instance().log(this, Level.WARNING, "Closing database '%s' waits for a columnar flush or merge to abandon", null,
            database.getName());
      }
    }
  }

  private void flushOnClose(final Slot slot, final long deadline) {
    final ColumnarProjection projection = slot.projection;
    try {
      while (hasPendingApply(projection) && System.nanoTime() - deadline < 0)
        ColumnarApplier.runPass(this, projection);
      slot.noWalEpochAtClose = projection.noWalEpoch();
      ColumnarFlusher.flush(projection, database.getLastTransactionId());
      final DirtyPageTracker external = projection.externalTracker();
      slot.cleanlyFlushed = projection.tracker().isEmpty() && (external == null || external.isEmpty()) && !projection.hasFrozenOverlay();
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.SEVERE, "Cannot flush the columnar projection of bucket '%s' on close", e, projection.bucketName());
    }
  }

  /**
   * The last step of a clean close, once the database flushed and synced every page: deletes the {@code NOWAL} marker of each
   * projection whose close flushed every page it was given and that took no no-WAL mark since (R8, Task 13 writes the marker). Nothing
   * after a kill, a failed close flush, or on a READ_ONLY database.
   */
  public void deleteNoWalMarkersAfterCleanClose() {
    if (readOnly || killed)
      return;
    for (final Slot slot : slots) {
      final ColumnarProjection projection = slot.projection;
      if (!slot.cleanlyFlushed || projection.noWalEpoch() != slot.noWalEpochAtClose)
        continue;
      synchronized (projection.noWalMarkerLock) {
        try {
          Files.deleteIfExists(projection.bucketDir().resolve(ColumnarStore.NOWAL_MARKER));
          projection.noWalMarkerEpoch = -1;
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.WARNING, "Cannot delete the NOWAL marker of the columnar projection of bucket '%s'", e,
              projection.bucketName());
        }
      }
    }
  }

  /** @return false when a pass is still scheduled or running at the deadline */
  private boolean awaitNoPass(final long deadline) {
    while (passScheduled.get()) {
      if (System.nanoTime() - deadline >= 0)
        return false;
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    return true;
  }
}
