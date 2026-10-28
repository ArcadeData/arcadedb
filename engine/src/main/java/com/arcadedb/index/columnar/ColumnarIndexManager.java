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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.index.Index;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntHashSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
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
 * when a tracker has pending pages, a watcher is pending, the retry of a page with an unreadable record is due, or a flush interval went
 * by since the last pass. A pass applies the pending pages of each projection ({@link ColumnarApplier}), runs the pending watchers, and
 * queues on {@link ColumnarMaintenancePool} the flush, then merge, of each projection that needs one, at most one job per projection.
 * A busy maintenance slot is no failure; a failed flush or merge delays the next attempt of that projection exponentially
 * ({@link #MAINTENANCE_BACKOFF_BASE_MS}, doubled per
 * consecutive failure up to {@link #MAINTENANCE_BACKOFF_MAX_MS}); a merge failing {@link #CORRUPT_MERGES_BEFORE_REBUILD} times in a
 * row with a {@link BlockCorruptionException} marks the projection {@code corrupt}, which requests its rebuild.
 * <p>
 * <b>Startup and recovery (spec 6.5).</b> {@link #startApplying(boolean)} runs once, after the WAL replay: the projections that need a
 * rebuild get one ({@code columnar/REBUILD} marker, then {@code missing} or {@code corrupt} files, then a {@code NOWAL} marker on an
 * unclean open), the others take the pages of {@code columnar/recovery.pages} back into their trackers; a clean open or a crash
 * recovery never counts a rebuild. Only then is the rebuild requester of each projection installed ({@link ColumnarRebuilder}), so a
 * projection a live DDL registers later is never rebuilt as {@code missing}. {@link #persistRecoverySet()} writes the pages every
 * tracker holds before a WAL file is dropped and at a clean close.
 * <p>
 * READ_ONLY databases (R16): no tick, no flush, no rebuild, no file written or deleted; the startup only records why a projection is
 * not usable.
 * <p>
 * <b>Schema events (spec 6.4).</b> The schema calls {@link #requestReconcile()} after each change; the next pass compares the COLUMNAR
 * columns of the schema, and the fingerprints of the types the hierarchies read, with those of the previous reconcile
 * ({@link ColumnarReconciler}) and applies the differences: projections dropped, columns dropped, renamed or retyped, {@code create}
 * rebuilds of columns no build covered. {@link #onSchemaEvent} takes the events sent from outside (a {@code check_fix} rebuild), and
 * every event reaches the {@link #addSchemaEventListener listeners}.
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
  /** Reached by {@link #persistRecoverySet()} once the recovery set is renamed, before it returns to {@code cleanWALFiles}. */
  static final String RECOVERY_SET_PERSISTED = "recoverySet.persisted";
  /** Reached by the reconcile of a pass before it reads the schema. */
  static final String RECONCILE = "manager.reconcile";

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
  /** The value of {@link #reconcileRequests} the last ended reconcile started from; written by the pass thread (one per database). */
  private volatile long       reconciledRequests;
  /** The last reconcile recorded definitions or hierarchy fingerprints: one is due on request even with no projection left. */
  private volatile boolean    reconcileTracked;
  private final List<Consumer<ColumnarSchemaEvent>> schemaListeners = new CopyOnWriteArrayList<>();
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
  /** The definitions and the hierarchy fingerprints of the last reconcile (V-B4, R20). */
  private Map<Integer, ColumnDefinition[]>            lastDefinitions = Map.of();
  private Map<String, String>                         lastFingerprints = Map.of();
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

  /** Serializes the writers of {@code columnar/recovery.pages} (WAL housekeeping, Raft checkpoint, close) and the startup's read. */
  private final Object                  recoveryLock      = new Object();
  /** {@link #startApplying(boolean)} ran: projections opened from now on get their rebuild requester at once. Under registryLock. */
  private boolean                       startupDone;
  /** The startup took the recovery set into the trackers; until then a persist keeps the pages of the file. Under recoveryLock. */
  private boolean                       recoveryMerged;
  /** Projections whose rebuild the {@code REBUILD} marker asked for and that did not publish yet; the marker is cleared once empty. */
  private final Set<ColumnarProjection> markerRebuilds    = ConcurrentHashMap.newKeySet();
  private volatile boolean              droppedDatabase;

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
      forgetMarkerRebuild(slot.projection);
    } finally {
      registryLock.unlock();
    }
  }

  /**
   * Whether the projection of {@code index} holds a valid state that publishes the index's column, to answer facets from; false while it
   * has none, and for an index added to a usable projection until its create rebuild publishes the column.
   */
  public boolean isUsable(final ColumnarIndex index) {
    final int bucketId = index.getAssociatedBucketId();
    final ColumnarProjection projection = projection(bucketId);
    // a projection that does not publish the index's column yet (a create rebuild pending) cannot answer for it
    return projection != null && projection.isPublishedWith(index.columnDefinition().indexFileId()) && !failedBuckets.contains(bucketId);
  }

  /**
   * Asks for the projections to be reconciled with the COLUMNAR indexes of the schema (spec 6.4, see {@link #reconcile(Map, Map)}): the
   * next apply pass does it, before it applies. Called by the schema after every change it publishes or persists (V-B5).
   */
  public void requestReconcile() {
    reconcileRequests.incrementAndGet();
  }

  /** The number of {@link #requestReconcile()} calls so far. */
  long reconcileRequests() {
    return reconcileRequests.get();
  }

  /**
   * The leader's {@link ColumnarIndex#build}: opens the projections of the indexes bound by now and schedules the {@code create}
   * rebuild of the index's projection unless it publishes the column already (an empty bucket is published with its columns at open,
   * V-B1). Under the registry lock, so a reconcile never sees the projection opened and its rebuild not scheduled yet: its
   * {@code ADD_COLUMN} then finds the rebuild queued, running or published and counts no second one (V-M14).
   */
  void buildRegistered(final ColumnarIndex index) {
    registryLock.lock();
    try {
      openRegistered();
      final ColumnarProjection projection = projection(index.getAssociatedBucketId());
      if (projection != null && !projection.isPublishedWith(index.columnDefinition().indexFileId()))
        ColumnarRebuilder.schedule(this, projection, ColumnarRebuilder.CREATE);
    } finally {
      registryLock.unlock();
    }
    requestReconcile();
  }

  // -------------------------------------------------------------------------------------------------------------- schema events

  /**
   * Receives every schema event this manager applies or is sent ({@link ColumnarSchemaEvent}), on the thread that applies it: for a
   * reconcile that is a {@link ColumnarApplyPool} thread shared by every database of the JVM, so a listener hands long work off.
   */
  public void addSchemaEventListener(final Consumer<ColumnarSchemaEvent> listener) {
    schemaListeners.add(listener);
  }

  /**
   * An event sent from outside the reconcile: a {@code REBUILD} schedules the rebuild of the bucket's projection with its reason
   * (nothing, and no listener called, for a bucket with no projection); every event then reaches the listeners.
   */
  public void onSchemaEvent(final ColumnarSchemaEvent event) {
    if (event.kind() == ColumnarSchemaEvent.Kind.REBUILD) {
      final ColumnarProjection projection = projection(event.bucketFileId());
      if (projection == null)
        return;
      ColumnarRebuilder.schedule(this, projection, event.reason());
    }
    notifyListeners(event);
  }

  private void notifyListeners(final ColumnarSchemaEvent event) {
    for (final Consumer<ColumnarSchemaEvent> listener : schemaListeners)
      try {
        listener.accept(event);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING, "A columnar schema event listener of database '%s' failed on %s", e,
            database.getName(), event);
      }
  }

  /** A reconcile was requested since the last one, and there is something to reconcile (a projection, or a tracked definition). */
  private boolean isReconcileDue() {
    return (reconcileTracked || slots.length > 0) && reconcileRequests.get() != reconciledRequests;
  }

  /** A pass, before its apply: reconciles with the schema when a reconcile was requested since the last one. */
  private void reconcileIfRequested() {
    // read before the schema is: a request arriving meanwhile runs another reconcile
    final long requested = reconcileRequests.get();
    if (requested == reconciledRequests)
      return;
    ColumnarTestHooks.reach(RECONCILE);
    final boolean complete = reconcile(schemaDefinitions(), schemaFingerprints());
    // only now, and not after a reconcile that threw (the next tick retries it): a reconcile is due (awaitCaughtUp waits) until its
    // events reached the listeners
    reconciledRequests = requested;
    if (!complete)
      reconcileRequests.incrementAndGet();
  }

  /** {@link #reconcile(Map, Map)} with the definitions given and the hierarchy fingerprints of the schema. */
  void reconcile(final Map<Integer, ColumnDefinition[]> fromSchema) {
    reconcile(fromSchema, schemaFingerprints());
  }

  /**
   * Spec 6.4: applies the events {@link ColumnarReconciler#diff} finds between the definitions of the previous reconcile and
   * {@code fromSchema}, and {@link ColumnarReconciler#hierarchyDiff} between the hierarchy fingerprints, then calls the listeners.
   * <ul>
   * <li>{@code DROP_PROJECTION}: the projection is dropped (its directory deleted once the last reader releases, P52);</li>
   * <li>{@code DROP_COLUMN}, {@code RENAME}, {@code RETYPE_COLUMN}: the projection's columns become the schema's
   * ({@link ColumnarProjection#redefineColumns}); a RETYPE is rebuilt with reason {@code schema};</li>
   * <li>{@code ADD_COLUMN}: a {@code create} rebuild, only when the published manifest lacks the column and no rebuild of the
   * projection is queued or running (R17, V-B4: the leader's {@link ColumnarIndex#build} covered it otherwise);</li>
   * <li>{@code CREATE_EMPTY}, {@code HIERARCHY_SOURCE_CHANGED}: the listeners only.</li>
   * </ul>
   * A projection whose bucket or type was renamed takes the new names ({@code RENAME} with no index file id). The registered indexes
   * the schema no longer has are forgotten, and the extractor cache is dropped (a supertype change changes the columns of a type).
   * A bucket the schema projects and this node did not open yet is left out of the recorded definitions, so a later reconcile sees it
   * as new.
   *
   * @return false when a projection could not take its new columns (its maintenance slot was busy): its events are reported by the
   * reconcile that applies them
   */
  private boolean reconcile(final Map<Integer, ColumnDefinition[]> fromSchema, final Map<String, String> fingerprints) {
    final List<ColumnarSchemaEvent> delivered = new ArrayList<>();
    boolean complete = true;
    registryLock.lock();
    try {
      if (closed)
        return true;
      final Map<Integer, ColumnDefinition[]> applied = new HashMap<>();
      for (final Map.Entry<Integer, ColumnDefinition[]> entry : fromSchema.entrySet())
        if (slotsByBucket.containsKey(entry.getKey()))
          applied.put(entry.getKey(), entry.getValue().clone());

      final List<ColumnarSchemaEvent> events = ColumnarReconciler.diff(lastDefinitions, applied, this::isBucketEmpty);
      final Set<Integer> redefined = new TreeSet<>();
      final Set<Integer> retyped = new TreeSet<>();
      final Set<Integer> deferred = new TreeSet<>();
      for (final ColumnarSchemaEvent event : events)
        switch (event.kind()) {
        case DROP_PROJECTION -> {
          // a bucket the schema still projects and this node has no projection of (its open failed): retried, not dropped
          if (!fromSchema.containsKey(event.bucketFileId()))
            dropProjection(event.bucketFileId());
          else
            deferred.add(event.bucketFileId());
        }
        case DROP_COLUMN, RENAME -> redefined.add(event.bucketFileId());
        case RETYPE_COLUMN -> {
          redefined.add(event.bucketFileId());
          retyped.add(event.bucketFileId());
        }
        default -> {
          // ADD_COLUMN below; CREATE_EMPTY: the projection was opened usable by the registration
        }
        }

      for (final int bucketId : redefined) {
        final ColumnarProjection projection = slotsByBucket.get(bucketId).projection;
        if (!projection.redefineColumns(redefinedColumns(projection.columns(), applied.get(bucketId)))) {
          // retried by the next reconcile, which finds the same differences again
          deferred.add(bucketId);
          applied.put(bucketId, lastDefinitions.get(bucketId));
          complete = false;
        } else if (retyped.contains(bucketId))
          ColumnarRebuilder.schedule(this, projection, ColumnarRebuilder.SCHEMA);
      }

      for (final ColumnarSchemaEvent event : events) {
        if (deferred.contains(event.bucketFileId()))
          continue;
        if (event.kind() == ColumnarSchemaEvent.Kind.ADD_COLUMN) {
          final ColumnarProjection projection = slotsByBucket.get(event.bucketFileId()).projection;
          // a column the registration did not add (its open came first): added, missing until a rebuild publishes it
          for (final ColumnDefinition column : applied.get(event.bucketFileId()))
            if (column.indexFileId() == event.indexFileId())
              projection.addColumn(column);
          // a running rebuild that captured the columns before this one is followed by a create (rebuildPublished, I3)
          if (!projection.isPublishedWith(event.indexFileId()) && !ColumnarRebuilder.isQueuedOrRunning(projection))
            ColumnarRebuilder.schedule(this, projection, ColumnarRebuilder.CREATE);
        }
        delivered.add(event);
      }

      renameProjections(delivered);
      forgetIndexesNotIn(fromSchema);
      publishRegistry();

      delivered.addAll(ColumnarReconciler.hierarchyDiff(lastFingerprints, fingerprints));
      lastDefinitions = applied;
      lastFingerprints = fingerprints;
      reconcileTracked = !applied.isEmpty() || !fingerprints.isEmpty();
    } finally {
      registryLock.unlock();
    }
    for (final ColumnarSchemaEvent event : delivered)
      notifyListeners(event);
    return complete;
  }

  /**
   * Under {@link #registryLock}: the columns a projection takes from the schema's: its own in their order, each replaced by the schema's
   * definition of the same index file id or left out when the schema has none, then the schema's it does not have yet.
   */
  private static ColumnDefinition[] redefinedColumns(final ColumnDefinition[] own, final ColumnDefinition[] schema) {
    final List<ColumnDefinition> result = new ArrayList<>(schema.length);
    for (final ColumnDefinition column : own)
      for (final ColumnDefinition defined : schema)
        if (defined.indexFileId() == column.indexFileId()) {
          result.add(defined);
          break;
        }
    for (final ColumnDefinition defined : schema)
      if (result.stream().noneMatch(c -> c.indexFileId() == defined.indexFileId()))
        result.add(defined);
    return result.toArray(new ColumnDefinition[0]);
  }

  /** Under {@link #registryLock}: drops the bucket's projection and forgets its indexes (a dropped type or bucket, a follower). */
  private void dropProjection(final int bucketId) {
    indexes.values().removeIf(index -> index.getAssociatedBucketId() == bucketId);
    failedOpens.remove(bucketId);
    final Slot slot = slotsByBucket.remove(bucketId);
    if (slot == null)
      return;
    publishRegistry();
    slot.projection.drop();
    forgetMarkerRebuild(slot.projection);
  }

  /** Under {@link #registryLock}: gives every projection the current names of its bucket and type, recording a RENAME per change. */
  private void renameProjections(final List<ColumnarSchemaEvent> delivered) {
    for (final Slot slot : slotsByBucket.values()) {
      final ColumnarProjection projection = slot.projection;
      final Bucket bucket = database.getSchema().getBucketByIdIfExists(projection.bucketId());
      if (bucket == null)
        continue;
      final DocumentType type = database.getSchema().getTypeByBucketId(projection.bucketId());
      final String typeName = type == null ? projection.typeName() : type.getName();
      if (bucket.getName().equals(projection.bucketName()) && typeName.equals(projection.typeName()))
        continue;
      projection.rename(bucket.getName(), typeName);
      delivered.add(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.RENAME, projection.bucketId(), -1, null, null));
    }
  }

  /** Under {@link #registryLock}: forgets the registered indexes, and the failed opens, of columns the schema does not have. */
  private void forgetIndexesNotIn(final Map<Integer, ColumnDefinition[]> fromSchema) {
    final IntHashSet known = new IntHashSet();
    for (final ColumnDefinition[] columns : fromSchema.values())
      for (final ColumnDefinition column : columns)
        known.add(column.indexFileId());
    indexes.keySet().removeIf(fileId -> !known.contains(fileId));
    for (final Iterator<List<ColumnDefinition>> it = failedOpens.values().iterator(); it.hasNext(); ) {
      final List<ColumnDefinition> failed = it.next();
      failed.removeIf(column -> !known.contains(column.indexFileId()));
      if (failed.isEmpty())
        it.remove();
    }
  }

  /** Whether the bucket has no record; false when it cannot be read (an {@code ADD_COLUMN} then rebuilds, which costs a scan only). */
  private boolean isBucketEmpty(final int bucketId) {
    try {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId);
      return bucket.getCommittedPageCount() == 0 || bucket.count() == 0;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  /** The COLUMNAR columns of the schema, by bucket file id, of every index bound to its bucket. */
  private Map<Integer, ColumnDefinition[]> schemaDefinitions() {
    final Map<Integer, List<ColumnDefinition>> byBucket = new HashMap<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof ColumnarIndex columnar && isBound(columnar))
        byBucket.computeIfAbsent(columnar.getAssociatedBucketId(), k -> new ArrayList<>()).add(columnar.columnDefinition());
    final Map<Integer, ColumnDefinition[]> result = new HashMap<>();
    for (final Map.Entry<Integer, List<ColumnDefinition>> entry : byBucket.entrySet())
      result.put(entry.getKey(), entry.getValue().toArray(new ColumnDefinition[0]));
    return result;
  }

  /** The fingerprints of the types the COLUMNAR hierarchies of the schema read (R20). */
  private Map<String, String> schemaFingerprints() {
    final List<ColumnarIndexMetadata> withHierarchy = new ArrayList<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof ColumnarIndex columnar && columnar.getColumnarMetadata() != null
          && columnar.getColumnarMetadata().getHierarchy() != null)
        withHierarchy.add(columnar.getColumnarMetadata());
    return ColumnarReconciler.hierarchyFingerprints(database.getSchema(), withHierarchy);
  }

  /** Startup, before the tick: the schema as it is now is the base the first reconcile compares with (no event for it). */
  private void recordReconcileBase() {
    reconciledRequests = reconcileRequests.get();
    final Map<Integer, ColumnDefinition[]> fromSchema = schemaDefinitions();
    final Map<String, String> fingerprints = schemaFingerprints();
    registryLock.lock();
    try {
      final Map<Integer, ColumnDefinition[]> base = new HashMap<>();
      for (final Map.Entry<Integer, ColumnDefinition[]> entry : fromSchema.entrySet())
        if (slotsByBucket.containsKey(entry.getKey()))
          base.put(entry.getKey(), entry.getValue());
      lastDefinitions = base;
      lastFingerprints = fingerprints;
      reconcileTracked = !base.isEmpty() || !fingerprints.isEmpty();
    } finally {
      registryLock.unlock();
    }
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
        final ColumnarProjection projection = ColumnarProjection.open(database, bucket, columns.toArray(new ColumnDefinition[0]), stats);
        slotsByBucket.put(bucketId, new Slot(projection));
        // after the startup only: the startup decides the rebuilds of the projections the open found (V-M10)
        if (startupDone)
          installRebuildRequester(projection);
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

  // ------------------------------------------------------------------------------------------------------ scrape-time gauges
  // Computed over the live projections of a type when a meter is read: lock free (volatile reads of published state, no apply or
  // publish lock), nothing is written on the apply, flush or merge paths, and a dropped or closed projection stops contributing.
  // A COLUMNAR index is polymorphic, so "the projections of a type" are those of every bucket the type owns, subtypes included: the
  // buckets a facet query over the type reads.

  /** Dead rows over rows of the manifests of every bucket projection the type covers; 0 when it has no rows. */
  public double deadRatio(final String type) {
    final int[] buckets = bucketsOf(type);
    long rows = 0;
    long dead = 0;
    for (final Slot slot : slots) {
      final ColumnarProjection projection = slot.projection;
      final ColumnarManifest manifest = projection.manifest();
      if (manifest == null || !covers(buckets, projection))
        continue;
      for (final ColumnarManifest.SegmentEntry entry : manifest.segments()) {
        rows += entry.rows();
        dead += entry.deadRows();
      }
    }
    return rows == 0 ? 0 : (double) dead / rows;
  }

  /** The largest dictionary of the property among the bucket projections the type covers: the size the cardinality limit is compared with. */
  public int dictionarySize(final String type, final String property) {
    final int[] buckets = bucketsOf(type);
    int max = 0;
    for (final Slot slot : slots) {
      final ColumnarProjection projection = slot.projection;
      if (!covers(buckets, projection))
        continue;
      final GlobalDictionary dictionary = projection.dictionary(property);
      if (dictionary != null)
        max = Math.max(max, dictionary.size());
    }
    return max;
  }

  /** True when the dictionary of the property of ANY bucket projection the type covers holds more values than its effective limit. */
  public boolean cardinalityExceeded(final String type, final String property) {
    final int size = dictionarySize(type, property);
    return size > 0 && size > cardinalityLimit(type, property);
  }

  /**
   * The limit a dictionary of the property is held to: the lowest over the COLUMNAR indexes of the property that cover the type, each
   * with its own {@code maxCardinality} metadata (which may be above the database's) or else the database's
   * {@code arcadedb.columnar.maxCardinality}, read now. The database's value is used only when no index matches.
   */
  int cardinalityLimit(final String type, final String property) {
    final int configured = database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY);
    int limit = Integer.MAX_VALUE;
    boolean matched = false;
    for (final ColumnarIndex index : registered) {
      if (!property.equals(index.columnDefinition().property()))
        continue;
      final String indexType = index.getTypeName();
      if (indexType != null && !indexType.equals(type)) {
        final DocumentType documentType = database.getSchema().getTypeOrNull(type);
        if (documentType == null || !documentType.instanceOf(indexType))
          continue;
      }
      final int own = index.getColumnarMetadata().getMaxCardinality();
      limit = Math.min(limit, own > 0 ? own : configured);
      matched = true;
    }
    return matched ? limit : configured;
  }

  /**
   * Age of the oldest committed page not yet applied, the maximum over the bucket projections the type covers; 0 when none is pending.
   * The trackers are stamped with the system clock, this reads the manager's (the same in production, injectable in tests).
   */
  public long applyLagMs(final String type) {
    final int[] buckets = bucketsOf(type);
    final long now = nowMs();
    long lag = 0;
    for (final Slot slot : slots) {
      final ColumnarProjection projection = slot.projection;
      if (!covers(buckets, projection))
        continue;
      lag = Math.max(lag, lagOf(projection.tracker(), now));
      if (projection.externalTracker() != null)
        lag = Math.max(lag, lagOf(projection.externalTracker(), now));
    }
    return lag;
  }

  private static long lagOf(final DirtyPageTracker tracker, final long now) {
    final long oldest = tracker.oldestPendingEpochStartMs();
    return oldest == 0 ? 0 : Math.max(0, now - oldest);
  }

  /** The file ids of the buckets the type owns, subtypes included; none when the type does not exist (any more). */
  private int[] bucketsOf(final String type) {
    final DocumentType documentType = database.getSchema().getTypeOrNull(type);
    if (documentType == null)
      return new int[0];
    final List<Bucket> buckets = documentType.getBuckets(true);
    final int[] ids = new int[buckets.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = buckets.get(i).getFileId();
    return ids;
  }

  private static boolean covers(final int[] buckets, final ColumnarProjection projection) {
    if (projection.isClosedOrDropped())
      return false;
    for (final int bucket : buckets)
      if (bucket == projection.bucketId())
        return true;
    return false;
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

  /**
   * After the WAL replay, once per open: runs the startup sequence over the projections the schema load opened, installs their rebuild
   * requesters, then registers the tick of this database with {@link ColumnarApplyPool} (never on a READ_ONLY database).
   * <ol>
   * <li>{@code columnar/REBUILD} present: every projection rebuilds with its reason; the marker is cleared once all published.</li>
   * <li>Otherwise, per projection: files {@code missing} or {@code corrupt} (as the open found them) rebuild with that reason; a
   * {@code NOWAL} marker rebuilds as {@code unclean_no_wal} on an unclean open, and is deleted as stray on a clean one (a clean close
   * flushed every page of the database, so no projection can be newer than its bucket).</li>
   * <li>The others take the pages {@code columnar/recovery.pages} names for their files (primary and external) back into their
   * trackers. A recovery set that fails its check writes the {@code REBUILD} marker with reason {@code corrupt} first: its pages are
   * lost, so every projection rebuilds, and keeps rebuilding at each open until all published.</li>
   * </ol>
   * READ_ONLY (R16): the reasons are only recorded (the projection is served by the fallback); nothing is scheduled, cleared or written.
   *
   * @param uncleanOpen the database was not closed properly (its lock file was found)
   */
  public void startApplying(final boolean uncleanOpen) {
    final Slot[] opened;
    registryLock.lock();
    try {
      if (closed || startupDone)
        return;
      startupDone = true;
      opened = slots;
    } finally {
      registryLock.unlock();
    }
    recover(opened, uncleanOpen);
    if (readOnly || closing || applying)
      return;
    recordReconcileBase();
    ColumnarApplyPool.getInstance().register(tick, () -> database.getConfiguration().getValueAsLong(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL));
    applying = true;
  }

  /** The startup sequence of {@link #startApplying(boolean)}. */
  private void recover(final Slot[] opened, final boolean uncleanOpen) {
    final Path databasePath = Path.of(database.getDatabasePath());
    final Path recoveryFile = databasePath.resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);
    final Map<ColumnarProjection, String> rebuilds = new LinkedHashMap<>();
    synchronized (recoveryLock) {
      String marker = readRebuildMarker(databasePath);
      Map<Integer, int[]> recovery = Map.of();
      if (!readOnly && marker == null)
        try {
          recovery = RecoverySetFile.read(recoveryFile);
        } catch (final IOException | RuntimeException e) {
          LogManager.instance().log(this, Level.SEVERE, "The columnar recovery set of database '%s' cannot be read: every projection is "
              + "rebuilt", e, database.getName());
          marker = ColumnarProjection.CORRUPT;
          try {
            ColumnarStore.writeRebuildReason(databasePath, marker);
          } catch (final IOException w) {
            LogManager.instance().log(this, Level.SEVERE, "Cannot write the columnar rebuild marker of database '%s'", w, database.getName());
          }
        }

      long recovered = 0;
      for (final Slot slot : opened) {
        final ColumnarProjection projection = slot.projection;
        final String reason = startupRebuildReason(projection, marker, uncleanOpen);
        if (reason != null) {
          rebuilds.put(projection, reason);
          if (marker != null)
            markerRebuilds.add(projection);
        } else if (!readOnly) {
          recovered += markRecovered(projection.bucketId(), recovery);
          if (projection.externalFileId() >= 0)
            recovered += markRecovered(projection.externalFileId(), recovery);
        }
      }
      if (recovered > 0)
        stats.recoveryPages(recovered);
      recoveryMerged = true;
      if (marker != null && !readOnly && markerRebuilds.isEmpty())
        clearRebuildMarker();
    }

    for (final Slot slot : opened) {
      final ColumnarProjection projection = slot.projection;
      final String reason = rebuilds.get(projection);
      if (readOnly) {
        if (reason != null && projection.isUsable())
          projection.markUnusableWithoutRequest(reason);
        continue;
      }
      installRebuildRequester(projection);
      if (reason != null)
        ColumnarRebuilder.schedule(this, projection, reason);
      else if (projection.hasRetypedColumn())
        // I2: a RETYPE whose schema rebuild did not publish before the close: the schema's definition is the one to build
        ColumnarRebuilder.schedule(this, projection, ColumnarRebuilder.SCHEMA);
    }
  }

  /** The reason the startup rebuilds the projection with, null when it is current (see {@link #startApplying(boolean)}). */
  private String startupRebuildReason(final ColumnarProjection projection, final String marker, final boolean uncleanOpen) {
    if (marker != null)
      return marker;
    if (projection.unusableReason() != null)
      return projection.unusableReason();
    final Path noWal = projection.bucketDir().resolve(ColumnarStore.NOWAL_MARKER);
    if (!Files.exists(noWal))
      return null;
    if (uncleanOpen)
      return ColumnarRebuilder.UNCLEAN_NO_WAL;
    if (!readOnly)
      try {
        // a marker the close could not delete, or one a no-WAL commit wrote after the columnar close: every page is on disk now
        Files.deleteIfExists(noWal);
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Cannot delete the stray NOWAL marker of the columnar projection of bucket '%s'", e,
            projection.bucketName());
      }
    return null;
  }

  /** The reason in {@code columnar/REBUILD}, null without one; an unreadable marker is a marker all the same. */
  private String readRebuildMarker(final Path databasePath) {
    try {
      final String reason = ColumnarStore.readRebuildReason(databasePath);
      return reason == null ? null : reason.isEmpty() ? ColumnarProjection.CORRUPT : reason;
    } catch (final UncheckedIOException e) {
      LogManager.instance().log(this, Level.WARNING, "The columnar rebuild marker of database '%s' cannot be read", e, database.getName());
      return ColumnarProjection.CORRUPT;
    }
  }

  /** Marks the pages the recovery set holds for the file; returns their number. */
  private long markRecovered(final int fileId, final Map<Integer, int[]> recovery) {
    final int[] pages = recovery.get(fileId);
    if (pages == null || pages.length == 0)
      return 0;
    mark(fileId, pages, pages.length);
    return pages.length;
  }

  private void installRebuildRequester(final ColumnarProjection projection) {
    if (!readOnly)
      projection.setRebuildRequester(reason -> ColumnarRebuilder.schedule(this, projection, reason));
  }

  /** {@link ColumnarRebuilder}, once a rebuild of the projection published: ends the obligations the rebuild discharged. */
  void rebuildPublished(final ColumnarProjection projection) {
    // I3: a column added after the rebuild captured its columns is still not published: built by a create (coalesced, this rebuild
    // is still running, so it runs once more after it)
    if (projection.hasUnpublishedColumn())
      ColumnarRebuilder.schedule(this, projection, ColumnarRebuilder.CREATE);
    // a NOWAL marker no no-WAL commit of this session wrote describes pages the scan read as they are now
    synchronized (projection.noWalMarkerLock) {
      if (projection.noWalMarkerEpoch == -1)
        try {
          Files.deleteIfExists(projection.bucketDir().resolve(ColumnarStore.NOWAL_MARKER));
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.WARNING, "Cannot delete the NOWAL marker of the rebuilt columnar projection of bucket '%s'",
              e, projection.bucketName());
        }
    }
    forgetMarkerRebuild(projection);
  }

  /** The projection no longer needs the rebuild the {@code REBUILD} marker asked for; the last one clears the marker. */
  private void forgetMarkerRebuild(final ColumnarProjection projection) {
    if (markerRebuilds.remove(projection) && markerRebuilds.isEmpty())
      clearRebuildMarker();
  }

  private void clearRebuildMarker() {
    try {
      ColumnarStore.clearRebuildReason(Path.of(database.getDatabasePath()));
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Cannot clear the columnar rebuild marker of database '%s'", e, database.getName());
    }
  }

  /**
   * Runs a rebuild body of the projection counted as a running maintenance job (a close waits for it); nothing when the projection is
   * not this manager's any more, or the manager closes.
   */
  void runRebuildBody(final ColumnarProjection projection, final Runnable body) {
    final Slot slot = slot(projection.bucketId());
    if (slot != null && slot.projection == projection)
      runMaintenanceBody(slot, body);
  }

  boolean isReadOnly() {
    return readOnly;
  }

  /** A close, drop or kill began: a rebuild aborts at its next page. */
  boolean isClosing() {
    return closing;
  }

  /** {@link #kill()} ran: a rebuild stops as a crash would, writing and deleting nothing. */
  boolean isKilled() {
    return killed;
  }

  /**
   * Persists the recovery set (spec 6.5): the pages every tracker holds (in an epoch, in flight or unflushed, primary and external,
   * R7) are written to {@code columnar/recovery.pages} atomically. Called before a WAL file is dropped (housekeeping, rotation, close),
   * at a clean close under the database's write lock, and by the Raft checkpoint; safe from any thread at any time, the writers are
   * serialized. Before the startup took the recovery set of the last session back, its pages are kept in what is written. Nothing is
   * written on a READ_ONLY database, for a database being dropped, or without a projection.
   *
   * @return false when the set could not be written (logged), or after a {@link #kill()}: the caller keeps its WAL files
   */
  public boolean persistRecoverySet() {
    if (readOnly || droppedDatabase)
      return true;
    if (killed)
      return false;
    synchronized (recoveryLock) {
      final Slot[] current = slots;
      if (current.length == 0)
        return true;
      final Path root = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR);
      final Path file = root.resolve(RecoverySetFile.FILE_NAME);
      try {
        final Map<Integer, IntHashSet> byFile = new LinkedHashMap<>();
        if (!recoveryMerged)
          for (final Map.Entry<Integer, int[]> entry : RecoverySetFile.read(file).entrySet())
            for (final int page : entry.getValue())
              byFile.computeIfAbsent(entry.getKey(), k -> new IntHashSet()).add(page);
        for (final Slot slot : current) {
          final ColumnarProjection projection = slot.projection;
          projection.tracker().collectRecoverySet(byFile.computeIfAbsent(projection.bucketId(), k -> new IntHashSet()));
          final DirtyPageTracker external = projection.externalTracker();
          if (external != null)
            external.collectRecoverySet(byFile.computeIfAbsent(projection.externalFileId(), k -> new IntHashSet()));
        }
        final int[] fileIds = new int[byFile.size()];
        final int[][] pages = new int[fileIds.length][];
        final int[] counts = new int[fileIds.length];
        int i = 0;
        for (final Map.Entry<Integer, IntHashSet> entry : byFile.entrySet()) {
          fileIds[i] = entry.getKey();
          pages[i] = entry.getValue().toArray();
          Arrays.sort(pages[i]);
          counts[i] = pages[i].length;
          i++;
        }
        FileUtils.createDirectoriesDurably(root);
        RecoverySetFile.write(file, fileIds, pages, counts);
        ColumnarTestHooks.reach(RECOVERY_SET_PERSISTED);
        return true;
      } catch (final ColumnarTestHooks.SimulatedCrash e) {
        throw e;
      } catch (final IOException | RuntimeException e) {
        logFailure("Cannot persist the columnar recovery set of database '%s': its WAL files are kept", e);
        return false;
      }
    }
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
    if (slots.length == 0 && !watchersPending && failedBuckets.isEmpty() && !isReconcileDue())
      return;
    // a rebuild that found no free permit starts here once one is free (a test, or a rebuild of another JVM component, held it)
    if (ColumnarRebuilder.hasWaiting())
      ColumnarRebuilder.drainWaiting();
    if (closing || suspensions.get() > 0)
      return;
    if (!watchersPending && !isReconcileDue() && !hasPendingApply() && !isOpenRetryDue() && !isUnreadableRetryDue()
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

  /** A page of some projection whose record was unreadable is due for its retry; lock free. */
  private boolean isUnreadableRetryDue() {
    final long now = clockMs.getAsLong();
    for (final Slot slot : slots)
      if (slot.projection.unreadablePages().isDue(now))
        return true;
    return false;
  }

  /** The clock of the maintenance schedule and of the retries of unreadable pages. */
  long nowMs() {
    return clockMs.getAsLong();
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
      reconcileInPass();
      for (final Slot slot : slots) {
        if (closing || suspensions.get() > 0)
          return;
        if (hasPendingApply(slot.projection) || slot.projection.unreadablePages().isDue(clockMs.getAsLong()))
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

  /** The reconcile of a pass, in a database context (the emptiness of a new bucket is read through the database). */
  private void reconcileInPass() {
    try {
      ColumnarApplier.inDatabaseContext(database, () -> {
        reconcileIfRequested();
        return null;
      });
    } catch (final RuntimeException e) {
      // the request stays due: the next tick retries
      logFailure("Columnar schema reconcile of database '%s' failed", e);
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

  /** Freezes, or moves, the clock of the maintenance schedule and of the unreadable page retries. */
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
   * Waits until every projection is usable, every registered index's column is published ({@link #isUsable(ColumnarIndex)}) and no
   * page is pending (in an epoch or in flight): every mark that returned before the call
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
    if (!failedBuckets.isEmpty() || applying && isReconcileDue())
      return false;
    for (final Slot slot : slots)
      if (!slot.projection.isUsable() || hasPendingApply(slot.projection))
        return false;
    for (final ColumnarIndex index : registered)
      if (!isUsable(index))
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
    droppedDatabase = true;
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
