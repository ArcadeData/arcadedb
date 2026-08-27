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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.logging.Level;

/**
 * Graph Analytical View (GAV) — a synchronized, read-optimized CSR representation of the OLTP graph.
 * <p>
 * Stores one {@link CSRAdjacencyIndex} per edge type, with a per-bucket
 * {@link NodeIdMapping} across all vertex types and per-bucket {@link ColumnStore}s
 * for property access. This enables:
 * <ul>
 *   <li>Type-filtered traversal and counting via edge type selection</li>
 *   <li>Per-bucket parallelism aligned with ArcadeDB's storage architecture</li>
 *   <li>Columnar property access (dictionary-encoded strings, null bitmaps)</li>
 *   <li>Optional auto-update on transaction commit</li>
 * </ul>
 * <p>
 * <b>Neighbor order is unrelated to OLTP order.</b> {@link #getVertices}, {@link #getNeighborView} and
 * {@link #scanNeighbors} always return a node's neighbors sorted ascending by internal dense node ID -
 * further permuted by a one-time cache-locality renumbering at build time ({@code CSRBuilder}'s BFS/RCM
 * pass) - never in edge-insertion or OLTP-recency order. This holds for every vertex, promoted
 * super-node or not: it is what lets {@link #isConnectedTo}, {@link #countEdgesBetween},
 * {@link #getMeanEdgesPerConnectedPair} and triangle counting binary-search or merge-join the adjacency
 * arrays instead of scanning them. A query the planner can answer from either the OLTP edge list or a
 * ready GAV (see {@code GAVExpandAll}/{@code GAVExpandInto}) may therefore return its rows in a different
 * order depending on which path was chosen - by design, not as an artifact of the OLTP layout underneath
 * (classic or striped/promoted - see {@link com.arcadedb.graph.StripedEdgeList}). An application that
 * needs a specific result order must request it explicitly (e.g. {@code ORDER BY}) rather than relying on
 * either path's default.
 * <p>
 * Usage:
 * <pre>
 *   // Via builder (recommended)
 *   GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
 *       .withVertexTypes("Person", "Company")
 *       .withEdgeTypes("FOLLOWS", "WORKS_AT")
 *       .withProperties("name", "age")
 *       .withUpdateMode(UpdateMode.SYNCHRONOUS)
 *       .build();
 *
 *   int nodeId = gav.getNodeId(vertexRID);
 *   long followsOut = gav.countEdges(nodeId, Vertex.DIRECTION.OUT, "FOLLOWS");
 *   int[] neighbors = gav.getVertices(nodeId, Vertex.DIRECTION.OUT, "FOLLOWS");
 *   Object name = gav.getProperty(nodeId, "name");
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalView implements GraphTraversalProvider {

  /** Maximum number of concurrent CSR builds/compactions across all databases. */
  private static final int MAX_CONCURRENT_BUILDS = Math.max(2, Runtime.getRuntime().availableProcessors());

  /**
   * The "multiplicity unknowable" answer, which the {@link com.arcadedb.graph.GraphTraversalProvider}
   * contract spells as any negative value. Still returned by {@link #getMeanEdgesPerConnectedPair}; since
   * issue #6775 an active delta overlay is no longer a reason for {@link #countEdgesBetween} to give it,
   * because a pair the overlay has deletions for is now counted exactly (see {@link #countBetweenForType}).
   */
  private static final long MULTIPLICITY_UNKNOWN = -1L;

  /** Semaphore bounding concurrent CPU-intensive build operations. */
  private static final Semaphore BUILD_PERMITS = new Semaphore(MAX_CONCURRENT_BUILDS);

  /** The answer for a node with no edges of the type and direction asked for. */
  private static final int[]            EMPTY_INT          = new int[0];
  private static final NodeEdgeWeights  EMPTY_EDGE_WEIGHTS = new NodeEdgeWeights(EMPTY_INT, new double[0]);

  /** Shared executor for all GAV async builds and compactions. Uses virtual threads for lightweight scheduling. */
  private static volatile ExecutorService EXECUTOR;

  private static ExecutorService getExecutor() {
    ExecutorService exec = EXECUTOR;
    if (exec == null || exec.isShutdown()) {
      synchronized (GraphAnalyticalView.class) {
        exec = EXECUTOR;
        if (exec == null || exec.isShutdown())
          EXECUTOR = exec = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(final Runnable r) {
              final Thread t = new Thread(r, "gav-worker-" + counter.getAndIncrement());
              t.setDaemon(true);
              return t;
            }
          });
      }
    }
    return exec;
  }

  /**
   * Gracefully shuts down the shared async build executor. Waits up to 30 seconds
   * for in-progress builds to complete, then forcibly terminates remaining tasks.
   * Called when the last REGISTERED database instance is closed. NOTE (#4927): unlike the PageManager -
   * whose lifecycle is refcounted - this teardown deliberately stays on the ACTIVE_INSTANCES-empty
   * heuristic: the executor lazily re-creates itself on the next getExecutor(), so the mid-flight-open
   * race that was fatal for the flush thread merely costs a re-created executor here.
   * The executor is lazily re-created if a new database is opened later.
   *
   * <p><b>Multi-database note:</b> The executor is shared across all databases in the JVM.
   * When an individual database closes, {@link GraphAnalyticalViewRegistry#shutdownAll} unregisters
   * its GAV listeners and traversal providers, but any already-submitted async build tasks remain
   * on the executor until all databases close. Those orphaned tasks self-terminate with
   * {@code DatabaseIsClosedException} when they attempt to access the closed database — no data
   * loss or corruption occurs. The 30-second drain timeout applies to all pending work from all
   * databases simultaneously.</p>
   */
  public static void closeExecutor() {
    synchronized (GraphAnalyticalView.class) {
      final ExecutorService exec = EXECUTOR;
      if (exec == null || exec.isShutdown())
        return;
      exec.shutdown();
      try {
        if (!exec.awaitTermination(30, TimeUnit.SECONDS))
          exec.shutdownNow();
      } catch (final InterruptedException e) {
        exec.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  public enum Status {
    NOT_BUILT, BUILDING, READY, STALE
  }

  public enum UpdateMode {
    OFF, SYNCHRONOUS, ASYNCHRONOUS
  }

  /**
   * Immutable snapshot of all mutable CSR state. Swapped atomically via a single volatile write
   * to guarantee readers always see a consistent view, even during background compaction/rebuild.
   */
  static final class Snapshot {
    final Map<String, CSRAdjacencyIndex> csrPerType;
    final NodeIdMapping                  nodeMapping;
    final ColumnStore[]                  bucketColumns;
    // Materialized edge property data — null until background materialization completes
    final Map<String, ColumnStore>       edgeColumnStores;
    final Map<String, int[]>             bwdToFwd;
    final DeltaOverlay                   overlay;
    final long                           buildTimestamp;
    final long                           buildDurationMs;
    // The database's last committed transaction id as of the start of the scan that produced this CSR (see
    // #6583). Persisted alongside the CSR as its freshness certificate: on a later open, if the database's last
    // committed transaction id still equals this value, nothing was committed in between, so the persisted CSR is
    // still exactly correct and can be reused without a rescan. -1 for a snapshot that must never be persisted
    // (not applicable — no build has produced one yet).
    final long                           asOfTransactionId;
    // True when this snapshot was loaded from a persisted CSR file rather than produced by a scan of the graph.
    // Exposed via isRestoredFromPersistedCsr() — mainly for tests and operational visibility.
    final boolean                        restoredFromDisk;
    // The vertex/edge/property scope actually scanned into this snapshot. Usually identical to the enclosing
    // view's own vertexTypes/edgeTypes/propertyFilter/edgePropertyFilter fields, EXCEPT when produced by the
    // public two-arg build(vertexTypes, edgeTypes), which lets a caller rescan a NAMED view with a scope
    // narrower than it was constructed with — those fields are final and never updated to match. Recorded here
    // (rather than read from the view's fields) so persistCsrIfPossible() can write a certificate that describes
    // what this snapshot actually covers, not what the view was originally configured for (#6583 review).
    final String[]                       vertexTypes;
    final String[]                       edgeTypes;
    final String[]                       propertyFilter;
    final String[]                       edgePropertyFilter;

    Snapshot(final Map<String, CSRAdjacencyIndex> csrPerType, final NodeIdMapping nodeMapping,
        final ColumnStore[] bucketColumns, final Map<String, ColumnStore> edgeColumnStores,
        final Map<String, int[]> bwdToFwd,
        final DeltaOverlay overlay, final long buildTimestamp, final long buildDurationMs,
        final long asOfTransactionId, final boolean restoredFromDisk,
        final String[] vertexTypes, final String[] edgeTypes, final String[] propertyFilter, final String[] edgePropertyFilter) {
      this.csrPerType = csrPerType;
      this.nodeMapping = nodeMapping;
      this.bucketColumns = bucketColumns;
      this.edgeColumnStores = edgeColumnStores;
      this.bwdToFwd = bwdToFwd;
      this.overlay = overlay;
      this.buildTimestamp = buildTimestamp;
      this.buildDurationMs = buildDurationMs;
      this.asOfTransactionId = asOfTransactionId;
      this.restoredFromDisk = restoredFromDisk;
      this.vertexTypes = vertexTypes;
      this.edgeTypes = edgeTypes;
      this.propertyFilter = propertyFilter;
      this.edgePropertyFilter = edgePropertyFilter;
    }

    Snapshot withOverlay(final DeltaOverlay newOverlay) {
      return new Snapshot(csrPerType, nodeMapping, bucketColumns, edgeColumnStores, bwdToFwd,
          newOverlay, buildTimestamp, buildDurationMs, asOfTransactionId, restoredFromDisk,
          vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);
    }

  }

  private final Database   database;
  private final String     name;
  private final String[]   vertexTypes;
  private final String[]   edgeTypes;
  private final String[]   propertyFilter;
  private final String[]   edgePropertyFilter; // null = no edge properties (default)
  private       int        propertySampleSize = CSRBuilder.DEFAULT_PROPERTY_SAMPLE_SIZE;
  private volatile boolean useWhenStale;
  private volatile UpdateMode updateMode;

  /** Single volatile reference for all mutable CSR state — ensures atomic visibility to readers. */
  private volatile Snapshot          snapshot;
  private volatile Status            status    = Status.NOT_BUILT;
  private volatile CountDownLatch    readyLatch = new CountDownLatch(1);
  private volatile Throwable         buildError;
  // True from the moment restoreFromDiskOrBuildAsync() finds a plausible persisted CSR (see #6632) until
  // whatever touches the view first — checkBuilt() or awaitReady() — resolves it. Written before `status`
  // is flipped to READY (see restoreFromDiskOrBuildAsync()) and read without synchronization elsewhere, so
  // its value is guaranteed visible to any thread that observes that READY write — the same plain-volatile
  // publish pattern `snapshot`+`status` already rely on throughout this class (e.g. build()).
  private volatile boolean           pendingDiskRestore;
  // True for the FULL duration of a dispatched deferred restore, including a buildAsync() fallback if the
  // restore turns out unusable — see dispatchDeferredRestore(). Unlike pendingDiskRestore (cleared the
  // instant the FIRST caller dispatches the work, not when the work finishes), this is what checkBuilt()
  // gates its wait on: it stays true until the view actually settles, so a second concurrent caller landing
  // after dispatch but before completion still waits instead of throwing (review on PR #6633), while a
  // caller that hits a genuinely unrelated BUILDING view — a brand-new one built directly via buildAsync(),
  // which never sets this flag at all — keeps that method's own documented fail-fast contract unchanged.
  private volatile boolean           deferredRestoreInFlight;

  // Monotonic ticket bumped by every path that DISPATCHES a snapshot-producing operation - build(),
  // buildAsync(), onRelevantCommit()'s async rebuild, applyDelta()'s compaction rebuild, and
  // dispatchDeferredRestore() - captured as that operation's own "myGeneration" at dispatch time. Right
  // before actually committing a result to this.snapshot/this.status, an operation compares its captured
  // value against the current field: if another, later-dispatched operation has since bumped it, this one
  // has been superseded and skips the commit instead of overwriting a newer result with an older one
  // (issue #6636). Every read and write of this field happens inside a block synchronized on `this`
  // (build()/buildAsync()/onRelevantCommit()/applyDelta()/dispatchDeferredRestore() are themselves
  // synchronized methods, and every executor-task commit site below re-enters synchronized(this) to write
  // it), so a plain long - not volatile, not an Atomic - is sufficient: the monitor alone establishes the
  // happens-before edge between one thread's bump and another's later read.
  // Deliberately NOT bumped by applyDelta()'s own synchronous overlay merge (this.snapshot =
  // current.withOverlay(merged)): that merge is cooperative with a concurrent compaction rebuild - which
  // buffers and re-applies exactly those deltas against its own captured generation - not competing with
  // it, so bumping here would make the compaction's own commit spuriously stale on every rebuild that
  // overlaps so much as one incoming delta.
  private long generation;

  // Incremental auto-update
  private DeltaCollector         deltaCollector;
  public static final int        DEFAULT_COMPACTION_THRESHOLD = 10_000;
  private static final int       MAX_PENDING_DELTAS           = 100_000;
  private volatile int           compactionThreshold = DEFAULT_COMPACTION_THRESHOLD;
  private final AtomicBoolean    compacting = new AtomicBoolean(false);
  private final AtomicBoolean    buildQueued = new AtomicBoolean(false);
  private volatile boolean       asyncRebuildNeeded;  // true when a commit arrived during an async rebuild
  // true when an edge property update (which cannot be represented in the overlay) was buffered
  // during a compaction rebuild and needs a follow-up rebuild to become visible. See issue #4513.
  private volatile boolean       edgePropRebuildNeeded;

  // Raw TxDeltas buffered during compaction. Non-null only while a compaction rebuild is in progress.
  // Accessed only under synchronized(this), so ArrayList is safe.
  private List<TxDelta>          pendingDeltas;

  // Tracks scheduled-but-not-yet-completed async builds and compactions for this view.
  // shutdown()/drop() block on this so a closing database does not race the worker virtual thread,
  // which would otherwise see a closed database or cleared transaction context and log a SEVERE error.
  private final AtomicInteger    inFlightTasks      = new AtomicInteger();
  private final Object           inFlightMonitor    = new Object();

  /** Maximum time shutdown() waits for an in-flight async build/compaction to settle. */
  private static final long      SHUTDOWN_AWAIT_MS  = 30_000L;

  /**
   * Creates a builder for configuring the analytical view.
   */
  public static GraphAnalyticalViewBuilder builder(final Database database) {
    return new GraphAnalyticalViewBuilder(database);
  }

  /**
   * Simple constructor for backward compatibility. Use {@link #builder(Database)} for full control.
   */
  public GraphAnalyticalView(final Database database) {
    this(database, null, null, null, null, null, UpdateMode.OFF);
  }

  GraphAnalyticalView(final Database database, final String name, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final String[] edgePropertyFilter, final UpdateMode updateMode) {
    this.database = database;
    this.name = name;
    this.vertexTypes = vertexTypes;
    this.edgeTypes = edgeTypes;
    this.propertyFilter = propertyFilter;
    this.edgePropertyFilter = edgePropertyFilter;
    this.updateMode = updateMode;
    this.useWhenStale = GlobalConfiguration.GAV_USE_WHEN_STALE.getValueAsBoolean();
  }

  /**
   * Registers this view as a {@link GraphTraversalProvider} so the query planner can discover it.
   * Called by the builder after construction.
   */
  void registerAsTraversalProvider() {
    GraphTraversalProviderRegistry.register(database, this);
  }

  /**
   * Builds (or rebuilds) the analytical view synchronously.
   * Status transitions: NOT_BUILT/READY → BUILDING → READY.
   */
  public void build() {
    build(vertexTypes, edgeTypes);
  }

  /**
   * Builds (or rebuilds) the analytical view synchronously.
   *
   * @param vertexTypes vertex type names to include (null = all)
   * @param edgeTypes   edge type names to include (null = all)
   */
  public synchronized void build(final String[] vertexTypes, final String[] edgeTypes) {
    // A direct build() (e.g. REBUILD GRAPH ANALYTICAL VIEW) supersedes any not-yet-resolved deferred
    // restore-from-disk (see #6632): without clearing this, a later awaitReady() call would still see
    // pendingDiskRestore == true and dispatch dispatchDeferredRestore(), re-reading a possibly-superseded
    // persisted file (or a wholly redundant rebuild) right after this scan already produced a fresh,
    // authoritative snapshot.
    pendingDiskRestore = false;
    // Bumping unconditionally (no commit-time check needed here, unlike every other path below): this
    // whole method is synchronized and runs the scan on the calling thread, so it holds this instance's
    // monitor for its entire duration - nothing else that reads/writes `generation` can interleave. The
    // bump alone is what matters: it retroactively supersedes any snapshot-producing task dispatched
    // earlier but still in flight (issue #6636), e.g. an already-dispatched deferred restore-from-disk.
    ++generation;
    final CountDownLatch latch = new CountDownLatch(1);
    readyLatch = latch;
    status = Status.BUILDING;
    buildError = null;
    try {
      // Unlike buildAsync()/onRelevantCommit()/applyDelta()'s rebuild - each of which calls database.begin() on a
      // fresh worker thread AFTER sampling asOfTransactionId, so the scan's transaction cannot have cached
      // anything before that point - this method runs the scan on whatever transaction is already active on the
      // CALLING thread (e.g. REBUILD GRAPH ANALYTICAL VIEW / a caller invoking build() directly inside its own
      // transaction), or with none active at all. Under the default READ_COMMITTED that's harmless (no per-page
      // caching, every read is current). Under REPEATABLE_READ, a transaction that was already open - and may
      // already have cached some of the pages this scan is about to read - can miss a commit that landed between
      // its own begin() and this sample, while asOfTransactionId (sampled here) claims coverage through it. The
      // certificate would then be wrong in the direction that matters: a "complete" persisted CSR that is
      // actually missing something. Since there is no way from here to know which pages (if any) that ambient
      // transaction already cached, treat the certificate as unusable whenever that risk exists at all, rather
      // than trying to bound it: asOfTransactionId=-1 makes persistCsrIfPossible() skip persisting this snapshot
      // (see its existing "< 0" guard), the same way a snapshot that must never be persisted already reads.
      final boolean certificateMayBeUnsound = database.isTransactionActive()
          && database.getTransactionIsolationLevel() == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ;
      final long asOfTransactionId = certificateMayBeUnsound ? -1L : currentLastTransactionId();
      final long buildStart = System.currentTimeMillis();
      final CSRBuilder builder = new CSRBuilder(database, propertyFilter, edgePropertyFilter, propertySampleSize);
      final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
      final long durationMs = System.currentTimeMillis() - buildStart;

      // Atomic swap — readers see all-or-nothing
      this.snapshot = snapshotFromResult(result, durationMs, asOfTransactionId, vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);
      this.status = Status.READY;
      this.notifyAll();
      invalidateGraphStatisticsCache();

      if (deltaCollector == null)
        registerChangeListeners();
    } catch (final Exception e) {
      this.buildError = e;
      this.status = snapshot != null ? Status.STALE : Status.NOT_BUILT;
      this.notifyAll();
      throw e;
    } finally {
      latch.countDown();
    }
  }

  /**
   * Builds the analytical view asynchronously in a background thread.
   * Returns immediately. Use {@link #awaitReady(long, TimeUnit)} or
   * {@link #getStatus()} to check completion.
   */
  public synchronized void buildAsync() {
    if (!buildQueued.compareAndSet(false, true))
      return; // a build is already queued or running
    // See build(vertexTypes, edgeTypes)'s identical guard: supersedes any not-yet-resolved deferred
    // restore-from-disk (see #6632). Also reached, harmlessly, from dispatchDeferredRestore()'s own
    // fallback call - pendingDiskRestore is already false there by the time it calls this.
    pendingDiskRestore = false;
    // Captured at dispatch time, not when the background task actually starts its scan: see the
    // `generation` field javadoc (issue #6636) - this is what lets a later-dispatched build correctly
    // supersede an earlier one regardless of which finishes first.
    final long myGeneration = ++generation;
    final CountDownLatch latch = new CountDownLatch(1);
    readyLatch = latch;
    status = Status.BUILDING;
    buildError = null;
    // Track the queued task synchronously so a concurrent close()/drop() can wait for it
    // even before the virtual thread has had a chance to mount.
    inFlightTasks.incrementAndGet();
    try {
      getExecutor().execute(() -> {
        BUILD_PERMITS.acquireUninterruptibly();
        try {
          final long asOfTransactionId = currentLastTransactionId();
          // The build thread needs its own read transaction for database iteration
          database.begin();
          try {
            final long buildStart = System.currentTimeMillis();
            final CSRBuilder builder = new CSRBuilder(database, propertyFilter, edgePropertyFilter, propertySampleSize);
            final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
            final long durationMs = System.currentTimeMillis() - buildStart;

            boolean committed = false;
            synchronized (GraphAnalyticalView.this) {
              if (myGeneration == generation) {
                this.snapshot = snapshotFromResult(result, durationMs, asOfTransactionId, vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);
                this.status = Status.READY;
                GraphAnalyticalView.this.notifyAll();
                if (deltaCollector == null)
                  registerChangeListeners();
                committed = true;
              } else
                LogManager.instance().log(this, Level.FINE,
                    "GraphAnalyticalView '%s': async build result discarded (superseded by a newer build/restore)", name);
            }
            if (committed)
              invalidateGraphStatisticsCache();
          } finally {
            if (database.isTransactionActive())
              database.rollback();
          }
        } catch (final Exception e) {
          synchronized (GraphAnalyticalView.this) {
            if (myGeneration == generation) {
              this.buildError = e;
              if (snapshot != null) {
                this.status = Status.STALE;
              } else {
                this.status = Status.NOT_BUILT;
                // Unregister failed GAV so the name can be reused for a fresh build
                GraphTraversalProviderRegistry.unregister(database, this);
                if (name != null)
                  GraphAnalyticalViewRegistry.unregister(database, name);
              }
            }
            GraphAnalyticalView.this.notifyAll();
          }
          if (isBenignShutdownError(e))
            LogManager.instance().log(this, Level.FINE,
                "Async build of GraphAnalyticalView '%s' aborted because the database is closing", name);
          else
            LogManager.instance().log(this, Level.SEVERE, "Async build of GraphAnalyticalView '%s' failed", e, name);
        } finally {
          // #4956: this virtual thread dies right after the task, but its DatabaseContext entry (pinning Binary
          // temp buffers) would otherwise linger until the next periodic sweep. Remove it before the latch is
          // released so callers observing readiness see a clean state.
          DatabaseContext.INSTANCE.removeCurrentThreadContexts();
          BUILD_PERMITS.release();
          buildQueued.set(false);
          latch.countDown();
          taskCompleted();
        }
      });
    } catch (final RejectedExecutionException e) {
      this.buildError = e;
      this.status = snapshot != null ? Status.STALE : Status.NOT_BUILT;
      this.notifyAll();
      buildQueued.set(false);
      latch.countDown();
      taskCompleted();
      LogManager.instance().log(this, Level.WARNING, "GraphAnalyticalView '%s': async build rejected (executor shut down)", name);
    }
  }

  /**
   * Waits until the view reaches READY status or the timeout expires.
   * <p>
   * If the view is still a deferred restore-from-disk (see #6632), this is one of the two ways — the
   * other being a real query, via {@link #checkBuilt()} — that dispatches it: an explicit status wait is
   * a legitimate signal that the caller wants the view actually resolved, not just optimistically READY.
   *
   * @return true if the view is READY, false if the timeout elapsed or the build failed
   */
  public boolean awaitReady(final long timeout, final TimeUnit unit) {
    triggerDeferredDiskRestoreIfPending();
    final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    try {
      while (true) {
        // Snapshot the three fields under the monitor: onRelevantCommit() resets
        // asyncRebuildNeeded=false and flips status=BUILDING in separate volatile
        // writes, so an unsynchronized reader can observe status=READY together
        // with asyncRebuildNeeded=false and return early on a stale snapshot.
        final Status currentStatus;
        final boolean rebuildPending;
        final CountDownLatch latch;
        synchronized (this) {
          currentStatus = status;
          rebuildPending = asyncRebuildNeeded;
          latch = readyLatch;
        }
        if (currentStatus == Status.READY && !rebuildPending)
          return true;
        if (currentStatus == Status.STALE)
          return false;
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0)
          return false;
        if (latch == null)
          return false;
        if (!latch.await(remainingNanos, TimeUnit.NANOSECONDS))
          return false;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Drops this view: unregisters listeners, removes from registries and schema.
   * Call this when the view is no longer needed (user-initiated removal).
   */
  public void drop() {
    // Dropped views have no schema definition left to restore against, so persisting the CSR here
    // would just be written and immediately deleted below.
    shutdown(false);
    if (name != null) {
      GraphAnalyticalViewPersistence.remove(database, name);
      GraphAnalyticalViewCSRPersistence.delete(database, name);
    }
  }

  /**
   * Shuts down this view without removing the schema definition.
   * Called during database close to release resources while preserving persistence.
   * <p>
   * Blocks (up to {@link #SHUTDOWN_AWAIT_MS}) for any queued or in-flight async build/compaction
   * to drain. This prevents a worker virtual thread from racing the database close path: without
   * this wait the worker can wake up after close() has cleared the per-thread transaction context
   * (or even closed the database) and crash with a misleading SEVERE log line.
   */
  public void shutdown() {
    shutdown(true);
  }

  private void shutdown(final boolean persistCsr) {
    awaitInFlightTasks(SHUTDOWN_AWAIT_MS);
    synchronized (this) {
      // Runs the persist-to-disk write (when eligible) while holding this instance's monitor: any concurrent
      // awaitReady()/getStatus() caller blocks for the duration of the write, not just of a scan - accepted
      // because it only happens once per close and is gated by GAV_PERSIST_CSR.
      if (persistCsr)
        persistCsrIfPossible();
      unregisterChangeListeners();
      GraphTraversalProviderRegistry.unregister(database, this);
      if (name != null)
        GraphAnalyticalViewRegistry.unregister(database, name);
    }
    invalidateGraphStatisticsCache();
  }

  /**
   * Clears the database-scoped {@link com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider}
   * multiplicity cache whenever this view transitions between being available and unavailable as a
   * {@link GraphTraversalProvider} (initial build, or shutdown/drop). Without this, a mean-edges estimate
   * cached from sampling (before this view existed, or after it was dropped) would keep being served
   * unchanged even though a newly built view could now answer it exactly - the count-stamp alone cannot
   * detect this, since building or dropping a view does not change the edge type's record count.
   * <p>
   * Not called from the incremental delta/compaction rebuild paths: those fire on data mutations, which
   * already change the edge count and invalidate the cache stamp on their own.
   */
  private void invalidateGraphStatisticsCache() {
    final Database unwrapped = DatabaseInternal.unwrap(database);
    if (unwrapped instanceof DatabaseInternal di && di.getGraphStatisticsCache() != null)
      di.getGraphStatisticsCache().clear();
  }

  /**
   * Waits up to {@code timeoutMs} for any queued or running async build/compaction to finish.
   * Counter is incremented synchronously when a task is scheduled, so this wait covers tasks
   * that have not yet started executing on the shared virtual-thread pool.
   */
  private void awaitInFlightTasks(final long timeoutMs) {
    if (inFlightTasks.get() == 0)
      return;
    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    synchronized (inFlightMonitor) {
      while (inFlightTasks.get() > 0) {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          LogManager.instance().log(this, Level.WARNING,
              "GraphAnalyticalView '%s': %d in-flight task(s) did not settle within %d ms during shutdown",
              name, inFlightTasks.get(), timeoutMs);
          return;
        }
        try {
          final long waitMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
          inFlightMonitor.wait(waitMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void taskCompleted() {
    if (inFlightTasks.decrementAndGet() == 0) {
      synchronized (inFlightMonitor) {
        inFlightMonitor.notifyAll();
      }
    }
  }

  /**
   * Returns true when {@code e} (or any cause in its chain) reflects the database closing
   * out from under an in-flight build or compaction. These races are benign: the user closed
   * the database while a virtual-thread build was still queued or just dispatched, so the
   * worker correctly aborts. We log them at FINE instead of SEVERE so they don't pollute logs.
   */
  private static boolean isBenignShutdownError(final Throwable e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof DatabaseIsClosedException || current instanceof TransactionException)
        return true;
      current = current.getCause();
    }
    return false;
  }

  // --- GraphTraversalProvider SPI ---

  @Override
  public boolean coversVertexType(final String typeName) {
    if (typeName == null) {
      if (vertexTypes == null)
        return true; // built without filter = all types
      // Check if explicit types cover all vertex types in the schema
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof VertexType && !containsType(vertexTypes, dt.getName()))
          return false;
      return true;
    }
    if (vertexTypes == null)
      return true; // we include all vertex types
    return containsType(vertexTypes, typeName);
  }

  @Override
  public boolean coversEdgeType(final String edgeTypeName) {
    if (edgeTypeName == null) {
      if (edgeTypes == null)
        return true; // built without filter = all types
      // Check if explicit types cover all edge types in the schema
      for (final DocumentType dt : database.getSchema().getTypes())
        if (dt instanceof EdgeType && !containsType(edgeTypes, dt.getName()))
          return false;
      return true;
    }
    if (edgeTypes == null)
      return true; // we include all edge types
    return containsType(edgeTypes, edgeTypeName);
  }

  private static boolean containsType(final String[] types, final String typeName) {
    for (final String t : types)
      if (t.equals(typeName))
        return true;
    return false;
  }

  @Override
  public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    return getVertices(nodeId, direction, edgeTypes);
  }

  @Override
  public void getDegrees(final int[] degrees, final Vertex.DIRECTION direction, final String edgeType) {
    final Snapshot snap = checkBuilt();
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    final DeltaOverlay ov = snap.overlay;
    final int n = Math.min(degrees.length, snap.nodeMapping.size());

    // Zero the whole buffer first: callers reuse buffers (zero-GC APIs), and the CSR fast path below only
    // writes indices [0, n). Without this, a missing CSR (csr == null) or an oversized buffer (degrees.length >
    // nodeMapping.size()) would leave stale values, onto which the overlay deltas would then accumulate.
    Arrays.fill(degrees, 0);

    if (csr != null) {
      // Fast path: direct offset subtraction from CSR arrays — no per-node method dispatch
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        for (int v = 0; v < n; v++)
          degrees[v] = csr.outDegree(v);
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        if (direction == Vertex.DIRECTION.BOTH) {
          for (int v = 0; v < n; v++)
            degrees[v] += csr.inDegree(v);
        } else {
          for (int v = 0; v < n; v++)
            degrees[v] = csr.inDegree(v);
        }
      }
    }

    // Apply overlay deltas if active
    if (ov != null) {
      for (int v = 0; v < degrees.length; v++) {
        if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
          degrees[v] += ov.getAddedOutNeighbors(v, edgeType).length;
          degrees[v] -= ov.countDeletedOutEdges(v, edgeType);
        }
        if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
          degrees[v] += ov.getAddedInNeighbors(v, edgeType).length;
          degrees[v] -= ov.countDeletedInEdges(v, edgeType);
        }
      }
    }
  }

  @Override
  public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();

    // Cannot provide zero-copy view when overlay is active (delta edges modify topology)
    if (hasActiveOverlay(snap))
      return null;

    final int n = snap.nodeMapping.size();

    if (edgeTypes != null && edgeTypes.length == 1) {
      // Single edge type: zero-copy — return CSR arrays directly
      final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeTypes[0]);
      if (csr == null)
        return null;
      return buildNeighborViewFromCSR(csr, n, direction);
    }

    // Multiple or all edge types: merge CSR arrays into a single packed structure
    final Collection<CSRAdjacencyIndex> indices;
    if (edgeTypes != null && edgeTypes.length > 0) {
      final List<CSRAdjacencyIndex> list = new ArrayList<>(edgeTypes.length);
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(et);
        if (csr != null)
          list.add(csr);
      }
      if (list.isEmpty())
        return null;
      if (list.size() == 1)
        return buildNeighborViewFromCSR(list.get(0), n, direction);
      indices = list;
    } else {
      indices = snap.csrPerType.values();
      if (indices.isEmpty())
        return null;
      if (indices.size() == 1)
        return buildNeighborViewFromCSR(indices.iterator().next(), n, direction);
    }

    return buildMergedNeighborView(indices, n, direction);
  }

  private static NeighborView buildNeighborViewFromCSR(final CSRAdjacencyIndex csr, final int n,
      final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.OUT)
      return new NeighborView(n, csr.getForwardOffsets(), csr.getForwardNeighbors());
    if (direction == Vertex.DIRECTION.IN)
      return new NeighborView(n, csr.getBackwardOffsets(), csr.getBackwardNeighbors());

    // BOTH: merge forward + backward into a single packed structure
    return buildMergedNeighborView(List.of(csr), n, direction);
  }

  private static NeighborView buildMergedNeighborView(final Collection<CSRAdjacencyIndex> indices,
      final int n, final Vertex.DIRECTION direction) {
    // First pass: compute degree per node
    final int[] offsets = new int[n + 1];
    for (final CSRAdjacencyIndex csr : indices)
      for (int i = 0; i < n; i++) {
        if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
          offsets[i + 1] += csr.outDegree(i);
        if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
          offsets[i + 1] += csr.inDegree(i);
      }

    // Convert counts to prefix sums
    for (int i = 1; i <= n; i++)
      offsets[i] += offsets[i - 1];

    // Second pass: fill neighbors
    final int totalEdges = offsets[n];
    final int[] neighbors = new int[totalEdges];
    final int[] pos = new int[n]; // current write position per node
    for (int i = 0; i < n; i++)
      pos[i] = offsets[i];

    for (final CSRAdjacencyIndex csr : indices) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        final int[] fwdNbrs = csr.getForwardNeighbors();
        for (int i = 0; i < n; i++) {
          final int start = csr.outOffset(i);
          final int end = csr.outOffsetEnd(i);
          for (int j = start; j < end; j++)
            neighbors[pos[i]++] = fwdNbrs[j];
        }
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        final int[] bwdNbrs = csr.getBackwardNeighbors();
        for (int i = 0; i < n; i++) {
          final int start = csr.inOffset(i);
          final int end = csr.inOffsetEnd(i);
          for (int j = start; j < end; j++)
            neighbors[pos[i]++] = bwdNbrs[j];
        }
      }
    }

    // Sort each node's merged neighbor list so algorithms using sorted intersection
    // (e.g., triangle counting via merge-join) produce correct results
    for (int i = 0; i < n; i++) {
      final int start = offsets[i];
      final int end = offsets[i + 1];
      if (end - start > 1)
        Arrays.sort(neighbors, start, end);
    }

    return new NeighborView(n, offsets, neighbors);
  }

  // --- Node ID / RID mapping ---

  /**
   * Returns the dense node ID for a given RID, or -1 if not in the view.
   */
  public int getNodeId(final RID rid) {
    final Snapshot snap = checkBuilt();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      return ov.resolveNodeId(rid, snap.nodeMapping);
    return snap.nodeMapping.getGlobalId(rid);
  }

  /**
   * Returns the RID for a given dense node ID.
   */
  public RID getRID(final int nodeId) {
    final Snapshot snap = checkBuilt();
    if (nodeId >= snap.nodeMapping.size()) {
      final DeltaOverlay ov = snap.overlay;
      if (ov != null)
        return ov.getOverflowRID(nodeId);
      return null;
    }
    return snap.nodeMapping.getRID(database, nodeId);
  }

  // --- Node type queries ---

  /**
   * Returns the vertex type name for the given node.
   */
  public String getNodeTypeName(final int nodeId) {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping.getTypeName(nodeId);
  }

  /**
   * Returns the bucket index for the given node.
   */
  public int getNodeBucketIdx(final int nodeId) {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping.getBucketIdx(nodeId);
  }

  // --- Edge counting (mirrors Vertex.countEdges) ---

  /**
   * Returns the edge count for a node in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.countEdges(DIRECTION, String...)}.
   */
  public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      long total = 0;
      for (final String edgeType : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
        if (csr != null)
          total += countDirectional(snap, csr, nodeId, direction, edgeType);
        else {
          // No base CSR but overlay may have edges for this type. Nothing to subtract here: the overlay's
          // deleted counts are an exclusion budget spent against a base CSR run, and there is no base run
          // for this type. An edge added and then deleted within the same window is withdrawn from the
          // added index by DeltaOverlay.merge() rather than masked (issue #6775), so what is left is
          // already the live set.
          final DeltaOverlay ov = snap.overlay;
          if (ov != null) {
            if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
              total += ov.getAddedOutNeighbors(nodeId, edgeType).length;
            if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
              total += ov.getAddedInNeighbors(nodeId, edgeType).length;
          }
        }
      }
      return total;
    }
    long total = 0;
    for (final var entry : snap.csrPerType.entrySet())
      total += countDirectional(snap, entry.getValue(), nodeId, direction, entry.getKey());
    return total;
  }

  // --- Neighbor access (mirrors Vertex.getVertices) ---

  /**
   * Returns dense node IDs of connected vertices in the given direction, optionally filtered by edge types.
   * Mirrors {@code Vertex.getVertices(DIRECTION, String...)}.
   */
  public int[] getVertices(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Snapshot snap = checkBuilt();

    if (edgeTypes != null && edgeTypes.length == 1) {
      final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeTypes[0]);
      if (csr != null)
        return getNeighborsFromCSR(snap, csr, nodeId, direction, edgeTypes[0]);
      // No base CSR — check overlay only
      if (snap.overlay == null)
        return new int[0];
      return getNeighborsFromCSR(snap, null, nodeId, direction, edgeTypes[0]);
    }

    // Multiple edge types: collect from each
    final List<int[]> segments = new ArrayList<>();
    int totalLen = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String et : edgeTypes) {
        final CSRAdjacencyIndex csr = snap.csrPerType.get(et);
        final int[] neighbors = csr != null
            ? getNeighborsFromCSR(snap, csr, nodeId, direction, et)
            : getNeighborsFromCSR(snap, null, nodeId, direction, et);
        if (neighbors.length > 0) {
          segments.add(neighbors);
          totalLen += neighbors.length;
        }
      }
    } else {
      for (final var entry : snap.csrPerType.entrySet()) {
        final int[] neighbors = getNeighborsFromCSR(snap, entry.getValue(), nodeId, direction, entry.getKey());
        if (neighbors.length > 0) {
          segments.add(neighbors);
          totalLen += neighbors.length;
        }
      }
    }

    if (totalLen == 0)
      return new int[0];
    if (segments.size() == 1)
      return segments.get(0);

    final int[] result = new int[totalLen];
    int pos = 0;
    for (final int[] seg : segments) {
      System.arraycopy(seg, 0, result, pos, seg.length);
      pos += seg.length;
    }
    Arrays.sort(result);
    return result;
  }

  // --- Vectorized scan ---

  /**
   * Creates a vectorized scan operator for the neighbors of a node, for a specific edge type.
   */
  public CSRScanOperator scanNeighbors(final int nodeId, final Vertex.DIRECTION direction, final String edgeType) {
    final Snapshot snap = checkBuilt();
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    if (csr == null)
      throw new IllegalArgumentException("Edge type not in view: " + edgeType);
    return new CSRScanOperator(csr, nodeId, direction);
  }

  // --- Connectivity (mirrors Vertex.isConnectedTo) ---

  /**
   * Checks if nodeA has an edge to nodeB, optionally filtered by edge type.
   * O(log(degree)) using binary search on sorted CSR.
   */
  public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        if (isConnectedForType(snap, nodeA, nodeB, direction, edgeType))
          return true;
      return false;
    }
    for (final String edgeType : snap.csrPerType.keySet())
      if (isConnectedForType(snap, nodeA, nodeB, direction, edgeType))
        return true;
    return false;
  }

  /**
   * Counts the edges joining nodeA to nodeB, optionally filtered by edge type. A negative value means
   * the count is unknowable; since issue #6775 a delta overlay is no longer a reason for that - a pair
   * the overlay has deletions for is counted exactly, in agreement with {@link #getVertices} and
   * {@link #isConnectedTo} rather than against them (see {@link #countBetweenForType}) - but the
   * "negative means unknown" branch below stays, because it is the contract
   * {@link com.arcadedb.graph.GraphTraversalProvider} publishes and a future edge type may need it.
   * <p>
   * This is the multiplicity {@link #isConnectedTo} collapses to a boolean, and a pattern
   * relationship matches once per edge, so a Cypher hop between two pinned vertices needs it rather
   * than the boolean. Parallel edges sit next to each other in the sorted adjacency array, so the
   * equal range around the binary search hit gives the count in O(log(degree) + multiplicity).
   */
  @Override
  public long countEdgesBetween(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    long total = 0;
    final Iterable<String> types = edgeTypes != null && edgeTypes.length > 0 ?
        Arrays.asList(edgeTypes) : snap.csrPerType.keySet();
    for (final String edgeType : types) {
      final long forType = countBetweenForType(snap, nodeA, nodeB, direction, edgeType);
      if (forType < 0)
        return MULTIPLICITY_UNKNOWN; // one unknown type makes the whole count unknown
      total += forType;
    }
    return total;
  }

  /**
   * Computes the exact mean number of parallel edges joining a connected pair of vertices for an edge
   * type, from the CSR forward-adjacency array - see {@link com.arcadedb.graph.GraphTraversalProvider
   * #getMeanEdgesPerConnectedPair}.
   * <p>
   * Returns {@link #MULTIPLICITY_UNKNOWN} when a delta overlay is active (uncommitted-to-CSR edges would
   * be silently missed) or when this view holds no CSR for the type, matching {@link #countEdgesBetween}'s
   * convention. Each node's forward-neighbor slice is sorted (built that way for {@link #hasForwardEdge}'s
   * binary search), so parallel edges to the same target form a contiguous run and the whole type can be
   * measured with a single linear pass - no per-pair lookups.
   * <p>
   * Unlike {@link com.arcadedb.query.opencypher.optimizer.statistics.StatisticsProvider
   * #calculateMeanEdgesPerConnectedPair}'s sampled estimate, the result here is deliberately not clamped
   * to {@code MAX_MEAN_EDGES_PER_CONNECTED_PAIR} (1000.0): that clamp exists to bound the damage a
   * pathologically clustered *sample* can do to a cost estimate, and this method has no such sampling
   * bias to guard against - it is the type's true population mean.
   */
  @Override
  public double getMeanEdgesPerConnectedPair(final String edgeType) {
    final Snapshot snap = checkBuilt();
    if (snap.overlay != null)
      return MULTIPLICITY_UNKNOWN;

    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    if (csr == null)
      return MULTIPLICITY_UNKNOWN;

    final int totalEdges = csr.getEdgeCount();
    if (totalEdges == 0)
      return 1.0;

    final int[] offsets = csr.getForwardOffsets();
    final int[] neighbors = csr.getForwardNeighbors();

    long distinctPairs = 0;
    final int n = snap.nodeMapping.size();
    for (int node = 0; node < n; node++) {
      final int start = offsets[node];
      final int end = offsets[node + 1];
      if (start == end)
        continue;
      // Sorted slice: a run of equal neighbor ids is one distinct pair with several parallel edges.
      distinctPairs++;
      for (int i = start + 1; i < end; i++)
        if (neighbors[i] != neighbors[i - 1])
          distinctPairs++;
    }

    return distinctPairs == 0 ? 1.0 : (double) totalEdges / distinctPairs;
  }

  /**
   * Counts common neighbors between two nodes, optionally filtered by edge types.
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Snapshot snap = checkBuilt();
    int total = 0;
    if (edgeTypes != null && edgeTypes.length > 0) {
      for (final String edgeType : edgeTypes)
        total += countCommonForType(snap, nodeA, nodeB, direction, edgeType);
    } else {
      for (final String edgeType : snap.csrPerType.keySet())
        total += countCommonForType(snap, nodeA, nodeB, direction, edgeType);
    }
    return total;
  }

  // --- Property access (per-bucket columnar) ---

  /**
   * Returns the property value for a given node, or null if not set.
   * Mirrors {@code Document.get(String)}.
   */
  public Object getProperty(final int nodeId, final String propertyName) {
    final Snapshot snap = checkBuilt();
    // Check overlay first (overrides and overflow nodes)
    final DeltaOverlay ov = snap.overlay;
    if (ov != null) {
      final Object override = ov.getPropertyOverride(nodeId, propertyName);
      if (override != DeltaOverlay.UNSET)
        return override;
    }
    // Fall back to base column store
    if (snap.bucketColumns == null || nodeId >= snap.nodeMapping.size())
      return null;
    final long packed = snap.nodeMapping.getBucketIdxAndLocalId(nodeId);
    return snap.bucketColumns[NodeIdMapping.unpackBucketIdx(packed)].getValue(NodeIdMapping.unpackLocalId(packed), propertyName);
  }

  /**
   * Returns the per-bucket column store for direct vectorized access.
   */
  public ColumnStore getBucketColumnStore(final int bucketIdx) {
    final Snapshot snap = checkBuilt();
    return snap.bucketColumns != null ? snap.bucketColumns[bucketIdx] : null;
  }

  /**
   * Returns the column store for a given node's bucket.
   */
  public ColumnStore getColumnStore() {
    final Snapshot snap = checkBuilt();
    if (snap.bucketColumns != null)
      for (final ColumnStore cs : snap.bucketColumns)
        if (cs.getColumnCount() > 0)
          return cs;
    return null;
  }

  // --- Metadata ---

  /**
   * True while pending committed changes are being served from the delta overlay rather than from the base CSR.
   * <p>
   * The predicate a kernel that reads the CSR arrays directly - {@link GraphAlgorithms#dijkstraSingleSource},
   * and {@link #getNeighborView} for the same reason - has to consult before it may believe them: the overlay
   * is where the additions, the deletions and the new vertices are until the next compaction, and the arrays
   * alone are the graph as it stood at the last build. Every method on this class that goes through
   * {@link #getVertices} applies the overlay itself and needs no such check.
   */
  boolean hasActiveOverlay() {
    return hasActiveOverlay(this.snapshot);
  }

  /**
   * The snapshot this view is currently serving, for a caller that has to read several of its parts and needs
   * them to be parts of the same one.
   * <p>
   * Every accessor on this class re-reads the field, which is right for a caller asking one question and wrong
   * for a kernel reading a CSR's offsets, its neighbours and its weight columns in turn: a commit landing
   * between two of those reads would hand it halves of two different graphs, and it would compute a plausible
   * wrong answer out of them rather than fail. Capture once, read everything through it.
   */
  Snapshot captureSnapshot() {
    return checkBuilt();
  }

  /**
   * The same question asked of a snapshot a caller has already captured, which is how every method that goes
   * on to read that snapshot must ask it: re-reading the field would let a commit land between the check and
   * the read and hand back base arrays belonging to a snapshot that does have an overlay.
   */
  private static boolean hasActiveOverlay(final Snapshot snap) {
    return snap != null && snap.overlay != null;
  }

  /**
   * Returns the CSR index for a specific edge type, or null if not present.
   */
  public CSRAdjacencyIndex getCSRIndex(final String edgeType) {
    final Snapshot snap = checkBuilt();
    return snap.csrPerType.get(edgeType);
  }

  public Set<String> getEdgeTypes() {
    final Snapshot snap = checkBuilt();
    return Collections.unmodifiableSet(snap.csrPerType.keySet());
  }

  public NodeIdMapping getNodeMapping() {
    final Snapshot snap = checkBuilt();
    return snap.nodeMapping;
  }

  public int getNodeCount() {
    final Snapshot snap = checkBuilt();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      return ov.getTotalNodeCount();
    return snap.nodeMapping.size();
  }

  public int getEdgeCount() {
    final Snapshot snap = checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      total += csr.getEdgeCount();
    final DeltaOverlay ov = snap.overlay;
    if (ov != null)
      total += ov.getDeltaEdgeCount();
    return total;
  }

  public int getEdgeCount(final String edgeType) {
    final Snapshot snap = checkBuilt();
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    return csr != null ? csr.getEdgeCount() : 0;
  }

  public String getName() {
    return name;
  }

  public Status getStatus() {
    return status;
  }

  /**
   * Returns the last build error, or null if the last build succeeded.
   */
  public Throwable getBuildError() {
    return buildError;
  }

  public long getBuildTimestamp() {
    final Snapshot snap = this.snapshot;
    return snap != null ? snap.buildTimestamp : 0;
  }

  public long getBuildDurationMs() {
    final Snapshot snap = this.snapshot;
    return snap != null ? snap.buildDurationMs : 0;
  }

  public boolean isBuilt() {
    return snapshot != null;
  }

  /**
   * Returns true when the current CSR was loaded from a persisted file (see #6583) rather than produced by a scan
   * of the graph on this open. Mainly useful for tests and operational visibility (e.g. confirming that a reopen
   * skipped the O(V+E) rebuild).
   */
  public boolean isRestoredFromPersistedCsr() {
    final Snapshot snap = this.snapshot;
    return snap != null && snap.restoredFromDisk;
  }

  /**
   * Whether this provider can be handed to a query right now (see {@link GraphTraversalProviderRegistry#findProvider}).
   * <p>
   * A deferred restore-from-disk (see #6632) reports {@code status == READY} the moment a persisted CSR
   * plausibly applies, before the disk read that actually verifies it has run - a hint, not a promise. Taking
   * that at face value here would let a query commit to this provider and then, inside {@link #checkBuilt()},
   * discover the file is unusable and block through a full {@code O(V+E)} rebuild with no way back to the
   * ordinary path (issue #6641): every {@code checkBuilt()} caller reaches it only after this method has
   * already said yes, so there is nothing left to fall back to once it is in there.
   * <p>
   * While a deferred restore is still pending, this triggers it in the background (a no-op if another caller
   * already has) and reports not-ready for THIS call - exactly the answer an eagerly-marked {@code STALE} view
   * would have given before #6632. The query this call belongs to takes the ordinary path, same as it always
   * did for a genuinely stale view; whichever query asks next sees the resolved outcome (restored from disk,
   * or rebuilt) once the background work completes. Callers that would rather wait for an accelerated first
   * query use {@link #awaitReady} explicitly instead (see {@link com.arcadedb.GlobalConfiguration#GAV_RESTORE_AWAIT_TIMEOUT}).
   * <p>
   * The trigger call runs unconditionally, ahead of reading {@code status}: it is a cheap no-op once nothing
   * is pending (single volatile read), and reading {@code status} afterwards - rather than short-circuiting to
   * {@code false} on {@code pendingDiskRestore} alone - reports the real, resolved state on the rare race
   * where another caller's dispatch settles between the two reads, instead of being needlessly pessimistic.
   */
  public boolean isReady() {
    triggerDeferredDiskRestoreIfPending();
    final Status s = status;
    if (s == Status.READY)
      return true;
    return s == Status.STALE && useWhenStale;
  }

  @Override
  public boolean isStale() {
    return status == Status.STALE;
  }

  public boolean isUseWhenStale() {
    return useWhenStale;
  }

  public void setUseWhenStale(final boolean useWhenStale) {
    this.useWhenStale = useWhenStale;
  }

  public boolean isAutoUpdate() {
    return updateMode != UpdateMode.OFF;
  }

  public UpdateMode getUpdateMode() {
    return updateMode;
  }

  /**
   * Changes the update mode at runtime. Re-registers change listeners as needed.
   * Synchronized to prevent a race window where a committing transaction's delta
   * could be lost between unregister and register.
   */
  public synchronized void setUpdateMode(final UpdateMode newMode) {
    if (this.updateMode == newMode)
      return;
    // Re-register listeners: DeltaCollector behavior depends on the mode.
    // Register new listener before unregistering old one to avoid a window
    // where no listener is active and a committing tx's delta is lost.
    final DeltaCollector oldCollector = this.deltaCollector;
    this.updateMode = newMode;
    if (snapshot != null)
      registerChangeListeners();
    if (oldCollector != null) {
      database.getEvents().unregisterListener((AfterRecordCreateListener) oldCollector);
      database.getEvents().unregisterListener((AfterRecordUpdateListener) oldCollector);
      database.getEvents().unregisterListener((AfterRecordDeleteListener) oldCollector);
      oldCollector.close();
    }
  }

  public String[] getVertexTypes() {
    return vertexTypes;
  }

  public String[] getEdgeTypeFilter() {
    return edgeTypes;
  }

  public String[] getPropertyFilter() {
    return propertyFilter;
  }

  public String[] getEdgePropertyFilter() {
    return edgePropertyFilter;
  }

  @Override
  public String[] getMaterializedEdgeTypes() {
    final Snapshot snap = this.snapshot;
    return snap == null ? null : snap.csrPerType.keySet().toArray(new String[0]);
  }

  /**
   * {@inheritDoc}
   * <p>
   * An active delta overlay no longer makes the answer no. It used to: the columns are aligned with the base
   * CSR's forward edge slots, while {@link #getNeighborIds} serves the overlay's view of the node - deleted
   * edges dropped, added ones merged in, the whole list re-sorted - so the n-th neighbour of that list is not
   * the n-th edge of the column store, and the SPI's old positional accessor answered against the wrong one of
   * the two. That accessor is gone (issue #6315): {@link #edgeWeightsForSlice} resolves the alignment here,
   * where the overlay is applied, and pairs every neighbour with its own edge's value before handing the two
   * back together. An added edge, which has no column slot at all, is served from the value the overlay
   * captured for it at commit time.
   * <p>
   * What the answer still turns on is {@link DeltaOverlay#isEdgePropertiesDirty(String)}: a committed change to
   * a base edge's own properties leaves that type's columns holding a value the database no longer has, and
   * unlike an addition or a deletion it has nothing in the overlay to correct it with - an edge already in the
   * base CSR is addressed by a column slot, and nothing maps that slot back from its RID. The rebuild
   * {@link #applyDelta} forces for it is what repairs the columns; until it lands the honest answer is no, the
   * same reading {@link #getMeanEdgesPerConnectedPair} gives when it cannot answer exactly. This method asks
   * the coarse form of that question - is ANY type out of date - because it is itself the coarse question;
   * {@link #hasEdgeProperty} and {@link #edgeWeightsForSlice}, which are asked about one type, ask about that
   * type alone.
   */
  @Override
  public boolean hasEdgeProperties() {
    final Snapshot snap = this.snapshot;
    return snap != null && snap.edgeColumnStores != null && !snap.edgeColumnStores.isEmpty()
        && !hasStaleEdgeColumns(snap);
  }

  @Override
  public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
    final Snapshot snap = this.snapshot;
    if (snap == null || snap.edgeColumnStores == null || hasStaleEdgeColumns(snap, edgeType))
      return false;
    final ColumnStore edgeColStore = snap.edgeColumnStores.get(edgeType);
    return edgeColStore != null && edgeColStore.getColumn(propertyName) != null;
  }

  /**
   * True when a committed transaction changed one of {@code edgeType}'s edges' properties and the rebuild that
   * repairs its columns has not landed yet. See {@link #hasEdgeProperties()}.
   */
  private static boolean hasStaleEdgeColumns(final Snapshot snap, final String edgeType) {
    return snap.overlay != null && snap.overlay.isEdgePropertiesDirty(edgeType);
  }

  /** True when any edge type's columns are out of date - the coarser question {@link #hasEdgeProperties()} asks. */
  private static boolean hasStaleEdgeColumns(final Snapshot snap) {
    return snap.overlay != null && snap.overlay.isEdgePropertiesDirty();
  }

  /**
   * {@inheritDoc}
   * <p>
   * The neighbours are the ones {@link #getNeighborIds} reports for this type and direction - the same
   * arithmetic, run once here so that each of them can carry its own edge's weight out with it:
   * <ul>
   *   <li>a base edge the overlay has not deleted takes the value of its own forward column slot, which the
   *       walk knows because it is the walk that decided to keep it;</li>
   *   <li>an edge the overlay added takes the value {@link DeltaCollector} captured for it at commit time - it
   *       has no column slot, the columns having been built with the base CSR.</li>
   * </ul>
   * The two are merged and sorted together, exactly as {@link #getNeighborsFromCSR} merges them, with each
   * entry's provenance riding in the low half of the sort key so that no weight can be left behind by the
   * re-sort. That re-sort is what the SPI's old positional accessor was addressing across (issue #6315).
   */
  @Override
  public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
      final String edgeType, final String propertyName, final double defaultWeight,
      final IntConsumer edgeCheckpoint) {
    // BOTH has no adjacency slice of its own to be aligned with; edgeWeightsOf() splits it before calling here.
    if (direction != Vertex.DIRECTION.OUT && direction != Vertex.DIRECTION.IN)
      return null;

    final Snapshot snap = checkBuilt();
    if (snap.edgeColumnStores == null || hasStaleEdgeColumns(snap, edgeType))
      return null;
    final ColumnStore edgeColStore = snap.edgeColumnStores.get(edgeType);
    if (edgeColStore == null)
      return null;
    final Column column = edgeColStore.getColumn(propertyName);
    if (column == null)
      return null;

    final boolean outgoing = direction == Vertex.DIRECTION.OUT;
    final DeltaOverlay ov = snap.overlay;
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);

    // The base slice this node's edges of this type occupy, as offsets into the CSR's own neighbour array.
    //
    // A missing CSR is not a reason to refuse, unlike in the positional accessor this replaced - that one had
    // to compute an index into the slice and had none without it. Here the base slice is simply empty, which
    // for a node with no base edges of this type is the truth, and the overlay's own additions below are
    // answerable regardless: they carry their values rather than being addressed by a column slot. A type with
    // a column store but no CSR is not a state a build produces (Snapshot builds the two together), and if it
    // ever were, an edge type whose edges all arrived after the build would answer for them exactly rather
    // than sending the caller to the records for nothing.
    int[] baseNeighbors = null;
    int baseStart = 0;
    int baseEnd = 0;
    if (csr != null && nodeId < snap.nodeMapping.size()) {
      baseStart = outgoing ? csr.outOffset(nodeId) : csr.inOffset(nodeId);
      baseEnd = outgoing ? csr.outOffsetEnd(nodeId) : csr.inOffsetEnd(nodeId);
      baseNeighbors = outgoing ? csr.getForwardNeighbors() : csr.getBackwardNeighbors();
    }

    // Incoming edges are listed in backward order while the columns are aligned with the forward one, so
    // without the mapping between the two there is no honest answer for a node that has any.
    final int[] bwdToFwd = outgoing || snap.bwdToFwd == null ? null : snap.bwdToFwd.get(edgeType);
    if (!outgoing && baseEnd > baseStart && bwdToFwd == null)
      return null;

    if (ov == null)
      return baseSliceWeights(column, baseNeighbors, baseStart, baseEnd, outgoing, bwdToFwd, defaultWeight,
          edgeCheckpoint);

    final boolean[] deleted = baseEnd > baseStart
        ? deletedSliceMask(baseNeighbors, baseStart, baseEnd, ov, edgeType, nodeId, outgoing) : null;
    // The one thing the overlay cannot resolve. Its deletions are counted per PAIR, which is all the neighbour
    // list needs - drop any one of a pair's parallel edges and the remaining neighbours are the same either way
    // - but not enough to say WHICH of them died, and parallel edges need not weigh the same. The surviving
    // weights would then be a plausible wrong multiset, so the slice is refused and the caller reads the edge
    // records, which know. A pair whose parallel edges were deleted outright is not ambiguous, and neither is
    // the ordinary pair joined by a single edge.
    if (deleted != null && isPartialDeletionOfParallelEdges(baseNeighbors, baseStart, baseEnd, deleted))
      return null;
    // One lookup for both halves: the neighbours and their weights come out of the same index entry.
    final DeltaOverlay.AddedNeighbors added = ov.getAdded(nodeId, edgeType, outgoing);
    final int[] addedNeighbors = added != null ? added.nodeIds() : EMPTY_INT;

    // An overlay somewhere in the graph is not an overlay on THIS node. One unrelated edit keeps an overlay
    // alive until the next compaction, and a full-graph walk would otherwise pay the merge below for every
    // node in it; a node the overlay has neither deleted from nor added to has the base slice for its answer,
    // arrived at by the same arithmetic and byte for byte the same.
    if (deleted == null && addedNeighbors.length == 0)
      return baseSliceWeights(column, baseNeighbors, baseStart, baseEnd, outgoing, bwdToFwd, defaultWeight,
          edgeCheckpoint);

    int keptBase = baseEnd - baseStart;
    if (deleted != null)
      for (final boolean d : deleted)
        if (d)
          keptBase--;

    final int degree = keptBase + addedNeighbors.length;
    if (degree == 0)
      return EMPTY_EDGE_WEIGHTS;

    // Deletions but nothing added: the survivors are a subsequence of a sorted slice, so they are already in
    // the order the merge below would have sorted them into. One linear pass, the same one
    // copyBaseExcludingDeleted makes for the neighbour list, with each survivor's column slot read as it goes.
    if (addedNeighbors.length == 0) {
      final int[] keptNeighbors = new int[degree];
      final double[] keptWeights = new double[degree];
      int kept = 0;
      for (int i = baseStart; i < baseEnd; i++) {
        if (deleted[i - baseStart])
          continue;
        if (edgeCheckpoint != null)
          edgeCheckpoint.accept(kept);
        keptNeighbors[kept] = baseNeighbors[i];
        keptWeights[kept] = columnWeight(column, outgoing ? i : bwdToFwd[i], defaultWeight);
        kept++;
      }
      return new NodeEdgeWeights(keptNeighbors, keptWeights);
    }

    // The neighbours as getNeighborsFromCSR would list them - base survivors then overlay additions, sorted -
    // with each entry's provenance carried through the sort beside it, so no weight can be left behind by the
    // re-sort. CSRBuilder.parallelSort is that exact permutation-carrying sort, already written and already
    // relied upon by the build; a second copy of it here would be one more place for a future fix to miss.
    final int[] neighbors = new int[degree];
    final int[] provenance = new int[degree];
    final int[] baseSlots = keptBase > 0 ? new int[keptBase] : EMPTY_INT;
    int pos = 0;
    for (int i = baseStart; i < baseEnd; i++) {
      if (deleted != null && deleted[i - baseStart])
        continue;
      baseSlots[pos] = outgoing ? i : bwdToFwd[i];
      neighbors[pos] = baseNeighbors[i];
      provenance[pos] = pos;
      pos++;
    }
    for (final int addedNeighbor : addedNeighbors) {
      neighbors[pos] = addedNeighbor;
      provenance[pos] = pos;
      pos++;
    }
    CSRBuilder.parallelSort(neighbors, provenance, 0, degree);

    // The property's position in the filter, resolved once for the whole slice rather than per added edge:
    // the overlay stores an added edge's values by that position, in the filter's own order.
    final Object[][] addedProperties = added != null ? added.properties() : null;
    final int propertyIndex = addedProperties == null ? -1 : indexOfMaterialisedProperty(propertyName);

    final double[] weights = new double[degree];
    for (int i = 0; i < degree; i++) {
      if (edgeCheckpoint != null)
        edgeCheckpoint.accept(i);
      final int origin = provenance[i];
      weights[i] = origin < keptBase ? columnWeight(column, baseSlots[origin], defaultWeight)
          : overlayWeight(addedProperties, origin - keptBase, propertyIndex, defaultWeight);
    }
    return new NodeEdgeWeights(neighbors, weights);
  }

  /**
   * The answer for a node whose edges of this type and direction are all in the base CSR: the slice verbatim,
   * each neighbour beside the value of its own forward column slot.
   */
  private static NodeEdgeWeights baseSliceWeights(final Column column, final int[] baseNeighbors,
      final int baseStart, final int baseEnd, final boolean outgoing, final int[] bwdToFwd,
      final double defaultWeight, final IntConsumer edgeCheckpoint) {
    final int degree = baseEnd - baseStart;
    if (degree == 0)
      return EMPTY_EDGE_WEIGHTS;
    final double[] weights = new double[degree];
    for (int i = 0; i < degree; i++) {
      if (edgeCheckpoint != null)
        edgeCheckpoint.accept(i);
      weights[i] = columnWeight(column, outgoing ? baseStart + i : bwdToFwd[baseStart + i], defaultWeight);
    }
    return new NodeEdgeWeights(Arrays.copyOfRange(baseNeighbors, baseStart, baseEnd), weights);
  }

  /** The position of {@code propertyName} in this view's edge property filter, or -1 if it holds no such name. */
  private int indexOfMaterialisedProperty(final String propertyName) {
    if (edgePropertyFilter == null)
      return -1;
    for (int i = 0; i < edgePropertyFilter.length; i++)
      if (edgePropertyFilter[i].equals(propertyName))
        return i;
    return -1;
  }

  /**
   * Reads one edge's weight out of its column slot, without boxing it on the way.
   * A slot the column has no value for weighs {@code defaultWeight}, the same as an edge that has no value.
   */
  private static double columnWeight(final Column column, final int slot, final double defaultWeight) {
    if (column.isNull(slot))
      return defaultWeight;
    return switch (column.getType()) {
      case DOUBLE -> column.getDouble(slot);
      case INT -> column.getInt(slot);
      case LONG -> column.getLong(slot);
      default -> defaultWeight;
    };
  }

  /** Reads an overlay-added edge's weight out of the values captured for it at commit time. */
  private static double overlayWeight(final Object[][] addedProperties, final int index,
      final int propertyIndex, final double defaultWeight) {
    if (addedProperties == null || propertyIndex < 0 || addedProperties[index] == null)
      return defaultWeight;
    return addedProperties[index][propertyIndex] instanceof Number number ? number.doubleValue() : defaultWeight;
  }

  /**
   * Returns the edge column store for a given edge type (forward-aligned), or null if not configured.
   */
  public ColumnStore getEdgeColumnStore(final String edgeType) {
    final Snapshot snap = checkBuilt();
    return snap.edgeColumnStores != null ? snap.edgeColumnStores.get(edgeType) : null;
  }

  /**
   * Returns the backward-to-forward index mapping for a given edge type, or null if not configured.
   */
  public int[] getBwdToFwdMapping(final String edgeType) {
    final Snapshot snap = checkBuilt();
    return snap.bwdToFwd != null ? snap.bwdToFwd.get(edgeType) : null;
  }

  Database getDatabase() {
    return database;
  }

  public int getCompactionThreshold() {
    return compactionThreshold;
  }

  public void setCompactionThreshold(final int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
  }

  public int getPropertySampleSize() {
    return propertySampleSize;
  }

  void setPropertySampleSize(final int propertySampleSize) {
    this.propertySampleSize = propertySampleSize;
  }

  public long getMemoryUsageBytes() {
    final Snapshot snap = this.snapshot;
    if (snap == null)
      return 0;
    long total = 0;
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      total += csr.getMemoryUsageBytes();
    if (snap.bucketColumns != null)
      for (final ColumnStore cs : snap.bucketColumns)
        total += cs.getMemoryUsageBytes();
    if (snap.nodeMapping != null)
      total += snap.nodeMapping.getMemoryUsageBytes();
    if (snap.edgeColumnStores != null)
      for (final ColumnStore ecs : snap.edgeColumnStores.values())
        total += ecs.getMemoryUsageBytes();
    if (snap.bwdToFwd != null)
      for (final int[] mapping : snap.bwdToFwd.values())
        total += (long) mapping.length * Integer.BYTES;
    return total;
  }

  /**
   * Returns detailed statistics about this view as a map.
   * Includes vertex/edge counts, memory breakdown, edge types, and overlay state.
   */
  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new HashMap<>();
    stats.put("name", name);
    stats.put("status", status.name());
    stats.put("updateMode", updateMode.name());

    final Snapshot snap = this.snapshot;
    if (snap == null) {
      stats.put("nodeCount", 0);
      stats.put("edgeCount", 0);
      stats.put("memoryUsageBytes", 0L);
      return stats;
    }

    stats.put("nodeCount", getNodeCount());
    stats.put("edgeCount", getEdgeCount());
    stats.put("buildTimestamp", snap.buildTimestamp);
    stats.put("buildDurationMs", snap.buildDurationMs);

    // Per-edge-type breakdown
    final Map<String, Object> edgeTypeStats = new HashMap<>();
    for (final var entry : snap.csrPerType.entrySet()) {
      final CSRAdjacencyIndex csr = entry.getValue();
      final Map<String, Object> etStat = new HashMap<>();
      etStat.put("edgeCount", csr.getEdgeCount());
      etStat.put("nodeCount", csr.getNodeCount());
      etStat.put("memoryBytes", csr.getMemoryUsageBytes());
      edgeTypeStats.put(entry.getKey(), etStat);
    }
    stats.put("edgeTypes", edgeTypeStats);

    // Memory breakdown
    long csrMemory = 0;
    for (final CSRAdjacencyIndex csr : snap.csrPerType.values())
      csrMemory += csr.getMemoryUsageBytes();
    long columnMemory = 0;
    int propertyCount = 0;
    if (snap.bucketColumns != null) {
      for (final ColumnStore cs : snap.bucketColumns) {
        columnMemory += cs.getMemoryUsageBytes();
        propertyCount = Math.max(propertyCount, cs.getColumnCount());
      }
    }
    long mappingMemory = snap.nodeMapping != null ? snap.nodeMapping.getMemoryUsageBytes() : 0;

    stats.put("memoryUsageBytes", csrMemory + columnMemory + mappingMemory);
    stats.put("csrMemoryBytes", csrMemory);
    stats.put("columnMemoryBytes", columnMemory);
    stats.put("mappingMemoryBytes", mappingMemory);
    stats.put("propertyCount", propertyCount);

    if (vertexTypes != null)
      stats.put("vertexTypes", Arrays.asList(vertexTypes));
    if (edgeTypes != null)
      stats.put("edgeTypeFilter", Arrays.asList(edgeTypes));
    if (propertyFilter != null)
      stats.put("propertyFilter", Arrays.asList(propertyFilter));
    if (edgePropertyFilter != null)
      stats.put("edgePropertyFilter", Arrays.asList(edgePropertyFilter));

    // Edge property memory
    long edgePropMemory = 0;
    int edgePropColumns = 0;
    if (snap.edgeColumnStores != null)
      for (final ColumnStore ecs : snap.edgeColumnStores.values()) {
        edgePropMemory += ecs.getMemoryUsageBytes();
        edgePropColumns += ecs.getColumnCount();
      }
    if (snap.bwdToFwd != null)
      for (final int[] mapping : snap.bwdToFwd.values())
        edgePropMemory += (long) mapping.length * Integer.BYTES;
    if (edgePropMemory > 0) {
      stats.put("edgePropertyMemoryBytes", edgePropMemory);
      stats.put("edgePropertyColumns", edgePropColumns);
    }

    // Overlay state
    final DeltaOverlay ov = snap.overlay;
    if (ov != null) {
      stats.put("overlayActive", true);
      stats.put("overlayOverflowNodes", ov.getOverflowCount());
      stats.put("overlayDeltaEdges", ov.getDeltaEdgeCount());
    } else {
      stats.put("overlayActive", false);
    }

    stats.put("compactionThreshold", compactionThreshold);
    final Throwable err = buildError;
    if (err != null)
      stats.put("buildError", err.getMessage());
    return stats;
  }

  // --- Private helpers ---

  private static Snapshot snapshotFromResult(final CSRBuilder.CSRResult result, final long durationMs, final long asOfTransactionId,
      final String[] builtVertexTypes, final String[] builtEdgeTypes, final String[] builtPropertyFilter,
      final String[] builtEdgePropertyFilter) {
    return new Snapshot(result.getCsrPerType(), result.getMapping(), result.getBucketColumns(),
        result.getEdgeColumnStores(), result.getBwdToFwd(),
        null, System.currentTimeMillis(), durationMs, asOfTransactionId, false,
        builtVertexTypes, builtEdgeTypes, builtPropertyFilter, builtEdgePropertyFilter);
  }

  /**
   * The database's last committed transaction id, sampled before a CSR scan starts (see {@link
   * Snapshot#asOfTransactionId}). Reading it before {@code database.begin()} for the scan — rather than after — is
   * what makes the certificate sound: whatever the scan reads, it reads no earlier than this point, so "nothing
   * committed since this value" on a later open implies "nothing committed since the scan ran", regardless of the
   * read transaction's own isolation semantics.
   */
  private long currentLastTransactionId() {
    return ((DatabaseInternal) database).getTransactionManager().getLastTransactionId();
  }

  /**
   * Persists the current CSR to disk (see #6583) so the next open can reuse it instead of rescanning the graph,
   * when doing so is both possible and safe:
   * <ul>
   *   <li>the view is named — an anonymous view has no schema definition to restore against on a later open, so
   *       there would be nothing to reload the file for</li>
   *   <li>status is READY with a valid certificate — a STALE view's on-disk certificate would already be
   *       superseded by the very commit that made it STALE, so persisting it would only cost a write that the
   *       next open's certificate check is guaranteed to reject; BUILDING has no completed snapshot yet</li>
   *   <li>the overlay (if any) has no pending changes — the certificate vouches only for the base CSR, so a
   *       SYNCHRONOUS view with buffered overlay deltas would silently drop them if reloaded from this file</li>
   * </ul>
   * Called from {@link #shutdown()} while holding this instance's monitor, after any in-flight build/compaction
   * has settled. Failures are logged and otherwise ignored: the next open simply falls back to a full rebuild,
   * exactly as it always has.
   */
  private void persistCsrIfPossible() {
    if (name == null || status != Status.READY)
      return;
    // The certificate is only sound where TransactionManager.getLastTransactionId() reflects every commit that
    // could have touched the covered types. That holds for a standalone database or an HA leader (the only paths
    // that call getNextTransactionId()), but not for a follower: TransactionManager.applyChanges(), the path
    // replicated commits are applied through, never advances that counter, so a follower's certificate would
    // freeze at whatever value it had when this snapshot was built, then "match" on every later reopen no matter
    // how much further replicated data has landed since - the same failure shape as the lost-recency-marker case,
    // but permanent rather than self-healing. Leader-only mirrors the existing precedent for HA-unsafe derived
    // state (see TimeSeriesMaintenanceScheduler.runMaintenance()) with no compile dependency on the HA module.
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    if (dbInternal.isReplicated() && !dbInternal.isLeader())
      return;
    final Snapshot snap = this.snapshot;
    if (snap == null || snap.asOfTransactionId < 0)
      return;
    if (snap.overlay != null && snap.overlay.hasChanges())
      return;
    // Nothing has changed since this exact snapshot was loaded from disk on this open (no overlay changes,
    // per the check above, and a fresh scan always produces a new Snapshot instance) — the file on disk
    // already matches it byte-for-byte, so writing it again would be pure waste.
    if (snap.restoredFromDisk)
      return;
    if (!database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GAV_PERSIST_CSR))
      return;
    try {
      // Written from snap's OWN recorded scope, not this.vertexTypes/edgeTypes/propertyFilter/edgePropertyFilter:
      // the public two-arg build(vertexTypes, edgeTypes) can rescan a named view with a scope narrower than it
      // was constructed with, and those fields (final, never updated) would then describe more than snap actually
      // covers - the header must match what was actually scanned, or a later reopen could restore a snapshot that
      // silently under-covers what the view claims to (#6583 review).
      GraphAnalyticalViewCSRPersistence.save(database, name, snap.vertexTypes, snap.edgeTypes, snap.propertyFilter,
          snap.edgePropertyFilter, snap);
    } catch (final OutOfMemoryError | Exception e) {
      // OutOfMemoryError is caught deliberately, matching load()'s equivalent guard: save() assembles the whole
      // CSR payload into one contiguous byte[] (and a second one if encryption is configured) before writing
      // anything, right at shutdown()/database.close() after a build/rescan has already put memory pressure on
      // the JVM. Left uncaught, an OOM here would propagate out of GraphAnalyticalViewRegistry.shutdownAll()'s
      // per-view loop with no try/catch of its own, aborting it and leaking every view after this one in
      // iteration order (unregisterChangeListeners()/GraphTraversalProviderRegistry.unregister() never run for
      // them) - directly contradicting this method's own "failures are logged and otherwise ignored" contract.
      LogManager.instance().log(this, Level.WARNING, "GraphAnalyticalView '%s': failed to persist CSR to disk", e, name);
    }
  }

  /**
   * Used by {@link GraphAnalyticalViewBuilder#restoreFromDiskOrBuildAsync} on database open (see #6583,
   * #6632). Before #6632 this eagerly called {@link #tryRestoreFromPersistedCsr()} right here, which reads
   * and deserializes the whole persisted CSR — a real, if bounded, cost (hundreds of ms to ~1s at 10M
   * vertices, per #6632's own measurements) that a session which never queries the view paid for nothing.
   * When a persisted file plausibly applies, this instead marks the view READY without touching the file
   * and defers the actual read to whatever happens first: a real query ({@link #checkBuilt()}) or an
   * explicit {@link #awaitReady} call — so a session that opens and closes without ever touching the view
   * now costs what the no-view baseline costs, not what a restore costs.
   * <p>
   * When no persisted file could plausibly apply — disabled, an HA follower, an unreliable recency signal,
   * or simply nothing on disk — falls back to the unchanged, already-async {@link #buildAsync()}
   * immediately instead of deferring: a full scan is expensive regardless of whether it's queried, so
   * there's nothing to gain by delaying its (already background) start, and every ms of head start
   * shortens how long a soon-arriving query has to wait for it.
   */
  void restoreFromDiskOrBuildAsync() {
    if (mayHavePersistedCsr()) {
      // Order matters: a concurrent reader that observes status==READY (a volatile read) is guaranteed,
      // by plain volatile publish semantics, to also observe every plain write that preceded it on this
      // thread — including pendingDiskRestore — so checkBuilt()/awaitReady() can never see READY paired
      // with a stale pendingDiskRestore=false. Same pattern build() already relies on for snapshot+status.
      pendingDiskRestore = true;
      status = Status.READY;
    } else
      buildAsync();
  }

  /**
   * Cheap, synchronous check: does a persisted CSR file plausibly apply right now? Mirrors every guard in
   * {@link #tryRestoreFromPersistedCsr()} except the (expensive) definition/certificate read — a "yes"
   * here is a hint, not a promise: {@link #tryRestoreFromPersistedCsr()} re-verifies everything itself,
   * from scratch, whenever the deferred restore actually runs, and correctly falls back to a rebuild if
   * anything no longer holds by then (including a commit that landed while the restore was pending, since
   * nothing is watching for one — see {@link #restoreFromDiskOrBuildAsync()}).
   * <p>
   * KEEP IN SYNC with {@link #tryRestoreFromPersistedCsr()}'s own guard clauses (review on PR #6633): a
   * drift between the two can't corrupt data — {@code tryRestoreFromPersistedCsr()} always has the final,
   * authoritative say — but it would either waste a lazy round-trip that immediately falls back to a
   * rebuild, or skip the lazy fast path entirely for a case that would have actually succeeded.
   */
  private boolean mayHavePersistedCsr() {
    if (name == null)
      return false;
    if (!database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GAV_PERSIST_CSR))
      return false;
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    if (dbInternal.isReplicated() && !dbInternal.isLeader())
      return false;
    if (!dbInternal.getTransactionManager().isRecencySignalReliable())
      return false;
    return GraphAnalyticalViewCSRPersistence.fileFor(database, name).isFile();
  }

  /**
   * Cheap fast-path check before {@link #dispatchDeferredRestore()}: avoids paying for that method's
   * {@code synchronized} entry (and the monitor contention that would imply under concurrent callers) on
   * every {@link #checkBuilt()}/{@link #awaitReady} call once nothing is pending anymore, which is the
   * overwhelmingly common case after the first caller resolves it.
   */
  private void triggerDeferredDiskRestoreIfPending() {
    if (pendingDiskRestore)
      dispatchDeferredRestore();
  }

  /**
   * Dispatches the deferred restore-or-build (see #restoreFromDiskOrBuildAsync()) exactly once, the first
   * time either {@link #checkBuilt()} or {@link #awaitReady} needs it resolved. A no-op once already
   * dispatched (including by a concurrent caller — the double-checked {@code pendingDiskRestore} flag is
   * the single point of truth for "has this already been kicked off").
   * <p>
   * The whole method is {@code synchronized}, mirroring {@link #buildAsync()} and {@link
   * #onRelevantCommit()} exactly: {@code inFlightTasks.incrementAndGet()} and the executor dispatch happen
   * while still holding {@code this}'s monitor, so {@link #shutdown()}'s {@code awaitInFlightTasks()} can
   * never observe {@code inFlightTasks == 0} in the gap between deciding to dispatch and the counter
   * actually reflecting it — the same race {@code awaitInFlightTasks()}'s own javadoc calls out ("counter is
   * incremented synchronously when a task is scheduled"). An earlier version of this method incremented the
   * counter just outside the synchronized block and reopened exactly that gap (review on PR #6633).
   */
  private synchronized void dispatchDeferredRestore() {
    if (!pendingDiskRestore)
      return;
    pendingDiskRestore = false;
    // Stays true for the FULL chain, including a buildAsync() fallback the dispatched task below waits out
    // itself — not just until the outer task hands off to it — so checkBuilt() only ever waits for work
    // this method actually started, never for an unrelated buildAsync()/onRelevantCommit() rebuild on a
    // brand-new view built directly (which keeps its own documented fail-fast contract unchanged).
    deferredRestoreInFlight = true;
    // See the `generation` field javadoc (issue #6636): captured at dispatch time, before the executor
    // task even queues, so a direct build()/buildAsync() call that runs and completes while this restore
    // is still parked (e.g. waiting on BUILD_PERMITS) correctly supersedes it - even though the restore's
    // own freshness certificate, re-sampled from scratch when it finally runs, would otherwise still
    // "match" (nothing needs to have been committed for a redundant rescan to be dispatched).
    final long myGeneration = ++generation;
    final CountDownLatch latch = new CountDownLatch(1);
    readyLatch = latch;
    status = Status.BUILDING;
    inFlightTasks.incrementAndGet();
    try {
      getExecutor().execute(() -> {
        try {
          BUILD_PERMITS.acquireUninterruptibly();
          final boolean restored;
          try {
            restored = tryRestoreFromPersistedCsr(myGeneration);
          } finally {
            BUILD_PERMITS.release();
          }
          if (!restored) {
            // No usable file after all (vanished, corrupted, invalidated by a commit that landed while
            // this was pending and unwatched, or superseded by a newer build/restore) — fall back exactly
            // as the eager #6583 path always did. Waited out here (this virtual thread parking costs
            // nothing) rather than left to complete on its own dispatched task, so deferredRestoreInFlight
            // stays true for the fallback's own duration too, not just for the near-instant hand-off to it.
            buildAsync();
            awaitDeferredDiskRestoreSettled();
          }
        } catch (final Exception e) {
          // tryRestoreFromPersistedCsr() only catches around its own load() call, so anything else
          // unexpected (e.g. the database closing mid-check) would otherwise leave status stuck at
          // BUILDING forever - awaitDeferredDiskRestoreSettled()'s loop would then busy-spin on every
          // future checkBuilt() call (its latch is already counted down, so await() returns instantly,
          // and status never leaves BUILDING to end the loop). Mirrors buildAsync()'s own catch exactly.
          synchronized (this) {
            if (myGeneration == generation) {
              buildError = e;
              status = snapshot != null ? Status.STALE : Status.NOT_BUILT;
            }
            notifyAll();
          }
          if (isBenignShutdownError(e))
            LogManager.instance().log(this, Level.FINE,
                "Deferred restore of GraphAnalyticalView '%s' aborted because the database is closing", name);
          else
            LogManager.instance().log(this, Level.WARNING, "Failed to resolve deferred restore for GraphAnalyticalView '%s'", e, name);
        } finally {
          deferredRestoreInFlight = false;
          latch.countDown();
          taskCompleted();
        }
      });
    } catch (final RejectedExecutionException e) {
      // Mirrors buildAsync()'s equivalent handler exactly (review on PR #6633): buildError/notifyAll()
      // were missing here, and status was unconditionally NOT_BUILT even when a prior snapshot exists
      // (e.g. a rebuild superseded by this same dispatch, or a stale-but-still-served one) - both quietly
      // made getBuildError()/isReady()'s STALE+useWhenStale path less reliable for this rejection path
      // than for every other one in this class.
      deferredRestoreInFlight = false;
      buildError = e;
      status = snapshot != null ? Status.STALE : Status.NOT_BUILT;
      notifyAll();
      latch.countDown();
      taskCompleted();
      LogManager.instance().log(this, Level.WARNING,
          "GraphAnalyticalView '%s': deferred restore-from-disk rejected (executor shut down)", name);
    }
  }

  /**
   * Blocks the calling thread until a BUILDING view settles to READY/STALE/NOT_BUILT, with no timeout.
   * Used by {@link #checkBuilt()} (whose caller has no fallback once it is already there) and, internally,
   * by {@link #dispatchDeferredRestore()} itself to wait out its own {@link #buildAsync()} fallback. Tolerant
   * of {@code readyLatch} being swapped mid-wait (e.g. {@link #dispatchDeferredRestore}'s own latch being
   * superseded by a nested {@link #buildAsync()} fallback's own): re-reads both fields together on every
   * iteration, same as {@link #awaitReady}'s loop.
   */
  private void awaitDeferredDiskRestoreSettled() {
    try {
      while (true) {
        final Status currentStatus;
        final CountDownLatch latch;
        synchronized (this) {
          currentStatus = status;
          latch = readyLatch;
        }
        if (currentStatus != Status.BUILDING)
          return;
        latch.await();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Attempts to load a persisted CSR from disk instead of scanning the graph (see #6583). Returns true and leaves
   * the view READY with the loaded snapshot when a file exists, its definition matches this view's configuration,
   * and its freshness certificate (the database's last committed transaction id at persist time) still matches the
   * database's current last committed transaction id — i.e. nothing was committed in between. Returns false
   * (leaving the view's state untouched) for a missing file, a mismatched definition or certificate, or any read
   * failure, so the caller can fall back to {@link #buildAsync()} exactly as it would have without this path.
   * <p>
   * The guard clauses below are mirrored (cheaply, without the file read) by {@link #mayHavePersistedCsr()} —
   * KEEP THEM IN SYNC (review on PR #6633).
   * <p>
   * {@code expectedGeneration} is {@link #dispatchDeferredRestore()}'s own {@code myGeneration}, captured
   * when it dispatched this restore. Checked right before the write to {@code this.snapshot}/{@code
   * this.status}: a direct {@code build()}/{@code buildAsync()} that ran and completed while this restore
   * was still in flight (e.g. parked on {@code BUILD_PERMITS}, which does not gate {@code build()}) bumps
   * {@link #generation} past this value even when nothing was committed in between - so the certificate
   * above can still "match" a now-superseded snapshot. Returns {@code true} (not {@code false}) in that
   * case: a usable file WAS found, so {@link #dispatchDeferredRestore()} must not additionally trigger a
   * redundant {@link #buildAsync()} fallback on top of the build that already won (issue #6636).
   */
  synchronized boolean tryRestoreFromPersistedCsr(final long expectedGeneration) {
    if (name == null)
      return false;
    if (!database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GAV_PERSIST_CSR))
      return false;
    // Mirrors the same leader-only gate in persistCsrIfPossible(): a follower's certificate can never be trusted
    // (its TransactionManager counter is frozen against replicated commits), so this file - if one even exists,
    // e.g. left over from a promotion/demotion - must never be restored on this node either. Checked before
    // isRecencySignalReliable() purely because it is the cheaper of the two disqualifying checks.
    final DatabaseInternal dbInternal = (DatabaseInternal) database;
    if (dbInternal.isReplicated() && !dbInternal.isLeader())
      return false;
    // The certificate is a plain equality check against the database's last committed transaction id, which is
    // only sound while that counter's own history is verifiably continuous. If this open couldn't reconstruct it
    // from real evidence (a lost/corrupt recency marker with no WAL left to recover it from - a clean close
    // removes the WAL, so the marker is the sole durable record at that point), the counter silently restarts
    // and will climb back through every value it held before, including one a stale file on disk might still
    // carry as its certificate. Refusing the fast path here until a fresh, trustworthy certificate is written at
    // this session's own close is the same "no manifest reads as unverifiable, not as valid" rule #6106 already
    // established for the vector index's correspondence artifact.
    if (!dbInternal.getTransactionManager().isRecencySignalReliable())
      return false;
    final long currentTxId = currentLastTransactionId();
    final Snapshot restored;
    try {
      restored = GraphAnalyticalViewCSRPersistence.load(database, name, vertexTypes, edgeTypes, propertyFilter,
          edgePropertyFilter, currentTxId);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "GraphAnalyticalView '%s': failed to load persisted CSR from disk", e, name);
      return false;
    }
    if (restored == null)
      return false;

    // Reuse whichever latch is already installed rather than publishing a new one: the view is registered
    // before this method runs (directly, as the first status transition on a freshly built view; or via
    // dispatchDeferredRestore(), which installs its own before calling this - see #6632), so a concurrent
    // awaitReady() caller may already be waiting on that installed latch. Counting down a *new* instance
    // instead would leave that caller hanging until its timeout even though the view is READY.
    final CountDownLatch latch = readyLatch;

    if (expectedGeneration != generation) {
      // A direct build()/buildAsync() (or another restore) already completed and published a fresher
      // snapshot while this one was in flight — see this method's javadoc (issue #6636). The file we just
      // loaded is redundant, not wrong; discard it without touching this.snapshot/this.status, but still
      // wake any waiter parked on the latch/monitor this restore itself installed.
      LogManager.instance().log(this, Level.FINE,
          "GraphAnalyticalView '%s': persisted CSR restore discarded (superseded by a newer build/restore)", name);
      this.notifyAll();
      latch.countDown();
      return true;
    }

    this.snapshot = restored;
    this.status = Status.READY;
    this.notifyAll();
    latch.countDown();
    invalidateGraphStatisticsCache();
    if (deltaCollector == null)
      registerChangeListeners();

    // A commit to a covered type landing between sampling currentTxId (above) and the listener registration
    // just above this line would be invisible to the restored snapshot (no scan ran to absorb it, unlike
    // buildAsync(), whose real scan runs AFTER its own sample so a racing commit is already captured in what
    // it reads) and invisible to onRelevantCommit()/applyDelta() too (the listeners were not live yet when it
    // happened). This specific few-instruction window is still only defensive - no existing test hits it -
    // but the surrounding claim that this whole method is unreachable outside LocalDatabase.open() no longer
    // holds since #6632 (review on PR #6633): dispatchDeferredRestore()'s background task also calls this
    // method, well after open() has returned and the caller can freely commit against it. The COARSER case -
    // a commit landing before this method is even dispatched, caught by the certificate mismatch in
    // GraphAnalyticalViewCSRPersistence.load() above rather than by this specific check - is genuinely
    // reachable and exercised by csrLazyPendingRestoreCatchesAnInterveningCommitBeforeFirstQuery. Marking
    // STALE here mirrors exactly what onRelevantCommit() does for a real relevant commit under
    // OFF/SYNCHRONOUS mode: conservative, but never wrong.
    if (currentLastTransactionId() != currentTxId) {
      LogManager.instance().log(this, Level.WARNING,
          "GraphAnalyticalView '%s': a commit landed while restoring its CSR from disk; marking it STALE rather than risk missing it",
          name);
      status = Status.STALE;
    }

    LogManager.instance().log(this, Level.INFO,
        "GraphAnalyticalView '%s': restored CSR from disk (asOfTransactionId=%d), skipping full rebuild", name, currentTxId);
    return true;
  }

  private void registerChangeListeners() {
    deltaCollector = new DeltaCollector(this);
    database.getEvents().registerListener((AfterRecordCreateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordUpdateListener) deltaCollector);
    database.getEvents().registerListener((AfterRecordDeleteListener) deltaCollector);
  }

  private void unregisterChangeListeners() {
    if (deltaCollector != null) {
      database.getEvents().unregisterListener((AfterRecordCreateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordUpdateListener) deltaCollector);
      database.getEvents().unregisterListener((AfterRecordDeleteListener) deltaCollector);
      deltaCollector.close();
      deltaCollector = null;
    }
  }

  /**
   * Called by the DeltaCollector (ASYNCHRONOUS/OFF mode) after a committed transaction affected
   * covered vertex/edge types. ASYNCHRONOUS triggers an async rebuild, OFF marks the view as STALE.
   */
  synchronized void onRelevantCommit() {
    if (updateMode == UpdateMode.ASYNCHRONOUS) {
      if (!compacting.compareAndSet(false, true)) {
        // A rebuild is already in progress — flag that another one is needed once it finishes.
        asyncRebuildNeeded = true;
        return;
      }
      asyncRebuildNeeded = false;
      // See the `generation` field javadoc (issue #6636): captured at dispatch time so a later-dispatched
      // build (e.g. a direct build()/buildAsync() call racing this rebuild) correctly supersedes it.
      final long myGeneration = ++generation;
      final CountDownLatch latch = new CountDownLatch(1);
      this.readyLatch = latch;
      this.status = Status.BUILDING;
      inFlightTasks.incrementAndGet();
      try {
        getExecutor().execute(() -> {
          BUILD_PERMITS.acquireUninterruptibly();
          try {
            final long asOfTransactionId = currentLastTransactionId();
            database.begin();
            try {
              final long buildStart = System.currentTimeMillis();
              final CSRBuilder builder = new CSRBuilder(database, propertyFilter, edgePropertyFilter, propertySampleSize);
              final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
              final long durationMs = System.currentTimeMillis() - buildStart;
              // Synchronized swap: check that the mode hasn't changed to SYNCHRONOUS during rebuild.
              // If it did, applyDelta() may have enriched the current snapshot with overlay deltas
              // that would be lost by an unconditional swap. Also check generation: a direct build()/
              // buildAsync() dispatched after this rebuild started must win even if this one finishes
              // last (issue #6636).
              synchronized (GraphAnalyticalView.this) {
                if (updateMode == UpdateMode.ASYNCHRONOUS && myGeneration == generation) {
                  this.snapshot = snapshotFromResult(result, durationMs, asOfTransactionId, vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);
                  this.status = Status.READY;
                } else
                  LogManager.instance().log(this, Level.INFO,
                      "GraphAnalyticalView '%s': async rebuild result discarded (update mode changed to %s during rebuild, or superseded by a newer build/restore)", name, updateMode);
              }

            } finally {
              if (database.isTransactionActive())
                database.rollback();
            }
          } catch (final Exception e) {
            synchronized (GraphAnalyticalView.this) {
              if (myGeneration == generation) {
                this.buildError = e;
                this.status = Status.STALE;
              }
            }
            if (isBenignShutdownError(e))
              LogManager.instance().log(this, Level.FINE,
                  "Async rebuild of GraphAnalyticalView '%s' aborted because the database is closing", name);
            else
              LogManager.instance().log(this, Level.WARNING, "Failed to rebuild GraphAnalyticalView '%s'", e, name);
          } finally {
            // #4956: drop this worker's DatabaseContext entry (see buildAsync)
            DatabaseContext.INSTANCE.removeCurrentThreadContexts();
            BUILD_PERMITS.release();
            compacting.set(false);
            latch.countDown();
            taskCompleted();
            // Volatile read outside the monitor is intentional: visibility is guaranteed by the
            // volatile flag, and onRelevantCommit() will acquire the lock when it executes.
            if (asyncRebuildNeeded)
              onRelevantCommit();
          }
        });
      } catch (final RejectedExecutionException e) {
        this.status = Status.STALE;
        compacting.set(false);
        latch.countDown();
        taskCompleted();
        LogManager.instance().log(this, Level.WARNING, "GraphAnalyticalView '%s': async rebuild rejected (executor shut down)", name);
      }
    } else {
      this.status = Status.STALE;
    }
  }

  /**
   * Applies a transaction delta to the overlay (SYNCHRONOUS mode). Called from the post-commit callback.
   * If the overlay grows beyond the compaction threshold, triggers a full rebuild in the background.
   * <p>
   * Thread-safety guarantees:
   * <ul>
   *   <li>Synchronized to prevent lost updates from concurrent post-commit callbacks:
   *       two threads reading the same snapshot, merging independently, and last-write-wins</li>
   *   <li>Only one compaction thread runs at a time (AtomicBoolean guard)</li>
   *   <li>Raw TxDeltas are buffered during compaction so they can be re-applied against the
   *       new NodeIdMapping after the swap — this avoids both the cost of a full retry rebuild
   *       and the risk of losing deltas that committed after the CSR scan started</li>
   * </ul>
   */
  synchronized void applyDelta(final TxDelta delta) {
    // Guard against post-shutdown invocation from a lingering commit callback
    final Snapshot current = this.snapshot;
    if (current == null)
      return;
    final DeltaOverlay base = current.overlay != null ? current.overlay : new DeltaOverlay(current.nodeMapping.size());
    final DeltaOverlay merged = base.merge(delta, current.nodeMapping);
    this.snapshot = current.withOverlay(merged);

    // Buffer raw delta during compaction for re-application against the new mapping.
    // TxDelta uses RIDs (not dense IDs), so it can be cleanly re-applied against any mapping.
    if (pendingDeltas != null) {
      if (pendingDeltas.size() >= MAX_PENDING_DELTAS) {
        // Too many deltas accumulated during compaction — the rebuild result would be immediately
        // stale again. Discard the buffer so the compaction thread aborts cleanly on swap.
        LogManager.instance().log(this, Level.WARNING,
            "GraphAnalyticalView '%s': pending delta buffer exceeded %d during compaction, aborting compaction", name, MAX_PENDING_DELTAS);
        pendingDeltas = null;
      } else
        pendingDeltas.add(delta);
    }

    // A change to a base edge's own properties can only be made visible by rebuilding the base columns: the
    // edge is addressed there by a column slot, and nothing maps that slot back from its RID. Force a rebuild
    // regardless of the edge-count threshold; otherwise the change would be silently dropped until the next
    // compaction (#4513). Asked of the merged overlay rather than of the delta because the two are not the
    // same question: an update to an edge the overlay itself holds is applied there and dirties nothing, and
    // that is the ordinary `newEdge(...).save()` (issue #6315). Sticky while the columns are still out of
    // date, so that a rebuild discarded as superseded is retried by the next commit rather than leaving the
    // view unable to serve edge properties until the compaction threshold happens to be crossed.
    final boolean forceRebuild = merged.isEdgePropertiesDirty();

    if (forceRebuild || (compactionThreshold > 0 && Math.abs(merged.getDeltaEdgeCount()) > compactionThreshold)) {
      // Guard: only one compaction thread at a time
      if (!compacting.compareAndSet(false, true))
        return;

      // Start buffering raw deltas for re-application after the swap
      pendingDeltas = new ArrayList<>();

      // See the `generation` field javadoc (issue #6636): captured at dispatch time, deliberately NOT
      // bumped by the synchronous overlay merge just above (or by any applyDelta() call re-entering while
      // this compaction is in flight) - those deltas are buffered and re-applied against this same
      // generation below, cooperatively, not competing with it. A direct build()/buildAsync() dispatched
      // after this compaction started must still win if it finishes first.
      final long myGeneration = ++generation;

      inFlightTasks.incrementAndGet();
      try {
        getExecutor().execute(() -> {
          BUILD_PERMITS.acquireUninterruptibly();
          try {
            final long asOfTransactionId = currentLastTransactionId();
            database.begin();
            try {
              final long buildStart = System.currentTimeMillis();
              final CSRBuilder builder = new CSRBuilder(database, propertyFilter, edgePropertyFilter, propertySampleSize);
              final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);
              final long durationMs = System.currentTimeMillis() - buildStart;

              // Synchronized swap: capture buffered deltas and re-apply against the new mapping
              synchronized (GraphAnalyticalView.this) {
                final List<TxDelta> buffered = pendingDeltas;
                pendingDeltas = null;

                if (buffered == null) {
                  // Buffer was aborted (overflow) — keep the current snapshot with its overlay.
                  // The current overlay is still valid and already has all deltas merged.
                  LogManager.instance().log(this, Level.INFO,
                      "GraphAnalyticalView '%s': compaction result discarded (delta buffer overflowed during rebuild)", name);
                } else if (myGeneration != generation) {
                  // Superseded by a newer build/restore (issue #6636): the buffered deltas were already
                  // merged into the pre-compaction overlay by applyDelta() as they arrived, and that
                  // overlay was discarded together with its base snapshot by whatever won instead, so
                  // there is nothing left here to reapply them onto.
                  LogManager.instance().log(this, Level.INFO,
                      "GraphAnalyticalView '%s': compaction result discarded (superseded by a newer build/restore)", name);
                } else {
                  Snapshot fresh = snapshotFromResult(result, durationMs, asOfTransactionId, vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);

                  // Re-apply any deltas that arrived during the rebuild.
                  // These may not be in the fresh CSR (committed after the CSR scan of their bucket).
                  // Merging against the new mapping resolves RIDs to correct dense IDs.
                  if (!buffered.isEmpty()) {
                    DeltaOverlay overlay = new DeltaOverlay(result.getMapping().size());
                    for (final TxDelta d : buffered) {
                      // Dedup against the fresh base CSR: a buffered delta committed before the scan
                      // crossed its bucket is already in the new base, so re-merging it blindly would
                      // create duplicate neighbours. See issue #4588.
                      overlay = overlay.merge(d, result.getMapping(), result.getCsrPerType());
                    }
                    // A change to a base edge's properties has no overlay representation, so a delta buffered
                    // during this rebuild may not be reflected in the fresh CSR (if it committed after the
                    // relevant bucket was scanned): flag a follow-up rebuild (#4513). Asked of the overlay the
                    // merges arrived at rather than of the deltas that went into them - an update to an edge
                    // the overlay itself holds is applied there and leaves nothing stale, so asking the deltas
                    // would force a rebuild for the ordinary insert, which reports one create and one update
                    // of the same edge (issue #6315).
                    if (overlay.isEdgePropertiesDirty())
                      edgePropRebuildNeeded = true;
                    if (overlay.hasChanges())
                      fresh = fresh.withOverlay(overlay);
                  }

                  this.snapshot = fresh;
                }
              }

            } finally {
              if (database.isTransactionActive())
                database.rollback();
            }
          } catch (final Exception e) {
            synchronized (GraphAnalyticalView.this) {
              pendingDeltas = null;
            }
            if (isBenignShutdownError(e))
              LogManager.instance().log(this, Level.FINE,
                  "Compaction of GraphAnalyticalView '%s' aborted because the database is closing", name);
            else
              LogManager.instance().log(this, Level.WARNING, "Failed to compact GraphAnalyticalView '%s'", e, name);
          } finally {
            BUILD_PERMITS.release();
            compacting.set(false);
            taskCompleted();
            // An edge property update buffered during this rebuild was not reflected in the fresh
            // CSR: schedule a follow-up rebuild now that compaction is released. Converges once
            // edge property updates stop. See issue #4513.
            if (edgePropRebuildNeeded) {
              edgePropRebuildNeeded = false;
              final TxDelta forced = new TxDelta();
              forced.forceEdgePropertyRebuild = true;
              applyDelta(forced);
            }
            // #4956: drop this worker's DatabaseContext entry (see buildAsync). Last statement because
            // applyDelta() above may touch the database on this thread and re-register a context.
            // NOTE: this ordering deliberately DIFFERS from buildAsync/rebuild (which unregister FIRST): here
            // applyDelta(forced) runs synchronously after taskCompleted() and can re-register a context on this
            // same thread, so the unregister must be the true last statement.
            DatabaseContext.INSTANCE.removeCurrentThreadContexts();
          }
        });
      } catch (final RejectedExecutionException e) {
        pendingDeltas = null;
        compacting.set(false);
        taskCompleted();
        LogManager.instance().log(this, Level.WARNING, "GraphAnalyticalView '%s': compaction rejected (executor shut down)", name);
      }
    }
  }

  private boolean isConnectedForType(final Snapshot snap, final int nodeA, final int nodeB,
      final Vertex.DIRECTION direction, final String edgeType) {
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeAInBase = nodeA < snap.nodeMapping.size();

    // Check base CSR. A pair with parallel edges stays connected as long as at least one of them
    // survives, so the base occurrence count is compared against the overlay's exact deleted count
    // for the pair rather than treating any deletion as removing the whole pair (issue #6769).
    if (csr != null && nodeAInBase) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        final int[] neighbors = csr.getForwardNeighbors();
        final int[] offsets = csr.getForwardOffsets();
        final int occurrences = equalRangeCount(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB);
        final int deleted = ov != null ? ov.countDeletedEdges(edgeType, nodeA, nodeB) : 0;
        if (occurrences > deleted)
          return true;
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        final int[] neighbors = csr.getBackwardNeighbors();
        final int[] offsets = csr.getBackwardOffsets();
        final int occurrences = equalRangeCount(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB);
        final int deleted = ov != null ? ov.countDeletedEdges(edgeType, nodeB, nodeA) : 0;
        if (occurrences > deleted)
          return true;
      }
    }

    // Check overlay added edges. No filtering against deletedEdgesPerType is needed - or correct - here:
    // that map is the exclusion budget for the BASE CSR run above and holds only deletions of edges the
    // overlay never added itself, because an edge added and then deleted within the same window is
    // withdrawn from the added index at merge time instead (issue #6775). Anything still listed here is
    // live.
    if (ov != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        for (final int neighbor : ov.getAddedOutNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            return true;
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        for (final int neighbor : ov.getAddedInNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            return true;
      }
    }
    return false;
  }

  /**
   * Counts the edges of one type joining nodeA to nodeB.
   * <p>
   * A self-loop is held by both adjacency lists of its vertex, so a BOTH walk would see it twice; the
   * backward side is skipped when the two nodes are the same, which is what the OLTP expansion
   * achieves by de-duplicating on edge identity.
   * <p>
   * <b>Why a pair with overlay deletions is now counted exactly rather than answered "unknown".</b> The
   * count is the same arithmetic {@link #isConnectedTo} and {@link #getNeighborIds} already run, only
   * kept as a number instead of collapsed to a boolean: the base CSR's occurrence count for the pair
   * minus the overlay's exact per-pair deleted count (issue #6769), plus the overlay's added occurrences.
   * It became safe once {@link DeltaOverlay#merge} stopped recording a pair deletion for an edge the same
   * overlay window had added (issue #6775) - the deleted count is now exclusively a budget against the
   * BASE run, so there is no double-spend between the two terms and no since-deleted add left to exclude
   * from the second. Answering "unknown" here while {@link #getVertices} happily lists the pair as a live
   * neighbour was the very inconsistency #6775 is about, so the two agree instead.
   * <p>
   * The subtraction is clamped at zero rather than trusted to stay non-negative: a budget wider than the
   * base run is exactly what the two documented overlay gaps (#4588's pair-level re-application dedup and
   * #6777's RID reuse) can produce, and a negative contribution would corrupt the sum for the OTHER edge
   * types {@link #countEdgesBetween} adds it to.
   */
  private long countBetweenForType(final Snapshot snap, final int nodeA, final int nodeB,
      final Vertex.DIRECTION direction, final String edgeType) {
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeAInBase = nodeA < snap.nodeMapping.size();
    final boolean selfLoop = nodeA == nodeB;
    final boolean walkForward = direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH;
    final boolean walkBackward = (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
        && !(direction == Vertex.DIRECTION.BOTH && selfLoop);

    long count = 0;

    if (csr != null && nodeAInBase) {
      if (walkForward) {
        final int occurrences = equalRangeCount(csr.getForwardNeighbors(), csr.getForwardOffsets()[nodeA],
            csr.getForwardOffsets()[nodeA + 1], nodeB);
        count += Math.max(0, occurrences - (ov != null ? ov.countDeletedEdges(edgeType, nodeA, nodeB) : 0));
      }
      if (walkBackward) {
        final int occurrences = equalRangeCount(csr.getBackwardNeighbors(), csr.getBackwardOffsets()[nodeA],
            csr.getBackwardOffsets()[nodeA + 1], nodeB);
        count += Math.max(0, occurrences - (ov != null ? ov.countDeletedEdges(edgeType, nodeB, nodeA) : 0));
      }
    }

    if (ov != null) {
      if (walkForward)
        for (final int neighbor : ov.getAddedOutNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            ++count;
      if (walkBackward)
        for (final int neighbor : ov.getAddedInNeighbors(nodeA, edgeType))
          if (neighbor == nodeB)
            ++count;
    }

    return count;
  }

  /**
   * Returns how many times {@code value} occurs in the sorted range {@code [from, to)}.
   */
  private static int equalRangeCount(final int[] sorted, final int from, final int to, final int value) {
    final int hit = Arrays.binarySearch(sorted, from, to, value);
    if (hit < 0)
      return 0;
    int start = hit;
    while (start > from && sorted[start - 1] == value)
      --start;
    int end = hit + 1;
    while (end < to && sorted[end] == value)
      ++end;
    return end - start;
  }

  private int countCommonForType(final Snapshot snap, final int nodeA, final int nodeB,
      final Vertex.DIRECTION direction, final String edgeType) {
    final CSRAdjacencyIndex csr = snap.csrPerType.get(edgeType);
    if (csr == null)
      return 0;
    int count = 0;
    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getForwardNeighbors();
      final int[] offsets = csr.getForwardOffsets();
      count += sortedIntersectionCount(neighbors, offsets[nodeA], offsets[nodeA + 1],
          neighbors, offsets[nodeB], offsets[nodeB + 1]);
    }
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      final int[] neighbors = csr.getBackwardNeighbors();
      final int[] offsets = csr.getBackwardOffsets();
      count += sortedIntersectionCount(neighbors, offsets[nodeA], offsets[nodeA + 1],
          neighbors, offsets[nodeB], offsets[nodeB + 1]);
    }
    return count;
  }

  private long countDirectional(final Snapshot snap, final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    long count = 0;
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeInBase = nodeId < snap.nodeMapping.size();

    if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
      if (nodeInBase)
        count += csr.outDegree(nodeId);
      if (ov != null) {
        count += ov.getAddedOutNeighbors(nodeId, edgeType).length;
        count -= ov.countDeletedOutEdges(nodeId, edgeType);
      }
    }
    if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
      if (nodeInBase)
        count += csr.inDegree(nodeId);
      if (ov != null) {
        count += ov.getAddedInNeighbors(nodeId, edgeType).length;
        count -= ov.countDeletedInEdges(nodeId, edgeType);
      }
    }
    return count;
  }

  private int[] getNeighborsFromCSR(final Snapshot snap, final CSRAdjacencyIndex csr, final int nodeId,
      final Vertex.DIRECTION direction, final String edgeType) {
    final DeltaOverlay ov = snap.overlay;
    final boolean nodeInBase = nodeId < snap.nodeMapping.size();

    // Fast path: no overlay, single direction — return a single copyOfRange (CSR is already sorted)
    if (ov == null && nodeInBase && csr != null && direction != Vertex.DIRECTION.BOTH) {
      if (direction == Vertex.DIRECTION.OUT) {
        final int start = csr.outOffset(nodeId);
        final int end = csr.outOffsetEnd(nodeId);
        return start == end ? EMPTY_INT : Arrays.copyOfRange(csr.getForwardNeighbors(), start, end);
      } else {
        final int start = csr.inOffset(nodeId);
        final int end = csr.inOffsetEnd(nodeId);
        return start == end ? EMPTY_INT : Arrays.copyOfRange(csr.getBackwardNeighbors(), start, end);
      }
    }

    // Fast path: no overlay, BOTH direction — merge two sorted slices without intermediate copies
    if (ov == null && nodeInBase && csr != null) {
      final int outStart = csr.outOffset(nodeId), outEnd = csr.outOffsetEnd(nodeId);
      final int inStart = csr.inOffset(nodeId), inEnd = csr.inOffsetEnd(nodeId);
      final int outLen = outEnd - outStart;
      final int inLen = inEnd - inStart;
      if (outLen == 0 && inLen == 0)
        return EMPTY_INT;
      if (outLen == 0)
        return Arrays.copyOfRange(csr.getBackwardNeighbors(), inStart, inEnd);
      if (inLen == 0)
        return Arrays.copyOfRange(csr.getForwardNeighbors(), outStart, outEnd);
      // Both non-empty: sorted merge into a single result array
      final int[] fwd = csr.getForwardNeighbors();
      final int[] bwd = csr.getBackwardNeighbors();
      final int[] result = new int[outLen + inLen];
      int i = outStart, j = inStart, k = 0;
      while (i < outEnd && j < inEnd)
        result[k++] = fwd[i] <= bwd[j] ? fwd[i++] : bwd[j++];
      while (i < outEnd)
        result[k++] = fwd[i++];
      while (j < inEnd)
        result[k++] = bwd[j++];
      return result;
    }

    // Slow path: overlay is active or node not in base — collect and merge all sources
    int[] baseOut = EMPTY_INT;
    int[] baseIn = EMPTY_INT;
    if (nodeInBase && csr != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH) {
        final int start = csr.outOffset(nodeId), end = csr.outOffsetEnd(nodeId);
        if (start < end)
          baseOut = copyBaseExcludingDeleted(csr.getForwardNeighbors(), start, end, ov, edgeType, nodeId, true);
      }
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH) {
        final int start = csr.inOffset(nodeId), end = csr.inOffsetEnd(nodeId);
        if (start < end)
          baseIn = copyBaseExcludingDeleted(csr.getBackwardNeighbors(), start, end, ov, edgeType, nodeId, false);
      }
    }

    // Unlike baseOut/baseIn above, the overlay-added neighbours below need no exclusion pass: the deleted
    // counts are the budget for the base CSR run and hold only deletions of edges the overlay never added,
    // an edge added and then deleted within the same not-yet-compacted window having been withdrawn from
    // the added index at merge time instead (issue #6775).
    int[] ovOut = EMPTY_INT;
    int[] ovIn = EMPTY_INT;
    if (ov != null) {
      if (direction == Vertex.DIRECTION.OUT || direction == Vertex.DIRECTION.BOTH)
        ovOut = ov.getAddedOutNeighbors(nodeId, edgeType);
      if (direction == Vertex.DIRECTION.IN || direction == Vertex.DIRECTION.BOTH)
        ovIn = ov.getAddedInNeighbors(nodeId, edgeType);
    }

    final int totalLen = baseOut.length + baseIn.length + ovOut.length + ovIn.length;
    if (totalLen == 0)
      return EMPTY_INT;

    final int[] result = new int[totalLen];
    int pos = 0;
    if (baseOut.length > 0) { System.arraycopy(baseOut, 0, result, pos, baseOut.length); pos += baseOut.length; }
    if (baseIn.length > 0) { System.arraycopy(baseIn, 0, result, pos, baseIn.length); pos += baseIn.length; }
    if (ovOut.length > 0) { System.arraycopy(ovOut, 0, result, pos, ovOut.length); pos += ovOut.length; }
    if (ovIn.length > 0) { System.arraycopy(ovIn, 0, result, pos, ovIn.length); pos += ovIn.length; }
    Arrays.sort(result);
    return result;
  }

  /**
   * Copies the base CSR neighbour slice {@code [start, end)} for {@code nodeId}, skipping any edge that the
   * overlay marks as deleted. For an outgoing slice each neighbour {@code n} represents the edge {@code nodeId -> n};
   * for an incoming slice it represents {@code n -> nodeId}. When no relevant deletions exist the original slice is
   * returned verbatim to keep the no-deletion case allocation-cheap.
   * <p>
   * The slice is sorted, so parallel edges to the same neighbour form a contiguous run; {@link DeltaOverlay
   * #countDeletedEdges} gives the number of THOSE parallel edges the overlay recorded as deleted, and only that
   * many slots of the run are excluded - not the whole run - so deleting one of several parallel edges between a
   * pair leaves the others discoverable instead of masking the whole pair (issue #6769).
   * <p>
   * The caller only reaches this helper on the slow path, which is taken precisely when an overlay is present, so
   * {@code ov} is always non-null here (the {@code ov == null} fast paths in {@link #getNeighborsFromCSR} return earlier).
   */
  private static int[] copyBaseExcludingDeleted(final int[] neighbors, final int start, final int end,
      final DeltaOverlay ov, final String edgeType, final int nodeId, final boolean outgoing) {
    final boolean[] deletedMask = deletedSliceMask(neighbors, start, end, ov, edgeType, nodeId, outgoing);
    if (deletedMask == null)
      return Arrays.copyOfRange(neighbors, start, end);

    int kept = 0;
    for (final boolean deleted : deletedMask)
      if (!deleted)
        kept++;
    if (kept == 0)
      return EMPTY_INT;

    final int[] result = new int[kept];
    int pos = 0;
    for (int i = 0; i < deletedMask.length; i++)
      if (!deletedMask[i])
        result[pos++] = neighbors[start + i];
    return result;
  }

  /**
   * True when the overlay deleted some but not all of a run of parallel edges between the same pair, which
   * leaves no way to tell which of that run's column slots the surviving edges hold. See the call site.
   */
  private static boolean isPartialDeletionOfParallelEdges(final int[] neighbors, final int start, final int end,
      final boolean[] deleted) {
    // The slice is sorted, so a pair's parallel edges are one contiguous run.
    final int len = end - start;
    int runStart = 0;
    while (runStart < len) {
      int runEnd = runStart + 1;
      while (runEnd < len && neighbors[start + runEnd] == neighbors[start + runStart])
        runEnd++;
      int deletedInRun = 0;
      for (int i = runStart; i < runEnd; i++)
        if (deleted[i])
          deletedInRun++;
      if (deletedInRun > 0 && deletedInRun < runEnd - runStart)
        return true;
      runStart = runEnd;
    }
    return false;
  }

  /**
   * Marks which slots of the base slice {@code [start, end)} the overlay has deleted, or returns {@code null}
   * when it has deleted none of them.
   * <p>
   * Shared by {@link #copyBaseExcludingDeleted} and {@link #edgeWeightsForSlice} rather than written out twice:
   * both have to reach the same set of surviving edges, and the second one then reads a column slot per
   * survivor - two spellings of this budget would put those weights on the wrong edges the first time they
   * disagreed, which is the failure mode issue #6315 exists to remove.
   * <p>
   * Single pass over the slice. One {@link DeltaOverlay#countDeletedEdges} lookup per distinct neighbour value
   * (cached for the run), not per slot. The mask is allocated lazily, only once the first deleted edge is
   * found, so the common no-deletion case stays allocation-free.
   */
  private static boolean[] deletedSliceMask(final int[] neighbors, final int start, final int end,
      final DeltaOverlay ov, final String edgeType, final int nodeId, final boolean outgoing) {
    final int len = end - start;
    boolean[] deletedMask = null;
    int runValue = 0;
    int runIndex = 0;
    int runDeletedBudget = 0;
    for (int i = 0; i < len; i++) {
      final int n = neighbors[start + i];
      if (i == 0 || n != runValue) {
        runValue = n;
        runIndex = 0;
        runDeletedBudget = outgoing ? ov.countDeletedEdges(edgeType, nodeId, n) : ov.countDeletedEdges(edgeType, n, nodeId);
      }
      final boolean deleted = runIndex < runDeletedBudget;
      runIndex++;
      if (deleted) {
        if (deletedMask == null)
          deletedMask = new boolean[len];
        deletedMask[i] = true;
      }
    }
    return deletedMask;
  }

  /**
   * Checks the view is built and returns a consistent snapshot for the caller to use.
   * All public methods should capture this once and use it throughout their execution.
   * <p>
   * If the view is still a deferred restore-from-disk (see #6632), a real query is the other legitimate
   * trigger for it (alongside an explicit {@link #awaitReady} call): unlike a caller that merely polls
   * {@link #getStatus()}, this method's caller has no OLTP fallback once it is already here, so this
   * blocks (no timeout — same contract {@link #build()} already has for a cold view) until the deferred
   * work resolves one way or another.
   * <p>
   * Gates the wait on {@code deferredRestoreInFlight} rather than on {@code pendingDiskRestore}: the latter
   * is a "has this been dispatched yet" flag that {@link #triggerDeferredDiskRestoreIfPending} clears for
   * every caller the instant the FIRST one dispatches it, not a "is work still in flight" flag. Gating on
   * it directly would let a second, concurrent caller land here after dispatch but before completion, see
   * {@code pendingDiskRestore == false}, and fall straight through to the exception below despite the view
   * being legitimately mid-restore rather than actually broken. Gating on plain {@code status == BUILDING}
   * instead would fix that but over-reach: it would also make this method block, with no timeout, for a
   * brand-new view whose {@link #buildAsync()} was called directly and hasn't completed yet — a real,
   * unrelated (and, per that method's own javadoc, documented as non-blocking) scenario this fix has no
   * business changing. {@code deferredRestoreInFlight} stays true for the exact duration of the work THIS
   * mechanism started (including a {@link #buildAsync()} fallback if the restore itself turns out unusable —
   * see {@link #dispatchDeferredRestore()}), so it correctly waits for that and only that.
   */
  private Snapshot checkBuilt() {
    Snapshot snap = this.snapshot;
    if (snap == null) {
      triggerDeferredDiskRestoreIfPending(); // no-op if not applicable, or another thread already dispatched it
      if (deferredRestoreInFlight)
        awaitDeferredDiskRestoreSettled();
      snap = this.snapshot;
    }
    if (snap == null)
      throw new IllegalStateException("GraphAnalyticalView has not been built yet. Call build() first.");
    return snap;
  }

  /**
   * Test-only hook: saturates the shared, JVM-wide {@link #BUILD_PERMITS} semaphore so any subsequently
   * dispatched async build/restore/compaction task blocks right before it starts its scan or disk read,
   * letting a test deterministically land a direct {@link #build()}/{@link #buildAsync()} call while that
   * task is in flight but stalled. Must be paired with {@link #releaseAllBuildPermitsForTest()} in a
   * {@code finally} block: forkCount=1 runs this module's tests in a single, sequential JVM, but a leaked
   * acquisition would still starve every later test in that JVM of build permits.
   */
  static void acquireAllBuildPermitsForTest() {
    BUILD_PERMITS.acquireUninterruptibly(MAX_CONCURRENT_BUILDS);
  }

  /** Test-only hook: releases the permits taken by {@link #acquireAllBuildPermitsForTest()}. */
  static void releaseAllBuildPermitsForTest() {
    BUILD_PERMITS.release(MAX_CONCURRENT_BUILDS);
  }

  /** Test-only hook: exposes {@link #deferredRestoreInFlight} so a test can poll for the dispatch. */
  boolean isDeferredRestoreInFlightForTest() {
    return deferredRestoreInFlight;
  }

  private static int sortedIntersectionCount(final int[] a, int startA, final int endA,
      final int[] b, int startB, final int endB) {
    int count = 0;
    while (startA < endA && startB < endB) {
      final int va = a[startA];
      final int vb = b[startB];
      if (va == vb) {
        count++;
        startA++;
        startB++;
      } else if (va < vb)
        startA++;
      else
        startB++;
    }
    return count;
  }
}
