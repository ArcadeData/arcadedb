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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockContext;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.function.BiFunction;

/**
 * Manages pages from disk to RAM. Each page can have different size.
 * <p>
 * <b>LOCK ORDER</b> for everything the point-in-time snapshot machinery of issue #6075 touches. It is stated here
 * rather than spread across the methods because no single call site shows more than two of these locks, so a change
 * to {@link #openSnapshot}, {@link FileManager#dropFile} or {@link FileManager#getOrCreateFile} could reintroduce a
 * cycle with no local signal that it had:
 * <ol>
 * <li>the per-database snapshot BARRIER monitor ({@code snapshotBarrierLocks}), taken first and only by
 * {@link #openSnapshot};</li>
 * <li>{@link TransactionManager#getApplyLock()};</li>
 * <li>this manager's own {@link com.arcadedb.utility.LockContext} lock;</li>
 * <li>the {@link FileManager} monitor (through {@link FileManager#executeWithFileSetLocked});</li>
 * <li>{@code snapshotRegistryLock}, always last.</li>
 * </ol>
 * {@link #openSnapshot} walks 1 to 5 in that order; {@link FileManager#dropFile} takes 4 then 5, which agrees. No
 * path takes the FileManager monitor and then this manager's lock, and none takes the registry lock and then
 * anything else - {@code PageSnapshot.close()} unregisters (5) and only then drops its retained files, holding
 * nothing.
 */
public class PageManager extends LockContext {
  public static final PageManager INSTANCE = new PageManager();

  // Package-private (instead of private) so the white-box regression test for #4925/#4933 can assert on
  // the cache content and RAM accounting directly, without reflection.
  volatile ConcurrentMap<PageId, CachedPage> readCache;
  // MANAGE CONCURRENT ACCESS TO THE PAGES. THE VALUE IS TRUE FOR WRITE OPERATION AND FALSE FOR READ
  private final    ConcurrentMap<PageId, Boolean>    pendingFlushPages                     = new ConcurrentHashMap<>();
  private volatile long                               maxRAM;
  final            AtomicLong                        totalReadCacheRAM                     = new AtomicLong();
  // #5636: the counters below, down to totalMergesDeclinedByCoverage, are exported as Prometheus COUNTERS (see
  // EngineMetricsBinder), which requires them never to decrease for the lifetime of the JVM. They are deliberately
  // reset nowhere - note close() resets only totalReadCacheRAM above, which is an instantaneous gauge. Adding a
  // "reset stats" affordance that touched them would make each reset read as a counter reset and fabricate a rate()
  // spike, which is the artifact #5636 exists to remove.
  private final    AtomicLong                        totalPagesRead                        = new AtomicLong();
  private final    AtomicLong                        totalPagesReadSize                    = new AtomicLong();
  private final    AtomicLong                        totalPagesWritten                     = new AtomicLong();
  private final    AtomicLong                        totalPagesWrittenSize                 = new AtomicLong();
  private final    AtomicLong                        cacheHits                             = new AtomicLong();
  private final    AtomicLong                        cacheMiss                             = new AtomicLong();
  private final    AtomicLong                        totalConcurrentModificationExceptions = new AtomicLong();
  private final    AtomicLong                        totalEdgeAppendMerges                 = new AtomicLong();
  private final    AtomicLong                        totalTxPageSlotMerges                 = new AtomicLong();
  private final    AtomicLong                        totalMergesDeclinedByCoverage         = new AtomicLong();
  private final    AtomicLong                        evictionRuns                          = new AtomicLong();
  private final    AtomicLong                        pagesEvicted                          = new AtomicLong();
  // #6116: the three snapshot totals below are exported as Prometheus counters together with the ones above, and
  // carry the same never-decrease requirement. They are JVM-wide like the rest of this class, so a database close
  // does not step them back.
  private final    AtomicLong                        totalSnapshotWindowsOpened            = new AtomicLong();
  private final    AtomicLong                        totalSnapshotWindowsInvalidated       = new AtomicLong();
  private final    AtomicLong                        totalSnapshotPreImagesCaptured        = new AtomicLong();
  private volatile long                              lastCheckForRAM                       = 0;
  // LIFECYCLE INVARIANT (#5070): flushThread and readCache are written only under LIFECYCLE_LOCK
  // during the 0->1 startup / 1->0 shutdown transitions, and read lock-free on the hot paths. That is safe
  // ONLY because a reader implies refcount > 0 (its database holds a reference), so no transition can run
  // concurrently. volatile: the barrier cost is negligible next to the
  // ConcurrentHashMap barriers already on these paths, and it removes the reliance on the external
  // database-publication happens-before for cross-thread visibility of the startup() writes.
  private volatile PageManagerFlushThread             flushThread;
  private volatile int                                freePageRAM;

  /**
   * #6075: the point-in-time snapshot windows currently open, {@code null} when there is none anywhere in the JVM -
   * which is the steady state. The write path therefore pays ONE volatile field read plus a branch the predictor
   * always gets right, inside a critical section that already performs a page-size {@code pwrite}: roughly 1 ns
   * against 10-50 us.
   * <p>
   * It is deliberately ONE nullable field and NOT a listener list: a {@code List<PageWriteListener>} iterated per
   * write costs real cycles and can go megamorphic, while a single nullable field inlines away. It is also a single
   * read yielding both the flag and the state, so there is no double-read race between "is a window open" and
   * "which window". Copy-on-write on registration, which only happens when a window opens or closes.
   */
  private volatile PageSnapshot[] activeSnapshots = null;
  private final    Object         snapshotRegistryLock = new Object();
  /** Serializes the t0 barrier per database (NOT the windows themselves, which may overlap freely). */
  private final ConcurrentHashMap<Database, Object>              snapshotBarrierLocks = new ConcurrentHashMap<>();
  /**
   * Challenge C2: files dropped while a snapshot window is open are not deleted, so index compaction runs freely
   * during a backup instead of being postponed. The count is how many windows still need the file; the last one to
   * close performs the physical delete.
   */
  private final ConcurrentHashMap<ComponentFile, AtomicInteger> deferredFileDrops    = new ConcurrentHashMap<>();
  private final AtomicLong                                      snapshotCounter      = new AtomicLong();
  /**
   * Reused per-thread scratch for the page pre-image. Sized to the largest page seen by this thread, so a capture
   * allocates nothing after the first one; {@link PageShadow#store} copies out of it, so it can be recycled at once.
   */
  private static final ThreadLocal<byte[]> PRE_IMAGE_BUFFER = ThreadLocal.withInitial(() -> new byte[0]);

  /** How many times the t0 barrier retries when commits slip in between the full drain and the suspension. */
  private static final int SNAPSHOT_BARRIER_ATTEMPTS = 3;

  @ExcludeFromJacocoGeneratedReport
  public interface ConcurrentPageAccessCallback {
    void access() throws IOException;
  }

  public static class PPageManagerStats {
    public long maxRAM;
    public long readCacheRAM;
    public long pagesRead;
    public long pagesReadSize;
    public long pagesWritten;
    public long pagesWrittenSize;
    public int  pageFlushQueueLength;
    public long cacheHits;
    public long cacheMiss;
    public long concurrentModificationExceptions;
    public long edgeAppendMerges;
    public long txPageSlotMerges;
    /** See {@link PageManager#incrementMergesDeclinedByCoverage()}: commit ATTEMPTS failed on a coverage decline, not pages. */
    public long mergesDeclinedByCoverage;
    public long evictionRuns;
    public long pagesEvicted;
    public int  readCachePages;
    /**
     * #6116: the point-in-time snapshot windows (#6075) as an operator sees them. The five instantaneous readings go
     * up AND down with the lifetime of a window, so they are gauges; the three totals below them are counters. A
     * window whose shadow approaches {@code arcadedb.pageSnapshotMaxSize} is about to be invalidated and force its
     * backup onto the writer-throttling fallback path, which is precisely the event an operator must be able to see
     * coming rather than read about afterwards in the log.
     */
    public int    snapshotWindowsOpen;
    public long   snapshotShadowedPages;
    public long   snapshotShadowBytes;
    public long   snapshotShadowSpilledBytes;
    /** How full the fullest open window's shadow is, as a percentage of {@code arcadedb.pageSnapshotMaxSize}. */
    public double snapshotShadowUsagePerc;
    /** Age of the OLDEST open window, in milliseconds: a window that never closes is a leak, not a slow backup. */
    public long   snapshotOldestWindowMillis;
    public long   snapshotWindowsOpened;
    public long   snapshotWindowsInvalidated;
    public long   snapshotPreImagesCaptured;
    /** See {@link PageManager#getDeferredRAMBytes()} (#6087): dirty pages held in RAM by a flush suspension. */
    public long   deferredRAMBytes;
  }

  private PageManager() {
  }

  /**
   * #4927: the JVM-wide PageManager is REFCOUNTED. Every DatabaseFactory open/create acquires one reference
   * (starting the flush machinery on 0 -> 1) and every database close releases it (tearing down on 1 -> 0),
   * all under one global lock. The previous lifecycle keyed on ACTIVE_INSTANCES.isEmpty() was a
   * check-then-act racing across factory instances (open/create synchronize on the factory, the map is
   * static, and registration happens only AFTER database.open() completes): closing the last instance of
   * one database could null the flush thread under a database whose open was mid-flight (NPE on the first
   * cache miss, or scheduled pages never flushed), and two opens could both see the map empty and start two
   * flush threads, leaking one with queued pages.
   */
  private static final Object LIFECYCLE_LOCK = new Object();
  private       int    lifecycleRefCount = 0; // guarded by LIFECYCLE_LOCK

  /** Acquires one lifecycle reference; the flush machinery starts on the 0 -> 1 transition. Pair with {@link #release()}. */
  public void acquire() {
    synchronized (LIFECYCLE_LOCK) {
      if (++lifecycleRefCount == 1)
        try {
          startup();
        } catch (final RuntimeException | Error e) {
          // #5070: startup() can throw (e.g. ConfigurationException on a negative MAX_PAGE_RAM). The
          // increment must not survive it, or the counter is left at 1 with no flush thread and the NEXT
          // acquire sees 1 -> 2 and never starts one - a wedged manager the caller's catch cannot repair
          // (its release() would just decrement back to the same broken state at 1).
          lifecycleRefCount = 0;
          throw e;
        }
    }
  }

  /** Releases one lifecycle reference; the flush machinery is torn down on the 1 -> 0 transition. Over-releases are clamped. */
  public void release() {
    synchronized (LIFECYCLE_LOCK) {
      if (lifecycleRefCount == 0)
        // Over-release (e.g. a test calling close() directly followed by a paired release): clamp instead of
        // going negative and tearing down a manager a later acquire believes it owns.
        return;
      if (--lifecycleRefCount == 0)
        shutdown();
    }
  }

  /**
   * Declares that the configuration changed (used by the GlobalConfiguration PROFILE setter). Always a
   * no-op at runtime: with no database open the next {@link #acquire()} reads the fresh settings anyway
   * (and starting a flush thread with zero databases - the old behavior - leaked it until the next
   * lifecycle transition); with databases OPEN a live shutdown+startup swap would race the hot paths, which
   * read {@code flushThread}/{@code readCache} without the lifecycle lock (#5070) - so a live
   * profile change is refused loudly instead. Set the PROFILE before opening databases.
   */
  public void configure() {
    synchronized (LIFECYCLE_LOCK) {
      if (lifecycleRefCount > 0)
        LogManager.instance().log(this, Level.WARNING,
            "Configuration profile PARTIALLY applied: %d database(s) are open, so the page manager keeps its current sizing while the profile's other settings (async/HTTP threads, queue sizes) did change. Set the profile before opening databases for a consistent configuration",
            null, lifecycleRefCount);
    }
  }

  /**
   * Force teardown regardless of refcount (test/emergency API): the counter resets so the next acquire
   * starts fresh. ONLY safe when no database is open: a live database's eventual release() would decrement
   * the NEW manager started by a later acquire and tear it down under that newcomer (#5070).
   */
  public void close() {
    synchronized (LIFECYCLE_LOCK) {
      lifecycleRefCount = 0;
      shutdown();
    }
  }

  private void startup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    this.freePageRAM = configuration.getValueAsInteger(GlobalConfiguration.FREE_PAGE_RAM);
    this.readCache = new ConcurrentHashMap<>(configuration.getValueAsInteger(GlobalConfiguration.INITIAL_PAGE_CACHE_SIZE));

    this.maxRAM = configuration.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM) * 1024 * 1024;
    if (this.maxRAM < 0)
      throw new ConfigurationException(
          GlobalConfiguration.MAX_PAGE_RAM.getKey() + " configuration is invalid (" + (maxRAM / (1024 * 1024)) + " MB)");

    flushThread = new PageManagerFlushThread(this, configuration);
    flushThread.start();
  }

  /**
   * INVARIANT (#5070): runs under LIFECYCLE_LOCK and joins the flush thread - the flush thread must
   * therefore NEVER call acquire()/release()/configure()/close(), or this join becomes a deadlock (the
   * flush thread blocking on the lock this thread holds while waiting for it to exit). Verified true today.
   */
  private void shutdown() {
    // #6075: a snapshot window outliving the page manager would keep its shadow file and any file whose deletion it
    // deferred alive forever. Consumers always close in a finally, so this only fires on an abnormal teardown - which
    // is exactly the situation where a window could be mid-registration, so the read takes the registry lock rather
    // than assuming the quiet teardown it is here to survive.
    final PageSnapshot[] leftovers;
    synchronized (snapshotRegistryLock) {
      leftovers = activeSnapshots;
    }
    if (leftovers != null) {
      LogManager.instance().log(this, Level.WARNING, "Closing %d snapshot window(s) still open at page manager shutdown",
          null, leftovers.length);
      for (final PageSnapshot snapshot : leftovers)
        CodeUtils.executeIgnoringExceptions(snapshot::close, "Error on closing a leftover snapshot window", false);
    }

    if (flushThread != null) {
      try {
        flushThread.closeAndJoin();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        // Null out regardless of interrupt (#5070): a stale dead reference with refcount 0 was
        // harmless (the next startup() overwrites it) but inconsistent.
        flushThread = null;
      }
    }

    if (readCache != null)
      // close() is a reachable test/emergency API and may run before any startup() (#5070).
      readCache.clear();
    totalReadCacheRAM.set(0L);
  }

  public void removeAllReadPagesOfDatabase(final Database database) {
    for (final CachedPage p : readCache.values()) {
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database))
        // #4933: drive the accounting from the value ACTUALLY removed. Subtracting before it.remove()
        // double-subtracted a page that a concurrent evictOldestPages/removePageFromCache removed first,
        // driving totalReadCacheRAM negative and permanently disabling eviction (unbounded cache growth).
        removePageFromCache(pageId);
    }
  }

  /** @return true when everything reached the disk; false when the bounded wait gave up (see #4928). */
  public boolean waitAllPagesOfDatabaseAreFlushed(final Database database) {
    if (flushThread != null)
      return flushThread.waitAllPagesOfDatabaseAreFlushed(database);
    return true;
  }

  public void removeModifiedPagesOfDatabase(final Database database) {
    if (flushThread != null)
      flushThread.removeAllPagesOfDatabase(database);
    // Forget the snapshot barrier monitor too, so a closed/dropped Database instance (and everything it pins) can be
    // garbage collected instead of being retained as a map key for the lifetime of the JVM-wide page manager.
    snapshotBarrierLocks.remove(database);
  }

  public void suspendFlushAndExecute(final Database database, final CallableNoReturn callback)
      throws IOException, InterruptedException {
    // #5068: the suspension is REFCOUNTED, so every caller (backup, verify, HA snapshot serving, nested
    // scopes per #4958) owns its whole window even when the windows overlap on the same database: flushing
    // is resumed (and the deferred batches flushed) only when the LAST suspender exits. The wait for the
    // in-flight batch runs INSIDE the try so an interrupt during the wait still releases this caller's
    // reference; it is cheap for non-first suspenders (the flush thread is already parked deferring).
    flushThread.setSuspended(database, true);
    try {
      flushThread.waitForCurrentFlushToComplete(database);
      CodeUtils.executeIgnoringExceptions(callback, "Error during suspend flush", true);
    } finally {
      flushThread.setSuspended(database, false);
    }
  }

  public boolean isPageFlushingSuspended(final Database database) {
    return flushThread.isSuspended(database);
  }

  // --------------------------------------------------------------------------------- POINT-IN-TIME SNAPSHOT (#6075)

  /**
   * Opens a point-in-time snapshot of every page file of a database: the primitive that replaces
   * {@link #suspendFlushAndExecute} for readers that need the data files to stand still (full backup, HA snapshot
   * ship, HA database verify). Unlike the suspension, which throttles writers for its whole duration and postpones
   * index compaction with it, the only stall is the bounded barrier below; after that writers run at full speed and
   * pay one page read plus one shadow write for the FIRST write to each page that existed at t0.
   * <p>
   * <b>The barrier is the correctness of the whole design, and its ORDER is what makes it correct.</b> Flipping the
   * flag is not the same as opening the window: {@code concurrentPageAccess} grants the per-page write slot with a
   * {@code putIfAbsent}, so at the instant of the flip a writer can already be inside its write section having read
   * {@code activeSnapshots == null}. It would overwrite its page without shadowing it, and the snapshot would hold
   * that page as of t0 plus epsilon, torn against everything else. So every writer is excluded BEFORE t0 is stamped,
   * each by the mechanism that already serializes it:
   * <ol>
   * <li>{@link #waitAllPagesOfDatabaseAreFlushed} drains the flush queue COMPLETELY - not just the in-flight batch
   * the way {@code waitForCurrentFlushToComplete} does - so every committed transaction is materialised. This is
   * what makes t0 a genuine, recent transaction boundary and closes the recency gap the suspension left (a restored
   * backup could be a few hundred transactions behind backup start).</li>
   * <li>The flush thread is then suspended and its in-flight batch awaited, so the asynchronous path is parked on a
   * BATCH boundary. That granularity matters: a batch is one transaction's pages, so a snapshot taken between
   * batches can never hold half a transaction. If commits slipped in between the drain and the suspension, the
   * suspension is released (which flushes what it deferred) and the barrier retries.</li>
   * <li>The transaction manager's apply lock is taken exclusively, which excludes Raft and crash-recovery replay -
   * the one writer that goes to the files outside the flush pipeline ({@code writePageWithLock}). Applies are
   * serialised on the state machine thread, so this is a single bounded wait.</li>
   * <li>The global page-manager lock is taken, which excludes synchronous commits: they publish their pages from
   * {@link #publishPages}, which holds it for the whole write.</li>
   * </ol>
   * Only then are the per-file page counts and {@code lastTxId} recorded and the window published. Doing these in
   * the other order produces subtly torn snapshots that pass tests and fail in production.
   * <p>
   * The tempting alternative - a shared read lock on the write path so the flip can take the exclusive side - is
   * correct but adds a permanent cost to the hot path that no flag can turn off. Do not.
   * <p>
   * Windows may overlap freely, on the same database or on different ones: each owns its own shadow and a write
   * captures the same freshly read pre-image into every window that still needs it (challenge C3). Only the barrier
   * is serialized per database, and only for its own duration.
   *
   * <b>Every failure surfaces as {@link PageSnapshotException}</b>, including an I/O error while enumerating the
   * files and an interrupt during the barrier. That is deliberate: a consumer's fallback to the suspend-and-freeze
   * path is only genuinely unconditional if there is ONE exception type to catch - and these callers run inside
   * {@code executeInReadLock}, which wraps a checked exception into something a {@code catch (PageSnapshotException)}
   * would never match, so a leaked {@code IOException} silently disables the fallback.
   *
   * @return a handle the caller MUST close, ideally in a try-with-resources: the shadow and any file whose deletion
   *     was deferred for it are released there.
   */
  public PageSnapshot openSnapshot(final DatabaseInternal database) {
    try {
      return openSnapshotInternal(database);
    } catch (final PageSnapshotException e) {
      throw e;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PageSnapshotException("Interrupted while opening a snapshot of database '" + database.getName() + "'", e);
    } catch (final Exception e) {
      throw new PageSnapshotException("Cannot open a snapshot of database '" + database.getName() + "'", e);
    }
  }

  private PageSnapshot openSnapshotInternal(final DatabaseInternal database) throws IOException, InterruptedException {
    final PageManagerFlushThread thread = flushThread;
    if (thread == null)
      throw new PageSnapshotException(
          "Cannot open a snapshot of database '" + database.getName() + "': the page manager is not running");

    synchronized (snapshotBarrierLock(database)) {
      boolean suspended = false;
      try {
        for (int attempt = 1; ; ++attempt) {
          if (!waitAllPagesOfDatabaseAreFlushed(database))
            LogManager.instance().log(this, Level.WARNING,
                "Snapshot of database '%s': the flush queue did not drain within the timeout, the snapshot point may be behind the last committed transaction",
                null, database.getName());

          thread.setSuspended(database, true);
          suspended = true;
          thread.waitForCurrentFlushToComplete(database);

          if (!thread.hasPendingPagesOfDatabase(database))
            // CLEAN: THE QUEUE DRAINED AND STAYED DRAINED ACROSS THE SUSPENSION, SO t0 IS A GENUINE, CURRENT
            // TRANSACTION BOUNDARY
            break;

          if (attempt >= SNAPSHOT_BARRIER_ATTEMPTS) {
            // GAVE UP: SUSTAINED WRITE PRESSURE KEPT LANDING COMMITS BETWEEN THE DRAIN AND THE SUSPENSION. THE
            // SNAPSHOT IS STILL CONSISTENT - IT IS TAKEN ON A BATCH BOUNDARY EITHER WAY - JUST AS SLIGHTLY BEHIND
            // THE LAST COMMIT AS THE SUSPEND-AND-FREEZE PATH ALWAYS WAS. LOGGED BECAUSE "t0 IS A REAL TRANSACTION
            // BOUNDARY" IS ONE OF THIS DESIGN'S CLAIMS, AND THE DEGRADED CASE MUST BE OBSERVABLE RATHER THAN ONLY
            // INFERABLE FROM BEHAVIOUR
            LogManager.instance().log(this, Level.WARNING,
                "Snapshot of database '%s': the flush pipeline still had pending pages after %d barrier attempts, so the snapshot point may be slightly behind the last committed transaction. Sustained write pressure, not an error",
                null, database.getName(), attempt);
            break;
          }

          // COMMITS LANDED BETWEEN THE DRAIN AND THE SUSPENSION. RELEASING THE SUSPENSION FLUSHES WHAT IT DEFERRED,
          // SO THE RETRY STARTS FROM A CLEANER PIPELINE
          thread.setSuspended(database, false);
          suspended = false;
        }

        final ReentrantReadWriteLock applyLock = database.getTransactionManager().getApplyLock();
        applyLock.writeLock().lock();
        try {
          lock();
          try {
            // LIST THE FILES AND PUBLISH THE WINDOW AS ONE ATOMIC STEP AGAINST FileManager.dropFile. Without this a
            // file dropped in the gap - which index compaction can do at any moment, taking no database lock - would
            // be physically deleted, because deferFileDrop only protects windows that are already published, and the
            // snapshot would then be holding a closed channel
            return database.getFileManager()
                .executeWithFileSetLocked(() -> registerSnapshot(buildSnapshot(database)));
          } finally {
            unlock();
          }
        } finally {
          applyLock.writeLock().unlock();
        }
      } finally {
        if (suspended)
          thread.setSuspended(database, false);
      }
    }
  }

  /**
   * True while at least one point-in-time snapshot window is open on the database: the public counterpart of
   * {@link #isPageFlushingSuspended}, for embedders, diagnostics and tests that need to observe a window's lifetime.
   * <p>
   * Deliberately NOT consulted by the compaction guards. {@code LSMTreeIndex} and {@code LSMVectorIndex} still gate
   * on {@code isPageFlushingSuspended}, which the snapshot path never sets - so compaction runs during a snapshot
   * by construction rather than by a second check that could drift out of step with the first.
   */
  public boolean isSnapshotWindowOpen(final Database database) {
    final PageSnapshot[] snapshots = activeSnapshots;
    if (snapshots == null)
      return false;
    for (final PageSnapshot snapshot : snapshots)
      if (snapshot.isFor(database))
        return true;
    return false;
  }

  /**
   * Deletion of a file dropped while a snapshot window needs it is DEFERRED to the close of the last such window
   * (challenge C2). Called by {@link FileManager#dropFile}.
   * <p>
   * The alternative - postponing compaction for the duration of the window, which is what the
   * {@code isPageFlushingSuspended} guard does today - would keep one of the costs this issue set out to remove: a
   * long backup silently stops LSM and vector index compaction. Deferring the delete instead lets compaction run
   * freely, at the cost of the dropped file's disk space until the window closes.
   *
   * @return {@code true} when the physical delete has been taken over, so the caller must NOT delete the file.
   */
  public boolean deferFileDrop(final Database database, final ComponentFile file) {
    if (activeSnapshots == null)
      return false;

    // UNDER THE REGISTRY LOCK: A WINDOW UNREGISTERS ITSELF (AND ONLY THEN DRAINS ITS RETAINED FILES) UNDER THE SAME
    // LOCK, SO A FILE HANDED OVER HERE IS ALWAYS EITHER TAKEN BY A LIVE WINDOW OR NOT HANDED OVER AT ALL
    synchronized (snapshotRegistryLock) {
      final PageSnapshot[] snapshots = activeSnapshots;
      if (snapshots == null)
        return false;

      int owners = 0;
      for (final PageSnapshot snapshot : snapshots)
        if (snapshot.isFor(database) && snapshot.getFile(file.getFileId()) != null)
          ++owners;

      if (owners == 0)
        return false;

      deferredFileDrops.put(file, new AtomicInteger(owners));
      for (final PageSnapshot snapshot : snapshots)
        if (snapshot.isFor(database) && snapshot.getFile(file.getFileId()) != null)
          snapshot.retainDroppedFile(file);

      LogManager.instance().log(this, Level.FINE,
          "Deferring the deletion of file '%s' until %d snapshot window(s) of database '%s' close", null, file.getFileName(),
          owners, database.getName());
      return true;
    }
  }

  /** Releases one window's claim on a deferred file deletion; the last one performs it. */
  void releaseDeferredFileDrop(final ComponentFile file) {
    final AtomicInteger owners = deferredFileDrops.get(file);
    if (owners == null)
      return;
    if (owners.decrementAndGet() <= 0) {
      deferredFileDrops.remove(file);
      try {
        file.drop();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error on deleting file '%s', whose deletion was deferred while a snapshot window was open", e, file.getFileName());
      }
    }
  }

  private PageSnapshot buildSnapshot(final DatabaseInternal database) throws IOException {
    final List<PageSnapshot.SnapshotFile> files = new ArrayList<>();
    for (final ComponentFile file : database.getFileManager().getFiles()) {
      // A null slot is a dropped file id and a not-yet-opened one is the reserved slot of a file id that has been
      // allocated but whose file does not exist yet: neither has content at t0, so both are correctly absent. Any
      // OTHER unusable file would silently shrink the archive while the backup still reported success, which is the
      // worst failure mode a backup has - so it is named in the log rather than dropped quietly.
      if (file == null)
        continue;
      if (!(file instanceof PaginatedComponentFile paginated) || !paginated.isOpen() || paginated.getPageSize() <= 0) {
        LogManager.instance().log(this, Level.FINE,
            "Snapshot of database '%s' skips file '%s' (id=%d): it is not an open paginated file at t0", null,
            database.getName(), file.getFileName(), file.getFileId());
        continue;
      }

      // getTotalPages() is a channel.size() syscall, and unlike SnapshotFile.lastModified() it CANNOT be made lazy:
      // the page count at t0 is what defines which pages the snapshot covers, and reading it later would let a page
      // appended after t0 into the snapshot un-shadowed. An fstat per file is orders of magnitude cheaper than the
      // flush-queue drain this same barrier already performed.
      files.add(new PageSnapshot.SnapshotFile(paginated.getFileId(), paginated, paginated.getPageSize(),
          (int) paginated.getTotalPages(), paginated.getFileName()));
    }

    final ContextConfiguration configuration = database.getConfiguration();
    final long maxRAM = configuration.getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM) * 1024 * 1024;
    final long maxSize = configuration.getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE) * 1024 * 1024;
    final File spillFile = new File(database.getDatabasePath(),
        "snapshot-" + snapshotCounter.incrementAndGet() + "." + PageSnapshot.SHADOW_FILE_EXT);

    return new PageSnapshot(database, this, database.getTransactionManager().getLastTransactionId(), files,
        new PageShadow(spillFile, maxRAM, maxSize));
  }

  private PageSnapshot registerSnapshot(final PageSnapshot snapshot) {
    synchronized (snapshotRegistryLock) {
      final PageSnapshot[] current = activeSnapshots;
      final PageSnapshot[] updated = current == null ? new PageSnapshot[1] : Arrays.copyOf(current, current.length + 1);
      updated[updated.length - 1] = snapshot;
      activeSnapshots = updated;
    }
    totalSnapshotWindowsOpened.incrementAndGet();
    return snapshot;
  }

  void unregisterSnapshot(final PageSnapshot snapshot) {
    synchronized (snapshotRegistryLock) {
      final PageSnapshot[] current = activeSnapshots;
      if (current == null)
        return;
      int found = -1;
      for (int i = 0; i < current.length; i++)
        if (current[i] == snapshot) {
          found = i;
          break;
        }
      if (found < 0)
        return;
      if (current.length == 1) {
        activeSnapshots = null;
        return;
      }
      final PageSnapshot[] updated = new PageSnapshot[current.length - 1];
      System.arraycopy(current, 0, updated, 0, found);
      System.arraycopy(current, found + 1, updated, found, current.length - found - 1);
      activeSnapshots = updated;
    }
  }

  private Object snapshotBarrierLock(final Database database) {
    return snapshotBarrierLocks.computeIfAbsent(database, k -> new Object());
  }

  /**
   * Copies the current on-disk image of a page into every open window that still needs it, from inside the write
   * slot the caller already holds. The image is read ONCE and shared: a window that has not shadowed the page yet
   * sees exactly the same content at t0, because any intervening write would have populated it.
   * <p>
   * A failure here never propagates to the writer - a snapshot must never be able to break the live database - it
   * invalidates the window instead, and every later read through it fails loudly.
   */
  private void capturePreImages(final PageSnapshot[] snapshots, final PageId pageId) {
    final BasicDatabase database = pageId.getDatabase();
    final int fileId = pageId.getFileId();
    final int pageNumber = pageId.getPageNumber();

    byte[] preImage = null;
    int pageSize = 0;

    for (final PageSnapshot snapshot : snapshots) {
      if (!snapshot.isFor(database) || !snapshot.needsPreImage(fileId, pageNumber))
        continue;

      if (preImage == null) {
        final PageSnapshot.SnapshotFile snapshotFile = snapshot.getFile(fileId);
        pageSize = snapshotFile.pageSize();
        byte[] buffer = PRE_IMAGE_BUFFER.get();
        if (buffer.length < pageSize) {
          buffer = new byte[pageSize];
          PRE_IMAGE_BUFFER.set(buffer);
        }

        try {
          snapshotFile.file().readPages(pageNumber, 1, ByteBuffer.wrap(buffer, 0, pageSize));
        } catch (final IOException e) {
          // THE FILE, NOT ONE WINDOW, IS WHAT FAILED: EVERY WINDOW THAT STILL NEEDED THIS PAGE LOSES ITS t0 IMAGE
          for (final PageSnapshot other : snapshots)
            if (other.isFor(database) && other.needsPreImage(fileId, pageNumber))
              other.invalidateOnCaptureError(fileId, pageNumber, e);
          return;
        }
        preImage = buffer;
      }

      snapshot.storePreImage(fileId, pageNumber, preImage, pageSize);
      // #6116: THE COPY-ON-WRITE WORK A WINDOW IS COSTING, AS A RATE. ONE ATOMIC INCREMENT NEXT TO A PAGE READ AND A
      // PAGE COPY, AND ONLY WHEN A WINDOW IS OPEN AND STILL NEEDS THIS PAGE
      totalSnapshotPreImagesCaptured.incrementAndGet();
    }
  }

  /**
   * Bytes of dirty pages currently held in memory because flushing is suspended on some database (a full backup, an HA
   * snapshot ship, an HA verify). Once this crosses {@link com.arcadedb.GlobalConfiguration#FLUSH_SUSPEND_MAX_DEFERRED_RAM}
   * the flush thread stops draining its queue and committing threads are throttled instead, so this is the direct
   * measure of how much a long suspension is costing writers.
   *
   * @return 0 when no suspension is in progress, or when the page manager is closed.
   */
  public long getDeferredRAMBytes() {
    final PageManagerFlushThread thread = flushThread;
    return thread != null ? thread.deferredRAMBytes.get() : 0L;
  }

  /**
   * Test only API.
   */
  public void simulateKillOfDatabase(final Database database) {
    removeAllReadPagesOfDatabase(database);
    if (flushThread != null)
      flushThread.removeAllPagesOfDatabase(database);
  }

  public void deleteFile(final Database database, final int fileId) {
    // Drain the async flush thread for this fileId first, otherwise its parked pages leak RAM and could be flushed to a dropped file.
    if (flushThread != null)
      flushThread.removeAllPagesOfFile(database, fileId);

    for (final CachedPage p : readCache.values()) {
      final PageId pageId = p.getPageId();
      if (pageId.getDatabase().equals(database) && pageId.getFileId() == fileId)
        // #4933: accounting driven by the value actually removed (see removeAllReadPagesOfDatabase).
        removePageFromCache(pageId);
    }
  }

  PageManagerFlushThread getFlushThread() {
    return flushThread;
  }

  private int getMostRecentVersionOfPage(final PageId pageId, final int pageSize) throws IOException {
    CachedPage page = readCache.get(pageId);
    if (page == null)
      page = loadPage(pageId, pageSize, false, true);

    if (page != null)
      return page.getVersion();

    // NOT EXISTS, RETURN 0
    return 0;
  }

  public ImmutablePage getImmutablePage(final PageId pageId, final int pageSize, final boolean isNew,
      final boolean createIfNotExists) throws IOException {
    final CachedPage page = getCachedPage(pageId, pageSize, isNew, createIfNotExists);
    if (page != null)
      // RETURN ALWAYS A VIEW OF THE PAGE. THIS PREVENTS CONCURRENCY ON THE BUFFER POSITION
      return page.useAsImmutable();
    return null;
  }

  public MutablePage getMutablePage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    final CachedPage page = getCachedPage(pageId, pageSize, isNew, createIfNotExists);
    if (page != null)
      // RETURN ALWAYS A VIEW OF THE PAGE. THIS PREVENT CONCURRENCY ON THE BUFFER POSITION
      return page.useAsMutable();
    return null;
  }

  /**
   * Counts a resolved commutative edge-append merge: a commit-time page conflict avoided by replaying appends
   * on the newer version instead of failing the whole transaction. Surfaced via {@link #getStats()}.
   */
  public void incrementEdgeAppendMerges() {
    totalEdgeAppendMerges.incrementAndGet();
  }

  /**
   * Counts a resolved disjoint-slot page merge: a commit-time page conflict avoided by re-applying this
   * transaction's slot writes (inserts / in-place updates of records the concurrent commit left untouched) on
   * the newer committed page instead of failing the whole transaction. Surfaced via {@link #getStats()}.
   */
  public void incrementTxPageSlotMerges() {
    totalTxPageSlotMerges.incrementAndGet();
  }

  /**
   * Counts a page merge declined because the page could not PROVE that every byte this transaction wrote to it was
   * replayable by that merge (#5596). The transaction falls back to an ordinary retry, so this is never a
   * correctness problem - but a non-zero and growing value means some writer is dirtying a mergeable page without
   * declaring it ({@code MutablePage.beginCoveredWrite}), i.e. contention that used to be absorbed is now being
   * retried. Watch it next to {@link PPageManagerStats#edgeAppendMerges}/{@link PPageManagerStats#txPageSlotMerges}: a jump here with a dip
   * there is exactly the shape of a forgotten declaration.
   * <p>
   * COUNTING SEMANTICS: this is "commit attempts that FAILED on a coverage decline", not "pages declined". The
   * report is made from the commit loop's terminal branch, which then rethrows, so a transaction whose commit
   * touched several undeclared pages contributes exactly one - and a retry that fails the same way contributes one
   * more. That is the right granularity for the question the metric answers (is a writer missing a declaration, and
   * is it costing retries?); do not read it as a page count or divide it by anything.
   */
  public void incrementMergesDeclinedByCoverage() {
    totalMergesDeclinedByCoverage.incrementAndGet();
  }

  public void checkPageVersion(final MutablePage page, final boolean isNew) throws IOException {
    final PageId pageId = page.getPageId();

    final FileManager fileManager = ((DatabaseInternal) pageId.getDatabase()).getFileManager();

    if (!fileManager.existsFile(pageId.getFileId()))
      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + ". The file with id " + pageId.getFileId()
              + " does not exist anymore. Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");

    final int mostRecentPageVersion = getMostRecentVersionOfPage(pageId, page.getPhysicalSize());

    if (mostRecentPageVersion != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      if (page.getVersion() == 0 && mostRecentPageVersion > 1) {
        final ComponentFile file = fileManager.getFile(page.pageId.getFileId());

        // @TODO: TEMPORARY PATCH TO OVERCOME THE ISSUE OF PAGES NOT UPDATED IN THE FILE MANAGER
        final Component component = page.pageId.getDatabase().getSchema().getFileById(page.pageId.getFileId());
        if (component instanceof LocalBucket b) {
          final int realPages = (int) (file.getSize() / b.pageSize);
          try {
            if (realPages > b.pageCount.get()) {
              LogManager.instance().log(this, Level.SEVERE,
                  "New page %s cannot be written because already present in file '%s' with version %d. Updating page count (threadId=%d)",
                  page, file.getFileName(), mostRecentPageVersion, Thread.currentThread().getId());

              b.updatePageCount(realPages);
            }
          } catch (ConcurrentModificationException e) {
            // IGNORE IT
          }
        }
      }

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
    }
  }

  /**
   * Atomic convenience form of {@link #validateAndBumpVersions} + {@link #publishPages}. NOTE: since #4936
   * the engine commit path no longer calls this - commit2ndPhase invokes the two halves separately with the
   * WAL append in between, so validate+publish are NOT atomic under one lock there. Kept as public API for
   * embedded users and backward compatibility; new engine code should call the halves explicitly and state
   * what serializes the gap.
   */
  public void updatePages(final Map<PageId, MutablePage> newPages, final Map<PageId, MutablePage> modifiedPages,
      final boolean asyncFlush) throws IOException, InterruptedException {
    publishPages(validateAndBumpVersions(newPages, modifiedPages), newPages, asyncFlush);
  }

  /**
   * First half of {@link #updatePages}: validates every page against the most recent committed version and
   * bumps the (transaction-private) page versions, WITHOUT publishing anything. #4936: the commit calls this
   * BEFORE appending the transaction to the WAL, so the append is the point of no return - a WAL record can
   * only ever exist for a transaction that already passed validation. Any {@link ConcurrentModificationException}
   * fires before anything durable exists, so crash recovery can never partially replay an aborted transaction.
   * The bump only mutates the transaction's own MutablePage instances: if the WAL append still fails
   * (e.g. interrupt), the reset() discards them and nothing observable happened.
   */
  public List<MutablePage> validateAndBumpVersions(final Map<PageId, MutablePage> newPages,
      final Map<PageId, MutablePage> modifiedPages) throws IOException, InterruptedException {
    lock();
    try {
      final List<MutablePage> pagesToWrite = new ArrayList<>((newPages != null ? newPages.size() : 0) + modifiedPages.size());

      if (newPages != null)
        for (final MutablePage p : newPages.values())
          pagesToWrite.add(updatePageVersion(p, true));

      for (final MutablePage p : modifiedPages.values())
        pagesToWrite.add(updatePageVersion(p, false));

      return pagesToWrite;
    } finally {
      unlock();
    }
  }

  /**
   * Second half of {@link #updatePages}: publishes the validated pages (read cache + flush scheduling).
   * Runs AFTER the WAL append (#4936): from the caller's perspective the transaction is committed once the
   * WAL is durable, and a failure here leaves the WAL to replay the pages on recovery. Releasing the global
   * PageManager lock between the two halves is safe only because the caller serializes committers per page
   * by other means: commit1stPhase holds the per-file commit locks until reset(), so no other transaction can
   * validate, bump or publish these pages in the gap.
   * <p>
   * A replica used to be exempt from that (lockedFiles was left EMPTY, on the assumption that the
   * single-threaded Raft apply serialized everything), but the apply only orders the WRITE side: concurrent
   * local transactions still validated against the same base page version and each shipped a delta stamped
   * with the same next version, which the state machine spliced together. Replicas now take the same locks
   * and hold them until their entry has been applied locally (#5503).
   */
  public void publishPages(final List<MutablePage> pagesToWrite, final Map<PageId, MutablePage> newPages,
      final boolean asyncFlush) throws IOException, InterruptedException {
    lock();
    try {
      // Write pages (and put in readCache) BEFORE updating pageCount, otherwise concurrent
      // transactions can observe pageCount > readCache state and treat the new page as a
      // pre-existing empty page (sparse-file semantics), allowing two records' chunk chains
      // to land on the same physical slot.
      writePages(pagesToWrite, asyncFlush);

      if (newPages != null)
        for (final MutablePage p : newPages.values()) {
          final PageId pid = p.getPageId();
          final PaginatedComponent component = (PaginatedComponent) ((DatabaseInternal) pid.getDatabase()).getSchema()
                  .getFileByIdIfExists(pid.getFileId());
          if (component != null)
            component.updatePageCount(pid.getPageNumber() + 1);
        }

    } finally {
      unlock();
    }
  }

  public MutablePage updatePageVersion(final MutablePage page, final boolean isNew) throws IOException, InterruptedException {
    final PageId pageId = page.getPageId();

    final int mostRecentPageVersion = getMostRecentVersionOfPage(pageId, page.getPhysicalSize());
    if (mostRecentPageVersion != page.getVersion()) {
      totalConcurrentModificationExceptions.incrementAndGet();

      final FileManager fileManager = ((DatabaseInternal) pageId.getDatabase()).getFileManager();
      if (page.getVersion() == 0 && mostRecentPageVersion > 1) {
        LogManager.instance().log(this, Level.SEVERE,
            "Page %s is new and has version 0, but the file '%s' has been modified. Please retry the operation (threadId=%d)",
            null, page, fileManager.getFile(pageId.getFileId()).getFileName(), Thread.currentThread().getId());
      }

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().getId() + ")");
    }

    page.incrementVersion();
    page.updateMetadata();

    LogManager.instance()
        .log(this, Level.FINE, "Updated page %s (size=%d records=%d threadId=%d)", null, page, page.getPhysicalSize(),
            page.readShort(0), Thread.currentThread().getId());

    return page;
  }

  public void overwritePage(final MutablePage page) throws IOException {
    readCache.remove(page.pageId);

    flushPage(page);

    LogManager.instance().log(this, Level.FINE, "Overwritten page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(),
        Thread.currentThread().getId());
  }

  /**
   * Writes a page to disk holding the per-page I/O lock so concurrent readers never observe partially-written bytes. This is used by
   * {@link TransactionManager#applyChanges} during replicated/recovery replay, which writes pages directly outside the normal flush
   * path. Unlike {@link #flushPage}, it does not touch the read cache: the caller is responsible for evicting the page afterwards (via
   * {@link #removePageFromCache(PageId)}) so subsequent reads reload the new content. Evicting after the write (rather than before)
   * avoids a window where a concurrent reader could reload the stale on-disk version into the cache while the write is in flight.
   */
  public void writePageWithLock(final PaginatedComponentFile file, final MutablePage page) throws IOException {
    // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES (same interlock used by flushPage and loadPage)
    concurrentPageAccess(page.pageId, true, () -> {
      final int written = file.write(page);
      totalPagesWrittenSize.addAndGet(written);
    });
    totalPagesWritten.incrementAndGet();
  }

  /**
   * Writes to disk, and takes out of the asynchronous flush pipeline, any page still pending for {@code pageId}.
   * <p>
   * Callers that write a page directly to its file instead of going through the flush queue
   * ({@link TransactionManager#applyChanges} during replicated / recovery replay) must call this first. A committed
   * page is published to the read cache and to the flush thread's index before it reaches the disk, and while that
   * older copy is still pending {@link #loadPage} resolves the page from the queue instead of from the file - so the
   * replicated write stays invisible to every later read - and the eventual flush of that copy overwrites the
   * replicated page, rolling its version backwards. Pushing the pending copy to disk here (rather than discarding it)
   * preserves its content, which is the only baseline the WAL delta can be applied on top of.
   */
  void materializePendingFlushOfPage(final Database database, final PageId pageId)
      throws IOException, InterruptedException {
    final PageManagerFlushThread thread = flushThread;
    if (thread == null)
      return;

    final List<MutablePage> pending = thread.detachPendingPages(database, pageId);
    if (pending.isEmpty())
      return;

    // The flush thread may have taken a batch before the detach reached it: let the in-flight write finish, or it
    // could still land after the caller's write and revert the page on disk. An interrupt here leaves the copies
    // detached but not yet written: they stay durable because their WAL acks have NOT been released
    // (notifyPageFlushed runs inside flushPage below), so their WAL entries are preserved and replayed on the next
    // open.
    thread.waitForCurrentFlushToComplete(database);

    // Successive commits can leave more than one copy pending. The most recent one is a full page image covering
    // every older one, so writing it alone puts the whole pending content on disk.
    MutablePage mostRecent = pending.get(0);
    for (int i = 1; i < pending.size(); i++)
      if (pending.get(i).getVersion() > mostRecent.getVersion())
        mostRecent = pending.get(i);

    flushPage(mostRecent);

    // The superseded copies will never be written. Release their WAL acks - as the dropped-file purge does - or the
    // stale pending count would keep their WAL files alive forever (the close-time ack gate, #4928). takeWALFile
    // makes the release exactly-once.
    for (int i = 0; i < pending.size(); i++) {
      final MutablePage page = pending.get(i);
      if (page != mostRecent) {
        final WALFile walFile = page.takeWALFile();
        if (walFile != null)
          walFile.notifyPageFlushed();
      }
    }

    // The read cache holds the same pending copy: drop it so the caller reads the content just written.
    removePageFromCache(pageId);
  }

  public PPageManagerStats getStats() {
    final PPageManagerStats stats = new PPageManagerStats();
    stats.maxRAM = maxRAM;
    stats.readCacheRAM = totalReadCacheRAM.get();
    // readCache and flushThread are populated by startup() (the 0 -> 1 acquire transition), which is called on first DB
    // open. When no database has been opened yet (e.g. a profiler snapshot taken at server
    // startup) they're still null - report empty cache/queue rather than NPE.
    stats.readCachePages = readCache != null ? readCache.size() : 0;
    stats.pagesRead = totalPagesRead.get();
    stats.pagesReadSize = totalPagesReadSize.get();
    stats.pagesWritten = totalPagesWritten.get();
    stats.pagesWrittenSize = totalPagesWrittenSize.get();
    stats.pageFlushQueueLength = flushThread != null ? flushThread.queue.size() : 0;
    stats.cacheHits = cacheHits.get();
    stats.cacheMiss = cacheMiss.get();
    stats.concurrentModificationExceptions = totalConcurrentModificationExceptions.get();
    stats.edgeAppendMerges = totalEdgeAppendMerges.get();
    stats.txPageSlotMerges = totalTxPageSlotMerges.get();
    stats.mergesDeclinedByCoverage = totalMergesDeclinedByCoverage.get();
    stats.evictionRuns = evictionRuns.get();
    stats.pagesEvicted = pagesEvicted.get();
    stats.snapshotWindowsOpened = totalSnapshotWindowsOpened.get();
    stats.snapshotWindowsInvalidated = totalSnapshotWindowsInvalidated.get();
    stats.snapshotPreImagesCaptured = totalSnapshotPreImagesCaptured.get();
    stats.deferredRAMBytes = getDeferredRAMBytes();
    collectSnapshotGauges(stats);
    return stats;
  }

  /**
   * Fills in the per-window readings of {@link PPageManagerStats} from the currently open snapshot windows (#6116).
   * <p>
   * Reads the published array ONCE, without the registry lock: a window that closes underneath this reports its last
   * values, which is what any sampled gauge does. The shadow accessors take the shadow's own monitor, which the
   * capture path also holds for the duration of one page copy - at the one-per-scrape rate this is called at, that
   * is immaterial, but it is the reason this is not called from anywhere hotter than a metrics scrape.
   */
  private void collectSnapshotGauges(final PPageManagerStats stats) {
    final PageSnapshot[] snapshots = activeSnapshots;
    if (snapshots == null)
      return;

    final long now = System.currentTimeMillis();
    long oldestOpenedOn = Long.MAX_VALUE;
    for (final PageSnapshot snapshot : snapshots) {
      ++stats.snapshotWindowsOpen;
      stats.snapshotShadowedPages += snapshot.getShadowedPages();
      final long shadowBytes = snapshot.getShadowSizeInBytes();
      stats.snapshotShadowBytes += shadowBytes;
      stats.snapshotShadowSpilledBytes += snapshot.getShadowSpilledBytes();

      final long maxBytes = snapshot.getShadowMaxSizeInBytes();
      if (maxBytes > 0)
        // THE MAXIMUM, NOT THE AVERAGE: WHAT MATTERS IS HOW CLOSE THE CLOSEST WINDOW IS TO BREACHING, BECAUSE THAT
        // IS THE ONE WHOSE BACKUP IS ABOUT TO RESTART ON THE SUSPEND-AND-FREEZE PATH
        stats.snapshotShadowUsagePerc = Math.max(stats.snapshotShadowUsagePerc, 100.0 * shadowBytes / maxBytes);

      oldestOpenedOn = Math.min(oldestOpenedOn, snapshot.getOpenedOn());
    }

    if (oldestOpenedOn != Long.MAX_VALUE)
      stats.snapshotOldestWindowMillis = Math.max(0L, now - oldestOpenedOn);
  }

  /** Counts a window that lost its point in time (cap breach or I/O error): the fallback-path early warning. */
  void incrementSnapshotWindowsInvalidated() {
    totalSnapshotWindowsInvalidated.incrementAndGet();
  }

  public void removePageFromCache(final PageId pageId) {
    final CachedPage page = readCache.remove(pageId);
    if (page != null)
      totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());
  }

  public void writePages(final List<MutablePage> updatedPages, final boolean asyncFlush) throws IOException, InterruptedException {
    if (asyncFlush) {
      for (final MutablePage page : updatedPages)
        putPageInReadCache(new CachedPage(page, true));
      flushThread.scheduleFlushOfPages(updatedPages);
    } else {
      // SYNCHRONOUS FLUSH
      for (final MutablePage page : updatedPages) {
        flushPage(page);
        // ADD THE PAGE IN TO READ CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED, SO IT CAN BE CACHED
        putPageInReadCache(new CachedPage(page, false));
      }
    }
  }

  protected void flushPage(final MutablePage page) throws IOException {
    final DatabaseInternal database = (DatabaseInternal) page.getPageId().getDatabase();

    if (!database.isOpen()) {
      LogManager.instance().log(this, Level.SEVERE, "Cannot flush page %s because the database is closed", page);
      return;
    }

    final FileManager fileManager = database.getFileManager();
    final int fileId = page.pageId.getFileId();

    if (fileManager.existsFile(fileId)) {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getFile(fileId);
      if (!file.isOpen())
        throw new DatabaseMetadataException("Cannot flush pages on disk because file '" + file.getFileName() + "' is closed");

      LogManager.instance()
          .log(this, Level.FINE, "Flushing page %s to disk (threadId=%d)...", null, page, Thread.currentThread().getId());

      // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
      concurrentPageAccess(page.pageId, true, () -> {
        final int written = file.write(page);
        totalPagesWrittenSize.addAndGet(written);
      });

      try {
        final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileByIdIfExists(fileId);
        if (component != null)
          component.updatePageCount(page.pageId.getPageNumber() + 1);

        totalPagesWritten.incrementAndGet();

        database.getTransactionManager().notifyPageFlushed(page);
      } catch (final DatabaseIsClosedException e) {
        // The database was closed concurrently after the isOpen() check above.
        // The page data has already been written to disk, so we can safely skip
        // the metadata updates.
      }

    } else {
      LogManager.instance()
          .log(this, Level.FINE, "Cannot flush page %s because the file has been dropped (threadId=%d)...", null, page,
              Thread.currentThread().getId());
      // The page will never be flushed and its content is irrelevant (the file is gone): release its WAL
      // ack, or the stale pending count would make every later clean close preserve the WAL for nothing
      // (the close-time ack gate, #4928). takeWALFile makes the release exactly-once against the racing
      // dropped-file batch purge.
      final WALFile walFile = page.takeWALFile();
      if (walFile != null)
        walFile.notifyPageFlushed();
    }
  }

  private CachedPage loadPage(final PageId pageId, final int size, final boolean createIfNotExists, final boolean cache)
      throws IOException {
    final DatabaseInternal database = (DatabaseInternal) pageId.getDatabase();

    // ASSURE THE PAGE IS NOT IN THE FLUSHING QUEUE
    CachedPage page = flushThread.getCachedPageFromMutablePageInQueue(pageId);
    if (page == null) {
      final PaginatedComponentFile file = (PaginatedComponentFile) database.getFileManager().getFile(pageId.getFileId());

      final boolean isNewPage = pageId.getPageNumber() >= file.getTotalPages();
      if (!createIfNotExists && isNewPage)
        // AVOID CREATING AN EMPTY PAGE JUST TO CHECK THE VERSION
        return null;

      checkForPageDisposal();

      page = new CachedPage(this, pageId, size);

      if (!isNewPage) {
        // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
        final CachedPage finalPage = page;
        concurrentPageAccess(pageId, false, () -> file.read(finalPage));
      }

      page.loadMetadata();

      LogManager.instance().log(this, Level.FINE, "Loaded page %s (threadId=%d)", null, page, Thread.currentThread().getId());
    }

    totalPagesRead.incrementAndGet();
    totalPagesReadSize.addAndGet(page.getPhysicalSize());

    if (cache)
      putPageInReadCache(page);

    return page;
  }

  /**
   * The single funnel every physical page read and write goes through, and therefore the natural home of the
   * copy-on-write hook of issue #6075: putting it in the write branch here covers {@link #flushPage} (the normal
   * sync/async flush path) and {@link #writePageWithLock} (Raft and recovery replay) BY CONSTRUCTION - they are the
   * only two physical page-write call sites in the engine - with no new lock and no new lock-ordering risk, and the
   * pre-image capture is serialized against concurrent readers of the same page for free.
   * <p>
   * <b>Nothing runs before the {@code null} check</b>: no {@code PageId} allocation, no map lookup, no extra lock.
   * With no window open the whole hook is one volatile read and a perfectly predicted branch.
   */
  private void concurrentPageAccess(final PageId pageId, final boolean writeAccess, final ConcurrentPageAccessCallback callback)
      throws IOException {
    // ACQUIRE A LOCK ON THE I/O OPERATION TO AVOID PARTIAL READS/WRITES
    while (true) {
      // Fail loud on interrupt instead of silently skipping the I/O (#4924). The interrupt is only checked
      // BEFORE acquiring the per-page slot, so an in-flight read/write is never torn. Returning here without
      // running the callback would let a caller cache a zero-filled page (loadPage) or ack a flush that never
      // reached disk via notifyPageFlushed (flushPage) - both silently lose committed data. The interrupt flag
      // is left set so upper layers still observe the cancellation.
      if (Thread.currentThread().isInterrupted())
        throw new InterruptedIOException("Interrupted while acquiring I/O lock for page " + pageId);

      if (pendingFlushPages.putIfAbsent(pageId, writeAccess) == null)
        try {
          if (writeAccess) {
            final PageSnapshot[] snapshots = activeSnapshots;
            if (snapshots != null)
              capturePreImages(snapshots, pageId);
          }
          callback.access();
          return;
        } finally {
          pendingFlushPages.remove(pageId);
        }

      // WAIT AND RETRY
      Thread.yield();
    }
  }

  private void checkForPageDisposal() {
    final long now = System.currentTimeMillis();
    if (now - lastCheckForRAM < 100)
      return;

    final long totalRAM = totalReadCacheRAM.get();
    if (totalRAM < maxRAM)
      return;

    final long ramToFree = totalRAM * freePageRAM / 100;

    evictOldestPages(ramToFree, totalRAM);
  }

  private synchronized void evictOldestPages(final long ramToFree, final long totalRAM) {
    evictionRuns.incrementAndGet();

    LogManager.instance()
        .log(this, Level.FINE, "Reached max RAM for page cache. Freeing pages from cache (target=%d current=%d max=%d threadId=%d)",
            null, ramToFree, totalRAM, maxRAM, Thread.currentThread().getId());

    // GET THE <DISPOSE_PAGES_PER_CYCLE> OLDEST PAGES
    // ORDER PAGES BY LAST ACCESS + SIZE
    final TreeSet<CachedPage> pagesOrderedByAge = new TreeSet<>((o1, o2) -> {
      final int lastAccessed = Long.compare(o1.getLastAccessed(), o2.getLastAccessed());
      if (lastAccessed != 0)
        return lastAccessed;

      // SAME TIMESTAMP, CHECK THE PAGE SIZE: LARGER PAGE SHOULD BE REMOVED FIRST THAN OTHERS
      final int pageSize = -Long.compare(o1.getPhysicalSize(), o2.getPhysicalSize());
      if (pageSize != 0)
        return pageSize;

      return o1.getPageId().compareTo(o2.getPageId());
    });

    pagesOrderedByAge.addAll(readCache.values());

    // REMOVE OLDEST PAGES FROM RAM
    long freedRAM = 0;
    for (final CachedPage page : pagesOrderedByAge) {
      final CachedPage removedPage = readCache.remove(page.getPageId());
      if (removedPage != null) {
        // Account the entry ACTUALLY removed, not the TreeSet snapshot: the version-monotonic put can swap
        // a same-PageId entry for a different instance between the snapshot and this remove (#4925/#4933).
        freedRAM += removedPage.getPhysicalSize();
        totalReadCacheRAM.addAndGet(-1L * removedPage.getPhysicalSize());
        pagesEvicted.incrementAndGet();

        if (freedRAM > ramToFree)
          break;
      }
    }

    final long newTotalRAM = totalReadCacheRAM.get();

    LogManager.instance()
        .log(this, Level.FINE, "Freed %s RAM (current=%s max=%s threadId=%d)", null, FileUtils.getSizeAsString(freedRAM),
            FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().log(this, Level.WARNING, "Cannot free pages in RAM (current=%s > max=%s threadId=%d)", null,
          FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().getId());

    lastCheckForRAM = System.currentTimeMillis();
  }

  // Reused per-thread slot for the RAM delta decided inside VERSION_MONOTONIC_MERGE: keeps the put hot
  // path allocation-free (a capturing lambda + holder array per call would rely on escape analysis that is
  // not guaranteed under a megamorphic merge call site).
  private static final ThreadLocal<long[]> PUT_RAM_DELTA = ThreadLocal.withInitial(() -> new long[1]);

  // #4925: version-monotonic merge. Keeps whichever version is newer; an EQUAL version replaces on purpose
  // (identical content, freshest instance, zero RAM delta). Static and non-capturing: merge() hands the new
  // page in as the second argument, so this function allocates nothing per call.
  private static final BiFunction<CachedPage, CachedPage, CachedPage> VERSION_MONOTONIC_MERGE = (prev, cur) -> {
    if (cur.getVersion() >= prev.getVersion()) {
      PUT_RAM_DELTA.get()[0] = cur.getPhysicalSize() - prev.getPhysicalSize();
      return cur;
    }
    // STALE WRITE ATTEMPT: KEEP THE NEWER CACHED VERSION, NO ACCOUNTING CHANGE
    PUT_RAM_DELTA.get()[0] = 0;
    return prev;
  };

  void putPageInReadCache(final CachedPage page) {
    // #4925: version-monotonic put. A reader that started a disk read of version N before a committer
    // cached version N+1 must not overwrite the newer committed page with its stale image: the poisoned
    // cache would serve vN to every subsequent reader AND to the commit-time version probe, letting a later
    // transaction pass its MVCC check and silently overwrite the lost committed update. The RAM delta is
    // decided inside the same atomic merge so the accounting always matches the actual cache content.
    final long[] ramDelta = PUT_RAM_DELTA.get();
    // Default covers the absent-key case: merge() inserts the page WITHOUT invoking the remapping function.
    ramDelta[0] = page.getPhysicalSize();
    readCache.merge(page.getPageId(), page, VERSION_MONOTONIC_MERGE);
    if (ramDelta[0] != 0)
      totalReadCacheRAM.addAndGet(ramDelta[0]);

    checkForPageDisposal();
  }

  private CachedPage getCachedPage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    checkForPageDisposal();

    CachedPage page = readCache.get(pageId);
    if (page == null) {
      // #4958: count the miss BEFORE returning the freshly loaded page. The counter used to be bumped
      // only on the page-not-found fall-through below, so cacheMiss stayed at ~0 forever and the
      // hit/miss ratio in the stats was meaningless.
      cacheMiss.incrementAndGet();

      page = loadPage(pageId, pageSize, createIfNotExists, true);
      if (page == null) {
        if (isNew)
          return null;
      } else
        return page;

    } else {
      cacheHits.incrementAndGet();
      page.updateLastAccesses();
    }

    if (page == null)
      throw new IllegalArgumentException(
          "Page id '" + pageId + "' does not exist (threadId=" + Thread.currentThread().getId() + ")");

    return page;
  }
}
