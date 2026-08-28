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
import com.arcadedb.exception.DatabaseOperationException;
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
  // #5636: the counters below, down to totalChunkChainReadRetries, are exported as Prometheus COUNTERS (see
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
  private final    AtomicLong                        totalChunkChainReadRevalidations      = new AtomicLong();
  private final    AtomicLong                        totalChunkChainReadRetries            = new AtomicLong();
  private final    AtomicLong                        evictionRuns                          = new AtomicLong();
  private final    AtomicLong                        pagesEvicted                          = new AtomicLong();
  // #6116: the snapshot totals below are exported as Prometheus counters together with the ones above, and
  // carry the same never-decrease requirement. They are JVM-wide like the rest of this class, so a database close
  // does not step them back.
  private final    AtomicLong                        totalSnapshotWindowsOpened            = new AtomicLong();
  private final    AtomicLong                        totalSnapshotWindowsInvalidated       = new AtomicLong();
  private final    AtomicLong                        totalSnapshotPreImagesCaptured        = new AtomicLong();
  // #6125: the two halves of totalSnapshotWindowsInvalidated. A cap breach is a TUNING problem (raise
  // PAGE_SNAPSHOT_MAX_SIZE, or give the spill volume room); a capture failure is a DISK problem. Both force the
  // consumer onto the writer-throttling fallback, so one summed counter cannot tell an operator which lever to pull.
  private final    AtomicLong                        totalSnapshotWindowsOverflowed        = new AtomicLong();
  private final    AtomicLong                        totalSnapshotWindowsFailed            = new AtomicLong();
  // #6125: the t0 barrier is the one stall this design still has, so it is measured like a Micrometer timer - a
  // count and a summed duration, whose ratio is the average, plus the high-water mark a percentile cannot be
  // reconstructed from a scalar snapshot.
  private final    AtomicLong                        totalSnapshotBarriers                 = new AtomicLong();
  private final    AtomicLong                        totalSnapshotBarrierMillis            = new AtomicLong();
  private final    AtomicLong                        maxSnapshotBarrierMillis              = new AtomicLong();
  private final    AtomicLong                        totalSnapshotBarriersInexact          = new AtomicLong();
  // #6132, item 2: a barrier that gives up before publishing a window was counted only as a barrier, indistinguishable
  // from one that succeeded. The two invalidation counters above require a window to EXIST, so they cannot see it.
  // Split by the step that gave up, because the answer differs: SUSPEND_TIMEOUT is a busy process (another suspender
  // resuming), FLUSH_TIMEOUT is a sick disk, and the rest is everything else - and the consumer's fallback to
  // suspend-and-freeze hides all three behind a backup that still completes, just by throttling every writer.
  private final    AtomicLong                        totalSnapshotBarriersFailed           = new AtomicLong();
  private final    AtomicLong                        totalSnapshotBarriersFailedSuspend    = new AtomicLong();
  private final    AtomicLong                        totalSnapshotBarriersFailedFlush      = new AtomicLong();
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

  /**
   * Hard wall-clock budget for EVERYTHING the t0 barrier does with the JVM-wide page-manager lock held (#6125): the
   * residual drain, the suspension acquisition and the wait for the in-flight flush batch. It is a ceiling on how
   * long a wedged flush thread or a concurrent resume can stall every committer of every database in the JVM, which
   * neither the progress-based {@code arcadedb.flushAllPagesTimeout} (60 s) nor the uncapped waits those steps use
   * elsewhere would bound acceptably here.
   * <p>
   * It is not a wait the barrier is expected to use: the first, lock-free drain has already emptied the pipeline, so
   * all three steps normally return on their first check. Sized with a hundredfold margin over the one legitimately
   * slow case - a single large transaction's batch reaching the disk - so that exhausting it means something is
   * genuinely wrong rather than merely busy.
   */
  private static final long SNAPSHOT_BARRIER_MAX_MILLIS = 5_000;

  /**
   * Fraction of the space still usable on the spill volume that the automatically sized shadow cap may claim
   * (#6125). Half, so a window can never be the reason a database runs out of disk: what remains still covers the
   * archive the backup is writing and the ordinary growth of the database while the window is open.
   */
  private static final int SNAPSHOT_MAX_SIZE_FREE_SPACE_DIVISOR = 2;

  /**
   * The installed test-only page-read fault injector, {@code null} when there is none - which is the steady state.
   * Declared here with the other fields, ahead of the interface that types it: a field after a nested type is a PMD
   * FieldDeclarationsShouldBeAtStartOfClass violation, and a forward reference to a type declared later in the same
   * file is perfectly ordinary Java. See {@link PageReadFaultInjector} for what it is for.
   */
  private static volatile PageReadFaultInjector pageReadFaultInjector = null;

  /**
   * TEST-ONLY fault injection on the logical page-read funnel (#6282, item 4).
   * <p>
   * There was no way to make a specific page read fail on demand, and the paths whose WRONG ANSWER is the most
   * expensive in the engine are exactly the I/O-failure paths: {@code LocalBucket.findBrokenChunkChain} answering
   * "the chain is broken" for a page it merely could not read condemns a HEALTHY record permanently and points the
   * operator at {@code CHECK DATABASE FIX}, which deletes it. That defect was found by reading rather than by a
   * test, in the third review round of #6258, and it could not have been found by a test.
   * <p>
   * It hooks {@link #getCachedPage}, the funnel {@link #getImmutablePage} and {@link #getMutablePage} both go
   * through, rather than the physical read in {@link #loadPage}: a test that wants a read to fail has no way to keep
   * the page out of the read cache, so a hook on the physical read would simply never fire for a page the engine has
   * just written.
   * <p>
   * <b>Cost when nothing is installed</b>: one read of a {@code static volatile} reference that is always null and a
   * branch the predictor always gets right - the same shape, and the same reasoning, as {@link #activeSnapshots} on
   * the write path. Nothing is allocated and nothing is looked up.
   * <p>
   * Static because a test installs it before it has a {@link PageManager} reference to hand, and JVM-wide because
   * this class is. Tests MUST clear it in a {@code finally}.
   */
  @ExcludeFromJacocoGeneratedReport
  @FunctionalInterface
  public interface PageReadFaultInjector {
    /**
     * @throws IOException to make this logical page read fail, as a disk that could not serve the page would. Return
     *                     normally to let it proceed.
     */
    void onPageRead(PageId pageId) throws IOException;
  }

  /** Installs (or, with {@code null}, removes) the test-only page-read fault injector. See {@link PageReadFaultInjector}. */
  public static void setPageReadFaultInjector(final PageReadFaultInjector injector) {
    pageReadFaultInjector = injector;
  }

  /** The installed test-only page-read fault injector, {@code null} when there is none - which is the steady state. */
  public static PageReadFaultInjector getPageReadFaultInjector() {
    return pageReadFaultInjector;
  }

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
    /**
     * Batches waiting in the flush pipeline across EVERY open database. Its scale changed with issue #6281: until
     * then the queue was capacity-bound to {@code arcadedb.pageFlushQueue}, so this reaching that value meant "the
     * pipeline is full". The bound is now per database, so this is a sum and can reach
     * {@code pageFlushQueue x open databases}. An alert comparing it against {@code pageFlushQueue} therefore no
     * longer says what it used to - {@link #pageFlushQueueMaxPerDatabase} is the number that still does.
     */
    public int  pageFlushQueueLength;
    /**
     * The busiest single database's share of that pipeline - reserved by a committer or occupied by its batch - which
     * is what {@code arcadedb.pageFlushQueue} actually bounds since issue #6281. This reaching the configured value
     * is the "a database is at its bound, and its committers are being held" signal that
     * {@link #pageFlushQueueLength} used to carry, and it is the one to alert on.
     */
    public int  pageFlushQueueMaxPerDatabase;
    public long cacheHits;
    public long cacheMiss;
    public long concurrentModificationExceptions;
    public long edgeAppendMerges;
    public long txPageSlotMerges;
    /** See {@link PageManager#incrementMergesDeclinedByCoverage()}: commit ATTEMPTS failed on a coverage decline, not pages. */
    public long mergesDeclinedByCoverage;
    /** See {@link PageManager#incrementChunkChainReadRevalidations()}: the read-path twin of {@code txPageSlotMerges}. */
    public long chunkChainReadRevalidations;
    /** See {@link PageManager#incrementChunkChainReadRetries()}: chunked reads restarted because the record itself moved. */
    public long chunkChainReadRetries;
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
    /**
     * #6125: the two halves of {@link #snapshotWindowsInvalidated}. Overflowed means the shadow hit
     * {@code arcadedb.pageSnapshotMaxSize} and is answered by raising it (or by giving the spill volume room, since
     * the automatic sizing measures against the free space there); failed means a pre-image could not be read or
     * written and is answered by looking at the disk. Both end in the same fallback, so the summed counter alone
     * tells an operator that a backup started throttling the writers but not which lever to pull.
     */
    public long   snapshotWindowsOverflowed;
    public long   snapshotWindowsFailed;
    public long   snapshotPreImagesCaptured;
    /**
     * #6125: the t0 barrier, measured the way a Micrometer timer is - a count and a summed duration, so
     * {@code rate(millis)/rate(count)} is the average - plus the high-water mark, which a scalar snapshot cannot
     * otherwise carry. The barrier is the ONLY stall left in the snapshot path, so it is the one reading that says
     * whether opening a window is costing a writer anything.
     */
    public long   snapshotBarriers;
    public long   snapshotBarrierMillis;
    public long   snapshotBarrierMaxMillis;
    /**
     * Barriers that could not prove the flush pipeline was empty at t0, so the snapshot point may sit slightly
     * behind the last committed transaction. Since #6125 no commit can cause this - the drain runs with the
     * publication locks held - so a non-zero value means index compaction was feeding the pipeline throughout.
     */
    public long   snapshotBarriersInexact;
    /**
     * #6132: barriers that gave up before publishing a window at all, and the split of the two steps that can. A
     * failed barrier leaves NO other trace an operator can alert on - {@link #snapshotWindowsInvalidated} and its two
     * halves are incremented from a window that exists, and the consumer falls back to suspend-and-freeze and still
     * completes - so without these a backup that has quietly gone back to throttling every writer on the server looks
     * exactly like one that has not. {@code snapshotBarriersFailedSuspend} is transient and expected under load;
     * {@code snapshotBarriersFailedFlush} is a write that never landed, which points at the disk.
     */
    public long   snapshotBarriersFailed;
    public long   snapshotBarriersFailedSuspend;
    public long   snapshotBarriersFailedFlush;
    /** See {@link PageManager#getDeferredRAMBytes()} (#6087): dirty pages held in RAM by a flush suspension. */
    public long   deferredRAMBytes;
    /** See {@link PageManager#getFlushQueueWaits()} (#6259): commits held waiting for room in the flush queue. */
    public long   flushQueueWaits;
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
   * the way {@code waitForCurrentFlushToComplete} does. This first drain takes no lock: it is the bulk of the work
   * and there is no reason to make anyone wait for it.</li>
   * <li>The transaction manager's apply lock is taken exclusively, which excludes Raft and crash-recovery replay -
   * the one writer that goes to the files outside the flush pipeline ({@code writePageWithLock}). Applies are
   * serialised on the state machine thread, so this is a single bounded wait.</li>
   * <li>The global page-manager lock is taken. {@link #publishPages} holds it across BOTH halves of publication -
   * the synchronous page write and the {@code scheduleFlushOfPages} enqueue - so from here on no committer can put
   * a page on disk OR into the flush pipeline.</li>
   * <li>The drain is repeated, now under those locks. It is guaranteed to converge because nothing can feed the
   * pipeline any more, and it is normally instant because step 1 already emptied it; what it covers is exactly the
   * commits that landed between step 1 and step 2. The flush thread is then suspended and its in-flight batch
   * awaited, parking the asynchronous path on a BATCH boundary - a batch is one transaction's pages, so a snapshot
   * taken between batches can never hold half a transaction.</li>
   * </ol>
   * <b>Nothing inside those locks may block on the filesystem</b>, for the same reason - which is why the shadow's
   * spill directory is resolved, and the room on that volume read, back in step 1: {@code mkdirs} and
   * {@code getUsableSpace} are unbounded blocking calls, and {@code arcadedb.pageSnapshotSpillPath} exists precisely
   * to name a volume that is not the database's own. What is left inside is arithmetic over the t0 file list, plus
   * the one {@code channel.size()} per file that is definitionally part of t0.
   * <p>
   * <b>Everything in step 4 shares one hard {@link #SNAPSHOT_BARRIER_MAX_MILLIS} deadline</b>, because the lock it
   * holds is JVM-wide: the waits those three steps use elsewhere are uncapped (the in-flight batch wait polls until
   * a synchronous {@code file.write} returns) or capped only by the 60 s progress-based flush timeout, and either
   * would let one sick disk stall every committer of every database in the process. Exhausting the budget is handled
   * per step according to what it costs: a pipeline that has not drained only means t0 may sit slightly behind the
   * last commit, which is logged and accepted, while a suspension that cannot be acquired or an in-flight batch that
   * has not landed abandons the window outright - the batch is half-written by definition - and the consumer falls
   * back to the suspend-and-freeze path, which is slower for one database rather than briefly fatal for all of them.
   * Only then are the per-file page counts and {@code lastTxId} recorded and the window published. Doing these in
   * the other order produces subtly torn snapshots that pass tests and fail in production.
   * <p>
   * <b>Why the drain moved inside the locks</b> (#6125). It used to run entirely outside them, which left a gap
   * between "the pipeline is empty" and "the flush thread is suspended" that a commit could land in; the barrier
   * coped by re-checking and retrying up to three times, and gave up after that with a t0 that could sit behind the
   * last committed transaction. Measured, that retry - not the flush-queue depth, which stays near zero on an SSD -
   * was the whole cost of the barrier: tens of milliseconds under sustained writes against 13 us idle. Holding the
   * publication lock across the residual drain removes the gap by construction, so the barrier is single-pass and
   * {@code lastTxId} is exact. Exactness matters beyond tidiness: the HA snapshot ship writes it into the
   * {@code last-tx-id.bin} recency marker the follower is judged by, and a marker naming a transaction whose pages
   * were still queued claims data the archive does not contain.
   * <p>
   * The obvious inversion does NOT work and must not be reintroduced: suspending the flush thread before the drain
   * makes the drain unable to ever complete, because a suspended thread defers batches instead of writing them.
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
   * <p>
   * {@link PageSnapshotException#getReason()} tells a caller that wants to distinguish them apart WHY the barrier
   * gave up: {@code SUSPEND_TIMEOUT} is transient and expected under load, {@code FLUSH_TIMEOUT} is a stuck disk
   * rather than a busy one. The unconditional fallback above does not need the distinction; diagnostics and tests do
   * (#6394).
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

  /** Feeds the timer over the t0 barrier: a count, a summed duration and the high-water mark (#6125). */
  private void recordSnapshotBarrier(final long elapsedMillis) {
    totalSnapshotBarriers.incrementAndGet();
    totalSnapshotBarrierMillis.addAndGet(elapsedMillis);
    maxSnapshotBarrierMillis.accumulateAndGet(elapsedMillis, Math::max);
  }

  /**
   * Counts a barrier that gave up before publishing a window (#6132, item 2). Always bumps the total, and the split
   * counter for the step that gave up when there is one - the total is what an operator alerts on, the split is what
   * tells them where to look. Paired with {@link #recordSnapshotBarrier}, which times this barrier whether it
   * succeeded or not.
   */
  private void recordSnapshotBarrierFailure(final PageSnapshotException.Reason reason) {
    totalSnapshotBarriersFailed.incrementAndGet();
    if (reason == PageSnapshotException.Reason.SUSPEND_TIMEOUT)
      totalSnapshotBarriersFailedSuspend.incrementAndGet();
    else if (reason == PageSnapshotException.Reason.FLUSH_TIMEOUT)
      totalSnapshotBarriersFailedFlush.incrementAndGet();
  }

  private PageSnapshot openSnapshotInternal(final DatabaseInternal database) throws IOException, InterruptedException {
    final PageManagerFlushThread thread = flushThread;
    if (thread == null)
      // NOT TIMED AND NOT COUNTED: NOTHING OF THE BARRIER RAN, AND A CALL THAT REFUSED INSTANTLY WOULD OTHERWISE PULL
      // THE AVERAGE THIS METRIC EXISTS TO REPORT TOWARDS ZERO
      throw new PageSnapshotException(
          "Cannot open a snapshot of database '" + database.getName() + "': the page manager is not running",
          PageSnapshotException.Reason.NOT_RUNNING);

    synchronized (snapshotBarrierLock(database)) {
      // THE CLOCK STARTS HERE, NOT AT THE PUBLIC ENTRY POINT: THIS MONITOR ONLY SERIALIZES BARRIERS ON THE SAME
      // DATABASE, AND A SECOND CALLER'S WAIT ON IT IS QUEUEING FOR THE BARRIER RATHER THAN THE BARRIER ITSELF. THE
      // SUSPENSION RELEASE IN THE OUTER finally IS DELIBERATELY INSIDE THE MEASUREMENT - IT FLUSHES WHAT THE
      // SUSPENSION DEFERRED, WHICH IS PART OF WHAT OPENING THE WINDOW COSTS
      final long beginTime = System.currentTimeMillis();
      boolean suspended = false;
      try {
        // STEP 1: THE BULK DRAIN, DELIBERATELY OUTSIDE EVERY LOCK. IT IS THE LONG PART, AND MAKING COMMITTERS WAIT
        // FOR IT WOULD BE THE STALL THIS DESIGN EXISTS TO REMOVE
        if (!waitAllPagesOfDatabaseAreFlushed(database))
          LogManager.instance().log(this, Level.WARNING,
              "Snapshot of database '%s': the flush queue did not drain within the timeout, the snapshot point may be behind the last committed transaction",
              null, database.getName());

        // RESOLVE THE SHADOW'S SPILL LOCATION AND THE ROOM ON THAT VOLUME HERE, WHILE NOTHING IS LOCKED. BOTH ARE
        // BLOCKING FILESYSTEM CALLS - isDirectory/mkdirs AND getUsableSpace - AND arcadedb.pageSnapshotSpillPath IS
        // MEANT TO POINT AT ANOTHER VOLUME, WHICH IN A REAL DEPLOYMENT MAY WELL BE NETWORK-ATTACHED. NEITHER CAN BE
        // BOUNDED BY A DEADLINE, SO RUNNING THEM UNDER THE JVM-WIDE LOCK WOULD REINTRODUCE EXACTLY THE STALL THE
        // REST OF THIS BARRIER EXISTS TO REMOVE. WHAT STAYS INSIDE THE LOCK IS PURE ARITHMETIC OVER THE t0 FILE LIST
        final File spillDirectory = snapshotSpillDirectory(database);
        final long spillVolumeUsableSpace = spillDirectory.getUsableSpace();

        final ReentrantReadWriteLock applyLock = database.getTransactionManager().getApplyLock();
        applyLock.writeLock().lock();
        try {
          lock();
          try {
            // ONE BUDGET FOR EVERYTHING BELOW, NOT ONE PER STEP: WHAT HAS TO BE BOUNDED IS THE TOTAL TIME THE
            // JVM-WIDE LOCK IS HELD, AND THREE INDEPENDENT CEILINGS WOULD MULTIPLY IT. STARTED ONLY NOW, AFTER BOTH
            // LOCKS ARE IN HAND: TIME SPENT WAITING TO ACQUIRE THEM IS QUEUEING, NOT HOLDING - CHARGING IT TO THE
            // BUDGET WOULD SHRINK THE REAL WAITS UNDER CONTENTION AND MAKE A TIMEOUT BLAME THE WRONG THING
            final long deadline = System.currentTimeMillis() + SNAPSHOT_BARRIER_MAX_MILLIS;

            // STEP 2: THE RESIDUAL DRAIN, WITH PUBLICATION EXCLUDED. publishPages HOLDS THIS SAME LOCK ACROSS BOTH
            // THE SYNCHRONOUS PAGE WRITE AND THE FLUSH ENQUEUE, SO NOTHING CAN FEED THE PIPELINE WHILE WE HOLD IT
            // AND THIS CONVERGES BY CONSTRUCTION. IT COVERS EXACTLY THE COMMITS THAT LANDED SINCE STEP 1, WHICH IS
            // WHY THE RETRY LOOP OF #6075 IS GONE (#6125)
            if (!thread.waitPendingPagesOfDatabaseUntil(database, deadline))
              // NOT FATAL: A SNAPSHOT TAKEN OVER A STILL-QUEUED BATCH IS CONSISTENT, JUST POSSIBLY BEHIND - THE
              // BATCH BOUNDARY IS GUARANTEED BY THE SUSPENSION BELOW, NOT BY THIS DRAIN
              LogManager.instance().log(this, Level.WARNING,
                  "Snapshot of database '%s': the flush pipeline did not settle within %d ms under the publication lock, the snapshot point may be behind the last committed transaction",
                  null, database.getName(), SNAPSHOT_BARRIER_MAX_MILLIS);

            if (!thread.trySuspendUntil(database, deadline))
              // A CONCURRENT RESUME IS FLUSHING ITS DEFERRED BACKLOG (UP TO flushSuspendMaxDeferredRAM) AND WOULD
              // KEEP EVERY COMMITTER IN THE JVM WAITING BEHIND THIS LOCK. GIVE THE WINDOW UP INSTEAD: THE CONSUMER
              // FALLS BACK TO SUSPEND-AND-FREEZE, WHICH IS SLOWER FOR ONE DATABASE RATHER THAN BRIEFLY FATAL FOR ALL
              throw new PageSnapshotException("Cannot open a snapshot of database '" + database.getName()
                  + "': the page flush could not be suspended within " + SNAPSHOT_BARRIER_MAX_MILLIS
                  + " ms because another suspender is still resuming", PageSnapshotException.Reason.SUSPEND_TIMEOUT);
            suspended = true;

            if (!thread.waitForCurrentFlushToCompleteUntil(database, deadline))
              // FATAL, UNLIKE THE DRAIN ABOVE: THE IN-FLIGHT BATCH IS HALF-WRITTEN BY DEFINITION, SO t0 WOULD HOLD
              // HALF A TRANSACTION. A WRITE THAT HAS NOT RETURNED IN THIS LONG IS A SICK DISK, NOT A BUSY ONE
              throw new PageSnapshotException("Cannot open a snapshot of database '" + database.getName()
                  + "': the in-flight page flush did not complete within " + SNAPSHOT_BARRIER_MAX_MILLIS + " ms",
                  PageSnapshotException.Reason.FLUSH_TIMEOUT);

            if (thread.hasPendingPagesOfDatabase(database)) {
              // NO COMMIT CAN CAUSE THIS ANY MORE, SO THE ONLY REMAINING FEEDER IS INDEX COMPACTION, WHICH SCHEDULES
              // ASYNCHRONOUS WRITES WITHOUT GOING THROUGH publishPages. THOSE ARE HARMLESS FOR CONSISTENCY - THEY GO
              // TO PAGES BEYOND THE t0 PAGE COUNT OF A FILE BEING BUILT, AND ANY THAT DID NOT WOULD BE SHADOWED LIKE
              // ANY OTHER WRITE - BUT THEY DO MEAN THE PIPELINE IS NOT PROVABLY EMPTY, SO SAY SO RATHER THAN LET AN
              // OPERATOR INFER IT FROM BEHAVIOUR
              totalSnapshotBarriersInexact.incrementAndGet();
              LogManager.instance().log(this, Level.FINE,
                  "Snapshot of database '%s': the flush pipeline still holds pages no committer can have queued (index compaction), so the snapshot point may be slightly behind the last committed transaction",
                  null, database.getName());
            }

            // LIST THE FILES AND PUBLISH THE WINDOW AS ONE ATOMIC STEP AGAINST FileManager.dropFile. Without this a
            // file dropped in the gap - which index compaction can do at any moment, taking no database lock - would
            // be physically deleted, because deferFileDrop only protects windows that are already published, and the
            // snapshot would then be holding a closed channel
            return database.getFileManager().executeWithFileSetLocked(
                () -> registerSnapshot(buildSnapshot(database, spillDirectory, spillVolumeUsableSpace)));
          } finally {
            unlock();
          }
        } finally {
          applyLock.writeLock().unlock();
        }
      } catch (final PageSnapshotException e) {
        // #6132: A BARRIER THAT NEVER PUBLISHES A WINDOW IS INVISIBLE TO EVERY OTHER SNAPSHOT COUNTER - THEY ALL
        // REQUIRE A WINDOW TO EXIST - AND ITS CONSUMER FALLS BACK TO SUSPEND-AND-FREEZE AND STILL COMPLETES, SO THE
        // BACKUP THAT NOW THROTTLES EVERY WRITER ON THE SERVER REPORTS SUCCESS. COUNTED HERE, SPLIT BY THE STEP THAT
        // GAVE UP
        recordSnapshotBarrierFailure(e.getReason());
        throw e;
      } catch (final IOException | InterruptedException | RuntimeException e) {
        recordSnapshotBarrierFailure(PageSnapshotException.Reason.OTHER);
        throw e;
      } finally {
        if (suspended)
          thread.setSuspended(database, false);
        // #6125: TIMED EVEN WHEN THE BARRIER THROWS - A BARRIER THAT FAILS SLOWLY IS EXACTLY AS MUCH OF A STALL AS
        // ONE THAT SUCCEEDS SLOWLY, AND ITS CONSUMER STILL FALLS BACK TO THE PATH THAT THROTTLES WRITERS
        recordSnapshotBarrier(System.currentTimeMillis() - beginTime);
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

  /**
   * @param spillDirectory          resolved by {@link #snapshotSpillDirectory}, and the usable space on it read,
   *                                BEFORE the barrier took its locks: both are unbounded blocking filesystem calls
   *                                on a volume the operator may have pointed elsewhere (#6125).
   * @param spillVolumeUsableSpace  bytes still usable there at that moment, {@code <= 0} when it could not be read.
   */
  private PageSnapshot buildSnapshot(final DatabaseInternal database, final File spillDirectory,
      final long spillVolumeUsableSpace) throws IOException {
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

      // The page count at t0 is what defines which pages the snapshot covers, so unlike SnapshotFile.lastModified()
      // it CANNOT be made lazy: reading it later would let a page appended after t0 into the snapshot un-shadowed.
      // It is no longer a channel.size() SYSCALL, though, which is a different question and the one #6132 asked:
      // #6126 waved the fstat through as "definitionally part of t0", true about WHEN it must be read and not about
      // what it has to cost. Dozens to hundreds of them - one per bucket, index and compacted sub-index, each with a
      // channelLock acquisition - ran under the JVM-wide lock the rest of this barrier works to keep short, and on a
      // stalled filesystem not one of them was bounded by SNAPSHOT_BARRIER_MAX_MILLIS the way every other step is.
      // PaginatedComponentFile now keeps the count itself; see its `totalPages` field for why it is exact, and
      // Issue6132SnapshotBarrierFollowupsTest for the assertion that it agrees with the filesystem at t0.
      files.add(new PageSnapshot.SnapshotFile(paginated.getFileId(), paginated, paginated.getPageSize(),
          (int) paginated.getTotalPages(), paginated.getFileName()));
    }

    final ContextConfiguration configuration = database.getConfiguration();
    final long maxRAM = configuration.getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM) * 1024 * 1024;
    final File spillFile = new File(spillDirectory,
        "snapshot-" + snapshotCounter.incrementAndGet() + "." + PageSnapshot.SHADOW_FILE_EXT);
    final long maxSize = snapshotMaxShadowSize(configuration, files, spillVolumeUsableSpace);

    return new PageSnapshot(database, this, database.getTransactionManager().getLastTransactionId(), files,
        new PageShadow(spillFile, maxRAM, maxSize));
  }

  /**
   * Where this window's shadow spills once its RAM budget is exhausted: {@code arcadedb.pageSnapshotSpillPath} when
   * set, the database directory otherwise (#6125). A shadow can grow to the size of the database, so on a volume
   * sized for the data alone it competes for space with the very files it is protecting.
   * <p>
   * Called from OUTSIDE the barrier's locks on purpose: {@code isDirectory} and {@code mkdirs} are blocking
   * filesystem calls that no deadline can bound, and the whole point of this setting is to name a volume that is not
   * the database's own - plausibly a network-attached one.
   */
  private File snapshotSpillDirectory(final DatabaseInternal database) {
    final String configured = database.getConfiguration().getValueAsString(GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH);
    if (configured == null || configured.isBlank())
      return new File(database.getDatabasePath());

    final File directory = new File(configured.trim());
    // THE isDirectory RE-CHECK IS NOT REDUNDANT: TWO WINDOWS OPENING AT ONCE AGAINST A NOT-YET-EXISTING DIRECTORY
    // BOTH CALL mkdirs, AND THE LOSER GETS false FOR A DIRECTORY THAT NOW EXISTS. WITHOUT IT THAT WINDOW WOULD
    // SPILL INTO THE DATABASE DIRECTORY INSTEAD, WHICH IS THE ONE THING THIS SETTING EXISTS TO AVOID
    if (!directory.isDirectory() && !directory.mkdirs() && !directory.isDirectory()) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot use '%s' as the snapshot shadow spill directory (%s): falling back to the database directory", null,
          directory, GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.getKey());
      return new File(database.getDatabasePath());
    }
    return directory;
  }

  /**
   * The cap on this window's shadow, in bytes (#6125).
   * <p>
   * A positive {@code arcadedb.pageSnapshotMaxSize} is an absolute number of MB and 0 means uncapped, both unchanged.
   * ANY negative value - -1 is merely the spelling the default and the documentation use - sizes it here instead,
   * from the two quantities that actually bound the shadow:
   * <ul>
   * <li>the t0 size of the page files, which the shadow provably cannot exceed - it holds ONE pre-image per page that
   * existed at t0, and pages appended after t0 need none (challenge C7);</li>
   * <li>half the space still usable on the spill volume, so a window can never be the reason the disk fills - but
   * never LESS than {@code arcadedb.pageSnapshotMaxRAM}, because that half of the budget never touches the disk at
   * all (#6132). Below it, a shadow that would have lived entirely in RAM was refused over free space it was never
   * going to use.</li>
   * </ul>
   * The flat 1 GB default this replaces was measured to be undersized for what it protects: on a 128 MB database
   * under a flat-out writer the shadow reached 100% of the database size, so ANY fixed number is simply the database
   * size above which backups silently start falling back to throttling the writers.
   * <p>
   * Pure arithmetic, deliberately: the free space it works from was read before the barrier took its locks, because
   * {@code getUsableSpace()} is a blocking filesystem call and this runs inside them. Package-private for the same
   * reason - being pure, the edge cases around {@code PageShadow}'s "0 means uncapped" sentinel are worth asserting
   * directly rather than through a volume a test cannot fill.
   *
   * @param spillVolumeUsableSpace bytes usable on the spill volume, {@code <= 0} when it could not be read - which
   *                               {@code File.getUsableSpace()} also answers for a path it cannot interrogate, so it
   *                               is not an error signal and must not be read as "no room"
   *
   * @return the cap in bytes, {@code 0} meaning uncapped - which this only ever returns for an explicitly configured
   *     {@code 0} or a t0 page set that is empty, never as a rounding artifact
   */
  static long snapshotMaxShadowSize(final ContextConfiguration configuration,
      final List<PageSnapshot.SnapshotFile> files, final long spillVolumeUsableSpace) {
    final long configured = configuration.getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE);
    if (configured >= 0)
      return configured * 1024 * 1024;

    long databaseSize = 0;
    for (final PageSnapshot.SnapshotFile file : files)
      databaseSize += file.size();

    // NOTHING TO CAP: NO PAGE EXISTED AT t0, SO NO PRE-IMAGE CAN EVER BE NEEDED AND THE VALUE IS MOOT
    if (databaseSize == 0)
      return 0;

    // WITH NO USABLE READING, FALL BACK TO THE PROVABLE CEILING ALONE RATHER THAN TO NO CAP AT ALL - A 0 CAP MEANS
    // "UNCAPPED" TO THE SHADOW. File.getUsableSpace() ALSO ANSWERS 0 FOR A PATH IT CANNOT INTERROGATE, WHICH IS WHY
    // THIS IS NOT READ AS "NO ROOM"
    if (spillVolumeUsableSpace <= 0)
      return databaseSize;

    // THE RAM BUDGET NEEDS NO DISK, SO IT IS NOT BOUNDED BY DISK (#6132, item 3). The cap covers RAM PLUS spill, and
    // sizing it from the free space alone put it BELOW arcadedb.pageSnapshotMaxRAM whenever the spill volume had less
    // than twice the RAM budget free - 20 MB against a 64 MB default on a volume with 40 MB free. A shadow that would
    // have lived entirely in RAM and never written a byte to that volume was then invalidated at 20 MB, and its
    // backup fell back to throttling every writer because of space it was never going to use, in exactly the case
    // pageSnapshotSpillPath exists for: an operator who pointed it at a small volume.
    final long ramBudget = configuration.getValueAsLong(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM) * 1024 * 1024;

    // CLAMPED TO AT LEAST ONE BYTE: THE DIVISION IS INTEGER, SO A VOLUME DOWN TO ITS LAST BYTE COMPUTES 1/2 == 0 AND
    // WOULD HAND THE SHADOW THE SAME "UNCAPPED" SENTINEL THE BRANCH ABOVE EXISTS TO AVOID - INVERTING THIS CAP
    // PRECISELY IN THE DISK-ALMOST-FULL CASE IT IS FOR. A ONE-BYTE CAP INSTEAD BREACHES ON THE FIRST CAPTURE, WHICH
    // IS THE CORRECT ANSWER: THE WINDOW IS ABANDONED AND ITS CONSUMER FALLS BACK.
    // The outer min() is unchanged and still the provable ceiling: whichever of the two budgets wins, no shadow can
    // ever need more than one pre-image per page that existed at t0.
    return Math.max(1L,
        Math.min(databaseSize, Math.max(ramBudget, spillVolumeUsableSpace / SNAPSHOT_MAX_SIZE_FREE_SPACE_DIVISOR)));
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
   * the committing threads OF THE SUSPENDED DATABASES are throttled (issue #6200 - before it, the flush thread stopped
   * draining its queue altogether, which throttled the committers of every open database), so this is the direct
   * measure of how much a long suspension is costing writers.
   *
   * @return 0 when no suspension is in progress, or when the page manager is closed.
   */
  public long getDeferredRAMBytes() {
    final PageManagerFlushThread thread = flushThread;
    return thread != null ? thread.deferredRAMBytes.get() : 0L;
  }

  /**
   * How many times a writer had to WAIT for room in the bounded flush queue instead of being admitted straight away
   * (issue #6259) - that is, how often the disk has not kept up with the write rate.
   * <p>
   * The companion of {@code pageFlushQueueLength} and the number that queue length cannot give: the length is an
   * instant, sampled between two bursts as easily as during one, while this is cumulative evidence that commits were
   * actually held. Rising while the length reads low means the pipeline is saturating in bursts.
   *
   * @return 0 when nothing has ever been throttled, or when the page manager is closed.
   */
  public long getFlushQueueWaits() {
    final PageManagerFlushThread thread = flushThread;
    return thread != null ? thread.queueSlotWaits.get() : 0L;
  }

  /**
   * The same backlog as {@link #getDeferredRAMBytes}, for a single database (issue #6200): the number that says WHICH
   * suspension is costing the heap, which the JVM-wide total cannot.
   *
   * @return 0 when that database has nothing deferred, or when the page manager is closed.
   */
  public long getDeferredRAMBytesOf(final BasicDatabase database) {
    final PageManagerFlushThread thread = flushThread;
    return thread != null ? thread.getDeferredRAMBytesOf(database) : 0L;
  }

  /**
   * Holds the caller while the deferred backlog of ITS database is over the cap (issue #4728). A no-op unless that
   * database is currently suspended - see {@link PageManagerFlushThread#awaitDeferredBacklogUnderCap}, and note that
   * this must never be called with the page-manager lock held (issue #6200).
   * <p>
   * ASSUMPTION, shared with {@code PagesToFlush} and therefore with the whole flush pipeline: a batch carries the
   * pages of ONE database, so the first page names it. Every caller satisfies it - a publication is one
   * transaction's pages, and the direct {@code writePages} callers (LSM compaction, bloom filter, sparse segment
   * builder) write one component of one database - and a mixed batch would already mis-key the deferral, the
   * per-database pending count and the suspension check long before it reached here. Do not introduce one.
   */
  private void awaitDeferredBacklogUnderCap(final List<MutablePage> pages) throws InterruptedException {
    if (flushThread == null || pages == null || pages.isEmpty())
      return;
    flushThread.awaitDeferredBacklogUnderCap(pages.getFirst().getPageId().getDatabase());
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

  /**
   * Counts a chunk-chain read that met a moved page and completed anyway (#6217): a page the read walked was at a
   * newer version by the time the read validated it, and none of the bytes this record owns on that page had
   * changed, so the assembled record is the committed one and the read returns it. Before #6217 that read failed and
   * was retried, and after {@code arcadedb.txRetries} attempts it raised a {@link ConcurrentModificationException}
   * on a record no writer had touched - continuation chunks of unrelated records share pages by design.
   * <p>
   * This is the read-path twin of {@link #incrementTxPageSlotMerges()}: both count a false conflict that did NOT
   * happen, so a rise here is contention being absorbed rather than a problem. Watch it next to
   * {@link PPageManagerStats#chunkChainReadRetries}, which counts the reads that had to restart because the record
   * really had moved.
   * <p>
   * COUNTING SEMANTICS: one per read ATTEMPT that completed after comparing, however many of its pages had moved.
   */
  public void incrementChunkChainReadRevalidations() {
    totalChunkChainReadRevalidations.incrementAndGet();
  }

  /**
   * Counts a chunk-chain read ATTEMPT thrown away because the record itself changed under it (see
   * {@link #incrementChunkChainReadRevalidations()} for the case that no longer counts as one). The last attempt of a
   * read that gives up contributes here too, right before it raises {@link ConcurrentModificationException}, so this
   * is "restarts", not "failed reads".
   * <p>
   * CONTENTION, and nothing else, since #6258: a chain that could not be WALKED used to be counted here as well and
   * retried like a busy record, which made this counter mean two unrelated things at once and put a corrupted record
   * on the contention chart. Such a chain now raises {@code BrokenChunkChainException} where it is found and
   * contributes nothing here. Restarts also stop as soon as another attempt provably cannot read anything else, so a
   * rise here is writers meeting readers rather than a budget being spent on a verdict already settled.
   */
  public void incrementChunkChainReadRetries() {
    totalChunkChainReadRetries.incrementAndGet();
  }

  public void checkPageVersion(final MutablePage page, final boolean isNew) throws IOException {
    final PageId pageId = page.getPageId();

    final FileManager fileManager = ((DatabaseInternal) pageId.getDatabase()).getFileManager();

    if (!fileManager.existsFile(pageId.getFileId()))
      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + ". The file with id " + pageId.getFileId()
              + " does not exist anymore. Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");

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
                  page, file.getFileName(), mostRecentPageVersion, Thread.currentThread().threadId());

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
              + "). Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");
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
    // BOTH WAITS ARE OUTSIDE THE LOCK, AND THAT IS THE ENTIRE POINT (issues #6200 and #6259). Publication holds the
    // JVM-wide page-manager lock across BOTH of its halves - the page write and the flush enqueue - because the
    // snapshot t0 barrier (#6075/#6125) rests on exactly that, so the enqueue cannot move out; what can, and must, is
    // everything the enqueue might WAIT for. Served one line lower, inside lock(), each of these would be charged to
    // every committer of every database in the process:
    //   - #6200: while a database is suspended its dirty pages pile up in RAM, and the cap that bounds that backlog
    //     is served by holding ITS committers.
    //   - #6259: when the bounded flush queue fills - a write burst, a slow volume, an fsync spike - the committer
    //     waits for room HERE, and reaches its offer inside the lock with a slot already reserved for it.
    boolean flushSlotReserved = false;
    if (asyncFlush) {
      awaitDeferredBacklogUnderCap(pagesToWrite);
      flushSlotReserved = reserveFlushQueueSlot(pagesToWrite);
    }

    boolean handedOver = false;
    lock();
    try {
      // Write pages (and put in readCache) BEFORE updating pageCount, otherwise concurrent
      // transactions can observe pageCount > readCache state and treat the new page as a
      // pre-existing empty page (sparse-file semantics), allowing two records' chunk chains
      // to land on the same physical slot.
      handedOver = true;
      writePagesNoBackpressure(pagesToWrite, asyncFlush, flushSlotReserved);

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
      if (flushSlotReserved && !handedOver)
        // Not a live path TODAY - nothing between the reservation and the hand-off below can throw, lock() being a
        // plain ReentrantLock - but it is not a comment about today: this covers ANY future statement inserted
        // between them that throws, which is exactly the refactor that would otherwise strand a reservation. And a
        // stranded one is silent and permanent: the flush pipeline is one slot smaller for the life of the process,
        // with nothing in a running system ever pointing at it (review of #6269).
        releaseFlushQueueSlot(pagesToWrite);
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
            null, page, fileManager.getFile(pageId.getFileId()).getFileName(), Thread.currentThread().threadId());
      }

      throw new ConcurrentModificationException(
          "Concurrent modification on page " + pageId + " in file '" + fileManager.getFile(pageId.getFileId()).getFileName()
              + "' (current v." + page.getVersion() + " <> database v." + mostRecentPageVersion
              + "). Please retry the operation (threadId=" + Thread.currentThread().threadId() + ")");
    }

    page.incrementVersion();
    page.updateMetadata();

    LogManager.instance()
        .log(this, Level.FINE, "Updated page %s (size=%d records=%d threadId=%d)", null, page, page.getPhysicalSize(),
            page.readShort(0), Thread.currentThread().threadId());

    return page;
  }

  public void overwritePage(final MutablePage page) throws IOException {
    readCache.remove(page.pageId);

    flushPage(page);

    LogManager.instance().log(this, Level.FINE, "Overwritten page %s (size=%d threadId=%d)", null, page, page.getPhysicalSize(),
        Thread.currentThread().threadId());
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
    MutablePage mostRecent = pending.getFirst();
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
    stats.pageFlushQueueMaxPerDatabase = flushThread != null ? flushThread.maxSlotsUsedByAnyDatabase() : 0;
    stats.cacheHits = cacheHits.get();
    stats.cacheMiss = cacheMiss.get();
    stats.concurrentModificationExceptions = totalConcurrentModificationExceptions.get();
    stats.edgeAppendMerges = totalEdgeAppendMerges.get();
    stats.txPageSlotMerges = totalTxPageSlotMerges.get();
    stats.mergesDeclinedByCoverage = totalMergesDeclinedByCoverage.get();
    stats.chunkChainReadRevalidations = totalChunkChainReadRevalidations.get();
    stats.chunkChainReadRetries = totalChunkChainReadRetries.get();
    stats.evictionRuns = evictionRuns.get();
    stats.pagesEvicted = pagesEvicted.get();
    stats.snapshotWindowsOpened = totalSnapshotWindowsOpened.get();
    stats.snapshotWindowsInvalidated = totalSnapshotWindowsInvalidated.get();
    stats.snapshotWindowsOverflowed = totalSnapshotWindowsOverflowed.get();
    stats.snapshotWindowsFailed = totalSnapshotWindowsFailed.get();
    stats.snapshotPreImagesCaptured = totalSnapshotPreImagesCaptured.get();
    stats.snapshotBarriers = totalSnapshotBarriers.get();
    stats.snapshotBarrierMillis = totalSnapshotBarrierMillis.get();
    stats.snapshotBarrierMaxMillis = maxSnapshotBarrierMillis.get();
    stats.snapshotBarriersInexact = totalSnapshotBarriersInexact.get();
    stats.snapshotBarriersFailed = totalSnapshotBarriersFailed.get();
    stats.snapshotBarriersFailedSuspend = totalSnapshotBarriersFailedSuspend.get();
    stats.snapshotBarriersFailedFlush = totalSnapshotBarriersFailedFlush.get();
    stats.deferredRAMBytes = getDeferredRAMBytes();
    stats.flushQueueWaits = getFlushQueueWaits();
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

  /**
   * Counts a window that lost its point in time: the fallback-path early warning. The total is kept alongside the
   * per-reason split (#6125) rather than derived from it, so a future invalidation reason that forgets to extend the
   * split still shows up in the total an operator alerts on.
   */
  void incrementSnapshotWindowsInvalidated(final PageSnapshot.STATUS reason) {
    totalSnapshotWindowsInvalidated.incrementAndGet();
    if (reason == PageSnapshot.STATUS.OVERFLOWED)
      totalSnapshotWindowsOverflowed.incrementAndGet();
    else if (reason == PageSnapshot.STATUS.FAILED)
      totalSnapshotWindowsFailed.incrementAndGet();
  }

  public void removePageFromCache(final PageId pageId) {
    final CachedPage page = readCache.remove(pageId);
    if (page != null)
      totalReadCacheRAM.addAndGet(-1L * page.getPhysicalSize());
  }

  public void writePages(final List<MutablePage> updatedPages, final boolean asyncFlush) throws IOException, InterruptedException {
    // Entry point of the asynchronous writers that do NOT go through publishPages (LSM index compaction): they hold
    // no page-manager lock, so the deferred-backlog cap of #4728 (issue #6200) and the flush-queue admission control
    // (issue #6259) are both served right here, in the same order and for the same reasons as in publishPages.
    boolean flushSlotReserved = false;
    if (asyncFlush) {
      awaitDeferredBacklogUnderCap(updatedPages);
      flushSlotReserved = reserveFlushQueueSlot(updatedPages);
    }
    writePagesNoBackpressure(updatedPages, asyncFlush, flushSlotReserved);
  }

  /**
   * {@link #writePages} without the two waits that must be served before the page-manager lock, for the caller that
   * has already served them: {@link #publishPages} (issues #6200 and #6259).
   *
   * @param flushSlotReserved a flush-pipeline slot taken by the caller, on the pages' own database's budget, before it
   *                          took the lock. This method owns it from here on - handing it to the enqueue, or giving it
   *                          back on any path that does not reach one - so the caller must not release it again.
   */
  private void writePagesNoBackpressure(final List<MutablePage> updatedPages, final boolean asyncFlush,
      final boolean flushSlotReserved) throws IOException, InterruptedException {
    boolean handedOver = false;
    try {
      if (asyncFlush) {
        for (final MutablePage page : updatedPages)
          putPageInReadCache(new CachedPage(page, true));
        handedOver = true;
        flushThread.scheduleFlushOfPages(updatedPages,
            flushSlotReserved ? updatedPages.getFirst().getPageId().getDatabase() : null);
      } else {
        // SYNCHRONOUS FLUSH
        for (final MutablePage page : updatedPages) {
          flushPage(page);
          // ADD THE PAGE IN TO READ CACHE. FROM THIS POINT THE PAGE IS NEVER MODIFIED, SO IT CAN BE CACHED
          putPageInReadCache(new CachedPage(page, false));
        }
      }
    } finally {
      if (flushSlotReserved && !handedOver)
        // The read-cache loop above threw, or anything a later refactor puts before the hand-off does: the enqueue
        // was never reached, so the slot goes back rather than being held by a publication that is not going to
        // happen. Unlike its twin in publishPages this one IS reachable today - putPageInReadCache does I/O.
        releaseFlushQueueSlot(updatedPages);
    }
  }

  /**
   * Reserves the flush-queue slot the enqueue inside the page-manager lock will need, waiting HERE - outside that
   * lock - for as long as the queue is full (issue #6259). See
   * {@link PageManagerFlushThread#reserveQueueSlot(com.arcadedb.database.BasicDatabase)} for why the wait cannot live
   * where the enqueue does, and why the budget it draws on is the batch's own database's (issue #6281).
   *
   * @return {@code false} when there is nothing to enqueue, or when the flush thread is shutting down: either way no
   *     reservation is held and none has to be released.
   */
  private boolean reserveFlushQueueSlot(final List<MutablePage> pages) throws InterruptedException {
    final PageManagerFlushThread thread = flushThread;
    if (thread == null || pages == null || pages.isEmpty())
      return false;
    return thread.reserveQueueSlot(pages.getFirst().getPageId().getDatabase());
  }

  /**
   * Gives back a reservation that never reached an enqueue. Re-reads the field rather than capturing the thread that
   * granted it: the only way to observe a different one here is a full page-manager shutdown and restart in the
   * window, which needs every database in the JVM to have been closed mid-publication - and would have thrown at the
   * enqueue long before reaching this.
   */
  private void releaseFlushQueueSlot(final List<MutablePage> pages) {
    final PageManagerFlushThread thread = flushThread;
    if (thread != null && pages != null && !pages.isEmpty())
      thread.releaseQueueReservation(pages.getFirst().getPageId().getDatabase());
  }

  protected void flushPage(final MutablePage page) throws IOException {
    final DatabaseInternal database = (DatabaseInternal) page.getPageId().getDatabase();

    if (!database.isOpen() || database.isFencedForRecovery()) {
      // A fenced database (#5053) is a "close and reopen to run recovery" state: isOpen() is still true, but
      // every page still queued here will be replayed from the WAL on the mandatory reopen anyway, so writing
      // it now through this async pipeline is moot - handled exactly like the closed/dropped case below.
      LogManager.instance()
          .log(this, Level.SEVERE, "Cannot flush page %s because the database is closed or fenced for recovery", page);
      // The page will never be flushed and its content is irrelevant (the database is closed, being dropped,
      // or fenced): release its WAL ack so the close-time ack gate (#4928) is not tripped by a pending count
      // that can never be satisfied - flushPage() is never called again for this page once the database is
      // closed, so without this the page's WALFile.pagesToFlush counter is stranded above zero forever and
      // TransactionManager.close()'s retry loop burns its whole budget waiting on it (issue #6440). Mirrors
      // the "file dropped" branch a few lines below and PageManagerFlushThread.removePagesOfFileFromBatch.
      // takeWALFile makes the release exactly-once against a racing caller of the same page.
      final WALFile walFile = page.takeWALFile();
      if (walFile != null)
        walFile.notifyPageFlushed();
      return;
    }

    final FileManager fileManager = database.getFileManager();
    final int fileId = page.pageId.getFileId();

    if (fileManager.existsFile(fileId)) {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getFile(fileId);
      if (!file.isOpen())
        throw new DatabaseMetadataException("Cannot flush pages on disk because file '" + file.getFileName() + "' is closed");

      LogManager.instance()
          .log(this, Level.FINE, "Flushing page %s to disk (threadId=%d)...", null, page, Thread.currentThread().threadId());

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
        // the metadata updates - but the WAL ack must still happen (issue #6440): the write above already
        // succeeded, so skipping just this call (as opposed to the metadata update) would strand the page's
        // WALFile.pagesToFlush counter above zero forever even though its content is safely on disk.
        final WALFile walFile = page.takeWALFile();
        if (walFile != null)
          walFile.notifyPageFlushed();
      } catch (final DatabaseOperationException e) {
        // The database was fenced for recovery (#5053) concurrently after the isOpen()/isFencedForRecovery()
        // check above - the same race the DatabaseIsClosedException catch above handles for a close. Only
        // swallow it for that reason: getSchema() throws this broad exception type for several unrelated
        // conditions too, and a genuine one must still propagate to the caller.
        if (!database.isFencedForRecovery())
          throw e;
        // The page data has already been written to disk; recovery on the next open replays it from the WAL
        // regardless, so the metadata update is skippable exactly like the closed-database case above.
        final WALFile walFile = page.takeWALFile();
        if (walFile != null)
          walFile.notifyPageFlushed();
      }

    } else {
      LogManager.instance()
          .log(this, Level.FINE, "Cannot flush page %s because the file has been dropped (threadId=%d)...", null, page,
              Thread.currentThread().threadId());
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

      LogManager.instance().log(this, Level.FINE, "Loaded page %s (threadId=%d)", null, page, Thread.currentThread().threadId());
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
            null, ramToFree, totalRAM, maxRAM, Thread.currentThread().threadId());

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
            FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().threadId());

    if (newTotalRAM > maxRAM)
      LogManager.instance().log(this, Level.WARNING, "Cannot free pages in RAM (current=%s > max=%s threadId=%d)", null,
          FileUtils.getSizeAsString(newTotalRAM), FileUtils.getSizeAsString(maxRAM), Thread.currentThread().threadId());

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

  /**
   * <b>A read answering is not the same fact as the file holding the page, and no caller can tell the two apart</b> (issue
   * #6351, note). The read cache is consulted FIRST and the flush queue second ({@link #loadPage}), so a resident image answers
   * for a page whose bytes may not be on disk yet - which is the whole point of both, and correct - and only a miss in both ever
   * reaches {@code file.getTotalPages()}. With {@code createIfNotExists} the read invents a zero page rather than refusing at
   * all.
   * <p>
   * So a guard must never infer "this page exists" from a read having succeeded. It has to read the BYTES and say what makes
   * them legal content, the way {@link Dictionary#reload()} does with the per-page header every dictionary page carries. Making
   * residency prove it corresponds to the file would be a large change on the hottest path in the engine, for a hazard nobody
   * has demonstrated; this note is here so the next person who finds a guard behaving oddly does not have to rediscover why.
   */
  private CachedPage getCachedPage(final PageId pageId, final int pageSize, final boolean isNew, final boolean createIfNotExists)
      throws IOException {
    // #6282, item 4: the ONE place a logical page read can be made to fail on demand. See PageReadFaultInjector for
    // why it is here and not at the physical read, and for what it costs when nothing is installed (a null field).
    final PageReadFaultInjector faultInjector = pageReadFaultInjector;
    if (faultInjector != null)
      faultInjector.onPageRead(pageId);

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
          "Page id '" + pageId + "' does not exist (threadId=" + Thread.currentThread().threadId() + ")");

    return page;
  }
}
