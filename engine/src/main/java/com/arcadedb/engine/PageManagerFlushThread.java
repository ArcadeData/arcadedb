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
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Flushes pages to disk asynchronously.
 * <p>
 * A {@link ConcurrentHashMap} ({@code pageIndex}) provides O(1) lookup for pages
 * waiting in the flush queue or currently being flushed.  This replaces the previous
 * O(n) {@code queue.toArray()} scan that allocated a new array on every call and was
 * a major bottleneck under high-throughput ingestion.
 */
public class PageManagerFlushThread extends Thread {
  private final        PageManager                                              pageManager;
  public final         ArrayBlockingQueue<PagesToFlush>                         queue;
  private final        String                                                   logContext;
  private volatile     boolean                                                  running             = true;
  // Per-database suspension REFCOUNT (issue #5068): flushing is suspended while the count is > 0. Backup,
  // verify and HA snapshot serving can overlap on the same database, and each caller must own its whole
  // window: the count is bumped per suspender and the resume (deferred-batch flush + flag clear) runs only
  // when the LAST suspender exits. Guarded by suspendLock(database) for every transition.
  private final        ConcurrentHashMap<Database, Integer>                     suspended           = new ConcurrentHashMap<>();
  // Databases whose LAST suspender is currently resuming (synchronously flushing the deferred batches).
  // A new suspender arriving in that window waits on suspendLock(database) until the resume completes, so
  // its suspension window can never overlap the resume's page writes (issue #5068).
  private final        Set<Database>                                            resumingDatabases   = ConcurrentHashMap.newKeySet();
  private final static PagesToFlush                                             SHUTDOWN_THREAD     = new PagesToFlush(null);
  /** Fallback wake-up interval of {@link #waitAllPagesOfDatabaseAreFlushed(Database)} - see {@link #flushWaitPollMillis}. */
  private final static long                                                     DEFAULT_FLUSH_WAIT_POLL_MILLIS = 10;
  /**
   * Fallback wake-up interval of the RESIDUAL drain, {@link #waitPendingPagesOfDatabaseUntil}. Deliberately shorter
   * than the bulk one: that wait runs with the JVM-wide page-manager lock held, so its worst case - the interval
   * itself, reached only if a drain notification is ever lost - is a stall of every committer in the process.
   */
  private final static long                                                     RESIDUAL_DRAIN_WAIT_POLL_MILLIS = 1;
  /**
   * Fallback wake-up interval of {@link #awaitDeferredBacklogUnderCap}. The release of the cap is signalled, so this
   * only bounds how often a parked committer re-reads the conditions that are NOT signalled: its database ceasing to
   * be suspended, and the shutdown flag.
   */
  private final static long                                                     DEFERRED_BACKLOG_WAIT_POLL_MILLIS = 100;
  /** How long a committer may be held by the deferred-backlog cap before saying so, once, at WARNING. */
  private final static long                                                     DEFERRED_BACKLOG_WARN_MILLIS      = 10_000;
  /** Default of {@link #queueSlotWaitPollMillis}. */
  private final static long                                                     DEFAULT_QUEUE_SLOT_WAIT_POLL_MILLIS = 100;
  /** How long a committer may be held waiting for a flush-queue slot before saying so, once, at WARNING. */
  private final static long                                                     QUEUE_SLOT_WARN_MILLIS              = 10_000;
  /**
   * Fallback wake-up interval of {@link #waitAllPagesOfDatabaseAreFlushed(Database)}, per instance so the
   * regression test of issue #6199 can stretch it far enough to prove the wait is released by the drain
   * notification of {@link FlushPageIndex#awaitDrain} and not by the interval elapsing.
   * <p>
   * Since #6199 this is NOT the latency of the wait: a drained pipeline wakes the waiter immediately. It only
   * bounds how often the no-progress timeout below is re-evaluated, and how long a lost notification would cost.
   */
  long                                                                          flushWaitPollMillis = DEFAULT_FLUSH_WAIT_POLL_MILLIS;
  /**
   * Fallback wake-up interval of {@link #reserveQueueSlot}. Every slot the flush thread frees is signalled, so this
   * only bounds how often a parked committer re-reads the one condition that is NOT signalled - the shutdown flag -
   * and how long a lost notification would cost.
   * <p>
   * Per instance, not static, for the same reason {@link #flushWaitPollMillis} is: the regression test of #6259
   * stretches it far enough to prove the wait is released by the poll's signal rather than by the interval elapsing,
   * and a static would leak that value into every other test class sharing the JVM.
   */
  long                                                                          queueSlotWaitPollMillis = DEFAULT_QUEUE_SLOT_WAIT_POLL_MILLIS;
  // Package-private so the white-box regression test for issue #5068 can fabricate an in-flight batch and
  // deterministically exercise an interrupt during waitForCurrentFlushToComplete.
  final                AtomicReference<PagesToFlush>                            nextPagesToFlush    = new AtomicReference<>();
  private final        ConcurrentHashMap<Database, ConcurrentLinkedQueue<PagesToFlush>> deferredByDatabase = new ConcurrentHashMap<>();
  // Per-database lock serializing the suspend-flag check + defer in flushPagesFromQueueToDisk against the
  // flag-clear + deferred-detach in setSuspended(false). Without it a batch could be deferred AFTER the
  // unsuspend already drained the deferred map, leaving it stuck in deferredByDatabase / pageIndex forever.
  // Also the monitor new suspenders wait on while a resume is in flight (see resumingDatabases).
  private final        ConcurrentHashMap<Database, Object>                      suspendLocks        = new ConcurrentHashMap<>();
  // Per-database mutex serializing detachPendingPages against resumeFlushing's Phase 1. Phase 1 detaches the deferred
  // batches and writes them from the resuming thread, outside both the queue and nextPagesToFlush, so without this
  // lock a replay could take the pipeline for empty and have its page written over by a deferred copy landing later.
  // Lock ordering is always this monitor BEFORE any batch.pages monitor; the flush thread takes batch.pages alone.
  private final        ConcurrentHashMap<Database, Object>                      replayDrainLocks    = new ConcurrentHashMap<>();

  /**
   * O(1) index: pageId → most recent MutablePage in the flush queue or currently being flushed, plus the O(1)
   * per-database count of those entries the drains poll (issue #6133 - see {@link FlushPageIndex}).
   * <p>
   * Package-private (instead of private) so the white-box regression tests for issues #4544 and #6133 can set up
   * and assert on entries directly. Every mutation must go through its methods: the pending-page count is
   * maintained there and nowhere else.
   */
  final                FlushPageIndex                         pageIndex = new FlushPageIndex();

  /**
   * Maximum bytes of dirty pages that may sit deferred (in {@link #deferredByDatabase}) while flushing is
   * suspended, before the committing threads OF THE SUSPENDED DATABASES are throttled in
   * {@link #awaitDeferredBacklogUnderCap}. {@code 0} disables the cap (unbounded, pre-#4728 behavior).
   */
  private final        long                                   maxDeferredRAM;

  /** Capacity of {@link #queue}, the ceiling the admission control of {@link #reserveQueueSlot} enforces (#6259). */
  private final        int                                    queueCapacity;

  /**
   * Slots of {@link #queue} promised to a caller that has not enqueued its batch yet (issue #6259).
   * <p>
   * A reservation is what lets {@code PageManager.publishPages} wait for queue room BEFORE taking the JVM-wide
   * page-manager lock and still find the room when it enqueues one line later, inside it. Admission is granted only
   * while {@code queue.size() + reservations < queueCapacity}, so the sum never exceeds the capacity and the holder
   * of a reservation is guaranteed a free slot: its {@code offer} inside the lock cannot block.
   * <p>
   * Deliberately derived from the QUEUE'S OWN size rather than from a permit counter mirroring it (a
   * {@link java.util.concurrent.Semaphore} sized to the capacity was the obvious alternative). A permit count has to
   * be kept in step with the queue by hand at every enqueue and every poll, and a single missed release silently
   * shrinks the pipeline for the life of the process; reading the queue tells the truth by construction, so a batch
   * that reaches the queue by any other route - a white-box test, a future call site - narrows admission instead of
   * corrupting the bound. The cost is one {@code ArrayBlockingQueue.size()} per publication, which takes the same
   * internal lock the {@code offer} on the next line takes anyway.
   * <p>
   * Package-private so the regression test of #6259 can assert the one thing inspection cannot: that every path out
   * of a publication gives its reservation back. A leaked one is invisible and permanent - it shrinks the pipeline
   * by a slot for the life of the process.
   */
  final                AtomicInteger                          queueReservations = new AtomicInteger();

  /**
   * How many callers are parked in {@link #reserveQueueSlot}. Read (not taken) by the flush thread on every poll, so
   * the common case - nobody waiting, the queue nowhere near full - costs one volatile read per batch instead of a
   * monitor. Same shape as the drain signal of #6199.
   */
  private final        AtomicInteger                          queueSlotWaiters  = new AtomicInteger();

  /** Monitor the callers of {@link #reserveQueueSlot} park on; notified whenever a batch leaves {@link #queue}. */
  private final        Object                                 queueSlotLock     = new Object();

  /**
   * How many times a caller had to WAIT for a flush-queue slot rather than being admitted straight away. Exposed
   * (package-private) because that is the difference between the two shapes of this backpressure the regression test
   * of #6259 has to tell apart: waiting outside the page-manager lock, or blocking inside it.
   */
  final                AtomicLong                             queueSlotWaits    = new AtomicLong();

  /**
   * Per-database count of pages that LEFT the flush pipeline: the progress signal for
   * {@link #waitAllPagesOfDatabaseAreFlushed} (#4928). Deliberately per-database and package-private (for
   * the regression test): a JVM-global signal (e.g. PageManager.totalPagesWritten) would let a busy sibling
   * database sharing this flush thread mask a wedged one forever, defeating the very timeout it feeds.
   * An entry is created on a database's FIRST bounded wait (close, rename, backup-suspend, compaction
   * shipping) and lives until {@link #removeAllPagesOfDatabase} (close/drop) - one bounded entry per open
   * database, not a leak.
   */
  final ConcurrentHashMap<BasicDatabase, AtomicLong> flushedPagesPerDatabase = new ConcurrentHashMap<>();

  /**
   * Running total of bytes currently deferred across all suspended databases. Package-private so the white-box
   * regression test for issue #4728 can assert the backlog stays bounded.
   * <p>
   * Derived, never assigned outside {@link #addDeferredRAM}: it is the sum of {@link #deferredRAMByDatabase}, kept
   * incrementally because the cap is read on the commit path and iterating a map there to re-derive it would put an
   * O(open databases) walk in front of every publication.
   */
  final                AtomicLong                             deferredRAMBytes = new AtomicLong();

  /**
   * The same total, split by database (issue #6200). The backlog is strictly per-database - only the batches of
   * SUSPENDED databases are ever deferred - so this is the number that actually explains a backlog, and the one
   * {@code arcadedb.pagemanager.deferred_ram_bytes} needs to be tagged by database (item 2 of #6087), which the
   * single JVM-wide total could not express. An entry is created with a database's first deferred batch and dropped
   * when its backlog ends - its last suspender resumed, or it was closed or dropped, both through
   * {@link #releaseResidualDeferredRAM} - so it is bounded by the number of open databases and empty whenever
   * nothing is deferred anywhere.
   */
  final ConcurrentHashMap<BasicDatabase, AtomicLong> deferredRAMByDatabase = new ConcurrentHashMap<>();

  /**
   * Monitor of the committer-side backpressure of issue #4728: waiters park here while the deferred backlog is over
   * the cap, and {@link #addDeferredRAM} notifies them when it drops back under. Bounded waits, so a suspension
   * released without freeing any RAM (nothing was deferred) still lets its committers out.
   * <p>
   * ONE monitor for every suspended database, not one per database, because the condition they are all waiting on is
   * one number: the JVM-wide total, which any database's release can move under the cap. Splitting it per database
   * would mean notifying every one of them on every release anyway - the same wake-ups through more monitors. The
   * herd is bounded by construction: the signal fires only on the DOWNWARD crossing of the cap (not per released
   * page), on a resume, on a purge and on shutdown, and every woken committer re-reads the total and either
   * proceeds or parks again.
   */
  private final Object deferredRAMLock = new Object();

  /**
   * How many times {@link #repairNegativeDeferredRAM} had to reset a negative total. Reported once (a drift of this
   * kind repeats), and package-private because the repair would otherwise HIDE the very bugs the regression tests of
   * #6200 exist to catch: a double release lands on zero after the clamp, indistinguishable from correct accounting.
   * The tests assert this stays at zero.
   */
  final AtomicLong deferredRAMDriftRepairs = new AtomicLong();

  public static class PagesToFlush {
    public final BasicDatabase     database;
    public final List<MutablePage> pages;

    public PagesToFlush(final List<MutablePage> pages) {
      // removeAllPagesOfDatabase()/removePagesOfFileFromBatch() mutate this list (clear()/it.remove()) when a
      // database or file is dropped, so it must be mutable. The hot commit path already hands us a fresh
      // ArrayList; only rare callers (e.g. compaction passing List.of(page)) supply an immutable list, which we
      // wrap here so a later drop does not throw UnsupportedOperationException - without adding an allocation to
      // the common path.
      this.pages = pages == null || pages instanceof ArrayList ? pages : new ArrayList<>(pages);
      this.database = pages == null || pages.isEmpty() ? null : pages.getFirst().pageId.getDatabase();
      // A BATCH CARRIES THE PAGES OF ONE DATABASE, and from here on the whole pipeline reads that off the FIRST
      // page: the suspension check that decides whether to defer, the deferred map's key, the per-database RAM
      // charge and the committer-side cap all key on `database` above (review of #6223). A mixed batch would not
      // throw anywhere - it would silently charge one database for another's pages and throttle the wrong
      // committers, which is the exact class of quiet accounting drift this class spent #6200 removing. Asserted
      // rather than checked: every caller satisfies it today (one transaction's pages, or one component's), and an
      // unconditional scan would put an O(batch) walk on the publication path to defend against a bug that has to
      // be introduced here first. Assertions are on in the test lanes, which is where such a bug would be written.
      assert isSingleDatabase(this.pages) : "a flush batch must carry the pages of ONE database, got " + this.pages;
    }

    private static boolean isSingleDatabase(final List<MutablePage> pages) {
      if (pages == null || pages.isEmpty())
        return true;
      final BasicDatabase first = pages.getFirst().pageId.getDatabase();
      for (int i = 1; i < pages.size(); i++)
        if (!first.equals(pages.get(i).pageId.getDatabase()))
          return false;
      return true;
    }
  }

  public PageManagerFlushThread(final PageManager pageManager, final ContextConfiguration configuration) {
    super("ArcadeDB AsyncFlush");
    // #5418: DAEMON. A non-daemon flush thread made an embedder that leaks a Database handle unable to exit
    // at all: DestroyJavaVM waits for every non-daemon thread, and this one only stops on Database.close(),
    // so the JVM hung forever (or crashed inside shutdownJVM on Windows) instead of shutting down. Durability
    // does NOT rest on the thread being non-daemon: DatabaseFactory installs a JVM shutdown hook that closes
    // every still-open database, and shutdown hooks run to completion BEFORE the JVM stops daemon threads, so
    // the pending pages are flushed exactly as they were by an explicit close. On a path where no hook runs at
    // all (Runtime.halt, SIGKILL) the pages were never guaranteed to reach the disk anyway, and the WAL replays
    // them on the next open.
    setDaemon(true);
    this.pageManager = pageManager;
    this.logContext = LogManager.instance().getContext();
    this.queueCapacity = configuration.getValueAsInteger(GlobalConfiguration.PAGE_FLUSH_QUEUE);
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    final long maxDeferredMB = configuration.getValueAsLong(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM);
    this.maxDeferredRAM = maxDeferredMB > 0 ? maxDeferredMB * 1024 * 1024 : 0;
  }

  /**
   * Enqueues a batch for the flush thread, reserving its queue slot HERE - which is why this overload exists and why
   * the engine's own callers do not use it: they reserve before taking the page-manager lock (issue #6259).
   */
  public void scheduleFlushOfPages(final List<MutablePage> pages) throws InterruptedException {
    scheduleFlushOfPages(pages, false);
  }

  /**
   * @param slotReserved whether the caller already holds a reservation from {@link #reserveQueueSlot} - which it must
   *                     have taken BEFORE the page-manager lock (issue #6259). This method releases it either way, so
   *                     a reservation is handed over here exactly once and never leaks. When it is {@code false} the
   *                     reservation is taken inline instead, and that wait is then served wherever the caller happens
   *                     to be: <b>never call this form while holding the page-manager lock</b>.
   */
  void scheduleFlushOfPages(final List<MutablePage> pages, final boolean slotReserved) throws InterruptedException {
    boolean reserved = slotReserved;
    boolean enqueued = false;
    try {
      if (pages.isEmpty())
        // AVOID INSERTING AN EMPTY LIST BECAUSE IS USED TO SHUTDOWN THE THREAD
        return;

      if (!reserved) {
        reserved = reserveQueueSlot();
        if (!reserved) {
          // SHUTTING DOWN: same outcome as the enqueue loop below giving up, and for the same reason.
          logFailedEnqueue(pages);
          return;
        }
      }

      // Index pages BEFORE enqueueing so that getCachedPageFromMutablePageInQueue()
      // can find them even if the queue.offer() hasn't completed yet.
      pageIndex.putAll(pages);

      // TRY TO INSERT THE PAGE IN THE QUEUE UNTIL THE THREAD IS STILL RUNNING. With a reservation in hand this
      // succeeds on the first attempt - a reserved slot cannot be taken by another reserver - so the timed retry
      // stays as the safety net it always was, for the one case the reservation does not cover: a batch that
      // reached the queue without one (see queueReservations).
      while (running) {
        if (queue.offer(new PagesToFlush(pages), 1, TimeUnit.SECONDS)) {
          enqueued = true;
          return;
        }
      }

      // Failed to enqueue (shutdown in progress) — remove from index
      pageIndex.removeAll(pages);
      logFailedEnqueue(pages);
    } finally {
      if (reserved)
        // The batch occupies the slot from now on (or nothing does, and the slot is free for the next caller).
        releaseQueueReservation(enqueued);
    }
  }

  private void logFailedEnqueue(final List<MutablePage> pages) {
    LogManager.instance()
        .log(this, Level.SEVERE, "Error on flushing pages %s during shutdown of the database (running=%s queue=%d)", pages, running,
            queue.size());
  }

  /**
   * Admission control on the bounded flush queue, and the reason a full queue no longer stalls the whole JVM
   * (issue #6259).
   * <p>
   * {@code PageManager.publishPages} holds the JVM-wide page-manager lock across BOTH halves of publication - the
   * page write and the flush enqueue - because the snapshot t0 barrier (#6075/#6125) rests on exactly that: it takes
   * the same lock so that from there on no committer can put a page on disk OR into the flush pipeline. The enqueue
   * therefore has to stay inside the lock, which leaves only one place for the WAIT to go: ahead of it. Before this,
   * a committer that found the queue full parked in {@code queue.offer} <i>holding a lock every committer of every
   * database in the process needs</i>, so one database's write burst against a slow volume serialized the commits of
   * unrelated databases on idle ones. The queue filling is backpressure and is meant to hold the writers it belongs
   * to; what was wrong is only that the wait was charged to everybody. Same distinction, and same fix, as #6200 drew
   * for the deferred-RAM cap.
   * <p>
   * <b>This must be called BEFORE the page-manager lock is taken</b>, and it must hold nothing the flush thread
   * needs: it holds only {@link #queueSlotLock}, which the flush thread takes solely to notify (never to write), and
   * the slot it waits for is freed by the poll itself, before any page is written. So a wedged disk delays this wait
   * by at most the batch already being written, never indefinitely.
   *
   * @return {@code true} when a slot was reserved, and the caller must hand it to
   *     {@link #scheduleFlushOfPages(List, boolean)} or give it back with {@link #releaseQueueReservation};
   *     {@code false} when the flush thread is shutting down and nothing more will be enqueued.
   */
  boolean reserveQueueSlot() throws InterruptedException {
    if (tryReserveQueueSlot())
      // THE COMMON CASE: THE QUEUE HAS ROOM, AND THE PUBLICATION PATH PAYS ONE CAS FOR IT
      return true;

    final long beginTime = System.currentTimeMillis();
    boolean reported = false;
    queueSlotWaits.incrementAndGet();
    queueSlotWaiters.incrementAndGet();
    try {
      while (running) {
        synchronized (queueSlotLock) {
          if (tryReserveQueueSlot())
            return true;
          queueSlotLock.wait(queueSlotWaitPollMillis);
        }

        // Reported from OUTSIDE the monitor: a log write is a file write, and holding this monitor across it would
        // make the flush thread's own signal queue behind it. Once per held committer, not once per round.
        if (!reported && System.currentTimeMillis() - beginTime > QUEUE_SLOT_WARN_MILLIS) {
          reported = true;
          LogManager.instance().log(this, Level.WARNING,
              "Commits have been throttled for %d ms waiting for room in the page-flush queue: the disk is not keeping up with the write rate, or flushing is suspended. The queue holds %d of the %d batches allowed by '%s'",
              null, System.currentTimeMillis() - beginTime, queue.size(), queueCapacity,
              GlobalConfiguration.PAGE_FLUSH_QUEUE.getKey());
        }
      }
      // SHUTTING DOWN. Deliberately no last-minute retry for a slot: the enqueue this reservation is for is itself
      // gated on `running`, so a slot handed out now would be released unused one line later - and the batch is
      // recovered from the WAL either way, its entry never having been acked.
      return false;
    } finally {
      queueSlotWaiters.decrementAndGet();
    }
  }

  /** Non-blocking half of {@link #reserveQueueSlot}: takes a slot only while the queue provably has one to give. */
  private boolean tryReserveQueueSlot() {
    while (true) {
      final int reservations = queueReservations.get();
      // The queue's own size, so a batch enqueued without a reservation narrows admission instead of overflowing it.
      if (queue.size() + reservations >= queueCapacity)
        return false;
      if (queueReservations.compareAndSet(reservations, reservations + 1))
        return true;
    }
  }

  /**
   * Gives a reservation back.
   *
   * @param enqueued whether the batch took the slot. When it did, the slot is now OCCUPIED and there is nothing to
   *                 wake anybody for - the sum {@code size + reservations} is unchanged - which is what keeps the
   *                 normal publication path from touching {@link #queueSlotLock} at all.
   */
  void releaseQueueReservation(final boolean enqueued) {
    final int remaining = queueReservations.decrementAndGet();
    // A count below zero means a reservation was released twice, and unlike a leak it fails OPEN: admission would
    // then hand out more slots than the queue has, putting the enqueue it exists to protect back inside the
    // page-manager lock. Asserted rather than clamped - a clamp would hide it, and the test lanes run with -ea,
    // which is where such a bug gets written (same reasoning as the single-database batch assert in PagesToFlush).
    assert remaining >= 0 : "flush-queue reservations went negative (" + remaining + ")";
    if (!enqueued)
      signalQueueSlotAvailable();
  }

  /** Wakes the callers parked in {@link #reserveQueueSlot}, if there are any: on the flush path, usually none. */
  private void signalQueueSlotAvailable() {
    if (queueSlotWaiters.get() == 0)
      return;
    synchronized (queueSlotLock) {
      queueSlotLock.notifyAll();
    }
  }

  @Override
  public void run() {
    if (logContext != null)
      LogManager.instance().setContext(logContext);

    while (running || !queue.isEmpty()) {
      try {
        // FLUSH ALL THE PAGES
        flushPagesFromQueueToDisk(null, 1_000L);

      } catch (final InterruptedException e) {
        running = false;
      } catch (final Throwable e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on processing page flush requests", e);
      }
    }
  }

  /** Records that a page of the database LEFT the flush pipeline: the per-database progress signal (#4928). */
  private void bumpFlushProgress(final MutablePage page) {
    final AtomicLong counter = flushedPagesPerDatabase.get(page.getPageId().getDatabase());
    if (counter != null)
      counter.incrementAndGet();
  }

  /**
   * Waits until all the pages of a database are flushed to disk.
   * <p>
   * Uses the {@link #pageIndex} as the authoritative source of truth for pending pages. Entries are added to
   * pageIndex BEFORE enqueueing and removed AFTER flushing each page, so checking pageIndex is race-free unlike
   * checking queue + nextPagesToFlush separately.
   *
   * @return {@code true} when every page of the database reached the disk, {@code false} when the wait gave
   *     up: either interrupted, or no flush PROGRESS was observed for {@code arcadedb.flushAllPagesTimeout}
   *     milliseconds (#4928 - a wedged flush thread or unwritable disk used to hang close()/rename()/backup
   *     forever here). The window resets whenever the pending-page count decreases, so a healthy but slow
   *     backlog never trips it. Callers on the close path treat {@code false} as a crash-equivalent close:
   *     the WAL files and the lock file are preserved so the next open replays the unflushed pages.
   */
  protected boolean waitAllPagesOfDatabaseAreFlushed(final Database database) {
    final long timeoutMs = database.getConfiguration().getValueAsLong(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT);
    int lastPending = Integer.MAX_VALUE;
    final AtomicLong flushedCounter = flushedPagesPerDatabase.computeIfAbsent(database, k -> new AtomicLong());
    long lastFlushed = flushedCounter.get();
    long lastProgressAt = System.currentTimeMillis();
    while (true) {
      final int pending = pageIndex.pendingOf(database);

      if (pending <= 0)
        return true;

      final long now = System.currentTimeMillis();
      final long flushed = flushedCounter.get();
      if (pending < lastPending || flushed > lastFlushed) {
        // The flush is making progress ON THIS DATABASE. Two signals on purpose: on LIVE callers (rename,
        // backup-suspend, compaction shipping) sustained commits can keep the pending count from ever
        // dipping below its minimum while the flusher works flat out, so this database's pages leaving the
        // pipeline also reset the window. The signal is per-database by design: a busy sibling database
        // sharing the flush thread must not mask a wedged one.
        lastPending = Math.min(lastPending, pending);
        lastFlushed = flushed;
        lastProgressAt = now;
      } else if (timeoutMs > 0 && now - lastProgressAt > timeoutMs) {
        LogManager.instance().log(this, Level.SEVERE,
            "No flush progress for %d ms with %d pages of database '%s' still pending: giving up the wait. The caller preserves the WAL so the next open recovers the unflushed pages",
            null, timeoutMs, pending, database.getName());
        return false;
      }

      try {
        // Parks on the database's drain signal rather than sleeping (#6199): the wait ends the moment the last page
        // of this database leaves the pipeline, and the interval only bounds how often the timeout above is
        // re-evaluated. On a process cycling through many databases the old sleep rounded every close, rename,
        // compaction and backup suspension up to the next poll boundary even when the pipeline was already empty.
        pageIndex.awaitDrain(database, flushWaitPollMillis);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  protected void flushPagesFromQueueToDisk(final Database database, final long timeout) throws InterruptedException, IOException {
    // NO CAP CHECK HERE ANY MORE (issue #6200). The backpressure of #4728 used to live at this exact point: once
    // the deferred backlog crossed the cap the flush thread stopped polling ALTOGETHER, so the bounded queue filled
    // and scheduleFlushOfPages throttled the committing threads - of EVERY open database, not just of the suspended
    // one whose backlog it was, and (because publishPages holds the JVM-wide page-manager lock across the enqueue)
    // by blocking them inside that lock. A leader shipping a multi-GB snapshot of database A therefore stalled the
    // committers of unrelated databases B and C, whose pages could have been written to disk immediately and whose
    // flushing would have RELIEVED the heap rather than added to it. The cap now throttles the committers of the
    // suspended databases directly, before that lock is taken - see awaitDeferredBacklogUnderCap - and this thread
    // always drains.
    final PagesToFlush pagesToFlush = queue.poll(timeout, TimeUnit.MILLISECONDS);

    if (pagesToFlush != null) {
      // A slot is free: admit a committer waiting for one (issue #6259). Signalled HERE, before the batch is
      // written, because the slot IS free from here - the batch has left the queue - and the point of the whole
      // mechanism is that a slow write delays the writes, not the admission of the next committer. The read is one
      // volatile load when nobody is waiting, which is the normal state of a queue that is not full.
      signalQueueSlotAvailable();

      // Publish the entry immediately after polling so that getCachedPageFromMutablePageInQueue()
      // can find pages that are no longer in the queue but not yet flushed to disk.  This
      // minimizes the window where a page is invisible to getMostRecentVersionOfPage().
      nextPagesToFlush.set(pagesToFlush);
      try {
        if (pagesToFlush == SHUTDOWN_THREAD)
          // SPECIAL CONTENT FOR SHUTDOWN
          running = false;
        else if (!pagesToFlush.pages.isEmpty()) {
          if (database == null || pagesToFlush.database.equals(database)) {
            if (database == null) {
              final Database db = (Database) pagesToFlush.database;
              // Check the suspended flag and defer the batch atomically under the per-database lock, so this
              // cannot interleave with the flag-clear + deferred-detach in setSuspended(false). Either we
              // observe "suspended" and the unsuspend's detach picks this batch up, or we observe "not
              // suspended" (flag already cleared) and fall through to flush it normally - never a defer that
              // outlives the detach.
              synchronized (suspendLock(db)) {
                if (isSuspended(db)) {
                  deferredByDatabase.computeIfAbsent(db, k -> new ConcurrentLinkedQueue<>()).offer(pagesToFlush);
                  addDeferredRAM(db, batchRAM(pagesToFlush));
                  return;
                }
              }
            }

            if (!pagesToFlush.database.isOpen())
              return;

            synchronized (pagesToFlush.pages) {
              for (final MutablePage page : pagesToFlush.pages) {
                if (!pagesToFlush.database.isOpen()) {
                  // Database was closed/dropped concurrently (e.g., during test teardown).
                  // Clean up remaining pageIndex entries and stop flushing this batch.
                  LogManager.instance().log(this, Level.FINE, "Skipping page flush for closed database '%s'",
                      pagesToFlush.database.getName());
                  for (final MutablePage remaining : pagesToFlush.pages)
                    removeFromFlushIndex(remaining);
                  break;
                }
                try {
                  pageManager.flushPage(page);
                } catch (final InterruptedIOException e) {
                  if (Thread.currentThread() != this)
                    throw e;
                  // #4924 follow-up: an interrupt of the flush thread is a shutdown request, never permission to
                  // drop a dirty page. Its WAL entry was already acked to the committer, so skipping the write
                  // would silently lose the page for readers once the cache evicts it, and abandoning the batch
                  // would leak its entries in pageIndex (hanging waitAllPagesOfDatabaseAreFlushed on close).
                  // Clear the flag (concurrentPageAccess rejects any I/O while it is set), stop accepting new
                  // work and retry the write: the run() loop then drains the remaining queue before exiting.
                  // The retry is NOT unbreakable: concurrentPageAccess re-checks the interrupt flag on every
                  // iteration of its I/O-slot spin, so a fresh interrupt surfaces as a second
                  // InterruptedIOException, contained below. A slot held forever by hung I/O would still spin,
                  // but that risk is pre-existing and shared by every flush path, interrupted or not.
                  Thread.interrupted();
                  running = false;
                  try {
                    pageManager.flushPage(page);
                  } catch (final DatabaseMetadataException e2) {
                    // FILE DELETED, CONTINUE WITH THE NEXT PAGES (same handling as the primary attempt)
                    LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e2, page);
                  } catch (final IOException e2) {
                    // Double fault: the retry failed too (a fresh interrupt broke the I/O-slot spin, or the disk
                    // genuinely errored). Do NOT let it escape and abort the batch: the remaining pages must
                    // still be flushed and released from pageIndex or the database close would hang. This page's
                    // WAL entry was never acked (notifyPageFlushed not reached), so it is recovered from the WAL.
                    LogManager.instance().log(this, Level.SEVERE,
                        "Error on flushing page '%s' to disk after interrupt, the page will be recovered from the WAL on restart",
                        e2, page);
                    // A re-interrupt set the flag again: clear it so the rest of the batch can reach the disk.
                    Thread.interrupted();
                  }
                } catch (final DatabaseMetadataException e) {
                  // FILE DELETED, CONTINUE WITH THE NEXT PAGES
                  LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
                } catch (final IOException e) {
                  // #4928: contain a plain I/O failure per page, same policy as the interrupt double-fault
                  // above. Letting it escape aborted the whole batch: the remaining pages were never flushed
                  // or retried, yet their pageIndex entries survived, so waitAllPagesOfDatabaseAreFlushed
                  // spun forever and close()/rename()/backup-suspend hung. The failed page's WAL entry was
                  // never acked (notifyPageFlushed not reached), which keeps its WAL file from being dropped:
                  // the page is durable via WAL replay on the next open, NOT repaired in place - until then,
                  // once the read cache evicts it, readers see the stale on-disk version.
                  LogManager.instance().log(this, Level.SEVERE,
                      "Error on flushing page '%s' to disk, the page will be recovered from the WAL on restart", e, page);
                } finally {
                  // Remove from index AFTER flushing: the page is now on disk and will be
                  // found in the read cache (putPageInReadCache was called at commit time).
                  // Reference identity ensures a NEWER MutablePage for the same PageId (queued
                  // by a later TX while this batch was waiting) is NOT removed from the index.
                  removeFromFlushIndex(page);
                  bumpFlushProgress(page);
                }
              }
            }
          }
        }
      } finally {
        nextPagesToFlush.set(null);
      }
    }
  }

  /**
   * Whether any page of the database is still anywhere in the flush pipeline. Used by the snapshot t0 barrier
   * (#6075) to tell "the pipeline drained and stayed drained" from "something is still feeding it", which since
   * #6125 can only be index compaction: the barrier's second drain runs with the publication lock held, so no
   * committer can queue a page.
   */
  boolean hasPendingPagesOfDatabase(final Database database) {
    return pageIndex.hasPendingOf(database);
  }

  /**
   * Waits for the flush pipeline of a database to empty, bounded by a HARD wall-clock deadline rather than by the
   * progress-based {@code arcadedb.flushAllPagesTimeout} (#6125).
   * <p>
   * This exists for exactly one caller: the snapshot t0 barrier's second drain, which runs with the JVM-wide
   * page-manager lock held. There it has to absorb only the handful of pages that were queued between the barrier's
   * first, lock-free drain and its acquisition of that lock, so it normally returns on the FIRST check - but a
   * flush thread wedged on a dead disk must not be able to hold the global lock for the 60 s the progress-based
   * timeout allows, which would stall every committer of every database in the JVM.
   * <p>
   * <b>Parks on the drain signal rather than polling (issue #6199).</b> It used to sleep 1 ms per round, and it does
   * that under the JVM-wide lock, so every one of those milliseconds was a millisecond in which no committer of any
   * database in the process could publish a page - a cost paid even when the pages had landed 999 us earlier. The
   * lock hold is now the time the writes actually take. Neither a spin nor a shorter sleep was the answer: what the
   * wait is waiting for is a synchronous file write, orders of magnitude longer than any sane spin, and a shorter
   * sleep would burn a core under a JVM-wide lock to improve the average without touching the worst case.
   * {@link #RESIDUAL_DRAIN_WAIT_POLL_MILLIS} survives as the bound on a hypothetical lost notification, which is why
   * it stays at the 1 ms this used to sleep: the worst case is unchanged, the common case is immediate.
   *
   * @return {@code true} when the pipeline emptied, {@code false} when the deadline expired first (the caller
   *     proceeds with a t0 that may sit slightly behind the last committed transaction, and says so).
   */
  boolean waitPendingPagesOfDatabaseUntil(final Database database, final long deadlineMillis)
      throws InterruptedException {
    while (hasPendingPagesOfDatabase(database)) {
      final long remaining = deadlineMillis - System.currentTimeMillis();
      if (remaining <= 0)
        return false;
      pageIndex.awaitDrain(database, Math.min(remaining, RESIDUAL_DRAIN_WAIT_POLL_MILLIS));
    }
    return true;
  }

  /**
   * Bounded {@link #setSuspended(Database, boolean)} acquisition for the snapshot t0 barrier (#6125).
   * <p>
   * The unbounded version parks on {@code suspendLock} for as long as another suspender's resume is in flight, and
   * that resume synchronously writes up to {@code arcadedb.flushSuspendMaxDeferredRAM} (512 MB by default) of
   * deferred pages. The barrier calls this with the JVM-wide page-manager lock held, so an unbounded park there
   * would put every committer of every database behind that write. Giving up instead lets the barrier fail cleanly
   * and its consumer fall back to the suspend-and-freeze path, which is slower for the writers of ONE database
   * rather than briefly fatal for all of them.
   *
   * @return {@code true} when a suspender reference was acquired - and the caller must release it exactly once with
   *     {@code setSuspended(database, false)} - or {@code false} when the deadline expired first, in which case
   *     nothing was acquired.
   */
  boolean trySuspendUntil(final Database database, final long deadlineMillis) {
    final Object lock = suspendLock(database);
    synchronized (lock) {
      boolean interrupted = false;
      try {
        while (resumingDatabases.contains(database)) {
          final long remaining = deadlineMillis - System.currentTimeMillis();
          if (remaining <= 0)
            return false;
          try {
            lock.wait(Math.min(remaining, 100));
          } catch (final InterruptedException e) {
            interrupted = true;
          }
        }
        suspended.merge(database, 1, Integer::sum);
        return true;
      } finally {
        if (interrupted)
          Thread.currentThread().interrupt();
      }
    }
  }

  public void waitForCurrentFlushToComplete(final Database database) throws InterruptedException {
    waitForCurrentFlushToCompleteUntil(database, Long.MAX_VALUE);
  }

  /**
   * Bounded {@link #waitForCurrentFlushToComplete(Database)} for the snapshot t0 barrier (#6125).
   * <p>
   * The unbounded version waits for the in-flight batch to reach the disk with no ceiling at all - it polls until
   * {@code PageManager.flushPage}'s synchronous {@code file.write} returns, and a write to a dead disk never does.
   * The barrier calls this with the JVM-wide page-manager lock held, so that wait has to be bounded like every other
   * step it performs there; a timeout is treated as a failure to establish t0, not as permission to proceed, because
   * the batch is half-written by definition and a snapshot taken over it would hold half a transaction.
   *
   * @return {@code true} when the pipeline is no longer writing this database's pages, {@code false} on deadline.
   */
  boolean waitForCurrentFlushToCompleteUntil(final Database database, final long deadlineMillis)
      throws InterruptedException {
    PagesToFlush current;
    while ((current = nextPagesToFlush.get()) != null && database.equals(current.database)) {
      if (System.currentTimeMillis() >= deadlineMillis)
        return false;
      Thread.sleep(1);
    }
    return true;
  }

  /**
   * Refcounted suspension (issue #5068). {@code value == true} adds a suspender: the return value is
   * {@code true} only for the FIRST suspender (count 0 to 1). {@code value == false} releases one
   * suspender: only the LAST release (count 1 to 0) resumes flushing - it synchronously flushes the
   * deferred batches and clears the flag - and returns {@code true}; a non-last release just decrements
   * the count and returns {@code false}, keeping the database suspended for the remaining suspenders.
   * <p>
   * A new suspender arriving while the last release is mid-resume waits until the resume completes:
   * otherwise its freshly acquired window would overlap the resume's synchronous page writes, exactly the
   * torn-read overlap this refcount exists to prevent. The wait is uninterruptible (the interrupt flag is
   * preserved and restored) and timed, so a lost notification degrades to polling instead of a hang.
   * <p>
   * Parking here uninterruptibly is an ACCEPTED TRADEOFF (#5074 review): the wait is bounded by the
   * resume's Phase 1, which writes at most {@code FLUSH_SUSPEND_MAX_DEFERRED_RAM} (the #4728 backpressure
   * cap) of deferred pages, and the only suspenders are admin-path threads - SQL BACKUP DATABASE, database
   * verify, and HA snapshot serving, the latter capped at {@code HA_SNAPSHOT_MAX_CONCURRENT} (default 2)
   * of Undertow's 500 workers and serialized per database by {@code SnapshotHttpHandler}. No pool can be
   * exhausted and shutdown is delayed by at most one bounded resume; a {@code flushPage} wedged on a dead
   * disk stalls the flush thread itself first, so this wait is never the limiting factor.
   */
  public boolean setSuspended(final Database database, final boolean value) {
    final Object lock = suspendLock(database);

    if (value) {
      synchronized (lock) {
        boolean interrupted = false;
        while (resumingDatabases.contains(database)) {
          try {
            lock.wait(100);
          } catch (final InterruptedException e) {
            interrupted = true;
          }
        }
        if (interrupted)
          Thread.currentThread().interrupt();

        final int count = suspended.merge(database, 1, Integer::sum);
        return count == 1;
      }
    }

    synchronized (lock) {
      final Integer count = suspended.get(database);
      if (count == null)
        // NOT SUSPENDED (E.G. UNBALANCED RELEASE OR DATABASE DROPPED MID-SUSPENSION): NOTHING TO RESUME
        return false;
      if (count > 1) {
        // OTHER SUSPENDERS STILL OWN THE WINDOW: JUST RELEASE THIS CALLER'S REFERENCE
        suspended.put(database, count - 1);
        return false;
      }
      // LAST SUSPENDER: RESUME BELOW. The count stays at 1 through Phase 1 so the flush thread keeps
      // deferring, and resumingDatabases blocks new suspenders until the flag is cleared in Phase 2.
      if (!resumingDatabases.add(database))
        // DEFENSIVE: A RESUME IS ALREADY IN FLIGHT (UNBALANCED CONCURRENT RELEASE)
        return false;
    }

    try {
      resumeFlushing(database);
    } finally {
      synchronized (lock) {
        // Idempotent on the success path (Phase 2 already removed the entry and the resume gate kept
        // anyone from re-adding it); on an unexpected exception out of the resume it heals the stuck
        // count so the database does not stay suspended forever.
        suspended.remove(database);
        resumingDatabases.remove(database);
        lock.notifyAll();
      }
      // The database is no longer suspended, so its committers are no longer subject to the deferred-backlog cap
      // even if the backlog of some OTHER suspended database is still over it (issue #6200).
      signalDeferredRAM();
    }
    return true;
  }

  /** Executes the resume of the LAST suspender: flushes the deferred batches and clears the suspension. */
  private void resumeFlushing(final Database database) {
    // Phase 1: synchronously flush all deferred batches accumulated while suspended. If the unsuspending
    // thread (e.g. backup/HA) is interrupted, the flag is consumed ONCE for the whole flush (a set flag makes
    // concurrentPageAccess reject every subsequent write, one throw+retry cycle per page) and restored at the
    // end of the method, so the caller still observes its own cancellation and the re-enqueue phase below runs
    // interrupt-free.
    boolean restoreCallerInterrupt = false;
    // The FIRST unexpected failure of the page loop below, re-raised once the whole backlog has been drained.
    Throwable unexpected = null;
    try {
      // Serialized against detachPendingPages: the detach must either see these batches (and take its page out of
      // them) or run after they have all reached the disk. Otherwise a superseded copy written here could land after
      // a replicated write and roll the page version backwards.
      synchronized (replayDrainLock(database)) {
        final ConcurrentLinkedQueue<PagesToFlush> deferred = deferredByDatabase.remove(database);
        if (deferred != null) {
          for (final PagesToFlush batch : deferred) {
            synchronized (batch.pages) {
              for (final MutablePage page : batch.pages) {
                try {
                  // A page whose database closed mid-unsuspend is NOT written - there is nothing left to write it
                  // to - but it still leaves the backlog through the finally below. Skipping it (which is what the
                  // `continue`/`break` here used to do) stranded its bytes in the deferred accounting for the life
                  // of the process, and every LATER suspension would then be throttled by that much: the drift the
                  // blanket "reset the counter when nothing is deferred anywhere" net existed to absorb. Releasing
                  // it at the source is exact, and per database, so no reset can ever wipe a sibling's live count.
                  if (batch.database.isOpen())
                    pageManager.flushPage(page);
                } catch (final DatabaseMetadataException e) {
                  // FILE DELETED, CONTINUE WITH THE NEXT PAGES
                  LogManager.instance().log(this, Level.WARNING, "Error on flushing deferred page '%s' to disk", e, page);
                } catch (final InterruptedIOException e) {
                  // Don't drop the deferred dirty page, its WAL entry was already acked: consume the flag and
                  // retry the write once.
                  restoreCallerInterrupt = true;
                  Thread.interrupted();
                  try {
                    pageManager.flushPage(page);
                  } catch (final DatabaseMetadataException | IOException e2) {
                    // Contain every retry failure (file dropped, re-interrupt, real I/O error): letting it escape
                    // would skip the unsuspend phases below, leaving the database suspended with batches stranded
                    // in the deferred map. An unflushed page is recovered from the WAL (its entry was never acked).
                    // A fresh re-interrupt set the flag again: clear it so the remaining pages flush cleanly.
                    LogManager.instance().log(this, Level.WARNING, "Error on flushing deferred page '%s' to disk", e2, page);
                    Thread.interrupted();
                  }
                } catch (final IOException e) {
                  LogManager.instance().log(this, Level.WARNING, "Error on flushing deferred page '%s' to disk", e, page);
                } catch (final Throwable e) {
                  // ONE PAGE'S UNEXPECTED FAILURE MUST NOT ABANDON THE REST OF THE BACKLOG (review of #6223). Letting
                  // it escape this loop left every page after it in the batch without the finally below: still in
                  // pageIndex, so this database's pending count stays above zero and the next close waits out the
                  // whole flushAllPagesTimeout before giving up. Contained like the I/O failures above - the page is
                  // recovered from the WAL, its ack was never given - and re-raised once the backlog is drained, so
                  // the caller still learns that the resume failed.
                  if (unexpected == null)
                    unexpected = e;
                  LogManager.instance().log(this, Level.SEVERE,
                      "Unexpected error on flushing deferred page '%s' to disk, the page will be recovered from the WAL on restart",
                      e, page);
                } finally {
                  // The page leaves the deferred backlog (flushed to disk), so release its reserved RAM (issue #4728).
                  addDeferredRAM(database, -page.getPhysicalSize());
                  removeFromFlushIndex(page);
                  bumpFlushProgress(page);
                }
              }
            }
          }
        }
      }

    } finally {
      // PHASES 2 TO 4 RUN EVEN IF PHASE 1 THREW. Its per-page loop contains every failure it expects (a dropped
      // file, an interrupt, an I/O error), but an UNEXPECTED exception escaping it used to abort the rest of the
      // resume outright: the batches deferred during Phase 1 stayed undetached - their pages pinned in pageIndex
      // forever, so the next close of that database would wait for a drain that can never happen - and its RAM
      // charge stayed on the cap. The caller's own finally heals the suspension COUNT, but nothing healed those.
      // Pre-existing shape, flagged in review of #6223 and closed here rather than left open, because the phases
      // below are pure bookkeeping: they cannot fail, and the exception still propagates after them.
      //
      // THE ONE CASE THIS DOES NOT COVER, and the reason it is an accepted residual rather than an oversight: the
      // per-page finally itself throwing. Every failure of the WRITE is contained per page now, so the loop always
      // reaches the end of the backlog; a finally that fails abandons the pages after it, which keep their pageIndex
      // entry, so this database's pending count stays above zero and its next close waits out
      // arcadedb.flushAllPagesTimeout before giving up - preserving the WAL, so those pages are recovered on the
      // next open rather than lost. Covering it would put a try/finally around the release of every deferred page,
      // on the resume path of every one of them, to defend against three field reads on a MutablePage.

      // Phase 2 + Phase 3a: under the per-database lock, clear the suspension refcount AND atomically detach
      // any batches deferred during Phase 1. Holding the lock makes this transition mutually exclusive with
      // the suspended-check + defer in flushPagesFromQueueToDisk: once the flag is cleared no further batch
      // can be added to deferredByDatabase for this database, and every batch deferred up to that point is
      // detached exactly once. A single detach therefore suffices - nothing can repopulate the map behind us.
      // New suspenders are still gated out by resumingDatabases (removed by the caller AFTER Phase 3b), so
      // the count going 1 to 0 here cannot interleave with a concurrent acquire.
      final ConcurrentLinkedQueue<PagesToFlush> newDeferred;
      synchronized (suspendLock(database)) {
        suspended.remove(database);
        newDeferred = deferredByDatabase.remove(database);
      }

      // Phase 3b: re-enqueue the detached batches into the main queue so the background thread picks them up.
      // They are appended to the tail of the queue, so if any post-unsuspend commits have already been
      // enqueued they will be flushed first. WAL-based recovery guarantees correctness even if the flush order
      // differs from commit order. This blocking re-enqueue is deliberately done OUTSIDE the lock: queue.offer
      // can block when the queue is full, and the only consumer that drains the queue (the flush thread) needs
      // the same per-database lock to make progress, so holding it across the offer would deadlock.
      if (newDeferred != null) {
        for (final PagesToFlush batch : newDeferred) {
          // The batch moves back to the main queue and leaves the deferred backlog accounting (issue #4728).
          addDeferredRAM(database, -batchRAM(batch));
          // Re-entering the bounded queue goes through the same admission control as any other enqueue (#6259):
          // the batch gave its slot back when the flush thread polled it, and a resume can carry thousands of them,
          // so without a reservation this loop could fill the queue under a committer that is holding one - putting
          // that committer's offer back inside the page-manager lock, which is the whole bug. Blocking here is safe
          // for the same reason the offer below always was: this runs outside every lock (see above).
          boolean reserved = false;
          boolean enqueued = false;
          try {
            reserved = reserveQueueSlot();
            if (!reserved)
              // SHUTTING DOWN: the remaining batches stay unwritten and are recovered from the WAL, as before.
              break;
            while (running) {
              if (queue.offer(batch, 1, TimeUnit.SECONDS)) {
                enqueued = true;
                break;
              }
              LogManager.instance().log(this, Level.WARNING,
                  "Page flush queue is full while re-enqueueing deferred batch for database '%s'; retrying", database.getName());
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          } finally {
            if (reserved)
              releaseQueueReservation(enqueued);
          }
        }
      }

      // Phase 4: this database's backlog has provably ended, so release whatever is still charged to it - and ONLY
      // to it. The suspension was cleared under suspendLock in Phase 2 and resumingDatabases keeps a new suspender
      // out until the caller's finally, so nothing can defer another of its batches behind us (the same argument
      // Phase 2 rests on). With Phase 1 now releasing every page it skips, this should always find zero; it stays as
      // the per-database safety net that the old blanket "if nothing is deferred anywhere, zero the counters" reset
      // was - that one read `deferredByDatabase.isEmpty()` on this thread while the flush thread could be deferring
      // the FIRST batch of an unrelated database, and would then wipe that database's fresh charge (review of #6200).
      releaseResidualDeferredRAM(database);

      if (restoreCallerInterrupt)
        // Consumed once during Phase 1: restore it so the caller still observes its own cancellation.
        Thread.currentThread().interrupt();
    }

    if (unexpected != null) {
      // Raised only now, with the backlog drained and the suspension cleared: the caller must still learn that its
      // resume failed, but not at the price of a pipeline left holding pages nothing will ever take out of it.
      if (unexpected instanceof final RuntimeException runtime)
        throw runtime;
      if (unexpected instanceof final Error error)
        throw error;
      throw new RuntimeException("Error on flushing the deferred pages of database '" + database.getName() + "'",
          unexpected);
    }
  }

  public boolean isSuspended(final Database database) {
    final Integer count = suspended.get(database);
    return count != null && count > 0;
  }

  /** Returns the stable per-database monitor used to serialize the suspend check/defer against unsuspend. */
  private Object suspendLock(final Database database) {
    return suspendLocks.computeIfAbsent(database, k -> new Object());
  }

  private Object replayDrainLock(final Database database) {
    return replayDrainLocks.computeIfAbsent(database, k -> new Object());
  }

  public void closeAndJoin() throws InterruptedException {
    running = false;
    // Release any committer parked on the deferred-backlog cap: its database may still be suspended, and the
    // backlog is only relieved by a resume that shutdown is not going to wait for (issue #6200).
    signalDeferredRAM();
    // And any committer waiting for a flush-queue slot, for the same reason: what it is waiting for is this thread
    // polling, and this thread is on its way out (issue #6259).
    signalQueueSlotAvailable();
    // The sentinel takes a queue slot like any other batch, so it is admitted like one - but never WAITS for one:
    // a full queue simply means no sentinel, and the run loop already exits on its own once `running` is false and
    // the queue is drained. The sentinel is what makes that exit immediate, never what makes it happen. Admitting it
    // through the same accounting is what keeps a reserved slot reserved: taken from under a committer that holds
    // one, it would put that committer's offer back inside the page-manager lock (issue #6259).
    if (tryReserveQueueSlot())
      releaseQueueReservation(queue.offer(SHUTDOWN_THREAD));
    join();
  }

  /**
   * Removes a just-flushed page from the {@link #pageIndex}, but ONLY if the indexed value is still the
   * exact same instance that was flushed (issue #4544 - see {@link FlushPageIndex#removeIfSame}).
   */
  void removeFromFlushIndex(final MutablePage page) {
    pageIndex.removeIfSame(page);
  }

  /**
   * Moves {@code delta} bytes into (positive) or out of (negative) the deferred backlog of a database, keeping the
   * per-database split and the derived total in step (issue #6200), and releases the committers waiting on the cap
   * when the total drops back under it.
   * <p>
   * The two counters are not updated atomically with respect to each other, and do not need to be: a reader that
   * catches the pair mid-update sees a per-database sum off by one batch from the total for a few instructions.
   * What they DO have to agree on is the amount that moves, which is why a release takes from the per-database
   * counter first and moves only what it actually got - see the release branch.
   */
  private void addDeferredRAM(final BasicDatabase database, final long delta) {
    if (delta == 0)
      return;

    if (delta > 0)
      deferredRAMByDatabase.computeIfAbsent(database, k -> new AtomicLong()).addAndGet(delta);
    else {
      // A release NEVER creates an entry: the only way to reach one here without a matching deferral is a database
      // whose bookkeeping was already purged by its close, and re-creating it with a negative value would both
      // report a nonsensical gauge and re-pin the closed instance as a map key.
      final AtomicLong bytes = deferredRAMByDatabase.get(database);
      if (bytes == null)
        // AND IT RELEASES NOTHING FROM THE TOTAL EITHER, which is why this returns rather than falling through.
        return;

      // THE PER-DATABASE COUNTER IS THE AUTHORITY ON WHAT MAY BE RELEASED: the take is atomic and clamped at zero,
      // and only what the counter actually still held moves off the JVM-wide total.
      //
      // Without that the same bytes are released twice. releaseResidualDeferredRAM takes a database's whole
      // remaining charge in ONE getAndSet(0) plus subtraction, and it is not mutually exclusive with the per-page
      // releases of that same database's resume: Phase 1 walks the deferred batches holding only replayDrainLock,
      // while the purge (close/drop) takes neither that nor anything else Phase 1 holds. So a purge landing
      // mid-resume takes bytes Phase 1 has not released yet, and Phase 1 then releases them again. A null check
      // alone does NOT cover it - this reference can be read BEFORE the purge removes the entry and used after -
      // which is why the guard is on the VALUE rather than on the entry.
      //
      // Getting it wrong drives the JVM-wide total NEGATIVE, and that total is what the cap is read from, so the
      // #4728 backpressure would be silently disabled for every OTHER suspended database in the process until
      // enough new deferrals pushed it back over the cap.
      final long releasing = -delta;
      final long before = bytes.getAndAccumulate(releasing, (current, amount) -> current - Math.min(current, amount));
      final long applied = Math.min(before, releasing);
      if (applied <= 0)
        // ALREADY TAKEN, BY THE RESIDUAL RELEASE OR BY A RACING ONE: NOTHING OF THIS CHARGE IS LEFT TO MOVE
        return;

      addDeferredRAMTotal(-applied);
      return;
    }

    addDeferredRAMTotal(delta);
  }

  /** Moves the JVM-wide total alone, for the residual release that has already zeroed the per-database part. */
  private void addDeferredRAMTotal(final long delta) {
    final long previous = deferredRAMBytes.getAndAdd(delta);
    // Signal only on the DOWNWARD crossing of the cap, so a resume that writes thousands of deferred pages takes the
    // monitor once instead of once per page, and a backlog that is growing never touches it at all.
    if (delta < 0 && maxDeferredRAM > 0 && previous >= maxDeferredRAM && previous + delta < maxDeferredRAM)
      signalDeferredRAM();

    if (previous + delta < 0)
      repairNegativeDeferredRAM();
  }

  /**
   * Last-resort guard on the ONE number the #4728 cap is read from.
   * <p>
   * Every release is of bytes a matching deferral put there, so with correct accounting the total cannot go below
   * zero - not even transiently, since a page has to be deferred before it can be released. A negative value is
   * therefore a bug, and unlike the per-database gauge (which {@link #getDeferredRAMBytesOf} merely clamps for
   * display) it is not cosmetic: the cap test is {@code total >= maxDeferredRAM}, so a total left below zero
   * DISABLES the backpressure for every suspended database in the process until enough new deferrals climb back
   * over the cap - the OOM this whole mechanism exists to prevent, silently re-enabled.
   * <p>
   * So it is both reported - once per process, because a drift of this kind repeats - and repaired. Reporting alone
   * would leave the protection off; repairing alone would hide the bug that turned it off.
   */
  private void repairNegativeDeferredRAM() {
    // getAndAccumulate, so the value REPORTED is the negative one that was found rather than the zero it became,
    // and so a total another thread has already repaired is left alone instead of being re-reported.
    final long negative = deferredRAMBytes.getAndAccumulate(0, (current, ignored) -> current < 0 ? 0 : current);
    if (negative >= 0)
      // ANOTHER THREAD GOT THERE FIRST: NOTHING WAS FOUND NEGATIVE HERE, SO NOTHING IS COUNTED OR SAID
      return;

    if (deferredRAMDriftRepairs.getAndIncrement() == 0)
      LogManager.instance().log(this, Level.WARNING,
          "The deferred page-flush backlog accounting went negative (%d bytes) and was reset to zero: this is a bug in the accounting, please report it. Until the reset the '%s' cap could not throttle the committers of a suspended database",
          null, negative, GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM.getKey());
  }

  /**
   * Releases whatever a database is STILL charged for once its backlog has provably ended - its last suspender
   * resumed, or it was closed or dropped - and forgets its entry.
   * <p>
   * Strictly per database, which is the point: the blanket reset this replaces zeroed the total and the whole split
   * whenever {@code deferredByDatabase} was observed empty, a check made on the resuming thread while the flush
   * thread could be deferring the FIRST batch of an unrelated database, and that database's fresh charge was then
   * wiped - its cap defeated until its next mutation (review of #6200). Nothing here can touch another database.
   * <p>
   * With the accounting now exact at the source (every path that takes a page out of the backlog releases its bytes,
   * including the pages skipped because their database closed mid-unsuspend) this should always find zero. It stays
   * because the alternative to healing a hypothetical drift is a total stuck above the cap, which would throttle the
   * committers of every later suspension in the process.
   */
  private void releaseResidualDeferredRAM(final BasicDatabase database) {
    final AtomicLong bytes = deferredRAMByDatabase.remove(database);
    if (bytes == null)
      return;
    final long residual = bytes.getAndSet(0);
    if (residual != 0) {
      LogManager.instance().log(this, Level.FINE,
          "Released %d residual deferred bytes of database '%s' whose backlog ended", null, residual, database.getName());
      addDeferredRAMTotal(-residual);
    }
  }

  private void signalDeferredRAM() {
    synchronized (deferredRAMLock) {
      deferredRAMLock.notifyAll();
    }
  }

  /**
   * Whether a deferred-batch queue is still held for this database. Exists so the regression test of issue #6200
   * can prove that purging a SUSPENDED database leaves no queue behind - one nothing would ever resume, pinning the
   * closed instance as a map key and its pages in RAM for the life of the process.
   */
  boolean hasDeferredBatches(final BasicDatabase database) {
    return deferredByDatabase.containsKey(database);
  }

  /** Bytes of dirty pages currently deferred for a single database (issue #6200, gauge of item 2 of #6087). */
  public long getDeferredRAMBytesOf(final BasicDatabase database) {
    final AtomicLong bytes = deferredRAMByDatabase.get(database);
    return bytes != null ? Math.max(0L, bytes.get()) : 0L;
  }

  /**
   * The committer-side half of the #4728 backpressure, and the reason the flush thread no longer has to stop
   * draining for everybody (issue #6200).
   * <p>
   * While a database is suspended (HA snapshot ship, full backup) its dirty pages cannot be written to disk and pile
   * up in {@link #deferredByDatabase}; on a busy leader shipping a multi-GB snapshot that exhausted the heap. The cap
   * that bounds it has to be JVM-wide, because heap is: what does NOT follow is that the RESPONSE should be. Only
   * the batches of suspended databases are ever deferred, so only their committers can grow the backlog, and only
   * they are held here. A database that is not suspended is never delayed by this: its pages go straight to the
   * disk, which strictly RELIEVES the heap pressure the cap exists to bound.
   * <p>
   * <b>This must be called BEFORE the page-manager lock is taken</b>, which is why it is a separate step of
   * {@code PageManager.publishPages} rather than a check inside {@code scheduleFlushOfPages}. Publication holds that
   * JVM-wide lock across both the page write and the flush enqueue, so a committer parked while holding it stalls
   * every committer of every database - the very coupling this method removes.
   * <p>
   * <b>And it must hold nothing the resume needs</b>, since the resume is what releases the backlog it waits for.
   * Checked for both entry points (review of #6223): a committer arrives through {@code publishPages}, before its
   * {@code lock()}; index compaction arrives through {@code writePages}, and cannot hold that lock at all, because
   * {@code LockContext.lock()} is PROTECTED and {@code com.arcadedb.index.lsm} neither shares the package nor
   * subclasses {@link PageManager} - a structural bar, not a property of the path it happens to take today (the
   * {@code updatePageVersion} it calls first does NOT lock; only {@code validateAndBumpVersions} wraps that).
   * Nothing outside this class can hold {@link #suspendLock} or {@link #replayDrainLock} at all, the resume takes no
   * database lock, and a waiter parked below has released {@link #deferredRAMLock} by definition. So the only thing
   * a parked caller holds is its own transaction's file locks, which the resume never asks for.
   * <p>
   * The wait ends when the backlog drops under the cap (the suspension resumed and wrote its deferred batches, or
   * the database was dropped) or when this database stops being suspended. It is bounded per round so a suspension
   * released without freeing any RAM still lets its committers out, and it stops waiting during shutdown so a
   * database left suspended cannot keep a committer parked past {@code closeAndJoin}.
   * <p>
   * <b>Only the ASYNCHRONOUS writers reach this</b>, and nothing is missing by it: what the cap bounds is
   * {@link #deferredByDatabase}, and {@link #flushPagesFromQueueToDisk} is the single place anything is ever put
   * there. A synchronous {@code writePages} goes straight to {@code flushPage} without passing through this thread
   * at all, so it cannot add a byte to the backlog and has nothing to be held for.
   */
  public void awaitDeferredBacklogUnderCap(final BasicDatabase database) throws InterruptedException {
    // The instanceof is a cast, not a filter: every page in the flush pipeline comes from a local Database (that is
    // what owns a PageManager at all), and the suspension bookkeeping this reads is keyed by Database. A
    // BasicDatabase that is not one has never been deferred and so can never be over the cap.
    if (maxDeferredRAM <= 0 || !(database instanceof final Database db) || !isSuspended(db))
      // NOT SUSPENDED: THE COMMON CASE, AND TWO MAP READS AWAY FROM THE PUBLICATION IT MUST NOT SLOW DOWN
      return;

    final long beginTime = System.currentTimeMillis();
    boolean reported = false;
    while (true) {
      synchronized (deferredRAMLock) {
        if (!running || deferredRAMBytes.get() < maxDeferredRAM || !isSuspended(db))
          return;
        deferredRAMLock.wait(DEFERRED_BACKLOG_WAIT_POLL_MILLIS);
      }

      // Reported from OUTSIDE the monitor: a log write is a file write, and holding this monitor across it would
      // make the resume that is releasing the backlog queue behind it. Once per held committer, not once per round:
      // a long suspension throttles many of them, and the point is to name the database whose backlog is doing it.
      if (!reported && System.currentTimeMillis() - beginTime > DEFERRED_BACKLOG_WARN_MILLIS) {
        reported = true;
        // Both backlogs, and the JVM-wide one named as such: the cap is on the total across every suspended
        // database, so the backlog holding this database's committers is very often NOT its own - which is the
        // whole subject of #6200, and precisely what an operator reading a single figure would misread. The
        // elapsed hold is reported rather than the threshold that triggered the report: the threshold is a
        // constant, while how long this committer has actually been held is the number the operator came for.
        LogManager.instance().log(this, Level.WARNING,
            "Commits on database '%s' have been throttled for %d ms: page flushing is suspended (backup or HA snapshot) and the JVM-wide deferred backlog is %s, of which %s is this database's own, at or above the '%s' cap",
            null, db.getName(), System.currentTimeMillis() - beginTime, FileUtils.getSizeAsString(deferredRAMBytes.get()),
            FileUtils.getSizeAsString(getDeferredRAMBytesOf(db)), GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM.getKey());
      }
    }
  }

  /** Sum of the in-RAM size of every page in a deferred batch, used to bound the deferred backlog (issue #4728). */
  private static long batchRAM(final PagesToFlush batch) {
    long bytes = 0;
    if (batch.pages != null)
      // Synchronize on the same monitor used by removePagesOfFileFromBatch / removeAllPagesOfDatabase, which
      // can concurrently mutate this list (it.remove() / clear()), to avoid a ConcurrentModificationException
      // and miscounting while a dropped file or database is being purged.
      synchronized (batch.pages) {
        for (final MutablePage page : batch.pages)
          bytes += page.getPhysicalSize();
      }
    return bytes;
  }

  public CachedPage getCachedPageFromMutablePageInQueue(final PageId pageId) {
    final MutablePage page = pageIndex.get(pageId);
    if (page != null)
      return new CachedPage(page, true);
    return null;
  }

  public void removeAllPagesOfDatabase(final Database database) {
    for (final PagesToFlush pagesToFlush : queue.stream().toList())
      if (database.equals(pagesToFlush.database))
        synchronized (pagesToFlush.pages) {
          for (final MutablePage page : pagesToFlush.pages)
            pageIndex.remove(page.getPageId());
          pagesToFlush.pages.clear();
        }

    // Also clean index entries for pages currently being flushed, and forget the database's pending count with them
    pageIndex.removeAllOfDatabase(database);

    // Forget the per-database suspend bookkeeping so the dropped Database instance (and the resources it
    // pins) can be garbage collected instead of being retained for the flush thread's lifetime as a map key.
    //
    // The suspension flag and the deferred batches are dropped UNDER the database's own suspend monitor - the same
    // one flushPagesFromQueueToDisk holds across its isSuspended check AND its deferredByDatabase.offer, and the
    // same argument Phase 2 of the resume rests on. Without it the flush thread could observe "suspended" just
    // before this method runs and offer its batch just after, re-creating a deferred queue for a database this
    // method has already forgotten: nothing would ever resume it, so the batch, the map entry pinning the closed
    // instance and its RAM charge would all leak for the life of the process - and that leaked charge would
    // throttle the committers of every later suspension in the JVM. Ordering the flag before the detach matters
    // for the same reason it does in Phase 2: once it is clear, no further batch can be deferred (review of #6200).
    final ConcurrentLinkedQueue<PagesToFlush> droppedDeferred;
    synchronized (suspendLock(database)) {
      suspended.remove(database);
      resumingDatabases.remove(database);
      droppedDeferred = deferredByDatabase.remove(database);
    }
    // Only now, or a defer racing the block above would resolve a FRESH monitor and be excluded by nothing.
    suspendLocks.remove(database);
    replayDrainLocks.remove(database);
    flushedPagesPerDatabase.remove(database);

    // Deliberately outside that monitor: batchRAM takes each batch's own monitor, and the one nesting this class
    // allows is suspendLock BEFORE batch.pages (what the defer path does). These batches are already detached, so
    // no one else can be looking at them, and keeping the accounting out here avoids widening that nesting.
    if (droppedDeferred != null)
      for (final PagesToFlush batch : droppedDeferred)
        addDeferredRAM(database, -batchRAM(batch));

    // The database is gone: drop its per-database backlog entry with the rest of its bookkeeping, taking whatever
    // it was still charged for out of the JVM-wide total with it - dropping the entry alone would leave the total
    // inflated by that much forever, throttling every later suspension (#6200).
    releaseResidualDeferredRAM(database);

    // Its committers cannot be throttled by a database that no longer exists, and neither can anyone else be by ITS
    // backlog: wake every waiter so they re-read a total that just lost this database's share.
    signalDeferredRAM();
  }

  /** Drops every pending {@link MutablePage} of a single dropped file from the queue, the deferred batches and the index. */
  public void removeAllPagesOfFile(final Database database, final int fileId) {
    for (final PagesToFlush pagesToFlush : queue.stream().toList())
      removePagesOfFileFromBatch(pagesToFlush, database, fileId);

    // Also drain batches deferred while the database is suspended (e.g. during a backup): they are
    // no longer in the main queue, so their MutablePages would otherwise leak until the unsuspend flush.
    final ConcurrentLinkedQueue<PagesToFlush> deferred = deferredByDatabase.get(database);
    if (deferred != null)
      for (final PagesToFlush pagesToFlush : deferred)
        // These pages leave the deferred backlog, so release their reserved RAM accounting (issue #4728).
        addDeferredRAM(database, -removePagesOfFileFromBatch(pagesToFlush, database, fileId));

    // Finally clean any index entry for a page currently being flushed.
    pageIndex.removeAllOfFile(database, fileId);
  }

  /**
   * Takes EVERY copy of {@code pageId} pending in the flush pipeline out of it and hands them to the caller, which
   * becomes responsible for writing the most recent one to disk. Used by {@link TransactionManager#applyChanges},
   * which writes replicated / recovery pages straight to the file: while an older copy of the same page stays in the
   * pipeline, reads resolve it from {@link #pageIndex} instead of the file, and the eventual flush of that copy
   * overwrites the replicated page and rolls its version backwards.
   * <p>
   * Copies are matched by {@link PageId}, NOT by reference identity: successive commits can leave more than one
   * {@link MutablePage} for the same page pending at once (the newest in {@link #pageIndex}, an older one still
   * sitting in an earlier batch - the two-instance case {@link #removeFromFlushIndex} exists for, issue #4544).
   * Removing only the indexed instance would leave the older copy free to write the superseded version over the
   * replicated one. Every returned copy is superseded by the most recent one, whose content is a full page image
   * covering all of them, so the caller writes that one and releases the others' WAL acks.
   * <p>
   * The batch currently being written is also visited: the flush thread mutates the very same list, so the removal
   * may lose that race, which is why the caller waits for the in-flight batch to complete before writing.
   * <p>
   * Serialized against {@link #resumeFlushing}'s Phase 1 through {@link #replayDrainLock}: that phase detaches the
   * deferred batches and writes them from the resuming thread, outside both {@link #queue} and
   * {@link #nextPagesToFlush}, so without the mutex this walk could find the pipeline empty while a superseded copy
   * was still on its way to the disk.
   * <p>
   * ASSUMPTION: no other thread is scheduling a flush of this page concurrently. {@link #scheduleFlushOfPages}
   * publishes to {@link #pageIndex} BEFORE enqueueing, so a commit sitting between those two statements would have
   * its batch enqueued after this detach walked the queue, and that batch would still carry a superseded copy. This
   * holds on the only caller's path: replicated and crash-recovery replay is the sole writer of the pages it
   * applies - it goes through {@code writePageWithLock}, which never touches this pipeline - and it runs on a
   * follower or during recovery, where no local transaction is committing pages.
   *
   * @return every detached copy, most recent one included; empty when nothing was pending for this page.
   */
  List<MutablePage> detachPendingPages(final Database database, final PageId pageId) {
    // O(1) fast path for the common replay case of a page that is not in the pipeline at all. Deliberately
    // conservative: it skips the walk only when the WHOLE pipeline is empty, because a copy can outlive its
    // pageIndex entry (a newer instance flushed out of order removes the entry while an older one is still queued),
    // so "no entry for this pageId" alone would not prove the batches are clean.
    if (pageIndex.isEmpty() && queue.isEmpty() && deferredRAMBytes.get() == 0 && nextPagesToFlush.get() == null)
      return Collections.emptyList();

    synchronized (replayDrainLock(database)) {
      final MutablePage indexed = pageIndex.remove(pageId);
      final List<MutablePage> detached = new ArrayList<>(1);

      // Iterated directly rather than through a snapshot: ArrayBlockingQueue's iterator is weakly consistent and
      // never throws ConcurrentModificationException, and this runs per replayed page, so the copy would be pure
      // overhead. The walk is bounded by the flush queue depth (arcadedb.pageFlushQueue) plus the deferred backlog,
      // which is itself capped by arcadedb.flushSuspendMaxDeferredRAM.
      for (final PagesToFlush batch : queue)
        removePagesOfPageIdFromBatch(batch, pageId, detached);

      removePagesOfPageIdFromBatch(nextPagesToFlush.get(), pageId, detached);

      final ConcurrentLinkedQueue<PagesToFlush> deferred = deferredByDatabase.get(database);
      if (deferred != null)
        for (final PagesToFlush batch : deferred)
          // The copies leave the deferred backlog, so release their reserved RAM accounting (issue #4728).
          addDeferredRAM(database, -removePagesOfPageIdFromBatch(batch, pageId, detached));

      // The indexed copy is missing from every batch only in the mid-enqueue window ruled out above, but adding it
      // here keeps the caller's contract - "the pipeline no longer holds this page" - true without relying on it.
      if (indexed != null && !containsInstance(detached, indexed))
        detached.add(indexed);

      return detached;
    }
  }

  private static boolean containsInstance(final List<MutablePage> pages, final MutablePage page) {
    for (int i = 0; i < pages.size(); i++)
      if (pages.get(i) == page)
        return true;
    return false;
  }

  /** Removes every copy of {@code pageId} from a batch into {@code detached} and returns the sum of their in-RAM size. */
  private long removePagesOfPageIdFromBatch(final PagesToFlush batch, final PageId pageId,
      final List<MutablePage> detached) {
    // pages is null for the SHUTDOWN_THREAD marker.
    if (batch == null || batch.pages == null)
      return 0;

    long removedBytes = 0;
    synchronized (batch.pages) {
      for (final Iterator<MutablePage> it = batch.pages.iterator(); it.hasNext(); ) {
        final MutablePage page = it.next();
        if (pageId.equals(page.getPageId())) {
          it.remove();
          detached.add(page);
          removedBytes += page.getPhysicalSize();
        }
      }
    }
    return removedBytes;
  }

  /** Removes the dropped file's pages from a batch and returns the sum of their in-RAM size (issue #4728). */
  private long removePagesOfFileFromBatch(final PagesToFlush pagesToFlush, final Database database, final int fileId) {
    // pages is null for the SHUTDOWN_THREAD marker.
    if (pagesToFlush.pages == null || !database.equals(pagesToFlush.database))
      return 0;

    long removedBytes = 0;
    synchronized (pagesToFlush.pages) {
      for (final Iterator<MutablePage> it = pagesToFlush.pages.iterator(); it.hasNext(); ) {
        final MutablePage page = it.next();
        if (page.getPageId().getFileId() == fileId) {
          pageIndex.remove(page.getPageId());
          it.remove();
          removedBytes += page.getPhysicalSize();
          // The purged page will never be flushed and its content is irrelevant (its file was dropped):
          // release its WAL ack so the close-time ack gate (#4928) is not tripped by stale pending counts.
          // takeWALFile makes the release exactly-once against the racing flush loop (which does NOT remove
          // pages from this batch list, so both paths can visit the same page).
          final WALFile walFile = page.takeWALFile();
          if (walFile != null)
            walFile.notifyPageFlushed();
        }
      }
    }
    return removedBytes;
  }
}
