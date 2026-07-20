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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
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
  // Package-private so the white-box regression test for issue #5068 can fabricate an in-flight batch and
  // deterministically exercise an interrupt during waitForCurrentFlushToComplete.
  final                AtomicReference<PagesToFlush>                            nextPagesToFlush    = new AtomicReference<>();
  private final        ConcurrentHashMap<Database, ConcurrentLinkedQueue<PagesToFlush>> deferredByDatabase = new ConcurrentHashMap<>();
  // Per-database lock serializing the suspend-flag check + defer in flushPagesFromQueueToDisk against the
  // flag-clear + deferred-detach in setSuspended(false). Without it a batch could be deferred AFTER the
  // unsuspend already drained the deferred map, leaving it stuck in deferredByDatabase / pageIndex forever.
  // Also the monitor new suspenders wait on while a resume is in flight (see resumingDatabases).
  private final        ConcurrentHashMap<Database, Object>                      suspendLocks        = new ConcurrentHashMap<>();

  /**
   * O(1) index: pageId → most recent MutablePage in the flush queue or currently being flushed.
   * <p>
   * Package-private (instead of private) so the white-box regression test for issue #4544 can set up
   * and assert on entries directly.
   */
  final                ConcurrentHashMap<PageId, MutablePage> pageIndex = new ConcurrentHashMap<>();

  /**
   * Maximum bytes of dirty pages that may sit deferred (in {@link #deferredByDatabase}) while flushing is
   * suspended, before the flush thread stops draining its bounded queue and lets {@code scheduleFlushOfPages}
   * throttle the committing threads. {@code 0} disables the cap (unbounded, pre-#4728 behavior).
   */
  private final        long                                   maxDeferredRAM;

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
   */
  final                AtomicLong                             deferredRAMBytes = new AtomicLong();

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
    }
  }

  public PageManagerFlushThread(final PageManager pageManager, final ContextConfiguration configuration) {
    super("ArcadeDB AsyncFlush");
    setDaemon(false);
    this.pageManager = pageManager;
    this.logContext = LogManager.instance().getContext();
    this.queue = new ArrayBlockingQueue<>(configuration.getValueAsInteger(GlobalConfiguration.PAGE_FLUSH_QUEUE));
    final long maxDeferredMB = configuration.getValueAsLong(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM);
    this.maxDeferredRAM = maxDeferredMB > 0 ? maxDeferredMB * 1024 * 1024 : 0;
  }

  public void scheduleFlushOfPages(final List<MutablePage> pages) throws InterruptedException {
    if (pages.isEmpty())
      // AVOID INSERTING AN EMPTY LIST BECAUSE IS USED TO SHUTDOWN THE THREAD
      return;

    // Index pages BEFORE enqueueing so that getCachedPageFromMutablePageInQueue()
    // can find them even if the queue.offer() hasn't completed yet.
    for (final MutablePage page : pages)
      pageIndex.put(page.getPageId(), page);

    // TRY TO INSERT THE PAGE IN THE QUEUE UNTIL THE THREAD IS STILL RUNNING
    while (running) {
      if (queue.offer(new PagesToFlush(pages), 1, TimeUnit.SECONDS))
        return;
    }

    // Failed to enqueue (shutdown in progress) — remove from index
    for (final MutablePage page : pages)
      pageIndex.remove(page.getPageId());

    LogManager.instance()
        .log(this, Level.SEVERE, "Error on flushing pages %s during shutdown of the database (running=%s queue=%d)", pages, running,
            queue.size());
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

  /**
   * Waits until all the pages of a database are flushed to disk.
   * <p>
   * Uses the {@link #pageIndex} as the authoritative source of truth for pending pages.
   * Entries are added to pageIndex BEFORE enqueueing and removed AFTER flushing each page,
   * so checking pageIndex is race-free unlike checking queue + nextPagesToFlush separately.
   */
  /**
   * @return {@code true} when every page of the database reached the disk, {@code false} when the wait gave
   *     up: either interrupted, or no flush PROGRESS was observed for {@code arcadedb.flushAllPagesTimeout}
   *     milliseconds (#4928 - a wedged flush thread or unwritable disk used to hang close()/rename()/backup
   *     forever here). The window resets whenever the pending-page count decreases, so a healthy but slow
   *     backlog never trips it. Callers on the close path treat {@code false} as a crash-equivalent close:
   *     the WAL files and the lock file are preserved so the next open replays the unflushed pages.
   */
  /** Records that a page of the database LEFT the flush pipeline: the per-database progress signal (#4928). */
  private void bumpFlushProgress(final MutablePage page) {
    final AtomicLong counter = flushedPagesPerDatabase.get(page.getPageId().getDatabase());
    if (counter != null)
      counter.incrementAndGet();
  }

  protected boolean waitAllPagesOfDatabaseAreFlushed(final Database database) {
    final long timeoutMs = database.getConfiguration().getValueAsLong(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT);
    int lastPending = Integer.MAX_VALUE;
    final AtomicLong flushedCounter = flushedPagesPerDatabase.computeIfAbsent(database, k -> new AtomicLong());
    long lastFlushed = flushedCounter.get();
    long lastProgressAt = System.currentTimeMillis();
    while (true) {
      int pending = 0;
      for (final PageId key : pageIndex.keySet()) {
        if (database.equals(key.getDatabase()))
          ++pending;
      }

      if (pending == 0)
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
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  protected void flushPagesFromQueueToDisk(final Database database, final long timeout) throws InterruptedException, IOException {
    // Backpressure (issue #4728): while a database is suspended (HA snapshot ship / full backup) its dirty
    // pages cannot be written to disk and pile up in the unbounded deferredByDatabase map. On a busy leader
    // shipping a multi-GB snapshot this exhausted the heap. Once the deferred backlog crosses the cap, stop
    // draining the bounded queue: it fills up and scheduleFlushOfPages() throttles the committing threads, so
    // the dirty pages stay in the (separately bounded) page cache instead of growing the deferred map forever.
    // Gate on `running`: during shutdown the queue must still be drained to reach the SHUTDOWN_THREAD marker,
    // otherwise a database left suspended with a full backlog would spin the run loop forever and block join().
    if (running && maxDeferredRAM > 0 && deferredRAMBytes.get() >= maxDeferredRAM) {
      Thread.sleep(timeout);
      return;
    }

    final PagesToFlush pagesToFlush = queue.poll(timeout, TimeUnit.MILLISECONDS);

    if (pagesToFlush != null) {
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
                  deferredRAMBytes.addAndGet(batchRAM(pagesToFlush));
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

  public void waitForCurrentFlushToComplete(final Database database) throws InterruptedException {
    PagesToFlush current;
    while ((current = nextPagesToFlush.get()) != null && database.equals(current.database))
      Thread.sleep(1);
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
    final ConcurrentLinkedQueue<PagesToFlush> deferred = deferredByDatabase.remove(database);
    if (deferred != null) {
      for (final PagesToFlush batch : deferred) {
        if (!batch.database.isOpen())
          continue;
        synchronized (batch.pages) {
          for (final MutablePage page : batch.pages) {
            if (!batch.database.isOpen())
              break;
            try {
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
            } finally {
              // The page leaves the deferred backlog (flushed to disk), so release its reserved RAM (issue #4728).
              deferredRAMBytes.addAndGet(-page.getPhysicalSize());
              removeFromFlushIndex(page);
              bumpFlushProgress(page);
            }
          }
        }
      }
    }

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
        deferredRAMBytes.addAndGet(-batchRAM(batch));
        while (running) {
          try {
            if (queue.offer(batch, 1, TimeUnit.SECONDS))
              break;
            LogManager.instance().log(this, Level.WARNING,
                "Page flush queue is full while re-enqueueing deferred batch for database '%s'; retrying", database.getName());
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    // Safety net: once nothing is deferred for any database the backlog is provably empty, so reset the counter
    // to absorb any drift from edge cases above (a batch skipped because its database closed mid-unsuspend).
    if (deferredByDatabase.isEmpty())
      deferredRAMBytes.set(0);

    if (restoreCallerInterrupt)
      // Consumed once during Phase 1: restore it so the caller still observes its own cancellation.
      Thread.currentThread().interrupt();
  }

  public boolean isSuspended(final Database database) {
    final Integer count = suspended.get(database);
    return count != null && count > 0;
  }

  /** Returns the stable per-database monitor used to serialize the suspend check/defer against unsuspend. */
  private Object suspendLock(final Database database) {
    return suspendLocks.computeIfAbsent(database, k -> new Object());
  }

  public void closeAndJoin() throws InterruptedException {
    running = false;
    queue.offer(SHUTDOWN_THREAD);
    join();
  }

  /**
   * Removes a just-flushed page from the {@link #pageIndex}, but ONLY if the indexed value is still the
   * exact same instance that was flushed. A later transaction may have queued a NEWER {@link MutablePage}
   * for the same {@link PageId}; that newer entry must survive so reads keep seeing the latest version.
   * <p>
   * Reference identity ({@code indexed == page}) is used here on purpose instead of {@code remove(key, value)}:
   * {@link BasePage#equals} keys on the mutable {@code version} field, which is an unreliable discriminator
   * for a hash-map value (issue #4544). The atomic {@code computeIfPresent} guarantees the check-and-remove
   * happens as a single operation under the same concurrency guarantees as {@code remove(key, value)}.
   */
  void removeFromFlushIndex(final MutablePage page) {
    pageIndex.computeIfPresent(page.getPageId(), (id, indexed) -> indexed == page ? null : indexed);
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

    // Also clean index entries for pages currently being flushed
    pageIndex.entrySet().removeIf(e -> database.equals(e.getKey().getDatabase()));

    // Forget the per-database suspend bookkeeping so the dropped Database instance (and the resources it
    // pins) can be garbage collected instead of being retained for the flush thread's lifetime as a map key.
    suspendLocks.remove(database);
    suspended.remove(database);
    resumingDatabases.remove(database);
    flushedPagesPerDatabase.remove(database);
    final ConcurrentLinkedQueue<PagesToFlush> droppedDeferred = deferredByDatabase.remove(database);
    if (droppedDeferred != null) {
      for (final PagesToFlush batch : droppedDeferred)
        deferredRAMBytes.addAndGet(-batchRAM(batch));
      if (deferredByDatabase.isEmpty())
        deferredRAMBytes.set(0);
    }
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
        deferredRAMBytes.addAndGet(-removePagesOfFileFromBatch(pagesToFlush, database, fileId));

    // Finally clean any index entry for a page currently being flushed.
    pageIndex.entrySet()
        .removeIf(e -> database.equals(e.getKey().getDatabase()) && e.getKey().getFileId() == fileId);
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
    final MutablePage indexed = pageIndex.remove(pageId);
    final List<MutablePage> detached = new ArrayList<>(1);

    // Iterated directly rather than through a snapshot: ArrayBlockingQueue's iterator is weakly consistent and never
    // throws ConcurrentModificationException, and this runs per replayed page, so the copy would be pure overhead.
    for (final PagesToFlush batch : queue)
      removePagesOfPageIdFromBatch(batch, pageId, detached);

    removePagesOfPageIdFromBatch(nextPagesToFlush.get(), pageId, detached);

    final ConcurrentLinkedQueue<PagesToFlush> deferred = deferredByDatabase.get(database);
    if (deferred != null)
      for (final PagesToFlush batch : deferred)
        // The copies leave the deferred backlog, so release their reserved RAM accounting (issue #4728).
        deferredRAMBytes.addAndGet(-removePagesOfPageIdFromBatch(batch, pageId, detached));

    // The indexed copy is missing from every batch only in the mid-enqueue window ruled out above, but adding it
    // here keeps the caller's contract - "the pipeline no longer holds this page" - true without relying on it.
    if (indexed != null && !containsInstance(detached, indexed))
      detached.add(indexed);

    return detached;
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
