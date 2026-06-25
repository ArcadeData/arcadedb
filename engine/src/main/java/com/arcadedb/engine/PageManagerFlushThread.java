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
import java.util.Iterator;
import java.util.List;
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
  private final        ConcurrentHashMap<Database, Boolean>                     suspended           = new ConcurrentHashMap<>(); // USED DURING BACKUP
  private final static PagesToFlush                                             SHUTDOWN_THREAD     = new PagesToFlush(null);
  private final        AtomicReference<PagesToFlush>                            nextPagesToFlush    = new AtomicReference<>();
  private final        ConcurrentHashMap<Database, ConcurrentLinkedQueue<PagesToFlush>> deferredByDatabase = new ConcurrentHashMap<>();
  // Per-database lock serializing the suspend-flag check + defer in flushPagesFromQueueToDisk against the
  // flag-clear + deferred-detach in setSuspended(false). Without it a batch could be deferred AFTER the
  // unsuspend already drained the deferred map, leaving it stuck in deferredByDatabase / pageIndex forever.
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
   * Running total of bytes currently deferred across all suspended databases. Package-private so the white-box
   * regression test for issue #4728 can assert the backlog stays bounded.
   */
  final                AtomicLong                             deferredRAMBytes = new AtomicLong();

  public static class PagesToFlush {
    public final BasicDatabase     database;
    public final List<MutablePage> pages;

    public PagesToFlush(final List<MutablePage> pages) {
      this.pages = pages;
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
  protected void waitAllPagesOfDatabaseAreFlushed(final Database database) {
    while (true) {
      boolean hasPendingPages = false;
      for (final PageId key : pageIndex.keySet()) {
        if (database.equals(key.getDatabase())) {
          hasPendingPages = true;
          break;
        }
      }

      if (!hasPendingPages)
        return;

      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
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
                } catch (final DatabaseMetadataException e) {
                  // FILE DELETED, CONTINUE WITH THE NEXT PAGES
                  LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
                } finally {
                  // Remove from index AFTER flushing: the page is now on disk and will be
                  // found in the read cache (putPageInReadCache was called at commit time).
                  // Reference identity ensures a NEWER MutablePage for the same PageId (queued
                  // by a later TX while this batch was waiting) is NOT removed from the index.
                  removeFromFlushIndex(page);
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

  public boolean setSuspended(final Database database, final boolean value) {
    if (value)
      return suspended.putIfAbsent(database, true) == null;

    // Phase 1: synchronously flush all deferred batches accumulated while suspended
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
            } catch (final IOException e) {
              LogManager.instance().log(this, Level.WARNING, "Error on flushing deferred page '%s' to disk", e, page);
            } finally {
              // The page leaves the deferred backlog (flushed to disk), so release its reserved RAM (issue #4728).
              deferredRAMBytes.addAndGet(-page.getPhysicalSize());
              removeFromFlushIndex(page);
            }
          }
        }
      }
    }

    // Phase 2 + Phase 3a: under the per-database lock, clear the suspended flag AND atomically detach any
    // batches deferred during Phase 1. Holding the lock makes this transition mutually exclusive with the
    // suspended-check + defer in flushPagesFromQueueToDisk: once the flag is cleared no further batch can be
    // added to deferredByDatabase for this database, and every batch deferred up to that point is detached
    // exactly once. A single detach therefore suffices - nothing can repopulate the map behind us.
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

    return true;
  }

  public boolean isSuspended(final Database database) {
    final Boolean s = suspended.get(database);
    return s != null ? s : false;
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
        }
      }
    }
    return removedBytes;
  }
}
