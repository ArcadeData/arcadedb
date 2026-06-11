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

  /**
   * O(1) index: pageId → most recent MutablePage in the flush queue or currently being flushed.
   * <p>
   * Package-private (instead of private) so the white-box regression test for issue #4544 can set up
   * and assert on entries directly.
   */
  final                ConcurrentHashMap<PageId, MutablePage> pageIndex = new ConcurrentHashMap<>();

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
            if (database == null && isSuspended((Database) pagesToFlush.database)) {
              deferredByDatabase.computeIfAbsent((Database) pagesToFlush.database, k -> new ConcurrentLinkedQueue<>()).offer(pagesToFlush);
              return;
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
              removeFromFlushIndex(page);
            }
          }
        }
      }
    }

    // Phase 2: mark the database as no longer suspended
    suspended.remove(database);

    // Phase 3: drain any batches that were deferred during Phase 1 back into the main queue.
    // These batches were committed while Phase 1 was running; enqueue them so the background
    // thread picks them up. Note: they are appended to the tail of the queue, so if any
    // post-unsuspend commits have already been enqueued they will be flushed first. WAL-based
    // recovery guarantees correctness even if the flush order differs from commit order.
    final ConcurrentLinkedQueue<PagesToFlush> newDeferred = deferredByDatabase.remove(database);
    if (newDeferred != null) {
      for (final PagesToFlush batch : newDeferred) {
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

    return true;
  }

  public boolean isSuspended(final Database database) {
    final Boolean s = suspended.get(database);
    return s != null ? s : false;
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
        removePagesOfFileFromBatch(pagesToFlush, database, fileId);

    // Finally clean any index entry for a page currently being flushed.
    pageIndex.entrySet()
        .removeIf(e -> database.equals(e.getKey().getDatabase()) && e.getKey().getFileId() == fileId);
  }

  private void removePagesOfFileFromBatch(final PagesToFlush pagesToFlush, final Database database, final int fileId) {
    // pages is null for the SHUTDOWN_THREAD marker.
    if (pagesToFlush.pages == null || !database.equals(pagesToFlush.database))
      return;

    synchronized (pagesToFlush.pages) {
      for (final Iterator<MutablePage> it = pagesToFlush.pages.iterator(); it.hasNext(); ) {
        final MutablePage page = it.next();
        if (page.getPageId().getFileId() == fileId) {
          pageIndex.remove(page.getPageId());
          it.remove();
        }
      }
    }
  }
}
