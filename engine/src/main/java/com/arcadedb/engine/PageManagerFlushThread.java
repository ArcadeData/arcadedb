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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Flushes pages to disk asynchronously.
 * <p>
 * A {@link ConcurrentHashMap} ({@code pageIndex}) provides O(1) lookup for pages
 * waiting in the flush queue or currently being flushed.  This replaces the previous
 * O(n) {@code queue.toArray()} scan that allocated a new array on every call and was
 * a major bottleneck under high-throughput ingestion.
 */
public class PageManagerFlushThread extends Thread {
  private final        PageManager                          pageManager;
  public final         ArrayBlockingQueue<PagesToFlush>     queue;
  private final        String                               logContext;
  private volatile     boolean                              running          = true;
  private final        ConcurrentHashMap<Database, Boolean> suspended        = new ConcurrentHashMap<>(); // USED DURING BACKUP
  private final static PagesToFlush                         SHUTDOWN_THREAD  = new PagesToFlush(null);
  private final        AtomicReference<PagesToFlush>        nextPagesToFlush = new AtomicReference<>();

  /** O(1) index: pageId → most recent MutablePage in the flush queue or currently being flushed. */
  private final        ConcurrentHashMap<PageId, MutablePage> pageIndex = new ConcurrentHashMap<>();

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
   * Waits until all the pages of a database are flushed.
   */
  protected void waitAllPagesOfDatabaseAreFlushed(final Database database) {
    // WAIT FOR PENDING THREAD
    PagesToFlush pending = nextPagesToFlush.get();
    while (true) {
      if (pending == null || !database.equals(pending.database))
        break;

      // WAIT UNTIL
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }

      pending = nextPagesToFlush.get();
    }

    if (queue.isEmpty()) {
      // All pages flushed — clean any stale index entries for this database.
      // Entries can linger when remove(key, value) didn't match due to version differences
      // (a newer version of the same page was committed after the older version was indexed).
      pageIndex.keySet().removeIf(k -> database.equals(k.getDatabase()));
      return;
    }

    boolean foundPages;
    do {
      foundPages = false;
      for (final PagesToFlush pages : queue.stream().toList()) {
        if (database.equals(pages.database)) {
          foundPages = true;
          // WAIT UNTIL
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          break;
        }
      }
    } while (foundPages);

    // All pages for this database have been flushed — clean any stale index entries.
    pageIndex.keySet().removeIf(k -> database.equals(k.getDatabase()));
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
            if (!pagesToFlush.database.isOpen())
              return;

            synchronized (pagesToFlush.pages) {
              for (final MutablePage page : pagesToFlush.pages) {
                try {
                  pageManager.flushPage(page);
                } catch (final DatabaseMetadataException e) {
                  // FILE DELETED, CONTINUE WITH THE NEXT PAGES
                  LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
                } finally {
                  // Remove from index AFTER flushing: the page is now on disk and will be
                  // found in the read cache (putPageInReadCache was called at commit time).
                  // Use remove(key, value) so that a NEWER version of the same page (committed
                  // by a later TX while this batch was queued) is NOT removed from the index.
                  // BasePage.equals() compares both pageId and version, so this is safe.
                  pageIndex.remove(page.getPageId(), page);
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

  public boolean setSuspended(final Database database, final boolean value) {
    if (value)
      return suspended.putIfAbsent(database, true) == null;
    suspended.remove(database);
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
}
