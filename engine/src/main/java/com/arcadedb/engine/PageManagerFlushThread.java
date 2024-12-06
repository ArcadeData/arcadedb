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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Flushes pages to disk asynchronously.
 */
public class PageManagerFlushThread extends Thread {
  private final        PageManager                           pageManager;
  public final         ArrayBlockingQueue<List<MutablePage>> queue;
  private final        String                                logContext;
  private volatile     boolean                               running          = true;
  private final        ConcurrentHashMap<Database, Boolean>  suspended        = new ConcurrentHashMap<>(); // USED DURING BACKUP
  private final static List<MutablePage>                     SHUTDOWN_THREAD  = new ArrayList<>();
  private final        AtomicReference<List<MutablePage>>    nextPagesToFlush = new AtomicReference<>();

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

    // TRY TO INSERT THE PAGE IN THE QUEUE UNTIL THE THREAD IS STILL RUNNING
    while (running) {
      if (queue.offer(pages, 1, TimeUnit.SECONDS))
        return;
    }

    LogManager.instance()
        .log(this, Level.SEVERE, "Error on flushing pages %s during shutdown of the database (running=%s queue=%d)", pages,
            running, queue.size());
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
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on processing page flush requests", e);
      }
    }
  }

  /**
   * Browse all the pages and flush to disk the pages that belong to a specific database
   *
   * @param database
   */
  protected void flushAllPagesOfDatabase(final Database database) {
    // FLUSH PENDING PAGES FROM THREAD
    final List<MutablePage> pending = nextPagesToFlush.get();
    if (pending != null)
      flushPagesOfDatabase(database, pending);

    if (queue.isEmpty())
      return;

    for (final List<MutablePage> pages : queue.stream().toList())
      flushPagesOfDatabase(database, pages);
  }

  private void flushPagesOfDatabase(Database database, List<MutablePage> pages) {
    if (!pages.isEmpty())
      synchronized (pages) {
        if (pages.get(0).getPageId().getDatabase().equals(database))
          for (final Iterator<MutablePage> it = pages.iterator(); it.hasNext(); ) {
            final MutablePage page = it.next();
            try {
              pageManager.flushPage(page);
              it.remove();
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
            }
          }
      }
  }

  protected void flushPagesFromQueueToDisk(final Database database, final long timeout) throws InterruptedException, IOException {
    final List<MutablePage> pages = queue.poll(timeout, TimeUnit.MILLISECONDS);

    if (pages != null) {
      if (pages == SHUTDOWN_THREAD)
        // SPECIAL CONTENT FOR SHUTDOWN
        running = false;
      else if (!pages.isEmpty()) {
        final PageId firstPage = pages.get(0).pageId;
        if (database == null || firstPage.getDatabase().equals(database)) {
          // SET THE PAGES TO FLUSH TO BE RETRIEVED BY A CONCURRENT DB CLOSE = FORCE FLUSH OF PAGES
          nextPagesToFlush.set(pages);
          try {
            // EXECUTE THE FLUSH IN A DB READ LOCK TO PREVENT CONCURRENT CLOSING
            ((DatabaseInternal) firstPage.getDatabase()).executeInReadLock(() -> {
              synchronized (pages) {
                for (final MutablePage page : pages) {
                  try {
                    pageManager.flushPage(page);
                  } catch (final DatabaseMetadataException e) {
                    // FILE DELETED, CONTINUE WITH THE NEXT PAGES
                    LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
                  }
                }
              }
              return null;
            });
          } finally {
            nextPagesToFlush.set(null);
          }
        }
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
    final Object[] content = queue.toArray();
    for (int i = 0; i < content.length; i++) {
      final List<MutablePage> pages = (List<MutablePage>) content[i];
      if (pages != null) {
        synchronized (pages) {
          for (int j = 0; j < pages.size(); j++) {
            final MutablePage page = pages.get(j);
            if (page.getPageId().equals(pageId))
              return new CachedPage(page, true);
          }
        }
      }
    }
    return null;
  }

  public void removeAllPagesOfDatabase(final Database database) {
    for (final List<MutablePage> pages : queue.stream().toList())
      synchronized (pages) {
        pages.removeIf(page -> page.getPageId().getDatabase().equals(database));
      }
  }
}
