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
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.log.LogManager;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

/**
 * Flushes pages to disk asynchronously.
 */
public class PageManagerFlushThread extends Thread {
  private final    PageManager                           pageManager;
  public final     ArrayBlockingQueue<List<MutablePage>> queue;
  private final    String                                logContext;
  private volatile boolean                               running   = true;
  private final    ConcurrentHashMap<Database, Boolean>  suspended = new ConcurrentHashMap<>(); // USED DURING BACKUP

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
  protected void flushModifiedPagesOfDatabase(final Database database) {
    if (queue.isEmpty())
      return;

    for (final List<MutablePage> pages : queue.stream().toList()) {
      synchronized (pages) {
        for (final Iterator<MutablePage> it = pages.iterator(); it.hasNext(); ) {
          final MutablePage page = it.next();
          if (page.getPageId().getDatabase().equals(database))
            try {
              pageManager.flushPage(page);
              it.remove();
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
            }
        }
      }
    }
  }

  protected void flushPagesFromQueueToDisk(final Database database, final long timeout) throws InterruptedException, IOException {
    final List<MutablePage> pages = queue.poll(timeout, TimeUnit.MILLISECONDS);

    if (pages != null) {
      if (pages.isEmpty())
        // EMPTY PAGES IS A SPECIAL CONTENT FOR SHUTDOWN
        running = false;
      else {
        final PageId firstPage = pages.get(0).pageId;
        if (database == null || firstPage.getDatabase().equals(database)) {
          // EXECUTE THE FLUSH IN A DB READ LOCK TO PREVENT CONCURRENT CLOSING
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
    queue.offer(Collections.emptyList()); // EMPTY LIST MEANS SHUTDOWN OF THE THREAD
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
