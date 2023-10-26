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
  private final    PageManager                           pageManager;
  public final     ArrayBlockingQueue<List<MutablePage>> queue;
  private final    String                                logContext;
  private volatile boolean                               running   = true;
  private final    AtomicBoolean                         suspended = new AtomicBoolean(false); // USED DURING BACKUP

  public PageManagerFlushThread(final PageManager pageManager, final ContextConfiguration configuration,
      final String databaseName) {
    super("ArcadeDB AsyncFlush " + databaseName);
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

    LogManager.instance().log(this, Level.SEVERE, "Error on flushing pages %s during shutdown of the database", pages);
  }

  @Override
  public void run() {
    if (logContext != null)
      LogManager.instance().setContext(logContext);

    while (running || !queue.isEmpty()) {
      try {
        if (suspended.get()) {
          // FLUSH SUSPENDED (BACKUP IN PROGRESS?)
          Thread.sleep(100);
          continue;
        }

        flushPagesFromQueueToDisk(1_000);

      } catch (final InterruptedException e) {
        running = false;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on processing page flush requests", e);
      }
    }
  }

  protected void flushPagesFromQueueToDisk(final long timeout) throws InterruptedException, IOException {
    final List<MutablePage> pages = queue.poll(timeout, TimeUnit.MILLISECONDS);

    if (pages != null) {
      if (pages.isEmpty())
        running = false;
      else
        for (final MutablePage page : pages)
          try {
            pageManager.flushPage(page);
          } catch (final DatabaseMetadataException e) {
            // FILE DELETED, CONTINUE WITH THE NEXT PAGES
            LogManager.instance().log(this, Level.WARNING, "Error on flushing page '%s' to disk", e, page);
          }
    }
  }

  public void setSuspended(final boolean value) {
    suspended.set(value);
  }

  public boolean isSuspended() {
    return suspended.get();
  }

  public void closeAndJoin() throws InterruptedException {
    running = false;
    queue.offer(Collections.emptyList()); // EMPTY LIST MEANS SHUTDOWN OF THE THREAD
    join();
  }
}
