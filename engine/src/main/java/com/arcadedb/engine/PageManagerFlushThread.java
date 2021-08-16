/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.engine;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Flushes pages to disk asynchronously.
 */
public class PageManagerFlushThread extends Thread {
  private final    PageManager                     pageManager;
  public final     ArrayBlockingQueue<MutablePage> queue;
  private final    String                          logContext;
  private volatile boolean                         running = true;

  public PageManagerFlushThread(final PageManager pageManager, final ContextConfiguration configuration) {
    super("ArcadeDB AsyncFlush");
    setDaemon(false);
    this.pageManager = pageManager;
    this.logContext = LogManager.instance().getContext();
    this.queue = new ArrayBlockingQueue<>(configuration.getValueAsInteger(GlobalConfiguration.PAGE_FLUSH_QUEUE));
  }

  public void asyncFlush(final MutablePage page) throws InterruptedException {
    LogManager.instance().log(this, Level.FINE, "Enqueuing flushing page %s in background...", null, page);

    // TRY TO INSERT THE PAGE IN THE QUEUE UNTIL THE THREAD IS STILL RUNNING
    while (running) {
      if (queue.offer(page, 1, TimeUnit.SECONDS))
        return;
    }

    LogManager.instance().log(this, Level.SEVERE, "Error on flushing page %s during shutdown of the database", null, page);
  }

  @Override
  public void run() {
    if (logContext != null)
      LogManager.instance().setContext(logContext);

    while (running || !queue.isEmpty()) {
      try {
        flushStream();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        running = false;
        return;
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on processing page flush requests", e);
      }
    }
  }

  private void flushStream() throws InterruptedException, IOException {
    final MutablePage page = queue.poll(300l, TimeUnit.MILLISECONDS);

    if (page != null) {
      if (LogManager.instance().isDebugEnabled())
        LogManager.instance().log(this, Level.FINE, "Flushing page %s in bg...", null, page);

      pageManager.flushPage(page);
    }
  }

  public void close() {
    running = false;
  }
}
