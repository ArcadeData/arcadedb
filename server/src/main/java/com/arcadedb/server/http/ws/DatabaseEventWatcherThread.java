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
package com.arcadedb.server.http.ws;

import com.arcadedb.database.Database;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

final public class DatabaseEventWatcherThread extends Thread {
  private final    WebSocketEventBus               eventBus;
  private final    ArrayBlockingQueue<ChangeEvent> eventQueue;
  private final    Database                        database;
  private volatile boolean                         running = true;
  private final    CountDownLatch                  runningLock;
  private final    WebSocketEventListener          listener;

  public DatabaseEventWatcherThread(final WebSocketEventBus eventBus, final Database database, final int queueSize) {
    super("WS-Events-" + database.getName());
    this.eventBus = eventBus;
    this.eventQueue = new ArrayBlockingQueue<>(queueSize);
    this.database = database;
    this.listener = new WebSocketEventListener(this);
    this.runningLock = new CountDownLatch(1);

    this.database.getEvents().registerListener((AfterRecordCreateListener) listener).registerListener((AfterRecordUpdateListener) listener)
        .registerListener((AfterRecordDeleteListener) listener);
  }

  public void push(final ChangeEvent event) {
    if (!running)
      // NOT RUNNING
      return;

    if (!this.eventQueue.offer(event)) {
      LogManager.instance().log(this, Level.WARNING, "Skipping event for database %s as eventQueue is full. Consider increasing eventBusQueueSize", null,
          this.database.getName());
    }
  }

  public boolean isRunning() {
    return running;
  }

  /**
   * Sends the shutdown signal to the thread and waits for termination.
   */
  public void shutdown() {
    if (!running)
      return;

    this.running = false;
    try {
      runningLock.await();
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  @Override
  public void run() {
    try {
      while (this.running) {
        var event = this.eventQueue.poll(500, TimeUnit.MILLISECONDS);
        if (event == null)
          continue;
        this.eventBus.publish(event);
      }

    } catch (InterruptedException ignored) {
    } finally {
      try {
        this.database.getEvents().unregisterListener((AfterRecordCreateListener) listener).unregisterListener((AfterRecordUpdateListener) listener)
            .unregisterListener((AfterRecordDeleteListener) listener);

        eventQueue.clear();
      } finally {
        runningLock.countDown();
      }
    }
  }
}
