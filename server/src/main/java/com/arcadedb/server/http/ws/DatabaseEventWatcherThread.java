package com.arcadedb.server.http.ws;

import com.arcadedb.database.Database;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.concurrent.*;
import java.util.logging.*;

final public class DatabaseEventWatcherThread extends Thread {
  private final    WebSocketEventBus               eventBus;
  private final    ArrayBlockingQueue<ChangeEvent> eventQueue;
  private final    Database                        database;
  private volatile boolean                         running = true;
  private          CountDownLatch                  runningLock;

  public DatabaseEventWatcherThread(final WebSocketEventBus eventBus, final Database database, final int queueSize) {
    super("WS-Events-" + database.getName());
    this.eventBus = eventBus;
    this.eventQueue = new ArrayBlockingQueue<>(queueSize);
    this.database = database;
  }

  public void push(ChangeEvent event) {
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
    this.running = false;
    try {
      runningLock.await();
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  @Override
  public void run() {
    var listener = new WebSocketEventListener(this);

    runningLock = new CountDownLatch(1);
    try {
      LogManager.instance().log(this, Level.INFO, "Starting up watcher thread for %s.", null, database);

      this.database.getEvents().registerListener((AfterRecordCreateListener) listener).registerListener((AfterRecordUpdateListener) listener)
          .registerListener((AfterRecordDeleteListener) listener);

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

        LogManager.instance().log(this, Level.INFO, "Shutting down watcher thread for %s.", null, database);
        eventQueue.clear();
      } finally {
        runningLock.countDown();
      }
    }
  }
}
