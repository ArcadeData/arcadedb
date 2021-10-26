package com.arcadedb.server.http.ws;

import com.arcadedb.database.Database;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ChangeEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

final public class DatabaseEventWatcherThread extends Thread {
  private final WebSocketEventBus               eventBus;
  private final ArrayBlockingQueue<ChangeEvent> eventQueue;
  private final Database                        database;

  volatile boolean                running = true;
  private  WebSocketEventListener listener;

  public boolean isRunning() {
    return running;
  }

  public void shutdown() {
    this.running = false;
  }

  public DatabaseEventWatcherThread(final WebSocketEventBus eventBus, final Database database, final int queueSize) {
    this.eventBus = eventBus;
    this.eventQueue = new ArrayBlockingQueue<>(queueSize, true);
    this.database = database;
  }

  public void push(ChangeEvent event) {
    if (!this.eventQueue.offer(event)) {
      LogManager.instance().log(this, Level.WARNING, "Skipping event for database %s as eventQueue is full. Consider increasing eventBusQueueSize.",
          null, this.database.getName());
    }
  }

  @Override
  public void run() {
    try {
      LogManager.instance().log(this, Level.INFO, "Starting up watcher thread for %s.", null, database);

      this.listener = new WebSocketEventListener(this);
      this.database.getEvents()
          .registerListener((AfterRecordCreateListener) this.listener)
          .registerListener((AfterRecordUpdateListener) this.listener)
          .registerListener((AfterRecordDeleteListener) this.listener);

      while (this.running) {
        var event = this.eventQueue.poll(500, TimeUnit.MILLISECONDS);
        if (event == null) continue;
        this.eventBus.publish(event);
      }

    } catch (InterruptedException ignored) {
    } finally {
      this.database.getEvents()
          .unregisterListener((AfterRecordCreateListener) this.listener)
          .unregisterListener((AfterRecordUpdateListener) this.listener)
          .unregisterListener((AfterRecordDeleteListener) this.listener);

      LogManager.instance().log(this, Level.INFO, "Shutting down watcher thread for %s.", null, database);
    }
  }
}
