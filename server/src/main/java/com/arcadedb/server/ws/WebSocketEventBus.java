package com.arcadedb.server.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ChangeEvent;
import com.arcadedb.utility.Pair;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class WebSocketEventBus {
  private final ArrayBlockingQueue<ChangeEvent> eventQueue;
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<Pair<UUID, WebSocketChannel>>>>
                                                subscribers      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Pair<WebSocketEventListener, Thread>>
                                                databaseWatchers = new ConcurrentHashMap<>();
  private final ArcadeDBServer                  arcadeServer;

  public static final String CHANNEL_ID = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
    this.eventQueue = new ArrayBlockingQueue<>(server.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_WS_EVENT_BUS_QUEUE_SIZE), true);
  }

  public void push(ChangeEvent event) {
    try {
      this.eventQueue.add(event);
    } catch (IllegalStateException ex) {
      this.arcadeServer.log(this, Level.WARNING, "Skipping event as eventQueue is full. Consider increasing eventBusQueueSize.");
    }
  }

  public void subscribe(String database, String type, WebSocketChannel channel) {
    this.getSubscriberSet(database, type).add(new Pair<>((UUID) channel.getAttribute(CHANNEL_ID), channel));
    if (!this.databaseWatchers.containsKey(database)) this.startDatabaseWatcher(database);
  }

  public void unsubscribe(String database, String type, UUID id) {
    this.getSubscriberSet(database, type).removeIf(pair -> pair.getFirst() == id);
  }

  private void startDatabaseWatcher(String database) {
    WebSocketEventListener listener = new WebSocketEventListener(this);
    this.arcadeServer.getDatabase(database).getEvents()
        .registerListener((AfterRecordCreateListener) listener)
        .registerListener((AfterRecordUpdateListener) listener)
        .registerListener((AfterRecordDeleteListener) listener);

    var watcherThread = new Thread(() -> {
      try {
        this.arcadeServer.log(this, Level.INFO, "Starting up watcher thread for %s.", database);

        while (true) {
          var event = eventQueue.take();
          var databaseWatchers = this.getSubscriberSet(database, "*");
          var typeWatchers = this.getSubscriberSet(database, event.getRecord().asDocument().getTypeName());
          var matchingSubscribers = new HashSet<Pair<UUID, WebSocketChannel>>() {{
            addAll(databaseWatchers);
            addAll(typeWatchers);
          }};

          var json = event.toJSON();
          if (matchingSubscribers.isEmpty()) {
            this.stopDatabaseWatcher(database);
          } else {
            matchingSubscribers.forEach(pair -> WebSockets.sendText(json, pair.getSecond(), null));
          }
        }
      } catch (InterruptedException ignored) {
        this.arcadeServer.log(this, Level.INFO, "Shutting down watcher thread for %s.", database);
      }
    });
    watcherThread.start();

    this.databaseWatchers.put(database, new Pair<>(listener, watcherThread));
  }

  private void stopDatabaseWatcher(String database) {
    var pair = this.databaseWatchers.get(database);
    this.arcadeServer.getDatabase(database).getEvents()
        .unregisterListener((AfterRecordCreateListener) pair.getFirst())
        .unregisterListener((AfterRecordUpdateListener) pair.getFirst())
        .unregisterListener((AfterRecordDeleteListener) pair.getFirst());

    pair.getSecond().interrupt();
    this.databaseWatchers.remove(database);
  }

  private Set<Pair<UUID, WebSocketChannel>> getSubscriberSet(String database, String typeFilter) {
    var type = typeFilter == null || typeFilter.trim().isEmpty() ? "*" : typeFilter;
    return this.subscribers
        .computeIfAbsent(database, key -> new ConcurrentHashMap<>())
        .computeIfAbsent(type, key -> new HashSet<>());
  }

  public void unsubscribeAll(UUID id) {
    this.subscribers.values().forEach(types -> types.values().forEach(sets -> sets.removeIf(pair -> pair.getFirst() == id)));
  }
}

