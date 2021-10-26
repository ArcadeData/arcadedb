package com.arcadedb.server.http.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ChangeEvent;
import com.arcadedb.utility.Pair;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WebSocketEventBus {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<Pair<UUID, WebSocketChannel>>>>
                               subscribers      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, DatabaseEventWatcherThread>
                               databaseWatchers = new ConcurrentHashMap<>();
  private final ArcadeDBServer arcadeServer;

  public static final String CHANNEL_ID = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void subscribe(String database, String type, WebSocketChannel channel) {
    this.getSubscriberSet(database, type).add(new Pair<>((UUID) channel.getAttribute(CHANNEL_ID), channel));
    if (!this.databaseWatchers.containsKey(database)) this.startDatabaseWatcher(database);
  }

  public void unsubscribe(String database, String type, UUID id) {
    this.getSubscriberSet(database, type).removeIf(pair -> pair.getFirst() == id);
  }

  public void publish(ChangeEvent event) {
    var databaseName = event.getRecord().getDatabase().getName();
    var typeName = event.getRecord().asDocument().getTypeName();

    var databaseSubscribers = this.getSubscriberSet(databaseName, "*");
    var typeSubscribers = this.getSubscriberSet(databaseName, typeName);
    var matchingSubscribers = new HashSet<Pair<UUID, WebSocketChannel>>() {{
      addAll(databaseSubscribers);
      addAll(typeSubscribers);
    }};

    if (matchingSubscribers.isEmpty()) {
      // If we no longer have any subscribers for this database, stop the watcher.
      this.stopDatabaseWatcher(databaseName);
    } else {
      matchingSubscribers.forEach(pair -> WebSockets.sendText(event.toJSON(), pair.getSecond(), null));
    }
  }

  private void startDatabaseWatcher(String database) {
    var queueSize = this.arcadeServer.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_WS_EVENT_BUS_QUEUE_SIZE);
    var watcherThread = new DatabaseEventWatcherThread(this, this.arcadeServer.getDatabase(database), queueSize);
    watcherThread.start();
    this.databaseWatchers.put(database, watcherThread);
  }

  private void stopDatabaseWatcher(String database) {
    this.databaseWatchers.get(database).shutdown();
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

