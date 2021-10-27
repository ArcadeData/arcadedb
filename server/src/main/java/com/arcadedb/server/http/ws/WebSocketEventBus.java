package com.arcadedb.server.http.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.Pair;
import io.undertow.websockets.core.WebSockets;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WebSocketEventBus {
  private final ConcurrentHashMap<String, List<Pair<UUID, EventWatcherSubscription>>> subscribers      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, DatabaseEventWatcherThread>                 databaseWatchers = new ConcurrentHashMap<>();
  private final ArcadeDBServer                                                        arcadeServer;

  public static final String CHANNEL_ID = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void subscribe(EventWatcherSubscription subscription) {
    var channelId = (UUID) subscription.getChannel().getAttribute(CHANNEL_ID);
    this.getSubscriberSet(subscription.getDatabase()).add(new Pair<>(channelId, subscription));
    if (!this.databaseWatchers.containsKey(subscription.getDatabase())) {
      this.startDatabaseWatcher(subscription.getDatabase());
    }
  }

  public void unsubscribe(String database, UUID id) {
    this.getSubscriberSet(database).removeIf(pair -> pair.getFirst() == id);
  }

  public void publish(ChangeEvent event) {
    var databaseName = event.getRecord().getDatabase().getName();

    var subscribers = this.subscribers.get(databaseName);
    if (subscribers == null) return;

    if (subscribers.isEmpty()) {
      // If we no longer have any subscribers for this database, stop the watcher.
      this.stopDatabaseWatcher(databaseName);
    } else {
      subscribers.forEach(pair -> {
        var subscription = pair.getSecond();
        if (subscription.isMatch(event)) {
          WebSockets.sendText(event.toJSON(), subscription.getChannel(), null);
        }
      });
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

  private List<Pair<UUID, EventWatcherSubscription>> getSubscriberSet(String database) {
    return this.subscribers.computeIfAbsent(database, key -> new LinkedList<>());
  }

  public void unsubscribeAll(UUID id) {
    this.subscribers.values().forEach(sets -> sets.removeIf(pair -> pair.getFirst() == id));
  }
}

