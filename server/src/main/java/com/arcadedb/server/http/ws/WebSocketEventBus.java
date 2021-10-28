package com.arcadedb.server.http.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.Pair;
import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

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
    var subscriberSet = this.subscribers.computeIfAbsent(subscription.getDatabase(),
        key -> Collections.synchronizedList(new ArrayList<>()));

    subscriberSet.add(new Pair<>(channelId, subscription));
    if (!this.databaseWatchers.containsKey(subscription.getDatabase())) {
      this.startDatabaseWatcher(subscription.getDatabase());
    }
  }

  public void unsubscribe(String databaseName, UUID id) {
    var subscriberSet = this.subscribers.get(databaseName);
    if (subscriberSet == null) return;
    subscriberSet.removeIf(pair -> pair.getFirst() == id);
    if (subscriberSet.isEmpty()) this.stopDatabaseWatcher(databaseName);
  }

  public void publish(ChangeEvent event) {
    var databaseName = event.getRecord().getDatabase().getName();
    var zombieConnections = new ArrayList<UUID>();
    this.subscribers.get(databaseName).forEach(pair -> {
      var subscription = pair.getSecond();
      if (subscription.isMatch(event)) {
        WebSockets.sendText(event.toJSON(), subscription.getChannel(), new WebSocketCallback<>() {
          @Override
          public void complete(WebSocketChannel webSocketChannel, Void unused) {
            // ignored
          }

          @Override
          public void onError(WebSocketChannel webSocketChannel, Void unused, Throwable throwable) {
            var channelId = (UUID) webSocketChannel.getAttribute(CHANNEL_ID);
            if (throwable instanceof IOException) {
              LogManager.instance().log(this, Level.INFO, "Closing zombie connection: %s", null, channelId);
              zombieConnections.add(channelId);
            } else {
              LogManager.instance().log(this, Level.SEVERE, "Unexpected error while sending message.", throwable);
            }
          }
        });
      }
    });

    // This will mutate subscribers, so we can't do it while iterating!
    zombieConnections.forEach(this::unsubscribeAll);
  }

  public List<Pair<UUID, EventWatcherSubscription>> getDatabaseSubscriptions(String database) {
    return this.subscribers.get(database);
  }

  public void unsubscribeAll(UUID id) {
    this.subscribers.forEach((databaseName, subscribers) -> {
      subscribers.removeIf(pair -> pair.getFirst() == id);
      if (subscribers.isEmpty()) this.stopDatabaseWatcher(databaseName);
    });
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
}
