package com.arcadedb.server.http.ws;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class WebSocketEventBus {
  private final ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>> subscribers      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, DatabaseEventWatcherThread>                        databaseWatchers = new ConcurrentHashMap<>();
  private final ArcadeDBServer                                                               arcadeServer;

  public static final String CHANNEL_ID = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void subscribe(final String databaseName, final String type, final Set<ChangeEvent.TYPE> changeTypes,
                        final WebSocketChannel channel) {
    var channelId = (UUID) channel.getAttribute(CHANNEL_ID);
    var databaseSubscribers = this.subscribers.computeIfAbsent(databaseName, k -> new ConcurrentHashMap<>());

    databaseSubscribers.computeIfAbsent(channelId, k -> new EventWatcherSubscription(databaseName, channel))
        .add(type, changeTypes);
    if (!this.databaseWatchers.containsKey(databaseName)) this.startDatabaseWatcher(databaseName);
  }

  public void unsubscribe(String databaseName, UUID id) {
    var databaseSubscribers = this.subscribers.get(databaseName);
    if (databaseSubscribers == null) return;
    databaseSubscribers.remove(id);
    if (databaseSubscribers.isEmpty()) this.stopDatabaseWatcher(databaseName);
  }

  public void publish(ChangeEvent event) {
    var databaseName = event.getRecord().getDatabase().getName();
    var zombieConnections = new ArrayList<UUID>();
    this.subscribers.get(databaseName).values().forEach(subscription -> {
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

  public Collection<EventWatcherSubscription> getDatabaseSubscriptions(String database) {
    return this.subscribers.get(database).values();
  }

  public void unsubscribeAll(UUID id) {
    this.subscribers.forEach((databaseName, subscribers) -> {
      subscribers.remove(id);
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
    var watcher = this.databaseWatchers.remove(database);
    if (watcher != null) watcher.shutdown();
  }
}
