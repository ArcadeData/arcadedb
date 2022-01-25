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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class WebSocketEventBus {
  private final       ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>> subscribers      = new ConcurrentHashMap<>();
  private final       ConcurrentHashMap<String, DatabaseEventWatcherThread>                        databaseWatchers = new ConcurrentHashMap<>();
  private final       ArcadeDBServer                                                               arcadeServer;
  public static final String                                                                       CHANNEL_ID       = "ID";

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void stop() {
    subscribers.values().forEach(x -> x.values().forEach(y -> y.close()));
    subscribers.clear();
    databaseWatchers.values().forEach(x -> x.shutdown());
    databaseWatchers.clear();
  }

  public void subscribe(final String databaseName, final String type, final Set<ChangeEvent.TYPE> changeTypes, final WebSocketChannel channel) {
    final var channelId = (UUID) channel.getAttribute(CHANNEL_ID);
    final var databaseSubscribers = this.subscribers.computeIfAbsent(databaseName, k -> new ConcurrentHashMap<>());

    databaseSubscribers.computeIfAbsent(channelId, k -> new EventWatcherSubscription(databaseName, channel)).add(type, changeTypes);

    if (!this.databaseWatchers.containsKey(databaseName))
      this.startDatabaseWatcher(databaseName);
  }

  public void unsubscribe(final String databaseName, final UUID channelId) {
    final var databaseSubscribers = this.subscribers.get(databaseName);
    if (databaseSubscribers == null)
      return;
    databaseSubscribers.remove(channelId);
    if (databaseSubscribers.isEmpty())
      this.stopDatabaseWatcher(databaseName);
  }

  public void publish(final ChangeEvent event) {
    final var databaseName = event.getRecord().getDatabase().getName();
    final var zombieConnections = new ArrayList<UUID>();
    this.subscribers.get(databaseName).values().forEach(subscription -> {
      if (subscription.isMatch(event)) {
        WebSockets.sendText(event.toJSON(), subscription.getChannel(), new WebSocketCallback<>() {
          @Override
          public void complete(final WebSocketChannel webSocketChannel, final Void unused) {
            webSocketChannel.flush();
          }

          @Override
          public void onError(final WebSocketChannel webSocketChannel, final Void unused, final Throwable throwable) {
            final var channelId = (UUID) webSocketChannel.getAttribute(CHANNEL_ID);
            if (throwable instanceof IOException) {
              LogManager.instance().log(this, Level.FINE, "Closing zombie connection: %s", null, channelId);
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

  public Collection<EventWatcherSubscription> getDatabaseSubscriptions(final String database) {
    return this.subscribers.get(database).values();
  }

  public void unsubscribeAll(final UUID channelId) {
    this.subscribers.forEach((databaseName, channels) -> {
      channels.remove(channelId);
      if (channels.isEmpty())
        this.stopDatabaseWatcher(databaseName);
    });
  }

  private void startDatabaseWatcher(final String database) {
    final var queueSize = this.arcadeServer.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_WS_EVENT_BUS_QUEUE_SIZE);
    final var watcherThread = new DatabaseEventWatcherThread(this, this.arcadeServer.getDatabase(database), queueSize);
    watcherThread.start();
    this.databaseWatchers.put(database, watcherThread);
  }

  private void stopDatabaseWatcher(final String database) {
    var watcher = this.databaseWatchers.remove(database);
    if (watcher != null)
      watcher.shutdown();
  }
}
