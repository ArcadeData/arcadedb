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
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

public class WebSocketEventBus {
  private final       ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>> subscribers      = new ConcurrentHashMap<>();
  private final       ConcurrentHashMap<String, DatabaseEventWatcherThread>                        databaseWatchers = new ConcurrentHashMap<>();
  // One lock per database orders the subscriber-presence <-> watcher-lifecycle transitions so a watcher is never started
  // twice nor stopped underneath a live subscribe. Publish never takes it, keeping the change-stream hot path lock-free.
  private final       ConcurrentHashMap<String, Object>                                            databaseLocks    = new ConcurrentHashMap<>();
  private final       ArcadeDBServer                                                               arcadeServer;
  public static final String                                                                       CHANNEL_ID       = "ID";
  public static final String                                                                       USER             = "USER";

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
    synchronized (lockFor(databaseName)) {
      final var databaseSubscribers = this.subscribers.computeIfAbsent(databaseName, k -> new ConcurrentHashMap<>());
      databaseSubscribers.computeIfAbsent(channelId, k -> new EventWatcherSubscription(databaseName, channel)).add(type, changeTypes);
      // computeIfAbsent guarantees a single watcher even when two Undertow IO threads subscribe to the same database at once.
      this.databaseWatchers.computeIfAbsent(databaseName, k -> createAndStartWatcher(databaseName));
    }
  }

  public void unsubscribe(final String databaseName, final UUID channelId) {
    DatabaseEventWatcherThread toStop = null;
    synchronized (lockFor(databaseName)) {
      final var databaseSubscribers = this.subscribers.get(databaseName);
      if (databaseSubscribers == null)
        return;
      databaseSubscribers.remove(channelId);
      if (databaseSubscribers.isEmpty())
        toStop = this.databaseWatchers.remove(databaseName);
    }
    // Join the watcher OUTSIDE the lock: shutdown() blocks until the watcher thread terminates, and that thread may need
    // the same per-database lock (unsubscribeAll during publish). Holding the lock across the join would deadlock them.
    if (toStop != null)
      toStop.shutdown();
  }

  public void publish(final ChangeEvent event) {
    final var databaseName = event.getRecord().getDatabase().getName();
    final var databaseSubscribers = this.subscribers.get(databaseName);
    if (databaseSubscribers == null)
      return;

    // Serialize the event ONCE for the whole fan-out instead of once per subscriber: ChangeEvent.toJSON does a full
    // record.toJSON(true) + String materialization, so per-subscriber serialization is O(N x record size) on the single
    // watcher thread and can back up the bounded queue and drop events.
    final String json = event.toJSON();

    // onError may run on an XNIO IO thread after sendText returns, so the zombie collector must be thread-safe.
    final var zombieConnections = new ConcurrentLinkedQueue<UUID>();

    // A single callback shared by every subscriber of this event: onError reads the failing channel from its argument,
    // so there is no need to allocate a new callback per subscriber per event.
    final WebSocketCallback<Void> callback = new WebSocketCallback<>() {
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
    };

    databaseSubscribers.values().forEach(subscription -> {
      try {
        if (subscription.isMatch(event))
          WebSockets.sendText(json, subscription.getChannel(), callback);
      } catch (final Exception e) {
        // NEVER LET A SINGLE SUBSCRIPTION FAILURE KILL THE WATCHER THREAD AND STOP THE WHOLE CHANGE STREAM (ISSUE #4479).
        LogManager.instance().log(this, Level.SEVERE, "Error while publishing change event to subscription %s", e, subscription);
      }
    });

    // Drain zombies AFTER the send loop: unsubscribeAll mutates subscribers, so it can't run while iterating. Draining a
    // ConcurrentLinkedQueue via poll() tolerates concurrent adds from late async onError callbacks without throwing.
    UUID zombie;
    while ((zombie = zombieConnections.poll()) != null)
      this.unsubscribeAll(zombie);
  }

  public Collection<EventWatcherSubscription> getDatabaseSubscriptions(final String database) {
    return this.subscribers.get(database).values();
  }

  public void unsubscribeAll(final UUID channelId) {
    this.subscribers.forEach((databaseName, channels) -> {
      DatabaseEventWatcherThread toStop = null;
      synchronized (lockFor(databaseName)) {
        channels.remove(channelId);
        if (channels.isEmpty())
          toStop = this.databaseWatchers.remove(databaseName);
      }
      // shutdown() outside the lock. When this runs on the watcher thread (zombie cleanup during publish), shutdown()
      // detects the self-call and returns without awaiting, so the run() loop can unwind and unregister its listeners.
      if (toStop != null)
        toStop.shutdown();
    });
  }

  private Object lockFor(final String database) {
    return this.databaseLocks.computeIfAbsent(database, k -> new Object());
  }

  private DatabaseEventWatcherThread createAndStartWatcher(final String database) {
    final var queueSize = this.arcadeServer.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_WS_EVENT_BUS_QUEUE_SIZE);
    final var watcherThread = new DatabaseEventWatcherThread(this, this.arcadeServer.getDatabase(database), queueSize);
    watcherThread.start();
    return watcherThread;
  }
}
