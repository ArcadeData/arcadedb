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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
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
      if (databaseSubscribers.isEmpty()) {
        toStop = this.databaseWatchers.remove(databaseName);
        // Flip the watcher off while still holding the lock (issue #6762): a subscribe() that wins the lock in the
        // gap between here and the shutdown() below would otherwise start a second watcher and register its
        // listeners while this one is still running with its own attached, so a commit in that window would be
        // enqueued on both and published twice to the very same subscriber.
        if (toStop != null)
          toStop.signalStop();
      }
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

    // Charged per subscriber before the send and released when Undertow reports the frame done, so a subscriber that
    // stops reading cannot accumulate an unbounded send backlog on the server's heap (issue #6762).
    final int messageSize = json.length();
    final long maxPendingBytes = this.arcadeServer != null ?
        this.arcadeServer.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES) :
        GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.getValueAsLong();

    // A single callback shared by every subscriber of this event: onError reads the failing channel from its argument,
    // so there is no need to allocate a new callback per subscriber per event.
    final WebSocketCallback<Void> callback = new WebSocketCallback<>() {
      @Override
      public void complete(final WebSocketChannel webSocketChannel, final Void unused) {
        releasePending(webSocketChannel);
        webSocketChannel.flush();
      }

      @Override
      public void onError(final WebSocketChannel webSocketChannel, final Void unused, final Throwable throwable) {
        releasePending(webSocketChannel);
        final var channelId = (UUID) webSocketChannel.getAttribute(CHANNEL_ID);
        if (throwable instanceof IOException) {
          LogManager.instance().log(this, Level.FINE, "Closing zombie connection: %s", null, channelId);
          zombieConnections.add(channelId);
        } else {
          LogManager.instance().log(this, Level.SEVERE, "Unexpected error while sending message.", throwable);
        }
      }

      private void releasePending(final WebSocketChannel webSocketChannel) {
        final var subscription = databaseSubscribers.get((UUID) webSocketChannel.getAttribute(CHANNEL_ID));
        if (subscription != null)
          subscription.releasePending(messageSize);
      }
    };

    databaseSubscribers.forEach((channelId, subscription) -> {
      try {
        if (!subscription.isMatch(event))
          return;

        // Authorization is re-checked on DELIVERY, not only at SUBSCRIBE time (issue #6762): the identity is
        // captured once at the handshake, so revoking a user's access to the database - or dropping the user
        // outright - otherwise left the change stream flowing until the client happened to disconnect.
        if (!isStillAuthorized(subscription, databaseName)) {
          LogManager.instance().log(this, Level.WARNING,
              "Dropping change-stream subscription %s: the user is no longer authorized on database '%s'", null,
              channelId, databaseName);
          zombieConnections.add(channelId);
          subscription.close();
          return;
        }

        if (!subscription.reservePending(messageSize, maxPendingBytes)) {
          LogManager.instance().log(this, Level.WARNING,
              "Evicting slow change-stream subscriber %s on database '%s': more than %d bytes are still outstanding "
                  + "towards it. Raise " + GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.getKey()
                  + " if this is a legitimately bursty consumer", null, channelId, databaseName, maxPendingBytes);
          zombieConnections.add(channelId);
          subscription.close();
          return;
        }

        try {
          WebSockets.sendText(json, subscription.getChannel(), callback);
        } catch (final Exception e) {
          // The frame never reached Undertow, so nothing is outstanding: give the reservation back rather than let
          // a channel that fails synchronously look like a slow consumer.
          subscription.releasePending(messageSize);
          throw e;
        }
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
        if (channels.isEmpty()) {
          toStop = this.databaseWatchers.remove(databaseName);
          // See unsubscribe(): stop accepting events while the lock still excludes a concurrent subscribe.
          if (toStop != null)
            toStop.signalStop();
        }
      }
      // shutdown() outside the lock. When this runs on the watcher thread (zombie cleanup during publish), shutdown()
      // detects the self-call and returns without awaiting, so the run() loop can unwind and unregister its listeners.
      if (toStop != null)
        toStop.shutdown();
    });
  }

  /**
   * Whether the identity that opened this channel may still read {@code databaseName}.
   * <p>
   * The {@link ServerSecurityUser} captured at the handshake is a snapshot: {@code ServerSecurity.updateUser}
   * replaces the instance and {@code dropUser} removes it, so the current grant has to be re-resolved by name. A
   * channel with no identity attached (embedded/test wiring, where no security is installed) is left alone.
   */
  private boolean isStillAuthorized(final EventWatcherSubscription subscription, final String databaseName) {
    final WebSocketChannel channel = subscription.getChannel();
    if (channel == null || this.arcadeServer == null)
      return true;
    final var connectedUser = (ServerSecurityUser) channel.getAttribute(USER);
    if (connectedUser == null)
      return true;
    final ServerSecurity security = this.arcadeServer.getSecurity();
    if (security == null)
      return true;
    final ServerSecurityUser current = security.getUser(connectedUser.getName());
    return current != null && current.canAccessToDatabase(databaseName);
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
