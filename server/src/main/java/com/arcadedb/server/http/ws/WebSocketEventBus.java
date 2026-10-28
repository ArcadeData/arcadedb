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
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketCallback;
import io.undertow.websockets.core.WebSocketChannel;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
  /** How many times a subscribe resolves a name whose instance is closed under it before it refuses (issue #10801). */
  private static final int MAX_SUBSCRIBE_RESOLUTIONS = 10;

  public WebSocketEventBus(final ArcadeDBServer server) {
    this.arcadeServer = server;
  }

  public void stop() {
    subscribers.values().forEach(x -> x.values().forEach(y -> y.close()));
    subscribers.clear();
    databaseWatchers.values().forEach(x -> x.shutdown());
    databaseWatchers.clear();
  }

  /**
   * Subscribes {@code channel} to the change stream of {@code databaseName}.
   * <p>
   * The name comes from a client frame, so it is resolved through {@link ArcadeDBServer#getDatabaseForClient(String)} like every
   * other client surface (issue #10690): a database an operator closed with {@code close database}, or one a restore holds, is
   * refused with {@link DatabaseNotAvailableException} instead of being opened from disk - which also
   * ended the {@code close database} mark for every other client. It is resolved on every subscribe, before anything is
   * registered: a refused subscribe leaves no subscription behind, and a watcher started by an earlier subscribe does not let a
   * later one through to a database closed since.
   * <p>
   * The subscription is to the NAME, and the watcher of the name listens on the instance the server registers under it now
   * (issue #10801): a watcher left on an instance a {@code close database} closed is replaced here, and
   * {@link #onDatabaseRegistered} moves it as soon as a new instance is registered, so the subscribers already there keep their
   * stream too.
   */
  public void subscribe(final String databaseName, final String type, final Set<ChangeEvent.TYPE> changeTypes, final WebSocketChannel channel) {
    // Resolved outside the per-database lock: the snapshot installer holds the registry lock while it reports the instance it
    // registered, and that report takes the per-database lock (onDatabaseRegistered)
    for (int attempt = 0; attempt <= MAX_SUBSCRIBE_RESOLUTIONS; attempt++)
      if (subscribeTo(this.arcadeServer.getDatabaseForClient(databaseName), databaseName, type, changeTypes, channel))
        return;
    throw new DatabaseNotAvailableException("Database '" + databaseName + "' is not available: it keeps being closed");
  }

  /**
   * Registers the subscription on {@code database}, the instance just resolved for the name, and returns false when it was closed
   * in the meantime: a newer instance was registered, or is about to be, and its report may already have found no subscriber to
   * move. A watcher started on this one would never deliver anything, so the caller resolves the name again, which also gives a
   * {@code close database} its refusal.
   */
  private boolean subscribeTo(final ServerDatabase database, final String databaseName, final String type,
      final Set<ChangeEvent.TYPE> changeTypes, final WebSocketChannel channel) {
    synchronized (lockFor(databaseName)) {
      if (!database.isOpen())
        return false;

      // The watcher first: one that fails to start leaves no subscription behind. Under the lock, so a single watcher is started
      // even when two Undertow IO threads subscribe to the same database at once
      final DatabaseEventWatcherThread current = this.databaseWatchers.get(databaseName);
      if (current == null || !current.isWatching(database)) {
        final DatabaseEventWatcherThread started = createAndStartWatcher(database);
        // A watcher left on a closed instance has nothing left to deliver: stopped, not joined (see onDatabaseRegistered)
        if (current != null)
          current.signalStop();
        this.databaseWatchers.put(databaseName, started);
      }

      final var channelId = (UUID) channel.getAttribute(CHANNEL_ID);
      this.subscribers.computeIfAbsent(databaseName, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(channelId, k -> new EventWatcherSubscription(databaseName, channel)).add(type, changeTypes);
      return true;
    }
  }

  /**
   * The server registered an instance under {@code databaseName}: an {@code open database} after a {@code close database}, a
   * restore, an HA snapshot install, a database created again after a drop (issue #10801). A watcher listens on the instance it
   * was started on, and the name's subscribers were left on the closed one: {@code subscribe} answered {@code ok}, the commit
   * succeeded, and nothing was pushed until the last subscriber left. The name's watcher is moved onto {@code database} here, so
   * every subscriber of the name, the ones from before the close included, receives its changes from now on.
   * <p>
   * Called on the thread that changed the registry, which may hold the registry lock (the snapshot installer does): the stale
   * watcher is stopped, not joined. It listens on an instance that is closed, so it has nothing left to deliver, and it
   * unregisters its listeners as it exits.
   */
  public void onDatabaseRegistered(final String databaseName, final ServerDatabase database) {
    synchronized (lockFor(databaseName)) {
      final var databaseSubscribers = this.subscribers.get(databaseName);
      if (databaseSubscribers == null || databaseSubscribers.isEmpty() || !database.isOpen())
        return;
      final DatabaseEventWatcherThread current = this.databaseWatchers.get(databaseName);
      if (current != null && current.isWatching(database))
        return;
      if (current != null)
        current.signalStop();
      this.databaseWatchers.put(databaseName, createAndStartWatcher(database));
    }
  }

  /**
   * The server deregistered {@code databaseName} (a {@code close database}, a drop, a restore or an HA snapshot install taking it
   * away): its watcher is stopped, and its subscriptions are kept, so {@link #onDatabaseRegistered} gives them the stream of the
   * instance registered under the name next (issue #10801). Stopped and not joined, for the reason given there.
   */
  public void onDatabaseUnregistered(final String databaseName) {
    synchronized (lockFor(databaseName)) {
      final DatabaseEventWatcherThread current = this.databaseWatchers.remove(databaseName);
      if (current != null)
        current.signalStop();
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
    //
    // The UTF-8 BYTE length, not json.length(): the cap is expressed in bytes and what actually sits in Undertow's
    // send buffer is the UTF-8 encoding WebSockets.sendText produces, which is up to 3 bytes per char for CJK and
    // 4 for an emoji. Counting chars would let a non-ASCII change stream hold several times the configured budget.
    // Computed in one pass rather than via getBytes(UTF_8).length, which would copy the whole payload per event.
    final int messageSize = WebSocketFrameSender.utf8Length(json);
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
        // Looks the subscription up rather than capturing it, so this callback stays shared by the whole fan-out
        // instead of being allocated per subscriber per event. That is safe because releasePending() floors at
        // zero: if the client unsubscribed and re-subscribed before this frame completed, the reservation is
        // released against the REPLACEMENT subscription, which never made it - and a release that would drive a
        // counter negative is dropped instead, so the replacement cannot end up with a negative baseline that
        // silently lets it exceed the cap. The original subscription is unreachable by then, so its own
        // outstanding bytes die with it.
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
          WebSocketFrameSender.send(subscription.getChannel(), json, callback);
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
    // Empty, not a NullPointerException, for a database nobody subscribed to: a refused subscribe registers nothing (issue #10690)
    final var databaseSubscribers = this.subscribers.get(database);
    return databaseSubscribers == null ? Collections.emptyList() : databaseSubscribers.values();
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
    try {
      final ServerSecurityUser current = revalidatedUser(channel);
      return current == null || current.canAccessToDatabase(databaseName);
    } catch (final ServerSecurityException e) {
      return false;
    }
  }

  /**
   * The identity attached to {@code channel}, replaced by the one currently registered under its name (issue #9311). The
   * principal captured at the handshake is a snapshot that nothing refreshes, so EVERY use of it on a long-lived
   * {@code /ws} channel - SUBSCRIBE, delivery, insert frames - goes through here: one rule for the transport, the one
   * {@link ServerSecurity#revalidate} applies to the wire protocols. The fresh instance is written back, so a caller
   * that reads the channel attribute afterwards sees it too.
   *
   * @return the live principal, or the attached one untouched when the channel has none or no security is installed
   *
   * @throws ServerSecurityException when the user was dropped, or its password changed since the handshake
   */
  public ServerSecurityUser revalidatedUser(final WebSocketChannel channel) {
    final ServerSecurityUser held = (ServerSecurityUser) channel.getAttribute(USER);
    if (held == null || this.arcadeServer == null)
      return held;
    final ServerSecurity security = this.arcadeServer.getSecurity();
    if (security == null)
      return held;
    final ServerSecurityUser live = security.revalidate(held);
    if (live != held)
      channel.setAttribute(USER, live);
    return live;
  }

  private Object lockFor(final String database) {
    return this.databaseLocks.computeIfAbsent(database, k -> new Object());
  }

  private DatabaseEventWatcherThread createAndStartWatcher(final ServerDatabase database) {
    final var queueSize = this.arcadeServer.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_WS_EVENT_BUS_QUEUE_SIZE);
    final var watcherThread = new DatabaseEventWatcherThread(this, database, queueSize);
    watcherThread.start();
    return watcherThread;
  }
}
