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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the WebSocket change-stream defects grouped under issue #6762: the duplicate-delivery race
 * on a last-unsubscribe / re-subscribe, and the unbounded send backlog a slow subscriber could accrue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6762WebSocketEventBusTest {
  private DatabaseFactory factory;
  private Database        database;
  private Document        record;

  @BeforeEach
  void setUp() {
    final String path = "./target/databases/issue6762_" + UUID.randomUUID();
    factory = new DatabaseFactory(path);
    database = factory.create();
    database.getSchema().createDocumentType("Doc");
    database.begin();
    final MutableDocument doc = database.newDocument("Doc").set("k", "v");
    doc.save();
    database.commit();
    record = doc;
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    if (factory != null)
      factory.close();
  }

  /**
   * The duplicate-delivery race: the watcher was removed from the maps inside the per-database lock but only told
   * to stop OUTSIDE it, so a subscribe() winning the lock in that gap started a second watcher while the first was
   * still {@code running} with its listeners attached, and a commit in the window was enqueued on both.
   * <p>
   * Driven at the point of the race rather than by hitting it with threads: what makes the window harmless is that
   * the outgoing watcher stops accepting events at removal time, which is a property {@code signalStop()} either
   * has or does not.
   */
  @Test
  void aWatcherStopsAcceptingEventsAsSoonAsItIsSignalled() throws Exception {
    final CopyOnWriteArrayList<ChangeEvent> published = new CopyOnWriteArrayList<>();
    final WebSocketEventBus bus = new WebSocketEventBus(null) {
      @Override
      public void publish(final ChangeEvent event) {
        published.add(event);
      }
    };

    final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
    watcher.start();

    // This is the state the outgoing watcher is in while the lock is still held: removed from the maps, told to
    // stop, but its run() loop and its database listeners still very much alive.
    watcher.signalStop();
    watcher.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    watcher.join(TimeUnit.SECONDS.toMillis(5));
    assertThat(watcher.isAlive()).isFalse();
    assertThat(published)
        .as("an event pushed after the stop signal must be dropped, not published a second time alongside the "
            + "replacement watcher's copy")
        .isEmpty();
  }

  /**
   * {@code shutdown()} used to return early when {@code running} was already false, so once {@code signalStop()}
   * exists the join it is there to perform would be skipped - and the caller would carry on while the watcher's
   * listeners were still registered.
   */
  @Test
  void shutdownStillJoinsAfterASeparateStopSignal() throws Exception {
    final WebSocketEventBus bus = new WebSocketEventBus(null);
    final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
    watcher.start();

    watcher.signalStop();
    watcher.shutdown();

    assertThat(watcher.isAlive())
        .as("shutdown() must have waited for run() to unwind and unregister its listeners")
        .isFalse();
  }

  /**
   * The unbounded send backlog: frames are handed to Undertow fire-and-forget, so a subscriber that stops reading
   * used to accumulate them on the server's heap without limit. Past the cap the subscription is dropped instead.
   */
  @Test
  void aSlowSubscriberIsEvictedInsteadOfAccruingAnUnboundedBacklog() throws Exception {
    final long cap = 512;
    GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.setValue(cap);
    try {
      final WebSocketEventBus bus = new WebSocketEventBus(null);
      final String db = record.getDatabase().getName();

      // A subscriber whose sends never complete: nothing releases the reservation, exactly like a peer that has
      // stopped reading while its TCP window stays shut.
      final EventWatcherSubscription slow = neverCompletingSubscription();
      final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
      final UUID channelId = UUID.randomUUID();
      subscriptions.put(channelId, slow);
      injectSubscribers(bus, db, subscriptions);

      for (int i = 0; i < 200; i++)
        bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

      assertThat(slow.getPendingBytes())
          .as("the backlog charged to one subscriber must stay inside its budget")
          .isLessThanOrEqualTo(cap);
      assertThat(bus.getDatabaseSubscriptions(db))
          .as("and the subscriber that could not keep up is dropped rather than carried forever")
          .isEmpty();
    } finally {
      GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.reset();
    }
  }

  /** The cap is opt-out: 0 restores the pre-26.9.1 behaviour of never evicting on backlog alone. */
  @Test
  void aZeroCapDisablesTheEviction() throws Exception {
    GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.setValue(0L);
    try {
      final WebSocketEventBus bus = new WebSocketEventBus(null);
      final String db = record.getDatabase().getName();

      final EventWatcherSubscription slow = neverCompletingSubscription();
      final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
      subscriptions.put(UUID.randomUUID(), slow);
      injectSubscribers(bus, db, subscriptions);

      for (int i = 0; i < 50; i++)
        bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

      assertThat(bus.getDatabaseSubscriptions(db)).hasSize(1);
      assertThat(slow.getPendingBytes()).isGreaterThan(0);
    } finally {
      GlobalConfiguration.SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES.reset();
    }
  }

  /**
   * The cap is expressed in bytes and what sits in the send buffer is the UTF-8 encoding, so a non-ASCII change
   * stream must be charged its real byte cost - counting chars would let it hold several times the budget
   * (PR #6783 review).
   */
  @Test
  void theBacklogIsChargedInUtf8BytesNotCharacters() throws Exception {
    final String db = record.getDatabase().getName();
    // One emoji per record property value: 1 char in Java's count once, but 4 UTF-8 bytes on the wire.
    database.begin();
    ((com.arcadedb.database.MutableDocument) record.asDocument().modify()).set("k", "😀".repeat(64)).save();
    database.commit();

    final WebSocketEventBus bus = new WebSocketEventBus(null);
    final EventWatcherSubscription slow = neverCompletingSubscription();
    final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
    subscriptions.put(UUID.randomUUID(), slow);
    injectSubscribers(bus, db, subscriptions);

    final ChangeEvent event = new ChangeEvent(ChangeEvent.TYPE.UPDATE, record);
    bus.publish(event);

    assertThat(slow.getPendingBytes())
        .as("the emoji payload must be charged its UTF-8 byte cost, which exceeds its character count")
        .isGreaterThan(event.toJSON().length());
  }

  /**
   * A client that unsubscribes and re-subscribes while a frame is still in flight lands its completion callback on
   * the REPLACEMENT subscription. That release must not drive the replacement's counter negative, which would give
   * it a negative baseline and silently let it hold far more than the cap (PR #6783 review).
   */
  @Test
  void aReleaseOnAReplacementSubscriptionCannotDriveItsBacklogNegative() {
    final EventWatcherSubscription replacement = new EventWatcherSubscription("db", null);

    replacement.releasePending(10_000); // a frame the ORIGINAL subscription reserved, completing late

    assertThat(replacement.getPendingBytes()).isZero();
    assertThat(replacement.reservePending(512, 1024))
        .as("the replacement still has its full budget, not a negative head start")
        .isTrue();
    assertThat(replacement.reservePending(1024, 1024))
        .as("and the cap still bites at the configured value")
        .isFalse();
  }

  /**
   * Authorization was checked only at SUBSCRIBE time, and the identity is captured once at the handshake, so
   * revoking a user's access to the database - or dropping the user outright - left the change stream flowing
   * until the client happened to disconnect. It has to be re-checked on delivery.
   */
  @Test
  void aSubscriberWhoseAccessWasRevokedStopsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser revoked = mock(ServerSecurityUser.class);
    when(revoked.getName()).thenReturn("someone");
    when(revoked.canAccessToDatabase(db)).thenReturn(false);

    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getUser("someone")).thenReturn(revoked);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getSecurity()).thenReturn(security);

    final WebSocketChannel channel = mock(WebSocketChannel.class);
    final UUID channelId = UUID.randomUUID();
    when(channel.getAttribute(WebSocketEventBus.CHANNEL_ID)).thenReturn(channelId);
    when(channel.getAttribute(WebSocketEventBus.USER)).thenReturn(revoked);

    final WebSocketEventBus bus = new WebSocketEventBus(server);
    final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
    subscriptions.put(channelId, matchAllSubscription(channel));
    injectSubscribers(bus, db, subscriptions);

    bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    assertThat(bus.getDatabaseSubscriptions(db))
        .as("the stream must stop as soon as the grant is gone, not when the client happens to disconnect")
        .isEmpty();
  }

  /** Control: a user who still holds the grant keeps receiving events. */
  @Test
  void aSubscriberWhoStillHasAccessKeepsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser allowed = mock(ServerSecurityUser.class);
    when(allowed.getName()).thenReturn("someone");
    when(allowed.canAccessToDatabase(db)).thenReturn(true);

    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getUser("someone")).thenReturn(allowed);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getSecurity()).thenReturn(security);

    final WebSocketChannel channel = mock(WebSocketChannel.class);
    final UUID channelId = UUID.randomUUID();
    when(channel.getAttribute(WebSocketEventBus.CHANNEL_ID)).thenReturn(channelId);
    when(channel.getAttribute(WebSocketEventBus.USER)).thenReturn(allowed);

    final WebSocketEventBus bus = new WebSocketEventBus(server);
    final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
    subscriptions.put(channelId, matchAllSubscription(channel));
    injectSubscribers(bus, db, subscriptions);

    bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    assertThat(bus.getDatabaseSubscriptions(db)).hasSize(1);
  }

  /**
   * A user that was DROPPED, not merely narrowed: the captured snapshot still answers yes, so the check has to
   * re-resolve the principal by name rather than trust the object it was handed at the handshake.
   */
  @Test
  void aSubscriberWhoseUserWasDroppedStopsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser captured = mock(ServerSecurityUser.class);
    when(captured.getName()).thenReturn("gone");
    when(captured.canAccessToDatabase(db)).thenReturn(true); // the stale snapshot still says yes

    final ServerSecurity security = mock(ServerSecurity.class);
    when(security.getUser("gone")).thenReturn(null); // ...but the principal no longer exists

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getSecurity()).thenReturn(security);

    final WebSocketChannel channel = mock(WebSocketChannel.class);
    final UUID channelId = UUID.randomUUID();
    when(channel.getAttribute(WebSocketEventBus.CHANNEL_ID)).thenReturn(channelId);
    when(channel.getAttribute(WebSocketEventBus.USER)).thenReturn(captured);

    final WebSocketEventBus bus = new WebSocketEventBus(server);
    final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
    subscriptions.put(channelId, matchAllSubscription(channel));
    injectSubscribers(bus, db, subscriptions);

    bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    assertThat(bus.getDatabaseSubscriptions(db)).isEmpty();
  }

  private EventWatcherSubscription matchAllSubscription(final WebSocketChannel channel) {
    return new EventWatcherSubscription("db", channel) {
      @Override
      public boolean isMatch(final ChangeEvent event) {
        return true;
      }
    };
  }

  /**
   * A subscription that always matches and whose frames Undertow never reports done: the reservation is made and
   * never released, which is exactly what a peer that has stopped reading looks like to the accounting.
   */
  private EventWatcherSubscription neverCompletingSubscription() {
    return new EventWatcherSubscription("db", null) {
      @Override
      public boolean isMatch(final ChangeEvent event) {
        return true;
      }

      @Override
      public WebSocketChannel getChannel() {
        return null;
      }

      @Override
      public void releasePending(final int bytes) {
        // the frames stay outstanding forever
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static void injectSubscribers(final WebSocketEventBus bus, final String databaseName,
      final ConcurrentHashMap<UUID, EventWatcherSubscription> dbSubs) throws Exception {
    final Field field = WebSocketEventBus.class.getDeclaredField("subscribers");
    field.setAccessible(true);
    ((ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>>) field.get(bus))
        .put(databaseName, dbSubs);
  }
}
