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
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The {@link WebSocketEventBus} driven directly, without an HTTP server, so its concurrency is deterministic.
 * <p>
 * Issue #6762: the duplicate delivery a watcher could cause when stopped outside the per-database lock on a last
 * unsubscribe / re-subscribe, the unbounded send backlog a slow subscriber could accrue, and subscribers whose
 * access was revoked.
 * <p>
 * Issue #5024: WebSocket event-bus concurrency and per-subscriber re-serialization defects.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WebSocketEventBusTest {

  private DatabaseFactory factory;
  private Database        database;
  private Document        record;

  @BeforeEach
  void setUp() {
    final String path = "./target/databases/websocketeventbus_" + UUID.randomUUID();
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
  /**
   * {@code shutdown()} used to return early when {@code running} was already false, so once {@code signalStop()}
   * exists the join it is there to perform would be skipped - and the caller would carry on while the watcher's
   * listeners were still registered.
   * <p>
   * Repeated rather than checked once, because the second half of the same contract is a narrow race and a single
   * shot is not a regression test for it: {@code shutdown()} used to wait on a latch that {@code run()} counts down
   * from INSIDE itself, so it returned during the JVM's own thread teardown with the watcher still alive. That lost
   * roughly one call in 300 - often enough to redden CI (run 33201895826), far too rarely for one attempt to catch
   * it.
   * <p>
   * The {@code interrupt()} is what keeps the loop cheap under load. A watcher that reads {@code running} before
   * {@link DatabaseEventWatcherThread#signalStop()} stores {@code false} parks in the 500ms queue poll, and
   * {@code shutdown()} then waits it out - measured at 446ms per occurrence, which 5,000 iterations could turn into
   * minutes on a loaded runner. Interrupting releases the poll, and it costs the assertion nothing: either exit path
   * runs the same {@code finally} and leaves the same window between {@code run()} returning and the thread dying
   * (PR #6885 review).
   */
  @Test
  void shutdownStillJoinsAfterASeparateStopSignal() throws Exception {
    final WebSocketEventBus bus = new WebSocketEventBus(null);

    int stillAlive = 0;
    for (int i = 0; i < 5_000; i++) {
      final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
      watcher.start();

      watcher.signalStop();
      watcher.interrupt();
      watcher.shutdown();

      if (watcher.isAlive())
        ++stillAlive;
    }

    assertThat(stillAlive)
        .as("shutdown() must have waited for run() to unwind, unregister its listeners AND the thread to die")
        .isZero();
  }

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
  /**
   * Authorization was checked only at SUBSCRIBE time, and the identity is captured once at the handshake, so
   * revoking a user's access to the database - or dropping the user outright - left the change stream flowing
   * until the client happened to disconnect. It has to be re-checked on delivery.
   */
  @Test
  void aSubscriberWhoseAccessWasRevokedStopsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser revoked = TestServerHelper.securityUser("someone");

    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("revalidate", args -> Objects.equals(args[0], revoked) ? revoked : null);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", new ContextConfiguration());
    server.security(security);

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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
  /** Control: a user who still holds the grant keeps receiving events. */
  @Test
  void aSubscriberWhoStillHasAccessKeepsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser allowed = TestServerHelper.securityUser("someone", db);

    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("revalidate", args -> Objects.equals(args[0], allowed) ? allowed : null);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", new ContextConfiguration());
    server.security(security);

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

  // Regression: #6762 watcher stop race, bounded subscriber backlog, revoked access
  /**
   * A user that was DROPPED, not merely narrowed: the captured snapshot still answers yes, so the check has to
   * re-resolve the principal by name rather than trust the object it was handed at the handshake.
   */
  @Test
  void aSubscriberWhoseUserWasDroppedStopsReceivingEvents() throws Exception {
    final String db = record.getDatabase().getName();

    final ServerSecurityUser captured = TestServerHelper.securityUser("gone", db); // the stale snapshot still says yes

    final FakeServerSecurity security = FakeServerSecurity.create();
    security.on("revalidate", args -> {
      if (Objects.equals(args[0], captured))
        throw new ServerSecurityException("User 'gone' no longer exists");
      return null;
    }); // ...but the principal no longer exists

    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", new ContextConfiguration());
    server.security(security);

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

  // Regression: #5024 event-bus concurrency and per-subscriber re-serialization
  /**
   * Defect 1: when the watcher thread itself triggers a stop (the last subscriber turns out to be a zombie during
   * {@code publish}), {@code shutdown()} must not block on its own termination. Before the fix the thread waited on
   * that barrier from inside {@code publish()} inside {@code run()} - so it waited on a termination that only its own
   * return could produce, and never terminated. The barrier was a latch then and is {@link Thread#join()} now; the
   * self-shutdown guard is what makes either one safe.
   */
  @Test
  void watcherSelfShutdownDoesNotHang() throws Exception {
    final AtomicReference<DatabaseEventWatcherThread> watcherRef = new AtomicReference<>();

    // A bus whose publish() simulates the "last subscriber is a zombie" cleanup: it stops the watcher from within
    // the watcher thread, exactly as the real unsubscribeAll -> stopDatabaseWatcher -> shutdown() chain does.
    final WebSocketEventBus bus = new WebSocketEventBus(null) {
      @Override
      public void publish(final ChangeEvent event) {
        watcherRef.get().shutdown();
      }
    };

    final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
    watcherRef.set(watcher);
    watcher.start();

    // Enqueue an event so run() calls publish() on the watcher thread.
    watcher.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    watcher.join(TimeUnit.SECONDS.toMillis(5));
    assertThat(watcher.isAlive()).as("Watcher thread must terminate after self-shutdown, not deadlock in await()").isFalse();
  }

  // Regression: #5024 event-bus concurrency and per-subscriber re-serialization
  /**
   * Defect 5: the event JSON must be serialized exactly once per publish, regardless of the number of subscribers.
   * Before the fix {@code event.toJSON()} was evaluated once per matching subscriber inside the loop.
   */
  @Test
  void publishSerializesEventOncePerCall() throws Exception {
    final AtomicInteger serializations = new AtomicInteger();
    final ChangeEvent event = new ChangeEvent(ChangeEvent.TYPE.CREATE, record) {
      @Override
      public String toJSON() {
        serializations.incrementAndGet();
        return super.toJSON();
      }
    };

    final WebSocketEventBus bus = new WebSocketEventBus(null);

    // Two subscribers that always match and expose no real channel: sendText fails per subscriber but is swallowed,
    // so publish still walks every subscriber - the only observable is how many times the event was serialized.
    final ConcurrentHashMap<UUID, EventWatcherSubscription> dbSubs = new ConcurrentHashMap<>();
    dbSubs.put(UUID.randomUUID(), matchAllSubscription());
    dbSubs.put(UUID.randomUUID(), matchAllSubscription());
    injectSubscribers(bus, record.getDatabase().getName(), dbSubs);

    bus.publish(event);

    assertThat(serializations.get()).as("Event must be serialized once regardless of subscriber count").isEqualTo(1);
  }

  // Regression: #5024 event-bus concurrency and per-subscriber re-serialization
  /**
   * Defect 4: publish must never throw ConcurrentModificationException even when send callbacks add zombies
   * concurrently, and must still drain every zombie it observes.
   */
  @Test
  void publishDrainsZombiesWithoutConcurrentModification() throws Exception {
    final WebSocketEventBus bus = new WebSocketEventBus(null);
    final String db = record.getDatabase().getName();

    final ConcurrentHashMap<UUID, EventWatcherSubscription> dbSubs = new ConcurrentHashMap<>();
    for (int i = 0; i < 200; i++)
      dbSubs.put(UUID.randomUUID(), matchAllSubscription());
    injectSubscribers(bus, db, dbSubs);

    // Repeated publishes must not blow up on the zombie-draining path.
    for (int i = 0; i < 50; i++)
      bus.publish(new ChangeEvent(ChangeEvent.TYPE.CREATE, record));

    // The subscriber map is thread-safe; publish completing without exception is the assertion.
    assertThat(bus.getDatabaseSubscriptions(db)).isNotNull();
  }

  // Regression: #5024 event-bus concurrency and per-subscriber re-serialization
  /**
   * Defect 3: EventWatcherSubscription.add() (Undertow IO threads) and isMatch() (watcher thread) run concurrently
   * on the same underlying set. A plain HashSet can throw or misread; the concurrent set must be safe and correct.
   */
  @Test
  void concurrentAddAndMatchIsThreadSafe() throws Exception {
    final EventWatcherSubscription subscription = new EventWatcherSubscription("db", null);
    final ChangeEvent createEvent = new ChangeEvent(ChangeEvent.TYPE.CREATE, record);

    final int threads = 8;
    final int iterations = 2000;
    final CountDownLatch start = new CountDownLatch(1);
    final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
    final Thread[] pool = new Thread[threads];

    for (int t = 0; t < threads; t++) {
      final boolean writer = (t % 2 == 0);
      pool[t] = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations; i++) {
            if (writer)
              subscription.add("Doc", EnumSet.allOf(ChangeEvent.TYPE.class));
            else
              subscription.isMatch(createEvent);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      });
      pool[t].start();
    }

    start.countDown();
    for (final Thread thread : pool)
      thread.join(TimeUnit.SECONDS.toMillis(10));

    assertThat(errors).as("Concurrent add()/isMatch() must not throw").isEmpty();
    // Type "Doc" subscribed to all change types must match a CREATE on a Doc record.
    assertThat(subscription.isMatch(createEvent)).isTrue();
  }

  private EventWatcherSubscription matchAllSubscription() {
    return new EventWatcherSubscription("db", null) {
      @Override
      public boolean isMatch(final ChangeEvent event) {
        return true;
      }

      @Override
      public WebSocketChannel getChannel() {
        return null;
      }
    };
  }
}
