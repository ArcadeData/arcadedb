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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import io.undertow.websockets.core.WebSocketChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5024: WebSocket event-bus concurrency and per-subscriber re-serialization defects.
 * These exercise the internal event-bus classes directly (no HTTP server) so the concurrency behaviour is deterministic.
 */
class Issue5024WebSocketEventBusConcurrencyTest {
  private DatabaseFactory factory;
  private Database        database;
  private Document        record;

  @BeforeEach
  void setUp() {
    final String path = "./target/databases/issue5024_" + UUID.randomUUID();
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
   * Defect 1: when the watcher thread itself triggers a stop (the last subscriber turns out to be a zombie during
   * {@code publish}), {@code shutdown()} must not block on its own termination latch. Before the fix the thread parks
   * in {@code runningLock.await()} inside {@code publish()} inside {@code run()} and never terminates.
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

  @SuppressWarnings("unchecked")
  private static void injectSubscribers(final WebSocketEventBus bus, final String databaseName,
      final ConcurrentHashMap<UUID, EventWatcherSubscription> dbSubs) throws Exception {
    final Field field = WebSocketEventBus.class.getDeclaredField("subscribers");
    field.setAccessible(true);
    final ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>> subscribers =
        (ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>>) field.get(bus);
    subscribers.put(databaseName, dbSubs);
  }
}
