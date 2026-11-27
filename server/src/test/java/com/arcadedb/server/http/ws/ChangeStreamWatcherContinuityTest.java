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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The change stream of a database goes on after one change that cannot be prepared for publishing: that change is dropped, the
 * watcher of the database keeps running and the changes after it are published. A watcher that did end is not reused by the
 * next subscribe.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ChangeStreamWatcherContinuityTest {
  private DatabaseFactory factory;
  private Database        database;
  private Document        record;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory("./target/databases/changestreamcontinuity_" + UUID.randomUUID());
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

  @Test
  void aChangeThatCannotBeSerializedIsDroppedAndTheNextOneIsPublished() throws Exception {
    final WebSocketEventBus bus = new WebSocketEventBus(null);
    final CopyOnWriteArrayList<ChangeEvent> reached = new CopyOnWriteArrayList<>();
    injectSubscription(bus, reached);

    assertThatCode(() -> bus.publish(failingEvent())).doesNotThrowAnyException();
    assertThat(reached).as("the change that could not be serialized reaches no subscriber").isEmpty();

    final ChangeEvent next = new ChangeEvent(ChangeEvent.TYPE.UPDATE, record);
    bus.publish(next);
    assertThat(reached).containsExactly(next);
  }

  @Test
  void theWatcherKeepsRunningAfterAChangeThatFailsToPublish() throws Exception {
    final ChangeEvent failing = failingEvent();
    final ChangeEvent next = new ChangeEvent(ChangeEvent.TYPE.UPDATE, record);
    final CopyOnWriteArrayList<ChangeEvent> published = new CopyOnWriteArrayList<>();
    final CountDownLatch nextPublished = new CountDownLatch(1);

    final WebSocketEventBus bus = new WebSocketEventBus(null) {
      @Override
      public void publish(final ChangeEvent event) {
        if (event == failing)
          throw new IllegalStateException("The change cannot be published");
        published.add(event);
        nextPublished.countDown();
      }
    };

    final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
    watcher.start();
    try {
      watcher.push(failing);
      watcher.push(next);

      assertThat(nextPublished.await(5, TimeUnit.SECONDS)).as("the change after the failing one is published").isTrue();
      assertThat(published).containsExactly(next);
      assertThat(watcher.isAlive()).as("the watcher is still running").isTrue();
    } finally {
      watcher.shutdown();
    }
  }

  @Test
  void aWatcherThatEndedIsNotReportedAsWatching() throws Exception {
    final WebSocketEventBus bus = new WebSocketEventBus(null);
    final DatabaseEventWatcherThread watcher = new DatabaseEventWatcherThread(bus, database, 16);
    watcher.start();
    assertThat(watcher.isWatching((DatabaseInternal) database)).isTrue();

    // The thread ends without being asked to stop through the bus
    watcher.interrupt();
    watcher.join(TimeUnit.SECONDS.toMillis(5));
    assertThat(watcher.isAlive()).isFalse();

    assertThat(watcher.isWatching((DatabaseInternal) database))
        .as("the next subscribe must start a watcher in place of the one that ended")
        .isFalse();
  }

  /** A change whose record cannot be serialized. */
  private ChangeEvent failingEvent() {
    return new ChangeEvent(ChangeEvent.TYPE.CREATE, record) {
      @Override
      public String toJSON() {
        throw new IllegalStateException("The record cannot be serialized");
      }
    };
  }

  /** One subscriber that records the changes reaching it and takes none of them. */
  @SuppressWarnings("unchecked")
  private void injectSubscription(final WebSocketEventBus bus, final CopyOnWriteArrayList<ChangeEvent> reached) throws Exception {
    final ConcurrentHashMap<UUID, EventWatcherSubscription> subscriptions = new ConcurrentHashMap<>();
    subscriptions.put(UUID.randomUUID(), new EventWatcherSubscription("db", null) {
      @Override
      public boolean isMatch(final ChangeEvent event) {
        reached.add(event);
        return false;
      }
    });
    final Field field = WebSocketEventBus.class.getDeclaredField("subscribers");
    field.setAccessible(true);
    ((ConcurrentHashMap<String, ConcurrentHashMap<UUID, EventWatcherSubscription>>) field.get(bus)).put(database.getName(),
        subscriptions);
  }
}
