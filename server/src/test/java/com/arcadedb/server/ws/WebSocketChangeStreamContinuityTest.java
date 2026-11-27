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
package com.arcadedb.server.ws;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.ws.ChangeEvent;
import com.arcadedb.server.http.ws.DatabaseEventWatcherThread;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the subscribers of {@code /ws} are sent goes on after a change that could not be prepared for publishing, and after the
 * watcher of the database ended: the next change is delivered, to the subscribers that were already there and to the ones that
 * subscribe afterwards.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class WebSocketChangeStreamContinuityTest extends BaseGraphServerTest {
  private static final int QUEUE   = 50;
  private static final int WAIT_MS = 5_000;

  @Test
  void aChangeThatCannotBePreparedDoesNotStopTheStream() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final WebSocketClientHelper client = client()) {
      assertOk(client.send(subscribe()));
      final DatabaseEventWatcherThread watcher = watcherOf(getDatabaseName());

      // A change whose record cannot be serialized
      watcher.push(new ChangeEvent(ChangeEvent.TYPE.CREATE, database.newVertex("V1")) {
        @Override
        public String toJSON() {
          throw new IllegalStateException("The record cannot be serialized");
        }
      });

      database.transaction(() -> database.newVertex("V1").set("name", "after").save());

      assertThat(createdName(client)).isEqualTo("after");
      assertThat(watcherOf(getDatabaseName())).isSameAs(watcher);
      assertThat(watcher.isAlive()).isTrue();
    }
  }

  @Test
  void aWatcherThatEndedIsReplacedByTheNextSubscribe() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final WebSocketClientHelper first = client(); final WebSocketClientHelper second = client()) {
      assertOk(first.send(subscribe()));
      final DatabaseEventWatcherThread ended = watcherOf(getDatabaseName());

      // The watcher thread ends without the bus having been asked to stop it
      ended.interrupt();
      Awaitility.await().atMost(WAIT_MS, TimeUnit.MILLISECONDS).until(() -> !ended.isAlive());

      assertOk(second.send(subscribe()));
      assertThat(watcherOf(getDatabaseName())).isNotSameAs(ended);

      database.transaction(() -> database.newVertex("V1").set("name", "after").save());

      assertThat(createdName(second)).isEqualTo("after");
      assertThat(createdName(first)).isEqualTo("after");
    }
  }

  private WebSocketClientHelper client() throws Exception {
    return new WebSocketClientHelper(getServerWsUrl("/ws"), "root", DEFAULT_PASSWORD_FOR_TESTS, QUEUE);
  }

  private String subscribe() {
    return new JSONObject().put("action", "subscribe").put("database", getDatabaseName()).toString();
  }

  private static void assertOk(final String answer) {
    assertThat(new JSONObject(answer).getString("result", "")).isEqualTo("ok");
  }

  /** The name of the record of the next change delivered on {@code client}, which has to be a creation. */
  private static String createdName(final WebSocketClientHelper client) {
    final String message = client.popMessage(WAIT_MS);
    assertThat(message).as("a change is delivered").isNotNull();
    final JSONObject change = new JSONObject(message);
    assertThat(change.getString("changeType", "")).isEqualTo("create");
    return change.getJSONObject("record").getString("name", "");
  }

  @SuppressWarnings("unchecked")
  private DatabaseEventWatcherThread watcherOf(final String databaseName) throws Exception {
    final Field field = getServer(0).getHttpServer().getWebSocketEventBus().getClass().getDeclaredField("databaseWatchers");
    field.setAccessible(true);
    return ((ConcurrentHashMap<String, DatabaseEventWatcherThread>) field.get(getServer(0).getHttpServer().getWebSocketEventBus())).get(
        databaseName);
  }
}
