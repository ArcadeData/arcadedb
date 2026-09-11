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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code /ws} insert session holds a transaction open between frames, so a client that opens one and walks away
 * would hold it for as long as its connection stays up. The idle sweep rolls it back on the same budget an
 * {@code arcadedb-session-id} transaction gets, and tells the client it did (issue #7382).
 * <p>
 * Its own class because the expiry budget is a server setting: the class lowers it to one second, which every
 * other insert session in the same server would then also be measured against.
 */
class WebSocketInsertSessionExpiryIT extends BaseGraphServerTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_WS_INSERT_SESSION_EXPIRE_TIMEOUT, 1);
  }

  @Test
  void anAbandonedSessionIsRolledBackAndItsClientToldSo() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = new WebSocketClientHelper(
        "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      final JSONObject started = new JSONObject(client.send(new JSONObject().put("action", "start")
          .put("database", getDatabaseName()).put("options", new JSONObject().put("targetType", "Person")).toString()));
      final String sessionId = started.getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(new JSONObject().put("action", "chunk")
          .put("sessionId", sessionId).put("chunkSeq", 1)
          .put("records", new JSONArray().put(new JSONObject().put("name", "abandoned"))).toString()));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);

      // The client now says nothing at all. The sweep is what has to notice.
      final JSONObject unsolicited = new JSONObject(client.popMessage(15_000));
      assertThat(unsolicited.getString("result", "")).isEqualTo("error");
      assertThat(unsolicited.getString("sessionId", "")).isEqualTo(sessionId);
      assertThat(unsolicited.getString("detail", "")).contains("inactivity");

      Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
          .until(() -> getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount() == 0);

      assertThat(database.countType("Person", false)).isZero();

      // And the id no longer resolves, so a late frame is refused rather than served.
      final JSONObject late = new JSONObject(client.send(new JSONObject().put("action", "commit")
          .put("sessionId", sessionId).toString()));
      assertThat(late.getString("result", "")).isEqualTo("error");
      assertThat(late.getString("detail", "")).contains("not found or expired");
    }
  }
}
