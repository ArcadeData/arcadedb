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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10801: a {@code /ws} change-stream watcher listens on the database INSTANCE it was started on, and outlived a
 * {@code close database} / {@code open database} cycle of its name. Every subscriber, the ones from before the close and every new
 * one after the reopen, was answered {@code ok} and then received nothing: {@code subscribe} found the old watcher in the map and
 * never started one on the new instance. The watcher of a name now follows the instance the server registers under it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10801WsStreamFollowsTheReopenedDatabaseIT extends BaseGraphServerTest {

  private static final int EVENT_WAIT_MS = 5_000;

  @Test
  void aSubscriberAfterTheReopenAndOneFromBeforeTheCloseBothReceiveTheNextCommit() throws Exception {
    try (final WebSocketClientHelper before = newClient()) {
      assertThat(new JSONObject(before.send(subscribe())).getString("result", "")).isEqualTo("ok");

      closeAndOpen();

      try (final WebSocketClientHelper after = newClient()) {
        assertThat(new JSONObject(after.send(subscribe())).getString("result", "")).isEqualTo("ok");

        getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-both").save();

        assertCreated(after, "issue10801-both", "a subscriber that subscribed after the reopen");
        assertCreated(before, "issue10801-both", "a subscriber from before the close, which the reopen did not end");
      }
    }
  }

  /** No subscribe follows the reopen: the registration of the new instance alone gives the subscriber already there its stream. */
  @Test
  void aSubscriberFromBeforeTheCloseReceivesTheNextCommitWithoutSubscribingAgain() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");

      closeAndOpen();
      getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-kept").save();

      assertCreated(client, "issue10801-kept", "the subscription is to the name, and the name was opened again");
    }
  }

  @Test
  void theStreamSurvivesSeveralCyclesAndDeliversEachCommitOnce() throws Exception {
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");

      for (int cycle = 0; cycle < 3; cycle++) {
        closeAndOpen();
        getServerDatabase(0, getDatabaseName()).newVertex("V1").set("name", "issue10801-cycle-" + cycle).save();
        assertCreated(client, "issue10801-cycle-" + cycle, "cycle " + cycle);
        assertThat(client.popMessage(500)).as("one commit, one event: no watcher of an earlier instance publishes it too").isNull();
      }
    }
  }

  private void assertCreated(final WebSocketClientHelper client, final String name, final String who) {
    final String message = client.popMessage(EVENT_WAIT_MS);
    assertThat(message).as("%s receives the commit", who).isNotNull();
    final JSONObject json = new JSONObject(message);
    assertThat(json.getString("changeType")).isEqualTo("create");
    assertThat(json.getJSONObject("record").getString("name")).isEqualTo(name);
  }

  private void closeAndOpen() throws Exception {
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper(getServerWsUrl(0, "/ws"), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String subscribe() {
    return new JSONObject().put("action", "subscribe").put("database", getDatabaseName()).toString();
  }

  private HttpResponse<String> postServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/server")))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
