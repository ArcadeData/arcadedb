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

import com.arcadedb.exception.DatabaseNotAvailableException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10690: a {@code /ws} SUBSCRIBE frame resolved its database through {@code ArcadeDBServer.getDatabaseForOperator(String)}, the
 * operator's entry point, which opens a database from disk past the {@code close database} mark and clears the mark on the way.
 * One subscribe frame from any user with access to the name reopened a database the operator had closed (for a cold backup, a
 * copy of the files) and served it again to every protocol. The subscribe now resolves through
 * {@code getDatabaseForClient}, like every other client surface, and is refused.
 */
class Issue10690WsSubscribeHonoursTheCloseMarkIT extends BaseGraphServerTest {

  @Test
  void aSubscribeDoesNotReopenADatabaseACloseCommandClosed() throws Exception {
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    try {
      try (final WebSocketClientHelper client = newClient()) {
        final JSONObject answer = new JSONObject(client.send(subscribe()));
        assertThat(answer.getString("result", "")).as("the subscribe is refused").isEqualTo("error");
        assertThat(answer.getString("exception", "")).isEqualTo(DatabaseNotAvailableException.class.getName());
      }

      assertThat(getServer(0).existsDatabase(getDatabaseName())).as("the subscribe opened nothing").isFalse();
      // The mark survives the frame: every other client is still refused
      assertThatThrownBy(() -> getServer(0).getDatabaseForClient(getDatabaseName())).isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("close database");
      assertThat(getServer(0).getHttpServer().getWebSocketEventBus().getDatabaseSubscriptions(getDatabaseName()))
          .as("a refused subscribe leaves no subscription behind").isEmpty();
    } finally {
      assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
    }

    // Once the operator opens it again the subscribe is served as before
    try (final WebSocketClientHelper client = newClient()) {
      assertThat(new JSONObject(client.send(subscribe())).getString("result", "")).isEqualTo("ok");
    }
  }

  /**
   * The watcher an earlier subscribe started is no pass for a later one: the database is resolved for the client on every
   * subscribe, not only when the first one creates the watcher.
   */
  @Test
  void aSubscribeIsRefusedEvenWhileAWatcherFromAnEarlierOneIsRunning() throws Exception {
    try (final WebSocketClientHelper first = newClient()) {
      assertThat(new JSONObject(first.send(subscribe())).getString("result", "")).isEqualTo("ok");

      assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
      try {
        try (final WebSocketClientHelper second = newClient()) {
          final JSONObject answer = new JSONObject(second.send(subscribe()));
          assertThat(answer.getString("result", "")).isEqualTo("error");
          assertThat(answer.getString("exception", "")).isEqualTo(DatabaseNotAvailableException.class.getName());
        }
        assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();
      } finally {
        assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
      }
    }
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
