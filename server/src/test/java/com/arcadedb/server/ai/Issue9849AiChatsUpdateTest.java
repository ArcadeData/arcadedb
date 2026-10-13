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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9849: the sweep that holds every {@code arcadedb-server} route to a verdict on the Undertow IO thread found that
 * {@code AiChatsHandler} did not dispatch off it, and that two things followed. Every verb reads or writes the chat files of the
 * caller, which is blocking file IO on a selector shared by every other connection. And {@code PUT /api/v1/ai/chats/{id}}, the
 * request Studio sends to persist a chat after a message is deleted, never got its body: {@code handleRequest} reads the payload
 * only for a handler that declares {@code mustExecuteOnWorkerThread()}, so the handler saw {@code null} and answered
 * {@code 400 Request body is required} to a request that carried one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9849AiChatsUpdateTest extends BaseGraphServerTest {
  private static final String USER = "root";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private HttpResponse<String> send(final String method, final String path, final String body) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/ai" + path)))
        .timeout(Duration.ofSeconds(30)).header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString((USER + ":" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (body != null)
      builder.header("Content-Type", "application/json");
    builder.method(method, body != null ? HttpRequest.BodyPublishers.ofString(body) : HttpRequest.BodyPublishers.noBody());
    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** A chat on disk for the caller, written the way the chat endpoint writes it, under the directory the route reads from. */
  private JSONObject storedChat() {
    final ChatStorage storage = new ChatStorage(getServer(0).getRootPath());
    final JSONObject chat = ChatStorage.createNewChat(getDatabaseName(), "A chat to update");
    storage.saveChat(USER, chat);
    return chat;
  }

  @Test
  void putReplacesTheMessagesOfTheChat() throws Exception {
    final JSONObject chat = storedChat();
    final String id = chat.getString("id");
    try {
      final JSONArray messages = new JSONArray().put(new JSONObject().put("role", "user").put("content", "the one that stays"));

      final HttpResponse<String> updated = send("PUT", "/chats/" + id, new JSONObject().put("messages", messages).toString());
      assertThat(updated.statusCode()).as("the body of the PUT must reach the handler: %s", updated.body()).isEqualTo(200);

      final HttpResponse<String> read = send("GET", "/chats/" + id, null);
      assertThat(read.statusCode()).isEqualTo(200);
      final JSONArray stored = new JSONObject(read.body()).getJSONArray("messages");
      assertThat(stored.length()).isEqualTo(1);
      assertThat(stored.getJSONObject(0).getString("content")).isEqualTo("the one that stays");
    } finally {
      send("DELETE", "/chats/" + id, null);
    }
  }

  @Test
  void putWithoutABodyIsStillRefused() throws Exception {
    final JSONObject chat = storedChat();
    final String id = chat.getString("id");
    try {
      final HttpResponse<String> refused = send("PUT", "/chats/" + id, null);
      assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);
    } finally {
      send("DELETE", "/chats/" + id, null);
    }
  }

  @Test
  void listGetAndDeleteStillWork() throws Exception {
    final JSONObject chat = storedChat();
    final String id = chat.getString("id");

    final HttpResponse<String> list = send("GET", "/chats", null);
    assertThat(list.statusCode()).isEqualTo(200);
    assertThat(list.body()).contains(id);

    assertThat(send("GET", "/chats/" + id, null).statusCode()).isEqualTo(200);
    assertThat(send("DELETE", "/chats/" + id, null).statusCode()).isEqualTo(200);
    assertThat(send("GET", "/chats/" + id, null).statusCode()).isEqualTo(404);
  }
}
