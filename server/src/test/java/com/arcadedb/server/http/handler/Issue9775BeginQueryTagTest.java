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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9775: {@code /begin} read the {@code X-ArcadeDB-Query-Tag} header only after it had created and published the
 * transaction session, so a header refused there left a session in the manager, with a transaction attached, whose id the
 * client was never sent. The header is now checked with the rest of the request envelope, before any route runs, so a
 * refused one allocates nothing on any route.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9775BeginQueryTagTest extends BaseGraphServerTest {
  private static final String TAG_HEADER = AbstractServerHttpHandler.QUERY_TAG_HEADER;

  @Test
  void aBeginWithATooLongTagIsRefusedWithoutLeavingASession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();
    final int before = sessions.getActiveSessions();

    final Response response = call("POST", "/api/v1/begin/graph", new JSONObject(),
        Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));

    assertThat(response.status).isEqualTo(400);
    assertThat(response.sessionId).as("no session id is handed out").isNull();
    assertThat(new JSONObject(response.body).getString("error"))
        .as("the client is told which header was refused, outside the detail production mode conceals")
        .contains(TAG_HEADER);
    assertThat(sessions.getActiveSessions())
        .as("the refused begin published no transaction session nobody could ever end")
        .isEqualTo(before);
  }

  @Test
  void aBeginWithAControlCharacterInTheTagIsRefusedWithoutLeavingASession() throws Exception {
    final HttpSessionManager sessions = getServer(0).getHttpServer().getSessionManager();
    final int before = sessions.getActiveSessions();

    final Response response = call("POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bad\u0007tag"));

    assertThat(response.status).isEqualTo(400);
    assertThat(response.sessionId).isNull();
    assertThat(sessions.getActiveSessions()).isEqualTo(before);
  }

  @Test
  void aBeginWithAValidTagKeepsItOnTheSession() throws Exception {
    final Response begin = call("POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "  batch-9775  "));
    assertThat(begin.status).isEqualTo(204);
    assertThat(begin.sessionId).isNotNull();

    try {
      final HttpSession session = getServer(0).getHttpServer().getSessionManager().getSessions().stream()
          .filter(s -> s.id.equals(begin.sessionId)).findFirst().orElseThrow();
      assertThat(session.getTag()).isEqualTo("batch-9775");
    } finally {
      assertThat(call("POST", "/api/v1/rollback/graph", new JSONObject(), Map.of(HttpSessionManager.ARCADEDB_SESSION_ID, begin.sessionId)).status)
          .isEqualTo(204);
    }
  }

  /**
   * The check belongs to the envelope, not to the routes that list a running statement: a route outside the admission gate
   * refuses the header too, rather than each handler having to remember to ask.
   */
  @Test
  void anUnusableTagIsRefusedOnARouteOutsideTheAdmissionGate() throws Exception {
    final Response response = call("POST", "/api/v1/server", new JSONObject().put("command", "list databases"),
        Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));
    assertThat(response.status).isEqualTo(400);

    assertThat(call("POST", "/api/v1/server", new JSONObject().put("command", "list databases"), Map.of(TAG_HEADER, "ok")).status)
        .isEqualTo(200);
  }

  /**
   * The check runs before authentication: a request with no credentials and an unusable label is answered 400, and the answer
   * never echoes the label back.
   */
  @Test
  void anUnusableTagWithoutCredentialsIsRefusedWithoutEchoingIt() throws Exception {
    final String tag = "echo9775-" + "y".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH);
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/begin/graph")).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty(TAG_HEADER, tag);
      final int status = connection.getResponseCode();
      assertThat(status).isEqualTo(400);
      assertThat(body(connection, status)).doesNotContain("echo9775");
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body, String sessionId) {
  }

  private Response call(final String method, final String path, final JSONObject payload, final Map<String, String> headers)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    try {
      connection.setRequestMethod(method);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      for (final Map.Entry<String, String> header : headers.entrySet())
        connection.setRequestProperty(header.getKey(), header.getValue());
      if (payload != null)
        formatPayload(connection, payload);
      final int status = connection.getResponseCode();
      return new Response(status, body(connection, status), connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID));
    } finally {
      connection.disconnect();
    }
  }

  private static String body(final HttpURLConnection connection, final int status) throws IOException {
    try (final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
      return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
