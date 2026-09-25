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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8331: a {@code restore database} (or {@code restore backup}, {@code import database}) asked for as an SSE
 * progress stream wrote the stream itself and returned no response, so its {@code X-Request-Id} reservation was
 * aborted instead of completed and a retry with the same id ran the restore again. It is now answered to the cache in
 * both encodings: a retry is replayed as a one-frame stream of the {@code completed} event when it asks for a stream,
 * and as the JSON body otherwise.
 * <p>
 * The restored database is dropped between the request and its retry: a retry that executed again would restore it
 * once more, so its absence is what proves the retry was a replay.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8331StreamedRestoreIdempotencyTest extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Restoring from a local file is an operator action that has to be opted into (issue #5027)
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  @Test
  void aStreamedRestoreIsReplayedNotExecutedAgain() throws Exception {
    final String command = "restore database restored8331 file://" + backup().getAbsolutePath();
    try {
      final HttpResponse<String> first = serverCommand(command, "req-8331-stream", true);
      assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
      assertThat(first.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).contains("text/event-stream"));
      assertThat(first.body()).contains("\"status\":\"completed\"");
      assertThat(getServer(0).existsDatabase("restored8331")).isTrue();
      drop("restored8331");

      // The same request again, still asking for a stream: the terminal event, once, and no second restore
      final HttpResponse<String> streamedRetry = serverCommand(command, "req-8331-stream", true);
      assertThat(streamedRetry.statusCode()).as(streamedRetry.body()).isEqualTo(200);
      assertThat(streamedRetry.headers().firstValue("Content-Type")).hasValueSatisfying(v -> assertThat(v).contains("text/event-stream"));
      assertThat(streamedRetry.body()).startsWith("data: ").contains("\"status\":\"completed\"")
          .contains("restored8331 restored successfully").doesNotContain("\"status\":\"progress\"");
      assertThat(streamedRetry.body().split("\n\n")).hasSize(1);

      // ...and as the JSON a buffered request is answered with
      final HttpResponse<String> bufferedRetry = serverCommand(command, "req-8331-stream", false);
      assertThat(bufferedRetry.statusCode()).as(bufferedRetry.body()).isEqualTo(200);
      assertThat(new JSONObject(bufferedRetry.body()).getString("result")).isEqualTo("ok");

      assertThat(getServer(0).existsDatabase("restored8331")).as("a retry restored the database a second time").isFalse();
    } finally {
      drop("restored8331");
    }
  }

  @Test
  void aBufferedRestoreRetriedAsAStreamGetsTheTerminalEvent() throws Exception {
    final String command = "restore database restored8331b file://" + backup().getAbsolutePath();
    try {
      final HttpResponse<String> first = serverCommand(command, "req-8331-buffered", false);
      assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
      assertThat(getServer(0).existsDatabase("restored8331b")).isTrue();
      drop("restored8331b");

      final HttpResponse<String> streamedRetry = serverCommand(command, "req-8331-buffered", true);
      assertThat(streamedRetry.statusCode()).as(streamedRetry.body()).isEqualTo(200);
      assertThat(streamedRetry.body()).startsWith("data: ").contains("\"status\":\"completed\"");
      assertThat(getServer(0).existsDatabase("restored8331b")).isFalse();
    } finally {
      drop("restored8331b");
    }
  }

  @Test
  void aStreamedRestoreThatFailsIsNotReplayed() throws Exception {
    // Failures are not cached, exactly as for a buffered request: the retry executes again (and fails again)
    final String command = "restore database restored8331c file:///nonexistent/archive-8331.zip";
    // The restore had started streaming when it failed, so the failure is an error frame of a 200 stream
    final HttpResponse<String> first = serverCommand(command, "req-8331-failed", true);
    assertThat(first.body()).contains("\"status\":\"error\"");
    final HttpResponse<String> retry = serverCommand(command, "req-8331-failed", true);
    assertThat(retry.body()).contains("\"status\":\"error\"").doesNotContain("\"status\":\"completed\"");
    assertThat(getServer(0).existsDatabase("restored8331c")).isFalse();
  }

  private File backup() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/command/graph")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", "backup database").toString()))
        .setHeader("Authorization", authorization())
        .build();
    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    final String fileName = new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getString("backupFile");
    final File file = new File("./target/backups/graph", fileName);
    assertThat(file).exists();
    return file;
  }

  private HttpResponse<String> serverCommand(final String command, final String requestId, final boolean stream) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization", authorization())
        .setHeader("Content-Type", "application/json")
        .setHeader(IdempotencyCache.HEADER_REQUEST_ID, requestId);
    if (stream)
      builder.setHeader("Accept", "text/event-stream");
    return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private void drop(final String database) throws Exception {
    if (!getServer(0).existsDatabase(database))
      return;
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", "drop database " + database).toString()))
        .setHeader("Authorization", authorization())
        .build();
    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
