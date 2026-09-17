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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7472, item 1: the control plane's SSE progress stream reported raw internal messages regardless of
 * {@code arcadedb.server.mode=production}.
 * <p>
 * A failure BEFORE the stream starts is still an HTTP error, and goes through
 * {@code AbstractServerHttpHandler}'s status mapping, which has always concealed. A failure AFTER it - by which
 * point the response has begun and only an {@code error} frame is left - reported the exception's own message
 * whatever the mode said. The surface is root-gated, so the exposure is to an already-privileged caller; but the
 * concealment is either a policy or it is not, and a surface that opts out silently makes it unreliable as one.
 * <p>
 * The frame keeps its bounded {@code exception} class name in every mode, exactly as the JSON error body keeps
 * its own {@code exception} field: it is what tells a client WHICH failure this was, and carries no free text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7472ProductionModeConcealsSseErrorsIT extends BaseGraphServerTest {

  /**
   * The first rows of a perfectly good CSV, followed by a source that goes quiet and never closes. That is a
   * failure the import can only hit AFTER it has begun - it announces the database, sniffs the format and reports
   * its first counters first - which is exactly the window in which an SSE error frame is the only way to report
   * anything at all.
   */
  private static final String HEAD           = "id,name\n1,Jay\n2,Ann\n";
  private static final int    DECLARED_BYTES = 1_000_000;
  /** Short enough that the test costs a fraction of a second, long enough to be unmistakably a stall. */
  private static final int    READ_TIMEOUT_MS = 500;

  private final CountDownLatch release = new CountDownLatch(1);

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_MODE, "production");
    // The import source is a loopback URL, which the SSRF guard blocks by default; this is the documented opt-out
    // and is not what is under test here.
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
    config.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, false);
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(READ_TIMEOUT_MS);
  }

  @AfterEach
  void restoreTimeout() {
    release.countDown();
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.reset();
  }

  @Test
  void anSseErrorFrameConcealsTheInternalMessageInProductionMode() throws Exception {
    assertThat(getServer(0).isProductionMode()).isTrue();

    final HttpServer origin = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    final String databaseName = "issue7472_sse_conceal";
    try {
      origin.createContext("/data", exchange -> {
        exchange.sendResponseHeaders(200, DECLARED_BYTES);
        exchange.getResponseBody().write(HEAD.getBytes(StandardCharsets.UTF_8));
        exchange.getResponseBody().flush();
        try {
          release.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        exchange.close();
      });
      origin.start();

      final String url = "http://127.0.0.1:" + origin.getAddress().getPort() + "/data";
      final HttpResponse<String> response = sse("import database " + databaseName + " " + url);

      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);

      final List<JSONObject> errors = errorFrames(response.body());
      assertThat(errors)
          .as("the failure must reach the client as an SSE error frame, or this test asserts nothing: " + response.body())
          .hasSizeGreaterThanOrEqualTo(1);

      for (final JSONObject error : errors) {
        assertThat(error.getString("message"))
            .as("the internal message is concealed: " + error)
            .doesNotContain(url)
            .doesNotContain(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getKey())
            .contains("Check the server log");
        assertThat(error.getString("exception", ""))
            .as("but the bounded exception class name survives, as it does in the JSON error body: " + error)
            .isNotEmpty();
      }
    } finally {
      // Let the stalling handler return before stopping the server, which waits for it: without this the test
      // pays the handler's full 30s latch timeout after it has already finished asserting.
      release.countDown();
      origin.stop(0);
      if (getServer(0).existsDatabase(databaseName))
        getServer(0).getDatabase(databaseName).getEmbedded().drop();
    }
  }

  private static List<JSONObject> errorFrames(final String body) {
    final List<JSONObject> errors = new ArrayList<>();
    for (final String frame : body.split("\n\n")) {
      final String trimmed = frame.trim();
      if (!trimmed.startsWith("data: "))
        continue;
      final JSONObject event = new JSONObject(trimmed.substring("data: ".length()));
      if ("error".equals(event.getString("status", "")))
        errors.add(event);
    }
    return errors;
  }

  private HttpResponse<String> sse(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:2480/api/v1/server"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();

    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }
}
