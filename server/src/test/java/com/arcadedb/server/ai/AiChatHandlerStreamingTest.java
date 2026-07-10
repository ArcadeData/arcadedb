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

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test: stands up a fake AI gateway that emits the new client-orchestrated
 * SSE protocol (session, tool_call, done) and verifies that ArcadeDB's AiChatHandler
 * executes the tool locally and POSTs the result back to /api/chat/tool_result/:sessionId,
 * with no inbound network connection from the gateway required.
 */
class AiChatHandlerStreamingTest extends BaseGraphServerTest {

  private Undertow                                fakeGateway;
  private final List<String>                      gatewayRequestBodies = new ArrayList<>();
  private final CompletableFuture<JSONObject>     toolResultPosted     = new CompletableFuture<>();
  private volatile String                         issuedSessionId;
  private volatile String                         issuedToolCallId;

  @BeforeEach
  void startFakeGateway() {
    fakeGateway = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(Handlers.path()
            // Fake gateway /api/chat: emit SSE session + tool_call + done.
            // The 'done' event is only written AFTER the ArcadeDB client has POSTed
            // a tool_result, which we synchronize via toolResultPosted CompletableFuture.
            .addExactPath("/api/chat", exchange -> {
              if (!exchange.getRequestMethod().equals(Methods.POST)) {
                exchange.setStatusCode(405);
                exchange.endExchange();
                return;
              }
              // Hand off to a worker thread - all the reads/writes below are blocking.
              exchange.dispatch(() -> {
                try {
                  exchange.startBlocking();
                  gatewayRequestBodies.add(new String(exchange.getInputStream().readAllBytes(), StandardCharsets.UTF_8));

                  exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
                  exchange.setStatusCode(200);

                  issuedSessionId = "test-session-" + System.nanoTime();
                  issuedToolCallId = "tc-" + System.nanoTime();

                  writeSse(exchange, new JSONObject()
                      .put("type", "session")
                      .put("sessionId", issuedSessionId));

                  writeSse(exchange, new JSONObject()
                      .put("type", "tool_call")
                      .put("id", issuedToolCallId)
                      .put("name", "get_schema")
                      .put("arguments", new JSONObject().put("database", getDatabaseName())));

                  // Wait until the ArcadeDB client delivers the tool result via callback
                  final JSONObject delivered = toolResultPosted.get(15, TimeUnit.SECONDS);

                  writeSse(exchange, new JSONObject()
                      .put("type", "done")
                      .put("response", "schema returned " + delivered.getString("result", "").length() + " bytes")
                      .put("commands", new JSONArray()));
                  exchange.endExchange();
                } catch (final Exception e) {
                  e.printStackTrace();
                  exchange.endExchange();
                }
              });
            })
            // Tool callback endpoint - we just record the body and unblock /api/chat.
            .addPrefixPath("/api/chat/tool_result/", exchange -> exchange.dispatch(() -> {
              try {
                exchange.startBlocking();
                final String body = new String(exchange.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                toolResultPosted.complete(new JSONObject(body));
                exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/json");
                exchange.getResponseSender().send("{\"ok\":true}");
              } catch (final Exception e) {
                e.printStackTrace();
                exchange.endExchange();
              }
            })))
        .build();
    fakeGateway.start();
  }

  @AfterEach
  void stopFakeGateway() {
    if (fakeGateway != null)
      fakeGateway.stop();
    // Wipe the chats this test wrote so the next test that asserts an empty chat
    // list (AiServerTest.listChatsReturnsEmpty) is not polluted; SERVER_ROOT_PATH is
    // shared across all tests in this module.
    final var rootDir = new File("./target/chats/root");
    if (rootDir.exists()) {
      final var files = rootDir.listFiles();
      if (files != null)
        for (final var f : files)
          //noinspection ResultOfMethodCallIgnored
          f.delete();
    }
  }

  private static void writeSse(final HttpServerExchange exchange, final JSONObject event) throws IOException {
    final byte[] bytes = ("data: " + event + "\n\n").getBytes(StandardCharsets.UTF_8);
    exchange.getOutputStream().write(bytes);
    exchange.getOutputStream().flush();
  }

  private int fakeGatewayPort() {
    return ((InetSocketAddress) fakeGateway.getListenerInfo().getFirst().getAddress()).getPort();
  }

  @Test
  void streamingAutoModeExecutesToolLocallyAndPostsResultBack() throws Exception {
    // Point the AI assistant at our fake gateway and activate it with a dummy token.
    final var aiConfig = getServer(0).getAiConfiguration();
    aiConfig.activate("test-token", "127.0.0.1", "hw-test", "test-version");
    setGatewayUrl(aiConfig, "http://127.0.0.1:" + fakeGatewayPort());

    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ai/chat").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Accept", "text/event-stream");
    conn.setDoOutput(true);

    final JSONObject body = new JSONObject()
        .put("database", getDatabaseName())
        .put("message", "What types exist?")
        .put("mode", "auto");
    try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
      out.write(body.toString().getBytes(StandardCharsets.UTF_8));
    }

    assertThat(conn.getResponseCode()).isEqualTo(200);

    final List<JSONObject> events = new ArrayList<>();
    try (final InputStream is = conn.getInputStream()) {
      final String full = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      for (final String frame : full.split("\n\n")) {
        final String trimmed = frame.trim();
        if (trimmed.startsWith("data: ")) {
          events.add(new JSONObject(trimmed.substring(6)));
        }
      }
    }

    // The handler should NOT forward arcadedb.url/sessionId to the gateway in this mode.
    assertThat(gatewayRequestBodies).hasSize(1);
    final JSONObject gatewayReq = new JSONObject(gatewayRequestBodies.getFirst());
    assertThat(gatewayReq.has("arcadedb")).isFalse();
    assertThat(gatewayReq.getBoolean("stream", false)).isTrue();

    // The studio stream should contain tool_start, tool_end, and done. Internal control
    // events (session) are consumed by the handler and should not leak through.
    assertThat(events.stream().anyMatch(e -> "session".equals(e.getString("type", "")))).isFalse();
    final JSONObject toolStart = events.stream()
        .filter(e -> "tool_start".equals(e.getString("type", ""))).findFirst().orElse(null);
    final JSONObject toolEnd = events.stream()
        .filter(e -> "tool_end".equals(e.getString("type", ""))).findFirst().orElse(null);
    final JSONObject done = events.stream()
        .filter(e -> "done".equals(e.getString("type", ""))).findFirst().orElse(null);

    assertThat(toolStart).as("expected tool_start event").isNotNull();
    assertThat(toolStart.getString("tool", null)).isEqualTo("get_schema");
    assertThat(toolEnd).as("expected tool_end event").isNotNull();
    assertThat(toolEnd.has("error")).isFalse();
    assertThat(done).as("expected done event").isNotNull();
    assertThat(done.getString("response", "")).contains("schema returned");
    assertThat(done.getString("chatId", null)).isNotBlank();

    // The gateway-side callback received the locally-executed schema JSON.
    final JSONObject delivered = toolResultPosted.get(2, TimeUnit.SECONDS);
    assertThat(delivered.getString("id", null)).isEqualTo(issuedToolCallId);
    final JSONObject schemaResult = new JSONObject(delivered.getString("result", "{}"));
    assertThat(schemaResult.getString("database", null)).isEqualTo(getDatabaseName());
    assertThat(schemaResult.getJSONArray("types").length()).isGreaterThan(0);
  }

  // AiConfiguration.gatewayUrl is a volatile package-private field; there's no public
  // setter (URL is normally driven by activation persistence). Reflect to inject the
  // test fake without touching AiConfiguration's public surface.
  private static void setGatewayUrl(final AiConfiguration config, final String url) throws Exception {
    final var field = AiConfiguration.class.getDeclaredField("gatewayUrl");
    field.setAccessible(true);
    field.set(config, url);
  }
}
