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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8721: the relay to the AI gateway answers every {@code tool_call} it reads by POSTing a result
 * to {@code /api/chat/tool_result/:sessionId}, and the gateway's model loop stays paused on the call until that result
 * arrives. A tool call the relay could not run - arguments that are not a JSON object, a name that is not a string, a tool
 * that threw instead of returning {@code {"error":...}} - ended the Studio stream with an {@code error} event but never
 * POSTed anything, so the gateway waited on the call until its own timeout. Each such call is now answered with an
 * {@code {"error":...}} result, the model sees it and can recover, and the answer completes.
 * <p>
 * A tool that throws cannot be planted in the real relay (it builds its own {@link ToolDispatcher}, which already turns
 * every {@link Exception} into an error result): {@code ToolDispatcherTest} drives {@link ToolDispatcher#executeSafely}, the
 * one call the relay makes, with a throwing tool. Here the same call is driven through the relay by the inputs that do
 * reach it from the gateway.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8721AiChatToolCallAlwaysAnsweredTest extends BaseGraphServerTest {

  /** A hang detector, not a latency bound: how long the fake gateway waits for each tool result. */
  private static final int HANG_DETECT_MS = 30_000;

  /** The tool results POSTed by the relay, not yet consumed by the fake gateway. */
  private final BlockingQueue<JSONObject> toolResults = new LinkedBlockingQueue<>();
  /** The tool results the fake gateway consumed, in the order they arrived. */
  private final List<JSONObject>          received    = new CopyOnWriteArrayList<>();
  private       Undertow                  gateway;

  @AfterEach
  void stopGateway() {
    if (gateway != null)
      gateway.stop();
    // Same clean-up as AiChatHandlerStreamingTest: the chats directory is shared by every test in this module.
    final File[] chats = new File("./target/chats/" + ChatStorage.hashUsername("root")).listFiles();
    if (chats != null)
      for (final File f : chats)
        //noinspection ResultOfMethodCallIgnored
        f.delete();
  }

  /** The arguments are a JSON array: the relay used to throw reading them, before the tool ran or any result was POSTed. */
  @Test
  void argumentsThatAreNotAJsonObjectAreAnsweredWithAnErrorResult() throws Exception {
    final JSONObject call = new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
        .put("arguments", new JSONArray().put("db"));

    final List<JSONObject> events = chatWith(call);

    final JSONObject delivered = delivered(0);
    assertThat(delivered).as("the gateway got a result for the call").isNotNull();
    assertThat(delivered.getString("id")).isEqualTo("tc-1");
    assertThat(new JSONObject(delivered.getString("result")).getString("error", "")).contains("not a JSON object");

    assertThat(toolEnd(events).getString("error", "")).contains("not a JSON object");
    assertAnswerCompleted(events, 1);
  }

  /** Some models send the arguments as a string holding the JSON object: the tool runs with them. */
  @Test
  void argumentsSentAsAStringHoldingAnObjectRunTheTool() throws Exception {
    final JSONObject call = new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
        .put("arguments", new JSONObject().put("database", getDatabaseName()).toString());

    final List<JSONObject> events = chatWith(call);

    final JSONObject delivered = delivered(0);
    assertThat(delivered).isNotNull();
    final JSONObject result = new JSONObject(delivered.getString("result"));
    assertThat(result.has("error")).as("result: %s", result).isFalse();
    assertThat(result.getString("database", null)).isEqualTo(getDatabaseName());

    assertThat(toolEnd(events).has("error")).isFalse();
    assertAnswerCompleted(events, 1);
  }

  /** A name that is not a string threw reading it, before anything was POSTed. */
  @Test
  void aToolNameThatIsNotAStringIsAnsweredWithAnErrorResult() throws Exception {
    final JSONObject call = new JSONObject().put("type", "tool_call").put("id", "tc-1")
        .put("name", new JSONObject().put("nested", true)).put("arguments", new JSONObject());

    final List<JSONObject> events = chatWith(call);

    final JSONObject delivered = delivered(0);
    assertThat(delivered).isNotNull();
    assertThat(delivered.getString("id")).isEqualTo("tc-1");
    assertThat(new JSONObject(delivered.getString("result")).getString("error", "")).contains("Unknown tool");
    assertAnswerCompleted(events, 1);
  }

  /** A failed call does not stop the next one: each call gets its own result, in order. */
  @Test
  void aFailedToolCallDoesNotStopTheNextOne() throws Exception {
    final JSONObject bad = new JSONObject().put("type", "tool_call").put("id", "tc-1").put("name", "get_schema")
        .put("arguments", "not json at all");
    final JSONObject good = new JSONObject().put("type", "tool_call").put("id", "tc-2").put("name", "get_server_info")
        .put("arguments", new JSONObject());

    final List<JSONObject> events = chatWith(bad, good);

    final JSONObject first = delivered(0);
    final JSONObject second = delivered(1);
    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    assertThat(first.getString("id")).isEqualTo("tc-1");
    assertThat(new JSONObject(first.getString("result")).has("error")).isTrue();
    assertThat(second.getString("id")).isEqualTo("tc-2");
    assertThat(new JSONObject(second.getString("result")).getString("version", null)).isNotBlank();
    assertAnswerCompleted(events, 2);
  }

  /** The index-th tool result the gateway got, or {@code null} when it got fewer. */
  private JSONObject delivered(final int index) {
    return index < received.size() ? received.get(index) : null;
  }

  private static JSONObject toolEnd(final List<JSONObject> events) {
    return events.stream().filter(e -> "tool_end".equals(e.getString("type", ""))).findFirst()
        .orElseThrow(() -> new AssertionError("no tool_end in " + events));
  }

  /** The gateway's 'done' says how many results it got before answering: the stream ends with it, and with no error. */
  private static void assertAnswerCompleted(final List<JSONObject> events, final int expectedResults) {
    assertThat(events).as("events: %s", events).noneMatch(e -> "error".equals(e.getString("type", "")));
    final JSONObject last = events.get(events.size() - 1);
    assertThat(last.getString("type", "")).as("events: %s", events).isEqualTo("done");
    assertThat(last.getString("response", "")).isEqualTo("got " + expectedResults + " tool results");
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

  /**
   * Starts a gateway that sends the session and the given tool calls, waits for a result to each (the way the real gateway's
   * model loop waits), then answers 'done'. A result that never comes ends the gateway's stream without 'done'.
   */
  private List<JSONObject> chatWith(final JSONObject... toolCalls) throws Exception {
    gateway = Undertow.builder().addHttpListener(0, "127.0.0.1").setHandler(Handlers.path()//
        .addExactPath("/api/chat", exchange -> exchange.dispatch(() -> {
          try {
            exchange.startBlocking();
            exchange.getInputStream().readAllBytes();
            exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
            exchange.setStatusCode(200);
            final OutputStream out = exchange.getOutputStream();
            writeSse(out, new JSONObject().put("type", "session").put("sessionId", "s-8721"));
            for (final JSONObject call : toolCalls) {
              writeSse(out, call);
              final JSONObject result = toolResults.poll(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
              if (result == null)
                return; // the relay never answered: end the stream without 'done'
              received.add(result);
            }
            writeSse(out, new JSONObject().put("type", "done").put("response", "got " + received.size() + " tool results"));
          } catch (final Exception e) {
            // the test asserts on the client side
          } finally {
            exchange.endExchange();
          }
        }))//
        .addPrefixPath("/api/chat/tool_result/", exchange -> exchange.dispatch(() -> {
          try {
            exchange.startBlocking();
            toolResults.add(new JSONObject(new String(exchange.getInputStream().readAllBytes(), StandardCharsets.UTF_8)));
            exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/json");
            exchange.getResponseSender().send("{\"ok\":true}");
          } catch (final Exception e) {
            exchange.endExchange();
          }
        }))).build();
    gateway.start();

    final AiConfiguration config = getServer(0).getAiConfiguration();
    config.activate("test-token", "127.0.0.1", "hw-test", "test-version");
    // No public setter: the URL is normally persisted by activation. Same injection as AiChatHandlerStreamingTest.
    final var field = AiConfiguration.class.getDeclaredField("gatewayUrl");
    field.setAccessible(true);
    field.set(config, "http://127.0.0.1:" + ((InetSocketAddress) gateway.getListenerInfo().get(0).getAddress()).getPort());

    return streamedChat();
  }

  private List<JSONObject> streamedChat() throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(getServerHttpUrl(0, "/api/v1/ai/chat/stream")).toURL()
        .openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setReadTimeout(2 * HANG_DETECT_MS);
      conn.setDoOutput(true);
      try (final OutputStream out = conn.getOutputStream()) {
        out.write(new JSONObject().put("database", getDatabaseName()).put("message", "What types exist?").toString()
            .getBytes(StandardCharsets.UTF_8));
      }
      assertThat(conn.getResponseCode()).isEqualTo(200);
      final String body;
      try (final InputStream in = conn.getInputStream()) {
        body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      final List<JSONObject> events = new ArrayList<>();
      for (final String frame : body.split("\n\n")) {
        final String trimmed = frame.trim();
        if (trimmed.startsWith("data: "))
          events.add(new JSONObject(trimmed.substring(6)));
      }
      assertThat(events).as("stream body: %s", body).isNotEmpty();
      return events;
    } finally {
      conn.disconnect();
    }
  }

  private static void writeSse(final OutputStream out, final JSONObject event) throws IOException {
    out.write(("data: " + event + "\n\n").getBytes(StandardCharsets.UTF_8));
    out.flush();
  }
}
