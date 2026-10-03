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
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A local stand-in for the customer portal's AI contract (docs/AI-ASSISTANT.md of the portal, section 12): the process endpoint that
 * runs {@code ai-send}, {@code ai-stop} and {@code ai-usage}, and the NDJSON stream of a turn. Tests never call the real portal.
 */
final class FakeAiPortal implements AutoCloseable {
  static final String CLIENT_ID = "ws-ai-1";
  static final String KEY       = "wsk_AIKEY0123456789abcdefghijklmnopqrstuvwxyz";

  final List<JSONObject>        sends    = new CopyOnWriteArrayList<>();
  final List<String>            stops    = new CopyOnWriteArrayList<>();
  final List<String>            streams  = new CopyOnWriteArrayList<>();
  final List<String>            usageAsks = new CopyOnWriteArrayList<>();
  private final Map<String, JSONObject> turns = new ConcurrentHashMap<>();

  /** What {@code ai-usage} answers. */
  volatile JSONObject usage = new JSONObject().put("tier", "assistant").put("turns", 3).put("spent", 3.0).put("budget", 20.0).put("percent", 15)
      .put("resetsOn", "2026-11-01").put("enabled", true);
  /** A process refusal for an {@code ai-send} body ("ai.not_entitled: ..."), or null to accept it. */
  volatile Function<JSONObject, String> refusal = body -> null;
  /** An HTTP failure of the whole key route ({status, body}), or null. */
  volatile Function<String, String[]> routeFailure = process -> null;
  /** The envelopes the portal streams for an accepted turn. */
  volatile Function<JSONObject, List<JSONObject>> script = body -> answer("hello", List.of());
  /** True when the stream has to be cut after its first line (no done). */
  volatile boolean cutStream;

  private final Undertow server;

  FakeAiPortal() {
    server = Undertow.builder().addHttpListener(0, "127.0.0.1").setHandler(Handlers.path()
        .addExactPath("/api/v1/process-execute", exchange -> exchange.dispatch(() -> process(exchange)))
        .addPrefixPath("/api/v1/key/chat-stream/", exchange -> exchange.dispatch(() -> stream(exchange)))).build();
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + ((InetSocketAddress) server.getListenerInfo().get(0).getAddress()).getPort();
  }

  /** The stream of a plain answer: some text, then the result event, then done. */
  static List<JSONObject> answer(final String text, final List<JSONObject> toolCalls) {
    return answer(text, toolCalls, new JSONArray());
  }

  static List<JSONObject> answer(final String text, final List<JSONObject> toolCalls, final JSONArray commands) {
    final JSONArray calls = new JSONArray();
    toolCalls.forEach(calls::put);
    return List.of(new JSONObject().put("type", "delta").put("text", text),
        new JSONObject().put("type", "event").put("name", "result").put("data",
            new JSONObject().put("response", text).put("commands", commands).put("toolCalls", calls)
                .put("usage", new JSONObject().put("turns", 4).put("spent", 3.41).put("budget", 20.0).put("percent", 17))),
        new JSONObject().put("type", "done"));
  }

  /** A plain answer that also asks Studio to draw charts (the portal's {@code charts} member of the result). */
  static List<JSONObject> answerWithCharts(final String text, final JSONArray charts) {
    final List<JSONObject> events = new ArrayList<>(answer(text, List.of()));
    events.get(1).getJSONObject("data").put("charts", charts);
    return events;
  }

  static JSONObject chart(final String type, final String query, final String x, final String... y) {
    final JSONArray columns = new JSONArray();
    for (final String column : y)
      columns.put(column);
    return new JSONObject().put("type", type).put("title", "A chart").put("language", "sql").put("query", query).put("x", x)
        .put("y", columns);
  }

  static JSONObject toolCall(final String id, final String name, final JSONObject arguments) {
    return new JSONObject().put("id", id).put("name", name).put("arguments", arguments);
  }

  private boolean authorized(final HttpServerExchange exchange) {
    final String auth = exchange.getRequestHeaders().getFirst("Authorization");
    if (!("Bearer " + KEY).equals(auth) || !CLIENT_ID.equals(exchange.getRequestHeaders().getFirst("X-Client-Id"))) {
      reply(exchange, 401, "{\"error\":\"invalid_key\",\"message\":\"The key is not valid\"}");
      return false;
    }
    return true;
  }

  private void process(final HttpServerExchange exchange) {
    try {
      exchange.startBlocking();
      final String raw = new String(exchange.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!authorized(exchange))
        return;
      final String process = exchange.getRequestHeaders().getFirst(new HttpString("x-api-process"));
      final String[] failure = routeFailure.apply(process);
      if (failure != null) {
        reply(exchange, Integer.parseInt(failure[0]), failure[1]);
        return;
      }
      final JSONObject envelope = raw.isBlank() ? new JSONObject() : new JSONObject(raw);
      final JSONObject body = envelope.getJSONObject("parameters", new JSONObject());
      switch (process) {
      case "ai-usage" -> {
        usageAsks.add("usage");
        reply(exchange, 200, new JSONObject().put("output", new JSONObject().put("data", usage)).toString());
      }
      case "ai-send" -> {
        sends.add(body);
        final String refused = refusal.apply(body);
        if (refused != null) {
          reply(exchange, 500, "{\"error\":\"process_failed\",\"message\":\"" + refused + "\"}");
          return;
        }
        turns.put(body.getString("turnId"), body);
        reply(exchange, 200, new JSONObject().put("output", new JSONObject().put("data",
            new JSONObject().put("turnId", body.getString("turnId")).put("accepted", true))).toString());
      }
      case "ai-stop" -> {
        stops.add(body.getString("turnId", ""));
        reply(exchange, 200, "{\"output\":{\"data\":{\"stopped\":true}}}");
      }
      default -> reply(exchange, 404, "{\"error\":\"no_such_process\"}");
      }
    } catch (final Exception e) {
      reply(exchange, 500, "{}");
    }
  }

  private void stream(final HttpServerExchange exchange) {
    try {
      if (!authorized(exchange))
        return;
      final String turnId = exchange.getRelativePath().substring(1);
      streams.add(turnId);
      final JSONObject turn = turns.get(turnId);
      if (turn == null) {
        reply(exchange, 200, "{\"type\":\"none\"}");
        return;
      }
      exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/x-ndjson");
      exchange.startBlocking();
      final List<JSONObject> lines = script.apply(turn);
      long seq = 1;
      for (final JSONObject line : lines) {
        line.put("seq", seq++);
        exchange.getOutputStream().write((line + "\n").getBytes(StandardCharsets.UTF_8));
        exchange.getOutputStream().flush();
        if (cutStream)
          break;
      }
      exchange.endExchange();
    } catch (final IOException e) {
      // the reader went away
    }
  }

  private static void reply(final HttpServerExchange exchange, final int status, final String body) {
    exchange.setStatusCode(status);
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "application/json");
    exchange.getResponseSender().send(body);
  }

  @Override
  public void close() {
    server.stop();
  }
}
