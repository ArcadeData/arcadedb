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
package com.arcadedb.server.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A local stand-in for the customer portal that speaks the support contract (SUPPORT-API.md, section 2). Tests never call the
 * real portal. Bound to the loopback interface on a port the OS assigns.
 */
final class MockPortal implements AutoCloseable {
  static final String CLIENT_ID = "ws-mock-1";
  static final String KEY       = "wsk_MOCKKEY0123456789abcdefghijklmnopqrstuvwxy";

  /** A request as received; the body is kept up to {@link #MAX_KEPT} bytes, its whole length is counted. */
  record Recorded(String method, String path, String query, Map<String, String> headers, byte[] body, long bodyLength) {
    String header(final String name) {
      return headers.get(name.toLowerCase());
    }

    String bodyText() {
      return new String(body, StandardCharsets.UTF_8);
    }
  }

  record Response(int status, String body, Map<String, String> headers) {
    Response(final int status, final String body) {
      this(status, body, Map.of());
    }
  }

  static final int MAX_KEPT = 4 * 1024 * 1024;

  private final HttpServer                server;
  final List<Recorded>                    requests = new CopyOnWriteArrayList<>();
  final AtomicLong                        received = new AtomicLong();
  volatile Function<Recorded, Response>   handler;

  MockPortal() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    handler = this::contract;
    server.createContext("/", this::handle);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  SupportConfiguration.Registration registration() {
    return new SupportConfiguration.Registration(url(), CLIENT_ID, KEY, "", false);
  }

  private void handle(final HttpExchange exchange) throws IOException {
    final ByteArrayOutputStream kept = new ByteArrayOutputStream();
    long total = 0;
    try (final InputStream in = exchange.getRequestBody()) {
      final byte[] buffer = new byte[65536];
      int n;
      while ((n = in.read(buffer)) >= 0) {
        total += n;
        if (kept.size() < MAX_KEPT)
          kept.write(buffer, 0, Math.min(n, MAX_KEPT - kept.size()));
      }
    }
    received.addAndGet(total);
    final Map<String, String> headers = new java.util.HashMap<>();
    exchange.getRequestHeaders().forEach((k, v) -> headers.put(k.toLowerCase(), v.get(0)));
    final Recorded recorded = new Recorded(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
        exchange.getRequestURI().getQuery(), headers, kept.toByteArray(), total);
    requests.add(recorded);

    final Response response = handler.apply(recorded);
    final byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    response.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
    if (response.status() == 204 || bytes.length == 0)
      exchange.sendResponseHeaders(response.status(), -1);
    else {
      exchange.sendResponseHeaders(response.status(), bytes.length);
      try (final OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
    exchange.close();
  }

  static String error(final String code, final String message) {
    return "{\"error\":\"" + code + "\",\"message\":\"" + message + "\"}";
  }

  /** The happy path of the contract, with the authentication checks. */
  private Response contract(final Recorded r) {
    final String auth = r.header("authorization");
    if (auth == null || !auth.equals("Bearer " + KEY))
      return new Response(401, error("invalid_key", "The key is not valid"));
    if (!CLIENT_ID.equals(r.header("x-client-id")))
      return new Response(403, error("client_mismatch", "The client id does not match"));

    final String path = r.path();
    if (path.equals("/api/v1/support/whoami"))
      return new Response(200, """
          {"workspace":{"id":"ws-mock-1","name":"Acme Corp"},"key":{"id":"k1","label":"Studio: prod","scopes":["support:create","support:read"]},\
          "plan":{"entitled":true,"label":"Gold","units":2,"endsOn":1893456000000},\
          "sla":{"S1":"1 hour","S2":"4 hours","S3":"1 business day","S4":"2 business days","coverage":"24x7"},\
          "buyUrl":"https://arcadedb.com/pricing.html"}""");
    if (path.equals("/api/v1/support/installation") && r.method().equals("POST"))
      return new Response(200, "{\"status\":\"created\",\"installationId\":\"inst-1\",\"name\":\"arcadedb_0\",\"filled\":[],\"differs\":[]}");
    if (path.equals("/api/v1/support/issues") && r.method().equals("POST"))
      return new Response(201, "{\"number\":42,\"url\":\"https://portal.arcadedb.com/#/issues/42\",\"workspaceId\":\"ws-mock-1\"}");
    if (path.equals("/api/v1/support/issues") && r.method().equals("GET"))
      return new Response(200, "[{\"number\":42,\"title\":\"Slow\",\"status\":\"open\",\"severity\":\"S2\",\"updatedOn\":1}]");
    if (path.matches("/api/v1/support/issues/\\d+") && r.method().equals("GET"))
      return new Response(200, "{\"number\":42,\"title\":\"Slow\",\"status\":\"open\",\"timeline\":[{\"side\":\"client\",\"body\":\"hi\"}]}");
    if (path.matches("/api/v1/support/issues/\\d+") && r.method().equals("PUT"))
      return new Response(204, "");
    if (path.matches("/api/v1/support/issues/\\d+/comments") && r.method().equals("POST"))
      return new Response(201, "{\"side\":\"client\",\"authorLabel\":\"Studio: prod\",\"body\":\"thanks\"}");
    if (path.matches("/api/v1/support/issues/\\d+/attachments") && r.method().equals("POST"))
      return new Response(200, "{\"attachments\":[{\"name\":\"logs.zip\"}]}");
    return new Response(404, error("not_found", "No such route"));
  }

  Recorded last() {
    return requests.get(requests.size() - 1);
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
